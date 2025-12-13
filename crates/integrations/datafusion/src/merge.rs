// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! MERGE operation support for Iceberg tables in DataFusion.
//!
//! This module provides programmatic MERGE (UPSERT) operations for Iceberg tables
//! using Copy-on-Write (CoW) semantics. MERGE combines INSERT, UPDATE, and DELETE
//! operations into a single atomic transaction, essential for CDC (Change Data Capture)
//! patterns.
//!
//! # Architecture
//!
//! The MERGE operation is implemented as a multi-stage execution plan:
//!
//! 1. **Scan**: Load target table with metadata columns (`_file_path`, `_pos`)
//! 2. **Join**: FULL OUTER JOIN between source and target on match condition
//! 3. **Classify**: Categorize each row as:
//!    - MATCHED (exists in both source and target)
//!    - NOT MATCHED (exists only in source - for INSERT)
//!    - NOT MATCHED BY SOURCE (exists only in target - for DELETE)
//! 4. **Apply WHEN Clauses**: First matching clause wins (order matters)
//! 5. **Write**: Write new data files and position delete files
//! 6. **Commit**: Atomically commit using `RowDeltaAction` (single snapshot)
//!
//! # Usage
//!
//! ## Programmatic API (Builder Pattern)
//!
//! ```ignore
//! use datafusion::prelude::*;
//! use iceberg_datafusion::IcebergTableProvider;
//!
//! let provider = IcebergTableProvider::try_new(catalog, namespace, "orders").await?;
//!
//! // CDC-style MERGE: Update existing, insert new, delete missing from source
//! let stats = provider
//!     .merge(source_df)
//!     .on(col("target.id").eq(col("source.id")))
//!     // Update matched rows
//!     .when_matched(Some(col("source.updated_at").gt(col("target.updated_at"))))
//!         .update_all()
//!     // Delete matched rows marked as deleted in source
//!     .when_matched(Some(col("source.is_deleted").eq(lit(true))))
//!         .delete()
//!     // Insert new rows from source
//!     .when_not_matched(None)
//!         .insert_all()
//!     // Delete target rows not in source (full CDC sync)
//!     .when_not_matched_by_source(None)
//!         .delete()
//!     .execute(&session_state)
//!     .await?;
//!
//! println!("Inserted: {}, Updated: {}, Deleted: {}",
//!     stats.rows_inserted, stats.rows_updated, stats.rows_deleted);
//! ```
//!
//! # Current Limitations
//!
//! - Updating partition columns is not supported
//! - Only works with Iceberg format version 2 tables (position deletes require v2)
//! - Source data must fit in memory for the JOIN operation

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::Session;
use datafusion::dataframe::DataFrame;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use iceberg::Catalog;
use iceberg::table::Table;

/// Statistics returned after a MERGE operation completes.
///
/// Provides both row-level and file-level metrics for operational monitoring.
/// Row-level metrics track logical changes, while file-level metrics help
/// understand the I/O cost of Copy-on-Write operations.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MergeStats {
    // === Row-level metrics ===
    /// Number of rows inserted (from WHEN NOT MATCHED THEN INSERT)
    pub rows_inserted: u64,
    /// Number of rows updated (from WHEN MATCHED THEN UPDATE)
    pub rows_updated: u64,
    /// Number of rows deleted (from WHEN MATCHED/NOT MATCHED BY SOURCE THEN DELETE)
    pub rows_deleted: u64,
    /// Number of unchanged rows that were rewritten due to Copy-on-Write.
    ///
    /// In CoW mode, updating 1 row in a 10,000-row file requires rewriting
    /// all 10,000 rows. This metric helps users understand the true I/O cost
    /// vs. logical changes (`rows_updated`).
    pub rows_copied: u64,

    // === File-level metrics ===
    /// Number of data files added (containing inserted/updated/copied rows)
    pub files_added: u64,
    /// Number of data files logically removed (via position deletes)
    pub files_removed: u64,

    // === Dynamic Partition Pruning (DPP) metrics ===
    /// Whether DPP was applied to prune the target table scan.
    ///
    /// DPP is enabled when:
    /// - The table is partitioned with identity transforms
    /// - A join key is also a partition column
    /// - The number of distinct partition values is below the threshold
    pub dpp_applied: bool,
    /// Number of distinct partition values found in source data.
    ///
    /// When `dpp_applied` is true, this indicates how many partitions
    /// the target scan was pruned to. When false, this may indicate
    /// the partition count that exceeded the threshold (if applicable).
    pub dpp_partition_count: usize,
}

impl MergeStats {
    /// Returns the total number of rows logically affected by the MERGE.
    ///
    /// This counts rows that were inserted, updated, or deleted - NOT rows
    /// that were merely copied due to Copy-on-Write semantics.
    pub fn total_affected(&self) -> u64 {
        self.rows_inserted + self.rows_updated + self.rows_deleted
    }

    /// Returns the total number of rows written (including copied rows).
    ///
    /// This represents the actual I/O cost: `rows_inserted + rows_updated + rows_copied`.
    /// Deleted rows are not written (only position delete entries).
    pub fn total_rows_written(&self) -> u64 {
        self.rows_inserted + self.rows_updated + self.rows_copied
    }
}

/// Action to take for matched rows (rows that exist in both source and target).
#[derive(Debug, Clone)]
pub enum MatchedAction {
    /// Update the matched row with specified column assignments.
    Update(Vec<(String, Expr)>),
    /// Update all columns from source (uses source column values).
    UpdateAll,
    /// Delete the matched row.
    Delete,
}

/// Action to take for rows that exist only in source (not in target).
#[derive(Debug, Clone)]
pub enum NotMatchedAction {
    /// Insert the row with specified column values.
    Insert(Vec<(String, Expr)>),
    /// Insert all columns from source row.
    InsertAll,
}

/// Action to take for rows that exist only in target (not in source).
#[derive(Debug, Clone)]
pub enum NotMatchedBySourceAction {
    /// Update the target row with specified column assignments.
    Update(Vec<(String, Expr)>),
    /// Delete the target row.
    Delete,
}

/// A WHEN MATCHED clause with optional condition and action.
#[derive(Debug, Clone)]
pub struct WhenMatchedClause {
    /// Optional additional condition (beyond the ON condition).
    pub condition: Option<Expr>,
    /// Action to take when this clause matches.
    pub action: MatchedAction,
}

/// A WHEN NOT MATCHED clause with optional condition and action.
#[derive(Debug, Clone)]
pub struct WhenNotMatchedClause {
    /// Optional additional condition on source row.
    pub condition: Option<Expr>,
    /// Action to take when this clause matches.
    pub action: NotMatchedAction,
}

/// A WHEN NOT MATCHED BY SOURCE clause with optional condition and action.
#[derive(Debug, Clone)]
pub struct WhenNotMatchedBySourceClause {
    /// Optional additional condition on target row.
    pub condition: Option<Expr>,
    /// Action to take when this clause matches.
    pub action: NotMatchedBySourceAction,
}

/// Builder for MERGE operations on Iceberg tables.
///
/// Provides a fluent API for constructing MERGE statements with multiple
/// WHEN clauses. Clause order matters - the first matching clause wins.
///
/// # Example
///
/// ```ignore
/// let stats = provider
///     .merge(source_df)
///     .on(col("target.id").eq(col("source.id")))
///     .when_matched(Some(col("source.op").eq(lit("U"))))
///         .update_all()
///     .when_matched(Some(col("source.op").eq(lit("D"))))
///         .delete()
///     .when_not_matched(None)
///         .insert_all()
///     .execute(&session_state)
///     .await?;
/// ```
/// Configuration options for MERGE operations.
pub use crate::physical_plan::merge::MergeConfig;

pub struct MergeBuilder {
    table: Table,
    catalog: Arc<dyn Catalog>,
    target_schema: ArrowSchemaRef,
    source: DataFrame,
    match_condition: Option<Expr>,
    when_matched: Vec<WhenMatchedClause>,
    when_not_matched: Vec<WhenNotMatchedClause>,
    when_not_matched_by_source: Vec<WhenNotMatchedBySourceClause>,
    merge_config: MergeConfig,
}

impl MergeBuilder {
    /// Creates a new MergeBuilder for the given table and source data.
    pub(crate) fn new(
        table: Table,
        catalog: Arc<dyn Catalog>,
        target_schema: ArrowSchemaRef,
        source: DataFrame,
    ) -> Self {
        Self {
            table,
            catalog,
            target_schema,
            source,
            match_condition: None,
            when_matched: Vec::new(),
            when_not_matched: Vec::new(),
            when_not_matched_by_source: Vec::new(),
            merge_config: MergeConfig::default(),
        }
    }

    /// Configures Dynamic Partition Pruning (DPP) for this MERGE operation.
    ///
    /// DPP optimizes MERGE performance by pruning target partitions based on
    /// the partition values present in the source data. This reduces I/O by
    /// only scanning partitions that could potentially match.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration options for DPP behavior.
    ///
    /// # Default Behavior
    ///
    /// By default, DPP is enabled with a maximum of 1000 partitions.
    /// If the source data touches more than 1000 distinct partition values,
    /// DPP is automatically disabled and a full scan is performed.
    ///
    /// # Limitations
    ///
    /// DPP currently only works when:
    /// - The partition uses an identity transform (not bucket/truncate/etc.)
    /// - The partition column is part of the join condition
    /// - The data type is one of: Int32, Int64, String, Date
    ///
    /// # Example
    ///
    /// ```ignore
    /// use iceberg_datafusion::merge::MergeConfig;
    ///
    /// // Disable DPP entirely
    /// builder.with_merge_config(MergeConfig::new().with_dpp(false))
    ///
    /// // Increase the partition threshold
    /// builder.with_merge_config(MergeConfig::new().with_dpp_max_partitions(5000))
    /// ```
    pub fn with_merge_config(mut self, config: MergeConfig) -> Self {
        self.merge_config = config;
        self
    }

    /// Sets the ON condition for matching source and target rows.
    ///
    /// This is the primary join condition. Source columns should be prefixed
    /// with "source." and target columns with "target.".
    ///
    /// # Example
    ///
    /// ```ignore
    /// builder.on(col("target.id").eq(col("source.id")))
    /// ```
    pub fn on(mut self, condition: Expr) -> Self {
        self.match_condition = Some(condition);
        self
    }

    /// Begins a WHEN MATCHED clause for rows that exist in both source and target.
    ///
    /// # Arguments
    ///
    /// * `condition` - Optional additional filter beyond the ON condition.
    ///   Use `None` to match all matched rows.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Match all matched rows
    /// builder.when_matched(None).update_all()
    ///
    /// // Match only when source is newer
    /// builder.when_matched(Some(col("source.ts").gt(col("target.ts")))).update_all()
    /// ```
    pub fn when_matched(self, condition: Option<Expr>) -> WhenMatchedBuilder {
        WhenMatchedBuilder {
            parent: self,
            condition,
        }
    }

    /// Begins a WHEN NOT MATCHED clause for rows that exist only in source.
    ///
    /// These are new rows that should typically be inserted.
    ///
    /// # Arguments
    ///
    /// * `condition` - Optional filter on source row.
    ///   Use `None` to match all unmatched source rows.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Insert all new rows
    /// builder.when_not_matched(None).insert_all()
    ///
    /// // Only insert if not a delete marker
    /// builder.when_not_matched(Some(col("source.op").neq(lit("D")))).insert_all()
    /// ```
    pub fn when_not_matched(self, condition: Option<Expr>) -> WhenNotMatchedBuilder {
        WhenNotMatchedBuilder {
            parent: self,
            condition,
        }
    }

    /// Begins a WHEN NOT MATCHED BY SOURCE clause for rows that exist only in target.
    ///
    /// These are rows in the target that don't have a matching source row,
    /// useful for implementing full CDC sync (delete rows missing from source).
    ///
    /// # Arguments
    ///
    /// * `condition` - Optional filter on target row.
    ///   Use `None` to match all unmatched target rows.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Delete all target rows not in source
    /// builder.when_not_matched_by_source(None).delete()
    ///
    /// // Only delete if target row is not protected
    /// builder.when_not_matched_by_source(Some(col("target.protected").eq(lit(false)))).delete()
    /// ```
    pub fn when_not_matched_by_source(
        self,
        condition: Option<Expr>,
    ) -> WhenNotMatchedBySourceBuilder {
        WhenNotMatchedBySourceBuilder {
            parent: self,
            condition,
        }
    }

    /// Checks for duplicate column assignments within individual WHEN clauses.
    ///
    /// Each UPDATE or INSERT clause should not assign the same column twice,
    /// as this would be confusing (last-write-wins behavior is unclear).
    fn check_duplicate_assignments(
        when_matched: &[WhenMatchedClause],
        when_not_matched: &[WhenNotMatchedClause],
        when_not_matched_by_source: &[WhenNotMatchedBySourceClause],
    ) -> DFResult<()> {
        // Helper to find duplicates in a list of column names
        fn find_duplicate(columns: &[(String, Expr)]) -> Option<&str> {
            let mut seen = std::collections::HashSet::new();
            columns
                .iter()
                .map(|(col, _)| col)
                .find(|&col| !seen.insert(col.as_str()))
                .map(|v| v as _)
        }

        // Check WHEN MATCHED UPDATE clauses
        for (idx, clause) in when_matched.iter().enumerate() {
            if let MatchedAction::Update(assignments) = &clause.action
                && let Some(dup) = find_duplicate(assignments)
            {
                return Err(DataFusionError::Plan(format!(
                    "Duplicate column '{}' in WHEN MATCHED clause #{}. \
                         Each column can only be assigned once per clause.",
                    dup,
                    idx + 1
                )));
            }
        }

        // Check WHEN NOT MATCHED INSERT clauses
        for (idx, clause) in when_not_matched.iter().enumerate() {
            if let NotMatchedAction::Insert(values) = &clause.action
                && let Some(dup) = find_duplicate(values)
            {
                return Err(DataFusionError::Plan(format!(
                    "Duplicate column '{}' in WHEN NOT MATCHED clause #{}. \
                         Each column can only be specified once per INSERT.",
                    dup,
                    idx + 1
                )));
            }
        }

        // Check WHEN NOT MATCHED BY SOURCE UPDATE clauses
        for (idx, clause) in when_not_matched_by_source.iter().enumerate() {
            if let NotMatchedBySourceAction::Update(assignments) = &clause.action
                && let Some(dup) = find_duplicate(assignments)
            {
                return Err(DataFusionError::Plan(format!(
                    "Duplicate column '{}' in WHEN NOT MATCHED BY SOURCE clause #{}. \
                         Each column can only be assigned once per clause.",
                    dup,
                    idx + 1
                )));
            }
        }

        Ok(())
    }

    /// Validates the MERGE configuration before execution.
    fn validate(&self) -> DFResult<()> {
        // Must have a match condition
        if self.match_condition.is_none() {
            return Err(DataFusionError::Plan(
                "MERGE requires an ON condition. Use .on(condition) to specify the match condition."
                    .to_string(),
            ));
        }

        // Must have at least one WHEN clause
        if self.when_matched.is_empty()
            && self.when_not_matched.is_empty()
            && self.when_not_matched_by_source.is_empty()
        {
            return Err(DataFusionError::Plan(
                "MERGE requires at least one WHEN clause".to_string(),
            ));
        }

        // Check for duplicate column assignments in WHEN clauses
        Self::check_duplicate_assignments(
            &self.when_matched,
            &self.when_not_matched,
            &self.when_not_matched_by_source,
        )?;

        // Validate partition column updates are not allowed
        let partition_spec = self.table.metadata().default_partition_spec();
        let iceberg_schema = self.table.metadata().current_schema();

        let partition_source_ids: std::collections::HashSet<i32> = partition_spec
            .fields()
            .iter()
            .map(|f| f.source_id)
            .collect();

        // Get source column names for UpdateAll validation
        let source_columns: Vec<String> = self
            .source
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect();

        // Build map of partition column names
        let partition_column_names: std::collections::HashSet<String> = partition_source_ids
            .iter()
            .filter_map(|id| iceberg_schema.field_by_id(*id).map(|f| f.name.to_string()))
            .collect();

        // Check UPDATE assignments in WHEN MATCHED clauses
        for clause in &self.when_matched {
            match &clause.action {
                MatchedAction::Update(assignments) => {
                    for (column_name, _) in assignments {
                        if let Some(field) = iceberg_schema.field_by_name(column_name)
                            && partition_source_ids.contains(&field.id)
                        {
                            return Err(DataFusionError::Plan(format!(
                                "Cannot UPDATE partition column '{column_name}' in MERGE. \
                                     Updating partition columns would require moving rows between partitions."
                            )));
                        }
                    }
                }
                MatchedAction::UpdateAll => {
                    // UpdateAll copies all source columns - check if any match partition columns
                    for source_col in &source_columns {
                        // Strip "source." prefix if present
                        let col_name = source_col.strip_prefix("source.").unwrap_or(source_col);
                        if partition_column_names.contains(col_name) {
                            return Err(DataFusionError::Plan(format!(
                                "Cannot use UPDATE * (update_all) when source contains partition column '{col_name}'. \
                                 Updating partition columns would require moving rows between partitions. \
                                 Use explicit .update([...]) instead, excluding partition columns."
                            )));
                        }
                    }
                }
                MatchedAction::Delete => {} // DELETE doesn't update columns
            }
        }

        // Check UPDATE assignments in WHEN NOT MATCHED BY SOURCE clauses
        for clause in &self.when_not_matched_by_source {
            if let NotMatchedBySourceAction::Update(assignments) = &clause.action {
                for (column_name, _) in assignments {
                    if let Some(field) = iceberg_schema.field_by_name(column_name)
                        && partition_source_ids.contains(&field.id)
                    {
                        return Err(DataFusionError::Plan(format!(
                            "Cannot UPDATE partition column '{column_name}' in MERGE. \
                                 Updating partition columns would require moving rows between partitions."
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    /// Executes the MERGE operation and returns statistics.
    ///
    /// This method:
    /// 1. Validates the MERGE configuration
    /// 2. Builds the execution plan (scan -> join -> classify -> write -> commit)
    /// 3. Executes the plan using RowDeltaAction for atomic commit
    /// 4. Returns statistics about affected rows
    ///
    /// # Arguments
    ///
    /// * `session` - The DataFusion session state
    ///
    /// # Returns
    ///
    /// Statistics about the MERGE operation (rows inserted, updated, deleted).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Validation fails (missing ON condition, no WHEN clauses, partition column update)
    /// - The merge operation fails
    /// - The commit fails (e.g., due to conflicts)
    pub async fn execute(self, _session: &dyn Session) -> DFResult<MergeStats> {
        use datafusion::execution::context::TaskContext;
        use datafusion::physical_plan::collect;

        use crate::physical_plan::merge::IcebergMergeExec;
        use crate::physical_plan::merge_commit::{
            IcebergMergeCommitExec, MERGE_RESULT_DELETED_COL, MERGE_RESULT_DPP_APPLIED_COL,
            MERGE_RESULT_DPP_PARTITION_COUNT_COL, MERGE_RESULT_INSERTED_COL,
            MERGE_RESULT_UPDATED_COL,
        };

        // Validate first
        self.validate()?;

        // Capture baseline snapshot ID for concurrency validation.
        // This ensures position deletes are still valid at commit time.
        let baseline_snapshot_id = self.table.metadata().current_snapshot_id();

        // Get the match condition (already validated to be Some)
        let match_condition = self.match_condition.clone().ok_or_else(|| {
            DataFusionError::Internal(
                "MERGE ON condition is required (should have been validated)".to_string(),
            )
        })?;

        // Get the source execution plan from the DataFrame
        let source_plan = self.source.clone().create_physical_plan().await?;

        // Build the MERGE execution plan (scan -> join -> classify -> write files)
        let merge_exec = Arc::new(IcebergMergeExec::new(
            self.table.clone(),
            self.target_schema.clone(),
            source_plan,
            match_condition,
            self.when_matched.clone(),
            self.when_not_matched.clone(),
            self.when_not_matched_by_source.clone(),
            self.merge_config.clone(),
        ));

        // Build the commit layer (deserialize files -> commit via RowDelta)
        let commit_exec = Arc::new(IcebergMergeCommitExec::new(
            self.table.clone(),
            self.catalog.clone(),
            merge_exec.clone() as Arc<dyn datafusion::physical_plan::ExecutionPlan>,
            merge_exec.schema(),
            baseline_snapshot_id,
        ));

        // Execute the full pipeline: join -> classify -> write -> commit
        let task_ctx = Arc::new(TaskContext::default());
        let batches = collect(commit_exec, task_ctx).await?;

        // Extract stats from the commit result
        if batches.is_empty() || batches[0].num_rows() == 0 {
            return Ok(MergeStats::default());
        }

        let batch = &batches[0];

        // Parse result columns from commit output
        let inserted = batch
            .column_by_name(MERGE_RESULT_INSERTED_COL)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Expected '{MERGE_RESULT_INSERTED_COL}' column in commit result"
                ))
            })?
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal("Expected UInt64Array for inserted count".to_string())
            })?
            .value(0);

        let updated = batch
            .column_by_name(MERGE_RESULT_UPDATED_COL)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Expected '{MERGE_RESULT_UPDATED_COL}' column in commit result"
                ))
            })?
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal("Expected UInt64Array for updated count".to_string())
            })?
            .value(0);

        let deleted = batch
            .column_by_name(MERGE_RESULT_DELETED_COL)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Expected '{MERGE_RESULT_DELETED_COL}' column in commit result"
                ))
            })?
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal("Expected UInt64Array for deleted count".to_string())
            })?
            .value(0);

        // Parse DPP columns from result batch
        let dpp_applied = batch
            .column_by_name(MERGE_RESULT_DPP_APPLIED_COL)
            .and_then(|col| {
                col.as_any()
                    .downcast_ref::<datafusion::arrow::array::BooleanArray>()
                    .map(|arr| arr.value(0))
            })
            .unwrap_or(false);

        let dpp_partition_count = batch
            .column_by_name(MERGE_RESULT_DPP_PARTITION_COUNT_COL)
            .and_then(|col| {
                col.as_any()
                    .downcast_ref::<datafusion::arrow::array::UInt64Array>()
                    .map(|arr| arr.value(0) as usize)
            })
            .unwrap_or(0);

        Ok(MergeStats {
            rows_inserted: inserted,
            rows_updated: updated,
            rows_deleted: deleted,
            // File metrics could be populated from manifest info in future
            rows_copied: 0,
            files_added: 0,
            files_removed: 0,
            dpp_applied,
            dpp_partition_count,
        })
    }

    /// Returns the source DataFrame for inspection.
    pub fn source(&self) -> &DataFrame {
        &self.source
    }

    /// Returns the target table for inspection.
    pub fn table(&self) -> &Table {
        &self.table
    }

    /// Returns the WHEN MATCHED clauses for inspection.
    pub fn when_matched_clauses(&self) -> &[WhenMatchedClause] {
        &self.when_matched
    }

    /// Returns the WHEN NOT MATCHED clauses for inspection.
    pub fn when_not_matched_clauses(&self) -> &[WhenNotMatchedClause] {
        &self.when_not_matched
    }

    /// Returns the WHEN NOT MATCHED BY SOURCE clauses for inspection.
    pub fn when_not_matched_by_source_clauses(&self) -> &[WhenNotMatchedBySourceClause] {
        &self.when_not_matched_by_source
    }
}

/// Builder for WHEN MATCHED clause action.
pub struct WhenMatchedBuilder {
    parent: MergeBuilder,
    condition: Option<Expr>,
}

impl WhenMatchedBuilder {
    /// Specifies UPDATE action with explicit column assignments.
    ///
    /// # Example
    ///
    /// ```ignore
    /// builder
    ///     .when_matched(None)
    ///     .update(vec![
    ///         ("name", col("source.name")),
    ///         ("updated_at", current_timestamp()),
    ///     ])
    /// ```
    pub fn update(mut self, assignments: Vec<(&str, Expr)>) -> MergeBuilder {
        let assignments: Vec<(String, Expr)> = assignments
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();

        self.parent.when_matched.push(WhenMatchedClause {
            condition: self.condition,
            action: MatchedAction::Update(assignments),
        });
        self.parent
    }

    /// Specifies UPDATE action that copies all columns from source.
    ///
    /// # Example
    ///
    /// ```ignore
    /// builder.when_matched(None).update_all()
    /// ```
    pub fn update_all(mut self) -> MergeBuilder {
        self.parent.when_matched.push(WhenMatchedClause {
            condition: self.condition,
            action: MatchedAction::UpdateAll,
        });
        self.parent
    }

    /// Specifies DELETE action for matched rows.
    ///
    /// # Example
    ///
    /// ```ignore
    /// builder.when_matched(Some(col("source.is_deleted").eq(lit(true)))).delete()
    /// ```
    pub fn delete(mut self) -> MergeBuilder {
        self.parent.when_matched.push(WhenMatchedClause {
            condition: self.condition,
            action: MatchedAction::Delete,
        });
        self.parent
    }
}

/// Builder for WHEN NOT MATCHED clause action.
pub struct WhenNotMatchedBuilder {
    parent: MergeBuilder,
    condition: Option<Expr>,
}

impl WhenNotMatchedBuilder {
    /// Specifies INSERT action with explicit column values.
    ///
    /// # Example
    ///
    /// ```ignore
    /// builder
    ///     .when_not_matched(None)
    ///     .insert(vec![
    ///         ("id", col("source.id")),
    ///         ("name", col("source.name")),
    ///         ("created_at", current_timestamp()),
    ///     ])
    /// ```
    pub fn insert(mut self, values: Vec<(&str, Expr)>) -> MergeBuilder {
        let values: Vec<(String, Expr)> = values
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();

        self.parent.when_not_matched.push(WhenNotMatchedClause {
            condition: self.condition,
            action: NotMatchedAction::Insert(values),
        });
        self.parent
    }

    /// Specifies INSERT action that copies all columns from source.
    ///
    /// # Example
    ///
    /// ```ignore
    /// builder.when_not_matched(None).insert_all()
    /// ```
    pub fn insert_all(mut self) -> MergeBuilder {
        self.parent.when_not_matched.push(WhenNotMatchedClause {
            condition: self.condition,
            action: NotMatchedAction::InsertAll,
        });
        self.parent
    }
}

/// Builder for WHEN NOT MATCHED BY SOURCE clause action.
pub struct WhenNotMatchedBySourceBuilder {
    parent: MergeBuilder,
    condition: Option<Expr>,
}

impl WhenNotMatchedBySourceBuilder {
    /// Specifies UPDATE action with explicit column assignments.
    ///
    /// # Example
    ///
    /// ```ignore
    /// builder
    ///     .when_not_matched_by_source(None)
    ///     .update(vec![("status", lit("orphaned"))])
    /// ```
    pub fn update(mut self, assignments: Vec<(&str, Expr)>) -> MergeBuilder {
        let assignments: Vec<(String, Expr)> = assignments
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();

        self.parent
            .when_not_matched_by_source
            .push(WhenNotMatchedBySourceClause {
                condition: self.condition,
                action: NotMatchedBySourceAction::Update(assignments),
            });
        self.parent
    }

    /// Specifies DELETE action for target rows not in source.
    ///
    /// # Example
    ///
    /// ```ignore
    /// builder.when_not_matched_by_source(None).delete()
    /// ```
    pub fn delete(mut self) -> MergeBuilder {
        self.parent
            .when_not_matched_by_source
            .push(WhenNotMatchedBySourceClause {
                condition: self.condition,
                action: NotMatchedBySourceAction::Delete,
            });
        self.parent
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_stats_default() {
        let stats = MergeStats::default();
        assert_eq!(stats.rows_inserted, 0);
        assert_eq!(stats.rows_updated, 0);
        assert_eq!(stats.rows_deleted, 0);
        assert_eq!(stats.rows_copied, 0);
        assert_eq!(stats.files_added, 0);
        assert_eq!(stats.files_removed, 0);
        assert_eq!(stats.total_affected(), 0);
        assert_eq!(stats.total_rows_written(), 0);
    }

    #[test]
    fn test_merge_stats_total() {
        let stats = MergeStats {
            rows_inserted: 10,
            rows_updated: 5,
            rows_deleted: 3,
            rows_copied: 100, // CoW overhead
            files_added: 2,
            files_removed: 1,
            dpp_applied: false,
            dpp_partition_count: 0,
        };
        assert_eq!(stats.total_affected(), 18); // 10 + 5 + 3 (logical changes)
        assert_eq!(stats.total_rows_written(), 115); // 10 + 5 + 100 (I/O cost)
    }

    #[test]
    fn test_merge_stats_equality() {
        let stats1 = MergeStats {
            rows_inserted: 10,
            rows_updated: 5,
            rows_deleted: 3,
            rows_copied: 100,
            files_added: 2,
            files_removed: 1,
            dpp_applied: true,
            dpp_partition_count: 5,
        };
        let stats2 = stats1.clone();
        assert_eq!(stats1, stats2);
    }

    #[test]
    fn test_when_matched_action_debug() {
        // Verify action types are Debug-printable
        let update = MatchedAction::Update(vec![("col".to_string(), datafusion::prelude::lit(1))]);
        let update_all = MatchedAction::UpdateAll;
        let delete = MatchedAction::Delete;

        assert!(format!("{:?}", update).contains("Update"));
        assert!(format!("{:?}", update_all).contains("UpdateAll"));
        assert!(format!("{:?}", delete).contains("Delete"));
    }

    #[test]
    fn test_when_not_matched_action_debug() {
        let insert =
            NotMatchedAction::Insert(vec![("col".to_string(), datafusion::prelude::lit(1))]);
        let insert_all = NotMatchedAction::InsertAll;

        assert!(format!("{:?}", insert).contains("Insert"));
        assert!(format!("{:?}", insert_all).contains("InsertAll"));
    }

    #[test]
    fn test_when_not_matched_by_source_action_debug() {
        let update = NotMatchedBySourceAction::Update(vec![(
            "col".to_string(),
            datafusion::prelude::lit(1),
        )]);
        let delete = NotMatchedBySourceAction::Delete;

        assert!(format!("{:?}", update).contains("Update"));
        assert!(format!("{:?}", delete).contains("Delete"));
    }
}
