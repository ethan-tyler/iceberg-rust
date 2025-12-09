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

//! Execution plan for MERGE operations on Iceberg tables.
//!
//! This module provides `IcebergMergeExec`, which implements Copy-on-Write (CoW)
//! MERGE semantics by:
//! 1. Scanning target table with metadata columns (_file_path, _pos)
//! 2. Performing a FULL OUTER JOIN with source data
//! 3. Classifying rows as MATCHED, NOT_MATCHED (source only), or NOT_MATCHED_BY_SOURCE (target only)
//! 4. Applying WHEN clauses in order (first match wins)
//! 5. Writing new data files and position delete files
//!
//! The output is serialized JSON for both data files and delete files, which are
//! then committed atomically by `IcebergMergeCommitExec` using `RowDeltaAction`.
//!
//! # Row Classification
//!
//! After the FULL OUTER JOIN, rows are classified based on NULL patterns:
//!
//! | Target Key | Source Key | Classification |
//! |------------|------------|----------------|
//! | NOT NULL   | NOT NULL   | MATCHED        |
//! | NULL       | NOT NULL   | NOT_MATCHED    |
//! | NOT NULL   | NULL       | NOT_MATCHED_BY_SOURCE |
//!
//! The classification determines which WHEN clauses are applicable.
//!
//! # Current Limitations
//!
//! **Partitioned tables**: MERGE on partitioned tables is **not supported** and will
//! return an error. Partition-aware file writing (routing rows to correct partitions
//! and setting partition values on DataFiles) is required before this can be enabled.
//!
//! **Memory**: Both target table and source data are materialized in memory for the
//! FULL OUTER JOIN. This will not scale to large tables. For production use with
//! large datasets, a streaming join implementation would be needed.
//!
//! **Single-key join**: The ON condition parser (`extract_join_keys`) only handles
//! simple equality conditions like `col("a") = col("b")`. Composite keys with AND
//! conditions are not yet supported.
//!
//! **WHEN clause conditions**: Conditional clauses (e.g., `WHEN MATCHED AND x > 0`)
//! are parsed but conditions are ignored during evaluation. Only unconditional
//! clauses are applied. Full condition support requires PhysicalExpr compilation.
//!
//! **Concurrency**: Baseline snapshot validation is not yet implemented. Concurrent
//! modifications to the table during MERGE execution may cause data inconsistencies.

use std::any::Any;
use std::collections::HashMap;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{
    DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use datafusion::common::DFSchema;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::JoinType;
use datafusion::physical_expr::planner::create_physical_expr;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::prelude::{Expr, SessionContext};
use futures::{StreamExt, TryStreamExt};
use iceberg::scan::FileScanTask;
use iceberg::spec::{DataFileFormat, TableProperties, serialize_data_file_to_json};
use iceberg::table::Table;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::base_writer::position_delete_writer::{
    PositionDeleteFileWriterBuilder, PositionDeleteWriterConfig,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use crate::merge::{
    MatchedAction, MergeStats, NotMatchedAction, NotMatchedBySourceAction, WhenMatchedClause,
    WhenNotMatchedBySourceClause, WhenNotMatchedClause,
};
use crate::to_datafusion_error;

/// Internal metadata column name for file path (added during scan)
pub const MERGE_FILE_PATH_COL: &str = "__iceberg_merge_file_path";
/// Internal metadata column name for row position (added during scan)
pub const MERGE_ROW_POS_COL: &str = "__iceberg_merge_row_pos";
/// Internal column name for merge action classification
#[allow(dead_code)] // Will be used in condition evaluation
pub const MERGE_ACTION_COL: &str = "__iceberg_merge_action";

/// Prefix for target columns in joined schema
pub const TARGET_PREFIX: &str = "target_";
/// Prefix for source columns in joined schema
pub const SOURCE_PREFIX: &str = "source_";

/// Column name for serialized data files in output
pub const MERGE_DATA_FILES_COL: &str = "data_files";
/// Column name for serialized delete files in output
pub const MERGE_DELETE_FILES_COL: &str = "delete_files";
/// Column names for stats in output
pub const MERGE_INSERTED_COUNT_COL: &str = "inserted_count";
pub const MERGE_UPDATED_COUNT_COL: &str = "updated_count";
pub const MERGE_DELETED_COUNT_COL: &str = "deleted_count";

/// Encoded action values for the MERGE_ACTION_COL column.
/// These are i8 values that encode the MergeAction enum.
pub mod action_codes {
    /// Insert a new row (from NOT MATCHED source row)
    pub const INSERT: i8 = 1;
    /// Update an existing row
    pub const UPDATE: i8 = 2;
    /// Delete an existing row
    pub const DELETE: i8 = 3;
    /// No action - keep the row as-is
    pub const NO_ACTION: i8 = 0;
}

/// Classification of a row after FULL OUTER JOIN.
///
/// This determines which WHEN clauses are applicable to a row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowClassification {
    /// Row exists in both source and target (key columns match).
    /// Contains the file path and position for position delete tracking.
    Matched,
    /// Row exists only in source (target key is NULL after join).
    /// Candidate for INSERT actions.
    NotMatched,
    /// Row exists only in target (source key is NULL after join).
    /// Candidate for DELETE or UPDATE BY SOURCE actions.
    NotMatchedBySource,
}

#[allow(dead_code)] // Will be used when IcebergMergeExec::execute() is implemented
impl RowClassification {
    /// Classifies a row based on NULL patterns in the join result.
    ///
    /// # Arguments
    /// * `target_key_is_null` - Whether the target side of the join key is NULL
    /// * `source_key_is_null` - Whether the source side of the join key is NULL
    ///
    /// # Returns
    /// The classification for this row, or None if both keys are NULL (shouldn't happen
    /// in a proper FULL OUTER JOIN on non-null keys).
    pub fn from_join_nulls(target_key_is_null: bool, source_key_is_null: bool) -> Option<Self> {
        match (target_key_is_null, source_key_is_null) {
            (false, false) => Some(RowClassification::Matched),
            (true, false) => Some(RowClassification::NotMatched),
            (false, true) => Some(RowClassification::NotMatchedBySource),
            (true, true) => None, // Both NULL - shouldn't happen with non-null join keys
        }
    }

    /// Returns a human-readable name for this classification.
    pub fn as_str(&self) -> &'static str {
        match self {
            RowClassification::Matched => "MATCHED",
            RowClassification::NotMatched => "NOT_MATCHED",
            RowClassification::NotMatchedBySource => "NOT_MATCHED_BY_SOURCE",
        }
    }
}

/// Action to take for a specific row after WHEN clause evaluation.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Variants and fields are for future condition evaluation
pub enum MergeAction {
    /// Insert a new row (from NOT MATCHED source row).
    Insert,
    /// Update an existing row (from MATCHED or NOT_MATCHED_BY_SOURCE).
    /// The String keys are column names, Expr values are the expressions to evaluate.
    Update(Vec<(String, Expr)>),
    /// Update all columns from source (only valid for MATCHED rows).
    UpdateAll,
    /// Delete an existing row.
    Delete,
    /// No action - keep the row as-is (used when no WHEN clause matches).
    /// For MATCHED: row is unchanged (neither updated nor deleted).
    /// For NOT_MATCHED: row is not inserted.
    /// For NOT_MATCHED_BY_SOURCE: row is kept as-is.
    NoAction,
}

impl MergeAction {
    /// Encodes the action as an i8 for storage in the action column.
    #[allow(dead_code)] // Will be used for condition evaluation
    pub fn to_code(&self) -> i8 {
        match self {
            MergeAction::Insert => action_codes::INSERT,
            MergeAction::Update(_) | MergeAction::UpdateAll => action_codes::UPDATE,
            MergeAction::Delete => action_codes::DELETE,
            MergeAction::NoAction => action_codes::NO_ACTION,
        }
    }

    /// Decodes an i8 action code to a simplified MergeAction.
    /// Note: Update details are lost during encoding.
    #[allow(dead_code)] // Will be used in file writing stages
    pub fn from_code(code: i8) -> Option<Self> {
        match code {
            action_codes::INSERT => Some(MergeAction::Insert),
            action_codes::UPDATE => Some(MergeAction::UpdateAll), // Simplified
            action_codes::DELETE => Some(MergeAction::Delete),
            action_codes::NO_ACTION => Some(MergeAction::NoAction),
            _ => None,
        }
    }
}

/// Execution plan for MERGE operations using Copy-on-Write semantics.
///
/// This plan performs a complete MERGE operation:
/// 1. Scans target table with metadata columns for position tracking
/// 2. Joins with source data using FULL OUTER JOIN semantics
/// 3. Applies WHEN clauses in order to classify actions
/// 4. Writes data files (inserts/updates) and delete files (updates/deletes)
///
/// Output schema: (data_files: Utf8, delete_files: Utf8, inserted_count: UInt64,
///                 updated_count: UInt64, deleted_count: UInt64)
#[derive(Debug)]
pub struct IcebergMergeExec {
    /// The target table
    table: Table,
    /// Arrow schema of the target table
    schema: ArrowSchemaRef,
    /// Source execution plan (e.g., from DataFrame)
    source: Arc<dyn ExecutionPlan>,
    /// ON condition for matching source and target rows
    match_condition: Expr,
    /// WHEN MATCHED clauses (order matters - first match wins)
    when_matched: Vec<WhenMatchedClause>,
    /// WHEN NOT MATCHED clauses for source-only rows
    when_not_matched: Vec<WhenNotMatchedClause>,
    /// WHEN NOT MATCHED BY SOURCE clauses for target-only rows
    when_not_matched_by_source: Vec<WhenNotMatchedBySourceClause>,
    /// Output schema for this execution plan
    output_schema: ArrowSchemaRef,
    /// Plan properties for query optimization
    plan_properties: PlanProperties,
}

impl IcebergMergeExec {
    /// Creates a new `IcebergMergeExec`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        table: Table,
        schema: ArrowSchemaRef,
        source: Arc<dyn ExecutionPlan>,
        match_condition: Expr,
        when_matched: Vec<WhenMatchedClause>,
        when_not_matched: Vec<WhenNotMatchedClause>,
        when_not_matched_by_source: Vec<WhenNotMatchedBySourceClause>,
    ) -> Self {
        let output_schema = Self::make_output_schema();
        let plan_properties = Self::compute_properties(output_schema.clone());

        Self {
            table,
            schema,
            source,
            match_condition,
            when_matched,
            when_not_matched,
            when_not_matched_by_source,
            output_schema,
            plan_properties,
        }
    }

    /// Creates the output schema for merge results.
    fn make_output_schema() -> ArrowSchemaRef {
        Arc::new(ArrowSchema::new(vec![
            Field::new(MERGE_DATA_FILES_COL, DataType::Utf8, false),
            Field::new(MERGE_DELETE_FILES_COL, DataType::Utf8, false),
            Field::new(MERGE_INSERTED_COUNT_COL, DataType::UInt64, false),
            Field::new(MERGE_UPDATED_COUNT_COL, DataType::UInt64, false),
            Field::new(MERGE_DELETED_COUNT_COL, DataType::UInt64, false),
        ]))
    }

    /// Computes plan properties for query optimization.
    fn compute_properties(schema: ArrowSchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        )
    }

    /// Creates a result batch with merge output (data files, delete files, stats).
    pub fn make_result_batch(
        data_files_json: Vec<String>,
        delete_files_json: Vec<String>,
        stats: &MergeStats,
    ) -> DFResult<RecordBatch> {
        let data_files_str = data_files_json.join("\n");
        let delete_files_str = delete_files_json.join("\n");

        let data_files_array = Arc::new(StringArray::from(vec![data_files_str])) as ArrayRef;
        let delete_files_array = Arc::new(StringArray::from(vec![delete_files_str])) as ArrayRef;
        let inserted_array = Arc::new(datafusion::arrow::array::UInt64Array::from(vec![
            stats.rows_inserted,
        ])) as ArrayRef;
        let updated_array = Arc::new(datafusion::arrow::array::UInt64Array::from(vec![
            stats.rows_updated,
        ])) as ArrayRef;
        let deleted_array = Arc::new(datafusion::arrow::array::UInt64Array::from(vec![
            stats.rows_deleted,
        ])) as ArrayRef;

        RecordBatch::try_new(
            Self::make_output_schema(),
            vec![
                data_files_array,
                delete_files_array,
                inserted_array,
                updated_array,
                deleted_array,
            ],
        )
        .map_err(|e| {
            DataFusionError::ArrowError(
                Box::new(e),
                Some("Failed to make merge result batch".to_string()),
            )
        })
    }

    /// Returns a human-readable description of the WHEN clauses for display.
    fn format_when_clauses(&self) -> String {
        let mut parts = Vec::new();

        for clause in &self.when_matched {
            let condition = clause
                .condition
                .as_ref()
                .map(|c| format!(" AND {}", c))
                .unwrap_or_default();
            let action = match &clause.action {
                MatchedAction::Update(assignments) => {
                    let assigns: Vec<_> = assignments
                        .iter()
                        .map(|(k, v)| format!("{}={}", k, v))
                        .collect();
                    format!("UPDATE SET {}", assigns.join(", "))
                }
                MatchedAction::UpdateAll => "UPDATE *".to_string(),
                MatchedAction::Delete => "DELETE".to_string(),
            };
            parts.push(format!("WHEN MATCHED{} THEN {}", condition, action));
        }

        for clause in &self.when_not_matched {
            let condition = clause
                .condition
                .as_ref()
                .map(|c| format!(" AND {}", c))
                .unwrap_or_default();
            let action = match &clause.action {
                NotMatchedAction::Insert(values) => {
                    let vals: Vec<_> = values.iter().map(|(k, v)| format!("{}={}", k, v)).collect();
                    format!("INSERT ({})", vals.join(", "))
                }
                NotMatchedAction::InsertAll => "INSERT *".to_string(),
            };
            parts.push(format!("WHEN NOT MATCHED{} THEN {}", condition, action));
        }

        for clause in &self.when_not_matched_by_source {
            let condition = clause
                .condition
                .as_ref()
                .map(|c| format!(" AND {}", c))
                .unwrap_or_default();
            let action = match &clause.action {
                NotMatchedBySourceAction::Update(assignments) => {
                    let assigns: Vec<_> = assignments
                        .iter()
                        .map(|(k, v)| format!("{}={}", k, v))
                        .collect();
                    format!("UPDATE SET {}", assigns.join(", "))
                }
                NotMatchedBySourceAction::Delete => "DELETE".to_string(),
            };
            parts.push(format!(
                "WHEN NOT MATCHED BY SOURCE{} THEN {}",
                condition, action
            ));
        }

        parts.join(", ")
    }
}

impl DisplayAs for IcebergMergeExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "IcebergMergeExec: table={}, on=[{}], clauses=[{}]",
                    self.table.identifier(),
                    self.match_condition,
                    self.format_when_clauses()
                )
            }
            DisplayFormatType::Verbose => {
                write!(
                    f,
                    "IcebergMergeExec: table={}, on=[{}], clauses=[{}], schema={:?}",
                    self.table.identifier(),
                    self.match_condition,
                    self.format_when_clauses(),
                    self.schema
                )
            }
        }
    }
}

impl ExecutionPlan for IcebergMergeExec {
    fn name(&self) -> &str {
        "IcebergMergeExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.source]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(
                "IcebergMergeExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(IcebergMergeExec::new(
            self.table.clone(),
            self.schema.clone(),
            children[0].clone(),
            self.match_condition.clone(),
            self.when_matched.clone(),
            self.when_not_matched.clone(),
            self.when_not_matched_by_source.clone(),
        )))
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        // Clone data needed for the async block
        let table = self.table.clone();
        let schema = self.schema.clone();
        let source = self.source.clone();
        let match_condition = self.match_condition.clone();
        let when_matched = self.when_matched.clone();
        let when_not_matched = self.when_not_matched.clone();
        let when_not_matched_by_source = self.when_not_matched_by_source.clone();
        let output_schema = self.output_schema.clone();

        let stream = futures::stream::once(async move {
            execute_merge(
                table,
                schema,
                source,
                partition,
                context,
                match_condition,
                when_matched,
                when_not_matched,
                when_not_matched_by_source,
            )
            .await
        })
        .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(output_schema, stream)))
    }
}

// ============================================================================
// Core MERGE Execution Logic
// ============================================================================

/// Executes the MERGE operation: scan target, join with source, classify rows, write files.
///
/// This function performs the following steps:
/// 1. Scans target table with metadata columns (_file_path, _row_pos)
/// 2. Materializes source data from the execution plan
/// 3. Performs a FULL OUTER JOIN on the match condition
/// 4. Classifies each row based on NULL patterns after the join
/// 5. Applies WHEN clauses to determine actions (INSERT, UPDATE, DELETE, NO_ACTION)
/// 6. Writes data files (for UPDATE) and position delete files (for UPDATE/DELETE)
///
/// # IMPORTANT Implementation Notes
///
/// A) Concurrency safety:
///    - Capture baseline_snapshot_id at plan construction time
///    - Validate baseline before commit (reject if table modified since scan)
///    - Use row_delta() for atomic commit of data + delete files in single snapshot
///
/// B) Partition column constraint:
///    - MergeBuilder::validate() already checks for partition column updates
///    - Runtime validation ensures output partition values match input
///
/// C) Metadata column correctness:
///    - add_metadata_columns() assumes per-file batches with sequential positions
///    - Track row_offset per file during scan, reset for each new file
#[allow(clippy::too_many_arguments)]
async fn execute_merge(
    table: Table,
    schema: ArrowSchemaRef,
    source: Arc<dyn ExecutionPlan>,
    partition: usize,
    context: Arc<TaskContext>,
    match_condition: Expr,
    when_matched: Vec<WhenMatchedClause>,
    when_not_matched: Vec<WhenNotMatchedClause>,
    when_not_matched_by_source: Vec<WhenNotMatchedBySourceClause>,
) -> DFResult<RecordBatch> {
    // === Validation: Reject partitioned tables (not yet supported) ===
    let partition_spec = table.metadata().default_partition_spec();
    if !partition_spec.is_unpartitioned() {
        return Err(DataFusionError::NotImplemented(
            "MERGE on partitioned tables is not yet supported. \
             Partition-aware file writing will be added in a future commit."
                .to_string(),
        ));
    }

    // === Step 1: Scan target table with metadata columns ===
    let target_batches = scan_target_with_metadata(&table, &schema).await?;

    if target_batches.is_empty() && when_not_matched.is_empty() {
        // No target rows and no INSERT clause - nothing to do
        return IcebergMergeExec::make_result_batch(vec![], vec![], &MergeStats::default());
    }

    // === Step 2: Materialize source data ===
    let source_batches = materialize_source(source, partition, context).await?;

    if source_batches.is_empty() && when_not_matched_by_source.is_empty() {
        // No source rows and no DELETE BY SOURCE clause - nothing to do
        return IcebergMergeExec::make_result_batch(vec![], vec![], &MergeStats::default());
    }

    // === Step 3: Extract join keys from match condition ===
    let (target_key, source_key) = extract_join_keys(&match_condition)?;

    // === Step 4: Perform FULL OUTER JOIN ===
    let joined_batches = perform_full_outer_join(
        &target_batches,
        &source_batches,
        &schema,
        &target_key,
        &source_key,
    )
    .await?;

    // === Step 5: Set up writers for file output ===
    let (mut data_file_writer, mut delete_file_writer, delete_schema) =
        setup_writers(&table, &schema).await?;

    // === Step 6: Process joined rows and write files ===
    let mut stats = MergeStats::default();

    let prefixed_target_key = format!("{}{}", TARGET_PREFIX, target_key);
    let prefixed_source_key = format!("{}{}", SOURCE_PREFIX, source_key);

    // Metadata column names after prefix
    let file_path_col_name = format!("{}{}", TARGET_PREFIX, MERGE_FILE_PATH_COL);
    let row_pos_col_name = format!("{}{}", TARGET_PREFIX, MERGE_ROW_POS_COL);

    for batch in &joined_batches {
        // Process MATCHED rows (both keys non-null)
        process_matched_rows(
            batch,
            &prefixed_target_key,
            &prefixed_source_key,
            &file_path_col_name,
            &row_pos_col_name,
            &schema,
            &when_matched,
            &mut data_file_writer,
            &mut delete_file_writer,
            &delete_schema,
            &mut stats,
        )
        .await?;

        // Process NOT_MATCHED rows (target key null, source key non-null)
        process_not_matched_rows(
            batch,
            &prefixed_target_key,
            &prefixed_source_key,
            &schema,
            &when_not_matched,
            &mut data_file_writer,
            &mut stats,
        )
        .await?;

        // Process NOT_MATCHED_BY_SOURCE rows (target key non-null, source key null)
        process_not_matched_by_source_rows(
            batch,
            &prefixed_target_key,
            &prefixed_source_key,
            &file_path_col_name,
            &row_pos_col_name,
            &schema,
            &when_not_matched_by_source,
            &mut data_file_writer,
            &mut delete_file_writer,
            &delete_schema,
            &mut stats,
        )
        .await?;
    }

    // === Step 7: Close writers and collect files ===
    let partition_type = table.metadata().default_partition_type().clone();
    let format_version = table.metadata().format_version();

    let data_files = data_file_writer
        .close()
        .await
        .map_err(to_datafusion_error)?;
    let delete_files = delete_file_writer
        .close()
        .await
        .map_err(to_datafusion_error)?;

    // Serialize to JSON
    let data_files_json: Vec<String> = data_files
        .into_iter()
        .map(|df| serialize_data_file_to_json(df, &partition_type, format_version))
        .collect::<Result<Vec<_>, _>>()
        .map_err(to_datafusion_error)?;

    let delete_files_json: Vec<String> = delete_files
        .into_iter()
        .map(|df| serialize_data_file_to_json(df, &partition_type, format_version))
        .collect::<Result<Vec<_>, _>>()
        .map_err(to_datafusion_error)?;

    // Update file counts in stats
    stats.files_added = data_files_json.len() as u64;
    stats.files_removed = delete_files_json.len() as u64;

    IcebergMergeExec::make_result_batch(data_files_json, delete_files_json, &stats)
}

/// Scans the target table and adds metadata columns (_file_path, _row_pos).
///
/// Uses the FileScanTask iteration pattern from UPDATE implementation.
async fn scan_target_with_metadata(
    table: &Table,
    _schema: &ArrowSchemaRef,
) -> DFResult<Vec<RecordBatch>> {
    // Build the scan - select all columns
    let scan_builder = table.scan().select_all();
    let table_scan = scan_builder.build().map_err(to_datafusion_error)?;

    // Get file scan tasks
    let file_scan_tasks: Vec<FileScanTask> = table_scan
        .plan_files()
        .await
        .map_err(to_datafusion_error)?
        .try_collect()
        .await
        .map_err(to_datafusion_error)?;

    if file_scan_tasks.is_empty() {
        return Ok(vec![]);
    }

    let file_io = table.file_io().clone();
    let mut all_batches = Vec::new();

    // Process each file and add metadata columns
    for task in file_scan_tasks {
        let file_path = task.data_file_path().to_string();

        // Create a fresh reader for each file (ArrowReader doesn't implement Clone)
        let reader = iceberg::arrow::ArrowReaderBuilder::new(file_io.clone()).build();

        // Read batches from this file
        let task_stream = Box::pin(futures::stream::iter(vec![Ok(task)]))
            as iceberg::scan::FileScanTaskStream;
        let mut batch_stream = reader
            .read(task_stream)
            .map_err(to_datafusion_error)?
            .map(|r| r.map_err(to_datafusion_error));

        let mut row_offset = 0i64;
        while let Some(batch_result) = batch_stream.next().await {
            let batch = batch_result?;
            let batch_size = batch.num_rows();

            // Add metadata columns
            let batch_with_meta = add_metadata_columns(batch, &file_path, row_offset)?;
            all_batches.push(batch_with_meta);

            row_offset += batch_size as i64;
        }
    }

    Ok(all_batches)
}

/// Materializes source data from the execution plan.
async fn materialize_source(
    source: Arc<dyn ExecutionPlan>,
    partition: usize,
    context: Arc<TaskContext>,
) -> DFResult<Vec<RecordBatch>> {
    let stream = source.execute(partition, context)?;
    let batches: Vec<RecordBatch> = stream.try_collect().await?;
    Ok(batches)
}

/// Sets up data file and position delete file writers for MERGE output.
///
/// Returns (data_file_writer, delete_file_writer, delete_schema).
async fn setup_writers(
    table: &Table,
    _schema: &ArrowSchemaRef,
) -> DFResult<(
    Box<dyn IcebergWriter + Send>,
    Box<dyn IcebergWriter + Send>,
    ArrowSchemaRef,
)> {
    let file_io = table.file_io().clone();

    let target_file_size = table
        .metadata()
        .properties()
        .get(TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES)
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

    // Create location generator
    let location_generator =
        DefaultLocationGenerator::new(table.metadata().clone()).map_err(to_datafusion_error)?;

    // === Data file writer ===
    let data_file_name_generator = DefaultFileNameGenerator::new(
        format!("merge-data-{}", Uuid::now_v7()),
        None,
        DataFileFormat::Parquet,
    );
    let parquet_writer = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
    );
    let data_rolling_writer = RollingFileWriterBuilder::new(
        parquet_writer.clone(),
        target_file_size,
        file_io.clone(),
        location_generator.clone(),
        data_file_name_generator,
    );
    let data_file_builder = DataFileWriterBuilder::new(data_rolling_writer);
    let data_file_writer = data_file_builder
        .build(None)
        .await
        .map_err(to_datafusion_error)?;

    // === Position delete file writer ===
    let delete_config = PositionDeleteWriterConfig::new();
    let delete_schema = delete_config.delete_schema().clone();

    // Build Iceberg schema for position deletes
    let iceberg_delete_schema = Arc::new(
        iceberg::spec::Schema::builder()
            .with_schema_id(table.metadata().current_schema_id())
            .with_fields(vec![
                iceberg::spec::NestedField::required(
                    iceberg::writer::base_writer::position_delete_writer::POSITION_DELETE_FILE_PATH_FIELD_ID,
                    "file_path",
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String),
                )
                .into(),
                iceberg::spec::NestedField::required(
                    iceberg::writer::base_writer::position_delete_writer::POSITION_DELETE_POS_FIELD_ID,
                    "pos",
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
                )
                .into(),
            ])
            .build()
            .map_err(to_datafusion_error)?,
    );

    let delete_parquet_writer =
        ParquetWriterBuilder::new(WriterProperties::default(), iceberg_delete_schema);
    let delete_file_name_generator = DefaultFileNameGenerator::new(
        format!("merge-delete-{}", Uuid::now_v7()),
        None,
        DataFileFormat::Parquet,
    );
    let delete_rolling_writer = RollingFileWriterBuilder::new(
        delete_parquet_writer,
        target_file_size,
        file_io,
        location_generator,
        delete_file_name_generator,
    );
    let delete_file_builder =
        PositionDeleteFileWriterBuilder::new(delete_rolling_writer, delete_config);
    let delete_file_writer = delete_file_builder
        .build(None)
        .await
        .map_err(to_datafusion_error)?;

    Ok((
        Box::new(data_file_writer),
        Box::new(delete_file_writer),
        delete_schema,
    ))
}

/// Processes MATCHED rows from joined batch and writes files.
///
/// For each MATCHED row (where both target and source keys are non-null):
/// - If action is UPDATE: Write transformed data to data file + position delete
/// - If action is DELETE: Write position delete only
///
/// # Arguments
/// * `batch` - The joined batch containing both target and source columns
/// * `target_key` - Prefixed target key column name
/// * `source_key` - Prefixed source key column name
/// * `file_path_col` - Prefixed file path metadata column name
/// * `row_pos_col` - Prefixed row position metadata column name
/// * `target_schema` - Original target table schema (for output)
/// * `when_matched` - WHEN MATCHED clauses
/// * `data_writer` - Data file writer for updated rows
/// * `delete_writer` - Position delete file writer
/// * `delete_schema` - Schema for position delete batches
/// * `stats` - Statistics to update
#[allow(clippy::too_many_arguments)]
async fn process_matched_rows(
    batch: &RecordBatch,
    target_key: &str,
    source_key: &str,
    file_path_col: &str,
    row_pos_col: &str,
    target_schema: &ArrowSchemaRef,
    when_matched: &[WhenMatchedClause],
    data_writer: &mut Box<dyn IcebergWriter + Send>,
    delete_writer: &mut Box<dyn IcebergWriter + Send>,
    delete_schema: &ArrowSchemaRef,
    stats: &mut MergeStats,
) -> DFResult<()> {
    if when_matched.is_empty() {
        return Ok(());
    }

    let batch_schema = batch.schema();

    // Find key columns
    let target_key_idx = batch_schema
        .index_of(target_key)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
    let source_key_idx = batch_schema
        .index_of(source_key)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    // Find metadata columns
    let file_path_idx = batch_schema
        .index_of(file_path_col)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
    let row_pos_idx = batch_schema
        .index_of(row_pos_col)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    let target_key_col = batch.column(target_key_idx);
    let source_key_col = batch.column(source_key_idx);
    let file_path_col_arr = batch
        .column(file_path_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Internal("file_path column must be StringArray".into()))?;
    let row_pos_col_arr = batch
        .column(row_pos_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Internal("row_pos column must be Int64Array".into()))?;

    // Collect all MATCHED row indices (both keys non-null)
    let mut matched_indices: Vec<usize> = Vec::new();
    for row_idx in 0..batch.num_rows() {
        let target_is_null = target_key_col.is_null(row_idx);
        let source_is_null = source_key_col.is_null(row_idx);

        if !target_is_null && !source_is_null {
            matched_indices.push(row_idx);
        }
    }

    if matched_indices.is_empty() {
        return Ok(());
    }

    // Create DFSchema for condition evaluation
    let df_schema = DFSchema::try_from(batch_schema.as_ref().clone())?;

    // Track which rows have been processed (first matching clause wins)
    let mut remaining_indices: Vec<usize> = matched_indices;

    // Process WHEN MATCHED clauses in order (first match wins)
    for clause in when_matched {
        if remaining_indices.is_empty() {
            break;
        }

        // Evaluate condition to find which rows this clause applies to
        let clause_indices =
            evaluate_condition_for_rows(&clause.condition, batch, &remaining_indices, &df_schema)?;

        if clause_indices.is_empty() {
            continue;
        }

        // Remove processed indices from remaining
        let clause_set: std::collections::HashSet<usize> = clause_indices.iter().copied().collect();
        remaining_indices.retain(|idx| !clause_set.contains(idx));

        // Process based on action type
        match &clause.action {
            MatchedAction::Delete => {
                // For DELETE: only write position deletes
                let file_paths: Vec<String> = clause_indices
                    .iter()
                    .map(|&idx| file_path_col_arr.value(idx).to_string())
                    .collect();
                let positions: Vec<i64> = clause_indices
                    .iter()
                    .map(|&idx| row_pos_col_arr.value(idx))
                    .collect();

                let delete_batch =
                    make_position_delete_batch(&file_paths, &positions, delete_schema.clone())?;
                delete_writer
                    .write(delete_batch)
                    .await
                    .map_err(to_datafusion_error)?;

                stats.rows_deleted += clause_indices.len() as u64;
            }
            MatchedAction::UpdateAll => {
                // For UPDATE *: Use source columns to create updated rows
                let data_batch =
                    extract_source_data_for_update(batch, &clause_indices, target_schema)?;

                if data_batch.num_rows() > 0 {
                    data_writer
                        .write(data_batch)
                        .await
                        .map_err(to_datafusion_error)?;
                }

                // Write position deletes for original rows
                let file_paths: Vec<String> = clause_indices
                    .iter()
                    .map(|&idx| file_path_col_arr.value(idx).to_string())
                    .collect();
                let positions: Vec<i64> = clause_indices
                    .iter()
                    .map(|&idx| row_pos_col_arr.value(idx))
                    .collect();

                let delete_batch =
                    make_position_delete_batch(&file_paths, &positions, delete_schema.clone())?;
                delete_writer
                    .write(delete_batch)
                    .await
                    .map_err(to_datafusion_error)?;

                stats.rows_updated += clause_indices.len() as u64;
            }
            MatchedAction::Update(assignments) => {
                // For UPDATE with assignments: Apply SET expressions
                let data_batch =
                    apply_update_assignments(batch, &clause_indices, assignments, target_schema)?;

                if data_batch.num_rows() > 0 {
                    data_writer
                        .write(data_batch)
                        .await
                        .map_err(to_datafusion_error)?;
                }

                // Write position deletes for original rows
                let file_paths: Vec<String> = clause_indices
                    .iter()
                    .map(|&idx| file_path_col_arr.value(idx).to_string())
                    .collect();
                let positions: Vec<i64> = clause_indices
                    .iter()
                    .map(|&idx| row_pos_col_arr.value(idx))
                    .collect();

                let delete_batch =
                    make_position_delete_batch(&file_paths, &positions, delete_schema.clone())?;
                delete_writer
                    .write(delete_batch)
                    .await
                    .map_err(to_datafusion_error)?;

                stats.rows_updated += clause_indices.len() as u64;
            }
        }
    }

    Ok(())
}

/// Processes NOT_MATCHED rows from joined batch and writes INSERT files.
///
/// For each NOT_MATCHED row (where target key is null and source key is non-null):
/// - If action is INSERT *: Write source data to data file
/// - If action is INSERT with values: Evaluate expressions and write to data file
///
/// # Arguments
/// * `batch` - The joined batch containing both target and source columns
/// * `target_key` - Prefixed target key column name
/// * `source_key` - Prefixed source key column name
/// * `target_schema` - Original target table schema (for output)
/// * `when_not_matched` - WHEN NOT MATCHED clauses
/// * `data_writer` - Data file writer for inserted rows
/// * `stats` - Statistics to update
#[allow(clippy::too_many_arguments)]
async fn process_not_matched_rows(
    batch: &RecordBatch,
    target_key: &str,
    source_key: &str,
    target_schema: &ArrowSchemaRef,
    when_not_matched: &[WhenNotMatchedClause],
    data_writer: &mut Box<dyn IcebergWriter + Send>,
    stats: &mut MergeStats,
) -> DFResult<()> {
    if when_not_matched.is_empty() {
        return Ok(());
    }

    let batch_schema = batch.schema();

    // Find key columns
    let target_key_idx = batch_schema
        .index_of(target_key)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
    let source_key_idx = batch_schema
        .index_of(source_key)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    let target_key_col = batch.column(target_key_idx);
    let source_key_col = batch.column(source_key_idx);

    // Collect all NOT_MATCHED row indices (target key null, source key non-null)
    let mut not_matched_indices: Vec<usize> = Vec::new();
    for row_idx in 0..batch.num_rows() {
        let target_is_null = target_key_col.is_null(row_idx);
        let source_is_null = source_key_col.is_null(row_idx);

        if target_is_null && !source_is_null {
            not_matched_indices.push(row_idx);
        }
    }

    if not_matched_indices.is_empty() {
        return Ok(());
    }

    // Create DFSchema for condition evaluation
    let df_schema = DFSchema::try_from(batch_schema.as_ref().clone())?;

    // Track which rows have been processed (first matching clause wins)
    let mut remaining_indices: Vec<usize> = not_matched_indices;

    // Process WHEN NOT MATCHED clauses in order (first match wins)
    for clause in when_not_matched {
        if remaining_indices.is_empty() {
            break;
        }

        // Evaluate condition to find which rows this clause applies to
        let clause_indices =
            evaluate_condition_for_rows(&clause.condition, batch, &remaining_indices, &df_schema)?;

        if clause_indices.is_empty() {
            continue;
        }

        // Remove processed indices from remaining
        let clause_set: std::collections::HashSet<usize> = clause_indices.iter().copied().collect();
        remaining_indices.retain(|idx| !clause_set.contains(idx));

        // Process based on action type
        match &clause.action {
            NotMatchedAction::InsertAll => {
                // For INSERT *: Use source columns to create new rows
                let data_batch =
                    extract_source_data_for_insert(batch, &clause_indices, target_schema)?;

                if data_batch.num_rows() > 0 {
                    data_writer
                        .write(data_batch)
                        .await
                        .map_err(to_datafusion_error)?;
                }

                stats.rows_inserted += clause_indices.len() as u64;
            }
            NotMatchedAction::Insert(values) => {
                // For INSERT with explicit values: Evaluate expressions
                let data_batch =
                    apply_insert_values(batch, &clause_indices, values, target_schema)?;

                if data_batch.num_rows() > 0 {
                    data_writer
                        .write(data_batch)
                        .await
                        .map_err(to_datafusion_error)?;
                }

                stats.rows_inserted += clause_indices.len() as u64;
            }
        }
    }

    Ok(())
}

/// Processes NOT_MATCHED_BY_SOURCE rows from joined batch and writes files.
///
/// For each NOT_MATCHED_BY_SOURCE row (where target key is non-null and source key is null):
/// - If action is DELETE: Write position delete only
/// - If action is UPDATE: Write updated data + position delete
///
/// # Arguments
/// * `batch` - The joined batch containing both target and source columns
/// * `target_key` - Prefixed target key column name
/// * `source_key` - Prefixed source key column name
/// * `file_path_col` - Prefixed file path metadata column name
/// * `row_pos_col` - Prefixed row position metadata column name
/// * `target_schema` - Original target table schema (for output)
/// * `when_not_matched_by_source` - WHEN NOT MATCHED BY SOURCE clauses
/// * `data_writer` - Data file writer for updated rows
/// * `delete_writer` - Position delete file writer
/// * `delete_schema` - Schema for position delete batches
/// * `stats` - Statistics to update
#[allow(clippy::too_many_arguments)]
async fn process_not_matched_by_source_rows(
    batch: &RecordBatch,
    target_key: &str,
    source_key: &str,
    file_path_col: &str,
    row_pos_col: &str,
    target_schema: &ArrowSchemaRef,
    when_not_matched_by_source: &[WhenNotMatchedBySourceClause],
    data_writer: &mut Box<dyn IcebergWriter + Send>,
    delete_writer: &mut Box<dyn IcebergWriter + Send>,
    delete_schema: &ArrowSchemaRef,
    stats: &mut MergeStats,
) -> DFResult<()> {
    if when_not_matched_by_source.is_empty() {
        return Ok(());
    }

    let batch_schema = batch.schema();

    // Find key columns
    let target_key_idx = batch_schema
        .index_of(target_key)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
    let source_key_idx = batch_schema
        .index_of(source_key)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    // Find metadata columns
    let file_path_idx = batch_schema
        .index_of(file_path_col)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
    let row_pos_idx = batch_schema
        .index_of(row_pos_col)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    let target_key_col = batch.column(target_key_idx);
    let source_key_col = batch.column(source_key_idx);
    let file_path_col_arr = batch
        .column(file_path_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Internal("file_path column must be StringArray".into()))?;
    let row_pos_col_arr = batch
        .column(row_pos_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Internal("row_pos column must be Int64Array".into()))?;

    // Collect all NOT_MATCHED_BY_SOURCE row indices (target key non-null, source key null)
    let mut not_matched_by_source_indices: Vec<usize> = Vec::new();
    for row_idx in 0..batch.num_rows() {
        let target_is_null = target_key_col.is_null(row_idx);
        let source_is_null = source_key_col.is_null(row_idx);

        if !target_is_null && source_is_null {
            not_matched_by_source_indices.push(row_idx);
        }
    }

    if not_matched_by_source_indices.is_empty() {
        return Ok(());
    }

    // Create DFSchema for condition evaluation
    let df_schema = DFSchema::try_from(batch_schema.as_ref().clone())?;

    // Track which rows have been processed (first matching clause wins)
    let mut remaining_indices: Vec<usize> = not_matched_by_source_indices;

    // Process WHEN NOT MATCHED BY SOURCE clauses in order (first match wins)
    for clause in when_not_matched_by_source {
        if remaining_indices.is_empty() {
            break;
        }

        // Evaluate condition to find which rows this clause applies to
        let clause_indices =
            evaluate_condition_for_rows(&clause.condition, batch, &remaining_indices, &df_schema)?;

        if clause_indices.is_empty() {
            continue;
        }

        // Remove processed indices from remaining
        let clause_set: std::collections::HashSet<usize> = clause_indices.iter().copied().collect();
        remaining_indices.retain(|idx| !clause_set.contains(idx));

        // Process based on action type
        match &clause.action {
            NotMatchedBySourceAction::Delete => {
                // For DELETE: only write position deletes
                let file_paths: Vec<String> = clause_indices
                    .iter()
                    .map(|&idx| file_path_col_arr.value(idx).to_string())
                    .collect();
                let positions: Vec<i64> = clause_indices
                    .iter()
                    .map(|&idx| row_pos_col_arr.value(idx))
                    .collect();

                let delete_batch =
                    make_position_delete_batch(&file_paths, &positions, delete_schema.clone())?;
                delete_writer
                    .write(delete_batch)
                    .await
                    .map_err(to_datafusion_error)?;

                stats.rows_deleted += clause_indices.len() as u64;
            }
            NotMatchedBySourceAction::Update(assignments) => {
                // For UPDATE with assignments: Apply SET expressions using target columns
                let data_batch = apply_not_matched_by_source_update(
                    batch,
                    &clause_indices,
                    assignments,
                    target_schema,
                )?;

                if data_batch.num_rows() > 0 {
                    data_writer
                        .write(data_batch)
                        .await
                        .map_err(to_datafusion_error)?;
                }

                // Write position deletes for original rows
                let file_paths: Vec<String> = clause_indices
                    .iter()
                    .map(|&idx| file_path_col_arr.value(idx).to_string())
                    .collect();
                let positions: Vec<i64> = clause_indices
                    .iter()
                    .map(|&idx| row_pos_col_arr.value(idx))
                    .collect();

                let delete_batch =
                    make_position_delete_batch(&file_paths, &positions, delete_schema.clone())?;
                delete_writer
                    .write(delete_batch)
                    .await
                    .map_err(to_datafusion_error)?;

                stats.rows_updated += clause_indices.len() as u64;
            }
        }
    }

    Ok(())
}

/// Applies UPDATE assignments for NOT_MATCHED_BY_SOURCE rows.
///
/// These rows only have target column values (source is NULL), so assignments
/// can only reference target columns.
fn apply_not_matched_by_source_update(
    joined_batch: &RecordBatch,
    indices: &[usize],
    assignments: &[(String, Expr)],
    target_schema: &ArrowSchemaRef,
) -> DFResult<RecordBatch> {
    use datafusion::arrow::compute::take;

    // Build indices array for filtering
    let indices_arr = datafusion::arrow::array::UInt32Array::from(
        indices.iter().map(|&i| i as u32).collect::<Vec<_>>(),
    );

    // Compile expressions for assignments
    let df_schema = DFSchema::try_from(joined_batch.schema().as_ref().clone())?;
    let ctx = SessionContext::new();
    let execution_props = ctx.state().execution_props().clone();

    let mut compiled_assignments: HashMap<String, Arc<dyn PhysicalExpr>> = HashMap::new();
    for (col, expr) in assignments {
        let phys_expr = create_physical_expr(expr, &df_schema, &execution_props)?;
        compiled_assignments.insert(col.clone(), phys_expr);
    }

    // Filter the batch to only target rows for expression evaluation
    let filtered_columns: Vec<ArrayRef> = joined_batch
        .columns()
        .iter()
        .map(|col| take(col, &indices_arr, None))
        .collect::<Result<Vec<_>, _>>()?;
    let filtered_batch = RecordBatch::try_new(joined_batch.schema(), filtered_columns)?;

    // Evaluate compiled expressions
    let mut evaluated_columns: HashMap<String, ArrayRef> = HashMap::new();
    for (col, phys_expr) in &compiled_assignments {
        let result = phys_expr.evaluate(&filtered_batch)?;
        evaluated_columns.insert(col.clone(), result.into_array(filtered_batch.num_rows())?);
    }

    // Build output batch using target schema
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(target_schema.fields().len());

    for field in target_schema.fields() {
        let column_name = field.name();

        if let Some(evaluated_col) = evaluated_columns.get(column_name) {
            // Use evaluated SET expression
            columns.push(evaluated_col.clone());
        } else {
            // Use original target column value
            let target_col_name = format!("{}{}", TARGET_PREFIX, column_name);
            let idx = joined_batch
                .schema()
                .index_of(&target_col_name)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            columns.push(take(joined_batch.column(idx), &indices_arr, None)?);
        }
    }

    RecordBatch::try_new(target_schema.clone(), columns).map_err(|e| {
        DataFusionError::ArrowError(
            Box::new(e),
            Some("Failed to create NOT_MATCHED_BY_SOURCE UPDATE data batch".to_string()),
        )
    })
}

/// Extracts source data for INSERT * action (NOT_MATCHED rows).
///
/// Creates a data batch with source columns mapped to target schema.
fn extract_source_data_for_insert(
    joined_batch: &RecordBatch,
    not_matched_indices: &[usize],
    target_schema: &ArrowSchemaRef,
) -> DFResult<RecordBatch> {
    use datafusion::arrow::compute::take;

    // Build indices array for filtering
    let indices = datafusion::arrow::array::UInt32Array::from(
        not_matched_indices
            .iter()
            .map(|&i| i as u32)
            .collect::<Vec<_>>(),
    );

    // For each target column, find corresponding source column
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(target_schema.fields().len());

    for field in target_schema.fields() {
        let source_col_name = format!("{}{}", SOURCE_PREFIX, field.name());

        let column = if let Ok(idx) = joined_batch.schema().index_of(&source_col_name) {
            // Take values at not_matched indices
            take(joined_batch.column(idx), &indices, None)?
        } else {
            // If source doesn't have this column, create nulls
            // This handles cases where source schema differs from target
            return Err(DataFusionError::Plan(format!(
                "Source column '{}' not found for INSERT. \
                 Use INSERT with explicit values to specify columns.",
                field.name()
            )));
        };

        columns.push(column);
    }

    RecordBatch::try_new(target_schema.clone(), columns).map_err(|e| {
        DataFusionError::ArrowError(
            Box::new(e),
            Some("Failed to create INSERT data batch".to_string()),
        )
    })
}

/// Applies INSERT values to create new rows.
///
/// Evaluates each value expression against the joined batch and creates
/// a new batch with the specified values.
fn apply_insert_values(
    joined_batch: &RecordBatch,
    not_matched_indices: &[usize],
    values: &[(String, Expr)],
    target_schema: &ArrowSchemaRef,
) -> DFResult<RecordBatch> {
    use datafusion::arrow::compute::take;

    // Build indices array for filtering
    let indices = datafusion::arrow::array::UInt32Array::from(
        not_matched_indices
            .iter()
            .map(|&i| i as u32)
            .collect::<Vec<_>>(),
    );

    // Compile expressions for INSERT values
    let df_schema = DFSchema::try_from(joined_batch.schema().as_ref().clone())?;
    let ctx = SessionContext::new();
    let execution_props = ctx.state().execution_props().clone();

    let mut compiled_values: HashMap<String, Arc<dyn PhysicalExpr>> = HashMap::new();
    for (col, expr) in values {
        let phys_expr = create_physical_expr(expr, &df_schema, &execution_props)?;
        compiled_values.insert(col.clone(), phys_expr);
    }

    // Filter batch to only not_matched rows for expression evaluation
    let filtered_columns: Vec<ArrayRef> = joined_batch
        .columns()
        .iter()
        .map(|col| take(col, &indices, None))
        .collect::<Result<Vec<_>, _>>()?;
    let filtered_batch = RecordBatch::try_new(joined_batch.schema(), filtered_columns)?;

    // Evaluate compiled expressions
    let mut evaluated_columns: HashMap<String, ArrayRef> = HashMap::new();
    for (col, phys_expr) in &compiled_values {
        let result = phys_expr.evaluate(&filtered_batch)?;
        evaluated_columns.insert(col.clone(), result.into_array(filtered_batch.num_rows())?);
    }

    // Build output batch using target schema
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(target_schema.fields().len());

    for field in target_schema.fields() {
        let column_name = field.name();

        if let Some(evaluated_col) = evaluated_columns.get(column_name) {
            // Use evaluated INSERT value
            columns.push(evaluated_col.clone());
        } else {
            // Try source column as fallback
            let source_col_name = format!("{}{}", SOURCE_PREFIX, column_name);
            if let Ok(idx) = joined_batch.schema().index_of(&source_col_name) {
                columns.push(take(joined_batch.column(idx), &indices, None)?);
            } else {
                return Err(DataFusionError::Plan(format!(
                    "No value provided for column '{}' in INSERT",
                    column_name
                )));
            }
        }
    }

    RecordBatch::try_new(target_schema.clone(), columns).map_err(|e| {
        DataFusionError::ArrowError(
            Box::new(e),
            Some("Failed to create INSERT data batch with values".to_string()),
        )
    })
}

/// Extracts source data for UPDATE * action.
///
/// Creates a data batch with source columns mapped to target schema.
fn extract_source_data_for_update(
    joined_batch: &RecordBatch,
    matched_indices: &[usize],
    target_schema: &ArrowSchemaRef,
) -> DFResult<RecordBatch> {
    use datafusion::arrow::compute::take;

    // Build indices array for filtering
    let indices = datafusion::arrow::array::UInt32Array::from(
        matched_indices.iter().map(|&i| i as u32).collect::<Vec<_>>(),
    );

    // For each target column, find corresponding source column
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(target_schema.fields().len());

    for field in target_schema.fields() {
        let source_col_name = format!("{}{}", SOURCE_PREFIX, field.name());

        let column = if let Ok(idx) = joined_batch.schema().index_of(&source_col_name) {
            // Take values at matched indices
            take(joined_batch.column(idx), &indices, None)?
        } else {
            // Fall back to target column if source doesn't have this column
            let target_col_name = format!("{}{}", TARGET_PREFIX, field.name());
            let idx = joined_batch
                .schema()
                .index_of(&target_col_name)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            take(joined_batch.column(idx), &indices, None)?
        };

        columns.push(column);
    }

    RecordBatch::try_new(target_schema.clone(), columns).map_err(|e| {
        DataFusionError::ArrowError(
            Box::new(e),
            Some("Failed to create UPDATE data batch".to_string()),
        )
    })
}

/// Applies SET assignments to create updated rows.
///
/// Evaluates each assignment expression against the joined batch and creates
/// a new batch with the updated values.
fn apply_update_assignments(
    joined_batch: &RecordBatch,
    matched_indices: &[usize],
    assignments: &[(String, Expr)],
    target_schema: &ArrowSchemaRef,
) -> DFResult<RecordBatch> {
    use datafusion::arrow::compute::take;

    // Build indices array for filtering
    let indices = datafusion::arrow::array::UInt32Array::from(
        matched_indices.iter().map(|&i| i as u32).collect::<Vec<_>>(),
    );

    // Build assignment map for quick lookup (reserved for condition evaluation)
    let _assignment_map: HashMap<String, &Expr> = assignments
        .iter()
        .map(|(col, expr)| (col.clone(), expr))
        .collect();

    // Compile expressions for assignments
    let df_schema = DFSchema::try_from(joined_batch.schema().as_ref().clone())?;
    let ctx = SessionContext::new();
    let execution_props = ctx.state().execution_props().clone();

    let mut compiled_assignments: HashMap<String, Arc<dyn PhysicalExpr>> = HashMap::new();
    for (col, expr) in assignments {
        let phys_expr = create_physical_expr(expr, &df_schema, &execution_props)?;
        compiled_assignments.insert(col.clone(), phys_expr);
    }

    // Evaluate all expressions against matched rows first
    // Filter the batch to only matched rows for expression evaluation
    let filtered_columns: Vec<ArrayRef> = joined_batch
        .columns()
        .iter()
        .map(|col| take(col, &indices, None))
        .collect::<Result<Vec<_>, _>>()?;
    let filtered_batch = RecordBatch::try_new(joined_batch.schema(), filtered_columns)?;

    // Evaluate compiled expressions
    let mut evaluated_columns: HashMap<String, ArrayRef> = HashMap::new();
    for (col, phys_expr) in &compiled_assignments {
        let result = phys_expr.evaluate(&filtered_batch)?;
        evaluated_columns.insert(col.clone(), result.into_array(filtered_batch.num_rows())?);
    }

    // Build output batch using target schema
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(target_schema.fields().len());

    for field in target_schema.fields() {
        let column_name = field.name();

        if let Some(evaluated_col) = evaluated_columns.get(column_name) {
            // Use evaluated SET expression
            columns.push(evaluated_col.clone());
        } else {
            // Use original target column value
            let target_col_name = format!("{}{}", TARGET_PREFIX, column_name);
            if let Ok(idx) = joined_batch.schema().index_of(&target_col_name) {
                columns.push(take(joined_batch.column(idx), &indices, None)?);
            } else {
                // Try source column as fallback
                let source_col_name = format!("{}{}", SOURCE_PREFIX, column_name);
                let idx = joined_batch
                    .schema()
                    .index_of(&source_col_name)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                columns.push(take(joined_batch.column(idx), &indices, None)?);
            }
        }
    }

    RecordBatch::try_new(target_schema.clone(), columns).map_err(|e| {
        DataFusionError::ArrowError(
            Box::new(e),
            Some("Failed to create UPDATE data batch with assignments".to_string()),
        )
    })
}

/// Extracts join key column names from the match condition.
///
/// Expects a simple equality condition like `col("target.id") = col("source.id")`.
/// Returns (target_key, source_key) column names.
fn extract_join_keys(match_condition: &Expr) -> DFResult<(String, String)> {
    match match_condition {
        Expr::BinaryExpr(binary) => {
            if binary.op != datafusion::logical_expr::Operator::Eq {
                return Err(DataFusionError::Plan(format!(
                    "MERGE ON condition must be an equality (=). Got: {:?}",
                    binary.op
                )));
            }

            let left_col = extract_column_name(&binary.left)?;
            let right_col = extract_column_name(&binary.right)?;

            // Determine which is target and which is source based on prefix
            let (target_key, source_key) = if left_col.starts_with("target.") {
                (
                    left_col.strip_prefix("target.").unwrap().to_string(),
                    right_col
                        .strip_prefix("source.")
                        .unwrap_or(&right_col)
                        .to_string(),
                )
            } else if right_col.starts_with("target.") {
                (
                    right_col.strip_prefix("target.").unwrap().to_string(),
                    left_col
                        .strip_prefix("source.")
                        .unwrap_or(&left_col)
                        .to_string(),
                )
            } else {
                // No prefix - assume left is target, right is source
                (left_col, right_col)
            };

            Ok((target_key, source_key))
        }
        _ => Err(DataFusionError::Plan(format!(
            "MERGE ON condition must be a binary equality expression. Got: {:?}",
            match_condition
        ))),
    }
}

/// Extracts column name from an expression.
fn extract_column_name(expr: &Expr) -> DFResult<String> {
    match expr {
        Expr::Column(col) => Ok(col.name.clone()),
        Expr::Alias(alias) => extract_column_name(&alias.expr),
        _ => Err(DataFusionError::Plan(format!(
            "Expected column reference in ON condition. Got: {:?}",
            expr
        ))),
    }
}

/// Performs a FULL OUTER JOIN between target and source batches.
///
/// Uses DataFusion's DataFrame API for the join operation.
async fn perform_full_outer_join(
    target_batches: &[RecordBatch],
    source_batches: &[RecordBatch],
    _target_schema: &ArrowSchemaRef,
    target_key: &str,
    source_key: &str,
) -> DFResult<Vec<RecordBatch>> {
    if target_batches.is_empty() && source_batches.is_empty() {
        return Ok(vec![]);
    }

    // Create session context for the join
    let ctx = SessionContext::new();

    // Get schemas, handling empty batch cases
    let target_schema = if !target_batches.is_empty() {
        target_batches[0].schema()
    } else {
        // Create empty schema with metadata columns
        Arc::new(ArrowSchema::new(vec![
            Field::new(MERGE_FILE_PATH_COL, DataType::Utf8, false),
            Field::new(MERGE_ROW_POS_COL, DataType::Int64, false),
        ]))
    };

    let source_schema = if !source_batches.is_empty() {
        source_batches[0].schema()
    } else {
        Arc::new(ArrowSchema::new(Vec::<Field>::new()))
    };

    // Rename target columns with prefix to avoid conflicts
    let target_fields: Vec<Field> = target_schema
        .fields()
        .iter()
        .map(|f| {
            let new_name = format!("{}{}", TARGET_PREFIX, f.name());
            Field::new(&new_name, f.data_type().clone(), f.is_nullable())
        })
        .collect();
    let prefixed_target_schema = Arc::new(ArrowSchema::new(target_fields));

    // Rename source columns with prefix
    let source_fields: Vec<Field> = source_schema
        .fields()
        .iter()
        .map(|f| {
            let new_name = format!("{}{}", SOURCE_PREFIX, f.name());
            Field::new(&new_name, f.data_type().clone(), f.is_nullable())
        })
        .collect();
    let prefixed_source_schema = Arc::new(ArrowSchema::new(source_fields));

    // Rename columns in target batches
    let prefixed_target_batches: Vec<RecordBatch> = target_batches
        .iter()
        .map(|batch| RecordBatch::try_new(prefixed_target_schema.clone(), batch.columns().to_vec()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    // Rename columns in source batches
    let prefixed_source_batches: Vec<RecordBatch> = source_batches
        .iter()
        .map(|batch| RecordBatch::try_new(prefixed_source_schema.clone(), batch.columns().to_vec()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    // Create MemTables
    let target_table = Arc::new(MemTable::try_new(
        prefixed_target_schema.clone(),
        vec![prefixed_target_batches],
    )?);

    let source_table = Arc::new(MemTable::try_new(
        prefixed_source_schema.clone(),
        vec![prefixed_source_batches],
    )?);

    // Register tables
    ctx.register_table("target", target_table)?;
    ctx.register_table("source", source_table)?;

    // Create DataFrames
    let target_df = ctx.table("target").await?;
    let source_df = ctx.table("source").await?;

    // Prefixed key column names
    let prefixed_target_key = format!("{}{}", TARGET_PREFIX, target_key);
    let prefixed_source_key = format!("{}{}", SOURCE_PREFIX, source_key);

    // Perform FULL OUTER JOIN
    let joined_df = target_df.join(
        source_df,
        JoinType::Full,
        &[&prefixed_target_key],
        &[&prefixed_source_key],
        None,
    )?;

    // Collect results
    let joined_batches = joined_df.collect().await?;

    Ok(joined_batches)
}

/// Classifies rows after the JOIN and counts actions.
///
/// For each row:
/// 1. Determine classification based on NULL patterns in join keys
/// 2. Apply WHEN clauses to determine action
/// 3. Count actions for stats
#[allow(dead_code)] // Reserved for future use in NOT MATCHED handling
fn classify_and_count_actions(
    joined_batches: &[RecordBatch],
    target_key: &str,
    source_key: &str,
    when_matched: &[WhenMatchedClause],
    when_not_matched: &[WhenNotMatchedClause],
    when_not_matched_by_source: &[WhenNotMatchedBySourceClause],
) -> DFResult<MergeStats> {
    let mut stats = MergeStats::default();

    let prefixed_target_key = format!("{}{}", TARGET_PREFIX, target_key);
    let prefixed_source_key = format!("{}{}", SOURCE_PREFIX, source_key);

    for batch in joined_batches {
        // Find key columns
        let target_key_idx = batch
            .schema()
            .index_of(&prefixed_target_key)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        let source_key_idx = batch
            .schema()
            .index_of(&prefixed_source_key)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let target_key_col = batch.column(target_key_idx);
        let source_key_col = batch.column(source_key_idx);

        // Process each row
        for row_idx in 0..batch.num_rows() {
            let target_is_null = target_key_col.is_null(row_idx);
            let source_is_null = source_key_col.is_null(row_idx);

            // Classify the row
            let classification = match RowClassification::from_join_nulls(target_is_null, source_is_null) {
                Some(c) => c,
                None => {
                    // Both keys NULL - shouldn't happen in proper JOIN
                    continue;
                }
            };

            // Determine action using WHEN clause evaluation
            let action = evaluate_when_clauses(
                classification,
                when_matched,
                when_not_matched,
                when_not_matched_by_source,
            );

            // Update stats based on action
            match action {
                MergeAction::Insert => stats.rows_inserted += 1,
                MergeAction::Update(_) | MergeAction::UpdateAll => stats.rows_updated += 1,
                MergeAction::Delete => stats.rows_deleted += 1,
                MergeAction::NoAction => {
                    // For NOT_MATCHED_BY_SOURCE with NoAction, the row is kept
                    // This counts as "copied" in CoW semantics if we were writing files
                    if classification == RowClassification::NotMatchedBySource {
                        stats.rows_copied += 1;
                    }
                }
            }
        }
    }

    Ok(stats)
}

// ============================================================================
// WHEN Clause Evaluation Helpers
// ============================================================================

/// Evaluates WHEN clauses for a row and returns the action to take.
///
/// This implements first-match-wins semantics: clauses are evaluated in order,
/// and the first matching clause determines the action.
///
/// # Arguments
/// * `classification` - The row classification (MATCHED, NOT_MATCHED, or NOT_MATCHED_BY_SOURCE)
/// * `when_matched` - WHEN MATCHED clauses to evaluate
/// * `when_not_matched` - WHEN NOT MATCHED clauses to evaluate
/// * `when_not_matched_by_source` - WHEN NOT MATCHED BY SOURCE clauses to evaluate
///
/// # Returns
/// The action to take for this row.
///
/// # Important Implementation Note
///
/// **CURRENT LIMITATION**: This function only handles clauses with `condition: None`.
/// Clauses with conditions are skipped. The execute() implementation MUST:
///
/// 1. Pre-compile all WHEN clause conditions into `Arc<dyn PhysicalExpr>`
/// 2. For each row, evaluate conditions against the joined batch
/// 3. Pass evaluated boolean results to determine which clause applies
///
/// Example pattern for execute():
/// ```ignore
/// // Pre-compile conditions once
/// let compiled_conditions: Vec<Option<Arc<dyn PhysicalExpr>>> = when_matched
///     .iter()
///     .map(|c| c.condition.as_ref().map(|e| compile_expr(e)))
///     .collect();
///
/// // For each row, evaluate conditions
/// for (idx, clause) in when_matched.iter().enumerate() {
///     let matches = match &compiled_conditions[idx] {
///         None => true, // No condition = always matches
///         Some(expr) => expr.evaluate(&batch)?.value(row_idx),
///     };
///     if matches {
///         return clause.action.clone();
///     }
/// }
/// ```
#[allow(dead_code)] // Will be used in MERGE execution implementation
pub fn evaluate_when_clauses(
    classification: RowClassification,
    when_matched: &[WhenMatchedClause],
    when_not_matched: &[WhenNotMatchedClause],
    when_not_matched_by_source: &[WhenNotMatchedBySourceClause],
) -> MergeAction {
    match classification {
        RowClassification::Matched => {
            // Try each WHEN MATCHED clause in order
            for clause in when_matched {
                // TODO: Evaluate condition expression if present
                // For now, assume all conditions match (will be implemented with physical expr evaluation)
                if clause.condition.is_none() {
                    // No additional condition - this clause applies
                    return match &clause.action {
                        MatchedAction::Update(assignments) => {
                            MergeAction::Update(assignments.clone())
                        }
                        MatchedAction::UpdateAll => MergeAction::UpdateAll,
                        MatchedAction::Delete => MergeAction::Delete,
                    };
                }
                // If condition exists, we need to evaluate it against the row
                // This will be implemented in execute_merge when we have the batch context
            }
            // No clause matched - keep row unchanged
            MergeAction::NoAction
        }
        RowClassification::NotMatched => {
            // Try each WHEN NOT MATCHED clause in order
            for clause in when_not_matched {
                if clause.condition.is_none() {
                    return match &clause.action {
                        NotMatchedAction::Insert(_) => MergeAction::Insert,
                        NotMatchedAction::InsertAll => MergeAction::Insert,
                    };
                }
            }
            MergeAction::NoAction
        }
        RowClassification::NotMatchedBySource => {
            // Try each WHEN NOT MATCHED BY SOURCE clause in order
            for clause in when_not_matched_by_source {
                if clause.condition.is_none() {
                    return match &clause.action {
                        NotMatchedBySourceAction::Update(assignments) => {
                            MergeAction::Update(assignments.clone())
                        }
                        NotMatchedBySourceAction::Delete => MergeAction::Delete,
                    };
                }
            }
            // Default for NOT_MATCHED_BY_SOURCE: keep the row unchanged
            MergeAction::NoAction
        }
    }
}

/// Adds metadata columns (file_path, row_pos) to a record batch.
///
/// This is used to track which file and position each target row came from,
/// enabling position delete generation after the JOIN.
///
/// # Important: Per-File Batch Assumption
///
/// This function assumes all rows in `batch` come from the same file at
/// sequential positions starting from `row_offset`. This matches the pattern
/// used in UPDATE execution where we iterate through `FileScanTask` objects
/// and process one file at a time:
///
/// ```ignore
/// // Correct usage pattern (from UPDATE implementation):
/// for task in file_scan_tasks {
///     let file_path = task.data_file_path().to_string();
///     let mut row_offset = 0i64;
///
///     for batch in read_file(&task) {
///         let batch_with_meta = add_metadata_columns(batch, &file_path, row_offset)?;
///         row_offset += batch.num_rows() as i64;
///         // Process batch_with_meta...
///     }
/// }
/// ```
///
/// **DO NOT** use this with batches containing rows from multiple files or
/// non-sequential positions - the metadata will be incorrect and position
/// deletes will target wrong rows.
#[allow(dead_code)] // Will be used in MERGE execution implementation
fn add_metadata_columns(
    batch: RecordBatch,
    file_path: &str,
    row_offset: i64,
) -> DFResult<RecordBatch> {
    use datafusion::error::DataFusionError;

    let num_rows = batch.num_rows();

    // Create file_path column (same value for all rows)
    let file_paths: Vec<&str> = vec![file_path; num_rows];
    let file_path_array = Arc::new(StringArray::from(file_paths)) as ArrayRef;

    // Create row_pos column (sequential from row_offset)
    let positions: Vec<i64> = (row_offset..(row_offset + num_rows as i64)).collect();
    let row_pos_array = Arc::new(Int64Array::from(positions)) as ArrayRef;

    // Build new schema with metadata columns
    let mut fields: Vec<Field> = batch.schema().fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(Field::new(MERGE_FILE_PATH_COL, DataType::Utf8, false));
    fields.push(Field::new(MERGE_ROW_POS_COL, DataType::Int64, false));
    let new_schema = Arc::new(ArrowSchema::new(fields));

    // Build new columns array
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(file_path_array);
    columns.push(row_pos_array);

    RecordBatch::try_new(new_schema, columns).map_err(|e| {
        DataFusionError::ArrowError(
            Box::new(e),
            Some("Failed to add metadata columns to batch".to_string()),
        )
    })
}

/// Evaluates a condition expression for a set of row indices and returns which indices satisfy the condition.
///
/// # Arguments
/// * `condition` - Optional condition expression. If None, all rows pass.
/// * `batch` - The record batch containing the data
/// * `row_indices` - The row indices to evaluate
/// * `batch_schema` - The DFSchema for expression compilation
///
/// # Returns
/// The subset of `row_indices` that satisfy the condition.
fn evaluate_condition_for_rows(
    condition: &Option<Expr>,
    batch: &RecordBatch,
    row_indices: &[usize],
    batch_schema: &DFSchema,
) -> DFResult<Vec<usize>> {
    // If no condition, all rows pass
    let Some(condition_expr) = condition else {
        return Ok(row_indices.to_vec());
    };

    if row_indices.is_empty() {
        return Ok(Vec::new());
    }

    // Compile the expression
    let ctx = SessionContext::new();
    let execution_props = ctx.state().execution_props().clone();
    let physical_expr = create_physical_expr(condition_expr, batch_schema, &execution_props)?;

    // Create a filtered batch with only the candidate rows for efficient evaluation
    let indices_array = datafusion::arrow::array::UInt32Array::from(
        row_indices.iter().map(|&i| i as u32).collect::<Vec<_>>(),
    );

    let filtered_columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|col| datafusion::arrow::compute::take(col, &indices_array, None))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    let filtered_batch = RecordBatch::try_new(batch.schema(), filtered_columns).map_err(|e| {
        DataFusionError::ArrowError(
            Box::new(e),
            Some("Failed to create filtered batch for condition evaluation".to_string()),
        )
    })?;

    // Evaluate the condition
    let result = physical_expr.evaluate(&filtered_batch)?;

    // Extract boolean results
    let bool_array = match result {
        datafusion::physical_plan::ColumnarValue::Array(arr) => arr
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("Condition must return boolean array".to_string())
            })?
            .clone(),
        datafusion::physical_plan::ColumnarValue::Scalar(scalar) => {
            match scalar {
                datafusion::scalar::ScalarValue::Boolean(Some(b)) => {
                    // Scalar true/false applies to all rows
                    if b {
                        return Ok(row_indices.to_vec());
                    } else {
                        return Ok(Vec::new());
                    }
                }
                datafusion::scalar::ScalarValue::Boolean(None) => {
                    // NULL condition = false for all rows
                    return Ok(Vec::new());
                }
                _ => {
                    return Err(DataFusionError::Internal(
                        "Condition must return boolean scalar".to_string(),
                    ));
                }
            }
        }
    };

    // Collect indices where condition is true
    let passing_indices: Vec<usize> = row_indices
        .iter()
        .enumerate()
        .filter_map(|(eval_idx, &original_idx)| {
            if bool_array.is_valid(eval_idx) && bool_array.value(eval_idx) {
                Some(original_idx)
            } else {
                None
            }
        })
        .collect();

    Ok(passing_indices)
}

/// Creates a position delete batch for the given file paths and positions.
#[allow(dead_code)] // Will be used in MERGE execution implementation
fn make_position_delete_batch(
    file_paths: &[String],
    positions: &[i64],
    schema: ArrowSchemaRef,
) -> DFResult<RecordBatch> {
    use datafusion::error::DataFusionError;

    let file_path_refs: Vec<&str> = file_paths.iter().map(|s| s.as_str()).collect();
    let file_path_array = Arc::new(StringArray::from(file_path_refs)) as ArrayRef;
    let pos_array = Arc::new(Int64Array::from(positions.to_vec())) as ArrayRef;

    RecordBatch::try_new(schema, vec![file_path_array, pos_array]).map_err(|e| {
        DataFusionError::ArrowError(
            Box::new(e),
            Some("Failed to create position delete batch".to_string()),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::lit;

    #[test]
    fn test_output_schema() {
        let schema = IcebergMergeExec::make_output_schema();
        assert_eq!(schema.fields().len(), 5);
        assert_eq!(schema.field(0).name(), MERGE_DATA_FILES_COL);
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(1).name(), MERGE_DELETE_FILES_COL);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(2).name(), MERGE_INSERTED_COUNT_COL);
        assert_eq!(schema.field(2).data_type(), &DataType::UInt64);
        assert_eq!(schema.field(3).name(), MERGE_UPDATED_COUNT_COL);
        assert_eq!(schema.field(3).data_type(), &DataType::UInt64);
        assert_eq!(schema.field(4).name(), MERGE_DELETED_COUNT_COL);
        assert_eq!(schema.field(4).data_type(), &DataType::UInt64);
    }

    // ============================================================================
    // RowClassification Tests
    // ============================================================================

    #[test]
    fn test_row_classification_from_join_nulls_matched() {
        // Both sides have data - MATCHED
        let result = RowClassification::from_join_nulls(false, false);
        assert_eq!(result, Some(RowClassification::Matched));
    }

    #[test]
    fn test_row_classification_from_join_nulls_not_matched() {
        // Target is NULL, source has data - NOT_MATCHED (insert candidate)
        let result = RowClassification::from_join_nulls(true, false);
        assert_eq!(result, Some(RowClassification::NotMatched));
    }

    #[test]
    fn test_row_classification_from_join_nulls_not_matched_by_source() {
        // Target has data, source is NULL - NOT_MATCHED_BY_SOURCE (delete candidate)
        let result = RowClassification::from_join_nulls(false, true);
        assert_eq!(result, Some(RowClassification::NotMatchedBySource));
    }

    #[test]
    fn test_row_classification_from_join_nulls_both_null() {
        // Both NULL - shouldn't happen in proper FULL OUTER JOIN
        let result = RowClassification::from_join_nulls(true, true);
        assert_eq!(result, None);
    }

    #[test]
    fn test_row_classification_as_str() {
        assert_eq!(RowClassification::Matched.as_str(), "MATCHED");
        assert_eq!(RowClassification::NotMatched.as_str(), "NOT_MATCHED");
        assert_eq!(
            RowClassification::NotMatchedBySource.as_str(),
            "NOT_MATCHED_BY_SOURCE"
        );
    }

    // ============================================================================
    // WHEN Clause Evaluation Tests
    // ============================================================================

    #[test]
    fn test_evaluate_when_clauses_matched_update() {
        let when_matched = vec![WhenMatchedClause {
            condition: None,
            action: MatchedAction::Update(vec![("col".to_string(), lit(1))]),
        }];

        let action = evaluate_when_clauses(
            RowClassification::Matched,
            &when_matched,
            &[],
            &[],
        );

        match action {
            MergeAction::Update(assignments) => {
                assert_eq!(assignments.len(), 1);
                assert_eq!(assignments[0].0, "col");
            }
            _ => panic!("Expected Update action"),
        }
    }

    #[test]
    fn test_evaluate_when_clauses_matched_update_all() {
        let when_matched = vec![WhenMatchedClause {
            condition: None,
            action: MatchedAction::UpdateAll,
        }];

        let action = evaluate_when_clauses(
            RowClassification::Matched,
            &when_matched,
            &[],
            &[],
        );

        assert!(matches!(action, MergeAction::UpdateAll));
    }

    #[test]
    fn test_evaluate_when_clauses_matched_delete() {
        let when_matched = vec![WhenMatchedClause {
            condition: None,
            action: MatchedAction::Delete,
        }];

        let action = evaluate_when_clauses(
            RowClassification::Matched,
            &when_matched,
            &[],
            &[],
        );

        assert!(matches!(action, MergeAction::Delete));
    }

    #[test]
    fn test_evaluate_when_clauses_not_matched_insert() {
        let when_not_matched = vec![WhenNotMatchedClause {
            condition: None,
            action: NotMatchedAction::InsertAll,
        }];

        let action = evaluate_when_clauses(
            RowClassification::NotMatched,
            &[],
            &when_not_matched,
            &[],
        );

        assert!(matches!(action, MergeAction::Insert));
    }

    #[test]
    fn test_evaluate_when_clauses_not_matched_by_source_delete() {
        let when_not_matched_by_source = vec![WhenNotMatchedBySourceClause {
            condition: None,
            action: NotMatchedBySourceAction::Delete,
        }];

        let action = evaluate_when_clauses(
            RowClassification::NotMatchedBySource,
            &[],
            &[],
            &when_not_matched_by_source,
        );

        assert!(matches!(action, MergeAction::Delete));
    }

    #[test]
    fn test_evaluate_when_clauses_no_matching_clause() {
        // No clauses defined - should return NoAction
        let action = evaluate_when_clauses(
            RowClassification::Matched,
            &[],
            &[],
            &[],
        );

        assert!(matches!(action, MergeAction::NoAction));
    }

    #[test]
    fn test_evaluate_when_clauses_first_match_wins() {
        // Two WHEN MATCHED clauses - first should win
        let when_matched = vec![
            WhenMatchedClause {
                condition: None,
                action: MatchedAction::Delete,
            },
            WhenMatchedClause {
                condition: None,
                action: MatchedAction::UpdateAll,
            },
        ];

        let action = evaluate_when_clauses(
            RowClassification::Matched,
            &when_matched,
            &[],
            &[],
        );

        // First clause (Delete) should win
        assert!(matches!(action, MergeAction::Delete));
    }

    // ============================================================================
    // Helper Function Tests
    // ============================================================================

    #[test]
    fn test_add_metadata_columns() {
        // Create a simple batch
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));
        let id_array = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![id_array]).unwrap();

        // Add metadata columns
        let result = add_metadata_columns(batch, "s3://bucket/file.parquet", 100).unwrap();

        // Verify schema
        assert_eq!(result.schema().fields().len(), 3);
        assert_eq!(result.schema().field(0).name(), "id");
        assert_eq!(result.schema().field(1).name(), MERGE_FILE_PATH_COL);
        assert_eq!(result.schema().field(2).name(), MERGE_ROW_POS_COL);

        // Verify data
        assert_eq!(result.num_rows(), 3);

        let file_path_col = result
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(file_path_col.value(0), "s3://bucket/file.parquet");
        assert_eq!(file_path_col.value(1), "s3://bucket/file.parquet");
        assert_eq!(file_path_col.value(2), "s3://bucket/file.parquet");

        let row_pos_col = result
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(row_pos_col.value(0), 100);
        assert_eq!(row_pos_col.value(1), 101);
        assert_eq!(row_pos_col.value(2), 102);
    }

    #[test]
    fn test_make_merge_result_batch() {
        let stats = MergeStats {
            rows_inserted: 10,
            rows_updated: 5,
            rows_deleted: 3,
            rows_copied: 0,
            files_added: 2,
            files_removed: 1,
        };

        let result = IcebergMergeExec::make_result_batch(
            vec!["data1.parquet".to_string(), "data2.parquet".to_string()],
            vec!["delete1.parquet".to_string()],
            &stats,
        )
        .unwrap();

        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.num_columns(), 5);

        let inserted = result
            .column(2)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .unwrap();
        assert_eq!(inserted.value(0), 10);

        let updated = result
            .column(3)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .unwrap();
        assert_eq!(updated.value(0), 5);

        let deleted = result
            .column(4)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .unwrap();
        assert_eq!(deleted.value(0), 3);
    }
}
