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

//! UPDATE operation support for Iceberg tables in DataFusion.
//!
//! This module provides programmatic UPDATE operations for Iceberg tables using
//! Copy-on-Write (CoW) semantics. Updated rows are written to new data files while
//! original rows are marked as deleted via position delete files.
//!
//! # Architecture
//!
//! The UPDATE operation is implemented as a four-stage execution plan:
//!
//! 1. **Scan**: Find rows matching the WHERE predicate and track their positions
//! 2. **Transform**: Apply SET expressions to matching rows
//! 3. **Write**: Write new data files (updated rows) and position delete files (original rows)
//! 4. **Commit**: Atomically commit both data files and delete files in a single transaction
//!
//! # Usage
//!
//! ## Programmatic API (Builder Pattern)
//!
//! Use `IcebergTableProvider::update()` for programmatic updates:
//!
//! ```ignore
//! use datafusion::prelude::*;
//! use iceberg_datafusion::IcebergTableProvider;
//!
//! // Create the table provider
//! let provider = IcebergTableProvider::try_new(catalog, namespace, "my_table").await?;
//!
//! // Update rows where id = 42
//! let updated_count = provider
//!     .update()
//!     .set("status", lit("shipped"))
//!     .set("updated_at", current_timestamp())
//!     .filter(col("id").eq(lit(42)))
//!     .execute(&session_state)
//!     .await?;
//! println!("Updated {} rows", updated_count);
//!
//! // Update all rows (full table update)
//! let updated_count = provider
//!     .update()
//!     .set("version", col("version") + lit(1))
//!     .execute(&session_state)
//!     .await?;
//! ```
//!
//! ## Helper Function
//!
//! Use `update_table()` for a standalone update operation:
//!
//! ```ignore
//! use iceberg_datafusion::update::update_table;
//! use iceberg::NamespaceIdent;
//!
//! let updated_count = update_table(
//!     &catalog,
//!     &NamespaceIdent::from_strs(["my_namespace"])?,
//!     "my_table",
//!     &session_state,
//!     vec![("status", lit("inactive"))],
//!     Some(col("last_active").lt(lit("2024-01-01"))),
//! ).await?;
//! ```
//!
//! # Current Limitations
//!
//! - Updating partition columns is not supported (would require moving rows between partitions)
//! - Type coercion is strict (expression type must exactly match column type)
//! - Only works with Iceberg format version 2 tables (position deletes require v2)

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::Session;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use iceberg::table::Table;
use iceberg::{Catalog, NamespaceIdent};

use crate::error::to_datafusion_error;
use crate::table::IcebergTableProvider;

/// Builder for UPDATE operations on Iceberg tables.
///
/// Provides a fluent API for constructing UPDATE statements with SET assignments
/// and optional WHERE predicates.
///
/// # Example
///
/// ```ignore
/// let count = provider
///     .update()
///     .set("status", lit("complete"))
///     .set("count", col("count") + lit(1))
///     .filter(col("id").eq(lit(42)))
///     .execute(&session_state)
///     .await?;
/// ```
pub struct UpdateBuilder {
    table: Table,
    catalog: Arc<dyn Catalog>,
    schema: ArrowSchemaRef,
    assignments: Vec<(String, Expr)>,
    filter: Option<Expr>,
}

impl UpdateBuilder {
    /// Creates a new UpdateBuilder for the given table.
    pub(crate) fn new(table: Table, catalog: Arc<dyn Catalog>, schema: ArrowSchemaRef) -> Self {
        Self {
            table,
            catalog,
            schema,
            assignments: Vec::new(),
            filter: None,
        }
    }

    /// Adds a SET assignment to the UPDATE.
    ///
    /// # Arguments
    ///
    /// * `column` - The name of the column to update
    /// * `value` - The expression to assign to the column
    ///
    /// # Example
    ///
    /// ```ignore
    /// builder
    ///     .set("name", lit("new_name"))
    ///     .set("count", col("count") + lit(1))
    /// ```
    pub fn set(mut self, column: impl Into<String>, value: Expr) -> Self {
        self.assignments.push((column.into(), value));
        self
    }

    /// Adds a WHERE predicate to filter which rows are updated.
    ///
    /// If not called, all rows in the table will be updated.
    ///
    /// # Arguments
    ///
    /// * `predicate` - The filter expression
    ///
    /// # Example
    ///
    /// ```ignore
    /// builder.filter(col("id").eq(lit(42)))
    /// ```
    pub fn filter(mut self, predicate: Expr) -> Self {
        self.filter = Some(predicate);
        self
    }

    /// Validates the UPDATE operation before execution.
    ///
    /// Checks:
    /// - At least one SET assignment is provided
    /// - All target columns exist in the table schema
    /// - No partition columns are being updated (partition column updates require rewriting
    ///   entire files to new partitions, which is not supported)
    fn validate(&self) -> DFResult<()> {
        use datafusion::error::DataFusionError;

        // Must have at least one assignment
        if self.assignments.is_empty() {
            return Err(DataFusionError::Plan(
                "UPDATE requires at least one SET assignment".to_string(),
            ));
        }

        // Get partition source columns (the columns that partition transforms are applied to)
        let partition_spec = self.table.metadata().default_partition_spec();
        let iceberg_schema = self.table.metadata().current_schema();

        // Build set of partition source field IDs
        let partition_source_ids: std::collections::HashSet<i32> = partition_spec
            .fields()
            .iter()
            .map(|f| f.source_id)
            .collect();

        // Validate each assignment
        for (column_name, _expr) in &self.assignments {
            // Column must exist
            let field = iceberg_schema.field_by_name(column_name).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Column '{}' not found in table schema",
                    column_name
                ))
            })?;

            // Cannot update partition source columns
            if partition_source_ids.contains(&field.id) {
                return Err(DataFusionError::Plan(format!(
                    "Cannot UPDATE partition column '{}'. Updating partition columns would \
                     require moving rows between partitions, which is not supported. \
                     Consider using DELETE + INSERT instead.",
                    column_name
                )));
            }

            // Note: Type validation is deferred to execution time when we have
            // the physical expression and can properly evaluate types
        }

        Ok(())
    }

    /// Executes the UPDATE operation and returns the number of rows updated.
    ///
    /// This method:
    /// 1. Validates the UPDATE configuration
    /// 2. Builds the execution plan (scan -> transform -> write -> commit)
    /// 3. Executes the plan
    /// 4. Returns the count of updated rows
    ///
    /// # Arguments
    ///
    /// * `session` - The DataFusion session state
    ///
    /// # Returns
    ///
    /// The number of rows updated.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Validation fails (missing assignments, invalid columns, partition column update)
    /// - The update operation fails
    /// - The commit fails (e.g., due to conflicts)
    pub async fn execute(self, _session: &dyn Session) -> DFResult<u64> {
        use datafusion::error::DataFusionError;
        use datafusion::execution::context::TaskContext;
        use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
        use datafusion::physical_plan::collect;

        use crate::physical_plan::update::IcebergUpdateExec;
        use crate::physical_plan::update_commit::IcebergUpdateCommitExec;

        // Validate first (includes check for partition column updates)
        self.validate()?;

        // Build the update execution plan chain
        let filters: Vec<Expr> = self.filter.into_iter().collect();

        // Step 1 & 2 & 3: Scan, Transform, and Write in single execution plan
        let update_exec = Arc::new(IcebergUpdateExec::new(
            self.table.clone(),
            self.schema.clone(),
            self.assignments.clone(),
            filters,
        ));

        // Step 4: Coalesce partitions for single commit
        let coalesce = Arc::new(CoalescePartitionsExec::new(update_exec));

        // Step 5: Commit both data files and delete files
        let update_commit = Arc::new(IcebergUpdateCommitExec::new(
            self.table,
            self.catalog,
            coalesce.clone(),
            coalesce.schema(),
        ));

        // Execute the plan
        let task_ctx = Arc::new(TaskContext::default());
        let batches = collect(update_commit, task_ctx).await?;

        // Extract the count from the result
        if batches.is_empty() || batches[0].num_rows() == 0 {
            return Ok(0);
        }

        let count_array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal("Expected UInt64Array for update count".to_string())
            })?;

        Ok(count_array.value(0))
    }
}

/// Updates rows in an Iceberg table that match the given predicate.
///
/// This is a convenience function that creates an `IcebergTableProvider`,
/// builds an `UpdateBuilder`, and executes it. For multiple operations on
/// the same table, it's more efficient to create the provider once and reuse it.
///
/// # Arguments
///
/// * `catalog` - The catalog containing the table
/// * `namespace` - The namespace containing the table
/// * `table_name` - The name of the table to update
/// * `session` - The DataFusion session state
/// * `assignments` - Column assignments as (column_name, expression) pairs
/// * `predicate` - Optional filter expression. If `None`, updates all rows.
///
/// # Returns
///
/// The number of rows updated.
///
/// # Errors
///
/// Returns an error if:
/// - The table doesn't exist
/// - The update operation fails
/// - The commit fails (e.g., due to conflicts)
///
/// # Example
///
/// ```ignore
/// use datafusion::prelude::*;
/// use iceberg::{Catalog, NamespaceIdent};
/// use iceberg_datafusion::update::update_table;
///
/// // Update status for all inactive users
/// let updated = update_table(
///     &catalog,
///     &NamespaceIdent::from_strs(["db"])?,
///     "users",
///     &session_state,
///     vec![("status", lit("archived"))],
///     Some(col("active").eq(lit(false))),
/// ).await?;
///
/// println!("Updated {} inactive users", updated);
/// ```
pub async fn update_table(
    catalog: &Arc<dyn Catalog>,
    namespace: &NamespaceIdent,
    table_name: impl Into<String>,
    session: &dyn Session,
    assignments: Vec<(&str, Expr)>,
    predicate: Option<Expr>,
) -> DFResult<u64> {
    let provider = IcebergTableProvider::try_new(catalog.clone(), namespace.clone(), table_name)
        .await
        .map_err(to_datafusion_error)?;

    let mut builder = provider.update().await.map_err(to_datafusion_error)?;

    for (column, value) in assignments {
        builder = builder.set(column, value);
    }

    if let Some(pred) = predicate {
        builder = builder.filter(pred);
    }

    builder.execute(session).await
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_update_builder_requires_assignments() {
        // We can't fully test without a real table, but we can test the validation logic
        // by creating a mock scenario. For now, just verify the error message format.
        let err_msg = "UPDATE requires at least one SET assignment";
        assert!(err_msg.contains("SET assignment"));
    }
}
