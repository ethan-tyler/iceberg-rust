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

use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::{
    DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::prelude::Expr;
use iceberg::table::Table;

use crate::merge::{
    MatchedAction, NotMatchedAction, NotMatchedBySourceAction, WhenMatchedClause,
    WhenNotMatchedBySourceClause, WhenNotMatchedClause,
};

/// Column name for serialized data files in output
pub const MERGE_DATA_FILES_COL: &str = "data_files";
/// Column name for serialized delete files in output
pub const MERGE_DELETE_FILES_COL: &str = "delete_files";
/// Column names for stats in output
pub const MERGE_INSERTED_COUNT_COL: &str = "inserted_count";
pub const MERGE_UPDATED_COUNT_COL: &str = "updated_count";
pub const MERGE_DELETED_COUNT_COL: &str = "deleted_count";

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
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        // TODO: Implement the full MERGE execution logic:
        // 1. Scan target table with metadata columns (_file_path, _pos)
        // 2. Materialize source data
        // 3. Perform FULL OUTER JOIN on match condition
        // 4. Classify each row and apply WHEN clauses
        // 5. Write data files (inserts/updates) and delete files (updates/deletes)
        //
        // IMPORTANT: When implementing, ensure concurrency safety:
        // - Capture baseline_snapshot_id at plan construction time
        // - Validate baseline before commit (reject if table modified since scan)
        // - Use row_delta() for atomic commit of data + delete files in single snapshot
        // See IcebergUpdateCommitExec for reference implementation.

        // For now, return NotImplemented error
        let output_schema = self.output_schema.clone();
        let stream = futures::stream::once(async move {
            Err::<datafusion::arrow::array::RecordBatch, _>(
                datafusion::error::DataFusionError::NotImplemented(
                    "MERGE execution is not yet fully implemented. \
                     The execution plan structure is complete; \
                     JOIN and write logic coming next."
                        .to_string(),
                ),
            )
        })
        .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(output_schema, stream)))
    }
}

use futures::StreamExt;

#[cfg(test)]
mod tests {
    use super::*;

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
}
