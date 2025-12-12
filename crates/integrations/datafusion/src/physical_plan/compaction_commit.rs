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

//! Commit executor for Iceberg table compaction.
//!
//! This module provides `IcebergCompactionCommitExec` which:
//! 1. Collects results from `IcebergCompactionExec`
//! 2. Parses the JSON-serialized DataFiles
//! 3. Commits via `RewriteDataFilesCommitter`

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use iceberg::table::Table;
use iceberg::transaction::rewrite_data_files::RewriteDataFilesPlan;

/// Schema for compaction commit output (metrics).
pub fn compaction_commit_output_schema() -> ArrowSchemaRef {
    Arc::new(ArrowSchema::new(vec![
        Field::new("rewritten_files", DataType::UInt64, false),
        Field::new("added_files", DataType::UInt64, false),
        Field::new("rewritten_bytes", DataType::UInt64, false),
        Field::new("added_bytes", DataType::UInt64, false),
    ]))
}

/// Physical plan for committing Iceberg compaction results.
///
/// Collects output from `IcebergCompactionExec`, parses the serialized
/// DataFiles, and commits via `RewriteDataFilesCommitter`.
#[allow(dead_code)]
pub struct IcebergCompactionCommitExec {
    /// The Iceberg table being compacted.
    table: Table,
    /// The compaction plan (for FileGroup metadata).
    plan: RewriteDataFilesPlan,
    /// The input execution plan (IcebergCompactionExec).
    input: Arc<dyn ExecutionPlan>,
    /// Output schema (metrics).
    output_schema: ArrowSchemaRef,
    /// Plan properties.
    plan_properties: PlanProperties,
}

impl IcebergCompactionCommitExec {
    /// Create a new compaction commit executor.
    #[allow(dead_code)]
    pub fn new(
        table: Table,
        plan: RewriteDataFilesPlan,
        input: Arc<dyn ExecutionPlan>,
    ) -> DFResult<Self> {
        let output_schema = compaction_commit_output_schema();
        let plan_properties = Self::compute_properties(output_schema.clone());

        Ok(Self {
            table,
            plan,
            input,
            output_schema,
            plan_properties,
        })
    }

    /// Get the table being compacted.
    #[allow(dead_code)]
    pub fn table(&self) -> &Table {
        &self.table
    }

    /// Get the compaction plan.
    #[allow(dead_code)]
    pub fn plan(&self) -> &RewriteDataFilesPlan {
        &self.plan
    }

    fn compute_properties(schema: ArrowSchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        )
    }
}

impl Debug for IcebergCompactionCommitExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergCompactionCommitExec")
            .field("file_groups", &self.plan.file_groups.len())
            .finish()
    }
}

impl DisplayAs for IcebergCompactionCommitExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "IcebergCompactionCommitExec: groups={}",
            self.plan.file_groups.len()
        )
    }
}

impl ExecutionPlan for IcebergCompactionCommitExec {
    fn name(&self) -> &str {
        "IcebergCompactionCommitExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.output_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "IcebergCompactionCommitExec requires exactly one child".to_string(),
            ));
        }

        Ok(Arc::new(Self {
            table: self.table.clone(),
            plan: self.plan.clone(),
            input: children[0].clone(),
            output_schema: self.output_schema.clone(),
            plan_properties: self.plan_properties.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "IcebergCompactionCommitExec only supports partition 0, got {}",
                partition
            )));
        }

        // TODO: Implement commit logic
        // 1. Execute input to get (group_id, data_files_json) batches
        // 2. Parse JSON back to DataFile entries
        // 3. Call RewriteDataFilesCommitter::commit()
        // 4. Return metrics batch
        Err(DataFusionError::NotImplemented(
            "IcebergCompactionCommitExec::execute() not yet implemented - \
             depends on IcebergCompactionExec being fully implemented"
                .to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compaction_commit_output_schema() {
        let schema = compaction_commit_output_schema();
        assert_eq!(schema.fields().len(), 4);
        assert!(schema.field_with_name("rewritten_files").is_ok());
        assert!(schema.field_with_name("added_files").is_ok());
        assert!(schema.field_with_name("rewritten_bytes").is_ok());
        assert!(schema.field_with_name("added_bytes").is_ok());
    }
}
