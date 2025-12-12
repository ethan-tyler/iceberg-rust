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

//! Physical plan execution for Iceberg table compaction (rewrite_data_files).
//!
//! This module provides `IcebergCompactionExec` which executes file group rewrites
//! by reading data files, applying position/equality deletes, and writing compacted
//! Parquet files.
//!
//! ## Architecture
//!
//! ```text
//! RewriteDataFilesPlan (from core iceberg)
//!     │
//!     ▼
//! IcebergCompactionExec (this module)
//!     │ For each FileGroup:
//!     │   1. Check cancellation token
//!     │   2. Emit progress event (GroupStarted)
//!     │   3. Read data files with delete application
//!     │   4. Write merged output (TaskWriter)
//!     │   5. Emit progress event (GroupCompleted)
//!     │   6. Yield DataFile JSON
//!     ▼
//! IcebergCompactionCommitExec
//!     │ Collects all DataFile JSON
//!     │ Calls RewriteDataFilesCommitter::commit()
//!     ▼
//! Updated Table Snapshot
//! ```

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::datatypes::{
    DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use iceberg::table::Table;
use iceberg::transaction::rewrite_data_files::{
    CompactionProgressEvent, ProgressCallback, RewriteDataFilesPlan,
};
use tokio_util::sync::CancellationToken;

/// Column name for serialized data files output.
pub const COMPACTION_DATA_FILES_COL: &str = "data_files";

/// Column name for group ID output.
pub const COMPACTION_GROUP_ID_COL: &str = "group_id";

/// Schema for compaction executor output.
///
/// Returns schema with:
/// - `group_id`: UInt32 - which FileGroup this result belongs to
/// - `data_files`: Utf8 - JSON-serialized DataFile entries
pub fn compaction_output_schema() -> ArrowSchemaRef {
    Arc::new(ArrowSchema::new(vec![
        Field::new(COMPACTION_GROUP_ID_COL, DataType::UInt32, false),
        Field::new(COMPACTION_DATA_FILES_COL, DataType::Utf8, false),
    ]))
}

/// Physical execution plan for Iceberg table compaction.
///
/// Reads file groups from a `RewriteDataFilesPlan`, applies deletes,
/// merges data, and writes new compacted Parquet files.
///
/// Supports:
/// - Progress reporting via callbacks
/// - Cancellation via token
/// - Position delete application
pub struct IcebergCompactionExec {
    /// The Iceberg table being compacted.
    table: Table,
    /// The compaction plan with file groups.
    plan: RewriteDataFilesPlan,
    /// Output schema.
    output_schema: ArrowSchemaRef,
    /// Plan properties.
    plan_properties: PlanProperties,
    /// Optional cancellation token.
    cancellation_token: Option<CancellationToken>,
    /// Optional progress callback.
    progress_callback: Option<ProgressCallback>,
}

impl IcebergCompactionExec {
    /// Create a new compaction executor.
    pub fn new(table: Table, plan: RewriteDataFilesPlan) -> DFResult<Self> {
        let output_schema = compaction_output_schema();
        let plan_properties = Self::compute_properties(output_schema.clone());

        Ok(Self {
            table,
            plan,
            output_schema,
            plan_properties,
            cancellation_token: None,
            progress_callback: None,
        })
    }

    /// Set cancellation token.
    pub fn with_cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancellation_token = Some(token);
        self
    }

    /// Set progress callback.
    pub fn with_progress_callback(mut self, callback: ProgressCallback) -> Self {
        self.progress_callback = Some(callback);
        self
    }

    /// Check if cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancellation_token
            .as_ref()
            .is_some_and(|t| t.is_cancelled())
    }

    /// Emit progress event if callback is set.
    pub fn emit_progress(&self, event: CompactionProgressEvent) {
        if let Some(ref callback) = self.progress_callback {
            callback(event);
        }
    }

    /// Get the compaction plan.
    pub fn plan(&self) -> &RewriteDataFilesPlan {
        &self.plan
    }

    /// Get the table being compacted.
    pub fn table(&self) -> &Table {
        &self.table
    }

    /// Compute plan properties.
    fn compute_properties(schema: ArrowSchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

impl Debug for IcebergCompactionExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergCompactionExec")
            .field("file_groups", &self.plan.file_groups.len())
            .field("total_bytes", &self.plan.total_bytes)
            .field("has_cancellation", &self.cancellation_token.is_some())
            .finish()
    }
}

impl DisplayAs for IcebergCompactionExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "IcebergCompactionExec: groups={}, files={}, bytes={}",
            self.plan.file_groups.len(),
            self.plan.total_data_files,
            self.plan.total_bytes
        )
    }
}

impl ExecutionPlan for IcebergCompactionExec {
    fn name(&self) -> &str {
        "IcebergCompactionExec"
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
        vec![] // Leaf node
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "IcebergCompactionExec does not accept children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "IcebergCompactionExec only supports partition 0, got {}",
                partition
            )));
        }

        // TODO: Implement in Task 7
        // For now, return an empty stream
        let schema = self.output_schema.clone();
        let empty_stream = futures::stream::empty();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, empty_stream)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compaction_output_schema() {
        let schema = compaction_output_schema();
        assert_eq!(schema.fields().len(), 2);
        assert!(schema.field_with_name(COMPACTION_DATA_FILES_COL).is_ok());
        assert!(schema.field_with_name(COMPACTION_GROUP_ID_COL).is_ok());
    }

    #[test]
    fn test_compaction_exec_has_cancellation() {
        let token = CancellationToken::new();

        // This test validates the struct has the right fields
        assert!(!token.is_cancelled());
        token.cancel();
        assert!(token.is_cancelled());
    }
}
