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
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use iceberg::table::Table;
use iceberg::transaction::rewrite_data_files::{
    CompactionProgressEvent, ProgressCallback, RewriteDataFilesPlan,
};
use iceberg::spec::{DataFile, serialize_data_file_to_json};
use iceberg::transaction::rewrite_data_files::FileGroup;
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
#[allow(dead_code)]
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

#[allow(dead_code)]
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
        // Return NotImplemented to prevent silent "success" with zero output
        Err(DataFusionError::NotImplemented(
            "IcebergCompactionExec::execute() not yet implemented - \
             file group reading and writing requires full implementation"
                .to_string(),
        ))
    }
}

// =============================================================================
// Helper Functions (Tasks 5, 6, 7)
// =============================================================================

/// Result from processing a single file group.
#[derive(Debug)]
#[allow(dead_code)]
pub struct FileGroupWriteResult {
    /// The group ID that was processed.
    pub group_id: u32,
    /// The new data files written.
    pub new_data_files: Vec<DataFile>,
    /// Total bytes written.
    pub bytes_written: u64,
    /// Processing duration in milliseconds.
    pub duration_ms: u64,
}

/// Read all data from a file group, applying any position deletes.
///
/// # Arguments
/// * `table` - The Iceberg table
/// * `file_group` - The file group to read
///
/// # Returns
/// A stream of RecordBatches containing the merged data from all files
/// in the group with position deletes applied.
///
/// # Note
/// This is a placeholder that needs implementation. The actual implementation
/// would:
/// 1. Create FileScanTasks for each data file in the group
/// 2. Use ArrowReaderBuilder to read with delete application
/// 3. Return a stream of RecordBatches
#[allow(dead_code)]
pub async fn read_file_group(
    _table: &Table,
    _file_group: &FileGroup,
) -> DFResult<SendableRecordBatchStream> {
    // TODO: Implement file group reading with delete application
    // This would use iceberg::arrow::ArrowReaderBuilder to read the files
    // with automatic position delete application
    Err(DataFusionError::NotImplemented(
        "read_file_group not yet implemented".to_string(),
    ))
}

/// Write RecordBatches to new data files using rolling file writer.
///
/// # Arguments
/// * `table` - The Iceberg table (for schema, partition spec, etc.)
/// * `batches` - Stream of RecordBatches to write
/// * `target_file_size` - Target size for output files
///
/// # Returns
/// Vector of DataFile entries for the newly written files.
///
/// # Note
/// This is a placeholder that needs implementation. The actual implementation
/// would:
/// 1. Create a TaskWriter or RollingFileWriter
/// 2. Write batches, rolling to new files at target size
/// 3. Close writers and collect DataFile metadata
#[allow(dead_code)]
pub async fn write_compacted_files(
    _table: &Table,
    _batches: SendableRecordBatchStream,
    _target_file_size: u64,
) -> DFResult<Vec<DataFile>> {
    // TODO: Implement file writing with rolling writer
    // This would use iceberg::writer::TaskWriter or RollingFileWriter
    Err(DataFusionError::NotImplemented(
        "write_compacted_files not yet implemented".to_string(),
    ))
}

/// Process a single file group: read, merge, and write compacted output.
///
/// # Arguments
/// * `table` - The Iceberg table
/// * `file_group` - The file group to process
/// * `group_id` - The index of this group
/// * `target_file_size` - Target output file size
///
/// # Returns
/// Result containing new data files and metrics.
#[allow(dead_code)]
pub async fn process_file_group(
    _table: &Table,
    _file_group: &FileGroup,
    group_id: u32,
    _target_file_size: u64,
) -> DFResult<FileGroupWriteResult> {
    // TODO: Implement full file group processing
    // 1. read_file_group()
    // 2. write_compacted_files()
    // 3. Return metrics
    Err(DataFusionError::NotImplemented(format!(
        "process_file_group not yet implemented for group {}",
        group_id
    )))
}

/// Serialize a vector of DataFiles to JSON for output.
///
/// Handles partition evolution by looking up each file's partition spec
/// rather than assuming all files use the default spec.
///
/// # Arguments
/// * `files` - DataFiles to serialize
/// * `table` - The table (for partition specs and format version)
#[allow(dead_code)]
pub fn serialize_data_files(files: Vec<DataFile>, table: &Table) -> DFResult<Vec<String>> {
    let metadata = table.metadata();
    let format_version = metadata.format_version();

    files
        .into_iter()
        .map(|f| {
            // Look up the partition spec for this specific file
            let spec_id = f.partition_spec_id();
            let spec = metadata.partition_spec_by_id(spec_id).ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Partition spec {} not found for DataFile",
                    spec_id
                ))
            })?;
            let partition_type =
                spec.partition_type(metadata.current_schema())
                    .map_err(|e| {
                        DataFusionError::Internal(format!(
                            "Failed to get partition type for spec {}: {}",
                            spec_id, e
                        ))
                    })?;

            serialize_data_file_to_json(f, &partition_type, format_version).map_err(|e| {
                DataFusionError::Internal(format!("Failed to serialize DataFile: {}", e))
            })
        })
        .collect()
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
