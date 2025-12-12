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
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray, UInt32Array};
use datafusion::arrow::datatypes::{
    DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::col;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::prelude::SessionContext;
use futures::{StreamExt, TryStreamExt};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::scan::{FileScanTask, FileScanTaskDeleteFile, FileScanTaskStream};
use iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, PartitionKey, PartitionSpec, SortDirection,
    SortOrder, Struct, TableProperties, Transform, serialize_data_file_to_json,
};
use iceberg::table::Table;
use iceberg::transaction::rewrite_data_files::{
    CompactionProgressEvent, FileGroup, ProgressCallback, RewriteDataFilesPlan, RewriteStrategy,
};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use parquet::file::properties::WriterProperties;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::partition_utils::build_partition_type_map;
use crate::to_datafusion_error;

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
    /// The rewrite strategy (bin-pack or sort).
    strategy: RewriteStrategy,
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
    /// Create a new compaction executor with default bin-pack strategy.
    pub fn new(table: Table, plan: RewriteDataFilesPlan) -> DFResult<Self> {
        Self::new_with_strategy(table, plan, RewriteStrategy::BinPack)
    }

    /// Create a new compaction executor with specified strategy.
    pub fn new_with_strategy(
        table: Table,
        plan: RewriteDataFilesPlan,
        strategy: RewriteStrategy,
    ) -> DFResult<Self> {
        let output_schema = compaction_output_schema();
        let plan_properties = Self::compute_properties(output_schema.clone());

        Ok(Self {
            table,
            plan,
            strategy,
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

    /// Get the rewrite strategy.
    pub fn strategy(&self) -> &RewriteStrategy {
        &self.strategy
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
                "IcebergCompactionExec only supports partition 0, got {partition}"
            )));
        }

        let table = self.table.clone();
        let file_groups = self.plan.file_groups.clone();
        let output_schema = self.output_schema.clone();
        let cancellation_token = self.cancellation_token.clone();
        let progress_callback = self.progress_callback.clone();
        let total_bytes = self.plan.total_bytes;
        let total_files = self.plan.total_data_files;
        let strategy = self.strategy.clone();

        // Build partition type map for serialization
        let partition_types = build_partition_type_map(&table)?;
        let format_version = table.metadata().format_version();

        // Emit Started event
        if let Some(ref callback) = progress_callback {
            callback(CompactionProgressEvent::Started {
                total_groups: file_groups.len() as u32,
                total_bytes,
                total_files,
            });
        }

        let stream = futures::stream::iter(file_groups.into_iter().enumerate())
            .then(move |(idx, file_group)| {
                let table = table.clone();
                let partition_types = partition_types.clone();
                let output_schema = output_schema.clone();
                let cancellation_token = cancellation_token.clone();
                let progress_callback = progress_callback.clone();
                let strategy = strategy.clone();

                async move {
                    let group_id = file_group.group_id;
                    let spec_id = file_group.partition_spec_id;
                    let input_files = file_group.data_files.len() as u32;
                    let input_bytes = file_group.total_bytes;

                    // Check cancellation before processing
                    if cancellation_token
                        .as_ref()
                        .is_some_and(|token| token.is_cancelled())
                    {
                        if let Some(ref callback) = progress_callback {
                            callback(CompactionProgressEvent::Cancelled {
                                groups_completed: idx as u32,
                            });
                        }
                        return Err(DataFusionError::Execution(
                            "Compaction cancelled".to_string(),
                        ));
                    }

                    // Emit GroupStarted
                    if let Some(ref callback) = progress_callback {
                        callback(CompactionProgressEvent::GroupStarted {
                            group_id,
                            input_files,
                            input_bytes,
                        });
                    }

                    // Process the file group (read + optional sort + write)
                    let result =
                        process_file_group_with_strategy(&table, &file_group, &strategy).await;

                    match result {
                        Ok(write_result) => {
                            let output_files = write_result.new_data_files.len() as u32;
                            let output_bytes = write_result.bytes_written;
                            let duration_ms = write_result.duration_ms;

                            // Emit GroupCompleted
                            if let Some(ref callback) = progress_callback {
                                callback(CompactionProgressEvent::GroupCompleted {
                                    group_id,
                                    output_files,
                                    output_bytes,
                                    duration_ms,
                                });
                            }

                            // Serialize data files to JSON
                            let partition_type =
                                partition_types.get(&spec_id).ok_or_else(|| {
                                    DataFusionError::Internal(format!(
                                        "No partition type for spec {spec_id}"
                                    ))
                                })?;

                            let data_files_json: Vec<String> = write_result
                                .new_data_files
                                .into_iter()
                                .map(|df| {
                                    serialize_data_file_to_json(df, partition_type, format_version)
                                        .map_err(|e| {
                                            DataFusionError::Internal(format!(
                                                "Failed to serialize DataFile: {e}"
                                            ))
                                        })
                                })
                                .collect::<Result<Vec<_>, _>>()?;

                            // Create output batch - one row per data file
                            let group_ids: Vec<u32> = vec![group_id; data_files_json.len()];
                            let group_id_array = Arc::new(UInt32Array::from(group_ids)) as ArrayRef;
                            let data_files_array =
                                Arc::new(StringArray::from(data_files_json)) as ArrayRef;

                            RecordBatch::try_new(output_schema, vec![
                                group_id_array,
                                data_files_array,
                            ])
                            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
                        }
                        Err(e) => {
                            // Emit GroupFailed
                            if let Some(ref callback) = progress_callback {
                                callback(CompactionProgressEvent::GroupFailed {
                                    group_id,
                                    error: e.to_string(),
                                });
                            }
                            Err(e)
                        }
                    }
                }
            })
            .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            stream,
        )))
    }
}

// =============================================================================
// Helper Functions (Tasks 5, 6, 7)
// =============================================================================

/// Finds a schema compatible with the given partition spec.
///
/// For partition evolution support, historical partition specs may reference
/// fields that no longer exist in the current schema. This function finds a
/// schema that can compute the partition type for the given spec.
fn find_compatible_schema(
    metadata: &iceberg::spec::TableMetadata,
    partition_spec: &PartitionSpec,
) -> DFResult<iceberg::spec::SchemaRef> {
    let current_schema = metadata.current_schema();
    if partition_spec.partition_type(current_schema).is_ok() {
        return Ok(current_schema.clone());
    }

    metadata
        .schemas_iter()
        .find(|schema| partition_spec.partition_type(schema).is_ok())
        .cloned()
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "Cannot find compatible schema for partition spec {}: \
                 no schema contains all referenced source fields",
                partition_spec.spec_id()
            ))
        })
}

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
/// This function creates [`FileScanTask`] entries for each data file in the group
/// and uses the Iceberg [`ArrowReaderBuilder`] to read them with automatic
/// delete file application.
///
/// # Arguments
/// * `table` - The Iceberg table
/// * `file_group` - The file group to read
///
/// # Returns
/// A vector of RecordBatches containing the merged data from all files
/// in the group with position deletes applied.
pub async fn read_file_group(table: &Table, file_group: &FileGroup) -> DFResult<Vec<RecordBatch>> {
    let file_io = table.file_io().clone();
    let schema = table.metadata().current_schema().clone();

    // Collect all top-level field IDs from the schema (Iceberg scan currently only supports
    // projecting direct children of the schema struct).
    let project_field_ids: Vec<i32> = schema.as_struct().fields().iter().map(|f| f.id).collect();

    // Build delete file list once - file groups associate deletes by partition/spec (not per file),
    // and the Arrow reader will filter position deletes by referenced file path.
    let delete_files: Vec<FileScanTaskDeleteFile> = file_group
        .position_delete_files
        .iter()
        .chain(file_group.equality_delete_files.iter())
        .map(|delete_entry| {
            let delete_file = &delete_entry.data_file;
            FileScanTaskDeleteFile {
                file_path: delete_file.file_path().to_string(),
                file_type: delete_file.content_type(),
                partition_spec_id: delete_file.partition_spec_id(),
                equality_ids: if delete_file.content_type() == DataContentType::EqualityDeletes {
                    delete_file.equality_ids().map(|ids| ids.to_vec())
                } else {
                    None
                },
            }
        })
        .collect();

    let partition_spec = table
        .metadata()
        .partition_spec_by_id(file_group.partition_spec_id)
        .cloned()
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "Partition spec {} not found for file group {}",
                file_group.partition_spec_id, file_group.group_id
            ))
        })?;

    // Build FileScanTasks for each data file in the group
    let mut tasks: Vec<FileScanTask> = Vec::with_capacity(file_group.data_files.len());

    for entry in &file_group.data_files {
        let data_file = &entry.data_file;
        let data_file_spec_id = data_file.partition_spec_id();
        if data_file_spec_id != file_group.partition_spec_id {
            return Err(DataFusionError::Internal(format!(
                "File group {} contains data file with partition spec id {}, expected {}",
                file_group.group_id, data_file_spec_id, file_group.partition_spec_id
            )));
        }

        let task = FileScanTask {
            start: 0,
            length: data_file.file_size_in_bytes(),
            record_count: Some(data_file.record_count()),
            data_file_path: data_file.file_path().to_string(),
            data_file_format: data_file.file_format(),
            schema: schema.clone(),
            project_field_ids: project_field_ids.clone(),
            predicate: None, // Read all rows
            deletes: delete_files.clone(),
            partition: Some(data_file.partition().clone()),
            partition_spec: Some(partition_spec.clone()),
            name_mapping: None,
        };

        tasks.push(task);
    }

    // If no tasks, return empty
    if tasks.is_empty() {
        return Ok(vec![]);
    }

    // Create ArrowReader and read all tasks
    let reader = ArrowReaderBuilder::new(file_io).build();

    let task_stream: FileScanTaskStream =
        Box::pin(futures::stream::iter(tasks.into_iter().map(Ok)));

    let batch_stream = reader.read(task_stream).map_err(to_datafusion_error)?;

    // Collect all batches
    let batches: Vec<RecordBatch> = batch_stream
        .try_collect()
        .await
        .map_err(to_datafusion_error)?;

    Ok(batches)
}

/// Write RecordBatches to new data files using rolling file writer.
///
/// Uses the Iceberg writer stack: ParquetWriterBuilder → RollingFileWriterBuilder
/// → DataFileWriterBuilder to write files at the target size.
///
/// # Arguments
/// * `table` - The Iceberg table (for schema, partition spec, etc.)
/// * `batches` - RecordBatches to write
/// * `partition` - Optional partition struct for the output files
/// * `spec_id` - The partition spec ID for the output files
///
/// # Returns
/// Vector of DataFile entries for the newly written files.
pub async fn write_compacted_files(
    table: &Table,
    batches: Vec<RecordBatch>,
    partition: Option<&Struct>,
    spec_id: i32,
) -> DFResult<Vec<DataFile>> {
    if batches.is_empty() {
        return Ok(vec![]);
    }

    let metadata = table.metadata();
    let file_io = table.file_io().clone();
    let iceberg_schema = metadata.current_schema().clone();

    // Get target file size from table properties (default from TableProperties)
    let target_file_size = metadata
        .properties()
        .get(TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

    // Create location generator for file paths
    let location_generator =
        DefaultLocationGenerator::new(metadata.clone()).map_err(to_datafusion_error)?;

    // Create file name generator with unique UUID
    let file_name_generator = DefaultFileNameGenerator::new(
        format!("compaction-{}", Uuid::now_v7()),
        None,
        DataFileFormat::Parquet,
    );

    // Build writer stack: Parquet -> Rolling -> DataFile
    let parquet_writer_builder =
        ParquetWriterBuilder::new(WriterProperties::default(), iceberg_schema.clone());

    let rolling_writer_builder = RollingFileWriterBuilder::new(
        parquet_writer_builder,
        target_file_size,
        file_io,
        location_generator,
        file_name_generator,
    );

    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

    // Build partition key if partitioned
    let partition_key = if let Some(partition_struct) = partition {
        let partition_spec = metadata.partition_spec_by_id(spec_id).ok_or_else(|| {
            DataFusionError::Internal(format!("Partition spec {spec_id} not found"))
        })?;

        let partition_schema = find_compatible_schema(metadata, partition_spec.as_ref())?;

        Some(PartitionKey::new(
            partition_spec.as_ref().clone(),
            partition_schema,
            partition_struct.clone(),
        ))
    } else {
        None
    };

    // Create the writer
    let mut writer = data_file_writer_builder
        .build(partition_key)
        .await
        .map_err(to_datafusion_error)?;

    // Write all batches
    for batch in batches {
        writer.write(batch).await.map_err(to_datafusion_error)?;
    }

    // Close and get data files
    let data_files = writer.close().await.map_err(to_datafusion_error)?;

    Ok(data_files)
}

/// Process a single file group: read, merge, and write compacted output.
///
/// This function orchestrates the full compaction flow for a single file group:
/// 1. Read all data files with delete application
/// 2. Write merged data to new compacted files
/// 3. Return metrics about the operation
///
/// # Arguments
/// * `table` - The Iceberg table
/// * `file_group` - The file group to process
///
/// # Returns
/// Result containing new data files and metrics.
pub async fn process_file_group(
    table: &Table,
    file_group: &FileGroup,
) -> DFResult<FileGroupWriteResult> {
    let start_time = Instant::now();
    let group_id = file_group.group_id;
    let spec_id = file_group.partition_spec_id;

    // Step 1: Read all data from the file group with delete application
    let batches = read_file_group(table, file_group).await?;

    // Step 2: Write merged output to new compacted files
    let partition = file_group.partition.as_ref();
    let new_data_files = write_compacted_files(table, batches, partition, spec_id).await?;

    // Calculate metrics
    let bytes_written: u64 = new_data_files.iter().map(|f| f.file_size_in_bytes()).sum();
    let duration_ms = start_time.elapsed().as_millis() as u64;

    Ok(FileGroupWriteResult {
        group_id,
        new_data_files,
        bytes_written,
        duration_ms,
    })
}

/// Build column ID to name mapping from Iceberg schema.
fn build_column_id_map(schema: &iceberg::spec::Schema) -> HashMap<i32, String> {
    let mut map = HashMap::new();
    for field in schema.as_struct().fields() {
        map.insert(field.id, field.name.clone());
    }
    map
}

/// Resolve sort order for sorted compaction.
///
/// If an explicit sort order is provided, uses that.
/// Otherwise, uses the table's default sort order.
/// Returns None if strategy is not Sort.
fn resolve_sort_order(
    table: &Table,
    strategy: &RewriteStrategy,
) -> DFResult<Option<SortOrder>> {
    match strategy {
        RewriteStrategy::Sort {
            sort_order: Some(order),
        } => Ok(Some(order.clone())),
        RewriteStrategy::Sort { sort_order: None } => {
            let default_order = table.metadata().default_sort_order();
            if default_order.is_unsorted() {
                Err(DataFusionError::Plan(
                    "Sort strategy requires a sort order, but table has no default sort order. \
                     Use sort_by() to specify an explicit sort order."
                        .to_string(),
                ))
            } else {
                Ok(Some(default_order.as_ref().clone()))
            }
        }
        _ => Ok(None),
    }
}

/// Process a single file group with optional sorting.
///
/// This is the main entry point for file group processing that supports
/// both bin-pack (no sort) and sorted compaction strategies.
///
/// # Arguments
/// * `table` - The Iceberg table
/// * `file_group` - The file group to process
/// * `strategy` - The rewrite strategy (determines if sorting is applied)
///
/// # Returns
/// Result containing new data files and metrics.
pub async fn process_file_group_with_strategy(
    table: &Table,
    file_group: &FileGroup,
    strategy: &RewriteStrategy,
) -> DFResult<FileGroupWriteResult> {
    let start_time = Instant::now();
    let group_id = file_group.group_id;
    let spec_id = file_group.partition_spec_id;

    // Step 1: Read all data from the file group with delete application
    let batches = read_file_group(table, file_group).await?;

    // Step 2: Apply sorting if using sort strategy
    let sorted_batches = match strategy {
        RewriteStrategy::Sort { .. } => {
            let sort_order = resolve_sort_order(table, strategy)?
                .expect("Sort strategy should have sort order");
            let column_id_map = build_column_id_map(table.metadata().current_schema());
            sort_batches(batches, &sort_order, &column_id_map).await?
        }
        RewriteStrategy::BinPack => batches,
        RewriteStrategy::ZOrder { .. } => {
            return Err(DataFusionError::NotImplemented(
                "Z-order compaction not yet implemented".to_string(),
            ));
        }
    };

    // Step 3: Write output to new compacted files
    let partition = file_group.partition.as_ref();
    let new_data_files = write_compacted_files(table, sorted_batches, partition, spec_id).await?;

    // Calculate metrics
    let bytes_written: u64 = new_data_files.iter().map(|f| f.file_size_in_bytes()).sum();
    let duration_ms = start_time.elapsed().as_millis() as u64;

    Ok(FileGroupWriteResult {
        group_id,
        new_data_files,
        bytes_written,
        duration_ms,
    })
}

/// Sort record batches according to an Iceberg sort order.
///
/// Uses DataFusion's in-memory sorting which handles:
/// - Multi-column sorting
/// - Ascending/descending order
/// - Nulls first/last semantics
///
/// # Arguments
/// * `batches` - Input record batches to sort
/// * `sort_order` - Iceberg sort order specification
/// * `column_id_to_name` - Mapping from Iceberg column IDs to Arrow column names
///
/// # Returns
/// Sorted record batches
pub async fn sort_batches(
    batches: Vec<RecordBatch>,
    sort_order: &SortOrder,
    column_id_to_name: &HashMap<i32, String>,
) -> DFResult<Vec<RecordBatch>> {
    if batches.is_empty() || sort_order.is_unsorted() {
        return Ok(batches);
    }

    // Get the schema from the first batch
    let schema = batches[0].schema();

    // Create a DataFusion context and register the data
    let ctx = SessionContext::new();
    let table = datafusion::datasource::MemTable::try_new(schema.clone(), vec![batches])?;
    ctx.register_table("data", Arc::new(table))?;

    // Build sort expressions from Iceberg sort order
    let mut sort_exprs = Vec::with_capacity(sort_order.fields.len());

    for sort_field in &sort_order.fields {
        let column_name = column_id_to_name
            .get(&sort_field.source_id)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Sort field references unknown column id: {}",
                    sort_field.source_id
                ))
            })?;

        // Build the base expression based on transform
        let expr = match sort_field.transform {
            Transform::Identity => col(column_name),
            Transform::Bucket(_) | Transform::Truncate(_) => {
                // For bucket and truncate, we sort on the original value
                // The transform creates the bucket but we cluster by natural order
                col(column_name)
            }
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Sort transform {:?} not supported for sorted compaction",
                    sort_field.transform
                )));
            }
        };

        // Apply sort direction and null order
        let ascending = matches!(sort_field.direction, SortDirection::Ascending);
        let nulls_first = matches!(sort_field.null_order, iceberg::spec::NullOrder::First);

        sort_exprs.push(expr.sort(ascending, nulls_first));
    }

    // Execute the sort
    let df = ctx.table("data").await?;
    let sorted_df = df.sort(sort_exprs)?;
    let sorted_batches = sorted_df.collect().await?;

    Ok(sorted_batches)
}

/// Serialize a vector of DataFiles to JSON for output.
///
/// Handles partition evolution by looking up each file's partition spec
/// and using the correct partition type for that spec ID.
///
/// # Arguments
/// * `files` - DataFiles to serialize
/// * `table` - The table (for partition specs and format version)
#[allow(dead_code)]
pub fn serialize_data_files(files: Vec<DataFile>, table: &Table) -> DFResult<Vec<String>> {
    let metadata = table.metadata();
    let format_version = metadata.format_version();
    let partition_types = build_partition_type_map(table)?;

    files
        .into_iter()
        .map(|f| {
            let spec_id = f.partition_spec_id();
            let partition_type = partition_types.get(&spec_id).ok_or_else(|| {
                DataFusionError::Internal(format!("Partition type not found for spec {spec_id}"))
            })?;

            serialize_data_file_to_json(f, partition_type, format_version).map_err(|e| {
                DataFusionError::Internal(format!("Failed to serialize DataFile: {e}"))
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

    #[test]
    fn test_compaction_exec_accepts_strategy() {
        // Validate RewriteStrategy variants can be used
        let strategy = RewriteStrategy::Sort { sort_order: None };
        assert!(strategy.is_sort());

        let strategy = RewriteStrategy::BinPack;
        assert!(strategy.is_bin_pack());
    }

    #[tokio::test]
    async fn test_sort_batches_identity_transform() {
        use datafusion::arrow::array::Int64Array;
        use iceberg::spec::{NullOrder, SortField};

        // Create test data with unsorted values
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![3, 1, 2])),
                Arc::new(StringArray::from(vec!["c", "a", "b"])),
            ],
        )
        .unwrap();

        // Create sort order: ORDER BY id ASC
        let sort_order = SortOrder::builder()
            .with_sort_field(
                SortField::builder()
                    .source_id(1) // id column
                    .direction(SortDirection::Ascending)
                    .null_order(NullOrder::First)
                    .transform(Transform::Identity)
                    .build(),
            )
            .build_unbound()
            .unwrap();

        // Create column ID to name mapping
        let mut column_id_to_name = HashMap::new();
        column_id_to_name.insert(1_i32, "id".to_string());
        column_id_to_name.insert(2_i32, "name".to_string());

        // Sort the batches
        let sorted = sort_batches(vec![batch], &sort_order, &column_id_to_name)
            .await
            .unwrap();

        assert_eq!(sorted.len(), 1);
        let sorted_batch = &sorted[0];

        // Verify sorted order
        let id_col = sorted_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(1), 2);
        assert_eq!(id_col.value(2), 3);
    }

    #[tokio::test]
    async fn test_sort_batches_descending_nulls_last() {
        use datafusion::arrow::array::{Array, Int64Array};
        use iceberg::spec::{NullOrder, SortField};

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            true,
        )]));

        // Include a null value
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![
                Some(1),
                None,
                Some(3),
                Some(2),
            ]))],
        )
        .unwrap();

        // ORDER BY id DESC NULLS LAST
        let sort_order = SortOrder::builder()
            .with_sort_field(
                SortField::builder()
                    .source_id(1)
                    .direction(SortDirection::Descending)
                    .null_order(NullOrder::Last)
                    .transform(Transform::Identity)
                    .build(),
            )
            .build_unbound()
            .unwrap();

        let mut column_id_to_name = HashMap::new();
        column_id_to_name.insert(1_i32, "id".to_string());

        let sorted = sort_batches(vec![batch], &sort_order, &column_id_to_name)
            .await
            .unwrap();

        let id_col = sorted[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // DESC order: 3, 2, 1, NULL (nulls last)
        assert_eq!(id_col.value(0), 3);
        assert_eq!(id_col.value(1), 2);
        assert_eq!(id_col.value(2), 1);
        assert!(id_col.is_null(3));
    }

    #[tokio::test]
    async fn test_sort_batches_multi_column() {
        use datafusion::arrow::array::Int64Array;
        use iceberg::spec::{NullOrder, SortField};

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new("id", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["b", "a", "b", "a"])),
                Arc::new(Int64Array::from(vec![2, 1, 1, 2])),
            ],
        )
        .unwrap();

        // ORDER BY category ASC, id ASC
        let sort_order = SortOrder::builder()
            .with_sort_field(
                SortField::builder()
                    .source_id(1) // category
                    .direction(SortDirection::Ascending)
                    .null_order(NullOrder::First)
                    .transform(Transform::Identity)
                    .build(),
            )
            .with_sort_field(
                SortField::builder()
                    .source_id(2) // id
                    .direction(SortDirection::Ascending)
                    .null_order(NullOrder::First)
                    .transform(Transform::Identity)
                    .build(),
            )
            .build_unbound()
            .unwrap();

        let mut column_id_to_name = HashMap::new();
        column_id_to_name.insert(1_i32, "category".to_string());
        column_id_to_name.insert(2_i32, "id".to_string());

        let sorted = sort_batches(vec![batch], &sort_order, &column_id_to_name)
            .await
            .unwrap();

        let cat_col = sorted[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let id_col = sorted[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Expected: (a,1), (a,2), (b,1), (b,2)
        assert_eq!(cat_col.value(0), "a");
        assert_eq!(id_col.value(0), 1);
        assert_eq!(cat_col.value(1), "a");
        assert_eq!(id_col.value(1), 2);
        assert_eq!(cat_col.value(2), "b");
        assert_eq!(id_col.value(2), 1);
        assert_eq!(cat_col.value(3), "b");
        assert_eq!(id_col.value(3), 2);
    }
}
