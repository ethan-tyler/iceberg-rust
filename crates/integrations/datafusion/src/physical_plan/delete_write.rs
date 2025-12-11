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

//! Execution plan for writing position delete files.
//!
//! This module provides `IcebergDeleteWriteExec`, which takes (file_path, pos) tuples
//! from `IcebergDeleteScanExec` and writes them to position delete files using
//! the `PositionDeleteFileWriter`.

use std::any::Any;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{
    DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, execute_input_stream,
};
use futures::StreamExt;
use iceberg::spec::{
    DataFileFormat, SchemaRef as IcebergSchemaRef, Struct, TableProperties,
    serialize_data_file_to_json,
};
use iceberg::table::Table;
use iceberg::writer::base_writer::position_delete_writer::{
    PositionDeleteFileWriter, PositionDeleteFileWriterBuilder, PositionDeleteWriterConfig,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use super::delete_scan::{DELETE_FILE_PATH_COL, DELETE_POS_COL};
use crate::partition_utils::{
    FilePartitionInfo, SerializedFileWithSpec, build_file_partition_map, build_partition_type_map,
    reject_partition_evolution,
};
use crate::to_datafusion_error;

/// Column name for serialized delete files in output
pub const DELETE_FILES_COL_NAME: &str = "delete_files";

// ============================================================================
// Partition Grouping Types
// ============================================================================

/// Key for grouping position deletes by partition.
///
/// Position delete files in Iceberg must be associated with the same partition
/// as the data files they reference. This key combines the partition spec ID
/// (to handle partition evolution) with the partition values.
#[derive(Clone, Debug)]
struct PartitionGroupKey {
    /// The partition spec ID from the data file's manifest
    spec_id: i32,
    /// The partition values (struct of partition field values)
    partition: Struct,
}

impl PartitionGroupKey {
    fn new(spec_id: i32, partition: Struct) -> Self {
        Self { spec_id, partition }
    }

    /// Creates a key for unpartitioned tables.
    ///
    /// Uses the actual default partition spec ID rather than assuming 0,
    /// to correctly handle tables that have evolved from partitioned to unpartitioned.
    fn unpartitioned(default_spec_id: i32) -> Self {
        Self {
            spec_id: default_spec_id,
            partition: Struct::empty(),
        }
    }
}

impl PartialEq for PartitionGroupKey {
    fn eq(&self, other: &Self) -> bool {
        self.spec_id == other.spec_id && self.partition == other.partition
    }
}

impl Eq for PartitionGroupKey {}

impl Hash for PartitionGroupKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.spec_id.hash(state);
        self.partition.hash(state);
    }
}

/// Type alias for the position delete writer builder.
type DeleteWriterBuilder = PositionDeleteFileWriterBuilder<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
>;

/// Type alias for the position delete writer used in partitioned delete collection.
type PartitionDeleteWriter = PositionDeleteFileWriter<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
>;

/// Collects position deletes grouped by partition and manages writers.
///
/// This collector maintains a HashMap of partition keys to writers, creating
/// new writers on-demand as deletes for new partitions are encountered.
struct PartitionedDeleteCollector {
    /// Map from partition key to position delete writer
    writers: HashMap<PartitionGroupKey, PartitionDeleteWriter>,
    /// Map from file path to partition info (built from manifest scan)
    file_partitions: HashMap<String, FilePartitionInfo>,
    /// Writer builder factory closure
    writer_builder: DeleteWriterBuilder,
    /// Delete config for creating writers
    delete_config: PositionDeleteWriterConfig,
    /// Table schema for building partition keys
    table_schema: IcebergSchemaRef,
    /// Whether the table is unpartitioned
    is_unpartitioned: bool,
    /// The default partition spec ID (used for unpartitioned tables)
    default_spec_id: i32,
}

impl PartitionedDeleteCollector {
    /// Creates a new collector with the given file-to-partition mapping.
    fn new(
        file_partitions: HashMap<String, FilePartitionInfo>,
        writer_builder: DeleteWriterBuilder,
        delete_config: PositionDeleteWriterConfig,
        table_schema: IcebergSchemaRef,
        is_unpartitioned: bool,
        default_spec_id: i32,
    ) -> Self {
        Self {
            writers: HashMap::new(),
            file_partitions,
            writer_builder,
            delete_config,
            table_schema,
            is_unpartitioned,
            default_spec_id,
        }
    }

    /// Gets the partition key for a given file path.
    ///
    /// For unpartitioned tables, returns the unpartitioned key with the actual default spec ID.
    /// For partitioned tables, looks up the partition from the file mapping.
    fn get_partition_key(&self, file_path: &str) -> DFResult<PartitionGroupKey> {
        if self.is_unpartitioned {
            return Ok(PartitionGroupKey::unpartitioned(self.default_spec_id));
        }

        self.file_partitions
            .get(file_path)
            .map(|info| PartitionGroupKey::new(info.spec_id, info.partition.clone()))
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "No partition info found for file '{file_path}'. This may indicate a consistency issue \
                     between the scan and write phases."
                ))
            })
    }

    /// Gets or creates a writer for the given partition key.
    async fn get_or_create_writer(
        &mut self,
        key: &PartitionGroupKey,
    ) -> DFResult<&mut PartitionDeleteWriter> {
        use iceberg::spec::PartitionKey;

        if !self.writers.contains_key(key) {
            // Build partition key for the writer
            let partition_key = if self.is_unpartitioned {
                None
            } else {
                // Look up the partition spec from any file with this partition
                let file_info = self
                    .file_partitions
                    .values()
                    .find(|info| info.spec_id == key.spec_id && info.partition == key.partition)
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Cannot find partition spec for partition key {key:?}"
                        ))
                    })?;

                Some(PartitionKey::new(
                    file_info.partition_spec.as_ref().clone(),
                    self.table_schema.clone(),
                    key.partition.clone(),
                ))
            };

            let writer = self
                .writer_builder
                .clone()
                .build(partition_key)
                .await
                .map_err(to_datafusion_error)?;

            self.writers.insert(key.clone(), writer);
        }

        Ok(self.writers.get_mut(key).unwrap())
    }

    /// Writes a batch of deletes, routing each row to the appropriate partition writer.
    async fn write_batch(&mut self, batch: &RecordBatch) -> DFResult<()> {
        use datafusion::arrow::array::{Int64Array, StringArray as ArrowStringArray};

        if batch.num_rows() == 0 {
            return Ok(());
        }

        // Extract columns
        let file_path_col = batch
            .column_by_name(DELETE_FILE_PATH_COL)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Expected '{DELETE_FILE_PATH_COL}' column in delete scan output"
                ))
            })?
            .as_any()
            .downcast_ref::<ArrowStringArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("Expected StringArray for file_path column".to_string())
            })?;

        let pos_col = batch
            .column_by_name(DELETE_POS_COL)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Expected '{DELETE_POS_COL}' column in delete scan output"
                ))
            })?
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal("Expected Int64Array for pos column".to_string())
            })?;

        // Group rows by partition
        let delete_schema = self.delete_config.delete_schema().clone();
        let mut partition_batches: HashMap<PartitionGroupKey, (Vec<String>, Vec<i64>)> =
            HashMap::new();

        for row_idx in 0..batch.num_rows() {
            let file_path = file_path_col.value(row_idx);
            let pos = pos_col.value(row_idx);

            let key = self.get_partition_key(file_path)?;
            let entry = partition_batches
                .entry(key)
                .or_insert_with(|| (Vec::new(), Vec::new()));
            entry.0.push(file_path.to_string());
            entry.1.push(pos);
        }

        // Write to each partition's writer
        for (key, (file_paths, positions)) in partition_batches {
            let file_path_array =
                Arc::new(datafusion::arrow::array::StringArray::from(file_paths)) as ArrayRef;
            let pos_array =
                Arc::new(datafusion::arrow::array::Int64Array::from(positions)) as ArrayRef;

            let delete_batch =
                RecordBatch::try_new(delete_schema.clone(), vec![file_path_array, pos_array])
                    .map_err(|e| {
                        DataFusionError::ArrowError(
                            Box::new(e),
                            Some("Failed to create delete batch".to_string()),
                        )
                    })?;

            let writer = self.get_or_create_writer(&key).await?;
            writer
                .write(delete_batch)
                .await
                .map_err(to_datafusion_error)?;
        }

        Ok(())
    }

    /// Closes all writers and returns the generated delete files.
    async fn close(self) -> DFResult<Vec<(i32, iceberg::spec::DataFile)>> {
        let mut all_delete_files = Vec::new();

        for (key, mut writer) in self.writers {
            let delete_files = writer.close().await.map_err(to_datafusion_error)?;
            for df in delete_files {
                all_delete_files.push((key.spec_id, df));
            }
        }

        Ok(all_delete_files)
    }
}

/// Execution plan for writing position delete files.
///
/// Takes (file_path, pos) tuples from input and writes them to position delete files.
/// Outputs serialized DataFile JSON for the created delete files.
#[derive(Debug)]
pub struct IcebergDeleteWriteExec {
    table: Table,
    input: Arc<dyn ExecutionPlan>,
    result_schema: ArrowSchemaRef,
    plan_properties: PlanProperties,
}

use datafusion::physical_plan::PlanProperties;

impl IcebergDeleteWriteExec {
    /// Creates a new `IcebergDeleteWriteExec`.
    pub fn new(table: Table, input: Arc<dyn ExecutionPlan>) -> Self {
        let result_schema = Self::make_result_schema();
        let plan_properties = Self::compute_properties(&input, result_schema.clone());

        Self {
            table,
            input,
            result_schema,
            plan_properties,
        }
    }

    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        schema: ArrowSchemaRef,
    ) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            EmissionType::Final,
            Boundedness::Bounded,
        )
    }

    /// Creates the result schema: a single column with serialized delete file JSON.
    fn make_result_schema() -> ArrowSchemaRef {
        Arc::new(ArrowSchema::new(vec![Field::new(
            DELETE_FILES_COL_NAME,
            DataType::Utf8,
            false,
        )]))
    }

    /// Creates a result batch with serialized delete files.
    fn make_result_batch(delete_files_json: Vec<String>) -> DFResult<RecordBatch> {
        let files_array = Arc::new(StringArray::from(delete_files_json)) as ArrayRef;

        RecordBatch::try_new(Self::make_result_schema(), vec![files_array]).map_err(|e| {
            DataFusionError::ArrowError(
                Box::new(e),
                Some("Failed to make delete result batch".to_string()),
            )
        })
    }
}

impl DisplayAs for IcebergDeleteWriteExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "IcebergDeleteWriteExec: table={}",
                    self.table.identifier()
                )
            }
            DisplayFormatType::Verbose => {
                write!(
                    f,
                    "IcebergDeleteWriteExec: table={}, result_schema={:?}",
                    self.table.identifier(),
                    self.result_schema
                )
            }
        }
    }
}

impl ExecutionPlan for IcebergDeleteWriteExec {
    fn name(&self) -> &str {
        "IcebergDeleteWriteExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.children().len()]
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
            return Err(DataFusionError::Internal(format!(
                "IcebergDeleteWriteExec expects exactly one child, but provided {}",
                children.len()
            )));
        }

        Ok(Arc::new(Self::new(self.table.clone(), children[0].clone())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        // Guard: Reject tables with partition evolution before expensive operations
        reject_partition_evolution(&self.table, "DELETE")?;

        let table = self.table.clone();
        let partition_types = build_partition_type_map(&table)?;
        let format_version = table.metadata().format_version();
        let result_schema = self.result_schema.clone();

        // Get input stream of (file_path, pos) tuples
        let input_stream = execute_input_stream(
            Arc::clone(&self.input),
            self.input.schema(),
            partition,
            Arc::clone(&context),
        )?;

        // Create the write stream
        let stream = futures::stream::once(async move {
            let default_partition_spec = table.metadata().default_partition_spec();
            let is_unpartitioned = default_partition_spec.is_unpartitioned();
            let default_spec_id = default_partition_spec.spec_id();

            // Build file-to-partition mapping for partitioned tables
            let file_partitions = if is_unpartitioned {
                HashMap::new()
            } else {
                build_file_partition_map(&table).await?
            };

            // Set up position delete writer config
            let delete_config = PositionDeleteWriterConfig::new();

            let file_io = table.file_io().clone();
            let location_generator = DefaultLocationGenerator::new(table.metadata().clone())
                .map_err(to_datafusion_error)?;

            // Use "delete" prefix for delete files
            let file_name_generator = DefaultFileNameGenerator::new(
                format!("delete-{}", Uuid::now_v7()),
                None,
                DataFileFormat::Parquet,
            );

            let target_file_size = table
                .metadata()
                .properties()
                .get(TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES)
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

            // Get the table's current schema ID for the delete schema
            let current_schema_id = table.metadata().current_schema_id();

            // Create Iceberg schema for position deletes using table's schema ID
            let iceberg_delete_schema = Arc::new(
                iceberg::spec::Schema::builder()
                    .with_schema_id(current_schema_id)
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

            let parquet_writer_builder = ParquetWriterBuilder::new(
                WriterProperties::default(),
                iceberg_delete_schema,
            );

            let rolling_writer_builder = RollingFileWriterBuilder::new(
                parquet_writer_builder,
                target_file_size,
                file_io,
                location_generator,
                file_name_generator,
            );

            let delete_writer_builder =
                PositionDeleteFileWriterBuilder::new(rolling_writer_builder, delete_config.clone());

            // Create partitioned delete collector
            let table_schema = table.metadata().current_schema().clone();
            let mut collector = PartitionedDeleteCollector::new(
                file_partitions,
                delete_writer_builder,
                delete_config,
                table_schema,
                is_unpartitioned,
                default_spec_id,
            );

            // Process input batches
            let mut input = input_stream;
            while let Some(batch_result) = input.next().await {
                let batch = batch_result?;
                collector.write_batch(&batch).await?;
            }

            // Close all writers and get the delete files
            let delete_files = collector.close().await?;

            // Serialize delete files to JSON
            let delete_files_json: Vec<String> = delete_files
                .into_iter()
                .map(|(spec_id, df)| {
                    let partition_type = partition_types.get(&spec_id).ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Missing partition type for partition spec {spec_id}"
                        ))
                    })?;

                    let data_file_json =
                        serialize_data_file_to_json(df, partition_type, format_version)
                            .map_err(to_datafusion_error)?;

                    serde_json::to_string(&SerializedFileWithSpec {
                        spec_id,
                        file_json: data_file_json,
                    })
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to serialize delete file metadata for spec {spec_id}: {e}"
                        ))
                    })
                })
                .collect::<DFResult<Vec<String>>>()?;

            Self::make_result_batch(delete_files_json)
        })
        .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            result_schema,
            stream,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_result_schema() {
        let schema = IcebergDeleteWriteExec::make_result_schema();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), DELETE_FILES_COL_NAME);
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_make_result_batch() {
        let batch = IcebergDeleteWriteExec::make_result_batch(vec![
            r#"{"file_path": "delete1.parquet"}"#.to_string(),
            r#"{"file_path": "delete2.parquet"}"#.to_string(),
        ])
        .unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 1);

        let files = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(files.value(0).contains("delete1.parquet"));
        assert!(files.value(1).contains("delete2.parquet"));
    }
}
