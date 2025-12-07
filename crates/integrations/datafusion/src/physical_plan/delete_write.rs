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
use std::fmt::{Debug, Formatter};
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
use iceberg::spec::{DataFileFormat, TableProperties, serialize_data_file_to_json};
use iceberg::table::Table;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::writer::base_writer::position_delete_writer::{
    PositionDeleteFileWriterBuilder, PositionDeleteWriterConfig,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use super::delete_scan::{DELETE_FILE_PATH_COL, DELETE_POS_COL};
use crate::to_datafusion_error;

/// Column name for serialized delete files in output
pub const DELETE_FILES_COL_NAME: &str = "delete_files";

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
        let table = self.table.clone();
        let format_version = table.metadata().format_version();
        // Build spec_id -> PartitionSpec lookup for partition evolution support.
        // Currently DELETE only supports unpartitioned tables, but this prepares
        // for future support and maintains consistency with UPDATE.
        let partition_specs: std::collections::HashMap<i32, std::sync::Arc<iceberg::spec::PartitionSpec>> =
            table.metadata().partition_specs_iter()
                .map(|spec| (spec.spec_id(), spec.clone()))
                .collect();
        let current_schema = table.metadata().current_schema().clone();
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
            // Check if table is partitioned - DELETE on partitioned tables not yet supported
            // TODO: For partitioned tables, we need to:
            //   1. Track which partition each data file belongs to
            //   2. Group position deletes by partition
            //   3. Write separate delete files per partition with correct partition values
            // See: https://iceberg.apache.org/spec/#position-delete-files
            let partition_spec = table.metadata().default_partition_spec();
            if !partition_spec.is_unpartitioned() {
                return Err(DataFusionError::NotImplemented(
                    "DELETE on partitioned tables is not yet supported. \
                     Position delete files must include partition values, which requires \
                     grouping deletes by partition. This will be added in a future release."
                        .to_string(),
                ));
            }

            // Set up position delete writer config
            let delete_config = PositionDeleteWriterConfig::new();
            let delete_schema = delete_config.delete_schema().clone();

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

            // Get partition value for partitioned tables
            // For position deletes, we use None (unpartitioned) since position deletes
            // reference rows across potentially different partitions via file_path.
            // The partition spec ID is recorded in the DataFile metadata.
            // TODO: For better locality, could group deletes by partition and write
            // separate delete files per partition.
            let mut delete_writer = delete_writer_builder
                .build(None)
                .await
                .map_err(to_datafusion_error)?;

            // Process input batches
            let mut input = input_stream;
            while let Some(batch_result) = input.next().await {
                let batch = batch_result?;

                if batch.num_rows() == 0 {
                    continue;
                }

                // Extract file_path and pos columns
                let file_path_col = batch
                    .column_by_name(DELETE_FILE_PATH_COL)
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Expected '{}' column in delete scan output",
                            DELETE_FILE_PATH_COL
                        ))
                    })?;

                let pos_col = batch.column_by_name(DELETE_POS_COL).ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "Expected '{}' column in delete scan output",
                        DELETE_POS_COL
                    ))
                })?;

                // Create a batch with just file_path and pos for the position delete writer
                let delete_batch = RecordBatch::try_new(
                    delete_schema.clone(),
                    vec![file_path_col.clone(), pos_col.clone()],
                )
                .map_err(|e| {
                    DataFusionError::ArrowError(
                        Box::new(e),
                        Some("Failed to create delete batch".to_string()),
                    )
                })?;

                delete_writer
                    .write(delete_batch)
                    .await
                    .map_err(to_datafusion_error)?;
            }

            // Close the writer and get the delete files
            let delete_files = delete_writer.close().await.map_err(to_datafusion_error)?;

            // Serialize delete files to JSON with per-file partition type lookup.
            // Each file may have been written with a different partition spec.
            //
            // Note: We derive partition_type using current_schema. This is correct because
            // Iceberg guarantees field IDs are stable across schema evolution.
            let delete_files_json: Vec<String> = delete_files
                .into_iter()
                .map(|df| {
                    let spec_id = df.partition_spec_id_or_default();
                    let ptype = partition_specs
                        .get(&spec_id)
                        .ok_or_else(|| {
                            iceberg::Error::new(
                                iceberg::ErrorKind::DataInvalid,
                                format!(
                                    "Partition spec {} not found in table metadata for delete file. \
                                     Available specs: {:?}.",
                                    spec_id,
                                    partition_specs.keys().collect::<Vec<_>>()
                                ),
                            )
                        })
                        .map_err(to_datafusion_error)?
                        .partition_type(&current_schema)
                        .map_err(to_datafusion_error)?;
                    serialize_data_file_to_json(df, &ptype, format_version)
                        .map_err(to_datafusion_error)
                })
                .collect::<DFResult<Vec<String>>>()?;

            Self::make_result_batch(delete_files_json)
        })
        .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(result_schema, stream)))
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
