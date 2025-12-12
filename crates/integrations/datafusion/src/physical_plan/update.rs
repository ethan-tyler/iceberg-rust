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

//! Execution plan for UPDATE operations on Iceberg tables.
//!
//! This module provides `IcebergUpdateExec`, which implements Copy-on-Write (CoW)
//! UPDATE semantics by:
//! 1. Scanning for rows matching the WHERE predicate
//! 2. Applying SET expressions to transform matching rows
//! 3. Writing new data files (with updated rows) and position delete files (for original rows)
//!
//! The output is serialized JSON for both data files and delete files, which are
//! then committed atomically by `IcebergUpdateCommitExec`.

use std::any::Any;
use std::collections::HashMap;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray,
};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::{
    DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use datafusion::common::{DFSchema, DataFusionError};
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::planner::create_physical_expr;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::prelude::{Expr, SessionContext};
use futures::{StreamExt, TryStreamExt};
use iceberg::arrow::RecordBatchPartitionSplitter;
use iceberg::expr::Predicate;
use iceberg::scan::FileScanTask;
use iceberg::spec::{
    DataFile, DataFileFormat, PartitionKey, PartitionSpec, Struct, TableProperties,
    serialize_data_file_to_json,
};
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
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use super::expr_to_predicate::convert_filters_to_predicate;
use crate::partition_utils::{
    SerializedFileWithSpec, build_file_partition_map, build_partition_type_map,
};
use crate::to_datafusion_error;

/// Column name for serialized data files in output
pub const UPDATE_DATA_FILES_COL: &str = "data_files";

// Type aliases
type IcebergSchemaRef = Arc<iceberg::spec::Schema>;

// Type aliases for the writers
type DataFileWriterType = iceberg::writer::base_writer::data_file_writer::DataFileWriter<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
>;

type DataFileWriterBuilderType =
    DataFileWriterBuilder<ParquetWriterBuilder, DefaultLocationGenerator, DefaultFileNameGenerator>;

type PositionDeleteWriterBuilderType = PositionDeleteFileWriterBuilder<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
>;

/// Data file writer that supports both partitioned and unpartitioned tables.
enum UpdateDataWriter {
    Unpartitioned(Box<DataFileWriterType>),
    Partitioned(Box<FanoutWriter<DataFileWriterBuilderType>>),
}

/// Position delete writer - always uses FanoutWriter for partition evolution support.
/// Historical files may have different partition specs than the current default,
/// so we always route deletes by their actual partition info.
type UpdateDeleteWriter = FanoutWriter<PositionDeleteWriterBuilderType>;

type PhysicalAssignments = HashMap<String, Arc<dyn PhysicalExpr>>;
type PhysicalExpressions = (Option<Arc<dyn PhysicalExpr>>, PhysicalAssignments);

/// Column name for serialized delete files in output
pub const UPDATE_DELETE_FILES_COL: &str = "delete_files";
/// Column name for update count in output
pub const UPDATE_COUNT_COL: &str = "count";

/// Finds a schema compatible with the given partition spec.
///
/// For partition evolution support, historical partition specs may reference
/// fields that no longer exist in the current schema. This function finds a
/// schema that can compute the partition type for the given spec.
fn find_compatible_schema(
    all_schemas: &[IcebergSchemaRef],
    partition_spec: &PartitionSpec,
) -> DFResult<IcebergSchemaRef> {
    for schema in all_schemas {
        if partition_spec.partition_type(schema).is_ok() {
            return Ok(schema.clone());
        }
    }
    Err(DataFusionError::Internal(format!(
        "Cannot find compatible schema for partition spec {}: \
         no schema contains all referenced source fields",
        partition_spec.spec_id()
    )))
}

/// Execution plan for UPDATE operations using Copy-on-Write semantics.
///
/// This plan:
/// 1. Scans the table for rows matching the WHERE predicate
/// 2. Applies SET expressions to transform matching rows
/// 3. Writes new data files (transformed rows) and position delete files (original row markers)
///
/// Output schema: (data_files: Utf8, delete_files: Utf8, count: UInt64)
#[derive(Debug, Clone)]
pub struct IcebergUpdateExec {
    /// The table to update
    table: Table,
    /// Arrow schema of the table
    schema: ArrowSchemaRef,
    /// SET assignments: (column_name, expression)
    assignments: Vec<(String, Expr)>,
    /// WHERE predicate for file/manifest pruning
    predicate: Option<Predicate>,
    /// All DataFusion filter expressions (for row-level evaluation)
    filter_exprs: Vec<Expr>,
    /// Output schema for this execution plan
    output_schema: ArrowSchemaRef,
    /// Plan properties for query optimization
    plan_properties: PlanProperties,
}

impl IcebergUpdateExec {
    /// Creates a new `IcebergUpdateExec`.
    ///
    /// # Arguments
    /// * `table` - The Iceberg table to update
    /// * `schema` - Arrow schema of the table
    /// * `assignments` - SET clause assignments as (column_name, expression) pairs
    /// * `filters` - WHERE clause filter expressions
    pub fn new(
        table: Table,
        schema: ArrowSchemaRef,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> Self {
        let output_schema = Self::make_output_schema();
        let plan_properties = Self::compute_properties(output_schema.clone());
        let predicate = convert_filters_to_predicate(&filters);

        Self {
            table,
            schema,
            assignments,
            predicate,
            filter_exprs: filters,
            output_schema,
            plan_properties,
        }
    }

    /// Creates the output schema for update results.
    /// Schema: (data_files: Utf8, delete_files: Utf8, count: UInt64)
    fn make_output_schema() -> ArrowSchemaRef {
        Arc::new(ArrowSchema::new(vec![
            Field::new(UPDATE_DATA_FILES_COL, DataType::Utf8, false),
            Field::new(UPDATE_DELETE_FILES_COL, DataType::Utf8, false),
            Field::new(UPDATE_COUNT_COL, DataType::UInt64, false),
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

    /// Creates a result batch with data files, delete files, and count.
    fn make_result_batch(
        data_files_json: Vec<String>,
        delete_files_json: Vec<String>,
        count: u64,
    ) -> DFResult<RecordBatch> {
        use datafusion::error::DataFusionError;

        // Serialize as newline-delimited JSON strings
        let data_files_str = data_files_json.join("\n");
        let delete_files_str = delete_files_json.join("\n");

        let data_files_array = Arc::new(StringArray::from(vec![data_files_str])) as ArrayRef;
        let delete_files_array = Arc::new(StringArray::from(vec![delete_files_str])) as ArrayRef;
        let count_array =
            Arc::new(datafusion::arrow::array::UInt64Array::from(vec![count])) as ArrayRef;

        RecordBatch::try_new(Self::make_output_schema(), vec![
            data_files_array,
            delete_files_array,
            count_array,
        ])
        .map_err(|e| {
            DataFusionError::ArrowError(
                Box::new(e),
                Some("Failed to make update result batch".to_string()),
            )
        })
    }
}

impl DisplayAs for IcebergUpdateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let filter_display = if self.filter_exprs.is_empty() {
            "ALL".to_string()
        } else {
            self.filter_exprs
                .iter()
                .map(|e| format!("{e}"))
                .collect::<Vec<_>>()
                .join(" AND ")
        };

        let assignments_display: Vec<String> = self
            .assignments
            .iter()
            .map(|(col, expr)| format!("{col} = {expr}"))
            .collect();

        match t {
            DisplayFormatType::Default | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "IcebergUpdateExec: table={}, set=[{}], where=[{}]",
                    self.table.identifier(),
                    assignments_display.join(", "),
                    filter_display
                )
            }
            DisplayFormatType::Verbose => {
                write!(
                    f,
                    "IcebergUpdateExec: table={}, set=[{}], where=[{}], schema={:?}",
                    self.table.identifier(),
                    assignments_display.join(", "),
                    filter_display,
                    self.schema
                )
            }
        }
    }
}

impl ExecutionPlan for IcebergUpdateExec {
    fn name(&self) -> &str {
        "IcebergUpdateExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![] // Leaf node - scans table directly
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let table = self.table.clone();
        let schema = self.schema.clone();
        let assignments = self.assignments.clone();
        let predicate = self.predicate.clone();
        let filter_exprs = self.filter_exprs.clone();
        let output_schema = self.output_schema.clone();

        let stream = futures::stream::once(async move {
            execute_update(table, schema, assignments, predicate, filter_exprs).await
        })
        .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            stream,
        )))
    }
}

/// Executes the UPDATE operation: scan, transform, and write.
async fn execute_update(
    table: Table,
    schema: ArrowSchemaRef,
    assignments: Vec<(String, Expr)>,
    predicate: Option<Predicate>,
    filter_exprs: Vec<Expr>,
) -> DFResult<RecordBatch> {
    // Build the scan - predicate is used for file-level pruning
    let mut scan_builder = table.scan().select_all();
    if let Some(pred) = predicate {
        scan_builder = scan_builder.with_filter(pred);
    }

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
        // No files to update
        return IcebergUpdateExec::make_result_batch(vec![], vec![], 0);
    }

    // Set up writers
    // Build partition type map for proper serialization
    let partition_types = build_partition_type_map(&table)?;
    let partition_spec = table.metadata().default_partition_spec().clone();
    let default_spec_id = partition_spec.spec_id();
    let iceberg_schema = table.metadata().current_schema().clone();
    // Collect all schemas for partition evolution schema compatibility
    let all_schemas: Vec<_> = table.metadata().schemas_iter().cloned().collect();
    let is_partitioned = !partition_spec.is_unpartitioned();
    let format_version = table.metadata().format_version();
    let file_io = table.file_io().clone();

    // Always build file partition map for partition-evolution-aware deletes.
    // Even if the default spec is unpartitioned, historical files may be partitioned.
    let file_partition_map = build_file_partition_map(&table).await?;

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
        format!("update-data-{}", Uuid::now_v7()),
        None,
        DataFileFormat::Parquet,
    );
    let parquet_writer =
        ParquetWriterBuilder::new(WriterProperties::default(), iceberg_schema.clone());
    let data_rolling_writer = RollingFileWriterBuilder::new(
        parquet_writer.clone(),
        target_file_size,
        file_io.clone(),
        location_generator.clone(),
        data_file_name_generator,
    );
    let data_file_builder = DataFileWriterBuilder::new(data_rolling_writer);

    // === Position delete file writer ===
    let delete_config = PositionDeleteWriterConfig::new();
    let delete_schema = delete_config.delete_schema().clone();

    // Build the Iceberg schema for position deletes
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
        format!("update-delete-{}", Uuid::now_v7()),
        None,
        DataFileFormat::Parquet,
    );
    let delete_rolling_writer = RollingFileWriterBuilder::new(
        delete_parquet_writer,
        target_file_size,
        file_io.clone(),
        location_generator,
        delete_file_name_generator,
    );
    let delete_file_builder =
        PositionDeleteFileWriterBuilder::new(delete_rolling_writer, delete_config.clone());

    // Create partition splitter if partitioned
    let partition_splitter = if is_partitioned {
        Some(
            RecordBatchPartitionSplitter::try_new_with_computed_values(
                iceberg_schema.clone(),
                partition_spec.clone(),
            )
            .map_err(to_datafusion_error)?,
        )
    } else {
        None
    };

    // Create writers based on partition status.
    // Data writer: based on default spec (new data goes to default partition layout)
    // Delete writer: always FanoutWriter (must handle files from any historical spec)
    let mut data_writer = if is_partitioned {
        UpdateDataWriter::Partitioned(Box::new(FanoutWriter::new(data_file_builder)))
    } else {
        UpdateDataWriter::Unpartitioned(Box::new(
            data_file_builder
                .build(None)
                .await
                .map_err(to_datafusion_error)?,
        ))
    };
    // Delete writer always uses FanoutWriter to handle partition evolution:
    // historical files may have different partition specs than the current default
    let mut delete_writer: UpdateDeleteWriter = FanoutWriter::new(delete_file_builder);

    let mut total_count = 0u64;

    // Process each file
    for task in file_scan_tasks {
        let file_path = task.data_file_path().to_string();

        // Get the file's partition info for delete routing.
        // Delete writer always uses FanoutWriter, so we always need a PartitionKey.
        // This supports partition evolution: files may have different specs than the current default.
        let file_partition_key = match file_partition_map.get(&file_path) {
            Some(info) if !info.partition.fields().is_empty() => {
                // File has partition data - use compatible schema for historical specs
                let compatible_schema = find_compatible_schema(&all_schemas, &info.partition_spec)?;
                PartitionKey::new(
                    (*info.partition_spec).clone(),
                    compatible_schema,
                    info.partition.clone(),
                )
            }
            Some(info) => {
                // File in map but with empty partition - use info's spec
                PartitionKey::new(
                    (*info.partition_spec).clone(),
                    iceberg_schema.clone(),
                    info.partition.clone(),
                )
            }
            None => {
                // File not in map - only valid if default spec is unpartitioned
                if is_partitioned {
                    return Err(DataFusionError::Internal(format!(
                        "No partition info found for file '{file_path}'. This may indicate a consistency issue \
                         between the scan and write phases."
                    )));
                }
                // Default spec is unpartitioned, so empty partition is correct
                PartitionKey::new(
                    (*partition_spec).clone(),
                    iceberg_schema.clone(),
                    Struct::empty(),
                )
            }
        };

        // Clear task predicate to read ALL rows for position tracking
        let mut task_without_predicate = task;
        task_without_predicate.predicate = None;

        // Read batches from this file - stream to prevent OOM
        let reader = iceberg::arrow::ArrowReaderBuilder::new(file_io.clone()).build();
        let task_stream = Box::pin(futures::stream::iter(vec![Ok(task_without_predicate)]))
            as iceberg::scan::FileScanTaskStream;
        let mut batch_stream = reader
            .read(task_stream)
            .map_err(to_datafusion_error)?
            .map(|r| r.map_err(to_datafusion_error));

        // Get first batch to extract schema for building physical expressions
        let first_batch = match batch_stream.next().await {
            Some(Ok(batch)) => batch,
            Some(Err(e)) => return Err(e),
            None => continue, // Empty file
        };

        let batch_schema = first_batch.schema();
        let (physical_filter, physical_assignments) =
            build_physical_expressions(&filter_exprs, &assignments, batch_schema.clone())?;

        // Process first batch
        let mut row_offset = 0i64;
        {
            let batch = first_batch;
            let batch_size = batch.num_rows();

            // Find matching rows
            let (matching_mask, positions) = if let Some(ref filter) = physical_filter {
                evaluate_filter_with_positions(filter, &batch, row_offset)?
            } else {
                // No filter - update all rows
                let all_true = BooleanArray::from(vec![true; batch_size]);
                let all_positions: Vec<i64> =
                    (row_offset..(row_offset + batch_size as i64)).collect();
                (all_true, all_positions)
            };

            if !positions.is_empty() {
                // Filter to only matching rows
                let matching_batch = filter_record_batch(&batch, &matching_mask)?;

                // Transform matching rows
                let transformed_batch =
                    transform_batch(&matching_batch, &physical_assignments, &schema)?;

                // Write transformed rows to data file (with partition awareness)
                write_data_batch(
                    &mut data_writer,
                    transformed_batch,
                    partition_splitter.as_ref(),
                )
                .await?;

                // Write position deletes for original rows (with partition awareness)
                let delete_batch =
                    make_position_delete_batch(&file_path, &positions, delete_schema.clone())?;
                write_delete_batch(&mut delete_writer, delete_batch, &file_partition_key).await?;

                total_count += positions.len() as u64;
            }

            row_offset += batch_size as i64;
        }

        // Stream remaining batches from file
        while let Some(batch_result) = batch_stream.next().await {
            let batch = batch_result?;
            let batch_size = batch.num_rows();

            // Find matching rows
            let (matching_mask, positions) = if let Some(ref filter) = physical_filter {
                evaluate_filter_with_positions(filter, &batch, row_offset)?
            } else {
                // No filter - update all rows
                let all_true = BooleanArray::from(vec![true; batch_size]);
                let all_positions: Vec<i64> =
                    (row_offset..(row_offset + batch_size as i64)).collect();
                (all_true, all_positions)
            };

            if !positions.is_empty() {
                // Filter to only matching rows
                let matching_batch = filter_record_batch(&batch, &matching_mask)?;

                // Transform matching rows
                let transformed_batch =
                    transform_batch(&matching_batch, &physical_assignments, &schema)?;

                // Write transformed rows to data file (with partition awareness)
                write_data_batch(
                    &mut data_writer,
                    transformed_batch,
                    partition_splitter.as_ref(),
                )
                .await?;

                // Write position deletes for original rows (with partition awareness)
                let delete_batch =
                    make_position_delete_batch(&file_path, &positions, delete_schema.clone())?;
                write_delete_batch(&mut delete_writer, delete_batch, &file_partition_key).await?;

                total_count += positions.len() as u64;
            }

            row_offset += batch_size as i64;
        }
    }

    // Close writers and collect files
    let data_files = close_data_writer(data_writer).await?;
    let delete_files = close_delete_writer(delete_writer).await?;

    // Serialize data files to JSON with partition spec wrapper.
    // Data files use the default partition spec (Migrate Forward semantic: new data uses current default).
    let default_partition_type = partition_types.get(&default_spec_id).ok_or_else(|| {
        DataFusionError::Internal(format!(
            "Missing partition type for default partition spec {default_spec_id}"
        ))
    })?;

    let data_files_json: Vec<String> = data_files
        .into_iter()
        .map(|df| {
            let file_json = serialize_data_file_to_json(df, default_partition_type, format_version)
                .map_err(to_datafusion_error)?;
            serde_json::to_string(&SerializedFileWithSpec {
                spec_id: default_spec_id,
                file_json,
            })
            .map_err(|e| {
                DataFusionError::Execution(format!("Failed to serialize data file metadata: {e}"))
            })
        })
        .collect::<DFResult<Vec<_>>>()?;

    // Serialize delete files to JSON with partition spec wrapper.
    // Delete files use the spec_id of the data file they reference (set by the writer).
    // This is required for partition evolution support: position deletes must use the
    // same partition spec as the data file being deleted, not the current default spec.
    let delete_files_json: Vec<String> = delete_files
        .into_iter()
        .map(|df| {
            let spec_id = df.partition_spec_id();
            let partition_type = partition_types.get(&spec_id).ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Missing partition type for partition spec {spec_id}"
                ))
            })?;
            let file_json = serialize_data_file_to_json(df, partition_type, format_version)
                .map_err(to_datafusion_error)?;
            serde_json::to_string(&SerializedFileWithSpec { spec_id, file_json }).map_err(|e| {
                DataFusionError::Execution(format!("Failed to serialize delete file metadata: {e}"))
            })
        })
        .collect::<DFResult<Vec<_>>>()?;

    IcebergUpdateExec::make_result_batch(data_files_json, delete_files_json, total_count)
}

/// Builds physical expressions for filter and SET assignments.
fn build_physical_expressions(
    filter_exprs: &[Expr],
    assignments: &[(String, Expr)],
    batch_schema: ArrowSchemaRef,
) -> DFResult<PhysicalExpressions> {
    let df_schema = DFSchema::try_from(batch_schema.as_ref().clone())?;
    let ctx = SessionContext::new();
    let execution_props = ctx.state().execution_props().clone();

    // Build physical filter
    let physical_filter = if filter_exprs.is_empty() {
        None
    } else {
        let combined = filter_exprs
            .iter()
            .cloned()
            .reduce(|a, b| a.and(b))
            .unwrap();
        Some(create_physical_expr(
            &combined,
            &df_schema,
            &execution_props,
        )?)
    };

    // Build physical assignment expressions
    // IMPORTANT: All expressions are built against the ORIGINAL schema
    // This correctly handles: SET a = b, b = a (both evaluated against original values)
    let mut physical_assignments = HashMap::new();
    for (column, expr) in assignments {
        let phys_expr = create_physical_expr(expr, &df_schema, &execution_props)?;
        physical_assignments.insert(column.clone(), phys_expr);
    }

    Ok((physical_filter, physical_assignments))
}

/// Evaluates a filter expression and returns the matching mask and positions.
fn evaluate_filter_with_positions(
    filter: &Arc<dyn PhysicalExpr>,
    batch: &RecordBatch,
    row_offset: i64,
) -> DFResult<(BooleanArray, Vec<i64>)> {
    use datafusion::error::DataFusionError;

    let batch_size = batch.num_rows();
    let result = filter.evaluate(batch)?;
    let mask = result
        .into_array(batch_size)?
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            DataFusionError::Internal("Filter did not return boolean array".to_string())
        })?
        .clone();

    // Collect positions where filter is true
    let positions: Vec<i64> = mask
        .iter()
        .enumerate()
        .filter_map(|(idx, value)| {
            if value.unwrap_or(false) {
                Some(row_offset + idx as i64)
            } else {
                None
            }
        })
        .collect();

    Ok((mask, positions))
}

/// Transforms a batch by applying SET assignments.
///
/// IMPORTANT: All expressions are evaluated against the ORIGINAL batch.
/// This correctly handles swap cases like: SET a = b, b = a
fn transform_batch(
    batch: &RecordBatch,
    physical_assignments: &HashMap<String, Arc<dyn PhysicalExpr>>,
    target_schema: &ArrowSchemaRef,
) -> DFResult<RecordBatch> {
    use datafusion::error::DataFusionError;

    let batch_size = batch.num_rows();

    // Evaluate ALL expressions against the ORIGINAL batch first
    let mut evaluated: HashMap<String, ArrayRef> = HashMap::new();
    for (column, expr) in physical_assignments {
        let result = expr.evaluate(batch)?;
        evaluated.insert(column.clone(), result.into_array(batch_size)?);
    }

    // Build the output batch using target schema
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(target_schema.fields().len());

    for field in target_schema.fields() {
        let column_name = field.name();

        if let Some(new_value) = evaluated.get(column_name) {
            // Use the evaluated SET expression
            columns.push(new_value.clone());
        } else {
            // Keep original column value
            let original_col = batch.column_by_name(column_name).ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Column '{column_name}' not found in source batch"
                ))
            })?;
            columns.push(original_col.clone());
        }
    }

    RecordBatch::try_new(target_schema.clone(), columns).map_err(|e| {
        DataFusionError::ArrowError(
            Box::new(e),
            Some("Failed to create transformed batch".to_string()),
        )
    })
}

/// Creates a position delete batch for the given file path and positions.
fn make_position_delete_batch(
    file_path: &str,
    positions: &[i64],
    schema: ArrowSchemaRef,
) -> DFResult<RecordBatch> {
    use datafusion::error::DataFusionError;

    let file_paths: Vec<&str> = vec![file_path; positions.len()];
    let file_path_array = Arc::new(StringArray::from(file_paths)) as ArrayRef;
    let pos_array = Arc::new(Int64Array::from(positions.to_vec())) as ArrayRef;

    RecordBatch::try_new(schema, vec![file_path_array, pos_array]).map_err(|e| {
        DataFusionError::ArrowError(
            Box::new(e),
            Some("Failed to create position delete batch".to_string()),
        )
    })
}

/// Writes a data batch to the appropriate writer, handling partitioned and unpartitioned cases.
async fn write_data_batch(
    writer: &mut UpdateDataWriter,
    batch: RecordBatch,
    splitter: Option<&RecordBatchPartitionSplitter>,
) -> DFResult<()> {
    match writer {
        UpdateDataWriter::Unpartitioned(w) => {
            w.write(batch).await.map_err(to_datafusion_error)?;
        }
        UpdateDataWriter::Partitioned(w) => {
            let splitter = splitter.ok_or_else(|| {
                DataFusionError::Internal(
                    "Partition splitter required for partitioned tables".to_string(),
                )
            })?;
            let partitioned_batches = splitter.split(&batch).map_err(to_datafusion_error)?;
            for (partition_key, batch) in partitioned_batches {
                w.write(partition_key, batch)
                    .await
                    .map_err(to_datafusion_error)?;
            }
        }
    }
    Ok(())
}

/// Writes a delete batch to the FanoutWriter.
/// Uses partition key routing to handle partition evolution (historical files may have different specs).
async fn write_delete_batch(
    writer: &mut UpdateDeleteWriter,
    batch: RecordBatch,
    partition_key: &PartitionKey,
) -> DFResult<()> {
    writer
        .write(partition_key.clone(), batch)
        .await
        .map_err(to_datafusion_error)?;
    Ok(())
}

/// Closes the data writer and returns all data files written.
async fn close_data_writer(writer: UpdateDataWriter) -> DFResult<Vec<DataFile>> {
    match writer {
        UpdateDataWriter::Unpartitioned(mut w) => w.close().await.map_err(to_datafusion_error),
        UpdateDataWriter::Partitioned(w) => w.close().await.map_err(to_datafusion_error),
    }
}

/// Closes the delete writer and returns all delete files written.
async fn close_delete_writer(writer: UpdateDeleteWriter) -> DFResult<Vec<DataFile>> {
    writer.close().await.map_err(to_datafusion_error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_output_schema() {
        let schema = IcebergUpdateExec::make_output_schema();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), UPDATE_DATA_FILES_COL);
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(1).name(), UPDATE_DELETE_FILES_COL);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(2).name(), UPDATE_COUNT_COL);
        assert_eq!(schema.field(2).data_type(), &DataType::UInt64);
    }

    #[test]
    fn test_make_result_batch() {
        let batch = IcebergUpdateExec::make_result_batch(
            vec!["data1.parquet".to_string(), "data2.parquet".to_string()],
            vec!["delete1.parquet".to_string()],
            42,
        )
        .unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 3);

        let data_files = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(data_files.value(0).contains("data1.parquet"));
        assert!(data_files.value(0).contains("data2.parquet"));

        let delete_files = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(delete_files.value(0).contains("delete1.parquet"));

        let count = batch
            .column(2)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .unwrap();
        assert_eq!(count.value(0), 42);
    }

    #[test]
    fn test_make_result_batch_empty() {
        let batch = IcebergUpdateExec::make_result_batch(vec![], vec![], 0).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 3);

        let count = batch
            .column(2)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .unwrap();
        assert_eq!(count.value(0), 0);
    }
}
