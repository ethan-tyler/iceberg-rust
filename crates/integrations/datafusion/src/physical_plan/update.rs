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
//!
//! # Documentation
//!
//! See [`docs/datafusion-update/DESIGN.md`](../../../../../docs/datafusion-update/DESIGN.md)
//! for architecture diagrams and detailed technical documentation.
//!
//! # Partition Evolution
//!
//! UPDATE operations support partition evolution. Each data file carries its original
//! `partition_spec_id`, which is used to:
//! 1. Look up the correct partition type for data file serialization
//! 2. Construct the correct PartitionKey for position delete file writes
//! 3. Ensure position delete files inherit the spec_id from their source data file
//!
//! See [ADR-002](../../../../../docs/datafusion-update/adr/ADR-002-partition-evolution.md)
//! for the design rationale and implementation details.

use std::any::Any;
use std::collections::HashMap;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, RecordBatch, StringArray};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::{
    DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use datafusion::common::DFSchema;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::planner::create_physical_expr;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties};
use datafusion::prelude::{Expr, SessionContext};
use futures::{StreamExt, TryStreamExt};
use iceberg::expr::Predicate;
use iceberg::scan::FileScanTask;
use iceberg::spec::{DataFileFormat, PartitionKey, Struct, TableProperties, serialize_data_file_to_json};
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
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use super::expr_to_predicate::convert_filters_to_predicate;
use crate::position_delete_task_writer::PositionDeleteTaskWriter;
use crate::task_writer::TaskWriter;
use crate::to_datafusion_error;

/// Column name for serialized data files in output
pub const UPDATE_DATA_FILES_COL: &str = "data_files";
/// Column name for serialized delete files in output
pub const UPDATE_DELETE_FILES_COL: &str = "delete_files";
/// Column name for update count in output
pub const UPDATE_COUNT_COL: &str = "count";

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

        RecordBatch::try_new(
            Self::make_output_schema(),
            vec![data_files_array, delete_files_array, count_array],
        )
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
            .map(|(col, expr)| format!("{} = {}", col, expr))
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

        Ok(Box::pin(RecordBatchStreamAdapter::new(output_schema, stream)))
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
    let format_version = table.metadata().format_version();
    let file_io = table.file_io().clone();

    let target_file_size = table
        .metadata()
        .properties()
        .get(TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES)
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

    // Create data file task writer (supports partitioned tables)
    let location_generator =
        DefaultLocationGenerator::new(table.metadata().clone()).map_err(to_datafusion_error)?;
    let data_file_name_generator = DefaultFileNameGenerator::new(
        format!("update-data-{}", Uuid::now_v7()),
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

    // Use TaskWriter for partition-aware data file writing
    // Use computed partitions mode since UPDATE reads existing files without a _partition column
    let iceberg_schema = table.metadata().current_schema().clone();
    let data_partition_spec = table.metadata().default_partition_spec().clone();
    let fanout_enabled = true; // Use fanout for unsorted partition data
    let mut data_file_writer = TaskWriter::try_new_with_computed_partitions(
        data_file_builder,
        fanout_enabled,
        iceberg_schema,
        data_partition_spec,
    )
    .map_err(to_datafusion_error)?;

    // Create position delete task writer (supports partitioned tables)
    let delete_config = PositionDeleteWriterConfig::new();

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
        ParquetWriterBuilder::new(WriterProperties::default(), iceberg_delete_schema.clone());
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

    // Get default partition spec for constructing partition keys
    let default_partition_spec = table.metadata().default_partition_spec().clone();

    // Build spec_id -> PartitionSpec lookup for partition evolution support.
    // This allows us to serialize each file's partition data using its correct partition type,
    // even when the table has evolved partition specs.
    let partition_specs: std::collections::HashMap<i32, std::sync::Arc<iceberg::spec::PartitionSpec>> =
        table.metadata().partition_specs_iter()
            .map(|spec| (spec.spec_id(), spec.clone()))
            .collect();

    let mut delete_task_writer = PositionDeleteTaskWriter::try_new(
        delete_file_builder,
        iceberg_delete_schema,
        default_partition_spec.clone(),
    )
    .await
    .map_err(to_datafusion_error)?;

    let mut total_count = 0u64;

    // Get the current schema for PartitionKey construction
    let current_schema = table.metadata().current_schema().clone();

    // Process each file
    for task in file_scan_tasks {
        let file_path = task.data_file_path().to_string();

        // Construct partition key for this file using its original partition spec.
        // This is critical for partition evolution: position delete files must inherit
        // the spec_id from their source data file, not the table's current default spec.
        let partition_key = {
            let spec_id = task.partition_spec_id().unwrap_or(0);
            let file_spec = partition_specs.get(&spec_id).ok_or_else(|| {
                iceberg::Error::new(
                    iceberg::ErrorKind::DataInvalid,
                    format!(
                        "Partition spec {} not found for file '{}'. Available specs: {:?}. \
                         This may indicate table metadata corruption or an unsupported partition evolution scenario.",
                        spec_id,
                        task.data_file_path(),
                        partition_specs.keys().collect::<Vec<_>>()
                    ),
                )
            }).map_err(to_datafusion_error)?;

            if !file_spec.is_unpartitioned() {
                let partition_data = task.partition.clone().unwrap_or_else(Struct::empty);
                Some(PartitionKey::new(
                    (**file_spec).clone(),
                    current_schema.clone(),
                    partition_data,
                ))
            } else {
                None
            }
        };

        // Clear task predicate to read ALL rows for position tracking
        let mut task_without_predicate = task;
        task_without_predicate.predicate = None;

        // Read batches from this file - stream to prevent OOM
        let reader = iceberg::arrow::ArrowReaderBuilder::new(file_io.clone()).build();
        let task_stream =
            Box::pin(futures::stream::iter(vec![Ok(task_without_predicate)]))
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

                // Write transformed rows to data file
                data_file_writer
                    .write(transformed_batch)
                    .await
                    .map_err(to_datafusion_error)?;

                // Write position deletes for original rows
                delete_task_writer
                    .write(&file_path, &positions, partition_key.clone())
                    .await
                    .map_err(to_datafusion_error)?;

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

                // Write transformed rows to data file
                data_file_writer
                    .write(transformed_batch)
                    .await
                    .map_err(to_datafusion_error)?;

                // Write position deletes for original rows
                delete_task_writer
                    .write(&file_path, &positions, partition_key.clone())
                    .await
                    .map_err(to_datafusion_error)?;

                total_count += positions.len() as u64;
            }

            row_offset += batch_size as i64;
        }
    }

    // Close writers and collect files
    let data_files = data_file_writer
        .close()
        .await
        .map_err(to_datafusion_error)?;
    let delete_files = delete_task_writer
        .close()
        .await
        .map_err(to_datafusion_error)?;

    // Serialize to JSON with per-file partition type lookup for partition evolution support.
    // Each file may have been written with a different partition spec, so we look up the
    // correct partition type based on the file's partition_spec_id.
    //
    // Note: We derive partition_type using current_schema. This is correct because:
    // 1. Iceberg guarantees field IDs are stable across schema evolution
    // 2. Partition specs reference source_id which must exist in the current schema
    // 3. If a field was dropped from the schema but is still referenced by an old spec,
    //    partition_type() will return an error (which is the correct behavior)
    let data_files_json: Vec<String> = data_files
        .into_iter()
        .map(|df| {
            let spec_id = df.partition_spec_id_or_default();
            let ptype = partition_specs
                .get(&spec_id)
                .ok_or_else(|| {
                    iceberg::Error::new(
                        iceberg::ErrorKind::DataInvalid,
                        format!(
                            "Partition spec {} not found in table metadata. \
                             Available specs: {:?}. This may indicate corrupted \
                             table metadata or a file written with an incompatible version.",
                            spec_id,
                            partition_specs.keys().collect::<Vec<_>>()
                        ),
                    )
                })?
                .partition_type(&current_schema)?;
            serialize_data_file_to_json(df, &ptype, format_version)
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(to_datafusion_error)?;

    // Delete files inherit partition spec from their source data file.
    // The position delete writer sets the correct partition_spec_id on each delete file.
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
                })?
                .partition_type(&current_schema)?;
            serialize_data_file_to_json(df, &ptype, format_version)
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(to_datafusion_error)?;

    IcebergUpdateExec::make_result_batch(data_files_json, delete_files_json, total_count)
}

/// Builds physical expressions for filter and SET assignments.
fn build_physical_expressions(
    filter_exprs: &[Expr],
    assignments: &[(String, Expr)],
    batch_schema: ArrowSchemaRef,
) -> DFResult<(
    Option<Arc<dyn PhysicalExpr>>,
    HashMap<String, Arc<dyn PhysicalExpr>>,
)> {
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
        Some(create_physical_expr(&combined, &df_schema, &execution_props)?)
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
                    "Column '{}' not found in source batch",
                    column_name
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
