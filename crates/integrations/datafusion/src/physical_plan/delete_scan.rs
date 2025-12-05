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

//! Execution plan for scanning rows to be deleted from an Iceberg table.
//!
//! This module provides `IcebergDeleteScanExec`, which scans a table and identifies
//! rows matching a DELETE predicate, outputting (file_path, pos) tuples that can
//! be used to create position delete files.

use std::any::Any;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{
    DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::planner::create_physical_expr;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::common::DFSchema;
use datafusion::prelude::{Expr, SessionContext};
use futures::{Stream, StreamExt, TryStreamExt};
use iceberg::expr::Predicate;
use iceberg::scan::FileScanTask;
use iceberg::table::Table;

use super::expr_to_predicate::convert_filters_to_predicate;
use crate::to_datafusion_error;

/// Column name for file path in delete scan output
pub const DELETE_FILE_PATH_COL: &str = "file_path";
/// Column name for row position in delete scan output
pub const DELETE_POS_COL: &str = "pos";

/// Execution plan for scanning rows to be deleted.
///
/// Unlike `IcebergTableScan`, this plan:
/// 1. Tracks the source file path for each row
/// 2. Tracks the 0-indexed row position within each file
/// 3. Evaluates the DELETE predicate
/// 4. Outputs (file_path, pos) for rows matching the predicate
#[derive(Debug, Clone)]
pub struct IcebergDeleteScanExec {
    /// The table to scan for deletes
    table: Table,
    /// Snapshot of the table to scan
    snapshot_id: Option<i64>,
    /// Stores plan properties for query optimization
    plan_properties: PlanProperties,
    /// The DELETE WHERE predicate - rows matching this will be deleted (for file pruning)
    predicate: Option<Predicate>,
    /// All DataFusion filter expressions (for row-level evaluation)
    filter_exprs: Vec<Expr>,
    /// Output schema: (file_path: Utf8, pos: Int64)
    output_schema: ArrowSchemaRef,
}

impl IcebergDeleteScanExec {
    /// Creates a new `IcebergDeleteScanExec`.
    ///
    /// # Arguments
    /// * `table` - The Iceberg table to scan
    /// * `snapshot_id` - Optional snapshot to scan (defaults to current)
    /// * `filters` - DELETE WHERE clause filters
    pub fn new(table: Table, snapshot_id: Option<i64>, filters: &[Expr]) -> Self {
        let output_schema = Self::make_output_schema();
        let plan_properties = Self::compute_properties(output_schema.clone());
        let predicate = convert_filters_to_predicate(filters);
        let filter_exprs = filters.to_vec();

        Self {
            table,
            snapshot_id,
            plan_properties,
            predicate,
            filter_exprs,
            output_schema,
        }
    }

    /// Creates the output schema for delete scan results.
    /// Schema: (file_path: Utf8, pos: Int64)
    fn make_output_schema() -> ArrowSchemaRef {
        Arc::new(ArrowSchema::new(vec![
            Field::new(DELETE_FILE_PATH_COL, DataType::Utf8, false),
            Field::new(DELETE_POS_COL, DataType::Int64, false),
        ]))
    }

    /// Computes plan properties for query optimization.
    fn compute_properties(schema: ArrowSchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }

    /// Creates a record batch with (file_path, pos) tuples.
    fn make_delete_batch(file_path: &str, positions: Vec<i64>) -> DFResult<RecordBatch> {
        if positions.is_empty() {
            return Ok(RecordBatch::new_empty(Self::make_output_schema()));
        }

        let file_paths: Vec<&str> = vec![file_path; positions.len()];
        let file_path_array = Arc::new(StringArray::from(file_paths)) as ArrayRef;
        let pos_array = Arc::new(Int64Array::from(positions)) as ArrayRef;

        RecordBatch::try_new(Self::make_output_schema(), vec![file_path_array, pos_array])
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
    }
}

impl DisplayAs for IcebergDeleteScanExec {
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

        match t {
            DisplayFormatType::Default | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "IcebergDeleteScanExec: table={}, predicate=[{}]",
                    self.table.identifier(),
                    filter_display
                )
            }
            DisplayFormatType::Verbose => {
                write!(
                    f,
                    "IcebergDeleteScanExec: table={}, snapshot_id={:?}, predicate=[{}]",
                    self.table.identifier(),
                    self.snapshot_id,
                    filter_display
                )
            }
        }
    }
}

impl ExecutionPlan for IcebergDeleteScanExec {
    fn name(&self) -> &str {
        "IcebergDeleteScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
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
        let fut = get_delete_positions_stream(
            self.table.clone(),
            self.snapshot_id,
            self.predicate.clone(),
            self.filter_exprs.clone(),
        );
        let stream = futures::stream::once(fut).try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            stream,
        )))
    }
}

/// Scans the table and returns (file_path, pos) tuples for rows matching the predicate.
///
/// This function:
/// 1. Builds a table scan with the given predicate for file/manifest pruning
/// 2. For each data file, scans all rows and tracks positions
/// 3. Evaluates the predicate on each row
/// 4. Returns (file_path, pos) for matching rows
async fn get_delete_positions_stream(
    table: Table,
    snapshot_id: Option<i64>,
    predicate: Option<Predicate>,
    filter_exprs: Vec<Expr>,
) -> DFResult<Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>> {
    // Build the scan - predicate is used for file-level pruning
    let scan_builder = match snapshot_id {
        Some(snapshot_id) => table.scan().snapshot_id(snapshot_id),
        None => table.scan(),
    };

    let mut scan_builder = scan_builder.select_all();
    if let Some(pred) = predicate.clone() {
        scan_builder = scan_builder.with_filter(pred);
    }

    let table_scan = scan_builder.build().map_err(to_datafusion_error)?;

    // Get file scan tasks - each represents a data file to scan
    // The predicate was used for file/manifest level pruning during plan_files().
    // However, the FileScanTask also carries the predicate for row-level filtering.
    // For DELETE position tracking, we need ALL rows to track correct positions,
    // so we clear the predicate from each task before reading.
    let file_scan_tasks: Vec<FileScanTask> = table_scan
        .plan_files()
        .await
        .map_err(to_datafusion_error)?
        .try_collect()
        .await
        .map_err(to_datafusion_error)?;

    // Process each file and collect (file_path, pos) for matching rows
    let table_clone = table.clone();
    let filter_exprs = Arc::new(filter_exprs);

    let stream = futures::stream::iter(file_scan_tasks)
        .then(move |task| {
            let table = table_clone.clone();
            let filters = filter_exprs.clone();
            async move {
                // CRITICAL: Clear the predicate from the task to disable row-level filtering.
                // We need to read ALL rows to track correct file positions.
                // The filter_exprs will be applied post-read to identify which positions to delete.
                let mut task_without_predicate = task;
                task_without_predicate.predicate = None;
                scan_file_for_deletes(&table, task_without_predicate, filters).await
            }
        })
        .try_flatten();

    Ok(Box::pin(stream))
}

/// Scans a single data file and returns (file_path, pos) for rows matching the predicate.
///
/// This function processes batches in a streaming fashion to avoid OOM for large files.
/// The physical filter expression is built lazily from the first batch's schema to handle
/// schema evolution correctly (files written under older schemas may have different columns).
async fn scan_file_for_deletes(
    table: &Table,
    task: FileScanTask,
    filter_exprs: Arc<Vec<Expr>>,
) -> DFResult<Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>> {
    let file_path = task.data_file_path().to_string();
    let file_io = table.file_io().clone();

    // Build a reader for this specific file task
    let reader = iceberg::arrow::ArrowReaderBuilder::new(file_io).build();

    // Create a stream from the single task
    let task_stream =
        Box::pin(futures::stream::iter(vec![Ok(task)])) as iceberg::scan::FileScanTaskStream;

    // Get the batch stream (not collected - streaming!)
    let batch_stream = reader.read(task_stream).map_err(to_datafusion_error)?;

    // Track row offset and lazily-built physical filter across batches
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::OnceLock;

    // Determine if we have filters to apply
    let has_filters = !filter_exprs.is_empty();
    let row_offset = Arc::new(AtomicI64::new(0));
    // Cache the physical filter expression after building it from the first batch's schema.
    // This handles schema evolution: files written under older schemas have different Arrow schemas.
    // Result<Option<Arc<...>>>: Ok(Some) = filter built, Ok(None) = no filters, Err = filter failed
    let cached_filter: Arc<OnceLock<Result<Option<Arc<dyn PhysicalExpr>>, String>>> =
        Arc::new(OnceLock::new());

    let stream = batch_stream
        .map(move |batch_result| {
            let file_path = file_path.clone();
            let filter_exprs = filter_exprs.clone();
            let row_offset = row_offset.clone();
            let cached_filter = cached_filter.clone();

            match batch_result {
                Ok(batch) => {
                    let batch_size = batch.num_rows();
                    let current_offset = row_offset.fetch_add(batch_size as i64, Ordering::SeqCst);

                    // Build physical filter lazily from first batch's schema (handles schema evolution)
                    let filter_result = cached_filter.get_or_init(|| {
                        if !has_filters {
                            return Ok(None); // No filter = full table delete
                        }
                        match build_physical_filter(&filter_exprs, batch.schema()) {
                            Ok(expr) => Ok(Some(expr)),
                            Err(e) => {
                                // Filter failed to build (e.g., column doesn't exist in evolved schema).
                                // We MUST fail here rather than silently skip - users need to know
                                // their DELETE predicate couldn't be evaluated on all files.
                                Err(format!(
                                    "Cannot evaluate DELETE predicate on file '{}': {}. \
                                     This may indicate schema evolution incompatibility.",
                                    file_path, e
                                ))
                            }
                        }
                    });

                    let positions = match filter_result {
                        Ok(Some(filter)) => {
                            // Evaluate the filter expression on this batch
                            match evaluate_filter_for_positions(filter, &batch, current_offset) {
                                Ok(pos) => pos,
                                Err(e) => return Err(e),
                            }
                        }
                        Ok(None) => {
                            // No filter - delete all rows (full table delete)
                            (current_offset..(current_offset + batch_size as i64)).collect()
                        }
                        Err(msg) => {
                            // Filter failed to build - fail the operation explicitly.
                            // Silent skips are a correctness hazard: users must know if
                            // their DELETE wasn't fully applied.
                            return Err(datafusion::error::DataFusionError::Execution(
                                msg.clone(),
                            ));
                        }
                    };

                    if positions.is_empty() {
                        // Return empty batch instead of None to keep stream structure simple
                        Ok(RecordBatch::new_empty(IcebergDeleteScanExec::make_output_schema()))
                    } else {
                        IcebergDeleteScanExec::make_delete_batch(&file_path, positions)
                    }
                }
                Err(e) => Err(to_datafusion_error(e)),
            }
        })
        // Filter out empty batches
        .filter(|result| {
            futures::future::ready(match result {
                Ok(batch) => batch.num_rows() > 0,
                Err(_) => true, // Keep errors
            })
        });

    Ok(Box::pin(stream))
}

/// Builds a physical filter expression from logical expressions using the given Arrow schema.
fn build_physical_filter(
    filter_exprs: &[Expr],
    arrow_schema: ArrowSchemaRef,
) -> DFResult<Arc<dyn PhysicalExpr>> {
    let df_schema = DFSchema::try_from(arrow_schema.as_ref().clone())?;

    // Combine all filter expressions with AND
    let combined_expr = filter_exprs
        .iter()
        .cloned()
        .reduce(|a, b| a.and(b))
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Internal("No filter expressions".to_string())
        })?;

    // Create a session context to get execution props
    let ctx = SessionContext::new();
    let execution_props = ctx.state().execution_props().clone();

    // Convert logical expression to physical expression
    create_physical_expr(&combined_expr, &df_schema, &execution_props)
}

/// Evaluates a filter expression on a batch and returns positions where the filter is true.
fn evaluate_filter_for_positions(
    filter: &Arc<dyn PhysicalExpr>,
    batch: &RecordBatch,
    row_offset: i64,
) -> DFResult<Vec<i64>> {
    let batch_size = batch.num_rows();
    let filter_result = filter.evaluate(batch)?;
    let filter_array = filter_result
        .into_array(batch_size)?
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Internal(
                "Filter expression did not return a boolean array".to_string(),
            )
        })?
        .clone();

    // Collect positions where filter is true
    let positions: Vec<i64> = filter_array
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

    Ok(positions)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_output_schema() {
        let schema = IcebergDeleteScanExec::make_output_schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), DELETE_FILE_PATH_COL);
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(1).name(), DELETE_POS_COL);
        assert_eq!(schema.field(1).data_type(), &DataType::Int64);
    }

    #[test]
    fn test_make_delete_batch() {
        let batch = IcebergDeleteScanExec::make_delete_batch(
            "s3://bucket/table/data/file.parquet",
            vec![0, 1, 5, 10],
        )
        .unwrap();

        assert_eq!(batch.num_rows(), 4);
        assert_eq!(batch.num_columns(), 2);

        let file_paths = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(file_paths.value(0), "s3://bucket/table/data/file.parquet");
        assert_eq!(file_paths.value(3), "s3://bucket/table/data/file.parquet");

        let positions = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(positions.value(0), 0);
        assert_eq!(positions.value(1), 1);
        assert_eq!(positions.value(2), 5);
        assert_eq!(positions.value(3), 10);
    }

    #[test]
    fn test_make_delete_batch_empty() {
        let batch = IcebergDeleteScanExec::make_delete_batch(
            "s3://bucket/file.parquet",
            vec![],
        )
        .unwrap();

        assert_eq!(batch.num_rows(), 0);
    }
}
