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

//! Execution plan for committing UPDATE operations to an Iceberg table.
//!
//! This module provides `IcebergUpdateCommitExec`, which collects serialized
//! data file and delete file JSON from `IcebergUpdateExec` and commits them
//! atomically using `Transaction::row_delta()` to create a single snapshot
//! containing both new data files and position delete files.

use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{
    DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::StreamExt;
use iceberg::spec::{DataFile, deserialize_data_file_from_json};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::Catalog;

use super::update::{UPDATE_COUNT_COL, UPDATE_DATA_FILES_COL, UPDATE_DELETE_FILES_COL};
use crate::to_datafusion_error;

/// Column name for the count of updated rows in output
pub const UPDATE_RESULT_COUNT_COL: &str = "count";

/// Execution plan for committing UPDATE files to an Iceberg table.
///
/// Collects serialized data file and delete file JSON from input and commits them
/// atomically using `Transaction::row_delta()` to produce a single snapshot containing
/// both data manifests and delete manifests. Returns a count of the updated rows.
///
/// # Concurrency Safety
///
/// This executor captures a baseline snapshot ID at plan construction time and validates
/// it at commit time. If the table has been modified since the scan started (i.e., another
/// transaction committed), the commit will fail with a clear error rather than potentially
/// applying stale position deletes to reorganized files.
#[derive(Debug)]
pub struct IcebergUpdateCommitExec {
    table: Table,
    catalog: Arc<dyn Catalog>,
    input: Arc<dyn ExecutionPlan>,
    input_schema: ArrowSchemaRef,
    count_schema: ArrowSchemaRef,
    plan_properties: PlanProperties,
    /// Snapshot ID captured at plan construction time for concurrency validation.
    /// If set, commit will fail if the table's current snapshot has changed.
    baseline_snapshot_id: Option<i64>,
}

impl IcebergUpdateCommitExec {
    /// Creates a new `IcebergUpdateCommitExec`.
    ///
    /// # Arguments
    /// * `table` - The Iceberg table to commit to
    /// * `catalog` - The catalog for committing changes
    /// * `input` - The input execution plan providing data and delete files
    /// * `input_schema` - Schema of the input plan
    /// * `baseline_snapshot_id` - Snapshot ID captured at plan construction time.
    ///   If provided, commit will validate that the table hasn't been modified since
    ///   the scan started to prevent applying stale position deletes.
    pub fn new(
        table: Table,
        catalog: Arc<dyn Catalog>,
        input: Arc<dyn ExecutionPlan>,
        input_schema: ArrowSchemaRef,
        baseline_snapshot_id: Option<i64>,
    ) -> Self {
        let count_schema = Self::make_count_schema();
        let plan_properties = Self::compute_properties(count_schema.clone());

        Self {
            table,
            catalog,
            input,
            input_schema,
            count_schema,
            plan_properties,
            baseline_snapshot_id,
        }
    }

    fn compute_properties(schema: ArrowSchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        )
    }

    /// Creates the output schema: a single "count" column.
    fn make_count_schema() -> ArrowSchemaRef {
        Arc::new(ArrowSchema::new(vec![Field::new(
            UPDATE_RESULT_COUNT_COL,
            DataType::UInt64,
            false,
        )]))
    }

    /// Creates a record batch with the count of updated rows.
    fn make_count_batch(count: u64) -> DFResult<RecordBatch> {
        let count_array = Arc::new(UInt64Array::from(vec![count])) as ArrayRef;

        RecordBatch::try_from_iter_with_nullable(vec![(UPDATE_RESULT_COUNT_COL, count_array, false)])
            .map_err(|e| {
                DataFusionError::ArrowError(
                    Box::new(e),
                    Some("Failed to make update count batch".to_string()),
                )
            })
    }
}

impl DisplayAs for IcebergUpdateCommitExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "IcebergUpdateCommitExec: table={}",
                    self.table.identifier()
                )
            }
            DisplayFormatType::Verbose => {
                write!(
                    f,
                    "IcebergUpdateCommitExec: table={}, input_schema={:?}",
                    self.table.identifier(),
                    self.input_schema
                )
            }
        }
    }
}

impl ExecutionPlan for IcebergUpdateCommitExec {
    fn name(&self) -> &str {
        "IcebergUpdateCommitExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn required_input_distribution(&self) -> Vec<datafusion::physical_plan::Distribution> {
        vec![datafusion::physical_plan::Distribution::SinglePartition; self.children().len()]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "IcebergUpdateCommitExec expects exactly one child, but provided {}",
                children.len()
            )));
        }

        Ok(Arc::new(Self::new(
            self.table.clone(),
            self.catalog.clone(),
            children[0].clone(),
            self.input_schema.clone(),
            self.baseline_snapshot_id,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        // IcebergUpdateCommitExec only has one partition (partition 0)
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "IcebergUpdateCommitExec only has one partition, but got partition {partition}"
            )));
        }

        let catalog = self.catalog.clone();
        let input_plan = self.input.clone();
        let count_schema = self.count_schema.clone();
        let baseline_snapshot_id = self.baseline_snapshot_id;
        let table_ident = self.table.identifier().clone();
        // Note: We don't clone the table here because we refresh it from the catalog
        // at commit time to get the latest state for validation.

        let spec_id = self.table.metadata().default_partition_spec_id();
        let partition_type = self.table.metadata().default_partition_type().clone();
        let current_schema = self.table.metadata().current_schema().clone();

        // Process the input and commit files
        let stream = futures::stream::once(async move {
            let mut data_files: Vec<DataFile> = Vec::new();
            let mut delete_files: Vec<DataFile> = Vec::new();
            let mut total_update_count: u64 = 0;

            // Execute and collect results from the input
            let mut batch_stream = input_plan.execute(0, context)?;

            while let Some(batch_result) = batch_stream.next().await {
                let batch = batch_result?;

                // Extract data files JSON
                let data_files_array = batch
                    .column_by_name(UPDATE_DATA_FILES_COL)
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Expected '{}' column in input batch",
                            UPDATE_DATA_FILES_COL
                        ))
                    })?
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Expected '{}' column to be StringArray",
                            UPDATE_DATA_FILES_COL
                        ))
                    })?;

                // Extract delete files JSON
                let delete_files_array = batch
                    .column_by_name(UPDATE_DELETE_FILES_COL)
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Expected '{}' column in input batch",
                            UPDATE_DELETE_FILES_COL
                        ))
                    })?
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Expected '{}' column to be StringArray",
                            UPDATE_DELETE_FILES_COL
                        ))
                    })?;

                // Extract count
                let count_array = batch
                    .column_by_name(UPDATE_COUNT_COL)
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Expected '{}' column in input batch",
                            UPDATE_COUNT_COL
                        ))
                    })?
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Expected '{}' column to be UInt64Array",
                            UPDATE_COUNT_COL
                        ))
                    })?;

                // Parse data files from newline-delimited JSON
                for data_files_str in data_files_array.iter().flatten() {
                    for json_line in data_files_str.lines() {
                        if !json_line.trim().is_empty() {
                            let df = deserialize_data_file_from_json(
                                json_line,
                                spec_id,
                                &partition_type,
                                &current_schema,
                            )
                            .map_err(to_datafusion_error)?;
                            data_files.push(df);
                        }
                    }
                }

                // Parse delete files from newline-delimited JSON
                for delete_files_str in delete_files_array.iter().flatten() {
                    for json_line in delete_files_str.lines() {
                        if !json_line.trim().is_empty() {
                            let df = deserialize_data_file_from_json(
                                json_line,
                                spec_id,
                                &partition_type,
                                &current_schema,
                            )
                            .map_err(to_datafusion_error)?;
                            delete_files.push(df);
                        }
                    }
                }

                // Sum up counts
                for i in 0..count_array.len() {
                    total_update_count += count_array.value(i);
                }
            }

            // If no files were collected, return zero count
            if data_files.is_empty() && delete_files.is_empty() {
                return Self::make_count_batch(0);
            }

            // Refresh table from catalog to get current state for commit
            let table = catalog
                .load_table(&table_ident)
                .await
                .map_err(to_datafusion_error)?;

            // Validate baseline snapshot if provided - ensures position deletes
            // are still valid (table hasn't been reorganized since scan started)
            if let Some(baseline) = baseline_snapshot_id {
                let current = table.metadata().current_snapshot_id();
                if current != Some(baseline) {
                    return Err(DataFusionError::Execution(format!(
                        "UPDATE conflict: table '{}' was modified by another transaction. \
                         Expected snapshot {}, but current snapshot is {:?}. \
                         Position deletes may be stale. Please retry the UPDATE.",
                        table_ident, baseline, current
                    )));
                }
            }

            // Commit both data files and delete files atomically using RowDelta.
            // This produces a single snapshot containing both data manifests and
            // delete manifests, ensuring readers never see a partial state where
            // deletes are visible but new data is not.
            let tx = Transaction::new(&table);
            let action = tx
                .row_delta()
                .add_data_files(data_files)
                .add_delete_files(delete_files);
            let tx = action.apply(tx).map_err(to_datafusion_error)?;
            let _updated_table = tx
                .commit(catalog.as_ref())
                .await
                .map_err(to_datafusion_error)?;

            Self::make_count_batch(total_update_count)
        })
        .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(count_schema, stream)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_schema() {
        let schema = IcebergUpdateCommitExec::make_count_schema();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), UPDATE_RESULT_COUNT_COL);
        assert_eq!(schema.field(0).data_type(), &DataType::UInt64);
    }

    #[test]
    fn test_make_count_batch() {
        let batch = IcebergUpdateCommitExec::make_count_batch(42).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);

        let count = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(count.value(0), 42);
    }

    #[test]
    fn test_make_count_batch_zero() {
        let batch = IcebergUpdateCommitExec::make_count_batch(0).unwrap();
        assert_eq!(batch.num_rows(), 1);

        let count = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(count.value(0), 0);
    }
}
