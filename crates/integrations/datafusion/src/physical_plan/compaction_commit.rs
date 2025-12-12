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
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, StringArray, UInt64Array};
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
use futures::StreamExt;
use iceberg::Catalog;
use iceberg::spec::{DataFile, deserialize_data_file_from_json};
use iceberg::table::Table;
use iceberg::transaction::rewrite_data_files::{
    FileGroupRewriteResult, RewriteDataFilesCommitter, RewriteDataFilesPlan,
};

use super::compaction::{COMPACTION_DATA_FILES_COL, COMPACTION_GROUP_ID_COL};
use crate::partition_utils::build_partition_type_map;
use crate::to_datafusion_error;

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
    /// The catalog for committing changes.
    catalog: Arc<dyn Catalog>,
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
    ///
    /// # Arguments
    ///
    /// * `table` - The Iceberg table being compacted
    /// * `catalog` - The catalog for committing changes
    /// * `plan` - The compaction plan containing file groups to rewrite
    /// * `input` - The input execution plan (IcebergCompactionExec)
    #[allow(dead_code)]
    pub fn new(
        table: Table,
        catalog: Arc<dyn Catalog>,
        plan: RewriteDataFilesPlan,
        input: Arc<dyn ExecutionPlan>,
    ) -> DFResult<Self> {
        let output_schema = compaction_commit_output_schema();
        let plan_properties = Self::compute_properties(output_schema.clone());

        Ok(Self {
            table,
            catalog,
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
            catalog: self.catalog.clone(),
            plan: self.plan.clone(),
            input: children[0].clone(),
            output_schema: self.output_schema.clone(),
            plan_properties: self.plan_properties.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "IcebergCompactionCommitExec only supports partition 0, got {partition}"
            )));
        }

        let table = self.table.clone();
        let catalog = self.catalog.clone();
        let plan = self.plan.clone();
        let output_schema = self.output_schema.clone();

        // Execute input plan
        let input_stream = self.input.execute(0, context)?;

        // Build partition type map for deserialization
        let partition_types = build_partition_type_map(&table)?;
        let schema = table.metadata().current_schema().clone();
        let group_spec_ids: HashMap<u32, i32> = plan
            .file_groups
            .iter()
            .map(|g| (g.group_id, g.partition_spec_id))
            .collect();

        // Create single-item stream that processes all input and commits
        let stream = futures::stream::once(async move {
            let mut results_by_group: HashMap<u32, Vec<DataFile>> = plan
                .file_groups
                .iter()
                .map(|g| (g.group_id, Vec::new()))
                .collect();

            let mut batch_stream = input_stream;
            while let Some(batch_result) = batch_stream.next().await {
                let batch = batch_result.map_err(|e| {
                    DataFusionError::Execution(format!("Failed to execute compaction: {e}"))
                })?;

                // Get group_id and data_files columns
                let group_id_col = batch
                    .column_by_name(COMPACTION_GROUP_ID_COL)
                    .ok_or_else(|| {
                        DataFusionError::Internal("Missing group_id column".to_string())
                    })?
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::UInt32Array>()
                    .ok_or_else(|| {
                        DataFusionError::Internal("group_id column wrong type".to_string())
                    })?;

                let data_files_col = batch
                    .column_by_name(COMPACTION_DATA_FILES_COL)
                    .ok_or_else(|| {
                        DataFusionError::Internal("Missing data_files column".to_string())
                    })?
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal("data_files column wrong type".to_string())
                    })?;

                // Parse each row
                for i in 0..batch.num_rows() {
                    let group_id = group_id_col.value(i);
                    let data_file_json = data_files_col.value(i);

                    let spec_id = *group_spec_ids.get(&group_id).ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Group {group_id} not found in compaction plan"
                        ))
                    })?;

                    let partition_type = partition_types.get(&spec_id).ok_or_else(|| {
                        DataFusionError::Internal(format!("No partition type for spec {spec_id}"))
                    })?;

                    // Deserialize the data file
                    let data_file = deserialize_data_file_from_json(
                        data_file_json,
                        spec_id,
                        partition_type,
                        &schema,
                    )
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to deserialize compacted DataFile JSON for group {group_id} (spec {spec_id}) row {i}: {e}"
                        ))
                    })?;

                    results_by_group
                        .get_mut(&group_id)
                        .ok_or_else(|| {
                            DataFusionError::Internal(format!(
                                "Group {group_id} not found in compaction plan"
                            ))
                        })?
                        .push(data_file);
                }
            }

            // Convert to FileGroupRewriteResult entries in plan order so that we always include
            // groups with zero output files (valid if deletes removed all rows).
            let results: Vec<FileGroupRewriteResult> = plan
                .file_groups
                .iter()
                .map(|g| FileGroupRewriteResult {
                    group_id: g.group_id,
                    new_data_files: results_by_group.remove(&g.group_id).unwrap_or_default(),
                })
                .collect();

            // Commit the compaction via RewriteDataFilesCommitter.
            // Note: Concurrency control is handled by TableRequirement::RefSnapshotIdMatch,
            // which is included in the ActionCommit and enforced atomically by the catalog.
            let table_ident = table.identifier().clone();
            let committer = RewriteDataFilesCommitter::new(&table, plan);
            let (action_commit, result) = committer
                .commit(results)
                .await
                .map_err(to_datafusion_error)?;

            // Apply the commit to the catalog (this is the actual catalog write)
            action_commit
                .commit_to_catalog(table_ident, catalog.as_ref())
                .await
                .map_err(to_datafusion_error)?;

            // Create output metrics batch
            let rewritten_files_array =
                Arc::new(UInt64Array::from(vec![result.rewritten_data_files_count]))
                    as Arc<dyn datafusion::arrow::array::Array>;
            let added_files_array = Arc::new(UInt64Array::from(vec![result.added_data_files_count]))
                as Arc<dyn datafusion::arrow::array::Array>;
            let rewritten_bytes_array = Arc::new(UInt64Array::from(vec![result.rewritten_bytes]))
                as Arc<dyn datafusion::arrow::array::Array>;
            let added_bytes_array = Arc::new(UInt64Array::from(vec![result.added_bytes]))
                as Arc<dyn datafusion::arrow::array::Array>;

            RecordBatch::try_new(output_schema, vec![
                rewritten_files_array,
                added_files_array,
                rewritten_bytes_array,
                added_bytes_array,
            ])
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            stream.boxed(),
        )))
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
