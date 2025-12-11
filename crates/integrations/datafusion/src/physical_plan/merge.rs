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

//! Execution plan for MERGE operations on Iceberg tables.
//!
//! This module provides `IcebergMergeExec`, which implements Copy-on-Write (CoW)
//! MERGE semantics by:
//! 1. Scanning target table with metadata columns (_file_path, _pos)
//! 2. Performing a FULL OUTER JOIN with source data
//! 3. Classifying rows as MATCHED, NOT_MATCHED (source only), or NOT_MATCHED_BY_SOURCE (target only)
//! 4. Applying WHEN clauses in order (first match wins)
//! 5. Writing new data files and position delete files
//!
//! The output is serialized JSON for both data files and delete files, which are
//! then committed atomically by `IcebergMergeCommitExec` using `RowDeltaAction`.
//!
//! # Row Classification
//!
//! After the FULL OUTER JOIN, rows are classified based on NULL patterns:
//!
//! | Target Key | Source Key | Classification |
//! |------------|------------|----------------|
//! | NOT NULL   | NOT NULL   | MATCHED        |
//! | NULL       | NOT NULL   | NOT_MATCHED    |
//! | NOT NULL   | NULL       | NOT_MATCHED_BY_SOURCE |
//!
//! The classification determines which WHEN clauses are applicable.
//!
//! # Current Limitations
//!
//! **Memory**: Both target table and source data are materialized in memory for the
//! FULL OUTER JOIN. This will not scale to large tables. For production use with
//! large datasets, a streaming join implementation would be needed.
//!
//! **WHEN clause conditions**: Conditional clauses (e.g., `WHEN MATCHED AND x > 0`)
//! are parsed but conditions are ignored during evaluation. Only unconditional
//! clauses are applied. Full condition support requires PhysicalExpr compilation.
//!
//! **Concurrency**: Baseline snapshot validation is not yet implemented. Concurrent
//! modifications to the table during MERGE execution may cause data inconsistencies.

use std::any::Any;
use std::collections::HashMap;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{
    DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use datafusion::common::DFSchema;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::JoinType;
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
use iceberg::expr::{Predicate, Reference};
use iceberg::scan::FileScanTask;
use iceberg::spec::{
    DataFile, DataFileFormat, PartitionKey, SchemaRef, TableProperties, Transform,
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

use crate::merge::{
    MatchedAction, MergeStats, NotMatchedAction, NotMatchedBySourceAction, WhenMatchedClause,
    WhenNotMatchedBySourceClause, WhenNotMatchedClause,
};
use crate::partition_utils::{
    FilePartitionInfo, SerializedFileWithSpec, build_file_partition_map, build_partition_type_map,
};
use crate::to_datafusion_error;

/// Internal metadata column name for file path (added during scan)
pub const MERGE_FILE_PATH_COL: &str = "__iceberg_merge_file_path";
/// Internal metadata column name for row position (added during scan)
pub const MERGE_ROW_POS_COL: &str = "__iceberg_merge_row_pos";
/// Internal column name for merge action classification
#[allow(dead_code)] // Will be used in condition evaluation
pub const MERGE_ACTION_COL: &str = "__iceberg_merge_action";

/// Prefix for target columns in joined schema
pub const TARGET_PREFIX: &str = "target_";
/// Prefix for source columns in joined schema
pub const SOURCE_PREFIX: &str = "source_";

/// Column name for serialized data files in output
pub const MERGE_DATA_FILES_COL: &str = "data_files";
/// Column name for serialized delete files in output
pub const MERGE_DELETE_FILES_COL: &str = "delete_files";
/// Column names for stats in output
pub const MERGE_INSERTED_COUNT_COL: &str = "inserted_count";
pub const MERGE_UPDATED_COUNT_COL: &str = "updated_count";
pub const MERGE_DELETED_COUNT_COL: &str = "deleted_count";
/// Column names for DPP metrics in output
pub const MERGE_DPP_APPLIED_COL: &str = "dpp_applied";
pub const MERGE_DPP_PARTITION_COUNT_COL: &str = "dpp_partition_count";

/// Encoded action values for the MERGE_ACTION_COL column.
/// These are i8 values that encode the MergeAction enum.
pub mod action_codes {
    /// Insert a new row (from NOT MATCHED source row)
    pub const INSERT: i8 = 1;
    /// Update an existing row
    pub const UPDATE: i8 = 2;
    /// Delete an existing row
    pub const DELETE: i8 = 3;
    /// No action - keep the row as-is
    pub const NO_ACTION: i8 = 0;
}

// ============================================================================
// Dynamic Partition Pruning (DPP) Configuration
// ============================================================================

/// Default maximum number of distinct partition values for DPP to be enabled.
/// If source touches more partitions than this, DPP is disabled to avoid huge IN lists.
pub const DPP_DEFAULT_MAX_PARTITIONS: usize = 1000;

/// Configuration options for MERGE operations.
///
/// Controls optimization features like Dynamic Partition Pruning (DPP) and
/// memory management for large-scale MERGE operations.
///
/// # Dynamic Partition Pruning (DPP)
///
/// DPP optimizes MERGE performance by pruning target partitions based on
/// the partition values present in the source data. This reduces I/O by
/// only scanning partitions that could potentially match.
///
/// ## When DPP Applies
///
/// DPP is applied when **all** of the following conditions are met:
/// - `enable_dpp` is `true` (default)
/// - The table uses **identity** partition transforms (not bucket, truncate, etc.)
/// - At least one join key in the ON condition is also a partition column
/// - The partition column has a supported data type (see below)
/// - The number of distinct partition values in source is ≤ `dpp_max_partitions`
///
/// ## Supported Data Types
///
/// DPP can extract partition values from these Arrow/Iceberg types:
/// - `Int32` / Iceberg `int`
/// - `Int64` / Iceberg `long`
/// - `Utf8` / `LargeUtf8` / Iceberg `string`
/// - `Date32` / Iceberg `date`
///
/// Other types (timestamps, decimals, binary, etc.) will cause DPP to be
/// skipped silently, falling back to a full table scan.
///
/// ## Current Limitations
///
/// - **Single partition column**: For compound partition keys like `(region, date)`,
///   DPP currently only prunes on the **first** partition column that appears in
///   the join condition. Future enhancement: prune on all matching partition columns.
///
/// - **Identity transforms only**: Transforms like `bucket()`, `truncate()`,
///   `days()`, `months()` are not supported. Future enhancement: support date/time
///   extraction transforms where the mapping is straightforward.
///
/// ## Observability
///
/// Check the returned [`MergeStats`] for DPP metrics:
/// - `dpp_applied`: Whether DPP was used for this operation
/// - `dpp_partition_count`: Number of partitions pruned to (or the count that exceeded threshold)
///
/// In debug builds, diagnostic messages are printed to stderr when DPP is
/// skipped due to non-identity transforms, unsupported types, or threshold limits.
#[derive(Debug, Clone)]
pub struct MergeConfig {
    /// Enable Dynamic Partition Pruning (DPP) to reduce target table scan.
    ///
    /// When enabled and the join key includes a partition column, MERGE will:
    /// 1. Extract distinct partition values from source data
    /// 2. Build an IN filter for target table scan
    /// 3. Skip partitions not touched by source data
    ///
    /// Default: `true`
    pub enable_dpp: bool,

    /// Maximum number of distinct partition values before disabling DPP.
    ///
    /// If source data touches more than this many partitions, DPP is disabled
    /// to avoid generating very large IN lists that may hurt performance.
    ///
    /// Default: `1000`
    pub dpp_max_partitions: usize,
}

impl Default for MergeConfig {
    fn default() -> Self {
        Self {
            enable_dpp: true,
            dpp_max_partitions: DPP_DEFAULT_MAX_PARTITIONS,
        }
    }
}

impl MergeConfig {
    /// Creates a new MergeConfig with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enables or disables Dynamic Partition Pruning.
    pub fn with_dpp(mut self, enable: bool) -> Self {
        self.enable_dpp = enable;
        self
    }

    /// Sets the maximum partition count threshold for DPP.
    pub fn with_dpp_max_partitions(mut self, max: usize) -> Self {
        self.dpp_max_partitions = max;
        self
    }
}

/// Classification of a row after FULL OUTER JOIN.
///
/// This determines which WHEN clauses are applicable to a row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowClassification {
    /// Row exists in both source and target (key columns match).
    /// Contains the file path and position for position delete tracking.
    Matched,
    /// Row exists only in source (target key is NULL after join).
    /// Candidate for INSERT actions.
    NotMatched,
    /// Row exists only in target (source key is NULL after join).
    /// Candidate for DELETE or UPDATE BY SOURCE actions.
    NotMatchedBySource,
}

#[allow(dead_code)] // Will be used when IcebergMergeExec::execute() is implemented
impl RowClassification {
    /// Classifies a row based on NULL patterns in the join result.
    ///
    /// # Arguments
    /// * `target_key_is_null` - Whether the target side of the join key is NULL
    /// * `source_key_is_null` - Whether the source side of the join key is NULL
    ///
    /// # Returns
    /// The classification for this row, or None if both keys are NULL (shouldn't happen
    /// in a proper FULL OUTER JOIN on non-null keys).
    pub fn from_join_nulls(target_key_is_null: bool, source_key_is_null: bool) -> Option<Self> {
        match (target_key_is_null, source_key_is_null) {
            (false, false) => Some(RowClassification::Matched),
            (true, false) => Some(RowClassification::NotMatched),
            (false, true) => Some(RowClassification::NotMatchedBySource),
            (true, true) => None, // Both NULL - shouldn't happen with non-null join keys
        }
    }

    /// Returns a human-readable name for this classification.
    pub fn as_str(&self) -> &'static str {
        match self {
            RowClassification::Matched => "MATCHED",
            RowClassification::NotMatched => "NOT_MATCHED",
            RowClassification::NotMatchedBySource => "NOT_MATCHED_BY_SOURCE",
        }
    }
}

/// Action to take for a specific row after WHEN clause evaluation.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Variants and fields are for future condition evaluation
pub enum MergeAction {
    /// Insert a new row (from NOT MATCHED source row).
    Insert,
    /// Update an existing row (from MATCHED or NOT_MATCHED_BY_SOURCE).
    /// The String keys are column names, Expr values are the expressions to evaluate.
    Update(Vec<(String, Expr)>),
    /// Update all columns from source (only valid for MATCHED rows).
    UpdateAll,
    /// Delete an existing row.
    Delete,
    /// No action - keep the row as-is (used when no WHEN clause matches).
    /// For MATCHED: row is unchanged (neither updated nor deleted).
    /// For NOT_MATCHED: row is not inserted.
    /// For NOT_MATCHED_BY_SOURCE: row is kept as-is.
    NoAction,
}

impl MergeAction {
    /// Encodes the action as an i8 for storage in the action column.
    #[allow(dead_code)] // Will be used for condition evaluation
    pub fn to_code(&self) -> i8 {
        match self {
            MergeAction::Insert => action_codes::INSERT,
            MergeAction::Update(_) | MergeAction::UpdateAll => action_codes::UPDATE,
            MergeAction::Delete => action_codes::DELETE,
            MergeAction::NoAction => action_codes::NO_ACTION,
        }
    }

    /// Decodes an i8 action code to a simplified MergeAction.
    /// Note: Update details are lost during encoding.
    #[allow(dead_code)] // Will be used in file writing stages
    pub fn from_code(code: i8) -> Option<Self> {
        match code {
            action_codes::INSERT => Some(MergeAction::Insert),
            action_codes::UPDATE => Some(MergeAction::UpdateAll), // Simplified
            action_codes::DELETE => Some(MergeAction::Delete),
            action_codes::NO_ACTION => Some(MergeAction::NoAction),
            _ => None,
        }
    }
}

// ============================================================================
// Partition-Aware Writers for MERGE Operations
// ============================================================================

// Type aliases for the complex writer types
type DataFileWriterType = iceberg::writer::base_writer::data_file_writer::DataFileWriter<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
>;

type PositionDeleteWriterType =
    iceberg::writer::base_writer::position_delete_writer::PositionDeleteFileWriter<
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

/// Handles both data file and position delete file writing with partition awareness.
///
/// For unpartitioned tables, writes go directly to single writers.
/// For partitioned tables, uses FanoutWriter to route writes to correct partitions.
struct MergeWriters {
    /// Data file writer - either unpartitioned or fanout
    data_writer: MergeDataWriter,
    /// Partition splitter for data files (None for unpartitioned)
    partition_splitter: Option<RecordBatchPartitionSplitter>,
    /// Delete file writer - either unpartitioned or fanout
    delete_writer: MergeDeleteWriter,
    /// Schema for position delete batches
    delete_schema: ArrowSchemaRef,
    /// Iceberg schema for partition key computation
    iceberg_schema: SchemaRef,
    /// Mapping from file path to partition info (for partition evolution support)
    file_partition_map: HashMap<String, FilePartitionInfo>,
}

/// Data file writer that supports both partitioned and unpartitioned tables.
enum MergeDataWriter {
    Unpartitioned(DataFileWriterType),
    Partitioned(FanoutWriter<DataFileWriterBuilderType>),
}

/// Position delete writer that supports both partitioned and unpartitioned tables.
enum MergeDeleteWriter {
    Unpartitioned(PositionDeleteWriterType),
    Partitioned(FanoutWriter<PositionDeleteWriterBuilderType>),
}

impl MergeWriters {
    /// Creates writers for MERGE output, either partitioned or unpartitioned.
    async fn new(table: &Table) -> DFResult<Self> {
        let file_io = table.file_io().clone();
        let partition_spec = table.metadata().default_partition_spec().clone();
        let iceberg_schema = table.metadata().current_schema().clone();
        let is_partitioned = !partition_spec.is_unpartitioned();

        // Build file partition map for partition-evolution-aware deletes
        let file_partition_map = if is_partitioned {
            build_file_partition_map(table).await?
        } else {
            HashMap::new()
        };

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
            format!("merge-data-{}", Uuid::now_v7()),
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

        // Build Iceberg schema for position deletes
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
            format!("merge-delete-{}", Uuid::now_v7()),
            None,
            DataFileFormat::Parquet,
        );
        let delete_rolling_writer = RollingFileWriterBuilder::new(
            delete_parquet_writer,
            target_file_size,
            file_io,
            location_generator,
            delete_file_name_generator,
        );
        let delete_file_builder =
            PositionDeleteFileWriterBuilder::new(delete_rolling_writer, delete_config);

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

        // Create writers based on partition status
        let (data_writer, delete_writer) = if is_partitioned {
            (
                MergeDataWriter::Partitioned(FanoutWriter::new(data_file_builder)),
                MergeDeleteWriter::Partitioned(FanoutWriter::new(delete_file_builder)),
            )
        } else {
            (
                MergeDataWriter::Unpartitioned(
                    data_file_builder
                        .build(None)
                        .await
                        .map_err(to_datafusion_error)?,
                ),
                MergeDeleteWriter::Unpartitioned(
                    delete_file_builder
                        .build(None)
                        .await
                        .map_err(to_datafusion_error)?,
                ),
            )
        };

        Ok(Self {
            data_writer,
            partition_splitter,
            delete_writer,
            delete_schema,
            iceberg_schema,
            file_partition_map,
        })
    }

    /// Writes a data batch, automatically splitting by partition if needed.
    async fn write_data(&mut self, batch: RecordBatch) -> DFResult<()> {
        match &mut self.data_writer {
            MergeDataWriter::Unpartitioned(writer) => {
                writer.write(batch).await.map_err(to_datafusion_error)
            }
            MergeDataWriter::Partitioned(writer) => {
                let splitter = self.partition_splitter.as_ref().ok_or_else(|| {
                    DataFusionError::Internal(
                        "Partition splitter required for partitioned table".into(),
                    )
                })?;
                let partitioned_batches = splitter.split(&batch).map_err(to_datafusion_error)?;
                for (partition_key, partition_batch) in partitioned_batches {
                    writer
                        .write(partition_key, partition_batch)
                        .await
                        .map_err(to_datafusion_error)?;
                }
                Ok(())
            }
        }
    }

    /// Writes a position delete batch.
    /// For partitioned tables, the partition_key must be provided.
    async fn write_deletes(
        &mut self,
        batch: RecordBatch,
        partition_key: Option<PartitionKey>,
    ) -> DFResult<()> {
        match &mut self.delete_writer {
            MergeDeleteWriter::Unpartitioned(writer) => {
                writer.write(batch).await.map_err(to_datafusion_error)
            }
            MergeDeleteWriter::Partitioned(writer) => {
                let key = partition_key.ok_or_else(|| {
                    DataFusionError::Internal(
                        "Partition key required for delete writes on partitioned table".into(),
                    )
                })?;
                writer.write(key, batch).await.map_err(to_datafusion_error)
            }
        }
    }

    /// Returns the delete schema for creating position delete batches.
    fn delete_schema(&self) -> &ArrowSchemaRef {
        &self.delete_schema
    }

    /// Checks if the table is partitioned.
    fn is_partitioned(&self) -> bool {
        self.partition_splitter.is_some()
    }

    /// Gets the partition key for a file path from the file partition map.
    ///
    /// This method is partition-evolution-aware: it uses the file's original
    /// partition spec and partition values from the manifest, rather than
    /// recomputing them from the current partition spec.
    ///
    /// For unpartitioned tables, returns None.
    /// For partitioned tables, looks up the file in the file_partition_map.
    fn get_partition_key_for_file(&self, file_path: &str) -> DFResult<Option<PartitionKey>> {
        if !self.is_partitioned() {
            return Ok(None);
        }

        let file_info = self.file_partition_map.get(file_path).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "No partition info found for file '{}'. This may indicate a consistency issue \
                 between the scan and write phases.",
                file_path
            ))
        })?;

        Ok(Some(PartitionKey::new(
            (*file_info.partition_spec).clone(),
            self.iceberg_schema.clone(),
            file_info.partition.clone(),
        )))
    }

    /// Closes all writers and returns the written data files.
    async fn close(self) -> DFResult<(Vec<DataFile>, Vec<DataFile>)> {
        let data_files = match self.data_writer {
            MergeDataWriter::Unpartitioned(mut writer) => {
                writer.close().await.map_err(to_datafusion_error)?
            }
            MergeDataWriter::Partitioned(writer) => {
                writer.close().await.map_err(to_datafusion_error)?
            }
        };

        let delete_files = match self.delete_writer {
            MergeDeleteWriter::Unpartitioned(mut writer) => {
                writer.close().await.map_err(to_datafusion_error)?
            }
            MergeDeleteWriter::Partitioned(writer) => {
                writer.close().await.map_err(to_datafusion_error)?
            }
        };

        Ok((data_files, delete_files))
    }
}

/// Execution plan for MERGE operations using Copy-on-Write semantics.
///
/// This plan performs a complete MERGE operation:
/// 1. Scans target table with metadata columns for position tracking
/// 2. Joins with source data using FULL OUTER JOIN semantics
/// 3. Applies WHEN clauses in order to classify actions
/// 4. Writes data files (inserts/updates) and delete files (updates/deletes)
///
/// Output schema: (data_files: Utf8, delete_files: Utf8, inserted_count: UInt64,
///                 updated_count: UInt64, deleted_count: UInt64)
#[derive(Debug)]
pub struct IcebergMergeExec {
    /// The target table
    table: Table,
    /// Arrow schema of the target table
    schema: ArrowSchemaRef,
    /// Source execution plan (e.g., from DataFrame)
    source: Arc<dyn ExecutionPlan>,
    /// ON condition for matching source and target rows
    match_condition: Expr,
    /// WHEN MATCHED clauses (order matters - first match wins)
    when_matched: Vec<WhenMatchedClause>,
    /// WHEN NOT MATCHED clauses for source-only rows
    when_not_matched: Vec<WhenNotMatchedClause>,
    /// WHEN NOT MATCHED BY SOURCE clauses for target-only rows
    when_not_matched_by_source: Vec<WhenNotMatchedBySourceClause>,
    /// Configuration for Dynamic Partition Pruning and other optimizations
    merge_config: MergeConfig,
    /// Output schema for this execution plan
    output_schema: ArrowSchemaRef,
    /// Plan properties for query optimization
    plan_properties: PlanProperties,
}

impl IcebergMergeExec {
    /// Creates a new `IcebergMergeExec`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        table: Table,
        schema: ArrowSchemaRef,
        source: Arc<dyn ExecutionPlan>,
        match_condition: Expr,
        when_matched: Vec<WhenMatchedClause>,
        when_not_matched: Vec<WhenNotMatchedClause>,
        when_not_matched_by_source: Vec<WhenNotMatchedBySourceClause>,
        merge_config: MergeConfig,
    ) -> Self {
        let output_schema = Self::make_output_schema();
        let plan_properties = Self::compute_properties(output_schema.clone());

        Self {
            table,
            schema,
            source,
            match_condition,
            when_matched,
            when_not_matched,
            when_not_matched_by_source,
            merge_config,
            output_schema,
            plan_properties,
        }
    }

    /// Creates the output schema for merge results.
    fn make_output_schema() -> ArrowSchemaRef {
        Arc::new(ArrowSchema::new(vec![
            Field::new(MERGE_DATA_FILES_COL, DataType::Utf8, false),
            Field::new(MERGE_DELETE_FILES_COL, DataType::Utf8, false),
            Field::new(MERGE_INSERTED_COUNT_COL, DataType::UInt64, false),
            Field::new(MERGE_UPDATED_COUNT_COL, DataType::UInt64, false),
            Field::new(MERGE_DELETED_COUNT_COL, DataType::UInt64, false),
            Field::new(MERGE_DPP_APPLIED_COL, DataType::Boolean, false),
            Field::new(MERGE_DPP_PARTITION_COUNT_COL, DataType::UInt64, false),
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

    /// Creates a result batch with merge output (data files, delete files, stats).
    pub fn make_result_batch(
        data_files_json: Vec<String>,
        delete_files_json: Vec<String>,
        stats: &MergeStats,
    ) -> DFResult<RecordBatch> {
        use datafusion::arrow::array::BooleanArray;

        let data_files_str = data_files_json.join("\n");
        let delete_files_str = delete_files_json.join("\n");

        let data_files_array = Arc::new(StringArray::from(vec![data_files_str])) as ArrayRef;
        let delete_files_array = Arc::new(StringArray::from(vec![delete_files_str])) as ArrayRef;
        let inserted_array = Arc::new(datafusion::arrow::array::UInt64Array::from(vec![
            stats.rows_inserted,
        ])) as ArrayRef;
        let updated_array = Arc::new(datafusion::arrow::array::UInt64Array::from(vec![
            stats.rows_updated,
        ])) as ArrayRef;
        let deleted_array = Arc::new(datafusion::arrow::array::UInt64Array::from(vec![
            stats.rows_deleted,
        ])) as ArrayRef;
        let dpp_applied_array = Arc::new(BooleanArray::from(vec![stats.dpp_applied])) as ArrayRef;
        let dpp_partition_count_array = Arc::new(datafusion::arrow::array::UInt64Array::from(vec![
            stats.dpp_partition_count as u64,
        ])) as ArrayRef;

        RecordBatch::try_new(Self::make_output_schema(), vec![
            data_files_array,
            delete_files_array,
            inserted_array,
            updated_array,
            deleted_array,
            dpp_applied_array,
            dpp_partition_count_array,
        ])
        .map_err(|e| {
            DataFusionError::ArrowError(
                Box::new(e),
                Some("Failed to make merge result batch".to_string()),
            )
        })
    }

    /// Returns a human-readable description of the WHEN clauses for display.
    fn format_when_clauses(&self) -> String {
        let mut parts = Vec::new();

        for clause in &self.when_matched {
            let condition = clause
                .condition
                .as_ref()
                .map(|c| format!(" AND {}", c))
                .unwrap_or_default();
            let action = match &clause.action {
                MatchedAction::Update(assignments) => {
                    let assigns: Vec<_> = assignments
                        .iter()
                        .map(|(k, v)| format!("{}={}", k, v))
                        .collect();
                    format!("UPDATE SET {}", assigns.join(", "))
                }
                MatchedAction::UpdateAll => "UPDATE *".to_string(),
                MatchedAction::Delete => "DELETE".to_string(),
            };
            parts.push(format!("WHEN MATCHED{} THEN {}", condition, action));
        }

        for clause in &self.when_not_matched {
            let condition = clause
                .condition
                .as_ref()
                .map(|c| format!(" AND {}", c))
                .unwrap_or_default();
            let action = match &clause.action {
                NotMatchedAction::Insert(values) => {
                    let vals: Vec<_> = values.iter().map(|(k, v)| format!("{}={}", k, v)).collect();
                    format!("INSERT ({})", vals.join(", "))
                }
                NotMatchedAction::InsertAll => "INSERT *".to_string(),
            };
            parts.push(format!("WHEN NOT MATCHED{} THEN {}", condition, action));
        }

        for clause in &self.when_not_matched_by_source {
            let condition = clause
                .condition
                .as_ref()
                .map(|c| format!(" AND {}", c))
                .unwrap_or_default();
            let action = match &clause.action {
                NotMatchedBySourceAction::Update(assignments) => {
                    let assigns: Vec<_> = assignments
                        .iter()
                        .map(|(k, v)| format!("{}={}", k, v))
                        .collect();
                    format!("UPDATE SET {}", assigns.join(", "))
                }
                NotMatchedBySourceAction::Delete => "DELETE".to_string(),
            };
            parts.push(format!(
                "WHEN NOT MATCHED BY SOURCE{} THEN {}",
                condition, action
            ));
        }

        parts.join(", ")
    }
}

impl DisplayAs for IcebergMergeExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "IcebergMergeExec: table={}, on=[{}], clauses=[{}]",
                    self.table.identifier(),
                    self.match_condition,
                    self.format_when_clauses()
                )
            }
            DisplayFormatType::Verbose => {
                write!(
                    f,
                    "IcebergMergeExec: table={}, on=[{}], clauses=[{}], schema={:?}",
                    self.table.identifier(),
                    self.match_condition,
                    self.format_when_clauses(),
                    self.schema
                )
            }
        }
    }
}

impl ExecutionPlan for IcebergMergeExec {
    fn name(&self) -> &str {
        "IcebergMergeExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.source]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(
                "IcebergMergeExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(IcebergMergeExec::new(
            self.table.clone(),
            self.schema.clone(),
            children[0].clone(),
            self.match_condition.clone(),
            self.when_matched.clone(),
            self.when_not_matched.clone(),
            self.when_not_matched_by_source.clone(),
            self.merge_config.clone(),
        )))
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        // Clone data needed for the async block
        let table = self.table.clone();
        let schema = self.schema.clone();
        let source = self.source.clone();
        let match_condition = self.match_condition.clone();
        let when_matched = self.when_matched.clone();
        let when_not_matched = self.when_not_matched.clone();
        let when_not_matched_by_source = self.when_not_matched_by_source.clone();
        let merge_config = self.merge_config.clone();
        let output_schema = self.output_schema.clone();

        let stream = futures::stream::once(async move {
            execute_merge(
                table,
                schema,
                source,
                partition,
                context,
                match_condition,
                when_matched,
                when_not_matched,
                when_not_matched_by_source,
                merge_config,
            )
            .await
        })
        .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            stream,
        )))
    }
}

// ============================================================================
// Core MERGE Execution Logic
// ============================================================================

/// Executes the MERGE operation: scan target, join with source, classify rows, write files.
///
/// This function performs the following steps:
/// 1. Scans target table with metadata columns (_file_path, _row_pos)
/// 2. Materializes source data from the execution plan
/// 3. Performs a FULL OUTER JOIN on the match condition
/// 4. Classifies each row based on NULL patterns after the join
/// 5. Applies WHEN clauses to determine actions (INSERT, UPDATE, DELETE, NO_ACTION)
/// 6. Writes data files (for UPDATE) and position delete files (for UPDATE/DELETE)
///
/// # IMPORTANT Implementation Notes
///
/// A) Concurrency safety:
///    - Capture baseline_snapshot_id at plan construction time
///    - Validate baseline before commit (reject if table modified since scan)
///    - Use row_delta() for atomic commit of data + delete files in single snapshot
///
/// B) Partition column constraint:
///    - MergeBuilder::validate() already checks for partition column updates
///    - Runtime validation ensures output partition values match input
///
/// C) Metadata column correctness:
///    - add_metadata_columns() assumes per-file batches with sequential positions
///    - Track row_offset per file during scan, reset for each new file
#[allow(clippy::too_many_arguments)]
async fn execute_merge(
    table: Table,
    schema: ArrowSchemaRef,
    source: Arc<dyn ExecutionPlan>,
    partition: usize,
    context: Arc<TaskContext>,
    match_condition: Expr,
    when_matched: Vec<WhenMatchedClause>,
    when_not_matched: Vec<WhenNotMatchedClause>,
    when_not_matched_by_source: Vec<WhenNotMatchedBySourceClause>,
    merge_config: MergeConfig,
) -> DFResult<RecordBatch> {
    // === Step 1: Extract join keys from match condition (supports compound keys) ===
    // We need join keys early for DPP computation
    let join_keys = extract_join_keys(&match_condition)?;

    // === Step 2: Materialize source data FIRST (required for DPP) ===
    let source_batches = materialize_source(source, partition, context).await?;

    // Memory warning for large MERGE operations (debug builds only)
    // Note: Streaming execution (Phase 3) will address memory bounds for very large merges
    #[cfg(debug_assertions)]
    {
        let source_row_count: usize = source_batches.iter().map(|b| b.num_rows()).sum();
        const LARGE_MERGE_THRESHOLD: usize = 1_000_000;
        if source_row_count > LARGE_MERGE_THRESHOLD {
            eprintln!(
                "MERGE: Large source dataset ({} rows). Memory usage may be significant. \
                 Consider batching source data for very large merges. \
                 DPP is {} to help reduce target scan scope.",
                source_row_count,
                if merge_config.enable_dpp {
                    "enabled"
                } else {
                    "disabled"
                }
            );
        }
    }

    if source_batches.is_empty() && when_not_matched_by_source.is_empty() {
        // No source rows and no DELETE BY SOURCE clause - nothing to do
        return IcebergMergeExec::make_result_batch(vec![], vec![], &MergeStats::default());
    }

    // === Step 3: Compute Dynamic Partition Pruning (DPP) filter ===
    let dpp_result = compute_dpp_filter(&source_batches, &join_keys, &table, &merge_config)?;

    // Log DPP status for debugging (will appear in debug builds)
    #[cfg(debug_assertions)]
    if let Some(ref col) = dpp_result.partition_column {
        if dpp_result.predicate.is_some() {
            eprintln!(
                "MERGE DPP: Enabled on column '{}', pruning to {} partition(s)",
                col, dpp_result.partition_count
            );
        } else if dpp_result.partition_count > merge_config.dpp_max_partitions {
            eprintln!(
                "MERGE DPP: Disabled - {} partitions exceeds threshold {}",
                dpp_result.partition_count, merge_config.dpp_max_partitions
            );
        }
    }

    // Capture DPP stats before consuming predicate
    let dpp_applied = dpp_result.predicate.is_some();
    let dpp_partition_count = dpp_result.partition_count;

    // === Step 4: Scan target table with metadata columns (with DPP filter) ===
    let target_batches = scan_target_with_metadata(&table, &schema, dpp_result.predicate).await?;

    if target_batches.is_empty() && when_not_matched.is_empty() {
        // No target rows and no INSERT clause - nothing to do
        // Still report DPP stats even on early return
        let stats = MergeStats {
            dpp_applied,
            dpp_partition_count,
            ..Default::default()
        };
        return IcebergMergeExec::make_result_batch(vec![], vec![], &stats);
    }

    // === Step 5: Perform FULL OUTER JOIN ===
    let joined_batches =
        perform_full_outer_join(&target_batches, &source_batches, &schema, &join_keys).await?;

    // === Step 6: Set up partition-aware writers for file output ===
    let mut writers = MergeWriters::new(&table).await?;

    // === Step 7: Process joined rows and write files ===
    let mut stats = MergeStats {
        dpp_applied,
        dpp_partition_count,
        ..Default::default()
    };

    // Build prefixed key column names for all join keys
    let prefixed_join_keys: Vec<(String, String)> = join_keys
        .iter()
        .map(|(t, s)| {
            (
                format!("{}{}", TARGET_PREFIX, t),
                format!("{}{}", SOURCE_PREFIX, s),
            )
        })
        .collect();

    // Metadata column names after prefix
    let file_path_col_name = format!("{}{}", TARGET_PREFIX, MERGE_FILE_PATH_COL);
    let row_pos_col_name = format!("{}{}", TARGET_PREFIX, MERGE_ROW_POS_COL);

    for batch in &joined_batches {
        // Process MATCHED rows (all keys non-null on both sides)
        process_matched_rows(
            batch,
            &prefixed_join_keys,
            &file_path_col_name,
            &row_pos_col_name,
            &schema,
            &when_matched,
            &mut writers,
            &mut stats,
        )
        .await?;

        // Process NOT_MATCHED rows (any target key null, all source keys non-null)
        process_not_matched_rows(
            batch,
            &prefixed_join_keys,
            &schema,
            &when_not_matched,
            &mut writers,
            &mut stats,
        )
        .await?;

        // Process NOT_MATCHED_BY_SOURCE rows (all target keys non-null, any source key null)
        process_not_matched_by_source_rows(
            batch,
            &prefixed_join_keys,
            &file_path_col_name,
            &row_pos_col_name,
            &schema,
            &when_not_matched_by_source,
            &mut writers,
            &mut stats,
        )
        .await?;
    }

    // === Step 8: Close writers and collect files ===
    // Build partition type map for proper serialization with partition evolution
    let partition_types = build_partition_type_map(&table)?;
    let current_spec_id = table.metadata().default_partition_spec_id();
    let format_version = table.metadata().format_version();

    let (data_files, delete_files) = writers.close().await?;

    // Serialize to JSON with partition spec wrapper for partition evolution support
    let partition_type = partition_types.get(&current_spec_id).ok_or_else(|| {
        DataFusionError::Internal(format!(
            "Missing partition type for current partition spec {}",
            current_spec_id
        ))
    })?;

    let data_files_json: Vec<String> = data_files
        .into_iter()
        .map(|df| {
            let file_json = serialize_data_file_to_json(df, partition_type, format_version)
                .map_err(to_datafusion_error)?;
            serde_json::to_string(&SerializedFileWithSpec {
                spec_id: current_spec_id,
                file_json,
            })
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to serialize data file metadata: {e}"
                ))
            })
        })
        .collect::<DFResult<Vec<_>>>()?;

    let delete_files_json: Vec<String> = delete_files
        .into_iter()
        .map(|df| {
            let file_json = serialize_data_file_to_json(df, partition_type, format_version)
                .map_err(to_datafusion_error)?;
            serde_json::to_string(&SerializedFileWithSpec {
                spec_id: current_spec_id,
                file_json,
            })
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to serialize delete file metadata: {e}"
                ))
            })
        })
        .collect::<DFResult<Vec<_>>>()?;

    // Update file counts in stats
    stats.files_added = data_files_json.len() as u64;
    stats.files_removed = delete_files_json.len() as u64;

    IcebergMergeExec::make_result_batch(data_files_json, delete_files_json, &stats)
}

// ============================================================================
// Dynamic Partition Pruning (DPP) Functions
// ============================================================================

/// Result of DPP analysis, containing the filter predicate and statistics.
#[derive(Debug)]
pub struct DppResult {
    /// The partition filter predicate (if applicable)
    pub predicate: Option<Predicate>,
    /// Number of distinct partition values found in source
    pub partition_count: usize,
    /// Name of the partition column used for DPP (if applicable)
    pub partition_column: Option<String>,
}

/// Analyzes source data to extract partition values for Dynamic Partition Pruning.
///
/// This function:
/// 1. Identifies if any join key is a partition column
/// 2. Extracts distinct partition values from source data
/// 3. Builds an IN predicate for partition pruning
///
/// # Arguments
///
/// * `source_batches` - Materialized source data
/// * `join_keys` - Join key column names (target, source) pairs
/// * `table` - Target table (for partition spec)
/// * `config` - MERGE configuration with DPP settings
///
/// # Returns
///
/// DppResult containing the filter predicate (if applicable) and statistics.
fn compute_dpp_filter(
    source_batches: &[RecordBatch],
    join_keys: &[(String, String)],
    table: &Table,
    config: &MergeConfig,
) -> DFResult<DppResult> {
    if !config.enable_dpp {
        return Ok(DppResult {
            predicate: None,
            partition_count: 0,
            partition_column: None,
        });
    }

    let partition_spec = table.metadata().default_partition_spec();
    if partition_spec.is_unpartitioned() {
        return Ok(DppResult {
            predicate: None,
            partition_count: 0,
            partition_column: None,
        });
    }

    let iceberg_schema = table.metadata().current_schema();

    // Find partition columns that are join keys
    // For identity partitioning: partition column name == source column name
    // Note: We only support identity transforms for DPP (bucket/truncate/etc. are not supported)
    let identity_fields: Vec<_> = partition_spec
        .fields()
        .iter()
        .filter(|f| f.transform == Transform::Identity)
        .collect();

    // Log when non-identity transforms are present (they won't be used for DPP)
    #[cfg(debug_assertions)]
    {
        let non_identity: Vec<_> = partition_spec
            .fields()
            .iter()
            .filter(|f| f.transform != Transform::Identity)
            .collect();
        #[cfg(debug_assertions)]
        if !non_identity.is_empty() {
            let transforms: Vec<_> = non_identity
                .iter()
                .filter_map(|f| {
                    iceberg_schema
                        .field_by_id(f.source_id)
                        .map(|sf| format!("{}={:?}", sf.name, f.transform))
                })
                .collect();
            eprintln!(
                "MERGE DPP: Skipping non-identity partition columns [{}] (only identity supported)",
                transforms.join(", ")
            );
        }
    }

    let partition_source_ids: std::collections::HashSet<i32> =
        identity_fields.iter().map(|f| f.source_id).collect();

    // Map partition source IDs to column names
    let partition_col_names: std::collections::HashMap<String, i32> = partition_source_ids
        .iter()
        .filter_map(|&id| {
            iceberg_schema
                .field_by_id(id)
                .map(|f| (f.name.to_string(), id))
        })
        .collect();

    // Find first join key that is a partition column
    let mut partition_join_key: Option<(&String, &String)> = None;
    for (target_key, source_key) in join_keys {
        if partition_col_names.contains_key(target_key) {
            partition_join_key = Some((target_key, source_key));
            break;
        }
    }

    let (target_col, source_col) = match partition_join_key {
        Some(keys) => keys,
        None => {
            // No join key is a partition column - DPP not applicable
            #[cfg(debug_assertions)]
            {
                let join_key_names: Vec<_> = join_keys.iter().map(|(t, _)| t.as_str()).collect();
                let partition_names: Vec<_> = partition_col_names.keys().collect();
                eprintln!(
                    "MERGE DPP: Not applicable - join keys [{}] do not include partition columns [{}]",
                    join_key_names.join(", "),
                    partition_names
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
            return Ok(DppResult {
                predicate: None,
                partition_count: 0,
                partition_column: None,
            });
        }
    };

    // Extract distinct partition values from source
    let distinct_values = extract_distinct_values(source_batches, source_col)?;

    if distinct_values.is_empty() {
        return Ok(DppResult {
            predicate: None,
            partition_count: 0,
            partition_column: Some(target_col.clone()),
        });
    }

    let partition_count = distinct_values.len();

    // Check if we exceed the threshold
    if partition_count > config.dpp_max_partitions {
        // Too many partitions - fall back to full scan
        return Ok(DppResult {
            predicate: None,
            partition_count,
            partition_column: Some(target_col.clone()),
        });
    }

    // Build IN predicate: target_col IN (val1, val2, ...)
    let predicate = Reference::new(target_col.clone()).is_in(distinct_values);

    Ok(DppResult {
        predicate: Some(predicate),
        partition_count,
        partition_column: Some(target_col.clone()),
    })
}

/// Extracts distinct values from a column in the source batches.
///
/// Converts Arrow array values to Iceberg Datum for predicate construction.
fn extract_distinct_values(
    batches: &[RecordBatch],
    column_name: &str,
) -> DFResult<Vec<iceberg::spec::Datum>> {
    use std::collections::HashSet;

    use datafusion::arrow::array::*;

    if batches.is_empty() {
        return Ok(vec![]);
    }

    // Find column index
    let schema = batches[0].schema();
    let col_idx = schema.index_of(column_name).map_err(|_| {
        DataFusionError::Internal(format!(
            "DPP: Source column '{}' not found in schema",
            column_name
        ))
    })?;

    let data_type = schema.field(col_idx).data_type().clone();

    // Use HashSet to collect distinct string representations, then convert to Datum
    // This approach handles all types uniformly
    let mut seen_strings: HashSet<String> = HashSet::new();
    let mut values: Vec<iceberg::spec::Datum> = Vec::new();

    for batch in batches {
        let col = batch.column(col_idx);

        for row_idx in 0..col.len() {
            if col.is_null(row_idx) {
                continue;
            }

            let datum =
                match &data_type {
                    DataType::Int32 => {
                        let arr = col.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                            DataFusionError::Internal("Expected Int32Array".into())
                        })?;
                        let val = arr.value(row_idx);
                        let key = val.to_string();
                        if seen_strings.insert(key) {
                            Some(iceberg::spec::Datum::int(val))
                        } else {
                            None
                        }
                    }
                    DataType::Int64 => {
                        let arr = col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                            DataFusionError::Internal("Expected Int64Array".into())
                        })?;
                        let val = arr.value(row_idx);
                        let key = val.to_string();
                        if seen_strings.insert(key) {
                            Some(iceberg::spec::Datum::long(val))
                        } else {
                            None
                        }
                    }
                    DataType::Utf8 => {
                        let arr = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                            DataFusionError::Internal("Expected StringArray".into())
                        })?;
                        let val = arr.value(row_idx);
                        if seen_strings.insert(val.to_string()) {
                            Some(iceberg::spec::Datum::string(val))
                        } else {
                            None
                        }
                    }
                    DataType::LargeUtf8 => {
                        let arr =
                            col.as_any()
                                .downcast_ref::<LargeStringArray>()
                                .ok_or_else(|| {
                                    DataFusionError::Internal("Expected LargeStringArray".into())
                                })?;
                        let val = arr.value(row_idx);
                        if seen_strings.insert(val.to_string()) {
                            Some(iceberg::spec::Datum::string(val))
                        } else {
                            None
                        }
                    }
                    DataType::Date32 => {
                        let arr = col.as_any().downcast_ref::<Date32Array>().ok_or_else(|| {
                            DataFusionError::Internal("Expected Date32Array".into())
                        })?;
                        let val = arr.value(row_idx);
                        let key = val.to_string();
                        if seen_strings.insert(key) {
                            Some(iceberg::spec::Datum::date(val))
                        } else {
                            None
                        }
                    }
                    other => {
                        // For unsupported types, skip DPP rather than fail
                        #[cfg(debug_assertions)]
                        eprintln!(
                            "MERGE DPP: Skipping - unsupported data type {:?} for column '{}' \
                         (supported: Int32, Int64, Utf8, LargeUtf8, Date32)",
                            other, column_name
                        );
                        return Ok(vec![]);
                    }
                };

            if let Some(d) = datum {
                values.push(d);
            }
        }
    }

    Ok(values)
}

/// Scans the target table and adds metadata columns (_file_path, _row_pos).
///
/// Uses the FileScanTask iteration pattern from UPDATE implementation.
/// Optionally applies a partition filter for Dynamic Partition Pruning.
async fn scan_target_with_metadata(
    table: &Table,
    _schema: &ArrowSchemaRef,
    partition_filter: Option<Predicate>,
) -> DFResult<Vec<RecordBatch>> {
    // Build the scan - select all columns
    let mut scan_builder = table.scan().select_all();

    // Apply DPP filter if provided
    if let Some(predicate) = partition_filter {
        scan_builder = scan_builder.with_filter(predicate);
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
        return Ok(vec![]);
    }

    let file_io = table.file_io().clone();
    let mut all_batches = Vec::new();

    // Process each file and add metadata columns
    for task in file_scan_tasks {
        let file_path = task.data_file_path().to_string();

        // Create a fresh reader for each file (ArrowReader doesn't implement Clone)
        let reader = iceberg::arrow::ArrowReaderBuilder::new(file_io.clone()).build();

        // Read batches from this file
        let task_stream =
            Box::pin(futures::stream::iter(vec![Ok(task)])) as iceberg::scan::FileScanTaskStream;
        let mut batch_stream = reader
            .read(task_stream)
            .map_err(to_datafusion_error)?
            .map(|r| r.map_err(to_datafusion_error));

        let mut row_offset = 0i64;
        while let Some(batch_result) = batch_stream.next().await {
            let batch = batch_result?;
            let batch_size = batch.num_rows();

            // Add metadata columns
            let batch_with_meta = add_metadata_columns(batch, &file_path, row_offset)?;
            all_batches.push(batch_with_meta);

            row_offset += batch_size as i64;
        }
    }

    Ok(all_batches)
}

/// Materializes source data from the execution plan.
async fn materialize_source(
    source: Arc<dyn ExecutionPlan>,
    partition: usize,
    context: Arc<TaskContext>,
) -> DFResult<Vec<RecordBatch>> {
    let stream = source.execute(partition, context)?;
    let batches: Vec<RecordBatch> = stream.try_collect().await?;
    Ok(batches)
}
/// Processes MATCHED rows from joined batch and writes files.
///
/// For each MATCHED row (where both target and source keys are non-null):
/// - If action is UPDATE: Write transformed data to data file + position delete
/// - If action is DELETE: Write position delete only
///
/// For partitioned tables, position deletes are routed to the correct partition
/// based on the partition key computed from the target row data.
///
/// # Arguments
/// * `batch` - The joined batch containing both target and source columns
/// * `target_key` - Prefixed target key column name
/// * `source_key` - Prefixed source key column name
/// * `file_path_col` - Prefixed file path metadata column name
/// * `row_pos_col` - Prefixed row position metadata column name
/// * `target_schema` - Original target table schema (for output)
/// * `when_matched` - WHEN MATCHED clauses
/// * `writers` - Partition-aware writers for data and delete files
/// * `stats` - Statistics to update
#[allow(clippy::too_many_arguments)]
async fn process_matched_rows(
    batch: &RecordBatch,
    join_keys: &[(String, String)],
    file_path_col: &str,
    row_pos_col: &str,
    target_schema: &ArrowSchemaRef,
    when_matched: &[WhenMatchedClause],
    writers: &mut MergeWriters,
    stats: &mut MergeStats,
) -> DFResult<()> {
    if when_matched.is_empty() {
        return Ok(());
    }

    let batch_schema = batch.schema();

    // Find all key column indices for compound key support
    let key_indices: Vec<(usize, usize)> = join_keys
        .iter()
        .map(|(t, s)| {
            let target_idx = batch_schema
                .index_of(t)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            let source_idx = batch_schema
                .index_of(s)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            Ok((target_idx, source_idx))
        })
        .collect::<DFResult<Vec<_>>>()?;

    // Find metadata columns
    let file_path_idx = batch_schema
        .index_of(file_path_col)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
    let row_pos_idx = batch_schema
        .index_of(row_pos_col)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    let file_path_col_arr = batch
        .column(file_path_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Internal("file_path column must be StringArray".into()))?;
    let row_pos_col_arr = batch
        .column(row_pos_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Internal("row_pos column must be Int64Array".into()))?;

    // Collect all MATCHED row indices (all keys non-null on both sides)
    let mut matched_indices: Vec<usize> = Vec::new();
    for row_idx in 0..batch.num_rows() {
        // For compound keys: target is null if ANY target key is null
        let target_is_null = key_indices
            .iter()
            .any(|(t_idx, _)| batch.column(*t_idx).is_null(row_idx));
        // For compound keys: source is null if ANY source key is null
        let source_is_null = key_indices
            .iter()
            .any(|(_, s_idx)| batch.column(*s_idx).is_null(row_idx));

        if !target_is_null && !source_is_null {
            matched_indices.push(row_idx);
        }
    }

    if matched_indices.is_empty() {
        return Ok(());
    }

    // Create DFSchema for condition evaluation
    let df_schema = DFSchema::try_from(batch_schema.as_ref().clone())?;

    // Track which rows have been processed (first matching clause wins)
    let mut remaining_indices: Vec<usize> = matched_indices;

    // Process WHEN MATCHED clauses in order (first match wins)
    for clause in when_matched {
        if remaining_indices.is_empty() {
            break;
        }

        // Evaluate condition to find which rows this clause applies to
        let clause_indices =
            evaluate_condition_for_rows(&clause.condition, batch, &remaining_indices, &df_schema)?;

        if clause_indices.is_empty() {
            continue;
        }

        // Remove processed indices from remaining
        let clause_set: std::collections::HashSet<usize> = clause_indices.iter().copied().collect();
        remaining_indices.retain(|idx| !clause_set.contains(idx));

        // Process based on action type
        match &clause.action {
            MatchedAction::Delete => {
                // For DELETE: only write position deletes
                write_position_deletes_partitioned(
                    batch,
                    &clause_indices,
                    file_path_col_arr,
                    row_pos_col_arr,
                    target_schema,
                    writers,
                )
                .await?;

                stats.rows_deleted += clause_indices.len() as u64;
            }
            MatchedAction::UpdateAll => {
                // For UPDATE *: Use source columns to create updated rows
                let data_batch =
                    extract_source_data_for_update(batch, &clause_indices, target_schema)?;

                if data_batch.num_rows() > 0 {
                    writers.write_data(data_batch).await?;
                }

                // Write position deletes for original rows
                write_position_deletes_partitioned(
                    batch,
                    &clause_indices,
                    file_path_col_arr,
                    row_pos_col_arr,
                    target_schema,
                    writers,
                )
                .await?;

                stats.rows_updated += clause_indices.len() as u64;
            }
            MatchedAction::Update(assignments) => {
                // For UPDATE with assignments: Apply SET expressions
                let data_batch =
                    apply_update_assignments(batch, &clause_indices, assignments, target_schema)?;

                if data_batch.num_rows() > 0 {
                    writers.write_data(data_batch).await?;
                }

                // Write position deletes for original rows
                write_position_deletes_partitioned(
                    batch,
                    &clause_indices,
                    file_path_col_arr,
                    row_pos_col_arr,
                    target_schema,
                    writers,
                )
                .await?;

                stats.rows_updated += clause_indices.len() as u64;
            }
        }
    }

    Ok(())
}

/// Writes position deletes with partition awareness.
///
/// For unpartitioned tables, writes all deletes at once.
/// For partitioned tables, groups deletes by partition key (from file's original partition)
/// and writes each group separately. This is partition-evolution-aware.
#[allow(clippy::too_many_arguments)]
async fn write_position_deletes_partitioned(
    _batch: &RecordBatch,
    indices: &[usize],
    file_path_col: &StringArray,
    row_pos_col: &Int64Array,
    _target_schema: &ArrowSchemaRef,
    writers: &mut MergeWriters,
) -> DFResult<()> {
    if indices.is_empty() {
        return Ok(());
    }

    let delete_schema = writers.delete_schema().clone();

    if !writers.is_partitioned() {
        // Unpartitioned: write all deletes at once
        let file_paths: Vec<String> = indices
            .iter()
            .map(|&idx| file_path_col.value(idx).to_string())
            .collect();
        let positions: Vec<i64> = indices.iter().map(|&idx| row_pos_col.value(idx)).collect();

        let delete_batch = make_position_delete_batch(&file_paths, &positions, delete_schema)?;
        writers.write_deletes(delete_batch, None).await?;
    } else {
        // Partitioned: group deletes by partition key (from file's original partition)
        // Use file partition map for partition-evolution-aware grouping
        let mut partition_groups: HashMap<String, (Vec<String>, Vec<i64>, PartitionKey)> =
            HashMap::new();

        for &idx in indices {
            let file_path = file_path_col.value(idx).to_string();
            let pos = row_pos_col.value(idx);

            // Get partition key from file's original partition info (not computed from row values)
            let partition_key = writers
                .get_partition_key_for_file(&file_path)?
                .ok_or_else(|| {
                    DataFusionError::Internal("Expected partition key for partitioned table".into())
                })?;

            // Use partition key's string representation as group key
            let key_str = format!("{:?}", partition_key.data());

            let entry = partition_groups
                .entry(key_str)
                .or_insert_with(|| (Vec::new(), Vec::new(), partition_key.clone()));
            entry.0.push(file_path);
            entry.1.push(pos);
        }

        // Write each partition's deletes
        for (_key_str, (file_paths, positions, partition_key)) in partition_groups {
            let delete_batch =
                make_position_delete_batch(&file_paths, &positions, delete_schema.clone())?;
            writers
                .write_deletes(delete_batch, Some(partition_key))
                .await?;
        }
    }

    Ok(())
}

/// Processes NOT_MATCHED rows from joined batch and writes INSERT files.
///
/// For each NOT_MATCHED row (where target key is null and source key is non-null):
/// - If action is INSERT *: Write source data to data file
/// - If action is INSERT with values: Evaluate expressions and write to data file
///
/// For partitioned tables, data is automatically routed to the correct partition
/// based on the partition key computed from the inserted row data.
///
/// # Arguments
/// * `batch` - The joined batch containing both target and source columns
/// * `target_key` - Prefixed target key column name
/// * `source_key` - Prefixed source key column name
/// * `target_schema` - Original target table schema (for output)
/// * `when_not_matched` - WHEN NOT MATCHED clauses
/// * `writers` - Partition-aware writers for data files
/// * `stats` - Statistics to update
#[allow(clippy::too_many_arguments)]
async fn process_not_matched_rows(
    batch: &RecordBatch,
    join_keys: &[(String, String)],
    target_schema: &ArrowSchemaRef,
    when_not_matched: &[WhenNotMatchedClause],
    writers: &mut MergeWriters,
    stats: &mut MergeStats,
) -> DFResult<()> {
    if when_not_matched.is_empty() {
        return Ok(());
    }

    let batch_schema = batch.schema();

    // Find all key column indices for compound key support
    let key_indices: Vec<(usize, usize)> = join_keys
        .iter()
        .map(|(t, s)| {
            let target_idx = batch_schema
                .index_of(t)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            let source_idx = batch_schema
                .index_of(s)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            Ok((target_idx, source_idx))
        })
        .collect::<DFResult<Vec<_>>>()?;

    // Collect all NOT_MATCHED row indices (any target key null, all source keys non-null)
    let mut not_matched_indices: Vec<usize> = Vec::new();
    for row_idx in 0..batch.num_rows() {
        // For compound keys: target is null if ANY target key is null
        let target_is_null = key_indices
            .iter()
            .any(|(t_idx, _)| batch.column(*t_idx).is_null(row_idx));
        // For compound keys: source is null if ANY source key is null
        let source_is_null = key_indices
            .iter()
            .any(|(_, s_idx)| batch.column(*s_idx).is_null(row_idx));

        if target_is_null && !source_is_null {
            not_matched_indices.push(row_idx);
        }
    }

    if not_matched_indices.is_empty() {
        return Ok(());
    }

    // Create DFSchema for condition evaluation
    let df_schema = DFSchema::try_from(batch_schema.as_ref().clone())?;

    // Track which rows have been processed (first matching clause wins)
    let mut remaining_indices: Vec<usize> = not_matched_indices;

    // Process WHEN NOT MATCHED clauses in order (first match wins)
    for clause in when_not_matched {
        if remaining_indices.is_empty() {
            break;
        }

        // Evaluate condition to find which rows this clause applies to
        let clause_indices =
            evaluate_condition_for_rows(&clause.condition, batch, &remaining_indices, &df_schema)?;

        if clause_indices.is_empty() {
            continue;
        }

        // Remove processed indices from remaining
        let clause_set: std::collections::HashSet<usize> = clause_indices.iter().copied().collect();
        remaining_indices.retain(|idx| !clause_set.contains(idx));

        // Process based on action type
        match &clause.action {
            NotMatchedAction::InsertAll => {
                // For INSERT *: Use source columns to create new rows
                let data_batch =
                    extract_source_data_for_insert(batch, &clause_indices, target_schema)?;

                if data_batch.num_rows() > 0 {
                    // write_data handles partition routing automatically
                    writers.write_data(data_batch).await?;
                }

                stats.rows_inserted += clause_indices.len() as u64;
            }
            NotMatchedAction::Insert(values) => {
                // For INSERT with explicit values: Evaluate expressions
                let data_batch =
                    apply_insert_values(batch, &clause_indices, values, target_schema)?;

                if data_batch.num_rows() > 0 {
                    // write_data handles partition routing automatically
                    writers.write_data(data_batch).await?;
                }

                stats.rows_inserted += clause_indices.len() as u64;
            }
        }
    }

    Ok(())
}

/// Processes NOT_MATCHED_BY_SOURCE rows from joined batch and writes files.
///
/// For each NOT_MATCHED_BY_SOURCE row (where target key is non-null and source key is null):
/// - If action is DELETE: Write position delete only
/// - If action is UPDATE: Write updated data + position delete
///
/// For partitioned tables, position deletes and data files are routed to the correct partition
/// based on the partition key computed from the target row data.
///
/// # Arguments
/// * `batch` - The joined batch containing both target and source columns
/// * `target_key` - Prefixed target key column name
/// * `source_key` - Prefixed source key column name
/// * `file_path_col` - Prefixed file path metadata column name
/// * `row_pos_col` - Prefixed row position metadata column name
/// * `target_schema` - Original target table schema (for output)
/// * `when_not_matched_by_source` - WHEN NOT MATCHED BY SOURCE clauses
/// * `writers` - Partition-aware writers for data and delete files
/// * `stats` - Statistics to update
#[allow(clippy::too_many_arguments)]
async fn process_not_matched_by_source_rows(
    batch: &RecordBatch,
    join_keys: &[(String, String)],
    file_path_col: &str,
    row_pos_col: &str,
    target_schema: &ArrowSchemaRef,
    when_not_matched_by_source: &[WhenNotMatchedBySourceClause],
    writers: &mut MergeWriters,
    stats: &mut MergeStats,
) -> DFResult<()> {
    if when_not_matched_by_source.is_empty() {
        return Ok(());
    }

    let batch_schema = batch.schema();

    // Find all key column indices for compound key support
    let key_indices: Vec<(usize, usize)> = join_keys
        .iter()
        .map(|(t, s)| {
            let target_idx = batch_schema
                .index_of(t)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            let source_idx = batch_schema
                .index_of(s)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            Ok((target_idx, source_idx))
        })
        .collect::<DFResult<Vec<_>>>()?;

    // Find metadata columns
    let file_path_idx = batch_schema
        .index_of(file_path_col)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
    let row_pos_idx = batch_schema
        .index_of(row_pos_col)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    let file_path_col_arr = batch
        .column(file_path_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Internal("file_path column must be StringArray".into()))?;
    let row_pos_col_arr = batch
        .column(row_pos_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Internal("row_pos column must be Int64Array".into()))?;

    // Collect all NOT_MATCHED_BY_SOURCE row indices (all target keys non-null, any source key null)
    let mut not_matched_by_source_indices: Vec<usize> = Vec::new();
    for row_idx in 0..batch.num_rows() {
        // For compound keys: target is null if ANY target key is null
        let target_is_null = key_indices
            .iter()
            .any(|(t_idx, _)| batch.column(*t_idx).is_null(row_idx));
        // For compound keys: source is null if ANY source key is null
        let source_is_null = key_indices
            .iter()
            .any(|(_, s_idx)| batch.column(*s_idx).is_null(row_idx));

        if !target_is_null && source_is_null {
            not_matched_by_source_indices.push(row_idx);
        }
    }

    if not_matched_by_source_indices.is_empty() {
        return Ok(());
    }

    // Create DFSchema for condition evaluation
    let df_schema = DFSchema::try_from(batch_schema.as_ref().clone())?;

    // Track which rows have been processed (first matching clause wins)
    let mut remaining_indices: Vec<usize> = not_matched_by_source_indices;

    // Process WHEN NOT MATCHED BY SOURCE clauses in order (first match wins)
    for clause in when_not_matched_by_source {
        if remaining_indices.is_empty() {
            break;
        }

        // Evaluate condition to find which rows this clause applies to
        let clause_indices =
            evaluate_condition_for_rows(&clause.condition, batch, &remaining_indices, &df_schema)?;

        if clause_indices.is_empty() {
            continue;
        }

        // Remove processed indices from remaining
        let clause_set: std::collections::HashSet<usize> = clause_indices.iter().copied().collect();
        remaining_indices.retain(|idx| !clause_set.contains(idx));

        // Process based on action type
        match &clause.action {
            NotMatchedBySourceAction::Delete => {
                // For DELETE: only write position deletes
                write_position_deletes_partitioned(
                    batch,
                    &clause_indices,
                    file_path_col_arr,
                    row_pos_col_arr,
                    target_schema,
                    writers,
                )
                .await?;

                stats.rows_deleted += clause_indices.len() as u64;
            }
            NotMatchedBySourceAction::Update(assignments) => {
                // For UPDATE with assignments: Apply SET expressions using target columns
                let data_batch = apply_not_matched_by_source_update(
                    batch,
                    &clause_indices,
                    assignments,
                    target_schema,
                )?;

                if data_batch.num_rows() > 0 {
                    writers.write_data(data_batch).await?;
                }

                // Write position deletes for original rows
                write_position_deletes_partitioned(
                    batch,
                    &clause_indices,
                    file_path_col_arr,
                    row_pos_col_arr,
                    target_schema,
                    writers,
                )
                .await?;

                stats.rows_updated += clause_indices.len() as u64;
            }
        }
    }

    Ok(())
}

/// Applies UPDATE assignments for NOT_MATCHED_BY_SOURCE rows.
///
/// These rows only have target column values (source is NULL), so assignments
/// can only reference target columns.
fn apply_not_matched_by_source_update(
    joined_batch: &RecordBatch,
    indices: &[usize],
    assignments: &[(String, Expr)],
    target_schema: &ArrowSchemaRef,
) -> DFResult<RecordBatch> {
    use datafusion::arrow::compute::take;

    // Build indices array for filtering
    let indices_arr = datafusion::arrow::array::UInt32Array::from(
        indices.iter().map(|&i| i as u32).collect::<Vec<_>>(),
    );

    // Compile expressions for assignments
    let df_schema = DFSchema::try_from(joined_batch.schema().as_ref().clone())?;
    let ctx = SessionContext::new();
    let execution_props = ctx.state().execution_props().clone();

    let mut compiled_assignments: HashMap<String, Arc<dyn PhysicalExpr>> = HashMap::new();
    for (col, expr) in assignments {
        let phys_expr = create_physical_expr(expr, &df_schema, &execution_props)?;
        compiled_assignments.insert(col.clone(), phys_expr);
    }

    // Filter the batch to only target rows for expression evaluation
    let filtered_columns: Vec<ArrayRef> = joined_batch
        .columns()
        .iter()
        .map(|col| take(col, &indices_arr, None))
        .collect::<Result<Vec<_>, _>>()?;
    let filtered_batch = RecordBatch::try_new(joined_batch.schema(), filtered_columns)?;

    // Evaluate compiled expressions
    let mut evaluated_columns: HashMap<String, ArrayRef> = HashMap::new();
    for (col, phys_expr) in &compiled_assignments {
        let result = phys_expr.evaluate(&filtered_batch)?;
        evaluated_columns.insert(col.clone(), result.into_array(filtered_batch.num_rows())?);
    }

    // Build output batch using target schema
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(target_schema.fields().len());

    for field in target_schema.fields() {
        let column_name = field.name();

        if let Some(evaluated_col) = evaluated_columns.get(column_name) {
            // Use evaluated SET expression
            columns.push(evaluated_col.clone());
        } else {
            // Use original target column value
            let target_col_name = format!("{}{}", TARGET_PREFIX, column_name);
            let idx = joined_batch
                .schema()
                .index_of(&target_col_name)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            columns.push(take(joined_batch.column(idx), &indices_arr, None)?);
        }
    }

    RecordBatch::try_new(target_schema.clone(), columns).map_err(|e| {
        DataFusionError::ArrowError(
            Box::new(e),
            Some("Failed to create NOT_MATCHED_BY_SOURCE UPDATE data batch".to_string()),
        )
    })
}

/// Extracts source data for INSERT * action (NOT_MATCHED rows).
///
/// Creates a data batch with source columns mapped to target schema.
fn extract_source_data_for_insert(
    joined_batch: &RecordBatch,
    not_matched_indices: &[usize],
    target_schema: &ArrowSchemaRef,
) -> DFResult<RecordBatch> {
    use datafusion::arrow::compute::take;

    // Build indices array for filtering
    let indices = datafusion::arrow::array::UInt32Array::from(
        not_matched_indices
            .iter()
            .map(|&i| i as u32)
            .collect::<Vec<_>>(),
    );

    // For each target column, find corresponding source column
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(target_schema.fields().len());

    for field in target_schema.fields() {
        let source_col_name = format!("{}{}", SOURCE_PREFIX, field.name());

        let column = if let Ok(idx) = joined_batch.schema().index_of(&source_col_name) {
            // Take values at not_matched indices
            take(joined_batch.column(idx), &indices, None)?
        } else {
            // If source doesn't have this column, create nulls
            // This handles cases where source schema differs from target
            return Err(DataFusionError::Plan(format!(
                "Source column '{}' not found for INSERT. \
                 Use INSERT with explicit values to specify columns.",
                field.name()
            )));
        };

        columns.push(column);
    }

    RecordBatch::try_new(target_schema.clone(), columns).map_err(|e| {
        DataFusionError::ArrowError(
            Box::new(e),
            Some("Failed to create INSERT data batch".to_string()),
        )
    })
}

/// Applies INSERT values to create new rows.
///
/// Evaluates each value expression against the joined batch and creates
/// a new batch with the specified values.
fn apply_insert_values(
    joined_batch: &RecordBatch,
    not_matched_indices: &[usize],
    values: &[(String, Expr)],
    target_schema: &ArrowSchemaRef,
) -> DFResult<RecordBatch> {
    use datafusion::arrow::compute::take;

    // Build indices array for filtering
    let indices = datafusion::arrow::array::UInt32Array::from(
        not_matched_indices
            .iter()
            .map(|&i| i as u32)
            .collect::<Vec<_>>(),
    );

    // Compile expressions for INSERT values
    let df_schema = DFSchema::try_from(joined_batch.schema().as_ref().clone())?;
    let ctx = SessionContext::new();
    let execution_props = ctx.state().execution_props().clone();

    let mut compiled_values: HashMap<String, Arc<dyn PhysicalExpr>> = HashMap::new();
    for (col, expr) in values {
        let phys_expr = create_physical_expr(expr, &df_schema, &execution_props)?;
        compiled_values.insert(col.clone(), phys_expr);
    }

    // Filter batch to only not_matched rows for expression evaluation
    let filtered_columns: Vec<ArrayRef> = joined_batch
        .columns()
        .iter()
        .map(|col| take(col, &indices, None))
        .collect::<Result<Vec<_>, _>>()?;
    let filtered_batch = RecordBatch::try_new(joined_batch.schema(), filtered_columns)?;

    // Evaluate compiled expressions
    let mut evaluated_columns: HashMap<String, ArrayRef> = HashMap::new();
    for (col, phys_expr) in &compiled_values {
        let result = phys_expr.evaluate(&filtered_batch)?;
        evaluated_columns.insert(col.clone(), result.into_array(filtered_batch.num_rows())?);
    }

    // Build output batch using target schema
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(target_schema.fields().len());

    for field in target_schema.fields() {
        let column_name = field.name();

        if let Some(evaluated_col) = evaluated_columns.get(column_name) {
            // Use evaluated INSERT value
            columns.push(evaluated_col.clone());
        } else {
            // Try source column as fallback
            let source_col_name = format!("{}{}", SOURCE_PREFIX, column_name);
            if let Ok(idx) = joined_batch.schema().index_of(&source_col_name) {
                columns.push(take(joined_batch.column(idx), &indices, None)?);
            } else {
                return Err(DataFusionError::Plan(format!(
                    "No value provided for column '{}' in INSERT",
                    column_name
                )));
            }
        }
    }

    RecordBatch::try_new(target_schema.clone(), columns).map_err(|e| {
        DataFusionError::ArrowError(
            Box::new(e),
            Some("Failed to create INSERT data batch with values".to_string()),
        )
    })
}

/// Extracts source data for UPDATE * action.
///
/// Creates a data batch with source columns mapped to target schema.
fn extract_source_data_for_update(
    joined_batch: &RecordBatch,
    matched_indices: &[usize],
    target_schema: &ArrowSchemaRef,
) -> DFResult<RecordBatch> {
    use datafusion::arrow::compute::take;

    // Build indices array for filtering
    let indices = datafusion::arrow::array::UInt32Array::from(
        matched_indices
            .iter()
            .map(|&i| i as u32)
            .collect::<Vec<_>>(),
    );

    // For each target column, find corresponding source column
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(target_schema.fields().len());

    for field in target_schema.fields() {
        let source_col_name = format!("{}{}", SOURCE_PREFIX, field.name());

        let column = if let Ok(idx) = joined_batch.schema().index_of(&source_col_name) {
            // Take values at matched indices
            take(joined_batch.column(idx), &indices, None)?
        } else {
            // Fall back to target column if source doesn't have this column
            let target_col_name = format!("{}{}", TARGET_PREFIX, field.name());
            let idx = joined_batch
                .schema()
                .index_of(&target_col_name)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            take(joined_batch.column(idx), &indices, None)?
        };

        columns.push(column);
    }

    RecordBatch::try_new(target_schema.clone(), columns).map_err(|e| {
        DataFusionError::ArrowError(
            Box::new(e),
            Some("Failed to create UPDATE data batch".to_string()),
        )
    })
}

/// Applies SET assignments to create updated rows.
///
/// Evaluates each assignment expression against the joined batch and creates
/// a new batch with the updated values.
fn apply_update_assignments(
    joined_batch: &RecordBatch,
    matched_indices: &[usize],
    assignments: &[(String, Expr)],
    target_schema: &ArrowSchemaRef,
) -> DFResult<RecordBatch> {
    use datafusion::arrow::compute::take;

    // Build indices array for filtering
    let indices = datafusion::arrow::array::UInt32Array::from(
        matched_indices
            .iter()
            .map(|&i| i as u32)
            .collect::<Vec<_>>(),
    );

    // Build assignment map for quick lookup (reserved for condition evaluation)
    let _assignment_map: HashMap<String, &Expr> = assignments
        .iter()
        .map(|(col, expr)| (col.clone(), expr))
        .collect();

    // Compile expressions for assignments
    let df_schema = DFSchema::try_from(joined_batch.schema().as_ref().clone())?;
    let ctx = SessionContext::new();
    let execution_props = ctx.state().execution_props().clone();

    let mut compiled_assignments: HashMap<String, Arc<dyn PhysicalExpr>> = HashMap::new();
    for (col, expr) in assignments {
        let phys_expr = create_physical_expr(expr, &df_schema, &execution_props)?;
        compiled_assignments.insert(col.clone(), phys_expr);
    }

    // Evaluate all expressions against matched rows first
    // Filter the batch to only matched rows for expression evaluation
    let filtered_columns: Vec<ArrayRef> = joined_batch
        .columns()
        .iter()
        .map(|col| take(col, &indices, None))
        .collect::<Result<Vec<_>, _>>()?;
    let filtered_batch = RecordBatch::try_new(joined_batch.schema(), filtered_columns)?;

    // Evaluate compiled expressions
    let mut evaluated_columns: HashMap<String, ArrayRef> = HashMap::new();
    for (col, phys_expr) in &compiled_assignments {
        let result = phys_expr.evaluate(&filtered_batch)?;
        evaluated_columns.insert(col.clone(), result.into_array(filtered_batch.num_rows())?);
    }

    // Build output batch using target schema
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(target_schema.fields().len());

    for field in target_schema.fields() {
        let column_name = field.name();

        if let Some(evaluated_col) = evaluated_columns.get(column_name) {
            // Use evaluated SET expression
            columns.push(evaluated_col.clone());
        } else {
            // Use original target column value
            let target_col_name = format!("{}{}", TARGET_PREFIX, column_name);
            if let Ok(idx) = joined_batch.schema().index_of(&target_col_name) {
                columns.push(take(joined_batch.column(idx), &indices, None)?);
            } else {
                // Try source column as fallback
                let source_col_name = format!("{}{}", SOURCE_PREFIX, column_name);
                let idx = joined_batch
                    .schema()
                    .index_of(&source_col_name)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                columns.push(take(joined_batch.column(idx), &indices, None)?);
            }
        }
    }

    RecordBatch::try_new(target_schema.clone(), columns).map_err(|e| {
        DataFusionError::ArrowError(
            Box::new(e),
            Some("Failed to create UPDATE data batch with assignments".to_string()),
        )
    })
}

/// Extracts join key column names from the match condition.
///
/// Supports both simple equality conditions like `col("target.id") = col("source.id")`
/// and compound conditions like `col("target.id") = col("source.id") AND col("target.region") = col("source.region")`.
///
/// Returns a vector of (target_key, source_key) column name pairs.
fn extract_join_keys(match_condition: &Expr) -> DFResult<Vec<(String, String)>> {
    let mut keys = Vec::new();
    extract_join_keys_recursive(match_condition, &mut keys)?;

    if keys.is_empty() {
        return Err(DataFusionError::Plan(
            "MERGE ON condition must contain at least one equality predicate".to_string(),
        ));
    }

    Ok(keys)
}

/// Recursively extracts join key pairs from an expression.
/// Handles AND expressions and equality predicates.
fn extract_join_keys_recursive(expr: &Expr, keys: &mut Vec<(String, String)>) -> DFResult<()> {
    match expr {
        // AND: recurse into both sides
        Expr::BinaryExpr(binary) if binary.op == datafusion::logical_expr::Operator::And => {
            extract_join_keys_recursive(&binary.left, keys)?;
            extract_join_keys_recursive(&binary.right, keys)?;
        }
        // Equality: extract key pair
        Expr::BinaryExpr(binary) if binary.op == datafusion::logical_expr::Operator::Eq => {
            let left_col = extract_column_name(&binary.left)?;
            let right_col = extract_column_name(&binary.right)?;

            // Determine which is target and which is source based on prefix
            let (target_key, source_key) = if left_col.starts_with("target.") {
                (
                    left_col
                        .strip_prefix("target.")
                        .expect("prefix check guarantees 'target.' exists")
                        .to_string(),
                    right_col
                        .strip_prefix("source.")
                        .unwrap_or(&right_col)
                        .to_string(),
                )
            } else if right_col.starts_with("target.") {
                (
                    right_col
                        .strip_prefix("target.")
                        .expect("prefix check guarantees 'target.' exists")
                        .to_string(),
                    left_col
                        .strip_prefix("source.")
                        .unwrap_or(&left_col)
                        .to_string(),
                )
            } else {
                // No prefix - assume left is target, right is source
                (left_col, right_col)
            };

            keys.push((target_key, source_key));
        }
        // Unsupported: OR conditions or complex expressions in join keys
        Expr::BinaryExpr(binary) if binary.op == datafusion::logical_expr::Operator::Or => {
            return Err(DataFusionError::Plan(
                "MERGE ON condition does not support OR predicates. Use AND to combine multiple equality conditions.".to_string(),
            ));
        }
        _ => {
            return Err(DataFusionError::Plan(format!(
                "MERGE ON condition must be equality predicates (=) optionally combined with AND. Got: {:?}",
                expr
            )));
        }
    }
    Ok(())
}

/// Extracts column name from an expression.
fn extract_column_name(expr: &Expr) -> DFResult<String> {
    match expr {
        Expr::Column(col) => Ok(col.name.clone()),
        Expr::Alias(alias) => extract_column_name(&alias.expr),
        _ => Err(DataFusionError::Plan(format!(
            "Expected column reference in ON condition. Got: {:?}",
            expr
        ))),
    }
}

/// Performs a FULL OUTER JOIN between target and source batches.
///
/// Uses DataFusion's DataFrame API for the join operation.
/// Supports multiple join keys for compound key joins.
async fn perform_full_outer_join(
    target_batches: &[RecordBatch],
    source_batches: &[RecordBatch],
    _target_schema: &ArrowSchemaRef,
    join_keys: &[(String, String)],
) -> DFResult<Vec<RecordBatch>> {
    if target_batches.is_empty() && source_batches.is_empty() {
        return Ok(vec![]);
    }

    // Create session context for the join
    let ctx = SessionContext::new();

    // Get schemas, handling empty batch cases
    let target_schema = if !target_batches.is_empty() {
        target_batches[0].schema()
    } else {
        // Create empty schema with metadata columns
        Arc::new(ArrowSchema::new(vec![
            Field::new(MERGE_FILE_PATH_COL, DataType::Utf8, false),
            Field::new(MERGE_ROW_POS_COL, DataType::Int64, false),
        ]))
    };

    let source_schema = if !source_batches.is_empty() {
        source_batches[0].schema()
    } else {
        Arc::new(ArrowSchema::new(Vec::<Field>::new()))
    };

    // Rename target columns with prefix to avoid conflicts
    let target_fields: Vec<Field> = target_schema
        .fields()
        .iter()
        .map(|f| {
            let new_name = format!("{}{}", TARGET_PREFIX, f.name());
            Field::new(&new_name, f.data_type().clone(), f.is_nullable())
        })
        .collect();
    let prefixed_target_schema = Arc::new(ArrowSchema::new(target_fields));

    // Rename source columns with prefix
    let source_fields: Vec<Field> = source_schema
        .fields()
        .iter()
        .map(|f| {
            let new_name = format!("{}{}", SOURCE_PREFIX, f.name());
            Field::new(&new_name, f.data_type().clone(), f.is_nullable())
        })
        .collect();
    let prefixed_source_schema = Arc::new(ArrowSchema::new(source_fields));

    // Rename columns in target batches
    let prefixed_target_batches: Vec<RecordBatch> = target_batches
        .iter()
        .map(|batch| RecordBatch::try_new(prefixed_target_schema.clone(), batch.columns().to_vec()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    // Rename columns in source batches
    let prefixed_source_batches: Vec<RecordBatch> = source_batches
        .iter()
        .map(|batch| RecordBatch::try_new(prefixed_source_schema.clone(), batch.columns().to_vec()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    // Create MemTables
    let target_table = Arc::new(MemTable::try_new(prefixed_target_schema.clone(), vec![
        prefixed_target_batches,
    ])?);

    let source_table = Arc::new(MemTable::try_new(prefixed_source_schema.clone(), vec![
        prefixed_source_batches,
    ])?);

    // Register tables
    ctx.register_table("target", target_table)?;
    ctx.register_table("source", source_table)?;

    // Create DataFrames
    let target_df = ctx.table("target").await?;
    let source_df = ctx.table("source").await?;

    // Prefixed key column names for compound keys
    let prefixed_target_keys: Vec<String> = join_keys
        .iter()
        .map(|(t, _)| format!("{}{}", TARGET_PREFIX, t))
        .collect();
    let prefixed_source_keys: Vec<String> = join_keys
        .iter()
        .map(|(_, s)| format!("{}{}", SOURCE_PREFIX, s))
        .collect();

    // Convert to slices for join API
    let target_key_refs: Vec<&str> = prefixed_target_keys.iter().map(|s| s.as_str()).collect();
    let source_key_refs: Vec<&str> = prefixed_source_keys.iter().map(|s| s.as_str()).collect();

    // Perform FULL OUTER JOIN with compound keys
    let joined_df = target_df.join(
        source_df,
        JoinType::Full,
        &target_key_refs,
        &source_key_refs,
        None,
    )?;

    // Collect results
    let joined_batches = joined_df.collect().await?;

    Ok(joined_batches)
}

/// Classifies rows after the JOIN and counts actions.
///
/// For each row:
/// 1. Determine classification based on NULL patterns in join keys
/// 2. Apply WHEN clauses to determine action
/// 3. Count actions for stats
///
/// With compound keys, a row is classified as MATCHED only if ALL target keys
/// and ALL source keys are non-null.
#[allow(dead_code)] // Reserved for future use in NOT MATCHED handling
fn classify_and_count_actions(
    joined_batches: &[RecordBatch],
    join_keys: &[(String, String)],
    when_matched: &[WhenMatchedClause],
    when_not_matched: &[WhenNotMatchedClause],
    when_not_matched_by_source: &[WhenNotMatchedBySourceClause],
) -> DFResult<MergeStats> {
    let mut stats = MergeStats::default();

    // Build prefixed key column names
    let prefixed_keys: Vec<(String, String)> = join_keys
        .iter()
        .map(|(t, s)| {
            (
                format!("{}{}", TARGET_PREFIX, t),
                format!("{}{}", SOURCE_PREFIX, s),
            )
        })
        .collect();

    for batch in joined_batches {
        // Find all key column indices
        let key_indices: Vec<(usize, usize)> = prefixed_keys
            .iter()
            .map(|(t, s)| {
                let target_idx = batch
                    .schema()
                    .index_of(t)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                let source_idx = batch
                    .schema()
                    .index_of(s)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                Ok((target_idx, source_idx))
            })
            .collect::<DFResult<Vec<_>>>()?;

        // Process each row
        for row_idx in 0..batch.num_rows() {
            // For compound keys: target is null if ANY target key is null
            // source is null if ANY source key is null
            let target_is_null = key_indices
                .iter()
                .any(|(t_idx, _)| batch.column(*t_idx).is_null(row_idx));
            let source_is_null = key_indices
                .iter()
                .any(|(_, s_idx)| batch.column(*s_idx).is_null(row_idx));

            // Classify the row
            let classification =
                match RowClassification::from_join_nulls(target_is_null, source_is_null) {
                    Some(c) => c,
                    None => {
                        // Both keys NULL - shouldn't happen in proper JOIN
                        continue;
                    }
                };

            // Determine action using WHEN clause evaluation
            let action = evaluate_when_clauses(
                classification,
                when_matched,
                when_not_matched,
                when_not_matched_by_source,
            );

            // Update stats based on action
            match action {
                MergeAction::Insert => stats.rows_inserted += 1,
                MergeAction::Update(_) | MergeAction::UpdateAll => stats.rows_updated += 1,
                MergeAction::Delete => stats.rows_deleted += 1,
                MergeAction::NoAction => {
                    // For NOT_MATCHED_BY_SOURCE with NoAction, the row is kept
                    // This counts as "copied" in CoW semantics if we were writing files
                    if classification == RowClassification::NotMatchedBySource {
                        stats.rows_copied += 1;
                    }
                }
            }
        }
    }

    Ok(stats)
}

// ============================================================================
// WHEN Clause Evaluation Helpers
// ============================================================================

/// Evaluates WHEN clauses for a row and returns the action to take.
///
/// This implements first-match-wins semantics: clauses are evaluated in order,
/// and the first matching clause determines the action.
///
/// # Arguments
/// * `classification` - The row classification (MATCHED, NOT_MATCHED, or NOT_MATCHED_BY_SOURCE)
/// * `when_matched` - WHEN MATCHED clauses to evaluate
/// * `when_not_matched` - WHEN NOT MATCHED clauses to evaluate
/// * `when_not_matched_by_source` - WHEN NOT MATCHED BY SOURCE clauses to evaluate
///
/// # Returns
/// The action to take for this row.
///
/// # Important Implementation Note
///
/// **CURRENT LIMITATION**: This function only handles clauses with `condition: None`.
/// Clauses with conditions are skipped. The execute() implementation MUST:
///
/// 1. Pre-compile all WHEN clause conditions into `Arc<dyn PhysicalExpr>`
/// 2. For each row, evaluate conditions against the joined batch
/// 3. Pass evaluated boolean results to determine which clause applies
///
/// Example pattern for execute():
/// ```ignore
/// // Pre-compile conditions once
/// let compiled_conditions: Vec<Option<Arc<dyn PhysicalExpr>>> = when_matched
///     .iter()
///     .map(|c| c.condition.as_ref().map(|e| compile_expr(e)))
///     .collect();
///
/// // For each row, evaluate conditions
/// for (idx, clause) in when_matched.iter().enumerate() {
///     let matches = match &compiled_conditions[idx] {
///         None => true, // No condition = always matches
///         Some(expr) => expr.evaluate(&batch)?.value(row_idx),
///     };
///     if matches {
///         return clause.action.clone();
///     }
/// }
/// ```
#[allow(dead_code)] // Will be used in MERGE execution implementation
pub fn evaluate_when_clauses(
    classification: RowClassification,
    when_matched: &[WhenMatchedClause],
    when_not_matched: &[WhenNotMatchedClause],
    when_not_matched_by_source: &[WhenNotMatchedBySourceClause],
) -> MergeAction {
    match classification {
        RowClassification::Matched => {
            // Try each WHEN MATCHED clause in order
            for clause in when_matched {
                // TODO: Evaluate condition expression if present
                // For now, assume all conditions match (will be implemented with physical expr evaluation)
                if clause.condition.is_none() {
                    // No additional condition - this clause applies
                    return match &clause.action {
                        MatchedAction::Update(assignments) => {
                            MergeAction::Update(assignments.clone())
                        }
                        MatchedAction::UpdateAll => MergeAction::UpdateAll,
                        MatchedAction::Delete => MergeAction::Delete,
                    };
                }
                // If condition exists, we need to evaluate it against the row
                // This will be implemented in execute_merge when we have the batch context
            }
            // No clause matched - keep row unchanged
            MergeAction::NoAction
        }
        RowClassification::NotMatched => {
            // Try each WHEN NOT MATCHED clause in order
            for clause in when_not_matched {
                if clause.condition.is_none() {
                    return match &clause.action {
                        NotMatchedAction::Insert(_) => MergeAction::Insert,
                        NotMatchedAction::InsertAll => MergeAction::Insert,
                    };
                }
            }
            MergeAction::NoAction
        }
        RowClassification::NotMatchedBySource => {
            // Try each WHEN NOT MATCHED BY SOURCE clause in order
            for clause in when_not_matched_by_source {
                if clause.condition.is_none() {
                    return match &clause.action {
                        NotMatchedBySourceAction::Update(assignments) => {
                            MergeAction::Update(assignments.clone())
                        }
                        NotMatchedBySourceAction::Delete => MergeAction::Delete,
                    };
                }
            }
            // Default for NOT_MATCHED_BY_SOURCE: keep the row unchanged
            MergeAction::NoAction
        }
    }
}

/// Adds metadata columns (file_path, row_pos) to a record batch.
///
/// This is used to track which file and position each target row came from,
/// enabling position delete generation after the JOIN.
///
/// # Important: Per-File Batch Assumption
///
/// This function assumes all rows in `batch` come from the same file at
/// sequential positions starting from `row_offset`. This matches the pattern
/// used in UPDATE execution where we iterate through `FileScanTask` objects
/// and process one file at a time:
///
/// ```ignore
/// // Correct usage pattern (from UPDATE implementation):
/// for task in file_scan_tasks {
///     let file_path = task.data_file_path().to_string();
///     let mut row_offset = 0i64;
///
///     for batch in read_file(&task) {
///         let batch_with_meta = add_metadata_columns(batch, &file_path, row_offset)?;
///         row_offset += batch.num_rows() as i64;
///         // Process batch_with_meta...
///     }
/// }
/// ```
///
/// **DO NOT** use this with batches containing rows from multiple files or
/// non-sequential positions - the metadata will be incorrect and position
/// deletes will target wrong rows.
#[allow(dead_code)] // Will be used in MERGE execution implementation
fn add_metadata_columns(
    batch: RecordBatch,
    file_path: &str,
    row_offset: i64,
) -> DFResult<RecordBatch> {
    use datafusion::error::DataFusionError;

    let num_rows = batch.num_rows();

    // Create file_path column (same value for all rows)
    let file_paths: Vec<&str> = vec![file_path; num_rows];
    let file_path_array = Arc::new(StringArray::from(file_paths)) as ArrayRef;

    // Create row_pos column (sequential from row_offset)
    let positions: Vec<i64> = (row_offset..(row_offset + num_rows as i64)).collect();
    let row_pos_array = Arc::new(Int64Array::from(positions)) as ArrayRef;

    // Build new schema with metadata columns
    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new(MERGE_FILE_PATH_COL, DataType::Utf8, false));
    fields.push(Field::new(MERGE_ROW_POS_COL, DataType::Int64, false));
    let new_schema = Arc::new(ArrowSchema::new(fields));

    // Build new columns array
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(file_path_array);
    columns.push(row_pos_array);

    RecordBatch::try_new(new_schema, columns).map_err(|e| {
        DataFusionError::ArrowError(
            Box::new(e),
            Some("Failed to add metadata columns to batch".to_string()),
        )
    })
}

/// Evaluates a condition expression for a set of row indices and returns which indices satisfy the condition.
///
/// # Arguments
/// * `condition` - Optional condition expression. If None, all rows pass.
/// * `batch` - The record batch containing the data
/// * `row_indices` - The row indices to evaluate
/// * `batch_schema` - The DFSchema for expression compilation
///
/// # Returns
/// The subset of `row_indices` that satisfy the condition.
fn evaluate_condition_for_rows(
    condition: &Option<Expr>,
    batch: &RecordBatch,
    row_indices: &[usize],
    batch_schema: &DFSchema,
) -> DFResult<Vec<usize>> {
    // If no condition, all rows pass
    let Some(condition_expr) = condition else {
        return Ok(row_indices.to_vec());
    };

    if row_indices.is_empty() {
        return Ok(Vec::new());
    }

    // Compile the expression
    let ctx = SessionContext::new();
    let execution_props = ctx.state().execution_props().clone();
    let physical_expr = create_physical_expr(condition_expr, batch_schema, &execution_props)?;

    // Create a filtered batch with only the candidate rows for efficient evaluation
    let indices_array = datafusion::arrow::array::UInt32Array::from(
        row_indices.iter().map(|&i| i as u32).collect::<Vec<_>>(),
    );

    let filtered_columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|col| datafusion::arrow::compute::take(col, &indices_array, None))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    let filtered_batch = RecordBatch::try_new(batch.schema(), filtered_columns).map_err(|e| {
        DataFusionError::ArrowError(
            Box::new(e),
            Some("Failed to create filtered batch for condition evaluation".to_string()),
        )
    })?;

    // Evaluate the condition
    let result = physical_expr.evaluate(&filtered_batch)?;

    // Extract boolean results
    let bool_array = match result {
        datafusion::physical_plan::ColumnarValue::Array(arr) => arr
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("Condition must return boolean array".to_string())
            })?
            .clone(),
        datafusion::physical_plan::ColumnarValue::Scalar(scalar) => {
            match scalar {
                datafusion::scalar::ScalarValue::Boolean(Some(b)) => {
                    // Scalar true/false applies to all rows
                    if b {
                        return Ok(row_indices.to_vec());
                    } else {
                        return Ok(Vec::new());
                    }
                }
                datafusion::scalar::ScalarValue::Boolean(None) => {
                    // NULL condition = false for all rows
                    return Ok(Vec::new());
                }
                _ => {
                    return Err(DataFusionError::Internal(
                        "Condition must return boolean scalar".to_string(),
                    ));
                }
            }
        }
    };

    // Collect indices where condition is true
    let passing_indices: Vec<usize> = row_indices
        .iter()
        .enumerate()
        .filter_map(|(eval_idx, &original_idx)| {
            if bool_array.is_valid(eval_idx) && bool_array.value(eval_idx) {
                Some(original_idx)
            } else {
                None
            }
        })
        .collect();

    Ok(passing_indices)
}

/// Creates a position delete batch for the given file paths and positions.
#[allow(dead_code)] // Will be used in MERGE execution implementation
fn make_position_delete_batch(
    file_paths: &[String],
    positions: &[i64],
    schema: ArrowSchemaRef,
) -> DFResult<RecordBatch> {
    use datafusion::error::DataFusionError;

    let file_path_refs: Vec<&str> = file_paths.iter().map(|s| s.as_str()).collect();
    let file_path_array = Arc::new(StringArray::from(file_path_refs)) as ArrayRef;
    let pos_array = Arc::new(Int64Array::from(positions.to_vec())) as ArrayRef;

    RecordBatch::try_new(schema, vec![file_path_array, pos_array]).map_err(|e| {
        DataFusionError::ArrowError(
            Box::new(e),
            Some("Failed to create position delete batch".to_string()),
        )
    })
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::lit;

    use super::*;

    #[test]
    fn test_output_schema() {
        let schema = IcebergMergeExec::make_output_schema();
        assert_eq!(schema.fields().len(), 7);
        assert_eq!(schema.field(0).name(), MERGE_DATA_FILES_COL);
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(1).name(), MERGE_DELETE_FILES_COL);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(2).name(), MERGE_INSERTED_COUNT_COL);
        assert_eq!(schema.field(2).data_type(), &DataType::UInt64);
        assert_eq!(schema.field(3).name(), MERGE_UPDATED_COUNT_COL);
        assert_eq!(schema.field(3).data_type(), &DataType::UInt64);
        assert_eq!(schema.field(4).name(), MERGE_DELETED_COUNT_COL);
        assert_eq!(schema.field(4).data_type(), &DataType::UInt64);
        assert_eq!(schema.field(5).name(), MERGE_DPP_APPLIED_COL);
        assert_eq!(schema.field(5).data_type(), &DataType::Boolean);
        assert_eq!(schema.field(6).name(), MERGE_DPP_PARTITION_COUNT_COL);
        assert_eq!(schema.field(6).data_type(), &DataType::UInt64);
    }

    // ============================================================================
    // RowClassification Tests
    // ============================================================================

    #[test]
    fn test_row_classification_from_join_nulls_matched() {
        // Both sides have data - MATCHED
        let result = RowClassification::from_join_nulls(false, false);
        assert_eq!(result, Some(RowClassification::Matched));
    }

    #[test]
    fn test_row_classification_from_join_nulls_not_matched() {
        // Target is NULL, source has data - NOT_MATCHED (insert candidate)
        let result = RowClassification::from_join_nulls(true, false);
        assert_eq!(result, Some(RowClassification::NotMatched));
    }

    #[test]
    fn test_row_classification_from_join_nulls_not_matched_by_source() {
        // Target has data, source is NULL - NOT_MATCHED_BY_SOURCE (delete candidate)
        let result = RowClassification::from_join_nulls(false, true);
        assert_eq!(result, Some(RowClassification::NotMatchedBySource));
    }

    #[test]
    fn test_row_classification_from_join_nulls_both_null() {
        // Both NULL - shouldn't happen in proper FULL OUTER JOIN
        let result = RowClassification::from_join_nulls(true, true);
        assert_eq!(result, None);
    }

    #[test]
    fn test_row_classification_as_str() {
        assert_eq!(RowClassification::Matched.as_str(), "MATCHED");
        assert_eq!(RowClassification::NotMatched.as_str(), "NOT_MATCHED");
        assert_eq!(
            RowClassification::NotMatchedBySource.as_str(),
            "NOT_MATCHED_BY_SOURCE"
        );
    }

    // ============================================================================
    // WHEN Clause Evaluation Tests
    // ============================================================================

    #[test]
    fn test_evaluate_when_clauses_matched_update() {
        let when_matched = vec![WhenMatchedClause {
            condition: None,
            action: MatchedAction::Update(vec![("col".to_string(), lit(1))]),
        }];

        let action = evaluate_when_clauses(RowClassification::Matched, &when_matched, &[], &[]);

        match action {
            MergeAction::Update(assignments) => {
                assert_eq!(assignments.len(), 1);
                assert_eq!(assignments[0].0, "col");
            }
            _ => panic!("Expected Update action"),
        }
    }

    #[test]
    fn test_evaluate_when_clauses_matched_update_all() {
        let when_matched = vec![WhenMatchedClause {
            condition: None,
            action: MatchedAction::UpdateAll,
        }];

        let action = evaluate_when_clauses(RowClassification::Matched, &when_matched, &[], &[]);

        assert!(matches!(action, MergeAction::UpdateAll));
    }

    #[test]
    fn test_evaluate_when_clauses_matched_delete() {
        let when_matched = vec![WhenMatchedClause {
            condition: None,
            action: MatchedAction::Delete,
        }];

        let action = evaluate_when_clauses(RowClassification::Matched, &when_matched, &[], &[]);

        assert!(matches!(action, MergeAction::Delete));
    }

    #[test]
    fn test_evaluate_when_clauses_not_matched_insert() {
        let when_not_matched = vec![WhenNotMatchedClause {
            condition: None,
            action: NotMatchedAction::InsertAll,
        }];

        let action =
            evaluate_when_clauses(RowClassification::NotMatched, &[], &when_not_matched, &[]);

        assert!(matches!(action, MergeAction::Insert));
    }

    #[test]
    fn test_evaluate_when_clauses_not_matched_by_source_delete() {
        let when_not_matched_by_source = vec![WhenNotMatchedBySourceClause {
            condition: None,
            action: NotMatchedBySourceAction::Delete,
        }];

        let action = evaluate_when_clauses(
            RowClassification::NotMatchedBySource,
            &[],
            &[],
            &when_not_matched_by_source,
        );

        assert!(matches!(action, MergeAction::Delete));
    }

    #[test]
    fn test_evaluate_when_clauses_no_matching_clause() {
        // No clauses defined - should return NoAction
        let action = evaluate_when_clauses(RowClassification::Matched, &[], &[], &[]);

        assert!(matches!(action, MergeAction::NoAction));
    }

    #[test]
    fn test_evaluate_when_clauses_first_match_wins() {
        // Two WHEN MATCHED clauses - first should win
        let when_matched = vec![
            WhenMatchedClause {
                condition: None,
                action: MatchedAction::Delete,
            },
            WhenMatchedClause {
                condition: None,
                action: MatchedAction::UpdateAll,
            },
        ];

        let action = evaluate_when_clauses(RowClassification::Matched, &when_matched, &[], &[]);

        // First clause (Delete) should win
        assert!(matches!(action, MergeAction::Delete));
    }

    // ============================================================================
    // Helper Function Tests
    // ============================================================================

    #[test]
    fn test_add_metadata_columns() {
        // Create a simple batch
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let id_array = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![id_array]).unwrap();

        // Add metadata columns
        let result = add_metadata_columns(batch, "s3://bucket/file.parquet", 100).unwrap();

        // Verify schema
        assert_eq!(result.schema().fields().len(), 3);
        assert_eq!(result.schema().field(0).name(), "id");
        assert_eq!(result.schema().field(1).name(), MERGE_FILE_PATH_COL);
        assert_eq!(result.schema().field(2).name(), MERGE_ROW_POS_COL);

        // Verify data
        assert_eq!(result.num_rows(), 3);

        let file_path_col = result
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(file_path_col.value(0), "s3://bucket/file.parquet");
        assert_eq!(file_path_col.value(1), "s3://bucket/file.parquet");
        assert_eq!(file_path_col.value(2), "s3://bucket/file.parquet");

        let row_pos_col = result
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(row_pos_col.value(0), 100);
        assert_eq!(row_pos_col.value(1), 101);
        assert_eq!(row_pos_col.value(2), 102);
    }

    #[test]
    fn test_make_merge_result_batch() {
        let stats = MergeStats {
            rows_inserted: 10,
            rows_updated: 5,
            rows_deleted: 3,
            rows_copied: 0,
            files_added: 2,
            files_removed: 1,
            dpp_applied: true,
            dpp_partition_count: 3,
        };

        let result = IcebergMergeExec::make_result_batch(
            vec!["data1.parquet".to_string(), "data2.parquet".to_string()],
            vec!["delete1.parquet".to_string()],
            &stats,
        )
        .unwrap();

        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.num_columns(), 7);

        let inserted = result
            .column(2)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .unwrap();
        assert_eq!(inserted.value(0), 10);

        let updated = result
            .column(3)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .unwrap();
        assert_eq!(updated.value(0), 5);

        let deleted = result
            .column(4)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .unwrap();
        assert_eq!(deleted.value(0), 3);

        let dpp_applied = result
            .column(5)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BooleanArray>()
            .unwrap();
        assert!(dpp_applied.value(0));

        let dpp_partition_count = result
            .column(6)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .unwrap();
        assert_eq!(dpp_partition_count.value(0), 3);
    }
}
