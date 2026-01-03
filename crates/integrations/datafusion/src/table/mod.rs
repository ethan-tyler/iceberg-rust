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

//! Iceberg table providers for DataFusion.
//!
//! This module provides two table provider implementations:
//!
//! - [`IcebergTableProvider`]: Catalog-backed provider with automatic metadata refresh.
//!   Use for write operations and when you need to see the latest table state.
//!
//! - [`IcebergStaticTableProvider`]: Static provider for read-only access to a specific
//!   table snapshot. Use for consistent analytical queries or time-travel scenarios.

pub mod metadata_table;
pub mod table_provider_factory;

use std::any::Any;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::Session;
use datafusion::common::DataFusionError;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::dml::{DmlCapabilities, InsertOp};
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use futures::StreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::expr::Predicate;
use iceberg::inspect::MetadataTableType;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, Error, ErrorKind, NamespaceIdent, Result, TableIdent};
use metadata_table::IcebergMetadataTableProvider;

use crate::error::to_datafusion_error;
use crate::merge::MergeBuilder;
use crate::physical_plan::commit::IcebergCommitExec;
use crate::physical_plan::delete_commit::IcebergDeleteCommitExec;
use crate::physical_plan::delete_scan::IcebergDeleteScanExec;
use crate::physical_plan::delete_write::IcebergDeleteWriteExec;
use crate::physical_plan::expr_to_predicate::convert_filters_to_predicate;
use crate::physical_plan::overwrite_commit::IcebergOverwriteCommitExec;
use crate::physical_plan::project::project_with_partition;
use crate::physical_plan::repartition::repartition;
use crate::physical_plan::scan::IcebergTableScan;
use crate::physical_plan::update::IcebergUpdateExec;
use crate::physical_plan::update_commit::IcebergUpdateCommitExec;
use crate::physical_plan::write::IcebergWriteExec;
use crate::update::UpdateBuilder;

const ARROW_FIELD_ID_META_KEY: &str = "PARQUET:field_id";

/// Catalog-backed table provider with automatic metadata refresh.
///
/// This provider loads fresh table metadata from the catalog on every scan and write
/// operation, ensuring you always see the latest table state. Use this when you need
/// write operations or want to see the most up-to-date data.
///
/// For read-only access to a specific snapshot without catalog overhead, use
/// [`IcebergStaticTableProvider`] instead.
#[derive(Debug, Clone)]
pub struct IcebergTableProvider {
    /// The catalog that manages this table
    catalog: Arc<dyn Catalog>,
    /// The table identifier (namespace + name)
    table_ident: TableIdent,
    /// A reference-counted arrow `Schema` (cached at construction)
    schema: ArrowSchemaRef,
    /// Iceberg field IDs used as partition sources across all partition specs.
    ///
    /// Used for filter pushdown classification and is safe to cache because field IDs are stable.
    partition_source_ids: HashSet<i32>,
}

impl IcebergTableProvider {
    /// Creates a new catalog-backed table provider.
    ///
    /// Loads the table once to get the initial schema, then stores the catalog
    /// reference for future metadata refreshes on each operation.
    pub async fn try_new(
        catalog: Arc<dyn Catalog>,
        namespace: NamespaceIdent,
        name: impl Into<String>,
    ) -> Result<Self> {
        let table_ident = TableIdent::new(namespace, name.into());

        // Load table once to get initial schema
        let table = catalog.load_table(&table_ident).await?;
        let schema = Arc::new(schema_to_arrow_schema(table.metadata().current_schema())?);
        let partition_source_ids = table
            .metadata()
            .partition_specs_iter()
            .flat_map(|spec| spec.partition_source_ids())
            .collect();

        Ok(IcebergTableProvider {
            catalog,
            table_ident,
            schema,
            partition_source_ids,
        })
    }

    pub(crate) async fn metadata_table(
        &self,
        r#type: MetadataTableType,
    ) -> Result<IcebergMetadataTableProvider> {
        // Load fresh table metadata for metadata table access
        let table = self.catalog.load_table(&self.table_ident).await?;
        Ok(IcebergMetadataTableProvider { table, r#type })
    }

    /// Deletes rows matching the given predicate from the table.
    ///
    /// This method creates position delete files for all rows matching the predicate
    /// and commits them to the table using the DeleteAction transaction.
    ///
    /// # Arguments
    /// * `state` - The DataFusion session state
    /// * `predicate` - Optional filter expression. If None, deletes all rows.
    ///
    /// # Returns
    /// The number of rows deleted.
    ///
    /// # Example
    /// ```ignore
    /// use datafusion::prelude::*;
    ///
    /// let deleted_count = provider
    ///     .delete(&session_state, Some(col("id").eq(lit(42))))
    ///     .await?;
    /// println!("Deleted {} rows", deleted_count);
    /// ```
    pub async fn delete(&self, _state: &dyn Session, predicate: Option<Expr>) -> DFResult<u64> {
        use datafusion::execution::context::TaskContext;
        use datafusion::physical_plan::collect;

        // Load fresh table metadata from catalog
        let table = self
            .catalog
            .load_table(&self.table_ident)
            .await
            .map_err(to_datafusion_error)?;

        let current_schema = Arc::new(
            schema_to_arrow_schema(table.metadata().current_schema())
                .map_err(to_datafusion_error)?,
        );

        if let Some(filter) = predicate.as_ref()
            && self.is_partition_only_exact_filter(&current_schema, filter)
            && let Some(iceberg_predicate) =
                convert_filters_to_predicate(std::slice::from_ref(filter))
        {
            return self
                .delete_by_partition_filter(&table, iceberg_predicate)
                .await;
        }

        // Capture baseline snapshot ID for concurrency validation
        let baseline_snapshot_id = table.metadata().current_snapshot_id();

        // Build the delete execution plan chain
        let filters: Vec<Expr> = predicate.into_iter().collect();

        // Step 1: Scan for rows to delete
        let delete_scan = Arc::new(IcebergDeleteScanExec::new(
            table.clone(),
            None, // Use current snapshot
            &filters,
            current_schema,
        ));

        // Step 2: Write position delete files
        let delete_write = Arc::new(IcebergDeleteWriteExec::new(table.clone(), delete_scan));

        // Step 3: Coalesce partitions for single commit
        let coalesce = Arc::new(CoalescePartitionsExec::new(delete_write));

        // Step 4: Commit delete files with baseline validation
        let delete_commit = Arc::new(IcebergDeleteCommitExec::new(
            table,
            self.catalog.clone(),
            coalesce.clone(),
            coalesce.schema(),
            baseline_snapshot_id,
        ));

        // Execute the plan
        let task_ctx = Arc::new(TaskContext::default());
        let batches = collect(delete_commit, task_ctx).await?;

        // Extract the count from the result
        if batches.is_empty() || batches[0].num_rows() == 0 {
            return Ok(0);
        }

        let count_array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal("Expected UInt64Array for delete count".to_string())
            })?;

        Ok(count_array.value(0))
    }

    fn is_partition_only_exact_filter(&self, schema: &ArrowSchemaRef, filter: &Expr) -> bool {
        let referenced_field_ids = referenced_field_ids(schema, filter);
        if referenced_field_ids.is_empty() {
            return false;
        }
        referenced_field_ids
            .iter()
            .all(|id| self.partition_source_ids.contains(id))
            && is_iceberg_filter_exact(filter)
    }

    async fn delete_by_partition_filter(
        &self,
        table: &Table,
        predicate: Predicate,
    ) -> DFResult<u64> {
        let scan = table
            .scan()
            .select_empty()
            .with_filter(predicate.clone())
            .build()
            .map_err(to_datafusion_error)?;
        let mut tasks = scan.plan_files().await.map_err(to_datafusion_error)?;
        let mut deleted_rows = 0u64;
        while let Some(task) = tasks.next().await {
            let task = task.map_err(to_datafusion_error)?;
            deleted_rows += task.record_count.unwrap_or(0);
        }

        if deleted_rows == 0 {
            return Ok(0);
        }

        let tx = Transaction::new(table);
        let action = tx.overwrite().overwrite_filter(predicate);
        let tx = action.apply(tx).map_err(to_datafusion_error)?;
        tx.commit(self.catalog.as_ref())
            .await
            .map_err(to_datafusion_error)?;
        Ok(deleted_rows)
    }

    /// Creates an UpdateBuilder for updating rows in the table.
    ///
    /// This method loads fresh table metadata and returns a builder that can be
    /// configured with SET assignments and an optional WHERE predicate.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use datafusion::prelude::*;
    ///
    /// let updated_count = provider
    ///     .update()
    ///     .await?
    ///     .set("status", lit("shipped"))
    ///     .set("updated_at", current_timestamp())
    ///     .filter(col("id").eq(lit(42)))
    ///     .execute(&session_state)
    ///     .await?;
    /// println!("Updated {} rows", updated_count);
    /// ```
    pub async fn update(&self) -> Result<UpdateBuilder> {
        // Load fresh table metadata from catalog
        let table = self.catalog.load_table(&self.table_ident).await?;
        Ok(UpdateBuilder::new(
            table,
            self.catalog.clone(),
            self.schema.clone(),
        ))
    }

    /// Creates a MergeBuilder for MERGE (UPSERT) operations on the table.
    ///
    /// MERGE combines INSERT, UPDATE, and DELETE operations into a single atomic
    /// transaction. This is essential for CDC (Change Data Capture) patterns where
    /// you need to synchronize a target table with a source dataset.
    ///
    /// # Arguments
    ///
    /// * `source` - The source DataFrame to merge into this table
    ///
    /// # Example
    ///
    /// ```ignore
    /// use datafusion::prelude::*;
    ///
    /// // CDC-style merge: update existing, insert new, delete missing
    /// let stats = provider
    ///     .merge(source_df)
    ///     .await?
    ///     .on(col("target.id").eq(col("source.id")))
    ///     .when_matched(None)
    ///         .update_all()
    ///     .when_not_matched(None)
    ///         .insert_all()
    ///     .when_not_matched_by_source(None)
    ///         .delete()
    ///     .execute(&session_state)
    ///     .await?;
    ///
    /// println!("Inserted: {}, Updated: {}, Deleted: {}",
    ///     stats.rows_inserted, stats.rows_updated, stats.rows_deleted);
    /// ```
    pub async fn merge(&self, source: DataFrame) -> Result<MergeBuilder> {
        // Load fresh table metadata from catalog
        let table = self.catalog.load_table(&self.table_ident).await?;
        Ok(MergeBuilder::new(
            table,
            self.catalog.clone(),
            self.schema.clone(),
            source,
        ))
    }
}

#[async_trait]
impl TableProvider for IcebergTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Load fresh table metadata from catalog
        let table = self
            .catalog
            .load_table(&self.table_ident)
            .await
            .map_err(to_datafusion_error)?;

        // Create scan with fresh metadata (always use current snapshot)
        Ok(Arc::new(IcebergTableScan::new(
            table,
            None, // Always use current snapshot for catalog-backed provider
            self.schema.clone(),
            projection,
            filters,
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(classify_pushdown_filters(
            &self.schema,
            &self.partition_source_ids,
            filters,
        ))
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Load fresh table metadata from catalog
        let table = self
            .catalog
            .load_table(&self.table_ident)
            .await
            .map_err(to_datafusion_error)?;

        let partition_spec = table.metadata().default_partition_spec();

        // Step 1: Project partition values for partitioned tables
        let plan_with_partition = if !partition_spec.is_unpartitioned() {
            project_with_partition(input, &table)?
        } else {
            input
        };

        // Step 2: Repartition for parallel processing
        let target_partitions =
            NonZeroUsize::new(state.config().target_partitions()).ok_or_else(|| {
                DataFusionError::Configuration(
                    "target_partitions must be greater than 0".to_string(),
                )
            })?;

        let repartitioned_plan =
            repartition(plan_with_partition, table.metadata_ref(), target_partitions)?;

        let write_plan = Arc::new(IcebergWriteExec::new(
            table.clone(),
            repartitioned_plan,
            self.schema.clone(),
        ));

        // Merge the outputs of write_plan into one so we can commit all files together
        let coalesce_partitions = Arc::new(CoalescePartitionsExec::new(write_plan));

        // Choose commit executor based on insert operation type
        let commit_exec: Arc<dyn ExecutionPlan> = match insert_op {
            InsertOp::Append => {
                // Standard INSERT: append new files without modifying existing data
                Arc::new(IcebergCommitExec::new(
                    table,
                    self.catalog.clone(),
                    coalesce_partitions,
                    self.schema.clone(),
                ))
            }
            InsertOp::Overwrite => {
                // INSERT OVERWRITE: replace partitions touched by the new data
                // Uses ReplacePartitionsAction for dynamic partition replacement
                Arc::new(IcebergOverwriteCommitExec::new(
                    table,
                    self.catalog.clone(),
                    coalesce_partitions,
                    self.schema.clone(),
                ))
            }
            InsertOp::Replace => {
                // REPLACE INTO: Not yet supported
                // Could use OverwriteAction with explicit filter in the future
                return Err(DataFusionError::NotImplemented(
                    "REPLACE INTO is not yet supported for Iceberg tables. \
                    Use INSERT OVERWRITE for partition-level replacement."
                        .to_string(),
                ));
            }
        };

        Ok(commit_exec)
    }

    /// Returns the DML capabilities supported by this table.
    ///
    /// IcebergTableProvider supports both DELETE and UPDATE operations
    /// using Copy-on-Write semantics with position delete files.
    fn dml_capabilities(&self) -> DmlCapabilities {
        DmlCapabilities::ALL
    }

    /// Handles SQL DELETE statements by building an execution plan.
    ///
    /// Creates position delete files for all rows matching the filter predicates
    /// and commits them atomically using the DeleteAction transaction.
    ///
    /// # Arguments
    /// * `_state` - The DataFusion session state
    /// * `filters` - Filter predicates from the DELETE WHERE clause
    ///
    /// # Returns
    /// An execution plan that produces a single row with column `count` (UInt64)
    /// indicating the number of rows deleted.
    async fn delete_from(
        &self,
        _state: &dyn Session,
        filters: Vec<Expr>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Load fresh table metadata from catalog
        let table = self
            .catalog
            .load_table(&self.table_ident)
            .await
            .map_err(to_datafusion_error)?;

        // Capture baseline snapshot ID for concurrency validation.
        // If another transaction commits between our scan and commit,
        // we'll detect it and fail rather than apply stale position deletes.
        let baseline_snapshot_id = table.metadata().current_snapshot_id();

        let current_schema = Arc::new(
            schema_to_arrow_schema(table.metadata().current_schema())
                .map_err(to_datafusion_error)?,
        );

        // Step 1: Scan for rows to delete
        let delete_scan = Arc::new(IcebergDeleteScanExec::new(
            table.clone(),
            None, // Use current snapshot
            &filters,
            current_schema,
        ));

        // Step 2: Write position delete files
        let delete_write = Arc::new(IcebergDeleteWriteExec::new(table.clone(), delete_scan));

        // Step 3: Coalesce partitions for single commit
        let coalesce = Arc::new(CoalescePartitionsExec::new(delete_write));

        // Step 4: Commit delete files with baseline validation
        Ok(Arc::new(IcebergDeleteCommitExec::new(
            table,
            self.catalog.clone(),
            coalesce.clone(),
            coalesce.schema(),
            baseline_snapshot_id,
        )))
    }

    /// Handles SQL UPDATE statements by building an execution plan.
    ///
    /// Uses Copy-on-Write semantics: reads matching rows, applies SET expressions,
    /// writes new data files, creates position delete files for originals,
    /// and commits everything atomically.
    ///
    /// # Arguments
    /// * `_state` - The DataFusion session state
    /// * `assignments` - Column assignments from SET clause (column_name, expression)
    /// * `filters` - Filter predicates from the UPDATE WHERE clause
    ///
    /// # Returns
    /// An execution plan that produces a single row with column `count` (UInt64)
    /// indicating the number of rows updated.
    async fn update(
        &self,
        _state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Load fresh table metadata from catalog
        let table = self
            .catalog
            .load_table(&self.table_ident)
            .await
            .map_err(to_datafusion_error)?;

        // Use fresh schema from loaded table to handle schema evolution
        let current_schema = Arc::new(
            schema_to_arrow_schema(table.metadata().current_schema())
                .map_err(to_datafusion_error)?,
        );

        // Validate that all assigned columns exist in the current schema
        for (column_name, _) in &assignments {
            if current_schema.field_with_name(column_name).is_err() {
                return Err(DataFusionError::Plan(format!(
                    "Column '{column_name}' not found in table schema"
                )));
            }
        }

        // Build the UPDATE execution plan with fresh schema
        let update_exec = Arc::new(IcebergUpdateExec::new(
            table.clone(),
            current_schema,
            assignments,
            filters,
        ));

        // Coalesce partitions for single commit
        let coalesce = Arc::new(CoalescePartitionsExec::new(update_exec));

        // Commit both data files and delete files atomically
        Ok(Arc::new(IcebergUpdateCommitExec::new(
            table,
            self.catalog.clone(),
            coalesce.clone(),
            coalesce.schema(),
        )))
    }
}

/// Static table provider for read-only snapshot access.
///
/// This provider holds a cached table instance and does not refresh metadata or support
/// write operations. Use this for consistent analytical queries, time-travel scenarios,
/// or when you want to avoid catalog overhead.
///
/// For catalog-backed tables with write support and automatic refresh, use
/// [`IcebergTableProvider`] instead.
#[derive(Debug, Clone)]
pub struct IcebergStaticTableProvider {
    /// The static table instance (never refreshed)
    table: Table,
    /// Optional snapshot ID for this static view
    snapshot_id: Option<i64>,
    /// A reference-counted arrow `Schema`
    schema: ArrowSchemaRef,
    /// Iceberg field IDs used as partition sources across all partition specs.
    partition_source_ids: HashSet<i32>,
}

impl IcebergStaticTableProvider {
    /// Creates a static provider from a table instance.
    ///
    /// Uses the table's current snapshot for all queries. Does not support write operations.
    pub async fn try_new_from_table(table: Table) -> Result<Self> {
        let schema = Arc::new(schema_to_arrow_schema(table.metadata().current_schema())?);
        let partition_source_ids = table
            .metadata()
            .partition_specs_iter()
            .flat_map(|spec| spec.partition_source_ids())
            .collect();
        Ok(IcebergStaticTableProvider {
            table,
            snapshot_id: None,
            schema,
            partition_source_ids,
        })
    }

    /// Creates a static provider for a specific table snapshot.
    ///
    /// Queries the specified snapshot for all operations. Useful for time-travel queries.
    /// Does not support write operations.
    pub async fn try_new_from_table_snapshot(table: Table, snapshot_id: i64) -> Result<Self> {
        let snapshot = table
            .metadata()
            .snapshot_by_id(snapshot_id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "snapshot id {snapshot_id} not found in table {}",
                        table.identifier().name()
                    ),
                )
            })?;
        let table_schema = snapshot.schema(table.metadata())?;
        let schema = Arc::new(schema_to_arrow_schema(&table_schema)?);
        let partition_source_ids = table
            .metadata()
            .partition_specs_iter()
            .flat_map(|spec| spec.partition_source_ids())
            .collect();
        Ok(IcebergStaticTableProvider {
            table,
            snapshot_id: Some(snapshot_id),
            schema,
            partition_source_ids,
        })
    }
}

#[async_trait]
impl TableProvider for IcebergStaticTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Use cached table (no refresh)
        Ok(Arc::new(IcebergTableScan::new(
            self.table.clone(),
            self.snapshot_id,
            self.schema.clone(),
            projection,
            filters,
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(classify_pushdown_filters(
            &self.schema,
            &self.partition_source_ids,
            filters,
        ))
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        _input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Err(to_datafusion_error(Error::new(
            ErrorKind::FeatureUnsupported,
            "Write operations are not supported on IcebergStaticTableProvider. \
             Use IcebergTableProvider with a catalog for write support."
                .to_string(),
        )))
    }
}

fn classify_pushdown_filters(
    schema: &ArrowSchemaRef,
    partition_source_ids: &HashSet<i32>,
    filters: &[&Expr],
) -> Vec<TableProviderFilterPushDown> {
    filters
        .iter()
        .map(|expr| classify_pushdown_filter(schema, partition_source_ids, expr))
        .collect()
}

fn classify_pushdown_filter(
    schema: &ArrowSchemaRef,
    partition_source_ids: &HashSet<i32>,
    filter: &Expr,
) -> TableProviderFilterPushDown {
    let referenced_field_ids = referenced_field_ids(schema, filter);
    if referenced_field_ids.is_empty() {
        return TableProviderFilterPushDown::Unsupported;
    }

    let only_partition_columns = referenced_field_ids
        .iter()
        .all(|id| partition_source_ids.contains(id));

    if only_partition_columns && is_iceberg_filter_exact(filter) {
        TableProviderFilterPushDown::Exact
    } else {
        // Conservatively classify as `Inexact` to keep behavior stable: the scan can still
        // use the filter for pruning and/or internal row filtering, and DataFusion will
        // retain the original filter for correctness.
        TableProviderFilterPushDown::Inexact
    }
}

fn referenced_field_ids(schema: &ArrowSchemaRef, expr: &Expr) -> Vec<i32> {
    let mut names = HashSet::<String>::new();
    collect_column_names(expr, &mut names);

    names
        .into_iter()
        .filter_map(|name| field_id_for_column_name(schema, &name))
        .collect()
}

fn collect_column_names(expr: &Expr, names: &mut HashSet<String>) {
    match expr {
        Expr::Column(col) => {
            names.insert(col.name().to_string());
        }
        Expr::BinaryExpr(binary) => {
            collect_column_names(&binary.left, names);
            collect_column_names(&binary.right, names);
        }
        Expr::Not(inner) => collect_column_names(inner, names),
        Expr::InList(inlist) => {
            collect_column_names(&inlist.expr, names);
            for item in &inlist.list {
                collect_column_names(item, names);
            }
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => collect_column_names(inner, names),
        Expr::Cast(cast) => collect_column_names(&cast.expr, names),
        _ => {}
    }
}

fn field_id_for_column_name(schema: &ArrowSchemaRef, name: &str) -> Option<i32> {
    let field = schema.field_with_name(name).ok()?;
    let id = field
        .metadata()
        .get(ARROW_FIELD_ID_META_KEY)?
        .parse::<i32>()
        .ok()?;
    Some(id)
}

fn is_iceberg_filter_exact(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryExpr(binary) => match binary.op {
            Operator::And | Operator::Or => {
                is_iceberg_filter_exact(&binary.left) && is_iceberg_filter_exact(&binary.right)
            }
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => {
                (is_column_expr(&binary.left) && is_literal_expr(&binary.right))
                    || (is_column_expr(&binary.right) && is_literal_expr(&binary.left))
            }
            _ => false,
        },
        Expr::InList(inlist) => {
            is_column_expr(&inlist.expr) && inlist.list.iter().all(is_literal_expr)
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => is_column_expr(inner),
        Expr::Not(inner) => is_iceberg_filter_exact(inner),
        Expr::Cast(cast) => {
            // Be conservative: some casts (e.g. to DATE) are not semantics-preserving for
            // predicate evaluation when translated to Iceberg expressions.
            if cast.data_type == datafusion::arrow::datatypes::DataType::Date32
                || cast.data_type == datafusion::arrow::datatypes::DataType::Date64
            {
                return false;
            }
            is_iceberg_filter_exact(&cast.expr)
        }
        _ => false,
    }
}

fn is_column_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Column(_) => true,
        Expr::Cast(cast) => is_column_expr(&cast.expr),
        _ => false,
    }
}

fn is_literal_expr(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(_, _))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::common::Column;
    use datafusion::prelude::{SessionContext, col, lit};
    use iceberg::io::FileIO;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use iceberg::table::{StaticTable, Table};
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
    use tempfile::TempDir;

    use super::*;

    async fn get_test_table_from_metadata_file() -> Table {
        let metadata_file_name = "TableMetadataV2Valid.json";
        let metadata_file_path = format!(
            "{}/tests/test_data/{}",
            env!("CARGO_MANIFEST_DIR"),
            metadata_file_name
        );
        let file_io = FileIO::from_path(&metadata_file_path)
            .unwrap()
            .build()
            .unwrap();
        let static_identifier = TableIdent::from_strs(["static_ns", "static_table"]).unwrap();
        let static_table =
            StaticTable::from_metadata_file(&metadata_file_path, static_identifier, file_io)
                .await
                .unwrap();
        static_table.into_table()
    }

    async fn get_test_catalog_and_table() -> (Arc<dyn Catalog>, NamespaceIdent, String, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let warehouse_path = temp_dir.path().to_str().unwrap().to_string();

        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse_path.clone())]),
            )
            .await
            .unwrap();

        let namespace = NamespaceIdent::new("test_ns".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();

        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        let table_creation = TableCreation::builder()
            .name("test_table".to_string())
            .location(format!("{warehouse_path}/test_table"))
            .schema(schema)
            .properties(HashMap::new())
            .build();

        catalog
            .create_table(&namespace, table_creation)
            .await
            .unwrap();

        (
            Arc::new(catalog),
            namespace,
            "test_table".to_string(),
            temp_dir,
        )
    }

    // Tests for IcebergStaticTableProvider

    #[tokio::test]
    async fn test_static_provider_from_table() {
        let table = get_test_table_from_metadata_file().await;
        let table_provider = IcebergStaticTableProvider::try_new_from_table(table.clone())
            .await
            .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("mytable", Arc::new(table_provider))
            .unwrap();
        let df = ctx.sql("SELECT * FROM mytable").await.unwrap();
        let df_schema = df.schema();
        let df_columns = df_schema.fields();
        assert_eq!(df_columns.len(), 3);
        let x_column = df_columns.first().unwrap();
        let column_data = format!(
            "{:?}:{:?}",
            x_column.name(),
            x_column.data_type().to_string()
        );
        assert_eq!(column_data, "\"x\":\"Int64\"");
        let has_column = df_schema.has_column(&Column::from_name("z"));
        assert!(has_column);
    }

    #[tokio::test]
    async fn test_static_provider_from_snapshot() {
        let table = get_test_table_from_metadata_file().await;
        let snapshot_id = table.metadata().snapshots().next().unwrap().snapshot_id();
        let table_provider =
            IcebergStaticTableProvider::try_new_from_table_snapshot(table.clone(), snapshot_id)
                .await
                .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("mytable", Arc::new(table_provider))
            .unwrap();
        let df = ctx.sql("SELECT * FROM mytable").await.unwrap();
        let df_schema = df.schema();
        let df_columns = df_schema.fields();
        assert_eq!(df_columns.len(), 3);
        let x_column = df_columns.first().unwrap();
        let column_data = format!(
            "{:?}:{:?}",
            x_column.name(),
            x_column.data_type().to_string()
        );
        assert_eq!(column_data, "\"x\":\"Int64\"");
        let has_column = df_schema.has_column(&Column::from_name("z"));
        assert!(has_column);
    }

    #[tokio::test]
    async fn test_static_provider_rejects_writes() {
        let table = get_test_table_from_metadata_file().await;
        let table_provider = IcebergStaticTableProvider::try_new_from_table(table.clone())
            .await
            .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("mytable", Arc::new(table_provider))
            .unwrap();

        // Attempt to insert into the static provider should fail
        let result = ctx.sql("INSERT INTO mytable VALUES (1, 2, 3)").await;

        // The error should occur during planning or execution
        // We expect an error indicating write operations are not supported
        assert!(
            result.is_err() || {
                let df = result.unwrap();
                df.collect().await.is_err()
            }
        );
    }

    #[tokio::test]
    async fn test_static_provider_scan() {
        let table = get_test_table_from_metadata_file().await;
        let table_provider = IcebergStaticTableProvider::try_new_from_table(table.clone())
            .await
            .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("mytable", Arc::new(table_provider))
            .unwrap();

        // Test that scan operations work correctly
        let df = ctx.sql("SELECT count(*) FROM mytable").await.unwrap();
        let physical_plan = df.create_physical_plan().await;
        assert!(physical_plan.is_ok());
    }

    #[tokio::test]
    async fn test_static_provider_supports_filters_pushdown_partition_columns() {
        let table = get_test_table_from_metadata_file().await;
        let provider = IcebergStaticTableProvider::try_new_from_table(table)
            .await
            .unwrap();

        let x_eq_5 = col("x").eq(lit(5_i64));
        let y_eq_5 = col("y").eq(lit(5_i64));

        let results = provider
            .supports_filters_pushdown(&[&x_eq_5, &y_eq_5])
            .unwrap();

        assert_eq!(results, vec![
            TableProviderFilterPushDown::Exact,
            TableProviderFilterPushDown::Inexact,
        ]);
    }

    // Tests for IcebergTableProvider

    #[tokio::test]
    async fn test_catalog_backed_provider_creation() {
        let (catalog, namespace, table_name, _temp_dir) = get_test_catalog_and_table().await;

        // Test creating a catalog-backed provider
        let provider =
            IcebergTableProvider::try_new(catalog.clone(), namespace.clone(), table_name.clone())
                .await
                .unwrap();

        // Verify the schema is loaded correctly
        let schema = provider.schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
    }

    #[tokio::test]
    async fn test_catalog_backed_provider_supports_filters_pushdown_non_partitioned_defaults_inexact()
     {
        let (catalog, namespace, table_name, _temp_dir) = get_test_catalog_and_table().await;

        let provider =
            IcebergTableProvider::try_new(catalog.clone(), namespace.clone(), table_name.clone())
                .await
                .unwrap();

        let id_eq_1 = col("id").eq(lit(1_i32));
        let name_eq_test = col("name").eq(lit("test"));

        let results = provider
            .supports_filters_pushdown(&[&id_eq_1, &name_eq_test])
            .unwrap();

        assert_eq!(results, vec![
            TableProviderFilterPushDown::Inexact,
            TableProviderFilterPushDown::Inexact,
        ]);
    }

    #[tokio::test]
    async fn test_catalog_backed_provider_scan() {
        let (catalog, namespace, table_name, _temp_dir) = get_test_catalog_and_table().await;

        let provider =
            IcebergTableProvider::try_new(catalog.clone(), namespace.clone(), table_name.clone())
                .await
                .unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("test_table", Arc::new(provider))
            .unwrap();

        // Test that scan operations work correctly
        let df = ctx.sql("SELECT * FROM test_table").await.unwrap();

        // Verify the schema in the query result
        let df_schema = df.schema();
        assert_eq!(df_schema.fields().len(), 2);
        assert_eq!(df_schema.field(0).name(), "id");
        assert_eq!(df_schema.field(1).name(), "name");

        let physical_plan = df.create_physical_plan().await;
        assert!(physical_plan.is_ok());
    }

    #[tokio::test]
    async fn test_catalog_backed_provider_insert() {
        let (catalog, namespace, table_name, _temp_dir) = get_test_catalog_and_table().await;

        let provider =
            IcebergTableProvider::try_new(catalog.clone(), namespace.clone(), table_name.clone())
                .await
                .unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("test_table", Arc::new(provider))
            .unwrap();

        // Test that insert operations work correctly
        let result = ctx.sql("INSERT INTO test_table VALUES (1, 'test')").await;

        // Insert should succeed (or at least not fail during planning)
        assert!(result.is_ok());

        // Try to execute the insert plan
        let df = result.unwrap();
        let execution_result = df.collect().await;

        // The execution should succeed
        assert!(execution_result.is_ok());
    }

    #[tokio::test]
    async fn test_physical_input_schema_consistent_with_logical_input_schema() {
        let (catalog, namespace, table_name, _temp_dir) = get_test_catalog_and_table().await;

        let provider =
            IcebergTableProvider::try_new(catalog.clone(), namespace.clone(), table_name.clone())
                .await
                .unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("test_table", Arc::new(provider))
            .unwrap();

        // Create a query plan
        let df = ctx.sql("SELECT id, name FROM test_table").await.unwrap();

        // Get logical schema before consuming df
        let logical_schema = df.schema().clone();

        // Get physical plan (this consumes df)
        let physical_plan = df.create_physical_plan().await.unwrap();
        let physical_schema = physical_plan.schema();

        // Verify that logical and physical schemas are consistent
        assert_eq!(
            logical_schema.fields().len(),
            physical_schema.fields().len()
        );

        for (logical_field, physical_field) in logical_schema
            .fields()
            .iter()
            .zip(physical_schema.fields().iter())
        {
            assert_eq!(logical_field.name(), physical_field.name());
            assert_eq!(logical_field.data_type(), physical_field.data_type());
        }
    }
}
