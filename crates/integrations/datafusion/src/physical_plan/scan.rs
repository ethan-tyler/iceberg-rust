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

use std::any::Any;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::vec;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::common::config::ConfigOptions;
use datafusion::common::stats::Precision;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
    PushedDown,
};
use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, Partitioning, PlanProperties, Statistics,
};
use datafusion::prelude::Expr;
use futures::{SinkExt, Stream, StreamExt, TryStreamExt};
use iceberg::Result;
use iceberg::expr::Predicate;
use iceberg::scan::{FileScanTask, FileScanTaskStream};
use iceberg::table::Table;

use super::expr_to_predicate::convert_filters_to_predicate;
use super::record_batch::coerce_batch_schema;
use crate::dynamic_filter::handler::DynamicFilterHandler;
use crate::to_datafusion_error;

/// Manages the scanning process of an Iceberg [`Table`], encapsulating the
/// necessary details and computed properties required for execution planning.
#[derive(Debug, Clone)]
struct CachedTasks {
    generation: u64,
    tasks: Vec<FileScanTask>,
}

pub struct IcebergTableScan {
    /// A table in the catalog.
    table: Table,
    /// Snapshot of the table to scan.
    snapshot_id: Option<i64>,
    /// Stores certain, often expensive to compute,
    /// plan properties used in query optimization.
    plan_properties: PlanProperties,
    /// Projection column names, None means all columns
    projection: Option<Vec<String>>,
    /// Filters to apply to the table scan
    predicates: Option<Predicate>,

    iceberg_schema: iceberg::spec::Schema,
    dynamic_filter_handler: Arc<DynamicFilterHandler>,
    cached_tasks: Arc<RwLock<Option<CachedTasks>>>,
    metrics: ExecutionPlanMetricsSet,
}

impl std::fmt::Debug for IcebergTableScan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergTableScan")
            .field("snapshot_id", &self.snapshot_id)
            .field("projection", &self.projection)
            .field("predicates", &self.predicates)
            .finish()
    }
}

impl IcebergTableScan {
    /// Creates a new [`IcebergTableScan`] object.
    pub(crate) fn new(
        table: Table,
        snapshot_id: Option<i64>,
        schema: ArrowSchemaRef,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
    ) -> Self {
        let output_schema = match projection {
            None => schema.clone(),
            Some(projection) => Arc::new(schema.project(projection).unwrap()),
        };
        let plan_properties = Self::compute_properties(output_schema.clone());
        let projection = get_column_names(schema.clone(), projection);
        let predicates = convert_filters_to_predicate(filters);
        let filterable_columns = table.get_filterable_columns();
        let iceberg_schema = table.metadata().current_schema().as_ref().clone();

        Self {
            table,
            snapshot_id,
            plan_properties,
            projection,
            predicates,
            iceberg_schema,
            dynamic_filter_handler: Arc::new(DynamicFilterHandler::new(filterable_columns)),
            cached_tasks: Arc::new(RwLock::new(None)),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    pub fn table(&self) -> &Table {
        &self.table
    }

    pub fn snapshot_id(&self) -> Option<i64> {
        self.snapshot_id
    }

    pub fn projection(&self) -> Option<&[String]> {
        self.projection.as_deref()
    }

    pub fn predicates(&self) -> Option<&Predicate> {
        self.predicates.as_ref()
    }

    /// Computes [`PlanProperties`] used in query optimization.
    fn compute_properties(schema: ArrowSchemaRef) -> PlanProperties {
        // TODO:
        // This is more or less a placeholder, to be replaced
        // once we support output-partitioning
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

impl ExecutionPlan for IcebergTableScan {
    fn name(&self) -> &str {
        "IcebergTableScan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<(dyn ExecutionPlan + 'static)>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn reset_state(self: Arc<Self>) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            table: self.table.clone(),
            snapshot_id: self.snapshot_id,
            plan_properties: self.plan_properties.clone(),
            projection: self.projection.clone(),
            predicates: self.predicates.clone(),
            iceberg_schema: self.iceberg_schema.clone(),
            dynamic_filter_handler: Arc::new(DynamicFilterHandler::new(
                self.dynamic_filter_handler.filterable_columns().to_vec(),
            )),
            cached_tasks: Arc::new(RwLock::new(None)),
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DFResult<Statistics> {
        if partition.is_some() {
            return Ok(Statistics::new_unknown(self.schema().as_ref()));
        }

        let mut stats = Statistics::new_unknown(self.schema().as_ref());
        let snapshot = match self.snapshot_id {
            Some(snapshot_id) => self.table.metadata().snapshot_by_id(snapshot_id),
            None => self.table.metadata().current_snapshot(),
        };

        let Some(snapshot) = snapshot else {
            return Ok(stats);
        };

        let summary = snapshot.summary();
        if let Some(total_records) = summary.additional_properties.get("total-records")
            && let Ok(total_records) = total_records.parse::<u64>()
            && let Ok(total_records) = usize::try_from(total_records)
        {
            stats.num_rows = Precision::Exact(total_records);
        }

        if let Some(total_size) = summary.additional_properties.get("total-files-size")
            && let Ok(total_size) = total_size.parse::<u64>()
            && let Ok(total_size) = usize::try_from(total_size)
        {
            stats.total_byte_size = Precision::Exact(total_size);
        }

        Ok(stats)
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        _parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> DFResult<FilterDescription> {
        Ok(FilterDescription::new())
    }

    fn handle_child_pushdown_result(
        &self,
        phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> DFResult<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        if !matches!(phase, FilterPushdownPhase::Post) {
            return Ok(FilterPushdownPropagation::with_parent_pushdown_result(
                vec![PushedDown::No; child_pushdown_result.parent_filters.len()],
            ));
        }

        let filterable_columns = self.dynamic_filter_handler.filterable_columns().to_vec();

        let mut updated_handler: Option<Arc<DynamicFilterHandler>> = None;
        let mut pushdown_results = Vec::with_capacity(child_pushdown_result.parent_filters.len());

        for parent_filter in child_pushdown_result.parent_filters {
            let expr = parent_filter.filter;
            match Arc::downcast::<DynamicFilterPhysicalExpr>(expr) {
                Ok(dynamic_filter) if updated_handler.is_none() => {
                    let candidate = DynamicFilterHandler::with_dynamic_filter(
                        filterable_columns.clone(),
                        dynamic_filter,
                        &self.iceberg_schema,
                    );
                    if candidate.dynamic_filter().is_some() {
                        updated_handler = Some(Arc::new(candidate));
                        pushdown_results.push(PushedDown::Yes);
                    } else {
                        pushdown_results.push(PushedDown::No);
                    }
                }
                Ok(_dynamic_filter) => {
                    // Support at most one dynamic filter for now.
                    pushdown_results.push(PushedDown::No);
                }
                Err(_expr) => pushdown_results.push(PushedDown::No),
            }
        }

        let mut propagation =
            FilterPushdownPropagation::with_parent_pushdown_result(pushdown_results);

        if let Some(dynamic_filter_handler) = updated_handler {
            let new_node: Arc<dyn ExecutionPlan> = Arc::new(Self {
                table: self.table.clone(),
                snapshot_id: self.snapshot_id,
                plan_properties: self.plan_properties.clone(),
                projection: self.projection.clone(),
                predicates: self.predicates.clone(),
                iceberg_schema: self.iceberg_schema.clone(),
                dynamic_filter_handler,
                cached_tasks: Arc::new(RwLock::new(None)),
                metrics: ExecutionPlanMetricsSet::new(),
            });
            propagation = propagation.with_updated_node(new_node);
        }

        Ok(propagation)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let planned_tasks_metric =
            MetricBuilder::new(&self.metrics).counter("planned_tasks", partition);
        let fut = get_batch_stream(BatchStreamArgs {
            table: self.table.clone(),
            snapshot_id: self.snapshot_id,
            column_names: self.projection.clone(),
            predicates: self.predicates.clone(),
            iceberg_schema: self.iceberg_schema.clone(),
            dynamic_filter_handler: Arc::clone(&self.dynamic_filter_handler),
            cached_tasks: Arc::clone(&self.cached_tasks),
            batch_size: context.session_config().batch_size(),
            planned_tasks_metric,
            expected_schema: self.schema(),
        });
        let stream = futures::stream::once(fut).try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

impl DisplayAs for IcebergTableScan {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        let projection = self
            .projection
            .clone()
            .map_or(String::new(), |v| v.join(","));
        let predicate = self
            .predicates
            .clone()
            .map_or(String::new(), |p| format!("{p}"));
        let dynamic_filter = if self.dynamic_filter_handler.dynamic_filter().is_some() {
            "enabled"
        } else {
            "none"
        };

        write!(f, "IcebergTableScan projection:[{projection}] predicate:[{predicate}] dynamic_filter:[{dynamic_filter}]")
    }
}

struct BatchStreamArgs {
    table: Table,
    snapshot_id: Option<i64>,
    column_names: Option<Vec<String>>,
    predicates: Option<Predicate>,
    iceberg_schema: iceberg::spec::Schema,
    dynamic_filter_handler: Arc<DynamicFilterHandler>,
    cached_tasks: Arc<RwLock<Option<CachedTasks>>>,
    batch_size: usize,
    planned_tasks_metric: Count,
    expected_schema: ArrowSchemaRef,
}

/// Asynchronously retrieves a stream of [`RecordBatch`] instances
/// from a given table.
///
/// This function initializes a [`TableScan`], builds it,
/// and then converts it into a stream of Arrow [`RecordBatch`]es.
async fn get_batch_stream(
    args: BatchStreamArgs,
) -> DFResult<Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>> {
    let BatchStreamArgs {
        table,
        snapshot_id,
        column_names,
        predicates,
        iceberg_schema,
        dynamic_filter_handler,
        cached_tasks,
        batch_size,
        planned_tasks_metric,
        expected_schema,
    } = args;

    let dynamic_generation = dynamic_filter_handler
        .dynamic_filter()
        .map(|f| f.snapshot_generation())
        .unwrap_or(0);

    let cached = cached_tasks
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .as_ref()
        .and_then(|c| (c.generation == dynamic_generation).then(|| c.tasks.clone()));

    let tasks: FileScanTaskStream = if let Some(tasks) = cached {
        planned_tasks_metric.add(tasks.len());
        Box::pin(futures::stream::iter(tasks.into_iter().map(Ok)))
    } else {
        let dynamic_predicate = dynamic_filter_handler
            .iceberg_predicate(&iceberg_schema)
            .map_err(to_datafusion_error)?
            .filter(|p| !matches!(p, Predicate::AlwaysTrue));

        let effective_predicate = combine_predicates(predicates, dynamic_predicate);

        let scan_builder = match snapshot_id {
            Some(snapshot_id) => table.scan().snapshot_id(snapshot_id),
            None => table.scan(),
        };

        let mut scan_builder = match column_names {
            Some(column_names) => scan_builder.select(column_names),
            None => scan_builder.select_all(),
        };

        if let Some(pred) = effective_predicate {
            scan_builder = scan_builder.with_filter(pred);
        }

        let table_scan = scan_builder.build().map_err(to_datafusion_error)?;
        let planned_tasks = table_scan.plan_files().await.map_err(to_datafusion_error)?;

        // Tee the planned task stream so we can both feed the reader and cache tasks for reuse.
        let (mut tx, rx) = futures::channel::mpsc::channel::<Result<FileScanTask>>(128);
        let cached_tasks = Arc::clone(&cached_tasks);
        let dynamic_filter_handler = Arc::clone(&dynamic_filter_handler);
        let planned_tasks_metric = planned_tasks_metric.clone();

        tokio::spawn(async move {
            let mut collected = Vec::new();
            let mut ok = true;
            let mut planned_tasks = planned_tasks;

            while let Some(task) = planned_tasks.next().await {
                if let Ok(ref t) = task {
                    planned_tasks_metric.add(1);
                    collected.push(t.clone());
                } else {
                    ok = false;
                }

                if tx.send(task).await.is_err() {
                    return;
                }
            }

            if !ok {
                return;
            }

            // Check if the dynamic filter's generation changed during task collection.
            // If it changed, the collected tasks were planned with a stale predicate,
            // so we discard them rather than caching stale results.
            //
            // This check at the END of collection is correct because:
            // 1. `dynamic_generation` was captured at the start of `get_batch_stream()`
            // 2. `iceberg_predicate()` always returns the current predicate state
            // 3. If generation changed between capture and now, tasks may be stale
            // 4. DataFusion's generation is monotonically increasing, so we can't
            //    accidentally match a future generation with a past capture
            let current_generation = dynamic_filter_handler
                .dynamic_filter()
                .map(|f| f.snapshot_generation())
                .unwrap_or(0);
            if current_generation != dynamic_generation {
                return;
            }

            let mut cached = cached_tasks.write().unwrap_or_else(|e| e.into_inner());
            *cached = Some(CachedTasks {
                generation: dynamic_generation,
                tasks: collected,
            });
        });

        Box::pin(rx)
    };

    let stream = table
        .reader_builder()
        .with_batch_size(batch_size)
        .build()
        .read(tasks)
        .map_err(to_datafusion_error)?
        .map_err(to_datafusion_error)
        .map(move |batch| batch.and_then(|batch| coerce_batch_schema(batch, &expected_schema)));

    Ok(Box::pin(stream))
}

fn get_column_names(
    schema: ArrowSchemaRef,
    projection: Option<&Vec<usize>>,
) -> Option<Vec<String>> {
    projection.map(|v| {
        v.iter()
            .map(|p| schema.field(*p).name().clone())
            .collect::<Vec<String>>()
    })
}

fn combine_predicates(left: Option<Predicate>, right: Option<Predicate>) -> Option<Predicate> {
    match (left, right) {
        (None, None) => None,
        (Some(p), None) | (None, Some(p)) => Some(p),
        (Some(l), Some(r)) => Some(l.and(r)),
    }
}
