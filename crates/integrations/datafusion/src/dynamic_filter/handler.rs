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

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_expr::utils::collect_columns;
use iceberg::Result;
use iceberg::expr::Predicate;
use iceberg::scan::FilterableColumn;
use iceberg::spec::Schema;

use crate::expr::convert_physical_expr_to_predicate;

#[derive(Debug)]
pub(crate) struct DynamicFilterHandler {
    dynamic_filter: Option<Arc<DynamicFilterPhysicalExpr>>,
    last_generation: AtomicU64,
    filterable_columns: Vec<FilterableColumn>,
    cached_iceberg_predicate: RwLock<Option<Predicate>>,
}

impl DynamicFilterHandler {
    pub(crate) fn new(filterable_columns: Vec<FilterableColumn>) -> Self {
        Self {
            dynamic_filter: None,
            last_generation: AtomicU64::new(0),
            filterable_columns,
            cached_iceberg_predicate: RwLock::new(None),
        }
    }

    pub(crate) fn with_dynamic_filter(
        filterable_columns: Vec<FilterableColumn>,
        dynamic_filter: Arc<DynamicFilterPhysicalExpr>,
        iceberg_schema: &Schema,
    ) -> Self {
        let mut handler = Self::new(filterable_columns);
        if handler.validate_dynamic_filter(&dynamic_filter, iceberg_schema) {
            handler
                .last_generation
                .store(dynamic_filter.snapshot_generation(), Ordering::Relaxed);
            handler.dynamic_filter = Some(dynamic_filter);
        }
        handler
    }

    pub(crate) fn dynamic_filter(&self) -> Option<&Arc<DynamicFilterPhysicalExpr>> {
        self.dynamic_filter.as_ref()
    }

    pub(crate) fn filterable_columns(&self) -> &[FilterableColumn] {
        &self.filterable_columns
    }

    pub(crate) fn invalidate_if_updated(&self) -> bool {
        let Some(dynamic_filter) = &self.dynamic_filter else {
            return false;
        };

        let current = dynamic_filter.snapshot_generation();
        let previous = self.last_generation.load(Ordering::Acquire);

        if current == previous {
            return false;
        }

        {
            let mut cached = self
                .cached_iceberg_predicate
                .write()
                .unwrap_or_else(|e| e.into_inner());
            *cached = None;
        }

        // Ensure readers that observe the new generation also observe the cache invalidation.
        self.last_generation.store(current, Ordering::Release);
        true
    }

    pub(crate) fn iceberg_predicate(&self, iceberg_schema: &Schema) -> Result<Option<Predicate>> {
        let Some(dynamic_filter) = &self.dynamic_filter else {
            return Ok(None);
        };

        self.invalidate_if_updated();

        {
            let cached = self
                .cached_iceberg_predicate
                .read()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(predicate) = cached.as_ref() {
                return Ok(Some(predicate.clone()));
            }
        }

        let current = dynamic_filter
            .current()
            .map_err(crate::from_datafusion_error)?;
        let predicate = convert_physical_expr_to_predicate(&current, iceberg_schema)?;

        {
            let mut cached = self
                .cached_iceberg_predicate
                .write()
                .unwrap_or_else(|e| e.into_inner());
            // Re-check after taking the write lock in case another thread populated the cache.
            if cached.is_none() {
                *cached = Some(predicate.clone());
            }
        }

        Ok(Some(predicate))
    }

    fn validate_dynamic_filter(
        &self,
        dynamic_filter: &Arc<DynamicFilterPhysicalExpr>,
        iceberg_schema: &Schema,
    ) -> bool {
        let referenced = collect_columns(&(Arc::clone(dynamic_filter) as Arc<dyn PhysicalExpr>));
        if referenced.is_empty() {
            return false;
        }

        let filterable_field_ids: HashSet<i32> = self
            .filterable_columns
            .iter()
            .filter(|c| c.is_partition_column || c.has_statistics)
            .map(|c| c.field_id)
            .collect();

        referenced.into_iter().all(|col| {
            iceberg_schema
                .field_by_name(col.name())
                .or_else(|| iceberg_schema.field_by_name_case_insensitive(col.name()))
                .is_some_and(|field| filterable_field_ids.contains(&field.id))
        })
    }
}

#[cfg(test)]
mod tests {
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
    use datafusion::scalar::ScalarValue;
    use iceberg::expr::Reference;
    use iceberg::spec::{Datum, NestedField, PrimitiveType, Type};

    use super::*;

    fn iceberg_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "region", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap()
    }

    fn filterable_columns() -> Vec<FilterableColumn> {
        vec![FilterableColumn {
            name: "region".to_string(),
            field_id: 1,
            is_partition_column: true,
            has_statistics: true,
        }]
    }

    #[test]
    fn test_validation_rejects_non_filterable_column() {
        let schema = iceberg_schema();

        let child: Arc<dyn PhysicalExpr> = Arc::new(Column::new("id", 0));
        let inner: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("id", 0)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(7)))),
        ));

        let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(vec![child], inner));
        let handler =
            DynamicFilterHandler::with_dynamic_filter(filterable_columns(), dynamic, &schema);

        assert_eq!(handler.filterable_columns().len(), 1);
        assert!(handler.dynamic_filter().is_none());
    }

    #[test]
    fn test_iceberg_predicate_updates_with_generation() {
        let schema = iceberg_schema();

        let child: Arc<dyn PhysicalExpr> = Arc::new(Column::new("region", 0));
        let inner: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("region", 0)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Utf8(Some("US".to_string())))),
        ));

        let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(vec![child], inner));
        let handler = DynamicFilterHandler::with_dynamic_filter(
            filterable_columns(),
            Arc::clone(&dynamic),
            &schema,
        );

        let predicate = handler.iceberg_predicate(&schema).unwrap().unwrap();
        assert_eq!(
            predicate,
            Reference::new("region").equal_to(Datum::string("US"))
        );

        dynamic
            .update(Arc::new(BinaryExpr::new(
                Arc::new(Column::new("region", 0)),
                Operator::Eq,
                Arc::new(Literal::new(ScalarValue::Utf8(Some("CA".to_string())))),
            )))
            .unwrap();

        let predicate = handler.iceberg_predicate(&schema).unwrap().unwrap();
        assert_eq!(
            predicate,
            Reference::new("region").equal_to(Datum::string("CA"))
        );
    }
}
