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

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::error::{DataFusionError, Result as DFResult};

pub(crate) fn coerce_batch_schema(
    batch: RecordBatch,
    expected_schema: &ArrowSchemaRef,
) -> DFResult<RecordBatch> {
    if batch.num_columns() != expected_schema.fields().len() {
        return Err(DataFusionError::Internal(format!(
            "Batch schema mismatch: expected {} columns but got {}",
            expected_schema.fields().len(),
            batch.num_columns()
        )));
    }

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    let mut changed = false;

    for (idx, field) in expected_schema.fields().iter().enumerate() {
        let column = batch.column(idx);
        if column.data_type() == field.data_type() {
            columns.push(Arc::clone(column));
            continue;
        }

        changed = true;
        let casted = cast(column.as_ref(), field.data_type()).map_err(|e| {
            DataFusionError::ArrowError(
                Box::new(e),
                Some(format!("Failed to cast batch column '{}'", field.name())),
            )
        })?;
        columns.push(casted);
    }

    if !changed {
        return Ok(batch);
    }

    RecordBatch::try_new(Arc::clone(expected_schema), columns).map_err(|e| {
        DataFusionError::ArrowError(
            Box::new(e),
            Some("Failed to build coerced batch".to_string()),
        )
    })
}

