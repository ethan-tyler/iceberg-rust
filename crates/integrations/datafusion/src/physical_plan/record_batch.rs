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
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::{DataType, SchemaRef as ArrowSchemaRef};
use datafusion::error::{DataFusionError, Result as DFResult};
use iceberg::arrow::arrow_type_to_type;
use iceberg::spec::PrimitiveType;

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

        if !is_safe_type_promotion(column.data_type(), field.data_type()) {
            return Err(DataFusionError::Internal(format!(
                "Unsupported schema evolution cast for column '{}': {:?} -> {:?}",
                field.name(),
                column.data_type(),
                field.data_type()
            )));
        }

        changed = true;
        let options = CastOptions {
            safe: false,
            ..Default::default()
        };
        let casted =
            cast_with_options(column.as_ref(), field.data_type(), &options).map_err(|e| {
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

/// Returns the underlying logical type, unwrapping Arrow encoding optimizations.
fn unwrap_encoding(dt: &DataType) -> &DataType {
    match dt {
        // RunEndEncoded stores values in its second field
        DataType::RunEndEncoded(_, values_field) => unwrap_encoding(values_field.data_type()),
        // Dictionary encoding stores values in the value type
        DataType::Dictionary(_, value_type) => unwrap_encoding(value_type.as_ref()),
        other => other,
    }
}

fn is_safe_type_promotion(from: &DataType, to: &DataType) -> bool {
    // Unwrap Arrow encoding optimizations (RunEndEncoded, Dictionary)
    let from_logical = unwrap_encoding(from);
    let to_logical = unwrap_encoding(to);

    if from_logical == to_logical {
        return true;
    }

    // Try to convert to Iceberg types for schema evolution checks
    let Ok(from_type) = arrow_type_to_type(from_logical) else {
        return false;
    };
    let Ok(to_type) = arrow_type_to_type(to_logical) else {
        return false;
    };

    let (Some(from_primitive), Some(to_primitive)) =
        (from_type.as_primitive_type(), to_type.as_primitive_type())
    else {
        return false;
    };

    if from_primitive == to_primitive {
        return true;
    }

    // Iceberg-spec safe type promotions
    match (from_primitive, to_primitive) {
        (PrimitiveType::Int, PrimitiveType::Long) => true,
        (PrimitiveType::Float, PrimitiveType::Double) => true,
        (
            PrimitiveType::Decimal {
                precision: from_precision,
                scale: from_scale,
            },
            PrimitiveType::Decimal {
                precision: to_precision,
                scale: to_scale,
            },
        ) => to_precision >= from_precision && to_scale == from_scale,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Float64Array, Int32Array, Int64Array};
    use datafusion::arrow::datatypes::{Field, Schema};

    use super::*;

    #[test]
    fn test_unwrap_encoding_run_end_encoded() {
        let ree_type = DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", DataType::Utf8, true)),
        );
        assert_eq!(unwrap_encoding(&ree_type), &DataType::Utf8);
    }

    #[test]
    fn test_unwrap_encoding_dictionary() {
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        assert_eq!(unwrap_encoding(&dict_type), &DataType::Utf8);
    }

    #[test]
    fn test_is_safe_type_promotion_ree_to_primitive() {
        let ree_utf8 = DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", DataType::Utf8, true)),
        );
        assert!(is_safe_type_promotion(&ree_utf8, &DataType::Utf8));

        let ree_int = DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", DataType::Int32, true)),
        );
        assert!(is_safe_type_promotion(&ree_int, &DataType::Int32));
    }

    #[test]
    fn test_coerce_batch_schema_allows_int_widening() {
        let batch_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let expected_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let batch = RecordBatch::try_new(Arc::clone(&batch_schema), vec![Arc::new(
            Int32Array::from(vec![1, 2, 3]),
        )])
        .unwrap();

        let coerced = coerce_batch_schema(batch, &expected_schema).unwrap();
        let values = coerced
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(2), 3);
    }

    #[test]
    fn test_coerce_batch_schema_rejects_unsafe_cast() {
        let batch_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]));
        let expected_schema =
            Arc::new(Schema::new(vec![Field::new("v", DataType::Float32, false)]));

        let batch = RecordBatch::try_new(Arc::clone(&batch_schema), vec![Arc::new(
            Float64Array::from(vec![1.0, 2.5, 3.75]),
        )])
        .unwrap();

        let err = coerce_batch_schema(batch, &expected_schema).unwrap_err();
        assert!(
            err.to_string()
                .contains("Unsupported schema evolution cast"),
            "unexpected error: {err}"
        );
    }
}
