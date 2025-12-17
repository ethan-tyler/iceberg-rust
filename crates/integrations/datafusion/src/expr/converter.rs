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

use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{
    BinaryExpr, CastExpr, Column, DynamicFilterPhysicalExpr, InListExpr, IsNotNullExpr, IsNullExpr,
    Literal, NotExpr,
};
use iceberg::Result;
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::{PrimitiveType, Schema, Type};
use tracing::debug;

use super::literal::scalar_value_to_datum;

const IN_LIST_LIMIT: usize = 1000;

pub(crate) fn convert_physical_expr_to_predicate(
    expr: &Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Predicate> {
    match convert_predicate(expr, schema) {
        Some(predicate) => {
            debug!(
                expr = %expr,
                predicate = %predicate,
                "Successfully converted dynamic filter to iceberg predicate"
            );
            Ok(predicate)
        }
        None => {
            debug!(
                expr = %expr,
                "Could not convert dynamic filter expression to iceberg predicate, \
                 returning AlwaysTrue (no partition pruning will occur)"
            );
            Ok(Predicate::AlwaysTrue)
        }
    }
}

fn convert_predicate(expr: &Arc<dyn PhysicalExpr>, schema: &Schema) -> Option<Predicate> {
    if let Some(dynamic) = expr.as_any().downcast_ref::<DynamicFilterPhysicalExpr>() {
        return dynamic
            .current()
            .ok()
            .and_then(|e| convert_predicate(&e, schema));
    }

    if let Some(binary) = expr.as_any().downcast_ref::<BinaryExpr>() {
        return convert_binary_expr(binary, schema);
    }

    if let Some(in_list) = expr.as_any().downcast_ref::<InListExpr>() {
        return convert_in_list_expr(in_list, schema);
    }

    if let Some(is_null) = expr.as_any().downcast_ref::<IsNullExpr>() {
        return convert_is_null_expr(is_null, schema);
    }

    if let Some(is_not_null) = expr.as_any().downcast_ref::<IsNotNullExpr>() {
        return convert_is_not_null_expr(is_not_null, schema);
    }

    if let Some(not) = expr.as_any().downcast_ref::<NotExpr>() {
        // NOT requires full conversion of inner expression to avoid inverted semantics.
        // If inner is AND with partial conversion, NOT(partial) would be wrong:
        // e.g., NOT(A AND B) with only A converted becomes NOT(A), but should be NOT(A) OR NOT(B).
        // Use strict conversion under NOT to ensure correctness.
        return convert_predicate_strict(not.arg(), schema).map(|p| !p);
    }

    if let Some(cast) = expr.as_any().downcast_ref::<CastExpr>() {
        // Casting to date can truncate and introduce incorrect pruning, so skip conversion.
        if matches!(cast.cast_type(), DataType::Date32 | DataType::Date64) {
            return None;
        }
        return convert_predicate(cast.expr(), schema);
    }

    None
}

fn convert_binary_expr(binary: &BinaryExpr, schema: &Schema) -> Option<Predicate> {
    convert_binary_expr_impl(binary, schema, false)
}

fn convert_binary_expr_strict(binary: &BinaryExpr, schema: &Schema) -> Option<Predicate> {
    convert_binary_expr_impl(binary, schema, true)
}

fn convert_binary_expr_impl(
    binary: &BinaryExpr,
    schema: &Schema,
    strict: bool,
) -> Option<Predicate> {
    match binary.op() {
        Operator::And => {
            let left = if strict {
                convert_predicate_strict(binary.left(), schema)
            } else {
                convert_predicate(binary.left(), schema)
            };
            let right = if strict {
                convert_predicate_strict(binary.right(), schema)
            } else {
                convert_predicate(binary.right(), schema)
            };
            match (left, right, strict) {
                (Some(l), Some(r), _) => Some(l.and(r)),
                // Partial AND is only allowed in non-strict mode
                (Some(l), None, false) => Some(l),
                (None, Some(r), false) => Some(r),
                _ => None,
            }
        }
        Operator::Or => {
            // OR always requires full conversion (both in strict and non-strict mode)
            let left = if strict {
                convert_predicate_strict(binary.left(), schema)?
            } else {
                convert_predicate(binary.left(), schema)?
            };
            let right = if strict {
                convert_predicate_strict(binary.right(), schema)?
            } else {
                convert_predicate(binary.right(), schema)?
            };
            Some(left.or(right))
        }
        op => convert_comparison(binary.left(), op, binary.right(), schema),
    }
}

/// Strict conversion that doesn't allow partial AND conversion.
/// Used under NOT to ensure correct semantics.
fn convert_predicate_strict(expr: &Arc<dyn PhysicalExpr>, schema: &Schema) -> Option<Predicate> {
    if let Some(dynamic) = expr.as_any().downcast_ref::<DynamicFilterPhysicalExpr>() {
        return dynamic
            .current()
            .ok()
            .and_then(|e| convert_predicate_strict(&e, schema));
    }

    if let Some(binary) = expr.as_any().downcast_ref::<BinaryExpr>() {
        return convert_binary_expr_strict(binary, schema);
    }

    if let Some(in_list) = expr.as_any().downcast_ref::<InListExpr>() {
        return convert_in_list_expr(in_list, schema);
    }

    if let Some(is_null) = expr.as_any().downcast_ref::<IsNullExpr>() {
        return convert_is_null_expr(is_null, schema);
    }

    if let Some(is_not_null) = expr.as_any().downcast_ref::<IsNotNullExpr>() {
        return convert_is_not_null_expr(is_not_null, schema);
    }

    if let Some(not) = expr.as_any().downcast_ref::<NotExpr>() {
        // Nested NOT also uses strict mode
        return convert_predicate_strict(not.arg(), schema).map(|p| !p);
    }

    if let Some(cast) = expr.as_any().downcast_ref::<CastExpr>() {
        if matches!(cast.cast_type(), DataType::Date32 | DataType::Date64) {
            return None;
        }
        return convert_predicate_strict(cast.expr(), schema);
    }

    None
}

fn convert_comparison(
    left: &Arc<dyn PhysicalExpr>,
    op: &Operator,
    right: &Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Option<Predicate> {
    let (reference, reference_type) = if let Some(col) = left.as_any().downcast_ref::<Column>() {
        iceberg_reference_for_column(schema, col.name())?
    } else if let Some(col) = right.as_any().downcast_ref::<Column>() {
        iceberg_reference_for_column(schema, col.name())?
    } else {
        return None;
    };

    let (literal_expr, op) = if left.as_any().is::<Column>() {
        (right, *op)
    } else {
        (left, reverse_df_operator(op)?)
    };

    let literal_value = literal_expr
        .as_any()
        .downcast_ref::<Literal>()
        .map(|l| l.value())?;
    let datum = scalar_value_to_datum(literal_value, &reference_type)?;

    Some(match op {
        Operator::Eq => reference.equal_to(datum),
        Operator::NotEq => reference.not_equal_to(datum),
        Operator::Lt => reference.less_than(datum),
        Operator::LtEq => reference.less_than_or_equal_to(datum),
        Operator::Gt => reference.greater_than(datum),
        Operator::GtEq => reference.greater_than_or_equal_to(datum),
        _ => return None,
    })
}

fn convert_in_list_expr(in_list: &InListExpr, schema: &Schema) -> Option<Predicate> {
    if in_list.list().len() > IN_LIST_LIMIT {
        return None;
    }

    let col = in_list.expr().as_any().downcast_ref::<Column>()?;
    let (reference, reference_type) = iceberg_reference_for_column(schema, col.name())?;

    let mut datums = Vec::with_capacity(in_list.list().len());
    for item in in_list.list() {
        let literal = item.as_any().downcast_ref::<Literal>()?;
        let datum = scalar_value_to_datum(literal.value(), &reference_type)?;
        datums.push(datum);
    }

    Some(if in_list.negated() {
        reference.is_not_in(datums)
    } else {
        reference.is_in(datums)
    })
}

fn convert_is_null_expr(is_null: &IsNullExpr, schema: &Schema) -> Option<Predicate> {
    let col = is_null.arg().as_any().downcast_ref::<Column>()?;
    let (reference, _ty) = iceberg_reference_for_column(schema, col.name())?;
    Some(reference.is_null())
}

fn convert_is_not_null_expr(is_not_null: &IsNotNullExpr, schema: &Schema) -> Option<Predicate> {
    let col = is_not_null.arg().as_any().downcast_ref::<Column>()?;
    let (reference, _ty) = iceberg_reference_for_column(schema, col.name())?;
    Some(reference.is_not_null())
}

fn iceberg_reference_for_column(
    schema: &Schema,
    column_name: &str,
) -> Option<(Reference, PrimitiveType)> {
    let field = schema
        .field_by_name(column_name)
        .or_else(|| schema.field_by_name_case_insensitive(column_name))?;

    let Type::Primitive(primitive) = field.field_type.as_ref() else {
        return None;
    };

    let canonical_name = schema
        .name_by_field_id(field.id)
        .unwrap_or(&field.name)
        .to_string();

    Some((Reference::new(canonical_name), primitive.clone()))
}

fn reverse_df_operator(op: &Operator) -> Option<Operator> {
    Some(match op {
        Operator::Eq => Operator::Eq,
        Operator::NotEq => Operator::NotEq,
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use iceberg::spec::{Datum, NestedField, PrimitiveType, Schema, Type};

    use super::*;

    fn iceberg_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "region", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(3, "ts", Type::Primitive(PrimitiveType::Timestamp)).into(),
            ])
            .build()
            .unwrap()
    }

    #[test]
    fn test_convert_equality_expr() {
        let schema = iceberg_schema();

        let expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("region", 0)),
            Operator::Eq,
            Arc::new(Literal::new(datafusion::scalar::ScalarValue::Utf8(Some(
                "US".to_string(),
            )))),
        ));

        let predicate = convert_physical_expr_to_predicate(&expr, &schema).unwrap();
        assert_eq!(
            predicate,
            Reference::new("region").equal_to(Datum::string("US"))
        );
    }

    #[test]
    fn test_convert_numeric_int64_literal_to_int32_column() {
        let schema = iceberg_schema();

        let expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("id", 1)),
            Operator::Eq,
            Arc::new(Literal::new(datafusion::scalar::ScalarValue::Int64(Some(
                7,
            )))),
        ));

        let predicate = convert_physical_expr_to_predicate(&expr, &schema).unwrap();
        assert_eq!(predicate, Reference::new("id").equal_to(Datum::int(7)));
    }

    #[test]
    fn test_convert_in_list_expr() {
        use datafusion::arrow::array::{ArrayRef, StringArray};

        let schema = iceberg_schema();

        let array: ArrayRef = Arc::new(StringArray::from(vec!["US", "CA"]));
        let expr: Arc<dyn PhysicalExpr> = Arc::new(
            InListExpr::try_new_from_array(Arc::new(Column::new("region", 0)), array, false)
                .unwrap(),
        );

        let predicate = convert_physical_expr_to_predicate(&expr, &schema).unwrap();
        assert_eq!(
            predicate,
            Reference::new("region").is_in([Datum::string("US"), Datum::string("CA")])
        );
    }

    #[test]
    fn test_convert_is_null_expr() {
        let schema = iceberg_schema();

        let expr: Arc<dyn PhysicalExpr> =
            Arc::new(IsNullExpr::new(Arc::new(Column::new("region", 0))));

        let predicate = convert_physical_expr_to_predicate(&expr, &schema).unwrap();
        assert_eq!(predicate, Reference::new("region").is_null());
    }

    #[test]
    fn test_convert_and_expr_keeps_partial() {
        let schema = iceberg_schema();

        let supported: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("id", 1)),
            Operator::Eq,
            Arc::new(Literal::new(datafusion::scalar::ScalarValue::Int32(Some(
                1,
            )))),
        ));

        // Unsupported part: `id + 1` can't be converted (arithmetic).
        let unsupported: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            supported.clone(),
            Operator::Plus,
            Arc::new(Literal::new(datafusion::scalar::ScalarValue::Int32(Some(
                1,
            )))),
        ));

        let expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            supported.clone(),
            Operator::And,
            unsupported,
        ));

        let predicate = convert_physical_expr_to_predicate(&expr, &schema).unwrap();
        assert_eq!(predicate, Reference::new("id").equal_to(Datum::int(1)));
    }

    #[test]
    fn test_not_partial_and_returns_always_true() {
        // Verifies that NOT(A AND B) where B doesn't convert returns AlwaysTrue (safe fallback)
        // rather than incorrectly returning NOT(A).
        let schema = iceberg_schema();

        let supported: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("id", 1)),
            Operator::Eq,
            Arc::new(Literal::new(datafusion::scalar::ScalarValue::Int32(Some(
                1,
            )))),
        ));

        // Unsupported part: arithmetic expression can't be converted.
        let unsupported: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            supported.clone(),
            Operator::Plus,
            Arc::new(Literal::new(datafusion::scalar::ScalarValue::Int32(Some(
                1,
            )))),
        ));

        let and_expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            supported.clone(),
            Operator::And,
            unsupported,
        ));

        // NOT(partial AND) must NOT convert to NOT(partial) - that would be wrong!
        // Instead it should return AlwaysTrue to avoid incorrect pruning.
        let not_expr: Arc<dyn PhysicalExpr> = Arc::new(NotExpr::new(and_expr));
        let predicate = convert_physical_expr_to_predicate(&not_expr, &schema).unwrap();
        assert_eq!(
            predicate,
            Predicate::AlwaysTrue,
            "NOT around partial AND should return AlwaysTrue for safety"
        );
    }

    #[test]
    fn test_not_full_and_converts_correctly() {
        // Verifies that NOT(A AND B) where both convert works correctly.
        let schema = iceberg_schema();

        let left: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("id", 1)),
            Operator::Eq,
            Arc::new(Literal::new(datafusion::scalar::ScalarValue::Int32(Some(
                1,
            )))),
        ));

        let right: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("region", 0)),
            Operator::Eq,
            Arc::new(Literal::new(datafusion::scalar::ScalarValue::Utf8(Some(
                "US".to_string(),
            )))),
        ));

        let and_expr: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(left, Operator::And, right));

        let not_expr: Arc<dyn PhysicalExpr> = Arc::new(NotExpr::new(and_expr));
        let predicate = convert_physical_expr_to_predicate(&not_expr, &schema).unwrap();

        // NOT(id=1 AND region='US') should convert to the negated AND predicate
        let expected = !(Reference::new("id")
            .equal_to(Datum::int(1))
            .and(Reference::new("region").equal_to(Datum::string("US"))));
        assert_eq!(predicate, expected);
    }
}
