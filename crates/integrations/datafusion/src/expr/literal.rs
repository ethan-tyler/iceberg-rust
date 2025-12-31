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

use datafusion::scalar::ScalarValue;
use iceberg::spec::{Datum, PrimitiveType};
use rust_decimal::Decimal;
use uuid::Uuid;

const MILLIS_PER_DAY: i64 = 24 * 60 * 60 * 1000;

pub(crate) fn scalar_value_to_datum(
    value: &ScalarValue,
    expected_type: &PrimitiveType,
) -> Option<Datum> {
    match value {
        ScalarValue::Null => None,
        ScalarValue::Dictionary(_, inner) => scalar_value_to_datum(inner, expected_type),

        ScalarValue::Boolean(Some(v)) if *expected_type == PrimitiveType::Boolean => {
            Some(Datum::bool(*v))
        }

        ScalarValue::Int8(Some(v)) => int_like_to_datum(i64::from(*v), expected_type),
        ScalarValue::Int16(Some(v)) => int_like_to_datum(i64::from(*v), expected_type),
        ScalarValue::Int32(Some(v)) => int_like_to_datum(i64::from(*v), expected_type),
        ScalarValue::Int64(Some(v)) => int_like_to_datum(*v, expected_type),

        ScalarValue::UInt8(Some(v)) => uint_like_to_datum(u64::from(*v), expected_type),
        ScalarValue::UInt16(Some(v)) => uint_like_to_datum(u64::from(*v), expected_type),
        ScalarValue::UInt32(Some(v)) => uint_like_to_datum(u64::from(*v), expected_type),
        ScalarValue::UInt64(Some(v)) => uint_like_to_datum(*v, expected_type),

        ScalarValue::Float16(Some(v)) => match expected_type {
            PrimitiveType::Float => Some(Datum::float(f32::from(*v))),
            PrimitiveType::Double => Some(Datum::double(f64::from(f32::from(*v)))),
            _ => None,
        },
        ScalarValue::Float32(Some(v)) => match expected_type {
            PrimitiveType::Float => Some(Datum::float(*v)),
            PrimitiveType::Double => Some(Datum::double(f64::from(*v))),
            _ => None,
        },
        ScalarValue::Float64(Some(v)) => match expected_type {
            PrimitiveType::Double => Some(Datum::double(*v)),
            // Avoid narrowing `f64 -> f32` to prevent precision-related pruning errors.
            PrimitiveType::Float => None,
            _ => None,
        },

        ScalarValue::Utf8(Some(v))
        | ScalarValue::Utf8View(Some(v))
        | ScalarValue::LargeUtf8(Some(v)) => match expected_type {
            PrimitiveType::String => Some(Datum::string(v.clone())),
            PrimitiveType::Uuid => Datum::uuid_from_str(v).ok(),
            _ => None,
        },

        ScalarValue::Binary(Some(v))
        | ScalarValue::BinaryView(Some(v))
        | ScalarValue::LargeBinary(Some(v)) => match expected_type {
            PrimitiveType::Binary => Some(Datum::binary(v.clone())),
            PrimitiveType::Fixed(len) if (*len as usize) == v.len() => {
                Some(Datum::fixed(v.clone()))
            }
            PrimitiveType::Uuid if v.len() == 16 => Some(Datum::uuid(Uuid::from_slice(v).ok()?)),
            _ => None,
        },
        ScalarValue::FixedSizeBinary(len, Some(v)) => match expected_type {
            PrimitiveType::Binary => Some(Datum::binary(v.clone())),
            PrimitiveType::Fixed(expected_len) if (*expected_len as i32) == *len => {
                Some(Datum::fixed(v.clone()))
            }
            PrimitiveType::Uuid if *len == 16 && v.len() == 16 => {
                Some(Datum::uuid(Uuid::from_slice(v).ok()?))
            }
            _ => None,
        },

        ScalarValue::Date32(Some(days)) if *expected_type == PrimitiveType::Date => {
            Some(Datum::date(*days))
        }
        ScalarValue::Date64(Some(ms)) if *expected_type == PrimitiveType::Date => {
            let days = ms.div_euclid(MILLIS_PER_DAY);
            Some(Datum::date(i32::try_from(days).ok()?))
        }

        ScalarValue::Time32Second(Some(seconds)) if *expected_type == PrimitiveType::Time => {
            Datum::time_micros(i64::from(*seconds) * 1_000_000).ok()
        }
        ScalarValue::Time32Millisecond(Some(ms)) if *expected_type == PrimitiveType::Time => {
            Datum::time_micros(i64::from(*ms) * 1_000).ok()
        }
        ScalarValue::Time64Microsecond(Some(us)) if *expected_type == PrimitiveType::Time => {
            Datum::time_micros(*us).ok()
        }
        ScalarValue::Time64Nanosecond(Some(_)) if *expected_type == PrimitiveType::Time => {
            // Avoid narrowing `ns -> us` for correctness (inequalities can become unsafe).
            None
        }

        ScalarValue::TimestampSecond(Some(v), tz) => {
            timestamp_to_datum(*v, 1_000_000, tz, expected_type)
        }
        ScalarValue::TimestampMillisecond(Some(v), tz) => {
            timestamp_to_datum(*v, 1_000, tz, expected_type)
        }
        ScalarValue::TimestampMicrosecond(Some(v), tz) => {
            timestamp_to_datum(*v, 1, tz, expected_type)
        }
        ScalarValue::TimestampNanosecond(Some(v), tz) => {
            timestamp_ns_to_datum(*v, tz, expected_type)
        }

        ScalarValue::Decimal32(Some(v), _p, s) => {
            decimal_i128_to_datum(i128::from(*v), *s, expected_type)
        }
        ScalarValue::Decimal64(Some(v), _p, s) => {
            decimal_i128_to_datum(i128::from(*v), *s, expected_type)
        }
        ScalarValue::Decimal128(Some(v), _p, s) => decimal_i128_to_datum(*v, *s, expected_type),
        ScalarValue::Decimal256(_, _, _) => None,

        _ => None,
    }
}

fn int_like_to_datum(value: i64, expected: &PrimitiveType) -> Option<Datum> {
    match expected {
        PrimitiveType::Int => Some(Datum::int(i32::try_from(value).ok()?)),
        PrimitiveType::Long => Some(Datum::long(value)),
        _ => None,
    }
}

fn uint_like_to_datum(value: u64, expected: &PrimitiveType) -> Option<Datum> {
    match expected {
        PrimitiveType::Int => Some(Datum::int(i32::try_from(value).ok()?)),
        PrimitiveType::Long => Some(Datum::long(i64::try_from(value).ok()?)),
        _ => None,
    }
}

fn decimal_i128_to_datum(unscaled: i128, scale: i8, expected: &PrimitiveType) -> Option<Datum> {
    let PrimitiveType::Decimal {
        precision,
        scale: expected_scale,
    } = expected
    else {
        return None;
    };

    let scale_u32 = u32::try_from(scale).ok()?;
    if scale_u32 != *expected_scale {
        return None;
    }

    // rust_decimal can't represent scales above 28.
    if scale_u32 > 28 {
        return None;
    }

    let decimal = Decimal::from_i128_with_scale(unscaled, scale_u32);
    Datum::decimal_with_precision(decimal, *precision).ok()
}

fn timestamp_to_datum(
    value: i64,
    multiplier_to_micros: i64,
    tz: &Option<Arc<str>>,
    expected: &PrimitiveType,
) -> Option<Datum> {
    let micros = value.checked_mul(multiplier_to_micros)?;

    match (expected, tz) {
        (PrimitiveType::Timestamp, None) => Some(Datum::timestamp_micros(micros)),
        (PrimitiveType::Timestamptz, Some(_)) => Some(Datum::timestamptz_micros(micros)),
        // If the Iceberg type expects a timezone, but DataFusion doesn't have one (or vice versa),
        // skip conversion to avoid semantic mismatches.
        _ => None,
    }
}

fn timestamp_ns_to_datum(
    value: i64,
    tz: &Option<Arc<str>>,
    expected: &PrimitiveType,
) -> Option<Datum> {
    match (expected, tz) {
        (PrimitiveType::TimestampNs, None) => Some(Datum::timestamp_nanos(value)),
        (PrimitiveType::TimestamptzNs, Some(_)) => Some(Datum::timestamptz_nanos(value)),
        // Avoid narrowing `ns -> us` by not converting nanos timestamps to micro types here.
        _ => None,
    }
}
