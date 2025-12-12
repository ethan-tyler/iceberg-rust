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

//! Partition filter evaluation for rewrite_data_files.

use std::sync::Arc;

use crate::error::Result;
use crate::expr::visitors::expression_evaluator::ExpressionEvaluator;
use crate::expr::visitors::inclusive_projection::InclusiveProjection;
use crate::expr::{Bind, BoundPredicate, Predicate};
use crate::spec::{PartitionSpecRef, Schema, SchemaRef, Struct};

/// Evaluates partition values against a filter predicate.
///
/// Used by `RewriteDataFilesPlanner` to filter which partitions should be compacted.
///
/// **Important:** This evaluator is specific to a single partition spec. When a table
/// has multiple partition specs (due to partition evolution), entries with different
/// spec_ids should not be evaluated with this evaluator. Use `spec_id()` to check
/// compatibility and `can_evaluate()` to safely handle entries.
pub(crate) struct PartitionFilterEvaluator {
    evaluator: ExpressionEvaluator,
    /// The partition spec ID this evaluator was created for.
    spec_id: i32,
}

impl PartitionFilterEvaluator {
    /// Create a new partition filter evaluator.
    ///
    /// # Arguments
    ///
    /// * `predicate` - Unbound predicate to filter partitions
    /// * `schema` - Table schema (for binding the predicate)
    /// * `partition_spec` - Partition spec to project predicate
    /// * `case_sensitive` - Whether field matching is case-sensitive
    pub fn try_new(
        predicate: &Predicate,
        schema: &SchemaRef,
        partition_spec: &PartitionSpecRef,
        case_sensitive: bool,
    ) -> Result<Self> {
        // Step 1: Normalize the predicate to eliminate NOT nodes
        // This is required because InclusiveProjection doesn't support NOT
        let normalized_predicate = predicate.clone().rewrite_not();

        // Step 2: Bind the normalized predicate to the table schema
        let bound_predicate = normalized_predicate.bind(schema.clone(), case_sensitive)?;

        // Step 3: Project to partition schema using InclusiveProjection
        let partition_filter = Self::create_partition_filter(
            partition_spec,
            schema,
            &bound_predicate,
            case_sensitive,
        )?;

        Ok(Self {
            evaluator: ExpressionEvaluator::new(partition_filter),
            spec_id: partition_spec.spec_id(),
        })
    }

    /// Returns the partition spec ID this evaluator was created for.
    #[allow(dead_code)]
    pub fn spec_id(&self) -> i32 {
        self.spec_id
    }

    /// Check if this evaluator can evaluate entries with the given spec_id.
    ///
    /// Returns `true` if the spec_id matches, `false` otherwise.
    /// Entries with non-matching spec_ids should be included by default
    /// (conservative behavior) since we can't properly evaluate them.
    pub fn can_evaluate(&self, entry_spec_id: i32) -> bool {
        self.spec_id == entry_spec_id
    }

    /// Check if a partition value matches the filter.
    pub fn matches(&self, partition: &Struct) -> Result<bool> {
        self.evaluator.eval_struct(partition)
    }

    fn create_partition_filter(
        partition_spec: &PartitionSpecRef,
        schema: &Schema,
        predicate: &BoundPredicate,
        case_sensitive: bool,
    ) -> Result<BoundPredicate> {
        let partition_type = partition_spec.partition_type(schema)?;
        let partition_fields = partition_type.fields().to_owned();

        let partition_schema = Schema::builder()
            .with_schema_id(partition_spec.spec_id())
            .with_fields(partition_fields)
            .build()?;

        let mut inclusive_projection = InclusiveProjection::new(partition_spec.clone());

        let partition_filter = inclusive_projection
            .project(predicate)?
            .rewrite_not()
            .bind(Arc::new(partition_schema), case_sensitive)?;

        Ok(partition_filter)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::expr::Reference;
    use crate::spec::{
        Datum, Literal, NestedField, PartitionSpec, PrimitiveType, Transform, Type,
        UnboundPartitionField,
    };

    fn create_test_date_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "date",
                Type::Primitive(PrimitiveType::Date),
            ))])
            .build()
            .unwrap()
    }

    fn create_test_date_partition_spec(schema: &Schema) -> PartitionSpec {
        PartitionSpec::builder(schema.clone())
            .with_spec_id(1)
            .add_unbound_field(
                UnboundPartitionField::builder()
                    .source_id(1)
                    .name("date".to_string())
                    .field_id(1000)
                    .transform(Transform::Identity)
                    .build(),
            )
            .unwrap()
            .build()
            .unwrap()
    }

    #[test]
    fn test_partition_filter_matches_partition() {
        let schema = create_test_date_schema();
        let partition_spec = create_test_date_partition_spec(&schema);

        let predicate =
            Reference::new("date").equal_to(Datum::date_from_str("2024-01-15").unwrap());

        let evaluator = PartitionFilterEvaluator::try_new(
            &predicate,
            &Arc::new(schema),
            &Arc::new(partition_spec),
            true, // case_sensitive
        )
        .unwrap();

        // Partition value: 2024-01-15 (days since epoch = 19737)
        let partition_match = Struct::from_iter([Some(Literal::date(19737))]);
        assert!(evaluator.matches(&partition_match).unwrap());

        // Partition value: 2024-01-16 (should not match)
        let partition_no_match = Struct::from_iter([Some(Literal::date(19738))]);
        assert!(!evaluator.matches(&partition_no_match).unwrap());
    }

    #[test]
    fn test_partition_filter_with_range_predicate() {
        let schema = create_test_date_schema();
        let partition_spec = create_test_date_partition_spec(&schema);

        // date >= '2024-01-15' AND date < '2024-01-20'
        let predicate = Reference::new("date")
            .greater_than_or_equal_to(Datum::date_from_str("2024-01-15").unwrap())
            .and(Reference::new("date").less_than(Datum::date_from_str("2024-01-20").unwrap()));

        let evaluator = PartitionFilterEvaluator::try_new(
            &predicate,
            &Arc::new(schema),
            &Arc::new(partition_spec),
            true,
        )
        .unwrap();

        // 2024-01-15 - should match (at lower bound)
        let partition_lower = Struct::from_iter([Some(Literal::date(19737))]);
        assert!(evaluator.matches(&partition_lower).unwrap());

        // 2024-01-17 - should match (in range)
        let partition_mid = Struct::from_iter([Some(Literal::date(19739))]);
        assert!(evaluator.matches(&partition_mid).unwrap());

        // 2024-01-14 - should NOT match (before range)
        let partition_before = Struct::from_iter([Some(Literal::date(19736))]);
        assert!(!evaluator.matches(&partition_before).unwrap());

        // 2024-01-20 - should NOT match (at upper bound, exclusive)
        let partition_upper = Struct::from_iter([Some(Literal::date(19742))]);
        assert!(!evaluator.matches(&partition_upper).unwrap());
    }

    #[test]
    fn test_partition_filter_with_not_predicate() {
        use std::ops::Not;

        let schema = create_test_date_schema();
        let partition_spec = create_test_date_partition_spec(&schema);

        // NOT(date = '2024-01-15') should match everything except 2024-01-15
        let predicate = Reference::new("date")
            .equal_to(Datum::date_from_str("2024-01-15").unwrap())
            .not();

        let evaluator = PartitionFilterEvaluator::try_new(
            &predicate,
            &Arc::new(schema),
            &Arc::new(partition_spec),
            true,
        )
        .unwrap();

        // 2024-01-15 - should NOT match (negated)
        let partition_excluded = Struct::from_iter([Some(Literal::date(19737))]);
        assert!(!evaluator.matches(&partition_excluded).unwrap());

        // 2024-01-16 - should match
        let partition_included = Struct::from_iter([Some(Literal::date(19738))]);
        assert!(evaluator.matches(&partition_included).unwrap());
    }

    #[test]
    fn test_partition_filter_with_not_in_predicate() {
        let schema = create_test_date_schema();
        let partition_spec = create_test_date_partition_spec(&schema);

        // date NOT IN ('2024-01-15', '2024-01-16')
        let predicate = Reference::new("date").is_not_in([
            Datum::date_from_str("2024-01-15").unwrap(),
            Datum::date_from_str("2024-01-16").unwrap(),
        ]);

        let evaluator = PartitionFilterEvaluator::try_new(
            &predicate,
            &Arc::new(schema),
            &Arc::new(partition_spec),
            true,
        )
        .unwrap();

        // 2024-01-15 - should NOT match (in excluded set)
        let partition_excluded1 = Struct::from_iter([Some(Literal::date(19737))]);
        assert!(!evaluator.matches(&partition_excluded1).unwrap());

        // 2024-01-16 - should NOT match (in excluded set)
        let partition_excluded2 = Struct::from_iter([Some(Literal::date(19738))]);
        assert!(!evaluator.matches(&partition_excluded2).unwrap());

        // 2024-01-17 - should match (not in excluded set)
        let partition_included = Struct::from_iter([Some(Literal::date(19739))]);
        assert!(evaluator.matches(&partition_included).unwrap());
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Non-Identity Transform Tests
    // ═══════════════════════════════════════════════════════════════════════

    fn create_test_timestamp_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "ts",
                Type::Primitive(PrimitiveType::Timestamptz),
            ))])
            .build()
            .unwrap()
    }

    fn create_day_partition_spec(schema: &Schema) -> PartitionSpec {
        PartitionSpec::builder(schema.clone())
            .with_spec_id(1)
            .add_unbound_field(
                UnboundPartitionField::builder()
                    .source_id(1)
                    .name("ts_day".to_string())
                    .field_id(1000)
                    .transform(Transform::Day)
                    .build(),
            )
            .unwrap()
            .build()
            .unwrap()
    }

    #[test]
    fn test_partition_filter_with_day_transform() {
        // Table has timestamp column, partitioned by day(ts)
        let schema = create_test_timestamp_schema();
        let partition_spec = create_day_partition_spec(&schema);

        // Filter: ts >= '2024-01-15T00:00:00Z'
        // This should match partitions with day >= 19737 (2024-01-15)
        let predicate = Reference::new("ts").greater_than_or_equal_to(
            Datum::timestamptz_from_str("2024-01-15T00:00:00Z").unwrap(),
        );

        let evaluator = PartitionFilterEvaluator::try_new(
            &predicate,
            &Arc::new(schema),
            &Arc::new(partition_spec),
            true,
        )
        .unwrap();

        // Day partition value for 2024-01-15 (day 19737)
        // Day transform produces Date type, not Int, so use Literal::date
        let partition_match = Struct::from_iter([Some(Literal::date(19737))]);
        assert!(evaluator.matches(&partition_match).unwrap());

        // Day partition value for 2024-01-16 (day 19738) - should match
        let partition_after = Struct::from_iter([Some(Literal::date(19738))]);
        assert!(evaluator.matches(&partition_after).unwrap());

        // Day partition value for 2024-01-14 (day 19736) - should NOT match
        let partition_before = Struct::from_iter([Some(Literal::date(19736))]);
        assert!(!evaluator.matches(&partition_before).unwrap());
    }

    fn create_test_string_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "category",
                Type::Primitive(PrimitiveType::String),
            ))])
            .build()
            .unwrap()
    }

    fn create_truncate_partition_spec(schema: &Schema, width: u32) -> PartitionSpec {
        PartitionSpec::builder(schema.clone())
            .with_spec_id(1)
            .add_unbound_field(
                UnboundPartitionField::builder()
                    .source_id(1)
                    .name("category_trunc".to_string())
                    .field_id(1000)
                    .transform(Transform::Truncate(width))
                    .build(),
            )
            .unwrap()
            .build()
            .unwrap()
    }

    #[test]
    fn test_partition_filter_with_truncate_transform() {
        // Table has string column, partitioned by truncate(3, category)
        let schema = create_test_string_schema();
        let partition_spec = create_truncate_partition_spec(&schema, 3);

        // Filter: category = 'electronics'
        // Truncate(3) of 'electronics' = 'ele'
        // This should match partitions with truncated value 'ele'
        let predicate = Reference::new("category").equal_to(Datum::string("electronics"));

        let evaluator = PartitionFilterEvaluator::try_new(
            &predicate,
            &Arc::new(schema),
            &Arc::new(partition_spec),
            true,
        )
        .unwrap();

        // Partition with truncated value 'ele' - should match
        let partition_match = Struct::from_iter([Some(Literal::string("ele"))]);
        assert!(evaluator.matches(&partition_match).unwrap());

        // Partition with truncated value 'foo' - should NOT match
        // Note: For equality predicates with truncate, the projection becomes
        // a starts_with check, so 'foo' definitely doesn't match 'ele'
        let partition_no_match = Struct::from_iter([Some(Literal::string("foo"))]);
        assert!(!evaluator.matches(&partition_no_match).unwrap());
    }

    fn create_test_int_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "user_id",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .unwrap()
    }

    fn create_bucket_partition_spec(schema: &Schema, num_buckets: u32) -> PartitionSpec {
        PartitionSpec::builder(schema.clone())
            .with_spec_id(1)
            .add_unbound_field(
                UnboundPartitionField::builder()
                    .source_id(1)
                    .name("user_id_bucket".to_string())
                    .field_id(1000)
                    .transform(Transform::Bucket(num_buckets))
                    .build(),
            )
            .unwrap()
            .build()
            .unwrap()
    }

    #[test]
    fn test_partition_filter_with_bucket_transform() {
        // Table has int column, partitioned by bucket(16, user_id)
        let schema = create_test_int_schema();
        let partition_spec = create_bucket_partition_spec(&schema, 16);

        // Filter: user_id = 42
        // Bucket transforms are lossy - the same bucket can contain many values
        // With inclusive projection, equality on bucketed column projects to
        // check if the bucket matches
        let user_id = 42i32;
        let predicate = Reference::new("user_id").equal_to(Datum::int(user_id));

        let evaluator = PartitionFilterEvaluator::try_new(
            &predicate,
            &Arc::new(schema),
            &Arc::new(partition_spec),
            true,
        )
        .unwrap();

        // Compute expected bucket using Iceberg's bucket algorithm:
        // bucket_N(x) = (murmur3_x86_32_hash(x) & Integer.MAX_VALUE) % N
        // For int values, hash uses the value as i64 in little-endian bytes
        let hash = murmur3::murmur3_32(&mut (user_id as i64).to_le_bytes().as_slice(), 0).unwrap();
        let expected_bucket = ((hash as i32) & i32::MAX) % 16;

        // Verify the expected bucket matches
        let partition_match = Struct::from_iter([Some(Literal::int(expected_bucket))]);
        assert!(
            evaluator.matches(&partition_match).unwrap(),
            "Expected bucket {} to match for user_id={}",
            expected_bucket,
            user_id
        );

        // Verify all other buckets do NOT match
        for bucket in 0..16 {
            if bucket != expected_bucket {
                let partition = Struct::from_iter([Some(Literal::int(bucket))]);
                assert!(
                    !evaluator.matches(&partition).unwrap(),
                    "Bucket {} should NOT match for user_id={}, only bucket {} should",
                    bucket,
                    user_id,
                    expected_bucket
                );
            }
        }
    }
}
