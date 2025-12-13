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

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::FilterStats;
use crate::error::{Error, ErrorKind, Result};
use crate::expr::visitors::expression_evaluator::ExpressionEvaluator;
use crate::expr::visitors::inclusive_projection::InclusiveProjection;
use crate::expr::{Bind, BoundPredicate, Predicate};
use crate::spec::{
    ManifestEntry, PartitionSpec, PartitionSpecRef, Schema, SchemaRef, Struct, TableMetadata,
};

/// Key for grouping and memoizing partition-related operations.
///
/// This struct is the canonical representation for:
/// - Partition filter memoization (evaluate once per unique key)
/// - Fail-open tracking and diagnostics (FilterStats)
///
/// # Partition Evolution
///
/// The key includes `spec_id` to correctly handle tables with multiple
/// partition specs. Files from different specs are kept separate even
/// if their partition values happen to be equal.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct PartitionKey {
    /// The partition spec ID this key belongs to.
    pub spec_id: i32,
    /// The partition value. `None` for unpartitioned specs.
    pub partition: Option<Struct>,
}

impl PartitionKey {
    /// Create a partition key from a manifest entry and its partition spec.
    ///
    /// # Arguments
    ///
    /// * `entry` - The manifest entry containing the data file
    /// * `spec` - The partition spec for this entry (should match entry's spec_id)
    ///
    /// # Correctness
    ///
    /// Callers should ensure `spec.spec_id()` matches `entry.data_file.partition_spec_id`
    /// for correct behavior. Mismatches won't panic but will produce incorrect keys.
    pub fn from_manifest_entry(entry: &ManifestEntry, spec: &PartitionSpec) -> Self {
        Self {
            spec_id: spec.spec_id(),
            partition: if spec.is_unpartitioned() {
                None
            } else {
                Some(entry.data_file.partition().clone())
            },
        }
    }

    /// Produce a stable sort key for deterministic group ordering.
    ///
    /// Returns `(spec_id, partition_repr)` where `partition_repr` is the
    /// Debug string representation of the partition struct (empty string for
    /// unpartitioned). This matches the existing planner ordering convention.
    #[allow(dead_code)] // Currently used only in tests; keep for deterministic ordering utilities.
    pub fn sort_key(&self) -> (i32, String) {
        let partition_repr = self
            .partition
            .as_ref()
            .map(|p| format!("{p:?}"))
            .unwrap_or_default();
        (self.spec_id, partition_repr)
    }
}

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
    /// Create an evaluator from a pre-bound predicate.
    ///
    /// This is the core constructor used by `MultiSpecPartitionFilter` to build
    /// evaluators for multiple partition specs from a single bound predicate.
    ///
    /// # Arguments
    ///
    /// * `bound_predicate` - Already normalized and bound predicate
    /// * `schema` - Table schema (for partition type derivation)
    /// * `partition_spec` - Partition spec to project predicate
    /// * `case_sensitive` - Whether field matching is case-sensitive
    ///
    /// # Errors
    ///
    /// Returns an error if the predicate cannot be projected to the partition
    /// spec (e.g., incompatible transforms or missing fields).
    pub fn from_bound(
        bound_predicate: &BoundPredicate,
        schema: &SchemaRef,
        partition_spec: &PartitionSpecRef,
        case_sensitive: bool,
    ) -> Result<Self> {
        let partition_filter =
            Self::create_partition_filter(partition_spec, schema, bound_predicate, case_sensitive)?;

        Ok(Self {
            evaluator: ExpressionEvaluator::new(partition_filter),
            spec_id: partition_spec.spec_id(),
        })
    }

    /// Create a new partition filter evaluator.
    ///
    /// This is a convenience constructor that normalizes, binds, and projects
    /// the predicate in one step. For multi-spec evaluation, use `from_bound`
    /// to avoid repeated binding.
    ///
    /// # Arguments
    ///
    /// * `predicate` - Unbound predicate to filter partitions
    /// * `schema` - Table schema (for binding the predicate)
    /// * `partition_spec` - Partition spec to project predicate
    /// * `case_sensitive` - Whether field matching is case-sensitive
    #[allow(dead_code)] // Used in tests; production code uses MultiSpecPartitionFilter
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

        // Step 3: Delegate to from_bound for projection
        Self::from_bound(&bound_predicate, schema, partition_spec, case_sensitive)
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

/// Multi-spec partition filter with fail-open semantics.
///
/// Evaluates partition filters across tables with multiple partition specs
/// (partition evolution). When a filter cannot be evaluated for a particular
/// spec or partition, files are included rather than excluded (fail-open).
///
/// # Usage
///
/// ```ignore
/// let filter = MultiSpecPartitionFilter::try_new(
///     &predicate,
///     table.metadata(),
///     true, // case_sensitive
/// )?;
///
/// for entry in entries {
///     let spec = spec_map.get(&entry.data_file.partition_spec_id).unwrap();
///     let key = PartitionKey::from_manifest_entry(&entry, spec);
///     if filter.matches(&key)? {
///         // Include this entry
///     }
/// }
///
/// let stats = filter.stats();
/// if stats.fail_open_partitions > 0 {
///     warn!("Some partitions could not be evaluated");
/// }
/// ```
pub(crate) struct MultiSpecPartitionFilter {
    /// Evaluators per spec_id: `Some(eval)` = evaluable, `None` = fail-open for this spec.
    evaluators: HashMap<i32, Option<PartitionFilterEvaluator>>,

    /// Memoized filter results per PartitionKey.
    cache: RefCell<HashMap<PartitionKey, bool>>,

    /// Accumulated stats.
    stats: RefCell<FilterStats>,

    /// Partition keys that were included via fail-open.
    fail_open_keys: RefCell<HashSet<PartitionKey>>,

    /// Partition keys that already hit eval errors (dedupe `eval_errors` increments).
    error_logged_keys: RefCell<HashSet<PartitionKey>>,
}

impl MultiSpecPartitionFilter {
    /// Build evaluators for all partition specs in table metadata.
    ///
    /// # Arguments
    ///
    /// * `predicate` - The partition filter predicate
    /// * `table_metadata` - Table metadata containing partition specs
    /// * `case_sensitive` - Whether field matching is case-sensitive
    ///
    /// # Errors
    ///
    /// Returns an error if the predicate cannot be bound to the table schema
    /// (invalid field names, type mismatch). Projection failures for individual
    /// specs are recorded as unevaluable (fail-open) rather than hard errors.
    pub fn try_new(
        predicate: &Predicate,
        table_metadata: &TableMetadata,
        case_sensitive: bool,
    ) -> Result<Self> {
        // Step 1: Normalize and bind to table schema (hard error if invalid)
        let normalized = predicate.clone().rewrite_not();
        let schema = table_metadata.current_schema();
        let bound = normalized.bind(schema.clone(), case_sensitive)?;

        // Step 2: Build evaluator for each spec (None if projection fails)
        let mut evaluators = HashMap::new();
        let mut stats = FilterStats::default();

        for spec in table_metadata.partition_specs_iter() {
            match PartitionFilterEvaluator::from_bound(&bound, schema, spec, case_sensitive) {
                Ok(eval) => {
                    evaluators.insert(spec.spec_id(), Some(eval));
                }
                Err(_) => {
                    // Projection failed - mark as unevaluable, fail-open for this spec
                    stats.unevaluable_specs += 1;
                    evaluators.insert(spec.spec_id(), None);
                }
            }
        }

        Ok(Self {
            evaluators,
            cache: RefCell::new(HashMap::new()),
            stats: RefCell::new(stats),
            fail_open_keys: RefCell::new(HashSet::new()),
            error_logged_keys: RefCell::new(HashSet::new()),
        })
    }

    /// Evaluate whether a partition key matches the filter.
    ///
    /// Returns `true` if:
    /// - The filter evaluates to true for this partition, OR
    /// - The filter cannot be evaluated (fail-open behavior)
    ///
    /// # Errors
    ///
    /// Returns an error only for hard failures:
    /// - Unknown `spec_id` not present in table metadata (data corruption)
    ///
    /// Soft failures (unevaluable spec, eval error) return `Ok(true)` (fail-open).
    pub fn matches(&self, key: &PartitionKey) -> Result<bool> {
        // Check cache first
        if let Some(&cached) = self.cache.borrow().get(key) {
            return Ok(cached);
        }

        // Lookup evaluator by spec_id
        let eval_option = self.evaluators.get(&key.spec_id).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Unknown partition spec ID {} in manifest entry. \
                     This may indicate table metadata corruption or an outdated filter.",
                    key.spec_id
                ),
            )
        })?;

        let result = match eval_option {
            None => {
                // Unevaluable spec → fail-open
                self.record_fail_open(key);
                true
            }
            Some(eval) => {
                match &key.partition {
                    None => {
                        // Unpartitioned spec → always include (not fail-open, just no filter applies)
                        true
                    }
                    Some(partition) => {
                        match eval.matches(partition) {
                            Ok(matches) => matches,
                            Err(_) => {
                                // Eval error → fail-open, track for stats (warn-once per key)
                                if self.error_logged_keys.borrow_mut().insert(key.clone()) {
                                    self.stats.borrow_mut().eval_errors += 1;
                                }
                                self.record_fail_open(key);
                                true
                            }
                        }
                    }
                }
            }
        };

        self.cache.borrow_mut().insert(key.clone(), result);
        Ok(result)
    }

    /// Record that a partition key was included via fail-open.
    fn record_fail_open(&self, key: &PartitionKey) {
        if self.fail_open_keys.borrow_mut().insert(key.clone()) {
            self.stats.borrow_mut().fail_open_partitions += 1;
        }
    }

    /// Check if a partition key was included via fail-open.
    ///
    /// Used by the planner to count `fail_open_files` after candidate selection.
    pub fn is_fail_open(&self, key: &PartitionKey) -> bool {
        self.fail_open_keys.borrow().contains(key)
    }

    /// Get accumulated stats (cloned).
    ///
    /// Call this after filtering is complete to get diagnostics.
    pub fn stats(&self) -> FilterStats {
        self.stats.borrow().clone()
    }

    /// Increment the fail_open_files counter.
    ///
    /// Called by the planner for each candidate file in a fail-open partition.
    pub fn increment_fail_open_files(&self) {
        self.stats.borrow_mut().fail_open_files += 1;
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
        let predicate = Reference::new("ts")
            .greater_than_or_equal_to(Datum::timestamptz_from_str("2024-01-15T00:00:00Z").unwrap());

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
            "Expected bucket {expected_bucket} to match for user_id={user_id}",
        );

        // Verify all other buckets do NOT match
        for bucket in 0..16 {
            if bucket != expected_bucket {
                let partition = Struct::from_iter([Some(Literal::int(bucket))]);
                assert!(
                    !evaluator.matches(&partition).unwrap(),
                    "Bucket {bucket} should NOT match for user_id={user_id}, only bucket {expected_bucket} should",
                );
            }
        }
    }

    // =========================================================================
    // PartitionKey Tests
    // =========================================================================

    #[test]
    fn test_partition_key_equality_same_spec_same_partition() {
        let partition = Struct::from_iter([Some(Literal::date(19737))]);
        let key1 = PartitionKey {
            spec_id: 1,
            partition: Some(partition.clone()),
        };
        let key2 = PartitionKey {
            spec_id: 1,
            partition: Some(partition),
        };
        assert_eq!(key1, key2);
    }

    #[test]
    fn test_partition_key_inequality_different_spec_same_partition() {
        let partition = Struct::from_iter([Some(Literal::date(19737))]);
        let key1 = PartitionKey {
            spec_id: 1,
            partition: Some(partition.clone()),
        };
        let key2 = PartitionKey {
            spec_id: 2,
            partition: Some(partition),
        };
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_partition_key_inequality_same_spec_different_partition() {
        let key1 = PartitionKey {
            spec_id: 1,
            partition: Some(Struct::from_iter([Some(Literal::date(19737))])),
        };
        let key2 = PartitionKey {
            spec_id: 1,
            partition: Some(Struct::from_iter([Some(Literal::date(19738))])),
        };
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_partition_key_unpartitioned() {
        let key = PartitionKey {
            spec_id: 0,
            partition: None,
        };
        assert_eq!(key.spec_id, 0);
        assert!(key.partition.is_none());
    }

    #[test]
    fn test_partition_key_hash_map_usage() {
        use std::collections::HashMap;

        let partition = Struct::from_iter([Some(Literal::date(19737))]);
        let key1 = PartitionKey {
            spec_id: 1,
            partition: Some(partition.clone()),
        };
        let key2 = PartitionKey {
            spec_id: 1,
            partition: Some(partition),
        };

        let mut map: HashMap<PartitionKey, u32> = HashMap::new();
        map.insert(key1, 42);

        // Same key should retrieve the value
        assert_eq!(map.get(&key2), Some(&42));
    }

    #[test]
    fn test_partition_key_sort_key_with_partition() {
        let key = PartitionKey {
            spec_id: 1,
            partition: Some(Struct::from_iter([Some(Literal::date(19737))])),
        };
        let (spec_id, partition_repr) = key.sort_key();
        assert_eq!(spec_id, 1);
        assert!(!partition_repr.is_empty());
    }

    #[test]
    fn test_partition_key_sort_key_unpartitioned() {
        let key = PartitionKey {
            spec_id: 0,
            partition: None,
        };
        let (spec_id, partition_repr) = key.sort_key();
        assert_eq!(spec_id, 0);
        assert_eq!(partition_repr, ""); // Empty string for unpartitioned
    }

    #[test]
    fn test_partition_key_sort_key_ordering() {
        // Keys should be sortable by (spec_id, partition_repr)
        let keys = vec![
            PartitionKey {
                spec_id: 2,
                partition: Some(Struct::from_iter([Some(Literal::string("b"))])),
            },
            PartitionKey {
                spec_id: 1,
                partition: Some(Struct::from_iter([Some(Literal::string("a"))])),
            },
            PartitionKey {
                spec_id: 1,
                partition: Some(Struct::from_iter([Some(Literal::string("b"))])),
            },
        ];

        let mut sorted_keys = keys.clone();
        sorted_keys.sort_by_key(|k| k.sort_key());

        // Should be ordered by spec_id first, then partition_repr
        assert_eq!(sorted_keys[0].spec_id, 1);
        assert_eq!(sorted_keys[1].spec_id, 1);
        assert_eq!(sorted_keys[2].spec_id, 2);
    }

    // =========================================================================
    // MultiSpecPartitionFilter Tests
    // =========================================================================

    /// Creates table metadata with two partition specs for testing multi-spec scenarios.
    ///
    /// # Partition Specs
    ///
    /// TableMetadataBuilder assigns spec_ids based on order of addition:
    /// - spec_id=0: Partitioned by date (identity) - the default/initial spec
    /// - spec_id=1: Unpartitioned (added via add_partition_spec)
    fn create_multi_spec_table_metadata() -> TableMetadata {
        use crate::spec::{
            FormatVersion, NestedField, SortOrder, TableMetadataBuilder, UnboundPartitionSpec,
        };

        // Schema with date and category columns
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                Arc::new(NestedField::optional(
                    1,
                    "date",
                    Type::Primitive(PrimitiveType::Date),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "category",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .unwrap();

        // Default spec: Partitioned by date (identity) - will get spec_id=0
        let partitioned_spec = UnboundPartitionSpec::builder()
            .add_partition_field(1, "date", Transform::Identity)
            .unwrap()
            .build();

        // Secondary spec: Unpartitioned - will get spec_id=1
        let unpartitioned_spec = UnboundPartitionSpec::builder().build();

        TableMetadataBuilder::new(
            schema,
            partitioned_spec,
            SortOrder::unsorted_order(),
            "s3://bucket/path".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .add_partition_spec(unpartitioned_spec)
        .unwrap()
        .build()
        .unwrap()
        .metadata
    }

    #[test]
    fn test_multi_spec_filter_unknown_spec_id_is_hard_error() {
        let metadata = create_multi_spec_table_metadata();

        // Filter: date >= 2024-01-15
        let predicate = Reference::new("date").greater_than_or_equal_to(Datum::date(19737));

        let filter = MultiSpecPartitionFilter::try_new(&predicate, &metadata, true).unwrap();

        // Use a spec_id that doesn't exist in metadata
        let unknown_key = PartitionKey {
            spec_id: 999,
            partition: Some(Struct::from_iter([Some(Literal::date(19737))])),
        };

        let result = filter.matches(&unknown_key);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Unknown partition spec ID 999")
        );
    }

    #[test]
    fn test_multi_spec_filter_unpartitioned_spec_always_includes() {
        let metadata = create_multi_spec_table_metadata();

        // Filter: date >= 2024-01-15
        let predicate = Reference::new("date").greater_than_or_equal_to(Datum::date(19737));

        let filter = MultiSpecPartitionFilter::try_new(&predicate, &metadata, true).unwrap();

        // Unpartitioned entry (spec_id=1, partition=None)
        // Note: spec_id=1 is unpartitioned per create_multi_spec_table_metadata
        let unpartitioned_key = PartitionKey {
            spec_id: 1,
            partition: None,
        };

        // Should be included (no filtering applies)
        assert!(filter.matches(&unpartitioned_key).unwrap());

        // Should NOT be counted as fail-open (it's expected behavior)
        assert!(!filter.is_fail_open(&unpartitioned_key));
    }

    #[test]
    fn test_multi_spec_filter_matching_partition() {
        let metadata = create_multi_spec_table_metadata();

        // Filter: date >= 2024-01-15 (day 19737)
        let predicate = Reference::new("date").greater_than_or_equal_to(Datum::date(19737));

        let filter = MultiSpecPartitionFilter::try_new(&predicate, &metadata, true).unwrap();

        // Partition with date 2024-01-16 (day 19738) - should match
        // Note: spec_id=0 is partitioned by date per create_multi_spec_table_metadata
        let matching_key = PartitionKey {
            spec_id: 0,
            partition: Some(Struct::from_iter([Some(Literal::date(19738))])),
        };

        assert!(filter.matches(&matching_key).unwrap());
        assert!(!filter.is_fail_open(&matching_key));
    }

    #[test]
    fn test_multi_spec_filter_non_matching_partition() {
        let metadata = create_multi_spec_table_metadata();

        // Filter: date >= 2024-01-15 (day 19737)
        let predicate = Reference::new("date").greater_than_or_equal_to(Datum::date(19737));

        let filter = MultiSpecPartitionFilter::try_new(&predicate, &metadata, true).unwrap();

        // Partition with date 2024-01-14 (day 19736) - should NOT match
        // Note: spec_id=0 is partitioned by date per create_multi_spec_table_metadata
        let non_matching_key = PartitionKey {
            spec_id: 0,
            partition: Some(Struct::from_iter([Some(Literal::date(19736))])),
        };

        assert!(!filter.matches(&non_matching_key).unwrap());
        assert!(!filter.is_fail_open(&non_matching_key));
    }

    #[test]
    fn test_multi_spec_filter_memoization() {
        let metadata = create_multi_spec_table_metadata();

        let predicate = Reference::new("date").greater_than_or_equal_to(Datum::date(19737));

        let filter = MultiSpecPartitionFilter::try_new(&predicate, &metadata, true).unwrap();

        // Note: spec_id=0 is partitioned by date per create_multi_spec_table_metadata
        let key = PartitionKey {
            spec_id: 0,
            partition: Some(Struct::from_iter([Some(Literal::date(19738))])),
        };

        // First call
        let result1 = filter.matches(&key).unwrap();

        // Second call should return cached result
        let result2 = filter.matches(&key).unwrap();

        assert_eq!(result1, result2);

        // Verify memoization by checking cache
        assert!(filter.cache.borrow().contains_key(&key));
    }

    #[test]
    fn test_multi_spec_filter_stats_tracking() {
        let metadata = create_multi_spec_table_metadata();

        let predicate = Reference::new("date").greater_than_or_equal_to(Datum::date(19737));

        let filter = MultiSpecPartitionFilter::try_new(&predicate, &metadata, true).unwrap();

        // Initial stats should be clean
        let stats = filter.stats();
        assert_eq!(stats.unevaluable_specs, 0);
        assert_eq!(stats.eval_errors, 0);
        assert_eq!(stats.fail_open_partitions, 0);
        assert_eq!(stats.fail_open_files, 0);

        // Test increment_fail_open_files
        filter.increment_fail_open_files();
        filter.increment_fail_open_files();
        filter.increment_fail_open_files();

        let stats = filter.stats();
        assert_eq!(stats.fail_open_files, 3);
    }

    #[test]
    fn test_multi_spec_filter_unevaluable_spec_fail_open() {
        use crate::spec::{FormatVersion, SortOrder, TableMetadataBuilder, UnboundPartitionSpec};

        // Schema with date column only
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "date",
                Type::Primitive(PrimitiveType::Date),
            ))])
            .build()
            .unwrap();

        // Spec A: Partitioned by unknown transform - projection should fail (unevaluable spec).
        let spec_unknown = UnboundPartitionSpec::builder()
            .add_partition_field(1, "date_unknown", Transform::Unknown)
            .unwrap()
            .build();

        // Spec B: Partitioned by identity(date) - projection should succeed.
        let spec_identity = UnboundPartitionSpec::builder()
            .add_partition_field(1, "date", Transform::Identity)
            .unwrap()
            .build();

        let metadata = TableMetadataBuilder::new(
            schema,
            spec_identity,
            SortOrder::unsorted_order(),
            "s3://bucket/path".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .add_partition_spec(spec_unknown)
        .unwrap()
        .build()
        .unwrap()
        .metadata;

        let predicate = Reference::new("date").greater_than_or_equal_to(Datum::date(19737));

        let filter = MultiSpecPartitionFilter::try_new(&predicate, &metadata, true).unwrap();

        let unknown_spec_id = metadata
            .partition_specs_iter()
            .find(|spec| {
                spec.fields()
                    .iter()
                    .any(|f| f.transform == Transform::Unknown)
            })
            .unwrap()
            .spec_id();

        let stats = filter.stats();
        assert_eq!(stats.unevaluable_specs, 1);

        // Unevaluable spec should be included via fail-open.
        let key = PartitionKey {
            spec_id: unknown_spec_id,
            partition: Some(Struct::from_iter([Some(Literal::string("ignored"))])),
        };
        assert!(filter.matches(&key).unwrap());
        assert!(filter.is_fail_open(&key));
        assert_eq!(filter.stats().fail_open_partitions, 1);
    }

    #[test]
    fn test_multi_spec_filter_cache_isolation() {
        let metadata = create_multi_spec_table_metadata();

        let predicate = Reference::new("date").greater_than_or_equal_to(Datum::date(19737));

        let filter = MultiSpecPartitionFilter::try_new(&predicate, &metadata, true).unwrap();

        // Two different keys: partitioned (spec_id=0) vs unpartitioned (spec_id=1)
        // per create_multi_spec_table_metadata
        let key1 = PartitionKey {
            spec_id: 0,
            partition: Some(Struct::from_iter([Some(Literal::date(19738))])),
        };

        let key2 = PartitionKey {
            spec_id: 1,
            partition: None, // Unpartitioned
        };

        // Evaluate both
        filter.matches(&key1).unwrap();
        filter.matches(&key2).unwrap();

        // Both should be cached separately
        let cache = filter.cache.borrow();
        assert!(cache.contains_key(&key1));
        assert!(cache.contains_key(&key2));
        assert_eq!(cache.len(), 2);
    }
}
