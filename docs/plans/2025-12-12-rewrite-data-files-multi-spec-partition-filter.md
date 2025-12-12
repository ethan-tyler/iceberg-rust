# Multi-Spec Partition Filter for RewriteDataFiles

**Status:** Ready for Implementation
**Author:** Brainstorming session 2025-12-12
**Goal:** Make `partition_filter` work correctly and safely on tables with partition evolution (multiple partition specs).

---

## Overview & Goals

**Problem:** Tables with partition evolution contain files written under different partition specs. The current `PartitionFilterEvaluator` is spec-specific, so a single evaluator cannot correctly evaluate partition filters for all files in an evolved table.

**Goals:**
- Allow `partition_filter` to work on tables with multiple partition specs
- Fail-open by default (never accidentally skip intended compaction work)
- Provide diagnostics when evaluation degrades to best-effort
- Maintain consistent grouping semantics across specs

**Non-goals (deferred):**
- Cross-spec partition "upgrade" (rewriting old data into a new partition scheme)
- Manifest-level pruning optimizations
- Row-level / metrics-based filtering within partitions

**Key principles:**
- **Safety:** If we can't determine whether a file matches the filter, include it. Worst case is compacting extra files; exclusion risks skipping intended work and leaving delete-file bloat.
- **Inclusive projection:** Filter evaluation uses per-spec inclusive projection; lossy transforms (bucket, truncate, etc.) may over-include by design.

---

## Behavior & Semantics

### Filter Decision Logic

When `partition_filter` is `None`:
- No partition pruning is applied.
- Files are still screened by size thresholds and `delete_file_threshold` before becoming compaction candidates.

When `partition_filter` is `Some(predicate)`:
1. Normalize the predicate (`rewrite_not`) and bind it to the table schema.
   - If binding fails (invalid field names, type mismatch), return a hard error.
2. Build per-spec evaluators (one per `spec_id` in table metadata):
   - If projection succeeds, store an evaluator for that `spec_id`.
   - If projection fails (incompatible transforms, missing fields), mark that `spec_id` as unevaluable; files in that spec are included via fail-open.
3. For each manifest entry:
   - If `entry.spec_id` is not present in table metadata: hard error (corruption/invalid manifest).
   - If evaluator for `entry.spec_id` is missing/unevaluable: include (fail-open).
   - Otherwise, evaluate the entry's partition value; if evaluation errors: include (fail-open).

### Grouping Key

All partition-aware operations use a common key:

```rust
PartitionKey = (spec_id: i32, partition: Option<Struct>)
```

Where:
- `partition = Some(struct)` for partitioned specs
- `partition = None` for unpartitioned specs

**Important:** "unpartitioned" is derived from the entry's own `spec_id` (via table metadata), not the table's current default spec. Evolved tables can contain both partitioned and unpartitioned specs.

This key must be consistent across:
- partition-filter memoization (evaluate once per unique key)
- file grouping/bin-packing (compact within key boundaries)
- delete-file association (MoR correctness)

---

## Planner Flow & Algorithm

This design intentionally preserves the current planner pipeline:

1. load data file manifest entries
2. apply `partition_filter` (this design)
3. load delete index (if `delete_file_threshold` enabled)
4. select candidates (size thresholds and/or delete thresholds)
5. group by partition key
6. bin-pack within each key
7. filter groups below minimum thresholds
8. order groups
9. load delete files into groups (V2+)

### Evaluator Construction (once per plan)

- Bind `predicate.clone().rewrite_not()` to the current table schema (hard error if invalid).
- For each partition spec in table metadata, attempt inclusive projection and build an evaluator.
  - Store `HashMap<i32, Option<PartitionFilterEvaluator>>` where:
    - `Some(evaluator)` means evaluable for that spec
    - `None` means projection failed for that spec (fail-open)

### Memoized Evaluation During Entry Scan

During `filter_by_partition`, evaluate at most once per unique `PartitionKey`:

- Compute `PartitionKey` from `entry.spec_id` and `entry.partition` (using per-spec `is_unpartitioned` mapping).
- Lookup evaluator by `spec_id`:
  - Missing key → hard error (unknown `spec_id`)
  - `None` → include (fail-open) and record `PartitionKey` as fail-open
  - `Some(eval)` → evaluate `PartitionKey` once; cache the result
    - `Ok(true)` include all entries with that key
    - `Ok(false)` exclude all entries with that key
    - `Err(_)` include (fail-open) and record `PartitionKey` as fail-open

### Candidate Selection & Delete Threshold Interaction

`delete_file_threshold` requires delete counts from `DeleteIndex`, which is currently built after partition filtering.

To support `fail_open_files` ("candidate files included via fail-open"), record the set of fail-open `PartitionKey`s during filtering, and then increment `fail_open_files` during candidate selection for any entry that:
- passes candidate selection (size/delete thresholds), and
- belongs to a fail-open `PartitionKey`.

### Deterministic Group Assignment

After grouping, sort keys deterministically (e.g., `(spec_id, format!("{:?}", partition))`) prior to bin-packing to keep `group_id` assignment stable across runs with identical input.

---

## Observability & Diagnostics

### FilterStats

Return diagnostic counters from planning:

```rust
#[derive(Clone, Debug, Default)]
pub struct FilterStats {
    pub unevaluable_specs: u32,
    pub eval_errors: u32,
    pub fail_open_partitions: u32,
    pub fail_open_files: u64,
}
```

Definitions:
- `unevaluable_specs`: incremented once per `spec_id` where projection fails
- `eval_errors`: incremented once per unique `PartitionKey` where evaluation errors
- `fail_open_partitions`: incremented once per unique `PartitionKey` included via fail-open
- `fail_open_files`: incremented per *candidate* file (after thresholds) in fail-open partitions

### Logging

The core `iceberg` crate stays log-free. Integrations (DataFusion, CLI) can decide whether/how to surface warnings based on returned stats (e.g., warn if `fail_open_partitions > 0`).

---

## API Changes

### Internal: PartitionFilterEvaluator factoring

Add a constructor that accepts a pre-bound predicate so binding happens once per plan:

```rust
impl PartitionFilterEvaluator {
    pub(crate) fn from_bound(
        bound_predicate: &BoundPredicate,
        schema: &SchemaRef,
        partition_spec: &PartitionSpecRef,
        case_sensitive: bool,
    ) -> Result<Self> { /* ... */ }
}
```

Keep `try_new` as a convenience wrapper that performs `rewrite_not` + binding, then calls `from_bound`.

### Internal: PartitionKey helper

Introduce a `PartitionKey { spec_id, partition }` struct (or equivalent tuple type) for hashing/memoization/grouping consistency.

### Public (additive): planning result with stats

Avoid breaking `RewriteDataFilesPlan` by adding a new return type:

```rust
pub struct RewriteDataFilesPlanningResult {
    pub plan: RewriteDataFilesPlan,
    pub filter_stats: FilterStats,
}

impl RewriteDataFilesPlanner<'_> {
    pub async fn plan_with_stats(&self) -> Result<RewriteDataFilesPlanningResult> { /* ... */ }
    pub async fn plan(&self) -> Result<RewriteDataFilesPlan> {
        self.plan_with_stats().await.map(|r| r.plan)
    }
}
```

---

## Implementation Tasks

### Phase 1: Core Types & Evaluator Refactoring

#### Task 1.1: Add `FilterStats` struct

**File:** `crates/iceberg/src/transaction/rewrite_data_files/mod.rs`

```rust
#[derive(Clone, Debug, Default)]
pub struct FilterStats {
    pub unevaluable_specs: u32,
    pub eval_errors: u32,
    pub fail_open_partitions: u32,
    pub fail_open_files: u64,
}
```

**Acceptance Criteria:**
- [ ] Struct is public and exported from `rewrite_data_files` module
- [ ] Implements `Clone`, `Debug`, `Default`
- [ ] Doc comments explain each field's semantics

---

#### Task 1.2: Add `PartitionKey` struct

**File:** `crates/iceberg/src/transaction/rewrite_data_files/partition_filter.rs`

```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct PartitionKey {
    pub spec_id: i32,
    pub partition: Option<Struct>,
}

impl PartitionKey {
    pub fn from_manifest_entry(
        entry: &ManifestEntry,
        spec: &PartitionSpec,
    ) -> Self {
        Self {
            spec_id: spec.spec_id(),
            partition: if spec.is_unpartitioned() {
                None
            } else {
                Some(entry.data_file().partition().clone())
            },
        }
    }

    /// Stable string representation for deterministic ordering
    pub fn sort_key(&self) -> (i32, String) {
        (self.spec_id, format!("{:?}", self.partition))
    }
}
```

**Acceptance Criteria:**
- [ ] Implements `Hash`, `Eq` for use as HashMap key
- [ ] `from_manifest_entry` correctly derives unpartitioned from spec (not table default)
- [ ] `sort_key()` provides stable ordering for deterministic group assignment
- [ ] Unit tests for partitioned and unpartitioned specs

---

#### Task 1.3: Refactor `PartitionFilterEvaluator` with `from_bound`

**File:** `crates/iceberg/src/transaction/rewrite_data_files/partition_filter.rs`

```rust
impl PartitionFilterEvaluator {
    /// Create from pre-bound predicate (for multi-spec evaluation)
    pub(crate) fn from_bound(
        bound_predicate: &BoundPredicate,
        schema: &SchemaRef,
        partition_spec: &PartitionSpecRef,
        case_sensitive: bool,
    ) -> Result<Self> {
        let partition_filter = Self::create_partition_filter(
            partition_spec, schema, bound_predicate, case_sensitive
        )?;
        Ok(Self {
            evaluator: ExpressionEvaluator::new(partition_filter),
            spec_id: partition_spec.spec_id(),
        })
    }

    /// Existing constructor - convenience wrapper
    pub(crate) fn try_new(
        predicate: &Predicate,
        schema: &SchemaRef,
        partition_spec: &PartitionSpecRef,
        case_sensitive: bool,
    ) -> Result<Self> {
        let normalized = predicate.clone().rewrite_not();
        let bound = normalized.bind(schema.clone(), case_sensitive)?;
        Self::from_bound(&bound, schema, partition_spec, case_sensitive)
    }
}
```

**Acceptance Criteria:**
- [ ] `from_bound` accepts pre-bound predicate
- [ ] `try_new` delegates to `from_bound` after normalization
- [ ] Existing tests continue to pass
- [ ] New test: `from_bound` with pre-normalized predicate

---

### Phase 2: Multi-Spec Evaluator Infrastructure

#### Task 2.1: Add `MultiSpecPartitionFilter` struct

**File:** `crates/iceberg/src/transaction/rewrite_data_files/partition_filter.rs`

```rust
/// Multi-spec partition filter with fail-open semantics
pub(crate) struct MultiSpecPartitionFilter {
    /// Evaluators per spec_id: Some = evaluable, None = fail-open for this spec
    evaluators: HashMap<i32, Option<PartitionFilterEvaluator>>,
    /// Memoized filter results per PartitionKey
    cache: RefCell<HashMap<PartitionKey, bool>>,
    /// Stats tracking
    stats: RefCell<FilterStats>,
    /// Partition keys that were included via fail-open
    fail_open_keys: RefCell<HashSet<PartitionKey>>,
    /// Spec_ids that already logged eval errors (warn-once)
    error_logged_keys: RefCell<HashSet<PartitionKey>>,
}

impl MultiSpecPartitionFilter {
    /// Build evaluators for all specs in table metadata
    pub fn try_new(
        predicate: &Predicate,
        table_metadata: &TableMetadata,
        case_sensitive: bool,
    ) -> Result<Self> {
        // 1. Normalize and bind to table schema (hard error if invalid)
        let normalized = predicate.clone().rewrite_not();
        let schema = table_metadata.current_schema();
        let bound = normalized.bind(schema.clone(), case_sensitive)?;

        // 2. Build evaluator for each spec (None if projection fails)
        let mut evaluators = HashMap::new();
        let mut stats = FilterStats::default();

        for spec in table_metadata.partition_specs_iter() {
            match PartitionFilterEvaluator::from_bound(&bound, schema, spec, case_sensitive) {
                Ok(eval) => {
                    evaluators.insert(spec.spec_id(), Some(eval));
                }
                Err(_) => {
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

    /// Evaluate whether a partition key matches the filter
    /// Returns true if matches OR if fail-open applies
    pub fn matches(&self, key: &PartitionKey) -> Result<bool> {
        // Check cache first
        if let Some(&cached) = self.cache.borrow().get(key) {
            return Ok(cached);
        }

        // Lookup evaluator
        let eval_option = self.evaluators.get(&key.spec_id).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Unknown spec_id {} in manifest entry", key.spec_id),
            )
        })?;

        let result = match eval_option {
            None => {
                // Unevaluable spec → fail-open
                self.record_fail_open(key);
                true
            }
            Some(eval) => {
                match key.partition.as_ref() {
                    None => true, // Unpartitioned → always include
                    Some(partition) => {
                        match eval.matches(partition) {
                            Ok(matches) => matches,
                            Err(_) => {
                                // Eval error → fail-open, warn once per key
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

    fn record_fail_open(&self, key: &PartitionKey) {
        if self.fail_open_keys.borrow_mut().insert(key.clone()) {
            self.stats.borrow_mut().fail_open_partitions += 1;
        }
    }

    /// Check if a key was included via fail-open (for fail_open_files counting)
    pub fn is_fail_open(&self, key: &PartitionKey) -> bool {
        self.fail_open_keys.borrow().contains(key)
    }

    /// Get accumulated stats
    pub fn take_stats(&self) -> FilterStats {
        self.stats.borrow().clone()
    }
}
```

**Acceptance Criteria:**
- [ ] Builds evaluators for all specs in table metadata
- [ ] Records `unevaluable_specs` during construction
- [ ] Memoizes results per `PartitionKey`
- [ ] Fail-open on unevaluable spec or eval error
- [ ] Tracks `fail_open_keys` for downstream `fail_open_files` counting
- [ ] Unit tests for: single spec, multi-spec, unevaluable spec, eval error

---

### Phase 3: Planner Integration

#### Task 3.1: Add `RewriteDataFilesPlanningResult` struct

**File:** `crates/iceberg/src/transaction/rewrite_data_files/mod.rs`

```rust
/// Result of planning, including diagnostics
pub struct RewriteDataFilesPlanningResult {
    pub plan: RewriteDataFilesPlan,
    pub filter_stats: FilterStats,
}
```

**Acceptance Criteria:**
- [ ] Public struct exported from module
- [ ] Contains plan and filter_stats

---

#### Task 3.2: Update `filter_by_partition` to use `MultiSpecPartitionFilter`

**File:** `crates/iceberg/src/transaction/rewrite_data_files/planner.rs`

Current signature:

```rust
fn filter_by_partition(
    entries: Vec<ManifestEntryRef>,
    predicate: &Predicate,
    schema: &SchemaRef,
    partition_spec: &PartitionSpecRef,
    case_sensitive: bool,
) -> Result<Vec<ManifestEntryRef>>
```

New signature:

```rust
fn filter_by_partition(
    entries: Vec<ManifestEntryRef>,
    filter: &MultiSpecPartitionFilter,
    spec_map: &HashMap<i32, PartitionSpecRef>,
) -> Result<Vec<ManifestEntryRef>>
```

**Implementation:**

```rust
fn filter_by_partition(
    entries: Vec<ManifestEntryRef>,
    filter: &MultiSpecPartitionFilter,
    spec_map: &HashMap<i32, PartitionSpecRef>,
) -> Result<Vec<ManifestEntryRef>> {
    let mut result = Vec::with_capacity(entries.len());

    for entry in entries {
        let spec_id = entry.data_file().partition_spec_id();
        let spec = spec_map.get(&spec_id).ok_or_else(|| {
            Error::new(ErrorKind::DataInvalid, format!("Unknown spec_id {}", spec_id))
        })?;

        let key = PartitionKey::from_manifest_entry(&entry, spec);

        if filter.matches(&key)? {
            result.push(entry);
        }
    }

    Ok(result)
}
```

**Acceptance Criteria:**
- [ ] Uses `MultiSpecPartitionFilter` instead of single evaluator
- [ ] Builds `PartitionKey` per entry using entry's own spec
- [ ] Propagates hard errors for unknown spec_id
- [ ] Existing tests updated and passing

---

#### Task 3.3: Add `plan_with_stats` method

**File:** `crates/iceberg/src/transaction/rewrite_data_files/planner.rs`

```rust
impl<'a> RewriteDataFilesPlanner<'a> {
    /// Plan with filter diagnostics
    pub async fn plan_with_stats(&self) -> Result<RewriteDataFilesPlanningResult> {
        // Build multi-spec filter if partition_filter is set
        let filter = match &self.options.partition_filter {
            Some(predicate) => Some(MultiSpecPartitionFilter::try_new(
                predicate,
                self.table.metadata(),
                self.options.case_sensitive,
            )?),
            None => None,
        };

        // ... existing planning logic with filter ...

        // Count fail_open_files during candidate selection
        let mut filter_stats = filter.as_ref()
            .map(|f| f.take_stats())
            .unwrap_or_default();

        // For each candidate file in a fail-open partition, increment counter
        if let Some(ref filter) = filter {
            for candidate in &candidates {
                let spec = spec_map.get(&candidate.spec_id()).unwrap();
                let key = PartitionKey::from_manifest_entry(candidate, spec);
                if filter.is_fail_open(&key) {
                    filter_stats.fail_open_files += 1;
                }
            }
        }

        Ok(RewriteDataFilesPlanningResult {
            plan,
            filter_stats,
        })
    }

    /// Existing API unchanged — discards stats
    pub async fn plan(&self) -> Result<RewriteDataFilesPlan> {
        self.plan_with_stats().await.map(|r| r.plan)
    }
}
```

**Acceptance Criteria:**
- [ ] `plan_with_stats` returns `RewriteDataFilesPlanningResult`
- [ ] `plan` delegates to `plan_with_stats` and discards stats
- [ ] `fail_open_files` counted after candidate selection
- [ ] Existing `plan()` behavior unchanged

---

### Phase 4: Testing

#### Task 4.1: Unit tests for `PartitionKey`

**File:** `crates/iceberg/src/transaction/rewrite_data_files/partition_filter.rs`

| Test Case | Description |
|-----------|-------------|
| `test_partition_key_from_partitioned_spec` | Verify `partition = Some(...)` for partitioned spec |
| `test_partition_key_from_unpartitioned_spec` | Verify `partition = None` for unpartitioned spec |
| `test_partition_key_hash_equality` | Same (spec_id, partition) produces same hash |
| `test_partition_key_sort_key_stable` | `sort_key()` is deterministic |

---

#### Task 4.2: Unit tests for `MultiSpecPartitionFilter`

**File:** `crates/iceberg/src/transaction/rewrite_data_files/partition_filter.rs`

| Test Case | Description |
|-----------|-------------|
| `test_multi_spec_single_spec_table` | Works correctly on non-evolved table |
| `test_multi_spec_evolved_table` | Evaluates correctly per spec_id |
| `test_multi_spec_unevaluable_spec_fail_open` | Includes files when projection fails |
| `test_multi_spec_eval_error_fail_open` | Includes files when matches() errors |
| `test_multi_spec_memoization` | Same key evaluated only once |
| `test_multi_spec_stats_counting` | All counters increment correctly |
| `test_multi_spec_unknown_spec_hard_error` | Unknown spec_id returns error |

---

#### Task 4.3: Integration tests for evolved tables

**File:** `crates/iceberg/src/transaction/rewrite_data_files/planner.rs` (tests module)

| Test Case | Description |
|-----------|-------------|
| `test_plan_evolved_table_no_filter` | Plans correctly without partition_filter |
| `test_plan_evolved_table_with_filter` | Filter applied per-spec correctly |
| `test_plan_evolved_table_fail_open_stats` | Stats reflect fail-open behavior |
| `test_plan_deterministic_group_ids` | Same input produces same group_id ordering |

---

## Execution Order & Dependencies

```text
Phase 1: Core Types (no dependencies)
├── Task 1.1: FilterStats struct
├── Task 1.2: PartitionKey struct
└── Task 1.3: PartitionFilterEvaluator.from_bound

Phase 2: Multi-Spec Infrastructure (depends on Phase 1)
└── Task 2.1: MultiSpecPartitionFilter

Phase 3: Planner Integration (depends on Phase 2)
├── Task 3.1: RewriteDataFilesPlanningResult
├── Task 3.2: Update filter_by_partition
└── Task 3.3: Add plan_with_stats

Phase 4: Testing (parallel with Phase 2-3)
├── Task 4.1: PartitionKey tests
├── Task 4.2: MultiSpecPartitionFilter tests
└── Task 4.3: Integration tests
```

**Estimated complexity:** Medium (isolated changes, no architectural shifts)

---

## Verification Checklist

Before marking complete:

- [ ] All existing `rewrite_data_files` tests pass
- [ ] New unit tests for `PartitionKey`, `MultiSpecPartitionFilter`
- [ ] Integration test with evolved table (multiple spec_ids)
- [ ] `plan()` API unchanged (backward compatible)
- [ ] `plan_with_stats()` returns correct `FilterStats`
- [ ] `fail_open_files` counts candidates, not raw entries
- [ ] Deterministic group ordering verified
- [ ] `cargo clippy` clean
- [ ] `cargo fmt` applied

---

## Future Work

- **Partition spec upgrade mode:** `target_spec_id` + recompute partition values from data to rewrite old-spec files into the target spec (true migration and cross-spec consolidation).
- **Manifest-level pruning:** per-spec manifest evaluators to skip loading entry lists when a manifest cannot match a filter.
- **Configurable strictness:** allow users to opt into "fail-closed" modes for CI/pipelines.
- **Richer diagnostics:** optionally return bounded structured warnings (deduplicated) for UIs and automated checks.
- **Filter + delete threshold optimization:** minimize passes by integrating delete counting into grouping/candidate selection (explicit memory/pass tradeoff).

---

## References

- [ADR-002: Partition Evolution Handling](../datafusion-update/adr/ADR-002-partition-evolution.md)
- [Existing PartitionFilterEvaluator](../../crates/iceberg/src/transaction/rewrite_data_files/partition_filter.rs)
- [Phase Roadmap](../../../Phase%20Roadmap%20for%20Rewrite%20Data%20Files.md)
