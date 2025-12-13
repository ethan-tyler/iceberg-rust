# Rewrite Data Files Implementation Plan (Phases 2.2-2.5)

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Complete the rewrite_data_files feature with partition filtering, delete-aware selection, dangling delete cleanup, and partial progress commits.

**Architecture:** The implementation extends the existing planner/committer workflow. Phase 2.2 adds predicate-based filtering during planning. Phase 2.3 adds delete-count-based candidate selection. Phase 2.4 tracks delete-to-data relationships for cleanup. Phase 2.5 restructures the committer to support multiple snapshot commits with failure handling.

**Tech Stack:** Rust, Apache Iceberg spec (V2+), existing `expr` module for predicate evaluation, `BoundPredicate`, `ExpressionEvaluator`, `InclusiveProjection`

---

## Phase 2.2: Partition Filter

**Complexity:** Low
**Dependencies:** None (isolated predicate evaluation)

### Overview

Enable users to compact specific partitions by providing a predicate. The planner filters entries based on partition value matching before grouping.

### Task 1: Create Partition Filter Helper Module

**Files:**
- Create: `crates/iceberg/src/transaction/rewrite_data_files/partition_filter.rs`
- Modify: `crates/iceberg/src/transaction/rewrite_data_files/mod.rs`

**Step 1: Write the failing test**

In `partition_filter.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::{Predicate, Reference};
    use crate::spec::{Datum, Literal, Struct};
    use std::sync::Arc;

    #[test]
    fn test_partition_filter_matches_partition() {
        // This test will fail until we implement PartitionFilterEvaluator
        let schema = create_test_schema();
        let partition_spec = create_test_partition_spec(&schema);

        let predicate = Reference::new("date").equal_to(Datum::date_from_str("2024-01-15").unwrap());

        let evaluator = PartitionFilterEvaluator::try_new(
            &predicate,
            &schema,
            &partition_spec,
            true, // case_sensitive
        ).unwrap();

        // Partition value: 2024-01-15
        let partition = Struct::from_iter([Some(Literal::date(19737))]); // 2024-01-15
        assert!(evaluator.matches(&partition).unwrap());

        // Partition value: 2024-01-16 (should not match)
        let partition = Struct::from_iter([Some(Literal::date(19738))]);
        assert!(!evaluator.matches(&partition).unwrap());
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p iceberg --lib partition_filter::tests::test_partition_filter_matches_partition`
Expected: FAIL with "cannot find value `PartitionFilterEvaluator`"

**Step 3: Write minimal implementation**

```rust
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Partition filter evaluation for rewrite_data_files.

use std::sync::Arc;

use crate::error::Result;
use crate::expr::visitors::expression_evaluator::ExpressionEvaluator;
use crate::expr::visitors::inclusive_projection::InclusiveProjection;
use crate::expr::{Bind, BoundPredicate, Predicate};
use crate::spec::{PartitionSpec, PartitionSpecRef, Schema, SchemaRef, Struct};

/// Evaluates partition values against a filter predicate.
///
/// Used by `RewriteDataFilesPlanner` to filter which partitions should be compacted.
pub(crate) struct PartitionFilterEvaluator {
    evaluator: ExpressionEvaluator,
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
        // Step 1: Bind the predicate to the table schema
        let bound_predicate = predicate.clone().bind(schema.clone(), case_sensitive)?;

        // Step 2: Project to partition schema using InclusiveProjection
        let partition_filter = Self::create_partition_filter(
            partition_spec,
            schema,
            &bound_predicate,
            case_sensitive,
        )?;

        Ok(Self {
            evaluator: ExpressionEvaluator::new(partition_filter),
        })
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
```

**Step 4: Add module to mod.rs**

Add to `crates/iceberg/src/transaction/rewrite_data_files/mod.rs`:

```rust
mod partition_filter;
```

**Step 5: Run test to verify it passes**

Run: `cargo test -p iceberg --lib partition_filter::tests::test_partition_filter_matches_partition`
Expected: PASS

**Step 6: Commit**

```bash
git add crates/iceberg/src/transaction/rewrite_data_files/partition_filter.rs
git add crates/iceberg/src/transaction/rewrite_data_files/mod.rs
git commit -m "feat(rewrite): add PartitionFilterEvaluator for partition filtering"
```

### Task 2: Extend ExpressionEvaluator for Struct Evaluation

**Files:**
- Modify: `crates/iceberg/src/expr/visitors/expression_evaluator.rs`

**Step 1: Write the failing test**

Add to `expression_evaluator.rs` tests:

```rust
#[test]
fn test_eval_struct_directly() {
    let case_sensitive = true;
    let (partition_spec, schema) = create_partition_spec(PrimitiveType::Float).unwrap();

    let predicate = Predicate::Binary(BinaryExpression::new(
        PredicateOperator::Eq,
        Reference::new("a"),
        Datum::float(1.0),
    ))
    .bind(schema.clone(), case_sensitive)
    .unwrap();

    let partition_filter =
        create_partition_filter(partition_spec, &schema, &predicate, case_sensitive).unwrap();

    let evaluator = ExpressionEvaluator::new(partition_filter);

    // Test eval_struct method
    let partition = Struct::from_iter([Some(Literal::float(1.0))]);
    let result = evaluator.eval_struct(&partition).unwrap();
    assert!(result);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p iceberg --lib expr::visitors::expression_evaluator::tests::test_eval_struct_directly`
Expected: FAIL with "no method named `eval_struct`"

**Step 3: Add eval_struct method**

In `expression_evaluator.rs`, add to `impl ExpressionEvaluator`:

```rust
/// Evaluate this filter against a partition struct directly.
///
/// This is useful when you have a partition Struct without a DataFile wrapper,
/// such as during partition filtering in rewrite_data_files.
pub(crate) fn eval_struct(&self, partition: &Struct) -> Result<bool> {
    let mut visitor = ExpressionEvaluatorVisitor::new(partition);
    visit(&mut visitor, &self.partition_filter)
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p iceberg --lib expr::visitors::expression_evaluator::tests::test_eval_struct_directly`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/iceberg/src/expr/visitors/expression_evaluator.rs
git commit -m "feat(expr): add eval_struct method to ExpressionEvaluator"
```

### Task 3: Integrate Partition Filter into Planner

**Files:**
- Modify: `crates/iceberg/src/transaction/rewrite_data_files/planner.rs`

**Step 1: Write the failing test**

Add to `planner.rs` tests:

```rust
#[test]
fn test_filter_by_partition_with_predicate() {
    // Create mock entries with different partitions
    let entry1 = test_entry_with_partition("file1.parquet", 50_000, Struct::from_iter([Some(Literal::date(19737))])); // 2024-01-15
    let entry2 = test_entry_with_partition("file2.parquet", 50_000, Struct::from_iter([Some(Literal::date(19738))])); // 2024-01-16
    let entries = vec![entry1, entry2];

    let schema = create_test_date_schema();
    let partition_spec = create_test_date_partition_spec(&schema);

    let predicate = Reference::new("date").equal_to(Datum::date_from_str("2024-01-15").unwrap());

    let evaluator = PartitionFilterEvaluator::try_new(
        &predicate,
        &Arc::new(schema),
        &Arc::new(partition_spec),
        true,
    ).unwrap();

    let filtered = filter_entries_by_partition(entries, Some(&evaluator)).unwrap();

    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].data_file.file_path, "file1.parquet");
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p iceberg --lib rewrite_data_files::planner::tests::test_filter_by_partition_with_predicate`
Expected: FAIL with "cannot find function `filter_entries_by_partition`"

**Step 3: Implement filter_entries_by_partition helper**

In `planner.rs`, add:

```rust
use super::partition_filter::PartitionFilterEvaluator;

/// Filter entries by partition predicate.
fn filter_entries_by_partition(
    entries: Vec<ManifestEntryRef>,
    evaluator: Option<&PartitionFilterEvaluator>,
) -> Result<Vec<ManifestEntryRef>> {
    match evaluator {
        None => Ok(entries),
        Some(eval) => {
            let mut filtered = Vec::with_capacity(entries.len());
            for entry in entries {
                let partition = entry.data_file.partition();
                if eval.matches(partition)? {
                    filtered.push(entry);
                }
            }
            Ok(filtered)
        }
    }
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p iceberg --lib rewrite_data_files::planner::tests::test_filter_by_partition_with_predicate`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/iceberg/src/transaction/rewrite_data_files/planner.rs
git commit -m "feat(rewrite): add partition entry filtering helper"
```

### Task 4: Update Planner to Use Partition Filter

**Files:**
- Modify: `crates/iceberg/src/transaction/rewrite_data_files/planner.rs`

**Step 1: Update filter_by_partition method**

Replace the current `filter_by_partition` method:

```rust
/// Filter entries by partition predicate.
fn filter_by_partition(
    &self,
    entries: Vec<ManifestEntryRef>,
) -> Result<Vec<ManifestEntryRef>> {
    match &self.partition_filter {
        None => Ok(entries),
        Some(predicate) => {
            // Create evaluator for the partition filter
            let schema = self.table.metadata().current_schema();
            let partition_spec = self.table.metadata().default_partition_spec();

            let evaluator = PartitionFilterEvaluator::try_new(
                predicate,
                &schema,
                &partition_spec,
                true, // case_sensitive (default)
            )?;

            filter_entries_by_partition(entries, Some(&evaluator))
        }
    }
}
```

**Step 2: Run all planner tests**

Run: `cargo test -p iceberg --lib rewrite_data_files::planner`
Expected: PASS

**Step 3: Commit**

```bash
git add crates/iceberg/src/transaction/rewrite_data_files/planner.rs
git commit -m "feat(rewrite): integrate partition filter into planner"
```

### Task 5: Remove FeatureUnsupported Error for partition_filter

**Files:**
- Modify: `crates/iceberg/src/transaction/rewrite_data_files/mod.rs`
- Modify: `crates/iceberg/src/transaction/rewrite_data_files/planner.rs`

**Step 1: Remove the FeatureUnsupported check in validate()**

In `mod.rs`, remove or comment out:

```rust
// Remove this block from validate():
// if self.partition_filter.is_some() {
//     return Err(Error::new(
//         ErrorKind::FeatureUnsupported,
//         "partition filtering is not yet implemented...",
//     ));
// }
```

**Step 2: Unhide the filter() method**

Remove `#[doc(hidden)]` from the `filter()` method.

**Step 3: Update planner validation**

In `planner.rs`, remove the `FeatureUnsupported` error from `filter_by_partition` (it should now work).

**Step 4: Run all rewrite tests**

Run: `cargo test -p iceberg --lib rewrite_data_files`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/iceberg/src/transaction/rewrite_data_files/mod.rs
git add crates/iceberg/src/transaction/rewrite_data_files/planner.rs
git commit -m "feat(rewrite): enable partition_filter feature"
```

### Task 6: Add Integration Test

**Files:**
- Modify: `crates/iceberg/src/transaction/rewrite_data_files/planner.rs`

**Step 1: Add comprehensive test**

Add test that verifies end-to-end partition filtering (can be an ignored test if full table setup is complex):

```rust
#[test]
fn test_partition_filter_integration() {
    // Document expected behavior for integration tests
    // Full integration test requires table with manifests
    // See crates/iceberg/tests/ for end-to-end testing
}
```

**Step 2: Commit**

```bash
git add crates/iceberg/src/transaction/rewrite_data_files/planner.rs
git commit -m "docs(rewrite): add partition filter integration test placeholder"
```

---

## Phase 2.3: Delete File Threshold

**Complexity:** Medium
**Dependencies:** Requires early delete file loading during candidate selection

### Overview

Allow files to become compaction candidates if they have too many associated delete files, regardless of their size.

### Task 7: Add Delete Count to Candidate Selection

**Files:**
- Modify: `crates/iceberg/src/transaction/rewrite_data_files/planner.rs`
- Modify: `crates/iceberg/src/transaction/rewrite_data_files/options.rs`

**Step 1: Write the failing test**

```rust
#[test]
fn test_is_candidate_with_delete_threshold() {
    let options = RewriteDataFilesOptions {
        delete_file_threshold: Some(3),
        ..Default::default()
    };

    // File is the right size (not a size-based candidate)
    let entry = test_entry("file1.parquet", 400 * 1024 * 1024, 4000);

    // Without delete info, not a candidate
    assert!(!is_candidate_with_deletes(&entry, &options, 0));

    // With 2 deletes (below threshold), not a candidate
    assert!(!is_candidate_with_deletes(&entry, &options, 2));

    // With 3 deletes (at threshold), IS a candidate
    assert!(is_candidate_with_deletes(&entry, &options, 3));

    // With 5 deletes (above threshold), IS a candidate
    assert!(is_candidate_with_deletes(&entry, &options, 5));
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p iceberg --lib rewrite_data_files::planner::tests::test_is_candidate_with_delete_threshold`
Expected: FAIL

**Step 3: Implement delete-aware candidate selection**

This requires restructuring the planner to:
1. Load delete files early (before candidate selection)
2. Build a map of data file path -> delete count
3. Pass delete count to is_candidate check

```rust
/// Check if a file is a candidate based on size OR delete count.
fn is_candidate_with_deletes(
    entry: &ManifestEntryRef,
    options: &RewriteDataFilesOptions,
    delete_count: usize,
) -> bool {
    let size = entry.file_size_in_bytes();

    // Size-based: too small
    if size < options.min_file_size_bytes {
        return true;
    }

    // Size-based: too large
    if size > options.max_file_size_bytes {
        return true;
    }

    // Delete-based: too many associated deletes
    if let Some(threshold) = options.delete_file_threshold {
        if delete_count >= threshold as usize {
            return true;
        }
    }

    false
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p iceberg --lib rewrite_data_files::planner::tests::test_is_candidate_with_delete_threshold`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/iceberg/src/transaction/rewrite_data_files/planner.rs
git commit -m "feat(rewrite): add delete-count-aware candidate selection"
```

### Task 8: Build Delete Index During Planning

**Files:**
- Modify: `crates/iceberg/src/transaction/rewrite_data_files/planner.rs`

**Step 1: Create delete index structure**

```rust
/// Index of delete files by data file path.
///
/// For position deletes, maps referenced data file path -> delete entries.
/// For equality deletes, maps partition -> delete entries (applied to all files in partition).
struct DeleteIndex {
    /// Position deletes by data file path
    position_deletes: HashMap<String, Vec<ManifestEntryRef>>,
    /// Equality deletes by (partition, spec_id)
    equality_deletes: HashMap<(Option<Struct>, i32), Vec<ManifestEntryRef>>,
}

impl DeleteIndex {
    fn new() -> Self {
        Self {
            position_deletes: HashMap::new(),
            equality_deletes: HashMap::new(),
        }
    }

    /// Get count of delete files affecting a data file.
    fn delete_count_for_file(&self, entry: &ManifestEntryRef, is_unpartitioned: bool) -> usize {
        let path = entry.data_file.file_path.as_str();
        let partition = if is_unpartitioned {
            None
        } else {
            Some(entry.data_file.partition().clone())
        };
        let spec_id = entry.data_file.partition_spec_id;

        let pos_count = self.position_deletes.get(path).map_or(0, |v| v.len());
        let eq_count = self.equality_deletes.get(&(partition, spec_id)).map_or(0, |v| v.len());

        pos_count + eq_count
    }
}
```

**Step 2: Integrate into planner**

Move delete loading before candidate selection when `delete_file_threshold` is set.

**Step 3: Run tests**

Run: `cargo test -p iceberg --lib rewrite_data_files`
Expected: PASS

**Step 4: Commit**

```bash
git add crates/iceberg/src/transaction/rewrite_data_files/planner.rs
git commit -m "feat(rewrite): build delete index for threshold-based selection"
```

### Task 9: Enable delete_file_threshold Option

**Files:**
- Modify: `crates/iceberg/src/transaction/rewrite_data_files/mod.rs`

**Step 1: Remove FeatureUnsupported error**

Remove the validation check that errors on `delete_file_threshold.is_some()`.

**Step 2: Unhide the method**

Remove `#[doc(hidden)]` from `delete_file_threshold()`.

**Step 3: Run tests**

Run: `cargo test -p iceberg --lib rewrite_data_files`
Expected: PASS

**Step 4: Commit**

```bash
git add crates/iceberg/src/transaction/rewrite_data_files/mod.rs
git commit -m "feat(rewrite): enable delete_file_threshold feature"
```

---

## Phase 2.4: Remove Dangling Deletes

**Complexity:** High
**Dependencies:** Phase 2.3 (delete index infrastructure)

### Overview

After compaction, delete files that no longer reference any data files (because all referenced data was compacted) should be removed.

### Task 10: Track Delete File References

**Files:**
- Create: `crates/iceberg/src/transaction/rewrite_data_files/delete_tracker.rs`

**Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delete_tracker_identifies_dangling() {
        let mut tracker = DeleteTracker::new();

        // Position delete references file1 and file2
        let pos_delete = test_position_delete_entry("delete1.parquet", vec!["file1.parquet", "file2.parquet"]);
        tracker.add_position_delete(pos_delete.clone());

        // After compacting file1 and file2, the delete is dangling
        let rewritten_files = vec!["file1.parquet".to_string(), "file2.parquet".to_string()];
        let remaining_files: HashSet<String> = HashSet::new(); // No files left

        let dangling = tracker.find_dangling_deletes(&rewritten_files, &remaining_files);
        assert_eq!(dangling.len(), 1);
    }

    #[test]
    fn test_delete_tracker_keeps_active() {
        let mut tracker = DeleteTracker::new();

        // Position delete references file1 and file2
        let pos_delete = test_position_delete_entry("delete1.parquet", vec!["file1.parquet", "file2.parquet"]);
        tracker.add_position_delete(pos_delete.clone());

        // Only file1 was compacted, file2 remains
        let rewritten_files = vec!["file1.parquet".to_string()];
        let remaining_files: HashSet<String> = vec!["file2.parquet".to_string()].into_iter().collect();

        let dangling = tracker.find_dangling_deletes(&rewritten_files, &remaining_files);
        assert_eq!(dangling.len(), 0); // Delete still needed for file2
    }
}
```

**Step 2: Implement DeleteTracker**

```rust
//! Delete file reference tracking for dangling delete cleanup.

use std::collections::{HashMap, HashSet};
use crate::spec::ManifestEntryRef;

/// Tracks which data files each delete file references.
pub(crate) struct DeleteTracker {
    /// Position delete -> referenced data file paths
    position_delete_refs: HashMap<String, HashSet<String>>,
    /// Position delete entries
    position_delete_entries: HashMap<String, ManifestEntryRef>,
    /// Equality delete entries by (partition, spec_id)
    equality_delete_entries: Vec<ManifestEntryRef>,
}

impl DeleteTracker {
    pub fn new() -> Self {
        Self {
            position_delete_refs: HashMap::new(),
            position_delete_entries: HashMap::new(),
            equality_delete_entries: Vec::new(),
        }
    }

    /// Add a position delete and its referenced data files.
    ///
    /// Note: This requires reading the delete file content to extract
    /// the `file_path` column values. For now, this is a placeholder
    /// that uses the delete file's `referenced_data_file` field if available.
    pub fn add_position_delete(&mut self, entry: ManifestEntryRef, referenced_files: HashSet<String>) {
        let path = entry.data_file.file_path.clone();
        self.position_delete_refs.insert(path.clone(), referenced_files);
        self.position_delete_entries.insert(path, entry);
    }

    /// Add an equality delete (applies to entire partition).
    pub fn add_equality_delete(&mut self, entry: ManifestEntryRef) {
        self.equality_delete_entries.push(entry);
    }

    /// Find position deletes that no longer reference any remaining data files.
    pub fn find_dangling_position_deletes(
        &self,
        rewritten_files: &[String],
        remaining_files: &HashSet<String>,
    ) -> Vec<ManifestEntryRef> {
        let rewritten_set: HashSet<&str> = rewritten_files.iter().map(|s| s.as_str()).collect();

        let mut dangling = Vec::new();

        for (delete_path, refs) in &self.position_delete_refs {
            // Check if ALL referenced files are either:
            // 1. In the rewritten set (being compacted away), OR
            // 2. Not in remaining files (already gone)
            let still_needed = refs.iter().any(|ref_path| {
                !rewritten_set.contains(ref_path.as_str()) && remaining_files.contains(ref_path)
            });

            if !still_needed {
                if let Some(entry) = self.position_delete_entries.get(delete_path) {
                    dangling.push(entry.clone());
                }
            }
        }

        dangling
    }
}
```

**Step 3: Run tests**

Run: `cargo test -p iceberg --lib rewrite_data_files::delete_tracker`
Expected: PASS

**Step 4: Commit**

```bash
git add crates/iceberg/src/transaction/rewrite_data_files/delete_tracker.rs
git commit -m "feat(rewrite): add DeleteTracker for dangling delete identification"
```

### Task 11: Integrate Delete Removal into Committer

**Files:**
- Modify: `crates/iceberg/src/transaction/rewrite_data_files/committer.rs`

**Step 1: Add delete files to removal set**

Extend `ReplaceDataFilesOperation` to also track delete files to remove:

```rust
struct ReplaceDataFilesOperation {
    files_to_delete: Vec<ManifestEntry>,
    delete_files_to_remove: Vec<ManifestEntry>,
    starting_sequence_number: i64,
}
```

**Step 2: Update deleted_entries to include delete files**

**Step 3: Run tests**

Run: `cargo test -p iceberg --lib rewrite_data_files::committer`
Expected: PASS

**Step 4: Commit**

```bash
git add crates/iceberg/src/transaction/rewrite_data_files/committer.rs
git commit -m "feat(rewrite): support removing dangling delete files in commit"
```

### Task 12: Enable remove_dangling_deletes Option

**Files:**
- Modify: `crates/iceberg/src/transaction/rewrite_data_files/mod.rs`

**Step 1: Remove FeatureUnsupported error**

**Step 2: Unhide the method**

**Step 3: Run tests and commit**

---

## Phase 2.5: Partial Progress Commits

**Complexity:** High
**Dependencies:** None (restructures commit model)

### Overview

Enable committing file groups in batches rather than all-or-nothing. This allows progress to be preserved even if some groups fail.

### Task 13: Add Batch Commit Infrastructure

**Files:**
- Modify: `crates/iceberg/src/transaction/rewrite_data_files/committer.rs`

**Step 1: Add batch commit method**

```rust
/// Commit a batch of file group results.
///
/// Used when `partial_progress_enabled` is true to commit groups incrementally.
pub async fn commit_batch(
    &self,
    results: Vec<FileGroupRewriteResult>,
    batch_index: u32,
) -> Result<(ActionCommit, RewriteDataFilesResult)> {
    // Similar to commit() but for a subset of groups
    // Returns ActionCommit that caller should apply to table
    // Caller is responsible for refreshing table between batches
}
```

**Step 2: Add batch planning**

```rust
/// Divide file groups into commit batches.
fn plan_batches(
    groups: &[FileGroup],
    max_commits: u32,
) -> Vec<Vec<u32>> {
    // Divide group IDs into roughly equal batches
    let groups_per_batch = (groups.len() as f64 / max_commits as f64).ceil() as usize;
    groups.chunks(groups_per_batch.max(1))
        .map(|chunk| chunk.iter().map(|g| g.group_id).collect())
        .collect()
}
```

**Step 3: Run tests and commit**

### Task 14: Update Result Tracking for Partial Progress

**Files:**
- Modify: `crates/iceberg/src/transaction/rewrite_data_files/result.rs`
- Modify: `crates/iceberg/src/transaction/rewrite_data_files/committer.rs`

**Step 1: Track per-batch results**

Update `RewriteDataFilesResult` to properly track `commits_count` and failed groups.

**Step 2: Run tests and commit**

### Task 15: Enable partial_progress Options

**Files:**
- Modify: `crates/iceberg/src/transaction/rewrite_data_files/mod.rs`

**Step 1: Remove FeatureUnsupported checks**

**Step 2: Add documentation for partial progress workflow**

**Step 3: Run all tests**

Run: `cargo test -p iceberg --lib rewrite_data_files`
Expected: PASS

**Step 4: Final commit**

```bash
git add crates/iceberg/src/transaction/rewrite_data_files/
git commit -m "feat(rewrite): enable partial progress commits"
```

---

## Verification Checklist

After completing all phases:

- [ ] All existing tests pass: `cargo test -p iceberg --lib rewrite_data_files`
- [ ] No new clippy warnings: `cargo clippy -p iceberg`
- [ ] Documentation builds: `cargo doc -p iceberg`
- [ ] Hidden `#[doc(hidden)]` attributes removed from implemented features
- [ ] FeatureUnsupported errors removed for implemented features

---

## Test Commands Summary

```bash
# Run all rewrite_data_files tests
cargo test -p iceberg --lib rewrite_data_files

# Run specific module tests
cargo test -p iceberg --lib rewrite_data_files::planner
cargo test -p iceberg --lib rewrite_data_files::committer
cargo test -p iceberg --lib rewrite_data_files::partition_filter
cargo test -p iceberg --lib rewrite_data_files::delete_tracker

# Run with output
cargo test -p iceberg --lib rewrite_data_files -- --nocapture

# Check for warnings
cargo clippy -p iceberg -- -D warnings
```
