# PR: rewrite_data_files Phases 2.3-2.5 Implementation

## Summary

Completes the `rewrite_data_files` compaction feature with delete-aware candidate selection, dangling delete cleanup, and partial progress commit support.

## Changes

### Phase 2.3: Delete File Threshold

Adds delete-count-based candidate selection for compaction. Files with too many associated delete files become compaction candidates even if their size is within the target range.

**Key components:**
- `DeleteIndex` - Indexes delete files by referenced data file path and partition
- `delete_file_threshold` option - Configures the threshold for delete-based selection
- Integration with existing size-based candidate selection

### Phase 2.4: Remove Dangling Deletes

Tracks and removes position delete files that no longer reference any remaining data files after compaction.

**Key components:**
- `DeleteTracker` - Tracks delete file references to data files
- `find_dangling_position_deletes()` - Identifies file-specific position deletes whose targets were compacted away
- Dangling deletes removed in the same commit as data file replacement

**Scope:**
- Only file-specific position deletes (with `referenced_data_file` set) are removed
- Partition-scoped position deletes and equality deletes are NOT removed (conservative approach)

### Phase 2.5: Partial Progress Commits

Enables incremental commits for large compaction jobs, allowing progress to be preserved if the job is interrupted.

**Key components:**
- `plan_batches()` - Divides file groups into batches based on `max_commits`
- `commit_batch()` - Commits a subset of file groups
- `partial_progress_enabled` / `max_commits` options

### Bug Fix: Partition Spec Evolution

Fixed a bug where `DeleteIndex`, `DeleteTracker`, and `group_by_partition` incorrectly derived `is_unpartitioned` from the table's default partition spec instead of each file's `partition_spec_id`.

**Impact:**
- On tables with evolved partition specs, files were incorrectly keyed
- Could cause incorrect delete counts and missed/over-removed dangling deletes

**Fix:**
- All components now derive `is_unpartitioned` per-entry from `partition_spec_id`
- Unknown spec IDs fail fast with clear error messages
- Prevents silent corruption from going unnoticed

## Files Changed

### New Files
- `crates/iceberg/src/transaction/rewrite_data_files/delete_tracker.rs`

### Modified Files
- `crates/iceberg/src/transaction/rewrite_data_files/planner.rs`
  - Added `DeleteIndex` for threshold-based selection
  - Added `build_delete_tracker()` for dangling delete tracking
  - Fixed partition spec evolution handling
- `crates/iceberg/src/transaction/rewrite_data_files/committer.rs`
  - Added batch commit infrastructure
  - Added `find_dangling_deletes()` integration
- `crates/iceberg/src/transaction/rewrite_data_files/mod.rs`
  - Enabled `remove_dangling_deletes` option
  - Updated `partial_progress_enabled` documentation

## Test Results

```
test result: ok. 83 passed; 0 failed; 0 ignored
```

### New Tests
- `test_delete_index_*` - DeleteIndex functionality (4 tests)
- `test_delete_tracker_*` - DeleteTracker functionality (9 tests)
- `test_plan_batches_*` - Batch planning (3 tests)
- `test_*_rejects_unknown_spec_id` - Spec ID validation (3 tests)

## API Changes

### New Options
```rust
RewriteDataFilesOptions {
    // Phase 2.3
    delete_file_threshold: Option<u32>,  // Files with >= this many deletes become candidates

    // Phase 2.4
    remove_dangling_deletes: bool,       // Clean up orphaned position deletes

    // Phase 2.5
    partial_progress_enabled: bool,      // Enable incremental commits
    max_commits: u32,                    // Max number of commits (batches)
}
```

## Known Limitations

1. **Partition-scoped deletes not removed** - Only file-specific position deletes (with `referenced_data_file`) are cleaned up. Partition-scoped position deletes and equality deletes require more complex tracking.

2. **Partial progress requires caller orchestration** - The `commit_batch()` API provides the building blocks, but callers must implement the execution loop and error handling.

## Checklist

- [x] All tests pass
- [x] No new warnings
- [x] Spec evolution edge case fixed
- [x] Unknown spec IDs fail fast with clear errors
- [x] Documentation updated

---

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
