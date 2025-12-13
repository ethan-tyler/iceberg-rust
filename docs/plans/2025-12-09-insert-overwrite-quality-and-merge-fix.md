# INSERT OVERWRITE Quality Review & MERGE Test Fix Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Verify Phase 1.6 (INSERT OVERWRITE) meets production quality standards and fix pre-existing MERGE test failures.

**Architecture:** INSERT OVERWRITE uses `ReplacePartitionsAction` for dynamic partition replacement via DataFusion SQL. The MERGE tests need updates to reflect the 7-column output schema (added DPP tracking columns).

**Tech Stack:** Rust, Apache Iceberg, DataFusion, Arrow

---

## Part 1: INSERT OVERWRITE Quality Verification

### Task 1: Verify Test Coverage Completeness

**Files:**
- Review: `crates/integrations/datafusion/tests/integration_datafusion_test.rs`
- Review: `crates/iceberg/src/transaction/replace_partitions.rs`

**Step 1: Run all INSERT OVERWRITE related tests**

Run: `cargo test -p iceberg-datafusion -- test_insert 2>&1`

Expected output:
```
test test_insert_into ... ok
test test_insert_into_partitioned ... ok
test test_insert_into_nested ... ok
test test_insert_overwrite_unpartitioned ... ok
test test_insert_overwrite_partitioned ... ok
test result: ok. 5 passed
```

**Step 2: Run all replace_partitions core tests**

Run: `cargo test -p iceberg -- replace_partitions 2>&1`

Expected output:
```
test result: ok. 14 passed; 0 failed
```

**Step 3: Run all overwrite action tests**

Run: `cargo test -p iceberg overwrite 2>&1`

Expected output:
```
test result: ok. 11 passed; 0 failed
```

**Verification:** All INSERT OVERWRITE tests pass. Phase 1.6 test coverage is complete.

---

### Task 2: Verify No Debug Output Remains

**Files:**
- Check: `crates/iceberg/src/transaction/replace_partitions.rs`
- Check: `crates/integrations/datafusion/src/physical_plan/overwrite_commit.rs`
- Check: `crates/integrations/datafusion/src/table/mod.rs`

**Step 1: Search for debug output**

Run: `grep -r "eprintln!" crates/iceberg/src/transaction/replace_partitions.rs crates/integrations/datafusion/src/physical_plan/overwrite_commit.rs crates/integrations/datafusion/src/table/mod.rs`

Expected: No output (no eprintln! statements)

**Step 2: Search for println! statements**

Run: `grep -r "println!" crates/iceberg/src/transaction/replace_partitions.rs crates/integrations/datafusion/src/physical_plan/overwrite_commit.rs crates/integrations/datafusion/src/table/mod.rs`

Expected: No output (no println! statements)

**Verification:** All debug output has been removed. Code is production-ready.

---

### Task 3: Verify Clippy Compliance

**Files:**
- All modified files

**Step 1: Run clippy on iceberg crate**

Run: `cargo clippy -p iceberg -- -D warnings 2>&1 | tail -20`

Expected: No warnings or errors

**Step 2: Run clippy on iceberg-datafusion crate**

Run: `cargo clippy -p iceberg-datafusion -- -D warnings 2>&1 | tail -20`

Expected: No warnings or errors

**Verification:** Code follows Rust best practices.

---

### Task 4: Verify Documentation Quality

**Files:**
- Review: `crates/iceberg/src/transaction/replace_partitions.rs` (module docs)
- Review: `crates/iceberg/src/transaction/overwrite.rs` (module docs)
- Review: `crates/iceberg/src/transaction/mod.rs` (API docs)

**Step 1: Check module documentation exists**

Run: `head -100 crates/iceberg/src/transaction/replace_partitions.rs | grep -E "^//!"`

Expected: Module-level documentation present

**Step 2: Check public API documentation**

Run: `cargo doc -p iceberg --no-deps 2>&1 | grep -E "(warning|error).*missing" | head -10`

Expected: No missing documentation warnings for public items

**Verification:** Documentation is comprehensive and follows Rust documentation standards.

---

## Part 2: Fix MERGE Test Failures

### Task 5: Update test_output_schema Test

**Files:**
- Modify: `crates/integrations/datafusion/src/physical_plan/merge.rs:3121-3134`

**Step 1: Read current failing test**

The test at line 3121 expects 5 columns but `make_output_schema()` returns 7 columns.

**Step 2: Update the test to expect 7 columns**

Replace lines 3121-3134 with:

```rust
    #[test]
    fn test_output_schema() {
        let schema = IcebergMergeExec::make_output_schema();
        assert_eq!(schema.fields().len(), 7);
        assert_eq!(schema.field(0).name(), MERGE_DATA_FILES_COL);
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(1).name(), MERGE_DELETE_FILES_COL);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(2).name(), MERGE_INSERTED_COUNT_COL);
        assert_eq!(schema.field(2).data_type(), &DataType::UInt64);
        assert_eq!(schema.field(3).name(), MERGE_UPDATED_COUNT_COL);
        assert_eq!(schema.field(3).data_type(), &DataType::UInt64);
        assert_eq!(schema.field(4).name(), MERGE_DELETED_COUNT_COL);
        assert_eq!(schema.field(4).data_type(), &DataType::UInt64);
        assert_eq!(schema.field(5).name(), MERGE_DPP_APPLIED_COL);
        assert_eq!(schema.field(5).data_type(), &DataType::Boolean);
        assert_eq!(schema.field(6).name(), MERGE_DPP_PARTITION_COUNT_COL);
        assert_eq!(schema.field(6).data_type(), &DataType::UInt64);
    }
```

**Step 3: Run test to verify it passes**

Run: `cargo test -p iceberg-datafusion -- physical_plan::merge::tests::test_output_schema 2>&1`

Expected: `test result: ok. 1 passed`

---

### Task 6: Update test_make_merge_result_batch Test

**Files:**
- Modify: `crates/integrations/datafusion/src/physical_plan/merge.rs:3356-3398`

**Step 1: Update the test to expect 7 columns**

Replace lines 3356-3398 with:

```rust
    #[test]
    fn test_make_merge_result_batch() {
        let stats = MergeStats {
            rows_inserted: 10,
            rows_updated: 5,
            rows_deleted: 3,
            rows_copied: 0,
            files_added: 2,
            files_removed: 1,
            dpp_applied: true,
            dpp_partition_count: 3,
        };

        let result = IcebergMergeExec::make_result_batch(
            vec!["data1.parquet".to_string(), "data2.parquet".to_string()],
            vec!["delete1.parquet".to_string()],
            &stats,
        )
        .unwrap();

        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.num_columns(), 7);

        let inserted = result
            .column(2)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .unwrap();
        assert_eq!(inserted.value(0), 10);

        let updated = result
            .column(3)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .unwrap();
        assert_eq!(updated.value(0), 5);

        let deleted = result
            .column(4)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .unwrap();
        assert_eq!(deleted.value(0), 3);

        let dpp_applied = result
            .column(5)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BooleanArray>()
            .unwrap();
        assert_eq!(dpp_applied.value(0), true);

        let dpp_partition_count = result
            .column(6)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .unwrap();
        assert_eq!(dpp_partition_count.value(0), 3);
    }
```

**Step 2: Run test to verify it passes**

Run: `cargo test -p iceberg-datafusion -- physical_plan::merge::tests::test_make_merge_result_batch 2>&1`

Expected: `test result: ok. 1 passed`

---

### Task 7: Verify All MERGE Tests Pass

**Files:**
- Test: `crates/integrations/datafusion/src/physical_plan/merge.rs`

**Step 1: Run all MERGE tests**

Run: `cargo test -p iceberg-datafusion -- physical_plan::merge::tests 2>&1`

Expected: All tests pass

**Step 2: Run full DataFusion test suite**

Run: `cargo test -p iceberg-datafusion 2>&1 | tail -20`

Expected: `test result: ok. 91 passed; 0 failed`

---

### Task 8: Final Commit

**Step 1: Stage changes**

Run: `git add crates/integrations/datafusion/src/physical_plan/merge.rs`

**Step 2: Create commit**

```bash
git commit -m "$(cat <<'EOF'
fix(datafusion): Update MERGE tests for DPP schema columns

The MERGE output schema was expanded from 5 to 7 columns to include
Dynamic Partition Pruning (DPP) tracking fields:
- dpp_applied (Boolean): Whether DPP was used
- dpp_partition_count (UInt64): Number of partitions pruned

Update tests to reflect the current schema:
- test_output_schema: Assert 7 fields instead of 5
- test_make_merge_result_batch: Assert 7 columns and verify DPP values

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

**Step 3: Verify commit**

Run: `git log -1 --oneline`

Expected: Commit with fix message

---

## Quality Checklist Summary

### Phase 1.6 (INSERT OVERWRITE) Status: ✅ COMPLETE

| Criterion | Status | Evidence |
|-----------|--------|----------|
| Unit tests pass | ✅ | 14 replace_partitions tests pass |
| Integration tests pass | ✅ | 5 DataFusion INSERT tests pass |
| No debug output | ✅ | No eprintln/println in code |
| Documentation | ✅ | Module docs present |
| Clippy clean | ✅ | No warnings |
| Best practices | ✅ | TDD, DRY, proper error handling |

### MERGE Test Fix Status: 🔧 TO BE FIXED

| Test | Issue | Fix |
|------|-------|-----|
| test_output_schema | Expects 5 columns, gets 7 | Update assertion to 7 |
| test_make_merge_result_batch | Expects 5 columns, gets 7 | Update assertion to 7, add DPP field checks |

---

## Execution Notes

- The MERGE test failures are **pre-existing** and unrelated to INSERT OVERWRITE
- The fix is a simple test update to match the current schema
- No production code changes are needed for the MERGE fix
