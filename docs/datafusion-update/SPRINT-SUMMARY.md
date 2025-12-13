# UPDATE Operation Sprint Summary

## Overview

| Sprint | Focus | Status | Dates |
|--------|-------|--------|-------|
| Sprint 1 | Core Infrastructure | Complete | - |
| Sprint 2 | Integration & Testing | Complete | - |
| Sprint 3 | Partitioned Tables & Hardening | Partial | Dec 2024 |
| Epic 2 | SQL Interface | Complete | Dec 2024 |

---

## Known Limitations

| Limitation | Severity | Details |
|------------|----------|---------|
| Partitioned tables | High | Both UPDATE and DELETE return `NotImplemented` error |
| V1 tables | High | Position deletes require format version 2 |

## Concurrency Safety

Both UPDATE and DELETE operations now implement **baseline snapshot validation**:

1. **Plan Construction**: When building the execution plan, the current snapshot ID is captured
2. **Commit Validation**: Before committing, the table is refreshed from the catalog and the current snapshot ID is compared against the baseline
3. **Conflict Detection**: If another transaction committed between scan and commit, the operation fails with a clear error message directing the user to retry

This prevents applying stale position deletes to tables that have been reorganized by concurrent transactions.

---

## Sprint 1: Core Infrastructure

### Objective
Establish the foundational components for UPDATE operations using Copy-on-Write semantics.

### Deliverables

| Component | File | Description |
|-----------|------|-------------|
| `IcebergUpdateExec` | `physical_plan/update.rs` | Core execution plan: scan, filter, transform, write |
| `IcebergUpdateCommitExec` | `physical_plan/update_commit.rs` | Atomic commit handling via RowDelta |
| `UpdateBuilder` | `update.rs` | Fluent API for programmatic UPDATE |
| `RowDeltaAction` | `transaction/row_delta.rs` | Transaction action for row-level operations |

### Key Decisions
- **ADR-001**: Copy-on-Write semantics chosen over Merge-on-Read
- **ADR-003**: Atomic commit via RowDeltaAction

### Architecture Established

```
UpdateBuilder.execute()
    └─► IcebergUpdateExec (scan + transform + write)
        └─► CoalescePartitionsExec
            └─► IcebergUpdateCommitExec (atomic commit)
```

---

## Sprint 2: Integration & Testing

### Objective
Validate the implementation with comprehensive tests and fix edge cases.

### Deliverables

| Component | Description |
|-----------|-------------|
| Position Delete Writer | `PositionDeleteFileWriterBuilder` for delete file generation |
| Basic Tests | Empty table, no match, predicate matching |
| Expression Tests | Self-reference, cross-column swap, computed values |
| Integration Tests | Select-after-update verification |

### Test Coverage

| Category | Tests | Status |
|----------|-------|--------|
| Basic UPDATE | 6 | Pass |
| Expression Handling | 3 | Pass |
| Data Integrity | 2 | Pass |

### Key Fixes
- Cross-column swap handling (evaluate against original batch)
- Position delete schema construction
- JSON serialization for file metadata

---

## Sprint 3: Partitioned Tables & Hardening

### Objective
Enable UPDATE on partitioned tables and improve production readiness.

### Status: PARTIAL

**Completed:**
- Position delete writer integrated into `update.rs` and `delete_write.rs`
- Both UPDATE and DELETE reject partitioned tables with clear error messages

**Not Completed:**
- Partitioned table support for either UPDATE or DELETE
- Dedicated `PositionDeleteTaskWriter` file (integrated inline instead)

### Deliverables

| Component | File | Description | Status |
|-----------|------|-------------|--------|
| Position Delete Writing | `physical_plan/update.rs`, `delete_write.rs` | Inline position delete file generation | ✅ |
| Partition Guard (UPDATE) | `update.rs` | Explicit NotImplemented for partitioned tables | ✅ |
| Partition Guard (DELETE) | `delete_write.rs` | Explicit NotImplemented for partitioned tables | ✅ |
| Error Context | `physical_plan/update_commit.rs` | Table name, snapshot ID in errors | ✅ |

### Existing Tests

| Test | Description | Status |
|------|-------------|--------|
| `test_update_partitioned_table` | Verifies UPDATE returns NotImplemented for partitioned tables | ✅ |
| `test_delete_partitioned_table` | Verifies DELETE returns NotImplemented for partitioned tables | ✅ |

### Benchmark Suite

Criterion.rs benchmarks added in `benches/update_benchmark.rs`:

- **Selectivity benchmarks**: 1%, 10%, 50%, 100% of rows
- **Row count benchmarks**: 100, 500, 1000 row tables

Run with: `cargo bench --bench update_benchmark -p iceberg-datafusion`

### Key Decisions
- **ADR-002**: Guard partition evolution rather than silent corruption

### Issues Addressed

| Issue | Severity | Resolution |
|-------|----------|------------|
| Position deletes used default spec | High | Use `task.partition_spec` |
| Serialization partition type mismatch | High | Guard for evolved specs |
| Error messages lacked context | Medium | Include table name, snapshot ID |
| Stale documentation | Low | Updated docs to reflect partitioned support |

### Documentation Updates
- Module docs clarify partition evolution limitation
- API docs updated for partitioned table support
- ADRs created for key decisions

### Deferred Items

| Item | Reason | Target |
|------|--------|--------|
| SQL Interface | Blocked by DataFusion #12406 | Sprint 4 |

### Final Test Results

```
test result: ok. 29 passed; 0 failed; 0 ignored
```

---

## Epic 2: SQL Interface

### Status: COMPLETE

Resolved via forked DataFusion with DML support at `https://github.com/ethan-tyler/datafusion`.

The fork adds `delete_from()` and `update()` methods to the `TableProvider` trait.

### Implementation

| Component | Description | Status |
|-----------|-------------|--------|
| `dml_capabilities()` | Returns `DmlCapabilities::ALL` | ✅ |
| `delete_from()` | Handles `DELETE FROM table WHERE ...` | ✅ |
| `update()` | Handles `UPDATE table SET col = val WHERE ...` | ✅ |

### SQL Usage

```sql
-- SQL DELETE
DELETE FROM catalog.namespace.table WHERE id = 42;

-- SQL UPDATE
UPDATE catalog.namespace.table SET status = 'shipped' WHERE id = 42;
```

### Tests Added

| Test | Description |
|------|-------------|
| `test_sql_update_with_predicate` | SQL UPDATE with WHERE clause |
| `test_sql_update_full_table` | SQL UPDATE all rows |
| `test_sql_delete_with_predicate` | SQL DELETE with WHERE clause |
| `test_sql_delete_full_table` | SQL DELETE all rows |

### Technical Notes

- Uses `[patch.crates-io]` in workspace `Cargo.toml` to override DataFusion
- Fresh schema loaded from `table.metadata().current_schema()` for evolution safety
- Reuses existing execution plan chains (IcebergUpdateExec, IcebergDeleteScanExec, etc.)
- Returns execution plans that DataFusion executes; output is row count (UInt64)

### Dependency

Requires forked DataFusion until upstream merges DML support:

```toml
[patch.crates-io]
datafusion = { git = "https://github.com/ethan-tyler/datafusion", branch = "main" }
# ... other datafusion-* crates
```

---

## Metrics Summary

### Code Changes

| Metric | Sprint 1-2 | Sprint 3 | Epic 2 | P1/P2 | Total |
|--------|------------|----------|--------|-------|-------|
| New Files | 5 | 1 | 0 | 0 | 6 |
| Modified Files | 4 | 3 | 4 | 4 | 15 |
| Lines Added | ~1500 | ~400 | ~200 | ~250 | ~2350 |
| Tests Added | 10 | 2 | 4 | 5 | 21 |
| Benchmarks | 0 | 2 | 0 | 2 | 4 |

### Test Coverage

| Category | Count |
|----------|-------|
| Unit Tests | 3 |
| Integration Tests (UPDATE) | 14 |
| Integration Tests (DELETE) | 8 |
| Error Handling Tests | 5 |
| SQL Interface Tests | 4 |
| Benchmark Groups | 4 |
| **Total** | **38** |

---

## Lessons Learned

### What Worked Well
1. **Incremental delivery**: Core → Tests → Partitions progression
2. **ADR documentation**: Captured decisions early
3. **Guard over corruption**: Explicit errors for unsupported cases
4. **Existing infrastructure**: Leveraged PositionDeleteFileWriterBuilder, RowDeltaAction

### What Could Improve
1. **Partition evolution**: Needs iceberg crate API exposure
2. **Test infrastructure**: MemoryCatalog limits evolution testing
3. **Documentation timing**: Should be written alongside code, not after

### Technical Debt

| Item | Priority | Notes |
|------|----------|-------|
| Partitioned table support | High | Both UPDATE and DELETE return NotImplemented |
| UPDATE atomicity | Done | Now uses RowDelta for atomic single-snapshot commits |
| Partition evolution support | Medium | Requires `DataFile.partition_spec_id` exposure |
| DataFusion fork dependency | Medium | Remove patch when upstream adds DML support |
| MoR support | Low | Requires reader changes |
| Performance benchmarks | Done | Criterion.rs suite added in Sprint 3 |

---

## References

- [Design Document](./DESIGN.md)
- [ADR-001: Copy-on-Write](./adr/ADR-001-cow-semantics.md)
- [ADR-002: Partition Evolution](./adr/ADR-002-partition-evolution.md)
- [ADR-003: Atomic Commit](./adr/ADR-003-atomic-commit.md)
