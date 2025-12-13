# UPDATE Operation Changelog

All notable changes to the UPDATE operation implementation.

## [P1/P2 Quality Improvements] - 2024-12

### Added

- **Error handling tests** for invalid operations:
  - `test_update_invalid_column` - verifies proper error on non-existent column
  - `test_delete_invalid_predicate` - verifies proper error on invalid predicate
  - `test_update_invalid_syntax` - verifies proper error on SQL syntax errors
  - `test_update_failure_no_side_effects` - verifies data unchanged after failed UPDATE
  - `test_select_after_concurrent_operations` - verifies reads after writes work correctly
- **Extended benchmark suite** with new benchmark groups:
  - `bench_delete_operations` - DELETE performance at different selectivities
  - `bench_update_vs_delete_insert` - compares native UPDATE vs DELETE+INSERT pattern
  - Throughput metrics (`Throughput::Elements`) for all benchmarks

### Changed

- Module documentation updated to reflect SQL DELETE support
- Removed outdated "Future Work" section from delete.rs

### Deprecated

- `parse_predicate()` function deprecated with `#[deprecated]` attribute
  - Guidance to use `col()` and `lit()` functions instead

---

## [Epic 2: SQL Interface] - 2024-12

### Added

- **SQL UPDATE/DELETE support** via new DataFusion TableProvider trait methods:
  - `dml_capabilities()` - returns `DmlCapabilities::ALL` (UPDATE + DELETE)
  - `delete_from()` - handles `DELETE FROM table WHERE ...` SQL statements
  - `update()` - handles `UPDATE table SET col = val WHERE ...` SQL statements
- Integration with forked DataFusion v51.0.0 (`https://github.com/ethan-tyler/datafusion`)
- SQL integration tests:
  - `test_sql_update_with_predicate`
  - `test_sql_update_full_table`
  - `test_sql_delete_with_predicate`
  - `test_sql_delete_full_table`

### Changed

- Updated to DataFusion 51.0.0 with Arrow 57.0 and Parquet 57.0
- Merged df51 branch (PR #1899) for Arrow/Parquet compatibility

### Technical Details

- Uses `[patch.crates-io]` to override DataFusion crates with forked version
- SQL statements now route through TableProvider DML methods
- Reuses existing execution plan chains (IcebergDeleteScanExec, IcebergUpdateExec, etc.)
- Returns execution plans that DataFusion executes; output is row count (UInt64)

### Resolved

- DataFusion #12406 blocker resolved via fork with DML support

---

## [Sprint 3] - 2024-12

### Added

- Position delete writing integrated into `update.rs` and `delete_write.rs`
- Partition guards for both UPDATE and DELETE with clear `NotImplemented` error messages
- Integration tests:
  - `test_update_partitioned_table` - verifies UPDATE rejects partitioned tables
  - `test_delete_partitioned_table` - verifies DELETE rejects partitioned tables
- Criterion.rs benchmark suite (`benches/update_benchmark.rs`):
  - Selectivity benchmarks (1%, 10%, 50%, 100%)
  - Row count benchmarks (100, 500, 1000 rows)
- Documentation folder with design docs, ADRs, and sprint summary

### Changed

- Position delete files now use `task.partition_spec` instead of default spec
- Error messages include table name and baseline snapshot ID

### Fixed

- Position deletes now correctly reference source file's partition spec

### Known Limitations

- **Partitioned tables**: Both UPDATE and DELETE return `NotImplemented` error
- **UPDATE atomicity**: Uses two-phase commit; if crash occurs between delete file commit and data file commit, table may be inconsistent. Recovery possible via time travel to previous snapshot.
- **V1 tables**: Position deletes require Iceberg format version 2

### Security

- Guard prevents data corruption from partition evolution mismatch

---

## [Sprint 2] - 2024

### Added
- `PositionDeleteFileWriterBuilder` integration
- Cross-column swap handling (evaluate against original batch)
- Integration tests:
  - `test_update_cross_column_swap`
  - `test_update_self_reference`
  - `test_update_with_expression`
  - `test_select_after_update`

### Fixed
- Position delete schema construction
- JSON serialization for file metadata transfer

---

## [Sprint 1] - 2024

### Added
- `IcebergUpdateExec` - Core execution plan for UPDATE operations
- `IcebergUpdateCommitExec` - Atomic commit handling
- `UpdateBuilder` - Fluent API for programmatic UPDATE
- `RowDeltaAction` - Transaction action for row-level operations
- Basic integration tests:
  - `test_update_empty_table`
  - `test_update_no_match`
  - `test_update_with_predicate`
  - `test_update_full_table`
  - `test_update_multiple_columns`

### Architecture
- Copy-on-Write semantics for UPDATE
- Atomic commit via RowDelta transaction
- Iceberg predicate pushdown for file pruning
