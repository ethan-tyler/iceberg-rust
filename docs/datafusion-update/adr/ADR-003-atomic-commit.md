# ADR-003: Atomic Commit via RowDeltaAction

## Status

**Accepted** - Implemented in Sprint 1-2

## Context

UPDATE operations using Copy-on-Write produce two types of files:

1. **Data files**: Contain the updated rows
2. **Position delete files**: Mark original rows as deleted

These files MUST be committed atomically. If only data files commit:
- Original rows remain visible
- Updated rows also visible
- **Result**: Duplicate data

If only delete files commit:
- Original rows are deleted
- Updated rows not visible
- **Result**: Data loss

We needed a transaction mechanism to ensure both file types commit together or not at all.

## Decision

**Use `RowDeltaAction` for atomic commits combining data files and position delete files in a single snapshot.**

```rust
let mut action = tx.row_delta();

// Add all data files
for data_file in data_files {
    action = action.add_data_file(data_file);
}

// Add all position delete files
for delete_file in delete_files {
    action = action.add_position_delete_file(delete_file);
}

// Atomic commit with conflict detection
let tx = action.apply(tx)?;
let _updated_table = tx.commit(catalog.as_ref()).await?;
```

### Rationale

1. **Iceberg Spec Compliance**: RowDelta is the standard action for row-level changes
2. **Existing Implementation**: `RowDeltaAction` was already available in iceberg-rust
3. **Conflict Detection**: Built-in support for baseline snapshot validation
4. **Single Snapshot**: Both file types recorded in one snapshot

## Alternatives Considered

### Option A: Separate FastAppend + Delete Actions

**Approach**: Two sequential transactions

**Pros**:
- Simpler individual transactions

**Cons**:
- Not atomic - gap between commits allows inconsistent reads
- No rollback if second transaction fails
- Violates Iceberg semantics

### Option B: Custom Transaction Action

**Approach**: Build a new action type for UPDATE

**Pros**:
- Could be optimized for UPDATE use case

**Cons**:
- Duplicates existing RowDelta functionality
- More code to maintain
- Non-standard approach

### Option C: Table-Level Locking

**Approach**: Lock table, perform both commits, unlock

**Pros**:
- Prevents concurrent modifications

**Cons**:
- Doesn't exist in Iceberg model
- Catalog-specific implementation
- Serializes all writes

## Consequences

### Positive
- Atomic semantics guaranteed
- Conflict detection via baseline snapshot
- Standard Iceberg approach
- No orphan files on failure

### Negative
- Must serialize all file metadata before commit
- JSON serialization overhead for large updates
- Single commit point (no streaming commits)

### Implementation Details

The commit flow in `IcebergUpdateCommitExec`:

```rust
// 1. Get baseline snapshot for conflict detection
let baseline_snapshot_id = table
    .metadata()
    .current_snapshot()
    .map(|s| s.snapshot_id());

// 2. Build transaction with RowDelta action
let tx = Transaction::new(&table);
let mut action = tx.row_delta();

// 3. Add files with baseline reference
for data_file in data_files {
    action = action.add_data_file(data_file);
}
for delete_file in delete_files {
    action = action.add_position_delete_file(delete_file);
}

// 4. Commit atomically
let tx = action.apply(tx)?;
let _updated_table = tx.commit(catalog.as_ref()).await?;
```

### Error Context

Enhanced error messages include:
- Table identifier
- Baseline snapshot ID
- Specific failure reason

```rust
Err(DataFusionError::Execution(format!(
    "UPDATE commit failed for table '{}' (baseline snapshot: {:?}): {}",
    table_ident, baseline_snapshot_id, e
)))
```

## References

- [Iceberg RowDelta](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/RowDelta.html)
- Implementation: `IcebergUpdateCommitExec` in `physical_plan/update_commit.rs`
- `RowDeltaAction` in `transaction/row_delta.rs`
