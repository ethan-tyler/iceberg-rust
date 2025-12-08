# UPDATE Operation Technical Design

## Summary

Implementation of row-level UPDATE operations for Iceberg tables via DataFusion integration using Copy-on-Write (CoW) semantics.

## Problem Statement

### Before
- Iceberg-rust supported INSERT and table-level DELETE operations
- No support for row-level modifications (UPDATE)
- Users had to implement workarounds: full table rewrites or external processing

### Pain Points Solved
- Enables standard SQL UPDATE semantics on Iceberg tables
- Atomic updates with conflict detection
- Proper position delete file generation per Iceberg spec
- Partition-aware updates with correct file placement

## Solution Overview

### Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        UpdateBuilder API                             │
│  provider.update().set("col", expr).filter(pred).execute()          │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       IcebergUpdateExec                              │
│  ┌──────────┐  ┌───────────┐  ┌────────────┐  ┌──────────────────┐  │
│  │   Scan   │→ │  Filter   │→ │ Transform  │→ │  Write Files     │  │
│  │  (Prune) │  │ (Row-lvl) │  │ (SET expr) │  │ Data + Deletes   │  │
│  └──────────┘  └───────────┘  └────────────┘  └──────────────────┘  │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    CoalescePartitionsExec                            │
│            (Merge results to single partition)                       │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    IcebergUpdateCommitExec                           │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │                    Transaction::row_delta()                  │    │
│  │  ├─ add_data_files(new_data)                                │    │
│  │  ├─ add_position_delete_files(deletes)                      │    │
│  │  └─ commit() with conflict detection                        │    │
│  └─────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **Scan Phase**: Use Iceberg predicate pushdown for file/manifest pruning
2. **Filter Phase**: Row-level evaluation of WHERE clause
3. **Transform Phase**: Apply SET expressions to matching rows
4. **Write Phase**:
   - Write transformed rows to new data files (current partition spec)
   - Write position deletes for original rows (source file's partition spec)
5. **Commit Phase**: Atomic commit via `RowDeltaAction`

## Technical Details

### Key Components

| Component | File | Purpose |
|-----------|------|---------|
| `UpdateBuilder` | `update.rs` | Fluent API for UPDATE construction |
| `IcebergUpdateExec` | `physical_plan/update.rs` | Scan, filter, transform, write |
| `IcebergUpdateCommitExec` | `physical_plan/update_commit.rs` | Atomic commit handling |
| `PositionDeleteTaskWriter` | `position_delete_task_writer.rs` | Partition-aware delete file writing |
| `TaskWriter` | `task_writer.rs` | Partition-aware data file writing |
| `RowDeltaAction` | `transaction/row_delta.rs` | Transaction action for row-level ops |

### Position Delete File Format

Per Iceberg spec, position delete files contain:
- `file_path` (string): Path to the data file
- `pos` (long): 0-indexed row position within the file

Position delete files MUST use the same partition spec as the data file they reference.

### Cross-Column Swap Handling

SET expressions are evaluated against the ORIGINAL batch, enabling correct swap semantics:

```sql
UPDATE t SET a = b, b = a  -- Both reference original values
```

Implementation:
```rust
// Evaluate ALL expressions against ORIGINAL batch first
let mut evaluated: HashMap<String, ArrayRef> = HashMap::new();
for (column, expr) in physical_assignments {
    let result = expr.evaluate(batch)?;  // Original batch
    evaluated.insert(column.clone(), result.into_array(batch_size)?);
}
// Then build output using evaluated results
```

### Partition Key Construction

```rust
// Use task's partition spec (critical for evolution correctness)
let partition_key = if let Some(ref task_spec) = task.partition_spec {
    if !task_spec.is_unpartitioned() {
        let partition_data = task.partition.clone().unwrap_or_else(Struct::empty);
        Some(PartitionKey::new(
            (**task_spec).clone(),
            current_schema.clone(),
            partition_data,
        ))
    } else {
        None
    }
} else if !default_partition_spec.is_unpartitioned() {
    // Fallback to default spec
    ...
}
```

## Configuration & Deployment Notes

### Requirements
- Iceberg format version 2 (position deletes require v2)
- DataFusion >= 44.0

### Table Properties
- `write.target-file-size-bytes`: Controls output file size (default: 512MB)

## Testing & Validation

### Test Categories

| Category | Count | Description |
|----------|-------|-------------|
| Basic UPDATE | 6 | Empty table, no match, full table, predicate |
| Expression | 3 | Self-reference, cross-column swap, computed |
| Partitioned | 5 | Identity, year transform, cross-partition, multi-file |
| Integration | 2 | Select-after-update verification |

### Test Files
- Unit tests: `physical_plan/update.rs` (3 tests)
- Integration tests: `tests/integration_datafusion_test.rs` (29 tests)

## Risks & Trade-offs

### Known Limitations

1. **Copy-on-Write Only**: No Merge-on-Read support
   - Impact: Full file rewrite for affected files
   - Mitigation: Use selective predicates, partition pruning

2. **Partition Evolution Not Supported**: All files must use same partition spec
   - Impact: Tables with evolved partitions cannot be updated
   - Mitigation: Clear error message; planned future enhancement

3. **Type Coercion Strict**: Expression type must exactly match column type
   - Impact: May require explicit casts in SET expressions
   - Mitigation: DataFusion's type coercion in expressions

### Performance Characteristics

- Complexity: O(affected_files) not O(affected_rows)
- Best with: Selective predicates, well-partitioned tables
- File overhead: One position delete file per affected partition

## How to Extend

### Adding Merge-on-Read Support

1. Create `IcebergMoRUpdateExec` that only writes delete files
2. Skip data file writing (readers merge at read time)
3. Requires reader-side delete application support

### Adding SQL Interface Support

1. Create `IcebergQueryPlanner` implementing `QueryPlanner`
2. Intercept `LogicalPlan::Dml(WriteOp::Update)`
3. Extract assignments and filter from DML input plan
4. Build `IcebergUpdateExec` chain

### Supporting Partition Evolution

1. Expose `DataFile.partition_spec_id` as public getter
2. Build `spec_id -> partition_type` map from table metadata
3. Serialize each file with its correct partition type
4. Remove evolution guard

## References

- [Iceberg Spec: Position Delete Files](https://iceberg.apache.org/spec/#position-delete-files)
- [Iceberg Spec: Row-level Deletes](https://iceberg.apache.org/spec/#row-level-deletes)
- [DataFusion ExecutionPlan](https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html)
