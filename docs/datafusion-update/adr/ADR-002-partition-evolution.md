# ADR-002: Partition Evolution Handling in UPDATE Operations

## Status

**Accepted** - Guard implemented in Sprint 3

## Context

Iceberg supports partition evolution: changing the partition scheme of a table while preserving existing data files. After evolution:

- Old files retain their original partition spec (different `partition_spec_id`)
- New files use the current partition spec
- Position delete files MUST use the same partition spec as the data file they reference

When implementing UPDATE for partitioned tables, we discovered:

1. `FileScanTask` carries the partition spec each file was written under
2. Position delete files must be serialized with the correct partition type
3. `DataFile.partition_spec_id` is `pub(crate)` - not accessible from iceberg-datafusion
4. Serialization uses a single `partition_type` derived from default spec

This creates a potential data corruption scenario: if we serialize position delete files with the wrong partition type, commits may fail or produce invalid metadata.

## Decision

**Implement an explicit guard that rejects UPDATE operations on tables where any file uses a non-default partition spec.**

```rust
for task in &file_scan_tasks {
    if let Some(ref task_spec) = task.partition_spec {
        if task_spec.spec_id() != default_spec_id {
            return Err(DataFusionError::NotImplemented(format!(
                "UPDATE does not yet support partition evolution. \
                 File '{}' uses partition spec {} but table default is spec {}.",
                task.data_file_path(), task_spec_id, default_spec_id
            )));
        }
    }
}
```

### Rationale

1. **Explicit Over Silent**: Clear error message is better than silent data corruption
2. **Safe Default**: Prevents invalid position delete files from being written
3. **Reversible**: Guard can be removed when proper support is added
4. **Low Impact**: Most production tables don't use partition evolution
5. **API Constraint**: Cannot access `DataFile.partition_spec_id` for per-file serialization

## Alternatives Considered

### Option A: Track Partition Types During Write

**Approach**: Build `file_path -> partition_type` mapping during write, use for serialization

**Pros**:
- Would enable full evolution support
- No iceberg crate changes needed

**Cons**:
- Complex coordination between write and serialize phases
- `PositionDeleteTaskWriter.close()` returns only `Vec<DataFile>`
- Significant refactoring required

### Option B: Expose `partition_spec_id` on DataFile

**Approach**: Add public getter in iceberg crate, look up partition type per file

**Pros**:
- Clean solution
- Enables per-file serialization
- Minimal iceberg-datafusion changes

**Cons**:
- Requires iceberg crate modification
- API change needs broader discussion

### Option C: Compute Partition Type from Struct

**Approach**: Infer partition type from the partition Struct field values

**Pros**:
- No API changes needed

**Cons**:
- Unreliable - struct doesn't carry type metadata
- Could produce incorrect results
- Fragile to schema changes

## Consequences

### Positive
- UPDATE is safe for the common case (single partition spec)
- Clear error message guides users
- No silent data corruption
- Buys time to implement proper solution

### Negative
- Tables with partition evolution cannot be updated
- May require users to compact/rewrite old partitions first

### Future Work

1. **Option B is preferred**: Add `pub fn partition_spec_id(&self) -> i32` to `DataFile`
2. Build `spec_id -> partition_type` map from table metadata
3. Serialize each file with its correct partition type
4. Remove the evolution guard

## References

- [Iceberg Partition Evolution](https://iceberg.apache.org/spec/#partition-evolution)
- Guard implementation: `physical_plan/update.rs:395-409`
- Related: `DataFile.partition_spec_id` in `spec/manifest/data_file.rs:163`
