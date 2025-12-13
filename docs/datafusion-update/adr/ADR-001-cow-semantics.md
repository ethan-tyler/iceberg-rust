# ADR-001: Copy-on-Write Semantics for UPDATE Operations

## Status

**Accepted** - Implemented in Sprint 1-2

## Context

Iceberg supports two strategies for row-level modifications:

1. **Copy-on-Write (CoW)**: Write new data files with updated rows, write position delete files for original rows
2. **Merge-on-Read (MoR)**: Write only delete files, merge updates at read time

We needed to decide which strategy to implement first for UPDATE operations in iceberg-datafusion.

## Decision

**Implement Copy-on-Write (CoW) semantics for UPDATE operations.**

### Rationale

1. **Simpler Reader Implementation**: CoW produces complete data files; no merge logic needed at read time
2. **Query Performance**: CoW favors read performance over write performance (appropriate for analytics)
3. **Ecosystem Compatibility**: More mature support across Iceberg readers
4. **Atomic Semantics**: Easier to reason about - each snapshot is complete
5. **iceberg-rust State**: Position delete file writing was already implemented; MoR would require delete-aware readers

## Alternatives Considered

### Merge-on-Read (MoR)

**Pros**:
- Faster writes (only delete files written)
- Lower storage amplification for frequent updates

**Cons**:
- Requires delete-aware readers (not fully implemented)
- Read performance degrades with accumulated deletes
- More complex compaction requirements
- Less ecosystem support in iceberg-rust

### Hybrid Approach

**Pros**:
- User choice per table/operation

**Cons**:
- Double implementation complexity
- Maintenance burden
- Premature optimization

## Consequences

### Positive
- UPDATE is available now with full correctness
- Query performance is predictable
- No reader changes required
- Simple mental model for users

### Negative
- Higher write amplification for small updates to large files
- Full file rewrite even for single-row updates
- Storage overhead for position delete files

### Mitigations
- Recommend selective predicates to minimize affected files
- Leverage partition pruning
- Future: configurable file size thresholds
- Future: MoR support when readers are ready

## References

- [Iceberg Row-Level Deletes](https://iceberg.apache.org/spec/#row-level-deletes)
- Implementation: `IcebergUpdateExec` in `physical_plan/update.rs`
