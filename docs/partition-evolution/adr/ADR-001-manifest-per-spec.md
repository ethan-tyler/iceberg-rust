# ADR-001: Separate Manifests Per Partition Spec

## Status

**Accepted** - December 2024

## Context

When implementing partition evolution support, we discovered that simply relaxing the `validate_added_data_files()` check to accept any valid spec_id is insufficient.

The Iceberg specification states:

> "A manifest stores files for a single partition spec."
> — [Iceberg Spec: Manifests](https://iceberg.apache.org/spec/#manifests)

This means when committing files with different partition specs, we cannot write them all to a single manifest. We must group files by spec_id and write separate manifests for each group.

### Current Behavior

The current `SnapshotProducer`:

1. Rejects any data file with `spec_id != default_spec_id`
2. Writes all files to a single manifest using the default partition spec

### The Problem

If we only relax validation (Part A) without fixing manifest writing (Part B):

- Files with different partition specs would be written to the same manifest
- The manifest would have a single `partition_spec_id` that doesn't match all entries
- This violates the Iceberg spec and could cause reader failures

## Decision

**Implement manifest-per-spec writing alongside validation relaxation.**

### Two-Part Fix

| Part | Change | Why |
|------|--------|-----|
| **Part A** | Accept any valid spec in validation | Allow evolved tables |
| **Part B** | Group files by spec, write separate manifests | Comply with spec |

Both parts must be implemented together for correctness.

### Implementation

```rust
fn write_added_data_manifests(&self) -> Result<Vec<ManifestFile>> {
    // Group files by partition spec
    let groups = group_files_by_spec(&self.added_data_files);

    // Write one manifest per spec
    groups.into_iter()
        .map(|(spec_id, files)| {
            let spec = self.table.metadata().partition_spec_by_id(spec_id)?;
            self.write_manifest_for_spec(&files, spec)
        })
        .collect()
}
```

## Alternatives Considered

### Option A: Relax Validation Only

**Approach**: Only change `validate_added_data_files()` to accept any valid spec

**Pros**:
- Minimal code change (~5 lines)
- Quick to implement

**Cons**:
- Violates Iceberg spec
- May cause reader failures in other engines
- Silent corruption risk

**Decision**: Rejected - spec violation is unacceptable

### Option B: Validate and Reject Mixed Specs

**Approach**: Allow non-default specs only if ALL files have the SAME spec

**Pros**:
- Simpler than full grouping
- Avoids multi-manifest complexity

**Cons**:
- Still limits use cases (can't UPDATE across spec boundaries)
- Doesn't achieve full evolution support
- Artificial constraint not present in Java Iceberg

**Decision**: Rejected - partial solution doesn't achieve parity

### Option C: Full Manifest-Per-Spec (Chosen)

**Approach**: Group files by spec_id, write separate manifests

**Pros**:
- Full spec compliance
- Complete evolution support
- Matches Java Iceberg behavior
- Single-spec fast path preserves performance for common case

**Cons**:
- More implementation effort (~100 lines)
- Slightly more complex code path

**Decision**: Accepted - correct solution

## Consequences

### Positive

- Full partition evolution support
- Spec-compliant manifest structure
- Cross-engine compatibility (Spark, Trino, etc.)
- Performance preserved for common case (single spec)

### Negative

- More manifests in snapshot when specs are mixed
- Slightly more complex code to maintain
- Must handle V1 compatibility (`spec_id` defaults to 0)

### Manifest List Structure

After implementation, commits on evolved tables produce:

```
ManifestList {
    entries: [
        ManifestFile { path: "manifest_0.avro", partition_spec_id: 0, ... },
        ManifestFile { path: "manifest_1.avro", partition_spec_id: 1, ... },
    ]
}
```

Each manifest contains only files from its respective partition spec.

## References

- [Iceberg Spec: Manifests](https://iceberg.apache.org/spec/#manifests)
- [Iceberg Spec: Partition Evolution](https://iceberg.apache.org/spec/#partition-evolution)
- [DESIGN.md](../DESIGN.md) - Full implementation details
