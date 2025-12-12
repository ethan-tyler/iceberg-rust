# Partition Evolution Support

## Summary

Full partition evolution support for iceberg-rust, enabling DELETE/UPDATE/MERGE operations on tables where the partition scheme has changed over time. This achieves parity with Java Iceberg's handling of evolved tables.

## Documentation Index

| Document | Description |
|----------|-------------|
| [DESIGN.md](./DESIGN.md) | Technical architecture and implementation details |
| [ADR-001-manifest-per-spec.md](./adr/ADR-001-manifest-per-spec.md) | Decision: Separate manifests per partition spec |

## Related Documentation

| Document | Description |
|----------|-------------|
| [ADR-002-partition-evolution.md](../datafusion-update/adr/ADR-002-partition-evolution.md) | Original guard implementation decision |

## Quick Links

- **Core Changes**: `crates/iceberg/src/transaction/snapshot.rs`
- **Guard Removal**: `crates/integrations/datafusion/src/physical_plan/`
- **Tests**: `crates/integrations/datafusion/tests/`

## Current Status

| Feature | Status | Notes |
|---------|--------|-------|
| Accept any valid spec_id | Planned | Relax validation in `SnapshotProducer` |
| Manifest-per-spec writing | Planned | Group files, write separate manifests |
| DELETE on evolved tables | Planned | Remove guard after core changes |
| UPDATE on evolved tables | Planned | Remove guard after core changes |
| MERGE on evolved tables | Planned | Remove guard after core changes |
| Cross-engine validation | Planned | Spark interop tests |

## Out of Scope

These features are separate concerns, not part of this work:

- **Dynamic partition pruning** - Query optimization feature
- **Partition-scoped conflict detection** - Optimistic concurrency feature

## Write Semantics

This implementation uses **Migrate Forward** semantics (matching Spark behavior):

| File Type | Partition Spec Used |
|-----------|---------------------|
| Position delete files | Same spec as referenced data file |
| New data files | Table's current default spec |

## Usage

After implementation, evolved tables work transparently:

```sql
-- Table was partitioned by day(ts), now also by region
-- Both old and new data can be modified

DELETE FROM evolved_table WHERE id = 123;
UPDATE evolved_table SET value = 0 WHERE region = 'us';
MERGE INTO evolved_table USING source ON ... WHEN MATCHED THEN UPDATE ...;
```
