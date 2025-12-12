# Partition Evolution Technical Design

## Summary

This document describes the implementation of full partition evolution support in iceberg-rust's transaction layer, enabling DELETE/UPDATE/MERGE operations on tables with evolved partition schemes.

## Problem Statement

### Before

The current `SnapshotProducer` in `crates/iceberg/src/transaction/snapshot.rs` enforces that all data files must use the table's default partition spec:

```rust
// snapshot.rs:172-177 - Current blocking validation
if self.table.metadata().default_partition_spec_id() != data_file.partition_spec_id {
    return Err(Error::new(
        ErrorKind::DataInvalid,
        "Data file partition spec id does not match table default partition spec id",
    ));
}
```

### Pain Points Solved

1. **Cannot operate on Spark/Trino tables** - Tables created by other engines with evolved partitioning are rejected
2. **No in-place evolution workflows** - Users cannot evolve partitions and continue using iceberg-rust
3. **Engine parity gap** - Java Iceberg handles this correctly; iceberg-rust does not

## Solution Overview

### Two-Part Fix

| Part | Change | Why |
|------|--------|-----|
| **Part A** | Relax validation to accept any valid spec | Allow reading/writing evolved tables |
| **Part B** | Group files by spec, write separate manifests | Comply with Iceberg spec requirement |

Part A alone would be a spec violation. Both parts together achieve correctness.

### Architecture

```
Before:
  all_files → single_manifest_writer → one_manifest

After:
  all_files → group_by_spec_id() → {
    spec_0_files → manifest_writer(spec_0) → manifest_0
    spec_1_files → manifest_writer(spec_1) → manifest_1
    ...
  } → Vec<ManifestFile>
```

### Data Flow

1. DML operation produces data files (may have different spec_ids)
2. `validate_added_data_files()` accepts any valid spec_id from table metadata
3. `group_files_by_spec()` partitions files by spec_id
4. For each group, `new_manifest_writer_for_spec()` creates appropriately-typed writer
5. Each manifest contains files from only one partition spec
6. Manifest list references all manifests with their respective spec_ids

## Technical Details

### Key Components

| Component | File | Purpose |
|-----------|------|---------|
| `validate_added_data_files()` | `snapshot.rs:172` | Validates spec_id exists in table metadata |
| `group_files_by_spec()` | `snapshot.rs` (new) | Groups DataFiles by partition spec |
| `group_entries_by_spec()` | `snapshot.rs` (new) | Groups ManifestEntries by partition spec |
| `new_manifest_writer_for_spec()` | `snapshot.rs` (new) | Creates manifest writer for specific spec |
| `write_added_manifest()` | `snapshot.rs` | Refactored: per-spec manifest writing for data files |
| `write_delete_manifest()` | `snapshot.rs` | Refactored: per-spec manifest writing for delete files |
| `write_deleted_data_manifest()` | `snapshot.rs` | Refactored: per-spec manifest writing for deleted entries |
| `write_existing_data_manifest()` | `snapshot.rs` | Refactored: per-spec manifest writing for existing entries |

### Core Implementation

#### Validation Change

```rust
// Proposed: Accept any valid spec from table metadata
fn validate_added_data_files(&self) -> Result<()> {
    for data_file in &self.added_data_files {
        // V1 files without explicit spec_id default to spec 0
        let spec_id = data_file.partition_spec_id().unwrap_or(0);

        if self.table.metadata().partition_spec_by_id(spec_id).is_none() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Data file references unknown partition spec: {}", spec_id)
            ));
        }
    }
    Ok(())
}
```

#### File Grouping (DataFiles)

```rust
// Uses BTreeMap for deterministic iteration order (sorted by spec_id),
// ensuring consistent manifest numbering across runs.
fn group_files_by_spec(files: Vec<DataFile>) -> BTreeMap<i32, Vec<DataFile>> {
    let mut groups: BTreeMap<i32, Vec<DataFile>> = BTreeMap::new();
    for file in files {
        groups.entry(file.partition_spec_id)
            .or_default()
            .push(file);
    }
    groups
}
```

#### Entry Grouping (ManifestEntries)

```rust
// Uses BTreeMap for deterministic iteration order (sorted by spec_id).
fn group_entries_by_spec(entries: Vec<ManifestEntry>) -> BTreeMap<i32, Vec<ManifestEntry>> {
    let mut groups: BTreeMap<i32, Vec<ManifestEntry>> = BTreeMap::new();
    for entry in entries {
        let spec_id = entry.data_file().partition_spec_id;
        groups.entry(spec_id)
            .or_default()
            .push(entry);
    }
    groups
}
```

#### Schema Selection for Non-Default Specs

```rust
/// For evolved tables where the partition spec references fields not present
/// in the current schema, find a compatible historical schema.
fn find_compatible_schema(
    &self,
    partition_spec: &PartitionSpecRef,
) -> Result<SchemaRef> {
    let current_schema = self.table.metadata().current_schema();
    if partition_spec.partition_type(current_schema).is_ok() {
        return Ok(current_schema.clone());
    }

    for schema in self.table.metadata().schemas_iter() {
        if partition_spec.partition_type(schema).is_ok() {
            return Ok(schema.clone());
        }
    }

    Err(Error::new(
        ErrorKind::DataInvalid,
        format!("Cannot find compatible schema for partition spec {}", partition_spec.spec_id()),
    ))
}
```

#### Per-Spec Manifest Writer

```rust
fn new_manifest_writer_for_spec(
    &self,
    spec: &PartitionSpec,
) -> Result<ManifestWriter> {
    let manifest_path = self.new_manifest_path();
    let output_file = self.table.file_io().new_output(&manifest_path)?;

    ManifestWriter::new(
        spec.spec_id(),           // manifest-level spec_id
        spec.partition_type()?,   // partition struct schema
        output_file,
        self.snapshot_id,
        // ...
    )
}
```

#### Manifest Writing with Fast Path

```rust
fn write_added_data_manifests(&self) -> Result<Vec<ManifestFile>> {
    if self.added_data_files.is_empty() {
        return Ok(vec![]);
    }

    // Fast path: check if all files share same spec
    let first_spec_id = self.added_data_files[0].partition_spec_id().unwrap_or(0);
    let all_same_spec = self.added_data_files.iter()
        .all(|f| f.partition_spec_id().unwrap_or(0) == first_spec_id);

    if all_same_spec {
        // Single manifest, no grouping overhead
        let spec = self.table.metadata().partition_spec_by_id(first_spec_id)?;
        let manifest = self.write_manifest_for_spec(&self.added_data_files, spec)?;
        return Ok(vec![manifest]);
    }

    // Multi-spec path: group and write separately
    let groups = group_files_by_spec(&self.added_data_files);
    groups.into_iter()
        .map(|(spec_id, files)| {
            let spec = self.table.metadata().partition_spec_by_id(spec_id)?;
            self.write_manifest_for_spec(&files, spec)
        })
        .collect()
}
```

### DataFusion Layer Updates

After transaction layer changes, remove guards from:

| File | Function | Line |
|------|----------|------|
| `delete_write.rs` | `reject_partition_evolution()` call | ~466 |
| `update.rs` | `reject_partition_evolution()` call | ~331 |
| `merge.rs` | `reject_partition_evolution()` call | ~330 |

Then remove the `reject_partition_evolution()` function from `partition_utils.rs`.

### Write Semantics

**Migrate Forward** (matches Spark behavior):

| File Type | Partition Spec Used |
|-----------|---------------------|
| Position delete files | Same spec as referenced data file |
| New data files | Table's current default spec |

This is already implemented in the DataFusion layer via `build_file_partition_map()`.

## Testing & Validation

### Test Matrix

| Test Type | Count | Coverage |
|-----------|-------|----------|
| Unit: file grouping | 4 | empty, single, multi, mixed specs |
| Unit: entry grouping | 3 | empty, single, multi |
| Unit: validation | 3 | valid non-default, unknown, error message |
| Integration: DML | 3 | DELETE, UPDATE, MERGE |
| Integration: semantics | 2 | delete inherits spec, data uses default |
| Cross-engine | 2 | Spark→Rust→Spark, Rust→Spark |
| **Total** | **17** | |

### Unit Tests

```rust
#[test]
fn test_group_files_by_spec_empty() {
    let files: Vec<DataFile> = vec![];
    let groups = group_files_by_spec(&files);
    assert!(groups.is_empty());
}

#[test]
fn test_group_files_by_spec_single_spec() {
    let files = vec![data_file_with_spec(0), data_file_with_spec(0)];
    let groups = group_files_by_spec(&files);
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[&0].len(), 2);
}

#[test]
fn test_group_files_by_spec_multiple_specs() {
    let files = vec![
        data_file_with_spec(0),
        data_file_with_spec(1),
        data_file_with_spec(0),
    ];
    let groups = group_files_by_spec(&files);
    assert_eq!(groups.len(), 2);
    assert_eq!(groups[&0].len(), 2);
    assert_eq!(groups[&1].len(), 1);
}

#[test]
fn test_group_files_by_spec_v1_defaults_to_zero() {
    let files = vec![data_file_without_spec(), data_file_with_spec(0)];
    let groups = group_files_by_spec(&files);
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[&0].len(), 2);
}

#[test]
fn test_validate_accepts_any_valid_spec() {
    let table = table_with_specs(vec![0, 1, 2]);
    let file = data_file_with_spec(1);
    assert!(validate_data_file(&table, &file).is_ok());
}

#[test]
fn test_validate_rejects_unknown_spec() {
    let table = table_with_specs(vec![0, 1]);
    let file = data_file_with_spec(99);
    assert!(validate_data_file(&table, &file).is_err());
}

#[test]
fn test_validate_rejects_unknown_spec_with_clear_message() {
    let table = table_with_specs(vec![0, 1]);
    let file = data_file_with_spec(99);
    let err = validate_data_file(&table, &file).unwrap_err();
    assert!(err.message().contains("99"));
}
```

### Integration Tests

```rust
#[tokio::test]
async fn test_update_with_partition_evolution() {
    // 1. Create table partitioned by day(ts)
    // 2. Insert data under spec_id=0
    // 3. Evolve partition scheme (ADD identity(region))
    // 4. Insert data under spec_id=1
    // 5. UPDATE spanning both specs
    // 6. Verify: correct data, separate manifests per spec
}

#[tokio::test]
async fn test_update_writes_new_files_with_default_spec() {
    // Verify Migrate Forward semantic:
    // - Position delete uses ORIGINAL spec
    // - New data file uses DEFAULT spec
}
```

### Cross-Engine Tests

```python
# Addition to provision.py

def test_spark_creates_iceberg_rust_modifies():
    """Spark creates evolved table, iceberg-rust modifies."""
    spark.sql("CREATE TABLE ... PARTITIONED BY (days(ts))")
    spark.sql("INSERT INTO ...")
    spark.sql("ALTER TABLE ... ADD PARTITION FIELD region")
    spark.sql("INSERT INTO ...")
    # iceberg-rust performs UPDATE
    # Spark verifies data and manifest structure

def test_iceberg_rust_creates_spark_reads():
    """iceberg-rust creates evolved table, Spark reads."""
    # iceberg-rust creates table, evolves, writes
    # Spark verifies data and manifest structure
```

## Implementation Plan

### Phases

| Phase | Deliverable | Files Changed |
|-------|-------------|---------------|
| **1** | `group_files_by_spec()` helper for DataFiles | `snapshot.rs` |
| **2** | `group_entries_by_spec()` helper for ManifestEntries | `snapshot.rs` |
| **3** | `new_manifest_writer_for_spec()` | `snapshot.rs` |
| **4** | Refactor `write_added_manifest()` for per-spec data files | `snapshot.rs` |
| **5** | Refactor `write_delete_manifest()` for per-spec delete files | `snapshot.rs` |
| **6** | Refactor `write_deleted_data_manifest()` for per-spec entries | `snapshot.rs` |
| **7** | Refactor `write_existing_data_manifest()` for per-spec entries | `snapshot.rs` |
| **8** | Update `validate_added_data_files()` | `snapshot.rs` |
| **9** | Unit tests (10 tests) | `snapshot.rs` |
| **10** | Integration tests (5 tests) | `datafusion/tests/` |
| **11** | Cross-engine tests (2 tests) | `provision.py` |
| **12** | Remove DataFusion guards | `delete_write.rs`, `update.rs`, `merge.rs` |
| **13** | Remove `reject_partition_evolution()` | `partition_utils.rs` |

### PR Structure

**PR 1: Core Transaction Layer** (Phases 1-9)
- All changes to `snapshot.rs`
- Grouping helpers for files and entries
- Refactored manifest writers for all file types
- Validation relaxation
- Unit tests
- Safe to merge independently (guards still block evolved tables)

**PR 2: Integration & DataFusion** (Phases 10-13)
- Integration tests
- Cross-engine tests
- Guard removal
- Depends on PR 1

## Risks & Trade-offs

### Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Manifest corruption | Low | Critical | Cross-engine validation before merge |
| V1 compatibility regression | Medium | High | Explicit V1 tests with `unwrap_or(0)` |
| Performance regression | Low | Medium | Single-spec fast path preserves common case |
| Spark read failures | Low | High | Bidirectional cross-engine tests |

### Rollback Strategy

If issues discovered post-merge:

```rust
// 1. Re-add guards (3 lines across 3 files)
reject_partition_evolution(&table, "DELETE")?;
reject_partition_evolution(&table, "UPDATE")?;
reject_partition_evolution(&table, "MERGE")?;

// 2. Re-add #[ignore] to partition evolution tests

// 3. Transaction layer changes can remain (no behavioral change
//    when guards prevent evolved tables from reaching commit)
```

### Success Criteria

| Criterion | Verification |
|-----------|--------------|
| All 17 tests pass | `cargo test partition_evolution` |
| No regression in existing tests | Full CI green |
| Spark reads iceberg-rust writes | Cross-engine test passes |
| iceberg-rust reads Spark writes | Cross-engine test passes |
| Manifests correctly structured | Manual Avro inspection |
| Data, delete, and entry manifests all per-spec | Integration test verification |

## References

- [Iceberg Partition Evolution Spec](https://iceberg.apache.org/spec/#partition-evolution)
- [ADR-002: Original Guard Decision](../datafusion-update/adr/ADR-002-partition-evolution.md)
- [Manifest Spec: "A manifest stores files for a single partition spec"](https://iceberg.apache.org/spec/#manifests)
