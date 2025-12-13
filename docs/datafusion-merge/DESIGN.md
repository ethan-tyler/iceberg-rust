# MERGE/UPSERT Technical Design

## Summary

This document describes the technical architecture of MERGE/UPSERT operations for Apache Iceberg tables via the DataFusion integration. The implementation follows SQL:2003 MERGE semantics with Copy-on-Write (CoW) execution.

## Problem Statement

### Before

Prior to this work, iceberg-rust/datafusion supported:
- Table scanning (SELECT)
- Basic INSERT via `insert_into()`
- UPDATE via `TableProvider::update()`
- DELETE via `TableProvider::delete()`

There was no way to perform atomic UPSERT operations combining inserts, updates, and deletes in a single transaction.

### Pain Points Solved

1. **Multiple transactions** - CDC pipelines required separate INSERT/UPDATE/DELETE statements
2. **Race conditions** - No atomic guarantees across multiple operations
3. **Inefficiency** - Multiple table scans for related operations
4. **Missing semantics** - No `NOT MATCHED BY SOURCE` for orphan cleanup

## Solution Overview

### Single-Pass FULL OUTER JOIN

The implementation uses a single FULL OUTER JOIN to classify all rows:

```
Target Table          Source Data
┌─────────────┐      ┌─────────────┐
│ id │ value  │      │ id │ value  │
├────┼────────┤      ├────┼────────┤
│  1 │ "old"  │      │  1 │ "new"  │  → MATCHED (update)
│  2 │ "keep" │      │  3 │ "add"  │  → NOT_MATCHED (insert)
│  4 │ "gone" │      └─────────────┘  → NOT_MATCHED_BY_SOURCE (delete)
└─────────────┘
```

### Row Classification

After the FULL OUTER JOIN, each row is classified:

| Classification | Target NULL? | Source NULL? | Meaning |
|----------------|--------------|--------------|---------|
| MATCHED | No | No | Row exists in both |
| NOT_MATCHED | Yes | No | Row only in source (insert candidate) |
| NOT_MATCHED_BY_SOURCE | No | Yes | Row only in target (orphan) |

### WHEN Clause Evaluation

Clauses are evaluated in declaration order (first-match-wins):

```rust
// Example: CDC with conditional logic
.when_matched(Some(col("source.op").eq(lit("D"))))  // 1st: Delete if op='D'
    .delete()
.when_matched(None)                                  // 2nd: Otherwise update
    .update(assignments)
```

### Action Application

Each classified row receives an action:

| Action | Description | Output |
|--------|-------------|--------|
| INSERT | New row from source | Data file row |
| UPDATE | Modified target row | Data file row + position delete |
| DELETE | Remove target row | Position delete only |
| COPY | Unchanged target row | Data file row (CoW overhead) |
| SKIP | No action | Nothing |

## Technical Details

### Key Components

| Component | File | Purpose |
|-----------|------|---------|
| `MergeBuilder` | [merge.rs](../../crates/integrations/datafusion/src/merge.rs) | Fluent API for building MERGE operations |
| `MergeStats` | [merge.rs](../../crates/integrations/datafusion/src/merge.rs) | Execution metrics |
| `MergeConfig` | [merge.rs](../../crates/integrations/datafusion/src/physical_plan/merge.rs) | DPP configuration |
| `IcebergMergeExec` | [merge.rs](../../crates/integrations/datafusion/src/physical_plan/merge.rs) | Physical execution plan |
| `IcebergMergeCommitExec` | [merge_commit.rs](../../crates/integrations/datafusion/src/physical_plan/merge_commit.rs) | Atomic commit layer |

### Data Flow

```
1. Source DataFrame
        │
        ▼
2. FULL OUTER JOIN with Target Table
        │
        ▼
3. Row Classification
   ├── MATCHED rows → evaluate WHEN MATCHED clauses
   ├── NOT_MATCHED rows → evaluate WHEN NOT MATCHED clauses
   └── NOT_MATCHED_BY_SOURCE rows → evaluate WHEN NOT MATCHED BY SOURCE clauses
        │
        ▼
4. Action Determination (first-match-wins)
        │
        ▼
5. Output Generation
   ├── INSERT/UPDATE/COPY → Data file rows
   └── UPDATE/DELETE → Position delete entries
        │
        ▼
6. File Writing
   ├── Data files (Parquet)
   └── Position delete files (Parquet)
        │
        ▼
7. Atomic Commit (RowDeltaAction)
        │
        ▼
8. Return MergeStats
```

### Dynamic Partition Pruning (DPP)

For partitioned tables, DPP optimizes target scanning:

```
Without DPP:
┌─────────────────────────────────────────┐
│ Scan ALL partitions                     │
│ partition=2024-01-01, 2024-01-02, ...   │
└─────────────────────────────────────────┘

With DPP:
┌─────────────────────────────────────────┐
│ 1. Extract distinct partition values    │
│    from source: [2024-01-01]            │
│ 2. Build predicate: date IN (2024-01-01)│
│ 3. Scan ONLY matching partitions        │
└─────────────────────────────────────────┘
```

**DPP Algorithm:**

1. Identify partition columns with identity transforms
2. Check if partition column is in join condition
3. Extract distinct values from source data
4. If count ≤ threshold (default: 1000), build `IN` predicate
5. Apply predicate to target table scan

### Position Delete Generation

For MATCHED DELETE and UPDATE actions:

```rust
// Position delete entry
struct PositionDelete {
    file_path: String,  // Path to data file containing row
    pos: i64,           // Row position within file
}
```

Position deletes reference the original data file, enabling efficient deletion without rewriting unchanged rows in other partitions.

### Atomic Commit

The commit uses `RowDeltaAction` for atomicity:

```rust
// Simplified commit flow
let mut action = RowDeltaAction::new(table);

// Add new data files
for data_file in data_files {
    action.add_data_file(data_file);
}

// Add position delete files
for delete_file in delete_files {
    action.add_delete_file(delete_file);
}

// Validate no conflicts since baseline snapshot
action.validate_from_snapshot(baseline_snapshot_id);

// Atomic commit
action.commit().await?;
```

### Conflict Detection

Conflicts are detected by validating the snapshot ID:

1. Capture `baseline_snapshot_id` before MERGE
2. Execute MERGE operations
3. At commit time, verify table hasn't changed
4. If changed, fail with conflict error

## Configuration & Deployment

### MergeConfig Options

```rust
pub struct MergeConfig {
    /// Enable Dynamic Partition Pruning
    pub enable_dpp: bool,           // default: true

    /// Maximum partitions before DPP is disabled
    pub dpp_max_partitions: usize,  // default: 1000
}
```

### Environment Considerations

- **Memory**: Source data is buffered; large sources may require batching
- **Concurrency**: Single-writer assumed; conflicts fail fast
- **Storage**: CoW may temporarily double storage during rewrite

## Testing & Validation

| Category | Count | Description |
|----------|-------|-------------|
| Unit | 3 | MergeStats calculations |
| Integration | 12 | End-to-end MERGE scenarios |

### Test Scenarios

1. **Basic Operations**: MATCHED UPDATE, NOT_MATCHED INSERT, DELETE
2. **CDC Pattern**: Full CDC with op column dispatch
3. **Partitioned Tables**: Multi-partition MERGE with DPP
4. **Compound Keys**: Two-column join conditions
5. **DPP Edge Cases**: High cardinality, non-partition keys

## Risks & Trade-offs

### Known Limitations

1. **Memory buffering** - Source data held in memory; not suitable for unbounded sources
2. **CoW overhead** - Unchanged rows in affected files are rewritten
3. **Identity transforms only** - DPP doesn't work with bucket/truncate/etc.
4. **Single-writer** - Concurrent MERGE will fail on conflict

### Performance Characteristics

| Scenario | Complexity | Notes |
|----------|------------|-------|
| Small source, large target | O(source + matched_target) | DPP critical |
| Large source, small target | O(source + target) | Consider batching |
| High-cardinality partitions | O(source + target) | DPP disabled |

## How to Extend

### Adding Merge-on-Read (MoR)

1. Add `MergeWriteMode::MergeOnRead` to `MergeConfig`
2. In `IcebergMergeExec`, skip COPY rows when MoR enabled
3. Write only position deletes + inserts
4. Commit via same `RowDeltaAction` path

### Adding Streaming Execution

1. Replace source buffering with streaming iterator
2. Process batches incrementally
3. Accumulate file writers per partition
4. Flush when size threshold reached

## References

- [Apache Iceberg Spec - Row-Level Deletes](https://iceberg.apache.org/spec/#row-level-deletes)
- [delta-rs MERGE Implementation](https://github.com/delta-io/delta-rs/blob/main/crates/core/src/operations/merge.rs)
- [SQL:2003 MERGE Semantics](https://en.wikipedia.org/wiki/Merge_(SQL))
