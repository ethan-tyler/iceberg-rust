# MERGE/UPSERT SQL Operations for DataFusion

## Summary

This implementation adds full MERGE/UPSERT support for Apache Iceberg tables via the DataFusion integration. It follows SQL:2003 MERGE semantics with extensions for CDC (Change Data Capture) workloads.

## Documentation Index

| Document | Description |
|----------|-------------|
| [DESIGN.md](./DESIGN.md) | Technical architecture and data flow |
| [API.md](./API.md) | API reference and usage examples |

## Quick Links

- **Source Code**: `crates/integrations/datafusion/src/merge.rs`
- **Physical Plans**: `crates/integrations/datafusion/src/physical_plan/merge.rs`
- **Commit Layer**: `crates/integrations/datafusion/src/physical_plan/merge_commit.rs`
- **Tests**: `crates/integrations/datafusion/tests/integration_datafusion_test.rs`

## Current Status

| Feature | Status | Notes |
|---------|--------|-------|
| WHEN MATCHED UPDATE | Complete | With optional conditions |
| WHEN MATCHED DELETE | Complete | With optional conditions |
| WHEN NOT MATCHED INSERT | Complete | With optional conditions |
| WHEN NOT MATCHED BY SOURCE UPDATE | Complete | With optional conditions |
| WHEN NOT MATCHED BY SOURCE DELETE | Complete | With optional conditions |
| Partitioned tables | Complete | Identity transforms |
| Compound join keys | Complete | Multi-column joins |
| Dynamic Partition Pruning (DPP) | Complete | With configurable threshold |
| Atomic commits | Complete | Via RowDeltaAction |
| Copy-on-Write execution | Complete | Default mode |
| Merge-on-Read execution | Not implemented | Future work |
| Streaming execution | Not implemented | Deferred to Phase 3 |

## Test Coverage

**15 tests total** (3 unit + 12 integration):

- Basic MERGE operations (3 tests)
- Full CDC pattern with all clause types
- End-to-end commit persistence
- Partitioned table MERGE (2 tests)
- Compound join keys (2 tests)
- Dynamic Partition Pruning scenarios (3 tests)

## Usage

### SQL Interface

```sql
-- Basic UPSERT
MERGE INTO target USING source ON target.id = source.id
WHEN MATCHED THEN UPDATE SET name = source.name, value = source.value
WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (source.id, source.name, source.value);

-- CDC Pattern with DELETE
MERGE INTO target USING source ON target.id = source.id
WHEN MATCHED AND source.op = 'D' THEN DELETE
WHEN MATCHED AND source.op = 'U' THEN UPDATE SET name = source.name
WHEN NOT MATCHED AND source.op = 'I' THEN INSERT (id, name) VALUES (source.id, source.name);

-- With NOT MATCHED BY SOURCE (orphan cleanup)
MERGE INTO target USING source ON target.id = source.id
WHEN MATCHED THEN UPDATE SET value = source.value
WHEN NOT MATCHED THEN INSERT (id, value) VALUES (source.id, source.value)
WHEN NOT MATCHED BY SOURCE THEN DELETE;
```

### Programmatic API

```rust
use iceberg_datafusion::merge::{MergeBuilder, MergeConfig};

let stats = table
    .merge(source_df)
    .on(col("target.id").eq(col("source.id")))
    .when_matched(Some(col("source.op").eq(lit("D"))))
        .delete()
    .when_matched(None)
        .update(vec![
            ("name".to_string(), col("source.name")),
            ("value".to_string(), col("source.value")),
        ])
    .when_not_matched(None)
        .insert(vec![
            ("id".to_string(), col("source.id")),
            ("name".to_string(), col("source.name")),
            ("value".to_string(), col("source.value")),
        ])
    .execute(&session_state)
    .await?;

println!("Inserted: {}, Updated: {}, Deleted: {}",
    stats.rows_inserted, stats.rows_updated, stats.rows_deleted);
```

### Configuring Dynamic Partition Pruning

```rust
use iceberg_datafusion::merge::MergeConfig;

// Disable DPP entirely
let config = MergeConfig::new().with_dpp(false);

// Increase partition threshold (default: 1000)
let config = MergeConfig::new().with_dpp_max_partitions(5000);

let stats = table
    .merge(source_df)
    .with_merge_config(config)
    // ... rest of builder
    .execute(&session_state)
    .await?;

// Check if DPP was applied
if stats.dpp_applied {
    println!("DPP pruned to {} partitions", stats.dpp_partition_count);
}
```

## Known Limitations

### Dynamic Partition Pruning (DPP)

DPP only works when:
- Partition uses **identity transform** (not bucket/truncate/year/month/etc.)
- Partition column is part of the **join condition**
- Data type is one of: Int32, Int64, String, Date
- Distinct partition count is below threshold (default: 1000)

### Memory Usage

Current implementation buffers source data in memory. For very large source datasets (>1M rows), a warning is emitted:

```
MERGE: Large source dataset (1500000 rows). Consider batching for memory efficiency.
```

### Execution Mode

Only Copy-on-Write (CoW) is implemented. Merge-on-Read (MoR) is planned for future work.

## Metrics

`MergeStats` provides detailed execution metrics:

```rust
pub struct MergeStats {
    // Row-level metrics
    pub rows_inserted: u64,
    pub rows_updated: u64,
    pub rows_deleted: u64,
    pub rows_copied: u64,      // CoW overhead

    // File-level metrics
    pub files_added: u64,
    pub files_removed: u64,

    // DPP metrics
    pub dpp_applied: bool,
    pub dpp_partition_count: usize,
}
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        MergeBuilder API                          │
│  .merge(source).on(condition).when_matched()...execute()        │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                      IcebergMergeExec                            │
│  • FULL OUTER JOIN (target × source)                            │
│  • Row classification (MATCHED/NOT_MATCHED/NOT_MATCHED_BY_SRC)  │
│  • WHEN clause evaluation (first-match-wins)                    │
│  • Action application (INSERT/UPDATE/DELETE/COPY)               │
│  • Data file + position delete file generation                  │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    IcebergMergeCommitExec                        │
│  • Deserialize file paths from upstream                         │
│  • Atomic commit via RowDeltaAction                             │
│  • Conflict detection (snapshot ID validation)                  │
│  • Return MergeStats                                            │
└─────────────────────────────────────────────────────────────────┘
```

## Future Work

1. **Streaming Execution** - Process batches without buffering entire source
2. **Merge-on-Read** - Write position deletes instead of rewriting files
3. **Partition-Scoped Conflicts** - Finer-grained conflict detection
4. **UPDATE SET * / INSERT *** - Convenience methods for all columns
