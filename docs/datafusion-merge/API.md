# MERGE API Reference

## MergeBuilder

The main entry point for MERGE operations.

### Creating a MergeBuilder

```rust
use iceberg_datafusion::IcebergTableProvider;

// Via table provider
let stats = table_provider
    .merge(source_dataframe)
    .on(join_condition)
    // ... add WHEN clauses
    .execute(&session_state)
    .await?;
```

### Methods

#### `on(condition: Expr) -> Self`

Sets the join condition for matching source and target rows.

```rust
.on(col("target.id").eq(col("source.id")))

// Compound key
.on(col("target.id").eq(col("source.id"))
    .and(col("target.region").eq(col("source.region"))))
```

#### `when_matched(condition: Option<Expr>) -> WhenMatchedBuilder`

Starts a WHEN MATCHED clause. Returns a builder for specifying the action.

```rust
// Unconditional
.when_matched(None)
    .update(assignments)

// With condition
.when_matched(Some(col("source.op").eq(lit("D"))))
    .delete()
```

#### `when_not_matched(condition: Option<Expr>) -> WhenNotMatchedBuilder`

Starts a WHEN NOT MATCHED clause for source rows without a target match.

```rust
// Insert all unmatched source rows
.when_not_matched(None)
    .insert(columns_and_values)

// Conditional insert
.when_not_matched(Some(col("source.op").eq(lit("I"))))
    .insert(columns_and_values)
```

#### `when_not_matched_by_source(condition: Option<Expr>) -> WhenNotMatchedBySourceBuilder`

Starts a WHEN NOT MATCHED BY SOURCE clause for target rows without a source match.

```rust
// Delete orphan rows
.when_not_matched_by_source(None)
    .delete()

// Conditional update
.when_not_matched_by_source(Some(col("target.status").eq(lit("active"))))
    .update(vec![("status".to_string(), lit("inactive"))])
```

#### `with_merge_config(config: MergeConfig) -> Self`

Configures MERGE behavior, particularly Dynamic Partition Pruning.

```rust
use iceberg_datafusion::merge::MergeConfig;

.with_merge_config(MergeConfig::new()
    .with_dpp(true)
    .with_dpp_max_partitions(5000))
```

#### `execute(session: &dyn Session) -> Result<MergeStats>`

Executes the MERGE operation and returns statistics.

---

## WhenMatchedBuilder

Builder for WHEN MATCHED actions.

### Methods

#### `update(assignments: Vec<(String, Expr)>) -> MergeBuilder`

Updates matched rows with the specified column assignments.

```rust
.when_matched(None)
    .update(vec![
        ("name".to_string(), col("source.name")),
        ("updated_at".to_string(), lit(Utc::now())),
    ])
```

#### `delete() -> MergeBuilder`

Deletes matched rows.

```rust
.when_matched(Some(col("source.deleted").eq(lit(true))))
    .delete()
```

---

## WhenNotMatchedBuilder

Builder for WHEN NOT MATCHED actions.

### Methods

#### `insert(columns_and_values: Vec<(String, Expr)>) -> MergeBuilder`

Inserts new rows with the specified column values.

```rust
.when_not_matched(None)
    .insert(vec![
        ("id".to_string(), col("source.id")),
        ("name".to_string(), col("source.name")),
        ("created_at".to_string(), lit(Utc::now())),
    ])
```

---

## WhenNotMatchedBySourceBuilder

Builder for WHEN NOT MATCHED BY SOURCE actions.

### Methods

#### `update(assignments: Vec<(String, Expr)>) -> MergeBuilder`

Updates orphan target rows.

```rust
.when_not_matched_by_source(None)
    .update(vec![
        ("status".to_string(), lit("inactive")),
    ])
```

#### `delete() -> MergeBuilder`

Deletes orphan target rows.

```rust
.when_not_matched_by_source(None)
    .delete()
```

---

## MergeConfig

Configuration for MERGE operations.

### Creating MergeConfig

```rust
use iceberg_datafusion::merge::MergeConfig;

// Default configuration
let config = MergeConfig::default();

// Custom configuration
let config = MergeConfig::new()
    .with_dpp(true)
    .with_dpp_max_partitions(5000);
```

### Methods

#### `new() -> Self`

Creates a new MergeConfig with default settings.

#### `with_dpp(enabled: bool) -> Self`

Enables or disables Dynamic Partition Pruning.

- **Default**: `true`
- **When disabled**: Full table scan is performed

#### `with_dpp_max_partitions(max: usize) -> Self`

Sets the maximum number of partitions before DPP is automatically disabled.

- **Default**: `1000`
- **Behavior**: If source touches more than `max` partitions, DPP falls back to full scan

---

## MergeStats

Statistics returned after MERGE execution.

### Fields

```rust
pub struct MergeStats {
    /// Number of rows inserted (from NOT MATCHED clause)
    pub rows_inserted: u64,

    /// Number of rows updated (from MATCHED UPDATE clause)
    pub rows_updated: u64,

    /// Number of rows deleted (from MATCHED DELETE or NOT MATCHED BY SOURCE DELETE)
    pub rows_deleted: u64,

    /// Number of rows copied unchanged (Copy-on-Write overhead)
    pub rows_copied: u64,

    /// Number of data files added
    pub files_added: u64,

    /// Number of data files removed (via position deletes)
    pub files_removed: u64,

    /// Whether Dynamic Partition Pruning was applied
    pub dpp_applied: bool,

    /// Number of partitions (pruned count if DPP applied, total if not)
    pub dpp_partition_count: usize,
}
```

### Methods

#### `total_affected() -> u64`

Returns the total number of logically affected rows (inserted + updated + deleted).

```rust
let stats = merge_builder.execute(&session).await?;
println!("Affected {} rows", stats.total_affected());
```

#### `total_rows_written() -> u64`

Returns the total rows written to storage (inserted + updated + copied).
This represents the actual I/O cost, including Copy-on-Write overhead.

```rust
println!("Wrote {} rows to storage", stats.total_rows_written());
```

---

## Complete Example

```rust
use datafusion::prelude::*;
use iceberg_datafusion::IcebergTableProvider;
use iceberg_datafusion::merge::MergeConfig;

#[tokio::main]
async fn main() -> Result<()> {
    // Setup
    let ctx = SessionContext::new();
    let table = // ... load Iceberg table

    // Create source DataFrame (e.g., from CDC stream)
    let source = ctx.read_parquet("cdc_events.parquet").await?;

    // Execute MERGE with CDC pattern
    let stats = table
        .merge(source)
        .on(col("target.id").eq(col("source.id")))

        // Handle deletes first (op = 'D')
        .when_matched(Some(col("source.op").eq(lit("D"))))
            .delete()

        // Handle updates (op = 'U')
        .when_matched(Some(col("source.op").eq(lit("U"))))
            .update(vec![
                ("name".to_string(), col("source.name")),
                ("email".to_string(), col("source.email")),
                ("updated_at".to_string(), col("source.ts")),
            ])

        // Handle inserts (op = 'I')
        .when_not_matched(Some(col("source.op").eq(lit("I"))))
            .insert(vec![
                ("id".to_string(), col("source.id")),
                ("name".to_string(), col("source.name")),
                ("email".to_string(), col("source.email")),
                ("created_at".to_string(), col("source.ts")),
            ])

        // Configure DPP
        .with_merge_config(MergeConfig::new().with_dpp_max_partitions(2000))

        .execute(&ctx.state())
        .await?;

    // Report results
    println!("MERGE completed:");
    println!("  Inserted: {}", stats.rows_inserted);
    println!("  Updated:  {}", stats.rows_updated);
    println!("  Deleted:  {}", stats.rows_deleted);
    println!("  DPP:      {} (pruned to {} partitions)",
        stats.dpp_applied, stats.dpp_partition_count);

    Ok(())
}
```
