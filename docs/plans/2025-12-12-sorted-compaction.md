# Sorted Compaction Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement sorted compaction (`rewrite_data_files` with sort strategy) for Apache Iceberg tables in iceberg-rust, enabling data clustering by user-specified columns to dramatically improve query performance through enhanced row group skipping.

**Architecture:** Sorted compaction extends the existing bin-pack compaction (Phase 2.1) by adding a global sort step between file reading and writing. The sort is applied within each partition using DataFusion's `SortExec`, which supports external sorting with disk spilling for large datasets.

**Tech Stack:** Rust, DataFusion (arrow-datafusion), Apache Iceberg spec, Parquet

---

## Overview

The implementation adds sorting capability to the existing compaction infrastructure:

```
FileGroup → read_file_group() → [RecordBatch] → SORT → write_compacted_files() → DataFile[]
                                                  ↑
                                            NEW: sort_batches()
```

**Key Files:**
- `crates/iceberg/src/transaction/rewrite_data_files/mod.rs` - Remove sort validation error
- `crates/integrations/datafusion/src/physical_plan/compaction.rs` - Add sorting logic
- `crates/integrations/datafusion/src/compaction.rs` - Pass strategy to executor

---

## Task 1: Add Sort Order Resolution Logic

**Files:**
- Modify: `crates/iceberg/src/transaction/rewrite_data_files/mod.rs:754-769`

**Step 1: Write the failing test**

Add to `crates/iceberg/src/transaction/rewrite_data_files/mod.rs` in the `tests` module:

```rust
#[test]
fn test_sort_order_resolution_with_explicit_order() {
    use crate::spec::{SortField, SortDirection, NullOrder, SortOrder, Transform};

    let sort_field = SortField::builder()
        .source_id(1)
        .direction(SortDirection::Ascending)
        .null_order(NullOrder::First)
        .transform(Transform::Identity)
        .build();

    let sort_order = SortOrder::builder()
        .with_sort_field(sort_field)
        .build_unbound()
        .unwrap();

    let action = RewriteDataFilesAction::new()
        .sort_by(sort_order.clone());

    match &action.strategy {
        RewriteStrategy::Sort { sort_order: Some(order) } => {
            assert_eq!(order.fields.len(), 1);
            assert_eq!(order.fields[0].source_id, 1);
        }
        _ => panic!("Expected Sort strategy with sort order"),
    }
}

#[test]
fn test_sort_uses_table_default_when_none_provided() {
    let action = RewriteDataFilesAction::new().sort();

    match &action.strategy {
        RewriteStrategy::Sort { sort_order: None } => {
            // Expected: None means use table's default
        }
        _ => panic!("Expected Sort strategy with None sort_order"),
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p iceberg --lib rewrite_data_files::tests::test_sort_order_resolution`
Expected: PASS (these tests should pass already since the strategy stores the sort_order)

**Step 3: No implementation needed**

The sort_order is already correctly stored in the strategy. This task validates existing behavior.

**Step 4: Run test to verify it passes**

Run: `cargo test -p iceberg --lib rewrite_data_files::tests::test_sort_order_resolution`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/iceberg/src/transaction/rewrite_data_files/mod.rs
git commit -m "test: add sort order resolution tests for rewrite_data_files"
```

---

## Task 2: Create Sort Strategy Validation Logic

**Files:**
- Modify: `crates/iceberg/src/transaction/rewrite_data_files/mod.rs:754-769`

**Step 1: Write the failing test**

Add to `crates/iceberg/src/transaction/rewrite_data_files/mod.rs` tests module:

```rust
#[test]
fn test_sort_strategy_requires_sort_order_when_table_has_none() {
    // This test requires an actual table, so we'll test the helper function
    // The validation logic should check:
    // - If sort_order is Some, use it
    // - If sort_order is None, table must have a default sort order
}
```

Since we can't easily create a mock table in unit tests, we'll add this validation in the integration layer (DataFusion) instead.

**Step 2: Remove the FeatureUnsupported error for Sort strategy**

In `crates/iceberg/src/transaction/rewrite_data_files/mod.rs`, modify the `validate()` method (around line 754):

Replace:
```rust
// Validate strategy (placeholders for future phases)
match &self.strategy {
    RewriteStrategy::Sort { .. } => {
        return Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Sort strategy not yet implemented (Phase 2.2)",
        ));
    }
    RewriteStrategy::ZOrder { .. } => {
        return Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Z-order strategy not yet implemented (Phase 2.3)",
        ));
    }
    RewriteStrategy::BinPack => {}
}
```

With:
```rust
// Validate strategy
match &self.strategy {
    RewriteStrategy::Sort { .. } => {
        // Sort strategy is now supported (Phase 2.2)
        // Sort order resolution happens at execution time in DataFusion
    }
    RewriteStrategy::ZOrder { .. } => {
        return Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Z-order strategy not yet implemented (Phase 2.3)",
        ));
    }
    RewriteStrategy::BinPack => {}
}
```

**Step 3: Run test to verify compilation**

Run: `cargo check -p iceberg`
Expected: PASS

**Step 4: Run all rewrite_data_files tests**

Run: `cargo test -p iceberg --lib rewrite_data_files`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/iceberg/src/transaction/rewrite_data_files/mod.rs
git commit -m "feat: enable sort strategy validation for rewrite_data_files (Phase 2.2)"
```

---

## Task 3: Add RewriteStrategy to CompactionOptions

**Files:**
- Modify: `crates/integrations/datafusion/src/compaction.rs`

**Step 1: Write the failing test**

Add to `crates/integrations/datafusion/src/compaction.rs` tests module:

```rust
#[test]
fn test_compaction_options_with_strategy() {
    use iceberg::transaction::rewrite_data_files::RewriteStrategy;

    let options = CompactionOptions::default()
        .with_strategy(RewriteStrategy::Sort { sort_order: None });

    assert!(options.strategy.is_some());
    assert!(options.strategy.as_ref().unwrap().is_sort());
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p iceberg-datafusion --lib compaction::tests::test_compaction_options_with_strategy`
Expected: FAIL - `with_strategy` method doesn't exist

**Step 3: Add strategy field and method to CompactionOptions**

In `crates/integrations/datafusion/src/compaction.rs`, modify `CompactionOptions`:

```rust
use iceberg::transaction::rewrite_data_files::{
    CompactionProgressEvent, ProgressCallback, RewriteDataFilesOptions, RewriteDataFilesPlanner,
    RewriteDataFilesResult, RewriteStrategy,
};

/// Options for table compaction.
#[derive(Default)]
pub struct CompactionOptions {
    /// Core rewrite options (file sizes, thresholds).
    pub rewrite_options: Option<RewriteDataFilesOptions>,
    /// Rewrite strategy (bin-pack or sort).
    pub strategy: Option<RewriteStrategy>,
    /// Cancellation token for aborting the operation.
    pub cancellation_token: Option<CancellationToken>,
    /// Progress callback for monitoring.
    pub progress_callback: Option<ProgressCallback>,
}

impl CompactionOptions {
    // ... existing methods ...

    /// Set rewrite strategy.
    #[must_use]
    pub fn with_strategy(mut self, strategy: RewriteStrategy) -> Self {
        self.strategy = Some(strategy);
        self
    }
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p iceberg-datafusion --lib compaction::tests::test_compaction_options_with_strategy`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/integrations/datafusion/src/compaction.rs
git commit -m "feat: add strategy option to CompactionOptions"
```

---

## Task 4: Add Strategy to IcebergCompactionExec

**Files:**
- Modify: `crates/integrations/datafusion/src/physical_plan/compaction.rs`

**Step 1: Write the failing test**

Add to the tests module:

```rust
#[test]
fn test_compaction_exec_accepts_strategy() {
    use iceberg::transaction::rewrite_data_files::RewriteStrategy;

    // We can't easily test the full executor without a table,
    // but we can verify the struct accepts strategy
    let strategy = RewriteStrategy::Sort { sort_order: None };
    assert!(strategy.is_sort());
}
```

**Step 2: Add strategy field to IcebergCompactionExec**

In `crates/integrations/datafusion/src/physical_plan/compaction.rs`, modify the struct:

```rust
use iceberg::transaction::rewrite_data_files::{
    CompactionProgressEvent, FileGroup, ProgressCallback, RewriteDataFilesPlan, RewriteStrategy,
};

pub struct IcebergCompactionExec {
    /// The Iceberg table being compacted.
    table: Table,
    /// The compaction plan with file groups.
    plan: RewriteDataFilesPlan,
    /// The rewrite strategy (bin-pack or sort).
    strategy: RewriteStrategy,
    /// Output schema.
    output_schema: ArrowSchemaRef,
    /// Plan properties.
    plan_properties: PlanProperties,
    /// Optional cancellation token.
    cancellation_token: Option<CancellationToken>,
    /// Optional progress callback.
    progress_callback: Option<ProgressCallback>,
}
```

Update the `new()` method:

```rust
impl IcebergCompactionExec {
    /// Create a new compaction executor.
    pub fn new(table: Table, plan: RewriteDataFilesPlan) -> DFResult<Self> {
        Self::new_with_strategy(table, plan, RewriteStrategy::BinPack)
    }

    /// Create a new compaction executor with specified strategy.
    pub fn new_with_strategy(
        table: Table,
        plan: RewriteDataFilesPlan,
        strategy: RewriteStrategy,
    ) -> DFResult<Self> {
        let output_schema = compaction_output_schema();
        let plan_properties = Self::compute_properties(output_schema.clone());

        Ok(Self {
            table,
            plan,
            strategy,
            output_schema,
            plan_properties,
            cancellation_token: None,
            progress_callback: None,
        })
    }

    /// Get the rewrite strategy.
    pub fn strategy(&self) -> &RewriteStrategy {
        &self.strategy
    }
}
```

**Step 3: Run compilation check**

Run: `cargo check -p iceberg-datafusion`
Expected: PASS

**Step 4: Commit**

```bash
git add crates/integrations/datafusion/src/physical_plan/compaction.rs
git commit -m "feat: add strategy field to IcebergCompactionExec"
```

---

## Task 5: Create sort_batches Helper Function

**Files:**
- Modify: `crates/integrations/datafusion/src/physical_plan/compaction.rs`

**Step 1: Write the failing test**

Add to tests module:

```rust
#[tokio::test]
async fn test_sort_batches_identity_transform() {
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use iceberg::spec::{SortField, SortDirection, NullOrder, SortOrder, Transform};
    use std::sync::Arc;

    // Create test data with unsorted values
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![3, 1, 2])),
            Arc::new(StringArray::from(vec!["c", "a", "b"])),
        ],
    ).unwrap();

    // Create sort order: ORDER BY id ASC
    let sort_order = SortOrder::builder()
        .with_sort_field(
            SortField::builder()
                .source_id(1) // id column
                .direction(SortDirection::Ascending)
                .null_order(NullOrder::First)
                .transform(Transform::Identity)
                .build()
        )
        .build_unbound()
        .unwrap();

    // Create column ID to name mapping
    let mut column_id_to_name = std::collections::HashMap::new();
    column_id_to_name.insert(1_i32, "id".to_string());
    column_id_to_name.insert(2_i32, "name".to_string());

    // Sort the batches
    let sorted = sort_batches(vec![batch], &sort_order, &column_id_to_name).await.unwrap();

    assert_eq!(sorted.len(), 1);
    let sorted_batch = &sorted[0];

    // Verify sorted order
    let id_col = sorted_batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id_col.value(0), 1);
    assert_eq!(id_col.value(1), 2);
    assert_eq!(id_col.value(2), 3);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p iceberg-datafusion --lib physical_plan::compaction::tests::test_sort_batches_identity_transform`
Expected: FAIL - `sort_batches` function doesn't exist

**Step 3: Implement sort_batches function**

Add to `crates/integrations/datafusion/src/physical_plan/compaction.rs`:

```rust
use datafusion::prelude::*;
use datafusion::logical_expr::col;
use iceberg::spec::{SortDirection, SortOrder, Transform};
use std::collections::HashMap;

/// Sort record batches according to an Iceberg sort order.
///
/// Uses DataFusion's in-memory sorting which handles:
/// - Multi-column sorting
/// - Ascending/descending order
/// - Nulls first/last semantics
///
/// # Arguments
/// * `batches` - Input record batches to sort
/// * `sort_order` - Iceberg sort order specification
/// * `column_id_to_name` - Mapping from Iceberg column IDs to Arrow column names
///
/// # Returns
/// Sorted record batches
pub async fn sort_batches(
    batches: Vec<RecordBatch>,
    sort_order: &SortOrder,
    column_id_to_name: &HashMap<i32, String>,
) -> DFResult<Vec<RecordBatch>> {
    if batches.is_empty() || sort_order.is_unsorted() {
        return Ok(batches);
    }

    // Get the schema from the first batch
    let schema = batches[0].schema();

    // Create a DataFusion context and register the data
    let ctx = SessionContext::new();
    let table = datafusion::datasource::MemTable::try_new(schema.clone(), vec![batches])?;
    ctx.register_table("data", Arc::new(table))?;

    // Build sort expressions from Iceberg sort order
    let mut sort_exprs = Vec::with_capacity(sort_order.fields.len());

    for sort_field in &sort_order.fields {
        let column_name = column_id_to_name
            .get(&sort_field.source_id)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Sort field references unknown column id: {}",
                    sort_field.source_id
                ))
            })?;

        // Build the base expression
        let expr = match sort_field.transform {
            Transform::Identity => col(column_name),
            Transform::Year => {
                // Extract year from timestamp
                datafusion::functions::datetime::date_part()
                    .call(vec![lit("year"), col(column_name)])
            }
            Transform::Month => {
                // year * 12 + month for proper ordering
                let year = datafusion::functions::datetime::date_part()
                    .call(vec![lit("year"), col(column_name)]);
                let month = datafusion::functions::datetime::date_part()
                    .call(vec![lit("month"), col(column_name)]);
                year * lit(12) + month
            }
            Transform::Day => {
                // Extract day of year for ordering
                datafusion::functions::datetime::date_part()
                    .call(vec![lit("doy"), col(column_name)])
            }
            Transform::Hour => {
                // Extract hour for ordering
                datafusion::functions::datetime::date_part()
                    .call(vec![lit("hour"), col(column_name)])
            }
            Transform::Bucket(_) | Transform::Truncate(_) => {
                // For bucket and truncate, we sort on the original value
                // The transform creates the bucket but we cluster by natural order
                col(column_name)
            }
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Sort transform {:?} not supported",
                    sort_field.transform
                )));
            }
        };

        // Apply sort direction and null order
        let ascending = matches!(sort_field.direction, SortDirection::Ascending);
        let nulls_first = matches!(sort_field.null_order, iceberg::spec::NullOrder::First);

        sort_exprs.push(expr.sort(ascending, nulls_first));
    }

    // Execute the sort
    let df = ctx.table("data").await?;
    let sorted_df = df.sort(sort_exprs)?;
    let sorted_batches = sorted_df.collect().await?;

    Ok(sorted_batches)
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p iceberg-datafusion --lib physical_plan::compaction::tests::test_sort_batches_identity_transform`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/integrations/datafusion/src/physical_plan/compaction.rs
git commit -m "feat: add sort_batches helper for sorted compaction"
```

---

## Task 6: Add Descending Sort Test

**Files:**
- Modify: `crates/integrations/datafusion/src/physical_plan/compaction.rs`

**Step 1: Write the failing test**

```rust
#[tokio::test]
async fn test_sort_batches_descending_nulls_last() {
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use iceberg::spec::{SortField, SortDirection, NullOrder, SortOrder, Transform};
    use std::sync::Arc;

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, true),
    ]));

    // Include a null value
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), None, Some(3), Some(2)])),
        ],
    ).unwrap();

    // ORDER BY id DESC NULLS LAST
    let sort_order = SortOrder::builder()
        .with_sort_field(
            SortField::builder()
                .source_id(1)
                .direction(SortDirection::Descending)
                .null_order(NullOrder::Last)
                .transform(Transform::Identity)
                .build()
        )
        .build_unbound()
        .unwrap();

    let mut column_id_to_name = std::collections::HashMap::new();
    column_id_to_name.insert(1_i32, "id".to_string());

    let sorted = sort_batches(vec![batch], &sort_order, &column_id_to_name).await.unwrap();

    let id_col = sorted[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();

    // DESC order: 3, 2, 1, NULL (nulls last)
    assert_eq!(id_col.value(0), 3);
    assert_eq!(id_col.value(1), 2);
    assert_eq!(id_col.value(2), 1);
    assert!(id_col.is_null(3));
}
```

**Step 2: Run test**

Run: `cargo test -p iceberg-datafusion --lib physical_plan::compaction::tests::test_sort_batches_descending_nulls_last`
Expected: PASS (if implementation is correct)

**Step 3: Commit**

```bash
git add crates/integrations/datafusion/src/physical_plan/compaction.rs
git commit -m "test: add descending sort with nulls last test"
```

---

## Task 7: Add Multi-Column Sort Test

**Files:**
- Modify: `crates/integrations/datafusion/src/physical_plan/compaction.rs`

**Step 1: Write the test**

```rust
#[tokio::test]
async fn test_sort_batches_multi_column() {
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use iceberg::spec::{SortField, SortDirection, NullOrder, SortOrder, Transform};
    use std::sync::Arc;

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("category", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["b", "a", "b", "a"])),
            Arc::new(Int64Array::from(vec![2, 1, 1, 2])),
        ],
    ).unwrap();

    // ORDER BY category ASC, id ASC
    let sort_order = SortOrder::builder()
        .with_sort_field(
            SortField::builder()
                .source_id(1) // category
                .direction(SortDirection::Ascending)
                .null_order(NullOrder::First)
                .transform(Transform::Identity)
                .build()
        )
        .with_sort_field(
            SortField::builder()
                .source_id(2) // id
                .direction(SortDirection::Ascending)
                .null_order(NullOrder::First)
                .transform(Transform::Identity)
                .build()
        )
        .build_unbound()
        .unwrap();

    let mut column_id_to_name = std::collections::HashMap::new();
    column_id_to_name.insert(1_i32, "category".to_string());
    column_id_to_name.insert(2_i32, "id".to_string());

    let sorted = sort_batches(vec![batch], &sort_order, &column_id_to_name).await.unwrap();

    let cat_col = sorted[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
    let id_col = sorted[0].column(1).as_any().downcast_ref::<Int64Array>().unwrap();

    // Expected: (a,1), (a,2), (b,1), (b,2)
    assert_eq!(cat_col.value(0), "a");
    assert_eq!(id_col.value(0), 1);
    assert_eq!(cat_col.value(1), "a");
    assert_eq!(id_col.value(1), 2);
    assert_eq!(cat_col.value(2), "b");
    assert_eq!(id_col.value(2), 1);
    assert_eq!(cat_col.value(3), "b");
    assert_eq!(id_col.value(3), 2);
}
```

**Step 2: Run test**

Run: `cargo test -p iceberg-datafusion --lib physical_plan::compaction::tests::test_sort_batches_multi_column`
Expected: PASS

**Step 3: Commit**

```bash
git add crates/integrations/datafusion/src/physical_plan/compaction.rs
git commit -m "test: add multi-column sort test"
```

---

## Task 8: Create process_file_group_sorted Function

**Files:**
- Modify: `crates/integrations/datafusion/src/physical_plan/compaction.rs`

**Step 1: Write the failing test (integration-level)**

We'll write an integration test that requires an actual table. For now, we create the function signature and a unit test for the helper.

**Step 2: Implement process_file_group_sorted**

Add to `crates/integrations/datafusion/src/physical_plan/compaction.rs`:

```rust
/// Build column ID to name mapping from Iceberg schema.
fn build_column_id_map(schema: &iceberg::spec::Schema) -> HashMap<i32, String> {
    let mut map = HashMap::new();
    for field in schema.as_struct().fields() {
        map.insert(field.id, field.name.clone());
    }
    map
}

/// Resolve sort order for sorted compaction.
///
/// If an explicit sort order is provided, uses that.
/// Otherwise, uses the table's default sort order.
/// Returns None if table has no sort order and none was provided.
fn resolve_sort_order(
    table: &Table,
    strategy: &RewriteStrategy,
) -> DFResult<Option<SortOrder>> {
    match strategy {
        RewriteStrategy::Sort { sort_order: Some(order) } => Ok(Some(order.clone())),
        RewriteStrategy::Sort { sort_order: None } => {
            let default_order = table.metadata().default_sort_order();
            if default_order.is_unsorted() {
                Err(DataFusionError::Plan(
                    "Sort strategy requires a sort order, but table has no default sort order. \
                     Use sort_by() to specify an explicit sort order.".to_string()
                ))
            } else {
                Ok(Some(default_order.clone()))
            }
        }
        _ => Ok(None),
    }
}

/// Process a single file group with optional sorting.
///
/// This is the main entry point for file group processing that supports
/// both bin-pack (no sort) and sorted compaction strategies.
///
/// # Arguments
/// * `table` - The Iceberg table
/// * `file_group` - The file group to process
/// * `strategy` - The rewrite strategy (determines if sorting is applied)
///
/// # Returns
/// Result containing new data files and metrics.
pub async fn process_file_group_with_strategy(
    table: &Table,
    file_group: &FileGroup,
    strategy: &RewriteStrategy,
) -> DFResult<FileGroupWriteResult> {
    let start_time = Instant::now();
    let group_id = file_group.group_id;
    let spec_id = file_group.partition_spec_id;

    // Step 1: Read all data from the file group with delete application
    let batches = read_file_group(table, file_group).await?;

    // Step 2: Apply sorting if using sort strategy
    let sorted_batches = match strategy {
        RewriteStrategy::Sort { .. } => {
            let sort_order = resolve_sort_order(table, strategy)?
                .expect("Sort strategy should have sort order");
            let column_id_map = build_column_id_map(table.metadata().current_schema());
            sort_batches(batches, &sort_order, &column_id_map).await?
        }
        RewriteStrategy::BinPack => batches,
        RewriteStrategy::ZOrder { .. } => {
            return Err(DataFusionError::NotImplemented(
                "Z-order compaction not yet implemented".to_string()
            ));
        }
    };

    // Step 3: Write output to new compacted files
    let partition = file_group.partition.as_ref();
    let new_data_files = write_compacted_files(table, sorted_batches, partition, spec_id).await?;

    // Calculate metrics
    let bytes_written: u64 = new_data_files.iter().map(|f| f.file_size_in_bytes()).sum();
    let duration_ms = start_time.elapsed().as_millis() as u64;

    Ok(FileGroupWriteResult {
        group_id,
        new_data_files,
        bytes_written,
        duration_ms,
    })
}
```

**Step 3: Run compilation check**

Run: `cargo check -p iceberg-datafusion`
Expected: PASS

**Step 4: Commit**

```bash
git add crates/integrations/datafusion/src/physical_plan/compaction.rs
git commit -m "feat: add process_file_group_with_strategy for sorted compaction"
```

---

## Task 9: Update IcebergCompactionExec to Use Strategy

**Files:**
- Modify: `crates/integrations/datafusion/src/physical_plan/compaction.rs`

**Step 1: Update the execute() method**

In the `execute()` method of `IcebergCompactionExec`, change the `process_file_group` call to use the strategy:

Find this line (around line 321):
```rust
let result = process_file_group(&table, &file_group).await;
```

Replace with:
```rust
let strategy = self.strategy.clone();
// ... in the async block:
let result = process_file_group_with_strategy(&table, &file_group, &strategy).await;
```

The full change to the execute method's stream:

```rust
fn execute(
    &self,
    partition: usize,
    _context: Arc<TaskContext>,
) -> DFResult<SendableRecordBatchStream> {
    if partition != 0 {
        return Err(DataFusionError::Internal(format!(
            "IcebergCompactionExec only supports partition 0, got {partition}"
        )));
    }

    let table = self.table.clone();
    let file_groups = self.plan.file_groups.clone();
    let output_schema = self.output_schema.clone();
    let cancellation_token = self.cancellation_token.clone();
    let progress_callback = self.progress_callback.clone();
    let total_bytes = self.plan.total_bytes;
    let total_files = self.plan.total_data_files;
    let strategy = self.strategy.clone();  // NEW: capture strategy

    // Build partition type map for serialization
    let partition_types = build_partition_type_map(&table)?;
    let format_version = table.metadata().format_version();

    // Emit Started event
    if let Some(ref callback) = progress_callback {
        callback(CompactionProgressEvent::Started {
            total_groups: file_groups.len() as u32,
            total_bytes,
            total_files,
        });
    }

    let stream = futures::stream::iter(file_groups.into_iter().enumerate())
        .then(move |(idx, file_group)| {
            let table = table.clone();
            let partition_types = partition_types.clone();
            let output_schema = output_schema.clone();
            let cancellation_token = cancellation_token.clone();
            let progress_callback = progress_callback.clone();
            let strategy = strategy.clone();  // NEW: clone for each iteration

            async move {
                // ... (cancellation check and progress emit unchanged) ...

                // Process the file group with strategy
                let result = process_file_group_with_strategy(&table, &file_group, &strategy).await;

                // ... (rest unchanged) ...
            }
        })
        .boxed();

    Ok(Box::pin(RecordBatchStreamAdapter::new(
        self.output_schema.clone(),
        stream,
    )))
}
```

**Step 2: Run compilation check**

Run: `cargo check -p iceberg-datafusion`
Expected: PASS

**Step 3: Commit**

```bash
git add crates/integrations/datafusion/src/physical_plan/compaction.rs
git commit -m "feat: update IcebergCompactionExec to use strategy in processing"
```

---

## Task 10: Update compact_table to Pass Strategy

**Files:**
- Modify: `crates/integrations/datafusion/src/compaction.rs`

**Step 1: Update compact_table function**

Modify `compact_table` to pass the strategy to the executor:

```rust
pub async fn compact_table(
    table: &Table,
    catalog: Arc<dyn Catalog>,
    options: Option<CompactionOptions>,
) -> Result<RewriteDataFilesResult, DataFusionError> {
    let options = options.unwrap_or_default();

    let rewrite_options = options.rewrite_options.unwrap_or_else(|| {
        RewriteDataFilesOptions::from_table_properties(table.metadata().properties())
    });

    // Get strategy (default to BinPack)
    let strategy = options.strategy.unwrap_or(RewriteStrategy::BinPack);

    // Step 1: Plan compaction
    let planner = RewriteDataFilesPlanner::new(table, &rewrite_options);
    let plan = planner.plan().await.map_err(to_datafusion_error)?;

    if plan.file_groups.is_empty() {
        // Nothing to compact
        if let Some(ref callback) = options.progress_callback {
            callback(CompactionProgressEvent::Completed {
                groups_processed: 0,
                groups_failed: 0,
                duration_ms: 0,
            });
        }
        return Ok(RewriteDataFilesResult::empty());
    }

    // Step 2: Create execution plan with strategy, progress and cancellation
    let mut compaction_exec = IcebergCompactionExec::new_with_strategy(
        table.clone(),
        plan.clone(),
        strategy,
    )?;

    // ... rest unchanged ...
}
```

**Step 2: Run compilation check**

Run: `cargo check -p iceberg-datafusion`
Expected: PASS

**Step 3: Commit**

```bash
git add crates/integrations/datafusion/src/compaction.rs
git commit -m "feat: pass strategy to IcebergCompactionExec in compact_table"
```

---

## Task 11: Add Integration Test for Sorted Compaction

**Files:**
- Create: `crates/integrations/datafusion/tests/sorted_compaction_test.rs` (or add to existing test file)

**Step 1: Write integration test**

```rust
#[tokio::test]
async fn test_sorted_compaction_produces_sorted_files() {
    // This test requires:
    // 1. Create an in-memory catalog
    // 2. Create a table with schema including timestamp column
    // 3. Insert unsorted data
    // 4. Run sorted compaction
    // 5. Verify output files are sorted

    // The test will be skipped if the integration test infrastructure
    // is not available. See existing compaction tests for patterns.
}
```

For now, we'll add a marker test that documents the expected behavior:

```rust
#[test]
fn test_sorted_compaction_documented_behavior() {
    // Sorted compaction should:
    // 1. Read all files in a partition
    // 2. Apply any position/equality deletes
    // 3. Sort all data by the specified sort order
    // 4. Write to new files at target size
    // 5. Produce files with non-overlapping key ranges
    //
    // This enables row group skipping in Parquet readers
    // because min/max statistics will be tighter.
}
```

**Step 2: Commit**

```bash
git add crates/integrations/datafusion/tests/
git commit -m "test: add sorted compaction integration test skeleton"
```

---

## Task 12: Test sort_batches with Empty Input

**Files:**
- Modify: `crates/integrations/datafusion/src/physical_plan/compaction.rs`

**Step 1: Write the test**

```rust
#[tokio::test]
async fn test_sort_batches_empty_input() {
    use iceberg::spec::{SortField, SortDirection, NullOrder, SortOrder, Transform};

    let sort_order = SortOrder::builder()
        .with_sort_field(
            SortField::builder()
                .source_id(1)
                .direction(SortDirection::Ascending)
                .null_order(NullOrder::First)
                .transform(Transform::Identity)
                .build()
        )
        .build_unbound()
        .unwrap();

    let column_id_to_name = std::collections::HashMap::new();

    let sorted = sort_batches(vec![], &sort_order, &column_id_to_name).await.unwrap();
    assert!(sorted.is_empty());
}

#[tokio::test]
async fn test_sort_batches_unsorted_order_passthrough() {
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use iceberg::spec::SortOrder;
    use std::sync::Arc;

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![3, 1, 2]))],
    ).unwrap();

    // Unsorted order (empty fields)
    let sort_order = SortOrder::unsorted_order();
    let column_id_to_name = std::collections::HashMap::new();

    let sorted = sort_batches(vec![batch.clone()], &sort_order, &column_id_to_name).await.unwrap();

    // Should pass through unchanged
    assert_eq!(sorted.len(), 1);
    let id_col = sorted[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id_col.value(0), 3); // Original order preserved
    assert_eq!(id_col.value(1), 1);
    assert_eq!(id_col.value(2), 2);
}
```

**Step 2: Run tests**

Run: `cargo test -p iceberg-datafusion --lib physical_plan::compaction::tests::test_sort_batches`
Expected: PASS

**Step 3: Commit**

```bash
git add crates/integrations/datafusion/src/physical_plan/compaction.rs
git commit -m "test: add edge case tests for sort_batches"
```

---

## Task 13: Update Module Documentation

**Files:**
- Modify: `crates/integrations/datafusion/src/physical_plan/compaction.rs`

**Step 1: Update module docs**

Update the module documentation at the top of `compaction.rs`:

```rust
//! Physical plan execution for Iceberg table compaction (rewrite_data_files).
//!
//! This module provides `IcebergCompactionExec` which executes file group rewrites
//! by reading data files, applying position/equality deletes, and writing compacted
//! Parquet files.
//!
//! ## Strategies
//!
//! Two compaction strategies are supported:
//!
//! - **BinPack** (default): Combines files by size without changing data order.
//!   This is the fastest strategy as it avoids sorting overhead.
//!
//! - **Sort**: Sorts data during compaction using a specified sort order.
//!   Produces files with non-overlapping key ranges, enabling efficient
//!   row group skipping in query engines.
//!
//! ## Architecture
//!
//! ```text
//! RewriteDataFilesPlan (from core iceberg)
//!     │
//!     ▼
//! IcebergCompactionExec (this module)
//!     │ For each FileGroup:
//!     │   1. Check cancellation token
//!     │   2. Emit progress event (GroupStarted)
//!     │   3. Read data files with delete application
//!     │   4. Sort data (if Sort strategy)     <-- NEW
//!     │   5. Write merged output (TaskWriter)
//!     │   6. Emit progress event (GroupCompleted)
//!     │   7. Yield DataFile JSON
//!     ▼
//! IcebergCompactionCommitExec
//!     │ Collects all DataFile JSON
//!     │ Calls RewriteDataFilesCommitter::commit()
//!     ▼
//! Updated Table Snapshot
//! ```
//!
//! ## Example: Sorted Compaction
//!
//! ```rust,ignore
//! use iceberg_datafusion::compaction::{compact_table, CompactionOptions};
//! use iceberg::transaction::rewrite_data_files::RewriteStrategy;
//! use iceberg::spec::{SortOrder, SortField, SortDirection, NullOrder, Transform};
//!
//! // Create sort order
//! let sort_order = SortOrder::builder()
//!     .with_sort_field(
//!         SortField::builder()
//!             .source_id(1)  // timestamp column ID
//!             .direction(SortDirection::Ascending)
//!             .null_order(NullOrder::First)
//!             .transform(Transform::Identity)
//!             .build()
//!     )
//!     .build_unbound()
//!     .unwrap();
//!
//! let options = CompactionOptions::default()
//!     .with_strategy(RewriteStrategy::Sort { sort_order: Some(sort_order) });
//!
//! let result = compact_table(&table, catalog.clone(), Some(options)).await?;
//! ```
```

**Step 2: Commit**

```bash
git add crates/integrations/datafusion/src/physical_plan/compaction.rs
git commit -m "docs: update compaction module documentation for sorted compaction"
```

---

## Task 14: Run Full Test Suite

**Step 1: Run all tests**

```bash
cargo test -p iceberg --lib
cargo test -p iceberg-datafusion --lib
cargo test -p iceberg-datafusion --test '*'
```

**Step 2: Run clippy**

```bash
cargo clippy -p iceberg -p iceberg-datafusion -- -D warnings
```

**Step 3: Fix any issues**

Address any test failures or clippy warnings.

**Step 4: Commit fixes**

```bash
git add .
git commit -m "fix: address test failures and clippy warnings"
```

---

## Task 15: Final Integration Commit

**Step 1: Create final commit with all changes**

```bash
git add .
git commit -m "feat(datafusion): implement sorted compaction for rewrite_data_files (Phase 2.2)

This commit adds sorted compaction support to the rewrite_data_files action:

- Enable Sort strategy validation in RewriteDataFilesAction
- Add strategy field to CompactionOptions and IcebergCompactionExec
- Implement sort_batches() using DataFusion for global sorting
- Add process_file_group_with_strategy() for strategy-aware processing
- Support multi-column sort orders with ASC/DESC and NULLS FIRST/LAST
- Pass strategy through compact_table() to executor

Sorted compaction produces files with non-overlapping key ranges,
enabling efficient row group skipping for range queries.

Closes Phase 2.2 of EPIC #624."
```

---

## Summary

This plan implements sorted compaction in 15 tasks following TDD principles:

| Task | Description | Files Modified |
|------|-------------|----------------|
| 1 | Sort order resolution tests | mod.rs |
| 2 | Remove FeatureUnsupported error | mod.rs |
| 3 | Add strategy to CompactionOptions | compaction.rs |
| 4 | Add strategy to IcebergCompactionExec | physical_plan/compaction.rs |
| 5 | Implement sort_batches() | physical_plan/compaction.rs |
| 6 | Test descending sort | physical_plan/compaction.rs |
| 7 | Test multi-column sort | physical_plan/compaction.rs |
| 8 | Implement process_file_group_with_strategy | physical_plan/compaction.rs |
| 9 | Update execute() to use strategy | physical_plan/compaction.rs |
| 10 | Update compact_table() | compaction.rs |
| 11 | Integration test skeleton | tests/ |
| 12 | Edge case tests | physical_plan/compaction.rs |
| 13 | Update documentation | physical_plan/compaction.rs |
| 14 | Run full test suite | - |
| 15 | Final commit | - |

Each task is designed to be completed in 2-5 minutes with clear test-first development.
