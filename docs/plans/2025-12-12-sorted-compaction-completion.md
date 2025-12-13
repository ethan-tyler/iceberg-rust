# Sorted Compaction Completion Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Complete the two missing acceptance criteria for sorted compaction: (1) update table sort order metadata after compaction, and (2) optimize row group boundaries for query skipping.

**Architecture:** The sort order metadata update will be integrated into the commit flow by extending `IcebergCompactionCommitExec` to optionally include `SetDefaultSortOrder` table updates when sorted compaction is used. Row group optimization will be implemented by configuring `WriterProperties` with appropriate row group size based on a new configuration option.

**Tech Stack:** Rust, Apache Iceberg spec, Parquet WriterProperties, iceberg-rust Transaction API

---

## Task 1: Add Sort Order to IcebergCompactionCommitExec

**Files:**
- Modify: `crates/integrations/datafusion/src/physical_plan/compaction_commit.rs`

**Step 1: Add sort_order field to IcebergCompactionCommitExec**

```rust
// In struct IcebergCompactionCommitExec, after plan field (around line 74):
    /// Optional sort order to set as table default after commit.
    /// Used when sorted compaction should update the table's sort order metadata.
    sort_order_for_update: Option<SortOrder>,
```

**Step 2: Add import for SortOrder**

At the top of the file, add to the iceberg imports:
```rust
use iceberg::spec::SortOrder;
```

**Step 3: Update the new() function signature and implementation**

```rust
    pub fn new(
        table: Table,
        catalog: Arc<dyn Catalog>,
        plan: RewriteDataFilesPlan,
        input: Arc<dyn ExecutionPlan>,
        sort_order_for_update: Option<SortOrder>,
    ) -> DFResult<Self> {
        let output_schema = compaction_commit_output_schema();
        let plan_properties = Self::compute_properties(output_schema.clone());

        Ok(Self {
            table,
            catalog,
            plan,
            input,
            output_schema,
            plan_properties,
            sort_order_for_update,
        })
    }
```

**Step 4: Update with_new_children to preserve sort_order_for_update**

```rust
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "IcebergCompactionCommitExec requires exactly one child".to_string(),
            ));
        }

        Ok(Arc::new(Self {
            table: self.table.clone(),
            catalog: self.catalog.clone(),
            plan: self.plan.clone(),
            input: children[0].clone(),
            output_schema: self.output_schema.clone(),
            plan_properties: self.plan_properties.clone(),
            sort_order_for_update: self.sort_order_for_update.clone(),
        }))
    }
```

**Step 5: Run cargo check to verify syntax**

Run: `cargo check -p iceberg-datafusion`
Expected: Compilation errors about sort_order_for_update not used yet (OK for now)

---

## Task 2: Integrate Sort Order Update into Commit Stream

**Files:**
- Modify: `crates/integrations/datafusion/src/physical_plan/compaction_commit.rs`

**Step 1: Add imports for sort order table updates**

```rust
use iceberg::{TableRequirement, TableUpdate};
```

**Step 2: Modify the commit stream to include sort order updates**

In the `execute()` function, after the successful catalog commit (around line 322), add logic to commit the sort order if provided:

```rust
            // Apply the commit to the catalog (this is the actual catalog write)
            action_commit
                .commit_to_catalog(table_ident.clone(), catalog.as_ref())
                .await
                .map_err(to_datafusion_error)?;

            // If a sort order update was requested, commit it separately
            // This is done after the data commit succeeds to avoid orphan files
            // if the sort order update fails
            if let Some(ref sort_order) = sort_order_for_update {
                let sort_order_updates = vec![
                    TableUpdate::AddSortOrder {
                        sort_order: sort_order.clone(),
                    },
                    TableUpdate::SetDefaultSortOrder { sort_order_id: -1 },
                ];
                let sort_order_requirements = vec![
                    TableRequirement::DefaultSortOrderIdMatch {
                        default_sort_order_id: table.metadata().default_sort_order().order_id,
                    },
                ];

                catalog
                    .update_table(iceberg::table::TableCommit::new(
                        table_ident,
                        sort_order_requirements,
                        sort_order_updates,
                    ))
                    .await
                    .map_err(to_datafusion_error)?;
            }
```

Note: The variable `sort_order_for_update` needs to be cloned into the async block. Update the stream creation to capture it:

```rust
        let sort_order_for_update = self.sort_order_for_update.clone();

        // Create single-item stream that processes all input and commits
        let stream = futures::stream::once(async move {
            // ... existing code ...
```

**Step 3: Run cargo check**

Run: `cargo check -p iceberg-datafusion`
Expected: PASS (or errors only about callers needing to be updated)

---

## Task 3: Update compact_table to Pass Sort Order

**Files:**
- Modify: `crates/integrations/datafusion/src/compaction.rs`

**Step 1: Extract sort order from strategy for commit**

In the `compact_table` function, after creating the compaction exec (around line 194), determine the sort order to update:

```rust
    // Determine if we should update the table's sort order after commit
    let sort_order_for_update = match &strategy {
        RewriteStrategy::Sort { sort_order } => {
            // Use provided sort order, or fall back to table's default
            sort_order.clone().or_else(|| {
                let default = table.metadata().default_sort_order();
                if default.is_unsorted() {
                    None  // Don't update if already unsorted and no custom order
                } else {
                    Some(default.as_ref().clone())
                }
            })
        }
        _ => None, // BinPack and ZOrder don't update sort order
    };
```

**Step 2: Pass sort_order_for_update to IcebergCompactionCommitExec**

Update the constructor call (around line 195):

```rust
    let commit_exec = Arc::new(IcebergCompactionCommitExec::new(
        table.clone(),
        catalog,
        plan,
        compaction_exec,
        sort_order_for_update,
    )?);
```

**Step 3: Run cargo check**

Run: `cargo check -p iceberg-datafusion`
Expected: PASS

**Step 4: Commit changes**

```bash
git add crates/integrations/datafusion/src/physical_plan/compaction_commit.rs crates/integrations/datafusion/src/compaction.rs
git commit -m "feat(compaction): update table sort order metadata after sorted compaction"
```

---

## Task 4: Add Row Group Size Configuration to CompactionOptions

**Files:**
- Modify: `crates/integrations/datafusion/src/compaction.rs`

**Step 1: Add row_group_size field to CompactionOptions**

```rust
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
    /// Target row group size for Parquet files (bytes).
    /// Smaller row groups enable better predicate pushdown but increase metadata overhead.
    /// Default: None (uses Parquet default of ~128MB)
    pub row_group_size: Option<usize>,
}
```

**Step 2: Add builder method**

```rust
    /// Set target row group size for Parquet files.
    ///
    /// Smaller row groups enable better predicate pushdown for sorted data
    /// but increase file metadata overhead. Recommended range: 8MB-64MB for
    /// sorted compaction.
    #[must_use]
    pub fn with_row_group_size(mut self, size_bytes: usize) -> Self {
        self.row_group_size = Some(size_bytes);
        self
    }
```

**Step 3: Run cargo check**

Run: `cargo check -p iceberg-datafusion`
Expected: PASS (warning about unused field is OK)

---

## Task 5: Wire Row Group Size Through to IcebergCompactionExec

**Files:**
- Modify: `crates/integrations/datafusion/src/physical_plan/compaction.rs`

**Step 1: Add row_group_size field to IcebergCompactionExec struct**

After the strategy field (around line 147):
```rust
    /// Target row group size for Parquet files.
    row_group_size: Option<usize>,
```

**Step 2: Update new_with_strategy to accept row_group_size**

```rust
    pub fn new_with_strategy(
        table: Table,
        plan: RewriteDataFilesPlan,
        strategy: RewriteStrategy,
        row_group_size: Option<usize>,
    ) -> DFResult<Self> {
        // ... existing code ...
        Ok(Self {
            table,
            plan,
            output_schema,
            plan_properties,
            cancellation_token: None,
            progress_callback: None,
            strategy,
            row_group_size,
        })
    }
```

**Step 3: Update the new() function too**

```rust
    pub fn new(table: Table, plan: RewriteDataFilesPlan) -> DFResult<Self> {
        Self::new_with_strategy(table, plan, RewriteStrategy::BinPack, None)
    }
```

**Step 4: Pass row_group_size through to process_file_group_with_strategy**

In the execute() stream (around line 382), update the call:

```rust
                    let result =
                        process_file_group_with_strategy(&table, &file_group, &strategy, row_group_size).await;
```

And capture it in the closure:
```rust
                let row_group_size = self.row_group_size;
```

**Step 5: Run cargo check**

Run: `cargo check -p iceberg-datafusion`
Expected: Errors about process_file_group_with_strategy signature (fix in next task)

---

## Task 6: Update process_file_group_with_strategy to Use Row Group Size

**Files:**
- Modify: `crates/integrations/datafusion/src/physical_plan/compaction.rs`

**Step 1: Update function signature**

```rust
async fn process_file_group_with_strategy(
    table: &Table,
    file_group: &FileGroup,
    strategy: &RewriteStrategy,
    row_group_size: Option<usize>,
) -> Result<WriteResult, DataFusionError> {
```

**Step 2: Pass row_group_size to write_data_files**

In the function body, update the write_data_files call (around line 595):

```rust
    // Write new files
    write_data_files(
        table,
        sorted_batches,
        file_group.partition_spec_id,
        file_group.partition_value.clone(),
        row_group_size,
    )
    .await
```

**Step 3: Update write_data_files signature and implementation**

```rust
async fn write_data_files(
    table: &Table,
    batches: Vec<RecordBatch>,
    partition_spec_id: i32,
    partition_value: Option<Struct>,
    row_group_size: Option<usize>,
) -> Result<WriteResult, DataFusionError> {
    // ... existing setup code ...

    // Build writer properties with optional row group size
    let writer_props = match row_group_size {
        Some(size) => WriterProperties::builder()
            .set_max_row_group_size(size)
            .build(),
        None => WriterProperties::default(),
    };

    // Build writer stack: Parquet -> Rolling -> DataFile
    let parquet_writer_builder =
        ParquetWriterBuilder::new(writer_props, iceberg_schema.clone());
    // ... rest of existing code ...
```

**Step 4: Run cargo check**

Run: `cargo check -p iceberg-datafusion`
Expected: PASS

---

## Task 7: Update compact_table to Pass Row Group Size

**Files:**
- Modify: `crates/integrations/datafusion/src/compaction.rs`

**Step 1: Extract row_group_size from options and pass to executor**

Around line 184, update the IcebergCompactionExec creation:

```rust
    // Step 2: Create execution plan with strategy, progress and cancellation
    let row_group_size = options.row_group_size;
    let mut compaction_exec =
        IcebergCompactionExec::new_with_strategy(table.clone(), plan.clone(), strategy, row_group_size)?;
```

**Step 2: Run cargo check**

Run: `cargo check -p iceberg-datafusion`
Expected: PASS

**Step 3: Commit changes**

```bash
git add crates/integrations/datafusion/src/compaction.rs crates/integrations/datafusion/src/physical_plan/compaction.rs
git commit -m "feat(compaction): add configurable row group size for better predicate pushdown"
```

---

## Task 8: Add Unit Test for Sort Order Update

**Files:**
- Modify: `crates/integrations/datafusion/src/compaction.rs`

**Step 1: Add test for sort order extraction logic**

```rust
    #[test]
    fn test_compaction_options_with_row_group_size() {
        let options = CompactionOptions::default()
            .with_row_group_size(8 * 1024 * 1024); // 8MB

        assert_eq!(options.row_group_size, Some(8 * 1024 * 1024));
    }
```

**Step 2: Run test**

Run: `cargo test -p iceberg-datafusion compaction_options_with_row_group_size`
Expected: PASS

---

## Task 9: Add Integration Test Placeholder for Sort Order Update

**Files:**
- Modify: `crates/integrations/datafusion/src/compaction.rs`

**Step 1: Update the existing ignored integration test with sort order verification**

```rust
    #[test]
    #[ignore = "TODO: requires catalog/table infrastructure for end-to-end test"]
    fn test_sorted_compaction_updates_table_sort_order() {
        // TODO: Full integration test for sorted compaction sort order update
        //
        // Test should:
        // 1. Create an in-memory catalog and table WITHOUT a sort order
        // 2. Insert unsorted data across multiple small files
        // 3. Run sorted compaction with explicit sort order via compact_table()
        // 4. Verify table.metadata().default_sort_order() matches the provided order
        // 5. Verify output files are sorted by reading back
        //
        // This verifies acceptance criteria:
        // "Updates table sort order metadata"
        panic!("Integration test not yet implemented")
    }
```

**Step 2: Run cargo test to verify compilation**

Run: `cargo test -p iceberg-datafusion --no-run`
Expected: PASS (compilation)

---

## Task 10: Update Documentation

**Files:**
- Modify: `crates/integrations/datafusion/src/compaction.rs`

**Step 1: Update module-level documentation**

Add to the module docs (around line 30):

```rust
//! ## Sort Order Metadata Update
//!
//! When using `RewriteStrategy::Sort`, the compaction will update the table's
//! default sort order metadata to match the sort order used during compaction.
//! This allows readers to leverage sort order information for query optimization.
//!
//! ## Row Group Configuration
//!
//! For sorted compaction, smaller row groups can improve predicate pushdown
//! effectiveness. Use `CompactionOptions::with_row_group_size()` to configure:
//!
//! ```rust,ignore
//! let options = CompactionOptions::default()
//!     .with_strategy(RewriteStrategy::Sort { sort_order: None })
//!     .with_row_group_size(8 * 1024 * 1024); // 8MB row groups
//! ```
```

**Step 2: Run cargo doc to verify**

Run: `cargo doc -p iceberg-datafusion --no-deps`
Expected: PASS

**Step 3: Commit all changes**

```bash
git add .
git commit -m "docs(compaction): document sort order metadata update and row group configuration"
```

---

## Task 11: Run Full Test Suite

**Step 1: Run iceberg tests**

Run: `cargo test -p iceberg`
Expected: All tests pass

**Step 2: Run datafusion integration tests**

Run: `cargo test -p iceberg-datafusion`
Expected: All tests pass

**Step 3: Run clippy**

Run: `cargo clippy -p iceberg-datafusion -- -D warnings`
Expected: No warnings

---

## Summary

This plan addresses the two missing acceptance criteria:

1. **Updates table sort order metadata** - Implemented by:
   - Extending `IcebergCompactionCommitExec` with optional `sort_order_for_update`
   - Adding `TableUpdate::AddSortOrder` and `TableUpdate::SetDefaultSortOrder` to commit
   - Extracting sort order from `RewriteStrategy::Sort` in `compact_table()`

2. **Enables row group skipping** - Improved by:
   - Adding `row_group_size` option to `CompactionOptions`
   - Configuring `WriterProperties::set_max_row_group_size()` in the writer
   - Smaller row groups = better predicate pushdown for sorted data

Note: True row group boundary alignment (ensuring row groups break at sort key boundaries) would require more complex logic in the Parquet writer. The current implementation uses a size-based approach which provides a reasonable approximation.
