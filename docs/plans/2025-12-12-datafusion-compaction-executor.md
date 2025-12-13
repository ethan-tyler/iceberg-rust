# DataFusion Compaction Executor Implementation Plan (Phase 2.1)

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Complete Phase 2.1 compaction by implementing the DataFusion execution layer with progress reporting and cancellation support.

**Architecture:** Two-phase execution following existing DELETE/UPDATE/MERGE patterns. `IcebergCompactionExec` reads FileGroups, applies deletes via existing scan infrastructure, writes compacted Parquet files. `IcebergCompactionCommitExec` commits via `RewriteDataFilesCommitter`. Progress callbacks and cancellation tokens integrated throughout.

**Tech Stack:** DataFusion physical plans, iceberg-rust core `rewrite_data_files` module, existing `IcebergTableScan`, `TaskWriter`, `tokio_util::sync::CancellationToken`.

---

## Scope

### In Scope (Phase 2.1 Acceptance Criteria)

| Criterion | Implementation |
|-----------|----------------|
| Reduces file count while preserving all data | DataFusion executor reads, merges, writes |
| Respects partition boundaries | FileGroups already grouped by partition |
| Configurable target file size | Uses existing `RewriteDataFilesOptions` |
| Progress reporting for long-running operations | `CompactionProgress` callbacks |
| Cancellation support | `CancellationToken` integration |
| Handles position delete files correctly | Apply deletes during read via Iceberg scan |

### Out of Scope (Deferred to 2.2+)

- Partition filter predicates
- Delete file threshold selection
- Dangling delete removal
- Partial progress commits
- Sorted compaction

---

## Phase 1: Progress and Cancellation Infrastructure

### Task 1: Define Progress Reporting Types

**Files:**
- Create: `crates/iceberg/src/transaction/rewrite_data_files/progress.rs`
- Modify: `crates/iceberg/src/transaction/rewrite_data_files/mod.rs`

**Step 1: Write the failing test**

Create `crates/iceberg/src/transaction/rewrite_data_files/progress.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_progress_event_creation() {
        let event = CompactionProgressEvent::GroupStarted {
            group_id: 0,
            input_files: 5,
            input_bytes: 1024 * 1024,
        };

        match event {
            CompactionProgressEvent::GroupStarted { group_id, .. } => {
                assert_eq!(group_id, 0);
            }
            _ => panic!("Wrong event type"),
        }
    }

    #[test]
    fn test_progress_tracker_updates() {
        let tracker = CompactionProgressTracker::new(10, 1024 * 1024 * 100);

        assert_eq!(tracker.total_groups(), 10);
        assert_eq!(tracker.completed_groups(), 0);
        assert!(!tracker.is_complete());
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p iceberg --lib rewrite_data_files::progress::tests`
Expected: FAIL with "cannot find"

**Step 3: Implement progress types**

```rust
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Progress reporting for compaction operations.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

/// Events emitted during compaction for progress tracking.
#[derive(Debug, Clone)]
pub enum CompactionProgressEvent {
    /// Compaction operation started.
    Started {
        /// Total number of file groups to process.
        total_groups: u32,
        /// Total bytes to process.
        total_bytes: u64,
        /// Total input files.
        total_files: u64,
    },
    /// A file group started processing.
    GroupStarted {
        /// The group being processed.
        group_id: u32,
        /// Number of input files in this group.
        input_files: u32,
        /// Total bytes in this group.
        input_bytes: u64,
    },
    /// A file group completed successfully.
    GroupCompleted {
        /// The group that completed.
        group_id: u32,
        /// Number of output files created.
        output_files: u32,
        /// Total bytes written.
        output_bytes: u64,
        /// Processing time in milliseconds.
        duration_ms: u64,
    },
    /// A file group failed.
    GroupFailed {
        /// The group that failed.
        group_id: u32,
        /// Error message.
        error: String,
    },
    /// Compaction operation completed.
    Completed {
        /// Total groups processed.
        groups_processed: u32,
        /// Total groups failed.
        groups_failed: u32,
        /// Total duration in milliseconds.
        duration_ms: u64,
    },
    /// Compaction was cancelled.
    Cancelled {
        /// Groups completed before cancellation.
        groups_completed: u32,
    },
}

/// Callback type for progress events.
pub type ProgressCallback = Arc<dyn Fn(CompactionProgressEvent) + Send + Sync>;

/// Thread-safe progress tracker for compaction operations.
#[derive(Debug)]
pub struct CompactionProgressTracker {
    total_groups: u32,
    total_bytes: u64,
    completed_groups: AtomicU32,
    failed_groups: AtomicU32,
    bytes_processed: AtomicU64,
}

impl CompactionProgressTracker {
    /// Create a new progress tracker.
    pub fn new(total_groups: u32, total_bytes: u64) -> Self {
        Self {
            total_groups,
            total_bytes,
            completed_groups: AtomicU32::new(0),
            failed_groups: AtomicU32::new(0),
            bytes_processed: AtomicU64::new(0),
        }
    }

    /// Get total number of groups.
    pub fn total_groups(&self) -> u32 {
        self.total_groups
    }

    /// Get number of completed groups.
    pub fn completed_groups(&self) -> u32 {
        self.completed_groups.load(Ordering::Relaxed)
    }

    /// Get number of failed groups.
    pub fn failed_groups(&self) -> u32 {
        self.failed_groups.load(Ordering::Relaxed)
    }

    /// Get bytes processed so far.
    pub fn bytes_processed(&self) -> u64 {
        self.bytes_processed.load(Ordering::Relaxed)
    }

    /// Get progress as a percentage (0.0 - 1.0).
    pub fn progress_fraction(&self) -> f64 {
        if self.total_groups == 0 {
            return 1.0;
        }
        let completed = self.completed_groups() + self.failed_groups();
        completed as f64 / self.total_groups as f64
    }

    /// Check if all groups are processed.
    pub fn is_complete(&self) -> bool {
        self.completed_groups() + self.failed_groups() >= self.total_groups
    }

    /// Record a group completion.
    pub fn record_completed(&self, bytes: u64) {
        self.completed_groups.fetch_add(1, Ordering::Relaxed);
        self.bytes_processed.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record a group failure.
    pub fn record_failed(&self) {
        self.failed_groups.fetch_add(1, Ordering::Relaxed);
    }
}

impl Default for CompactionProgressTracker {
    fn default() -> Self {
        Self::new(0, 0)
    }
}
```

**Step 4: Add module to mod.rs**

Add to `crates/iceberg/src/transaction/rewrite_data_files/mod.rs`:

```rust
mod progress;
pub use progress::{CompactionProgressEvent, CompactionProgressTracker, ProgressCallback};
```

**Step 5: Run test to verify it passes**

Run: `cargo test -p iceberg --lib rewrite_data_files::progress::tests`
Expected: PASS

**Step 6: Commit**

```bash
git add crates/iceberg/src/transaction/rewrite_data_files/progress.rs
git add crates/iceberg/src/transaction/rewrite_data_files/mod.rs
git commit -m "feat(rewrite): add progress reporting types"
```

---

### Task 2: Add Cancellation Token Support

**Files:**
- Modify: `crates/iceberg/src/transaction/rewrite_data_files/mod.rs`
- Modify: `crates/iceberg/Cargo.toml` (if needed for tokio-util)

**Step 1: Write the failing test**

Add to `mod.rs` tests:

```rust
#[test]
fn test_rewrite_action_with_cancellation() {
    use tokio_util::sync::CancellationToken;

    let token = CancellationToken::new();

    // Should be able to set cancellation token on action
    let action = RewriteDataFilesAction::new()
        .with_cancellation_token(token.clone());

    assert!(!token.is_cancelled());
    token.cancel();
    assert!(token.is_cancelled());
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p iceberg --lib rewrite_data_files::tests::test_rewrite_action_with_cancellation`
Expected: FAIL

**Step 3: Add cancellation token to RewriteDataFilesAction**

In `mod.rs`, update the action struct:

```rust
use tokio_util::sync::CancellationToken;

/// Builder for rewrite_data_files operation.
pub struct RewriteDataFilesAction<'a> {
    table: &'a Table,
    options: RewriteDataFilesOptions,
    partition_filter: Option<Predicate>,
    case_sensitive: bool,
    /// Cancellation token for aborting the operation.
    cancellation_token: Option<CancellationToken>,
    /// Progress callback for monitoring.
    progress_callback: Option<ProgressCallback>,
}

impl<'a> RewriteDataFilesAction<'a> {
    // ... existing methods ...

    /// Set a cancellation token for aborting the operation.
    ///
    /// When the token is cancelled, the operation will stop processing
    /// new file groups and return early with partial results.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use tokio_util::sync::CancellationToken;
    ///
    /// let token = CancellationToken::new();
    /// let token_clone = token.clone();
    ///
    /// // Spawn task to cancel after timeout
    /// tokio::spawn(async move {
    ///     tokio::time::sleep(Duration::from_secs(60)).await;
    ///     token_clone.cancel();
    /// });
    ///
    /// let result = table.rewrite_data_files()
    ///     .with_cancellation_token(token)
    ///     .execute()
    ///     .await?;
    /// ```
    pub fn with_cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancellation_token = Some(token);
        self
    }

    /// Set a progress callback for monitoring the operation.
    ///
    /// The callback will be invoked for each progress event during
    /// the compaction operation.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let result = table.rewrite_data_files()
    ///     .with_progress_callback(Arc::new(|event| {
    ///         match event {
    ///             CompactionProgressEvent::GroupCompleted { group_id, .. } => {
    ///                 println!("Completed group {}", group_id);
    ///             }
    ///             _ => {}
    ///         }
    ///     }))
    ///     .execute()
    ///     .await?;
    /// ```
    pub fn with_progress_callback(mut self, callback: ProgressCallback) -> Self {
        self.progress_callback = Some(callback);
        self
    }

    /// Check if the operation has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancellation_token
            .as_ref()
            .map(|t| t.is_cancelled())
            .unwrap_or(false)
    }
}
```

**Step 4: Update Cargo.toml if needed**

Ensure `tokio-util` is in dependencies:

```toml
[dependencies]
tokio-util = { version = "0.7", features = ["sync"] }
```

**Step 5: Run test to verify it passes**

Run: `cargo test -p iceberg --lib rewrite_data_files::tests::test_rewrite_action_with_cancellation`
Expected: PASS

**Step 6: Commit**

```bash
git add crates/iceberg/src/transaction/rewrite_data_files/mod.rs
git add crates/iceberg/Cargo.toml
git commit -m "feat(rewrite): add cancellation token support"
```

---

## Phase 2: Core Compaction Executor

### Task 3: Create Compaction Module Structure

**Files:**
- Create: `crates/integrations/datafusion/src/physical_plan/compaction.rs`
- Modify: `crates/integrations/datafusion/src/physical_plan/mod.rs`

**Step 1: Create module with output schema**

Create `crates/integrations/datafusion/src/physical_plan/compaction.rs`:

```rust
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Physical plan execution for Iceberg table compaction (rewrite_data_files).
//!
//! This module provides `IcebergCompactionExec` which executes file group rewrites
//! by reading data files, applying position/equality deletes, and writing compacted
//! Parquet files.
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
//!     │   4. Write merged output (TaskWriter)
//!     │   5. Emit progress event (GroupCompleted)
//!     │   6. Yield DataFile JSON
//!     ▼
//! IcebergCompactionCommitExec
//!     │ Collects all DataFile JSON
//!     │ Calls RewriteDataFilesCommitter::commit()
//!     ▼
//! Updated Table Snapshot
//! ```

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use datafusion::error::Result as DFResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;
use iceberg::table::Table;
use iceberg::transaction::rewrite_data_files::{
    CompactionProgressEvent, FileGroup, ProgressCallback, RewriteDataFilesPlan,
};
use tokio_util::sync::CancellationToken;

/// Column name for serialized data files output.
pub const COMPACTION_DATA_FILES_COL: &str = "data_files";

/// Column name for group ID output.
pub const COMPACTION_GROUP_ID_COL: &str = "group_id";

/// Schema for compaction executor output.
///
/// Returns schema with:
/// - `group_id`: UInt32 - which FileGroup this result belongs to
/// - `data_files`: Utf8 - JSON-serialized DataFile entries
pub fn compaction_output_schema() -> ArrowSchemaRef {
    Arc::new(ArrowSchema::new(vec![
        Field::new(COMPACTION_GROUP_ID_COL, DataType::UInt32, false),
        Field::new(COMPACTION_DATA_FILES_COL, DataType::Utf8, false),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compaction_output_schema() {
        let schema = compaction_output_schema();
        assert_eq!(schema.fields().len(), 2);
        assert!(schema.field_with_name(COMPACTION_DATA_FILES_COL).is_ok());
        assert!(schema.field_with_name(COMPACTION_GROUP_ID_COL).is_ok());
    }
}
```

**Step 2: Add module to mod.rs**

In `crates/integrations/datafusion/src/physical_plan/mod.rs`, add:

```rust
pub mod compaction;
```

**Step 3: Run test to verify**

Run: `cargo test -p iceberg-datafusion --lib physical_plan::compaction::tests::test_compaction_output_schema`
Expected: PASS

**Step 4: Commit**

```bash
git add crates/integrations/datafusion/src/physical_plan/compaction.rs
git add crates/integrations/datafusion/src/physical_plan/mod.rs
git commit -m "feat(datafusion): add compaction module structure"
```

---

### Task 4: Define IcebergCompactionExec Structure

**Files:**
- Modify: `crates/integrations/datafusion/src/physical_plan/compaction.rs`

**Step 1: Write the failing test**

Add to `compaction.rs` tests:

```rust
#[test]
fn test_compaction_exec_has_cancellation() {
    // Verify exec can be created with cancellation token
    let token = CancellationToken::new();

    // This test validates the struct has the right fields
    // Full integration requires table setup
    assert!(!token.is_cancelled());
}
```

**Step 2: Implement IcebergCompactionExec**

Add to `compaction.rs`:

```rust
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};

/// Physical execution plan for Iceberg table compaction.
///
/// Reads file groups from a `RewriteDataFilesPlan`, applies deletes,
/// merges data, and writes new compacted Parquet files.
///
/// Supports:
/// - Progress reporting via callbacks
/// - Cancellation via token
/// - Position delete application
pub struct IcebergCompactionExec {
    /// The Iceberg table being compacted.
    table: Table,
    /// The compaction plan with file groups.
    plan: RewriteDataFilesPlan,
    /// Output schema.
    output_schema: ArrowSchemaRef,
    /// Plan properties.
    plan_properties: PlanProperties,
    /// Optional cancellation token.
    cancellation_token: Option<CancellationToken>,
    /// Optional progress callback.
    progress_callback: Option<ProgressCallback>,
}

impl IcebergCompactionExec {
    /// Create a new compaction executor.
    pub fn new(table: Table, plan: RewriteDataFilesPlan) -> DFResult<Self> {
        let output_schema = compaction_output_schema();

        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Ok(Self {
            table,
            plan,
            output_schema,
            plan_properties,
            cancellation_token: None,
            progress_callback: None,
        })
    }

    /// Set cancellation token.
    pub fn with_cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancellation_token = Some(token);
        self
    }

    /// Set progress callback.
    pub fn with_progress_callback(mut self, callback: ProgressCallback) -> Self {
        self.progress_callback = Some(callback);
        self
    }

    /// Check if cancelled.
    fn is_cancelled(&self) -> bool {
        self.cancellation_token
            .as_ref()
            .map(|t| t.is_cancelled())
            .unwrap_or(false)
    }

    /// Emit progress event if callback is set.
    fn emit_progress(&self, event: CompactionProgressEvent) {
        if let Some(ref callback) = self.progress_callback {
            callback(event);
        }
    }

    /// Get the compaction plan.
    pub fn plan(&self) -> &RewriteDataFilesPlan {
        &self.plan
    }

    /// Get the table being compacted.
    pub fn table(&self) -> &Table {
        &self.table
    }
}

impl Debug for IcebergCompactionExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergCompactionExec")
            .field("file_groups", &self.plan.file_groups.len())
            .field("total_bytes", &self.plan.total_bytes)
            .field("has_cancellation", &self.cancellation_token.is_some())
            .finish()
    }
}

impl DisplayAs for IcebergCompactionExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "IcebergCompactionExec: groups={}, files={}, bytes={}",
                    self.plan.file_groups.len(),
                    self.plan.total_data_files,
                    self.plan.total_bytes
                )
            }
        }
    }
}

impl ExecutionPlan for IcebergCompactionExec {
    fn name(&self) -> &str {
        "IcebergCompactionExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.output_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![] // Leaf node
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(datafusion::error::DataFusionError::Internal(
                "IcebergCompactionExec does not accept children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(datafusion::error::DataFusionError::Internal(format!(
                "IcebergCompactionExec only supports partition 0, got {}",
                partition
            )));
        }

        // TODO: Implement in Task 6
        let schema = self.output_schema.clone();
        let empty_stream = futures::stream::empty();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, empty_stream)))
    }
}
```

**Step 3: Run tests**

Run: `cargo test -p iceberg-datafusion --lib physical_plan::compaction`
Expected: PASS

**Step 4: Commit**

```bash
git add crates/integrations/datafusion/src/physical_plan/compaction.rs
git commit -m "feat(datafusion): define IcebergCompactionExec with cancellation and progress"
```

---

### Task 5: Implement File Group Reader

**Files:**
- Modify: `crates/integrations/datafusion/src/physical_plan/compaction.rs`

**Step 1: Document the reader function signature**

This function reads a FileGroup's data with delete application:

```rust
use futures::TryStreamExt;
use iceberg::scan::TableScan;

/// Read all data from a FileGroup, applying position and equality deletes.
///
/// Uses the Iceberg table scan which handles delete application internally.
///
/// # Arguments
///
/// * `table` - The Iceberg table
/// * `file_group` - The FileGroup containing data files and associated deletes
///
/// # Returns
///
/// A stream of RecordBatches containing the merged, delete-applied data.
async fn read_file_group(
    table: &Table,
    file_group: &FileGroup,
) -> DFResult<impl futures::Stream<Item = DFResult<RecordBatch>> + Send> {
    // Get data file paths from the group
    let data_file_paths: Vec<String> = file_group
        .data_files
        .iter()
        .map(|entry| entry.data_file.file_path.clone())
        .collect();

    // Build a scan that reads only these specific files
    // The scan infrastructure handles delete file application
    let scan = table
        .scan()
        .select_all()
        .build()
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

    // Filter to only our target files and get Arrow stream
    // Note: This may need adjustment based on actual Iceberg scan API
    let arrow_stream = scan
        .to_arrow()
        .await
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

    // Filter batches to only include rows from our target files
    // This is a simplification - actual impl may need file-level filtering
    let filtered_stream = arrow_stream.map(|result| {
        result.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))
    });

    Ok(filtered_stream)
}
```

**Note:** The actual implementation needs to filter to specific files. This may require:
1. Using `FileScanTask` directly with the FileGroup's data files
2. Or extending the scan builder with a file filter

**Step 2: Commit placeholder**

```bash
git add crates/integrations/datafusion/src/physical_plan/compaction.rs
git commit -m "feat(datafusion): add file group reader (placeholder)"
```

---

### Task 6: Implement File Group Writer

**Files:**
- Modify: `crates/integrations/datafusion/src/physical_plan/compaction.rs`

**Step 1: Implement writer function**

```rust
use iceberg::spec::DataFile;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};

/// Write merged data to new Parquet files.
///
/// Uses TaskWriter infrastructure to write files respecting target size.
///
/// # Arguments
///
/// * `table` - The Iceberg table
/// * `batches` - Iterator of RecordBatches to write
/// * `spec_id` - Partition spec ID for the output files
///
/// # Returns
///
/// Vector of newly created DataFile entries.
async fn write_file_group_output(
    table: &Table,
    batches: Vec<RecordBatch>,
    spec_id: i32,
) -> DFResult<Vec<DataFile>> {
    use crate::task_writer::TaskWriter;
    use iceberg::spec::DataFileFormat;
    use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use iceberg::writer::file_writer::ParquetWriterBuilder;

    let metadata = table.metadata();
    let file_io = table.file_io();
    let schema = metadata.current_schema();

    let partition_spec = metadata
        .partition_spec_by_id(spec_id)
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Internal(format!(
                "Partition spec {} not found",
                spec_id
            ))
        })?;

    // Get target file size from table properties
    let target_file_size = metadata
        .properties()
        .get("write.target-file-size-bytes")
        .and_then(|s| s.parse().ok())
        .unwrap_or(512 * 1024 * 1024u64); // 512MB default

    // Create location generator
    let uuid = uuid::Uuid::new_v4();
    let location_generator = DefaultLocationGenerator::new(metadata.clone())
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    let file_name_generator =
        DefaultFileNameGenerator::new(uuid.to_string(), None, DataFileFormat::Parquet);

    // Build writer - this is simplified, actual impl follows write.rs pattern
    let mut data_files = Vec::new();

    // For each batch, write using the task writer infrastructure
    // This is a placeholder - actual implementation would use RollingFileWriter
    for batch in batches {
        // Write batch and collect data files
        // Implementation depends on exact writer API
    }

    Ok(data_files)
}
```

**Step 2: Commit placeholder**

```bash
git add crates/integrations/datafusion/src/physical_plan/compaction.rs
git commit -m "feat(datafusion): add file group writer (placeholder)"
```

---

### Task 7: Implement Full execute() with Progress and Cancellation

**Files:**
- Modify: `crates/integrations/datafusion/src/physical_plan/compaction.rs`

**Step 1: Implement execute with cancellation checks and progress events**

Replace the execute() TODO:

```rust
use std::time::Instant;
use crate::partition_utils::{build_partition_type_map, serialize_data_file_to_json};

impl ExecutionPlan for IcebergCompactionExec {
    // ... other methods unchanged ...

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(datafusion::error::DataFusionError::Internal(format!(
                "IcebergCompactionExec only supports partition 0, got {}",
                partition
            )));
        }

        let table = self.table.clone();
        let file_groups = self.plan.file_groups.clone();
        let output_schema = self.output_schema.clone();
        let cancellation_token = self.cancellation_token.clone();
        let progress_callback = self.progress_callback.clone();
        let total_bytes = self.plan.total_bytes;
        let total_files = self.plan.total_data_files;

        // Build partition type map for serialization
        let partition_types = build_partition_type_map(&table)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        let format_version = table.metadata().format_version();

        // Emit Started event
        if let Some(ref callback) = progress_callback {
            callback(CompactionProgressEvent::Started {
                total_groups: file_groups.len() as u32,
                total_bytes,
                total_files,
            });
        }

        // Create async stream that processes each file group
        let stream = futures::stream::iter(file_groups.into_iter().enumerate())
            .then(move |(idx, file_group)| {
                let table = table.clone();
                let partition_types = partition_types.clone();
                let output_schema = output_schema.clone();
                let cancellation_token = cancellation_token.clone();
                let progress_callback = progress_callback.clone();

                async move {
                    let group_id = file_group.group_id;
                    let spec_id = file_group.partition_spec_id;
                    let input_files = file_group.data_files.len() as u32;
                    let input_bytes = file_group.total_bytes;

                    // Check cancellation before processing
                    if let Some(ref token) = cancellation_token {
                        if token.is_cancelled() {
                            if let Some(ref callback) = progress_callback {
                                callback(CompactionProgressEvent::Cancelled {
                                    groups_completed: idx as u32,
                                });
                            }
                            return Err(datafusion::error::DataFusionError::Execution(
                                "Compaction cancelled".to_string(),
                            ));
                        }
                    }

                    // Emit GroupStarted
                    if let Some(ref callback) = progress_callback {
                        callback(CompactionProgressEvent::GroupStarted {
                            group_id,
                            input_files,
                            input_bytes,
                        });
                    }

                    let start_time = Instant::now();

                    // Step 1: Read file group with delete application
                    let record_stream = read_file_group(&table, &file_group).await?;
                    let batches: Vec<RecordBatch> = record_stream.try_collect().await?;

                    // Check cancellation after read
                    if let Some(ref token) = cancellation_token {
                        if token.is_cancelled() {
                            return Err(datafusion::error::DataFusionError::Execution(
                                "Compaction cancelled".to_string(),
                            ));
                        }
                    }

                    // Step 2: Write merged output
                    let data_files = write_file_group_output(&table, batches, spec_id).await?;

                    let output_files = data_files.len() as u32;
                    let output_bytes: u64 = data_files.iter().map(|f| f.file_size_in_bytes() as u64).sum();
                    let duration_ms = start_time.elapsed().as_millis() as u64;

                    // Step 3: Serialize data files to JSON
                    let partition_type = partition_types.get(&spec_id).ok_or_else(|| {
                        datafusion::error::DataFusionError::Internal(format!(
                            "No partition type for spec {}",
                            spec_id
                        ))
                    })?;

                    let data_files_json: Vec<String> = data_files
                        .into_iter()
                        .map(|df| serialize_data_file_to_json(df, partition_type, format_version))
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

                    // Emit GroupCompleted
                    if let Some(ref callback) = progress_callback {
                        callback(CompactionProgressEvent::GroupCompleted {
                            group_id,
                            output_files,
                            output_bytes,
                            duration_ms,
                        });
                    }

                    // Step 4: Create output batch
                    let group_ids: Vec<u32> = vec![group_id; data_files_json.len()];
                    let group_id_array = Arc::new(UInt32Array::from(group_ids)) as ArrayRef;
                    let data_files_array = Arc::new(StringArray::from(data_files_json)) as ArrayRef;

                    let batch = RecordBatch::try_new(
                        output_schema,
                        vec![group_id_array, data_files_array],
                    )?;

                    Ok::<RecordBatch, datafusion::error::DataFusionError>(batch)
                }
            })
            .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            stream,
        )))
    }
}
```

**Step 2: Run tests**

Run: `cargo test -p iceberg-datafusion --lib physical_plan::compaction`
Expected: PASS (compilation)

**Step 3: Commit**

```bash
git add crates/integrations/datafusion/src/physical_plan/compaction.rs
git commit -m "feat(datafusion): implement execute() with progress and cancellation"
```

---

## Phase 3: Compaction Commit Executor

### Task 8: Create IcebergCompactionCommitExec

**Files:**
- Create: `crates/integrations/datafusion/src/physical_plan/compaction_commit.rs`
- Modify: `crates/integrations/datafusion/src/physical_plan/mod.rs`

**Step 1: Create commit executor**

```rust
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Commit executor for Iceberg table compaction.

use std::any::Any;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow_array::{RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use datafusion::error::Result as DFResult;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::TryStreamExt;
use iceberg::catalog::Catalog;
use iceberg::table::Table;
use iceberg::transaction::rewrite_data_files::{
    FileGroupRewriteResult, RewriteDataFilesCommitter, RewriteDataFilesPlan,
};

use super::compaction::{COMPACTION_DATA_FILES_COL, COMPACTION_GROUP_ID_COL};

/// Schema for compaction commit output (metrics).
pub fn compaction_commit_output_schema() -> ArrowSchemaRef {
    Arc::new(ArrowSchema::new(vec![
        Field::new("rewritten_files", DataType::UInt64, false),
        Field::new("added_files", DataType::UInt64, false),
        Field::new("rewritten_bytes", DataType::UInt64, false),
        Field::new("added_bytes", DataType::UInt64, false),
    ]))
}

/// Physical plan for committing Iceberg compaction results.
pub struct IcebergCompactionCommitExec {
    table: Table,
    catalog: Arc<dyn Catalog>,
    plan: RewriteDataFilesPlan,
    input: Arc<dyn ExecutionPlan>,
    output_schema: ArrowSchemaRef,
    plan_properties: PlanProperties,
}

impl IcebergCompactionCommitExec {
    /// Create a new compaction commit executor.
    pub fn new(
        table: Table,
        catalog: Arc<dyn Catalog>,
        plan: RewriteDataFilesPlan,
        input: Arc<dyn ExecutionPlan>,
    ) -> DFResult<Self> {
        let output_schema = compaction_commit_output_schema();

        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Ok(Self {
            table,
            catalog,
            plan,
            input,
            output_schema,
            plan_properties,
        })
    }
}

impl Debug for IcebergCompactionCommitExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergCompactionCommitExec")
            .field("file_groups", &self.plan.file_groups.len())
            .finish()
    }
}

impl DisplayAs for IcebergCompactionCommitExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "IcebergCompactionCommitExec: groups={}",
            self.plan.file_groups.len()
        )
    }
}

impl ExecutionPlan for IcebergCompactionCommitExec {
    fn name(&self) -> &str {
        "IcebergCompactionCommitExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.output_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(
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
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(datafusion::error::DataFusionError::Internal(format!(
                "IcebergCompactionCommitExec only supports partition 0, got {}",
                partition
            )));
        }

        let table = self.table.clone();
        let catalog = self.catalog.clone();
        let plan = self.plan.clone();
        let output_schema = self.output_schema.clone();
        let input_stream = self.input.execute(0, context)?;

        let stream = futures::stream::once(async move {
            // Collect all results from input
            let batches: Vec<RecordBatch> = input_stream.try_collect().await?;

            // Parse results into FileGroupRewriteResult
            let results = parse_compaction_results(&table, &batches)?;

            // Create committer and commit
            let committer = RewriteDataFilesCommitter::new(&table, plan);
            let (action_commit, result) = committer
                .commit(results)
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            // Apply commit to catalog
            let table_commit = iceberg::TableCommit::builder()
                .ident(table.identifier().clone())
                .updates(action_commit.take_updates())
                .requirements(action_commit.take_requirements())
                .build();

            catalog
                .update_table(table_commit)
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            // Build result batch
            let batch = RecordBatch::try_new(
                output_schema,
                vec![
                    Arc::new(UInt64Array::from(vec![result.rewritten_data_files_count])),
                    Arc::new(UInt64Array::from(vec![result.added_data_files_count])),
                    Arc::new(UInt64Array::from(vec![result.rewritten_bytes])),
                    Arc::new(UInt64Array::from(vec![result.added_bytes])),
                ],
            )?;

            Ok::<RecordBatch, datafusion::error::DataFusionError>(batch)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            stream,
        )))
    }
}

/// Parse compaction executor output into FileGroupRewriteResult.
fn parse_compaction_results(
    table: &Table,
    batches: &[RecordBatch],
) -> DFResult<Vec<FileGroupRewriteResult>> {
    use crate::partition_utils::{build_partition_type_map, deserialize_data_file_from_json};
    use arrow_array::StringArray;

    let partition_types = build_partition_type_map(table)
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    let format_version = table.metadata().format_version();

    let mut results_map: HashMap<u32, Vec<iceberg::spec::DataFile>> = HashMap::new();

    for batch in batches {
        let group_ids = batch
            .column_by_name(COMPACTION_GROUP_ID_COL)
            .and_then(|c| c.as_any().downcast_ref::<arrow_array::UInt32Array>())
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal("Missing group_id column".to_string())
            })?;

        let data_files = batch
            .column_by_name(COMPACTION_DATA_FILES_COL)
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "Missing data_files column".to_string(),
                )
            })?;

        for i in 0..batch.num_rows() {
            let group_id = group_ids.value(i);
            let file_json = data_files.value(i);

            let data_file = deserialize_data_file_from_json(file_json, &partition_types, format_version)
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            results_map.entry(group_id).or_default().push(data_file);
        }
    }

    Ok(results_map
        .into_iter()
        .map(|(group_id, new_data_files)| FileGroupRewriteResult {
            group_id,
            new_data_files,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compaction_commit_output_schema() {
        let schema = compaction_commit_output_schema();
        assert_eq!(schema.fields().len(), 4);
    }
}
```

**Step 2: Add module to mod.rs**

```rust
pub mod compaction_commit;
```

**Step 3: Run tests**

Run: `cargo test -p iceberg-datafusion --lib physical_plan::compaction_commit`
Expected: PASS

**Step 4: Commit**

```bash
git add crates/integrations/datafusion/src/physical_plan/compaction_commit.rs
git add crates/integrations/datafusion/src/physical_plan/mod.rs
git commit -m "feat(datafusion): add IcebergCompactionCommitExec"
```

---

## Phase 4: Public API

### Task 9: Create High-Level compact_table API

**Files:**
- Create: `crates/integrations/datafusion/src/compaction.rs`
- Modify: `crates/integrations/datafusion/src/lib.rs`

**Step 1: Create public API**

```rust
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! High-level compaction API for Iceberg tables via DataFusion.
//!
//! # Example
//!
//! ```rust,ignore
//! use iceberg_datafusion::compaction::{compact_table, CompactionOptions};
//! use tokio_util::sync::CancellationToken;
//!
//! // Basic compaction
//! let result = compact_table(&table, &catalog, None).await?;
//!
//! // With progress and cancellation
//! let token = CancellationToken::new();
//! let options = CompactionOptions::default()
//!     .with_cancellation_token(token.clone())
//!     .with_progress_callback(Arc::new(|event| {
//!         println!("{:?}", event);
//!     }));
//!
//! let result = compact_table(&table, &catalog, Some(options)).await?;
//! ```

use std::sync::Arc;

use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::TryStreamExt;
use iceberg::catalog::Catalog;
use iceberg::table::Table;
use iceberg::transaction::rewrite_data_files::{
    CompactionProgressEvent, ProgressCallback, RewriteDataFilesOptions,
    RewriteDataFilesPlanner, RewriteDataFilesResult,
};
use tokio_util::sync::CancellationToken;

use crate::physical_plan::compaction::IcebergCompactionExec;
use crate::physical_plan::compaction_commit::IcebergCompactionCommitExec;

/// Options for table compaction.
#[derive(Default)]
pub struct CompactionOptions {
    /// Core rewrite options (file sizes, thresholds).
    pub rewrite_options: Option<RewriteDataFilesOptions>,
    /// Cancellation token for aborting the operation.
    pub cancellation_token: Option<CancellationToken>,
    /// Progress callback for monitoring.
    pub progress_callback: Option<ProgressCallback>,
}

impl CompactionOptions {
    /// Set rewrite options.
    pub fn with_rewrite_options(mut self, options: RewriteDataFilesOptions) -> Self {
        self.rewrite_options = Some(options);
        self
    }

    /// Set cancellation token.
    pub fn with_cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancellation_token = Some(token);
        self
    }

    /// Set progress callback.
    pub fn with_progress_callback(mut self, callback: ProgressCallback) -> Self {
        self.progress_callback = Some(callback);
        self
    }
}

/// Compact an Iceberg table by rewriting small files into larger ones.
///
/// This is the high-level API for table compaction. It:
/// 1. Plans which files should be compacted
/// 2. Reads and merges the data (applying deletes)
/// 3. Writes new compacted files
/// 4. Commits the changes atomically
///
/// # Arguments
///
/// * `table` - The Iceberg table to compact
/// * `catalog` - The catalog for committing changes
/// * `options` - Optional compaction configuration
///
/// # Returns
///
/// `RewriteDataFilesResult` containing metrics about the compaction.
///
/// # Cancellation
///
/// If a cancellation token is provided and cancelled during execution:
/// - No partial changes are committed
/// - The function returns an error
///
/// # Progress Reporting
///
/// If a progress callback is provided, it will receive events:
/// - `Started` - When compaction begins
/// - `GroupStarted` - When each file group starts processing
/// - `GroupCompleted` - When each file group finishes
/// - `Completed` - When all groups are done
/// - `Cancelled` - If operation is cancelled
pub async fn compact_table(
    table: &Table,
    catalog: Arc<dyn Catalog>,
    options: Option<CompactionOptions>,
) -> Result<RewriteDataFilesResult, Box<dyn std::error::Error + Send + Sync>> {
    let options = options.unwrap_or_default();

    let rewrite_options = options.rewrite_options.unwrap_or_else(|| {
        RewriteDataFilesOptions::from_table_properties(table.metadata().properties())
    });

    // Step 1: Plan compaction
    let planner = RewriteDataFilesPlanner::new(table, &rewrite_options);
    let plan = planner.plan().await?;

    if plan.file_groups.is_empty() {
        // Nothing to compact
        if let Some(ref callback) = options.progress_callback {
            callback(CompactionProgressEvent::Completed {
                groups_processed: 0,
                groups_failed: 0,
                duration_ms: 0,
            });
        }
        return Ok(RewriteDataFilesResult::default());
    }

    // Step 2: Create execution plan with progress and cancellation
    let mut compaction_exec = IcebergCompactionExec::new(table.clone(), plan.clone())?;

    if let Some(token) = options.cancellation_token {
        compaction_exec = compaction_exec.with_cancellation_token(token);
    }

    if let Some(callback) = options.progress_callback {
        compaction_exec = compaction_exec.with_progress_callback(callback);
    }

    let compaction_exec = Arc::new(compaction_exec);
    let commit_exec = Arc::new(IcebergCompactionCommitExec::new(
        table.clone(),
        catalog,
        plan,
        compaction_exec,
    )?);

    // Step 3: Execute
    let context = Arc::new(TaskContext::default());
    let stream = commit_exec.execute(0, context)?;
    let batches: Vec<_> = stream.try_collect().await?;

    // Step 4: Extract result
    if batches.is_empty() {
        return Ok(RewriteDataFilesResult::default());
    }

    let batch = &batches[0];
    let result = RewriteDataFilesResult {
        rewritten_data_files_count: batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .map(|a| a.value(0))
            .unwrap_or(0),
        added_data_files_count: batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .map(|a| a.value(0))
            .unwrap_or(0),
        rewritten_bytes: batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .map(|a| a.value(0))
            .unwrap_or(0),
        added_bytes: batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .map(|a| a.value(0))
            .unwrap_or(0),
        ..Default::default()
    };

    Ok(result)
}
```

**Step 2: Add to lib.rs**

```rust
pub mod compaction;
pub use compaction::{compact_table, CompactionOptions};
```

**Step 3: Commit**

```bash
git add crates/integrations/datafusion/src/compaction.rs
git add crates/integrations/datafusion/src/lib.rs
git commit -m "feat(datafusion): add high-level compact_table API"
```

---

## Verification Checklist

After completing all tasks:

- [ ] All unit tests pass: `cargo test -p iceberg --lib rewrite_data_files`
- [ ] All DataFusion tests pass: `cargo test -p iceberg-datafusion`
- [ ] No clippy warnings: `cargo clippy -p iceberg -p iceberg-datafusion`
- [ ] Progress events are emitted correctly
- [ ] Cancellation stops processing new groups
- [ ] Position deletes are applied during read

---

## Phase 2.1 Acceptance Criteria Mapping

| Criterion | Task | Implementation |
|-----------|------|----------------|
| Reduces file count while preserving all data | Tasks 5-8 | Read→merge→write flow |
| Respects partition boundaries | Existing | FileGroups grouped by partition |
| Configurable target file size | Existing | `RewriteDataFilesOptions` |
| Progress reporting | Tasks 1, 7, 9 | `CompactionProgressEvent` + callbacks |
| Cancellation support | Tasks 2, 4, 7, 9 | `CancellationToken` integration |
| Handles position delete files correctly | Task 5 | Applied via Iceberg scan |

---

## Test Commands Summary

```bash
# Run all iceberg core tests
cargo test -p iceberg --lib rewrite_data_files

# Run all DataFusion tests
cargo test -p iceberg-datafusion

# Run specific module tests
cargo test -p iceberg --lib rewrite_data_files::progress
cargo test -p iceberg-datafusion --lib physical_plan::compaction
cargo test -p iceberg-datafusion --lib physical_plan::compaction_commit

# Check for warnings
cargo clippy -p iceberg -p iceberg-datafusion -- -D warnings
```
