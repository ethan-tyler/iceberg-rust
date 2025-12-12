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
//! This module provides a user-friendly API for compacting Iceberg tables
//! by rewriting small files into larger, target-sized files.
//!
//! # Example
//!
//! ```rust,ignore
//! use std::sync::Arc;
//! use iceberg_datafusion::compaction::{compact_table, CompactionOptions};
//! use tokio_util::sync::CancellationToken;
//!
//! // Basic compaction
//! let result = compact_table(&table, catalog.clone(), None).await?;
//!
//! // With progress and cancellation
//! let token = CancellationToken::new();
//! let options = CompactionOptions::default()
//!     .with_cancellation_token(token.clone())
//!     .with_progress_callback(Arc::new(|event| {
//!         println!("{:?}", event);
//!     }));
//!
//! let result = compact_table(&table, catalog.clone(), Some(options)).await?;
//! ```

use std::sync::Arc;

use datafusion::arrow::array::UInt64Array;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::TryStreamExt;
use iceberg::Catalog;
use iceberg::table::Table;
use iceberg::transaction::rewrite_data_files::{
    CompactionProgressEvent, ProgressCallback, RewriteDataFilesOptions, RewriteDataFilesPlanner,
    RewriteDataFilesResult,
};
use tokio_util::sync::CancellationToken;

use crate::error::to_datafusion_error;
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
    #[must_use]
    pub fn with_rewrite_options(mut self, options: RewriteDataFilesOptions) -> Self {
        self.rewrite_options = Some(options);
        self
    }

    /// Set cancellation token.
    #[must_use]
    pub fn with_cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancellation_token = Some(token);
        self
    }

    /// Set progress callback.
    #[must_use]
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
/// 4. Commits the changes atomically to the catalog
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
/// # Errors
///
/// Returns an error if:
/// - Planning fails (e.g., no snapshot exists)
/// - Execution fails (e.g., I/O errors)
/// - Commit fails (e.g., concurrent modification)
/// - Operation is cancelled
///
/// # Orphan Files
///
/// If the rewrite writes new data files successfully but the final catalog commit fails
/// (for example, due to concurrent modification), those newly written files may become
/// orphaned and require cleanup (e.g., via `remove_orphan_files`).
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
) -> Result<RewriteDataFilesResult, DataFusionError> {
    let options = options.unwrap_or_default();

    let rewrite_options = options.rewrite_options.unwrap_or_else(|| {
        RewriteDataFilesOptions::from_table_properties(table.metadata().properties())
    });

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
        return Ok(RewriteDataFilesResult::empty());
    }

    let batch = &batches[0];
    let result = RewriteDataFilesResult {
        rewritten_data_files_count: batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|a| a.value(0))
            .unwrap_or(0),
        added_data_files_count: batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|a| a.value(0))
            .unwrap_or(0),
        rewritten_bytes: batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|a| a.value(0))
            .unwrap_or(0),
        added_bytes: batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|a| a.value(0))
            .unwrap_or(0),
        ..Default::default()
    };

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compaction_options_default() {
        let options = CompactionOptions::default();
        assert!(options.rewrite_options.is_none());
        assert!(options.cancellation_token.is_none());
        assert!(options.progress_callback.is_none());
    }

    #[test]
    fn test_compaction_options_builder() {
        let token = CancellationToken::new();
        let callback: ProgressCallback = Arc::new(|_event| {});

        let options = CompactionOptions::default()
            .with_rewrite_options(RewriteDataFilesOptions::default())
            .with_cancellation_token(token)
            .with_progress_callback(callback);

        assert!(options.rewrite_options.is_some());
        assert!(options.cancellation_token.is_some());
        assert!(options.progress_callback.is_some());
    }
}
