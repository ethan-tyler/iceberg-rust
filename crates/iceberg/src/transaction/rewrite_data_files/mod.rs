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

//! This module provides infrastructure for table compaction via bin-packing.
//!
//! The `rewrite_data_files` action consolidates small files into larger,
//! optimally-sized files while preserving data integrity. This is the
//! Rust-native implementation of Iceberg's `rewrite_data_files` action.
//!
//! # Architecture
//!
//! The implementation separates planning and execution:
//!
//! - **[`RewriteDataFilesPlanner`]**: Scans manifests and creates file groups
//! - **Execution**: External (query engine like DataFusion reads/writes files)
//! - **[`RewriteDataFilesCommitter`]**: Atomically commits the results
//!
//! This separation allows integration with any query engine for the
//! computationally intensive read/write operations.
//!
//! # Use Cases
//!
//! - Streaming pipelines producing many small files
//! - ETL workloads with frequent micro-batches
//! - Reducing query planning overhead from file count
//! - Merging position/equality deletes into data files
//!
//! # Example: Planner/Committer Workflow
//!
//! ```ignore
//! use iceberg::TableCommit;
//! use iceberg::transaction::{
//!     RewriteDataFilesOptions, RewriteDataFilesPlanner, RewriteDataFilesCommitter,
//!     FileGroupRewriteResult,
//! };
//!
//! // Configure compaction options (mutate the struct directly)
//! let mut options = RewriteDataFilesOptions::default();
//! options.target_file_size_bytes = 256 * 1024 * 1024;  // 256 MB
//! options.min_input_files = 5;
//!
//! // Or initialize from table properties:
//! // let options = RewriteDataFilesOptions::from_table_properties(table.metadata().properties());
//!
//! // 1. Plan: identify file groups to compact
//! let planner = RewriteDataFilesPlanner::new(&table, &options);
//! let plan = planner.plan().await?;
//!
//! println!("Found {} groups ({} files, {} bytes) to compact",
//!     plan.file_groups.len(), plan.total_data_files, plan.total_bytes);
//!
//! // 2. Execute: use your query engine to rewrite each group
//! let mut results = Vec::new();
//! for group in &plan.file_groups {
//!     // Read data files from group.data_files (Vec<ManifestEntryRef>)
//!     // Apply position deletes from group.position_delete_files
//!     // Apply equality deletes from group.equality_delete_files
//!     // Write new files at target size
//!     let new_files = your_engine.execute_rewrite(&table, group).await?;
//!
//!     results.push(FileGroupRewriteResult {
//!         group_id: group.group_id,
//!         new_data_files: new_files,
//!     });
//! }
//!
//! // 3. Commit: atomically replace old files with new files
//! let committer = RewriteDataFilesCommitter::new(&table, plan);
//! let (mut action_commit, result) = committer.commit(results).await?;
//!
//! // Apply directly to catalog (take_* methods consume the ActionCommit fields)
//! let table_commit = TableCommit::builder()
//!     .ident(table.identifier().clone())
//!     .updates(action_commit.take_updates())
//!     .requirements(action_commit.take_requirements())
//!     .build();
//! let table = catalog.update_table(table_commit).await?;
//!
//! println!("Compacted {} files into {} files, saved {} bytes",
//!     result.rewritten_data_files_count,
//!     result.added_data_files_count,
//!     result.bytes_saved());
//! ```
//!
//! # Atomicity
//!
//! The operation is atomic: files are only replaced on successful commit.
//! On failure, original files remain unchanged. Any files written before
//! failure become orphans and can be cleaned up with `remove_orphan_files`.
//!
//! # Merge-on-Read (MoR) Tables
//!
//! For V2 tables with position or equality deletes, compaction reconciles
//! delete files during the rewrite. The `use_starting_sequence_number` option
//! (default: true) ensures correct delete application ordering by setting
//! the data sequence number on new files to the snapshot sequence number
//! from when planning occurred.
//!
//! # Format Version Support
//!
//! - **V1 tables**: Simple bin-packing of data files (no delete handling)
//! - **V2+ tables**: Full compaction with delete file reconciliation
//!
//! # Configuration
//!
//! Key options in [`RewriteDataFilesOptions`]:
//!
//! - `target_file_size_bytes`: Target output file size (default: 512 MB)
//! - `min_file_size_bytes`: Files smaller than this are candidates (default: 75% of target)
//! - `max_file_size_bytes`: Files larger than this are split (default: 180% of target)
//! - `min_input_files`: Minimum files to form a group (default: 5)
//! - `max_file_group_size_bytes`: Maximum total size per group (default: 100 GB)

mod committer;
mod delete_tracker;
mod file_group;
mod options;
mod partition_filter;
mod planner;
mod progress;
mod result;
mod strategy;

pub use committer::{FileGroupRewriteResult, RewriteDataFilesCommitter};
pub use file_group::FileGroup;
pub use options::{RewriteDataFilesOptions, RewriteJobOrder};
pub use planner::{RewriteDataFilesPlanner, RewriteDataFilesPlan};
pub use progress::{CompactionProgressEvent, CompactionProgressTracker, ProgressCallback};
pub use result::{FileGroupFailure, FileGroupResult, RewriteDataFilesResult};
pub use strategy::RewriteStrategy;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::{Error, ErrorKind, Result};
use crate::spec::FormatVersion;
use crate::table::Table;
use crate::transaction::{ActionCommit, TransactionAction};

/// Action for compacting data files using bin-packing.
///
/// This action consolidates small files into larger, target-sized files
/// while respecting partition boundaries and handling delete files correctly.
///
/// # Configuration
///
/// Use builder methods to customize compaction behavior:
///
/// - `target_file_size_bytes()`: Target output file size (default: 512 MB)
/// - `min_file_size_bytes()`: Files smaller than this are candidates (default: 75% of target)
/// - `max_file_size_bytes()`: Files larger than this are split (default: 180% of target)
/// - `min_input_files()`: Minimum files to trigger rewrite (default: 5)
/// - `max_concurrent_file_group_rewrites()`: Parallelism limit (default: num_cpus)
///
/// # Strategies
///
/// - `bin_pack()`: Default - combines files by size (Phase 2.1)
/// - `sort()`: Sort data during compaction (Phase 2.2)
/// - `z_order()`: Z-order clustering for multi-column optimization (Phase 2.3)
#[derive(Debug)]
pub struct RewriteDataFilesAction {
    /// Configuration options
    options: RewriteDataFilesOptions,

    /// Strategy for rewriting files
    strategy: RewriteStrategy,

    /// Optional filter to limit which files/partitions to compact
    partition_filter: Option<crate::expr::Predicate>,

    /// Case sensitivity for partition filter field matching (default: true)
    case_sensitive: bool,

    /// Optional commit UUID
    commit_uuid: Option<Uuid>,

    /// Snapshot summary properties
    snapshot_properties: HashMap<String, String>,
}

impl RewriteDataFilesAction {
    /// Creates a new rewrite data files action with default options.
    pub(crate) fn new() -> Self {
        Self {
            options: RewriteDataFilesOptions::default(),
            strategy: RewriteStrategy::BinPack,
            partition_filter: None,
            case_sensitive: true,
            commit_uuid: None,
            snapshot_properties: HashMap::new(),
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Size Configuration
    // ═══════════════════════════════════════════════════════════════════════

    /// Set target output file size in bytes.
    ///
    /// Files will be written to approximately this size. Default is 512 MB
    /// regardless of table properties. Call this method explicitly to override.
    ///
    /// **Note**: The action does not automatically read the table's
    /// `write.target-file-size-bytes` property. This is intentional to avoid
    /// hidden behavior. To use table-configured sizes, read the property
    /// explicitly:
    ///
    /// ```ignore
    /// let target = table.metadata()
    ///     .properties()
    ///     .get("write.target-file-size-bytes")
    ///     .and_then(|s| s.parse().ok())
    ///     .unwrap_or(512 * 1024 * 1024);
    ///
    /// let action = tx.rewrite_data_files()
    ///     .target_file_size_bytes(target);
    /// ```
    #[must_use]
    pub fn target_file_size_bytes(mut self, size: u64) -> Self {
        self.options.target_file_size_bytes = size;
        // Recompute derived defaults if not explicitly set
        if !self.options.min_file_size_bytes_set {
            self.options.min_file_size_bytes = (size as f64 * 0.75) as u64;
        }
        if !self.options.max_file_size_bytes_set {
            self.options.max_file_size_bytes = (size as f64 * 1.80) as u64;
        }
        self
    }

    /// Set minimum file size threshold for candidates.
    ///
    /// Files smaller than this are candidates for compaction.
    /// Default is 75% of target file size.
    #[must_use]
    pub fn min_file_size_bytes(mut self, size: u64) -> Self {
        self.options.min_file_size_bytes = size;
        self.options.min_file_size_bytes_set = true;
        self
    }

    /// Set maximum file size threshold.
    ///
    /// Files larger than this are candidates for splitting.
    /// Default is 180% of target file size.
    #[must_use]
    pub fn max_file_size_bytes(mut self, size: u64) -> Self {
        self.options.max_file_size_bytes = size;
        self.options.max_file_size_bytes_set = true;
        self
    }

    /// Set minimum number of input files to trigger a rewrite.
    ///
    /// File groups with fewer files than this are not rewritten unless
    /// their total size exceeds the target file size. Default is 5.
    #[must_use]
    pub fn min_input_files(mut self, count: u32) -> Self {
        self.options.min_input_files = count;
        self
    }

    /// Set maximum file group size in bytes.
    ///
    /// Files within a partition are grouped up to this size for processing.
    /// Default is 100 GB.
    #[must_use]
    pub fn max_file_group_size_bytes(mut self, size: u64) -> Self {
        self.options.max_file_group_size_bytes = size;
        self
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Execution Configuration
    // ═══════════════════════════════════════════════════════════════════════

    /// Set maximum concurrent file group rewrites.
    ///
    /// Limits parallelism during execution. Default is the number of CPU cores.
    /// Set to 1 for minimal resource usage, or higher for faster execution
    /// on high-bandwidth storage.
    #[must_use]
    pub fn max_concurrent_file_group_rewrites(mut self, count: u32) -> Self {
        self.options.max_concurrent_file_group_rewrites = Some(count);
        self
    }

    /// Enable partial progress commits.
    ///
    /// When enabled, file groups should be committed in batches using
    /// [`RewriteDataFilesCommitter::commit_batch()`] instead of `commit()`.
    /// This allows progress to be preserved even if some batches fail.
    ///
    /// Default is false (all-or-nothing via `commit()`).
    ///
    /// # Workflow
    ///
    /// When partial progress is enabled, use the following workflow:
    ///
    /// ```ignore
    /// let options = RewriteDataFilesOptions::default();
    /// options.partial_progress_enabled = true;
    /// options.partial_progress_max_commits = 5;
    ///
    /// let planner = RewriteDataFilesPlanner::new(&table, &options);
    /// let plan = planner.plan().await?;
    ///
    /// let committer = RewriteDataFilesCommitter::new(&table, plan.clone());
    /// let batches = committer.plan_batches(options.partial_progress_max_commits);
    ///
    /// for batch_group_ids in &batches {
    ///     // Execute and collect results for this batch's groups
    ///     let batch_results = execute_batch(&plan, batch_group_ids).await?;
    ///
    ///     // Commit this batch
    ///     let (commit, result) = committer.commit_batch(batch_results, batch_group_ids).await?;
    ///
    ///     // Apply to catalog
    ///     catalog.update_table(commit).await?;
    ///
    ///     // Refresh table for next batch
    ///     table = catalog.load_table(&table_id).await?;
    ///     committer = RewriteDataFilesCommitter::new(&table, plan.clone());
    /// }
    /// ```
    #[must_use]
    pub fn partial_progress_enabled(mut self, enabled: bool) -> Self {
        self.options.partial_progress_enabled = enabled;
        self
    }

    /// Set maximum commits for partial progress mode.
    ///
    /// Only used when `partial_progress_enabled(true)`. Default is 10.
    #[must_use]
    pub fn partial_progress_max_commits(mut self, count: u32) -> Self {
        self.options.partial_progress_max_commits = count;
        self
    }

    /// Set maximum failed file groups before aborting.
    ///
    /// Only used when `partial_progress_enabled(true)`. If more than
    /// this many groups fail, the operation aborts. Default is unlimited.
    #[must_use]
    pub fn partial_progress_max_failed_commits(mut self, count: u32) -> Self {
        self.options.partial_progress_max_failed_commits = Some(count);
        self
    }

    /// Set file group execution order.
    ///
    /// Controls which file groups are processed first. Default is no ordering.
    #[must_use]
    pub fn rewrite_job_order(mut self, order: RewriteJobOrder) -> Self {
        self.options.rewrite_job_order = order;
        self
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Delete Handling (V2+ Tables)
    // ═══════════════════════════════════════════════════════════════════════

    /// Use starting sequence number for new files.
    ///
    /// **CRITICAL for MoR tables**: When true (default), new files are written
    /// with the sequence number from the snapshot at compaction start. This
    /// ensures equality deletes continue to apply correctly.
    ///
    /// **Note**: Setting to `false` is not yet implemented and will return a
    /// `FeatureUnsupported` error. The default `true` behavior is always used.
    #[doc(hidden)]
    #[must_use]
    pub fn use_starting_sequence_number(mut self, use_it: bool) -> Self {
        self.options.use_starting_sequence_number = use_it;
        self
    }

    /// Remove dangling delete files after compaction.
    ///
    /// When true, position delete files that no longer reference any remaining
    /// data files are removed from the table. This happens when all the data
    /// files a position delete references are compacted away.
    ///
    /// Default is false.
    ///
    /// **Note:** Only position deletes with `referenced_data_file` set are
    /// tracked. Partition-scoped position deletes (without the field) and
    /// equality deletes are not removed by this option.
    ///
    /// **Note:** Requires format version V2 or later.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Remove orphaned delete files during compaction
    /// table.new_transaction()
    ///     .rewrite_data_files()
    ///     .remove_dangling_deletes(true)
    ///     .commit()
    ///     .await?;
    /// ```
    #[must_use]
    pub fn remove_dangling_deletes(mut self, remove: bool) -> Self {
        self.options.remove_dangling_deletes = remove;
        self
    }

    /// Set delete file threshold for candidate selection.
    ///
    /// Files with this many or more associated delete files become
    /// candidates for rewrite regardless of size. Default is disabled.
    ///
    /// **Note:** Requires format version V2 or later. On V1 tables, this option
    /// will result in a `FeatureUnsupported` error during commit.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Compact files with 3+ delete files, even if they're the right size
    /// table.new_transaction()
    ///     .rewrite_data_files()
    ///     .delete_file_threshold(3)
    ///     .commit()
    ///     .await?;
    /// ```
    #[must_use]
    pub fn delete_file_threshold(mut self, threshold: u32) -> Self {
        self.options.delete_file_threshold = Some(threshold);
        self
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Filter Configuration
    // ═══════════════════════════════════════════════════════════════════════

    /// Filter files to rewrite by partition predicate.
    ///
    /// The predicate is projected to partition values using inclusive projection,
    /// meaning it will include any partition that *might* contain matching data.
    ///
    /// # Partition Evolution
    ///
    /// When a table has multiple partition specs (due to partition evolution),
    /// files with partition specs that don't match the table's current default spec
    /// are included conservatively (not filtered out). This ensures correctness
    /// at the cost of potentially compacting more files than strictly necessary.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    ///
    /// // Compact only files in the '2024-01-15' partition
    /// let action = table
    ///     .new_rewrite_data_files()
    ///     .filter(Reference::new("date").equal_to(Datum::date_from_str("2024-01-15").unwrap()))
    ///     .plan()
    ///     .await?;
    /// ```
    #[must_use]
    pub fn filter(mut self, predicate: crate::expr::Predicate) -> Self {
        self.partition_filter = Some(predicate);
        self
    }

    /// Set case sensitivity for partition filter field matching.
    ///
    /// When `true` (default), field names in the filter predicate must match
    /// column names exactly. When `false`, field matching is case-insensitive.
    ///
    /// This setting only affects the partition filter predicate. It has no
    /// effect if no filter is set.
    #[must_use]
    pub fn case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.case_sensitive = case_sensitive;
        self
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Strategy Selection
    // ═══════════════════════════════════════════════════════════════════════

    /// Use bin-pack strategy (default).
    ///
    /// Combines files based on size without changing data order.
    /// This is the fastest strategy as it avoids sorting.
    #[must_use]
    pub fn bin_pack(mut self) -> Self {
        self.strategy = RewriteStrategy::BinPack;
        self
    }

    /// Use sort strategy with table's default sort order.
    ///
    /// Sorts data during compaction using the table's defined sort order.
    /// Improves query performance for sorted access patterns.
    ///
    /// **Note**: This is a placeholder for Phase 2.2 implementation.
    #[must_use]
    pub fn sort(mut self) -> Self {
        self.strategy = RewriteStrategy::Sort { sort_order: None };
        self
    }

    /// Use sort strategy with custom sort order.
    ///
    /// **Note**: This is a placeholder for Phase 2.2 implementation.
    #[must_use]
    pub fn sort_by(mut self, sort_order: crate::spec::SortOrder) -> Self {
        self.strategy = RewriteStrategy::Sort {
            sort_order: Some(sort_order),
        };
        self
    }

    /// Use Z-order clustering on specified columns.
    ///
    /// Clusters data using Z-order (Morton) encoding for efficient
    /// multi-dimensional range queries.
    ///
    /// **Note**: This is a placeholder for Phase 2.3 implementation.
    #[must_use]
    pub fn z_order(mut self, columns: Vec<String>) -> Self {
        self.strategy = RewriteStrategy::ZOrder { columns };
        self
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Commit Configuration (Reserved for future use)
    // ═══════════════════════════════════════════════════════════════════════

    /// Set commit UUID for the snapshot.
    ///
    /// **Note**: This method is not yet functional as the action requires
    /// external execution. Use [`RewriteDataFilesCommitter::with_commit_uuid()`]
    /// instead when committing via the planner/committer workflow.
    #[doc(hidden)]
    #[must_use]
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set snapshot summary properties.
    ///
    /// **Note**: This method is not yet functional as the action requires
    /// external execution. Use [`RewriteDataFilesCommitter::with_snapshot_properties()`]
    /// instead when committing via the planner/committer workflow.
    #[doc(hidden)]
    #[must_use]
    pub fn set_snapshot_properties(mut self, properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = properties;
        self
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Validation
    // ═══════════════════════════════════════════════════════════════════════

    /// Validate options before execution.
    fn validate(&self, table: &Table) -> Result<()> {
        // Validate size thresholds
        if self.options.target_file_size_bytes == 0 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "target-file-size-bytes must be > 0",
            ));
        }

        if self.options.min_file_size_bytes >= self.options.target_file_size_bytes {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "min-file-size-bytes must be < target-file-size-bytes",
            ));
        }

        if self.options.max_file_size_bytes <= self.options.target_file_size_bytes {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "max-file-size-bytes must be > target-file-size-bytes",
            ));
        }

        if self.options.max_file_group_size_bytes == 0 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "max-file-group-size-bytes must be > 0",
            ));
        }

        // Validate execution configuration
        if self.options.min_input_files == 0 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "min-input-files must be > 0",
            ));
        }

        if let Some(max_concurrent) = self.options.max_concurrent_file_group_rewrites {
            if max_concurrent == 0 {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "max-concurrent-file-group-rewrites must be > 0",
                ));
            }
        }

        if self.options.partial_progress_enabled && self.options.partial_progress_max_commits == 0 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "partial-progress-max-commits must be > 0 when partial progress is enabled",
            ));
        }

        // V1 tables cannot use delete-related options
        if table.metadata().format_version() == FormatVersion::V1 {
            if self.options.remove_dangling_deletes {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    "remove_dangling_deletes requires format version 2 or later",
                ));
            }
            if self.options.delete_file_threshold.is_some() {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    "delete_file_threshold requires format version 2 or later",
                ));
            }
        }

        // use_starting_sequence_number=false is not yet implemented
        // The default (true) is critical for MoR correctness and is always used
        if !self.options.use_starting_sequence_number {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "use_starting_sequence_number=false is not yet implemented. The starting \
                 sequence number from plan time is always used to ensure correct delete \
                 ordering for MoR tables. Custom sequence number handling will be supported \
                 in a future release.",
            ));
        }

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

        Ok(())
    }
}

#[async_trait]
impl TransactionAction for RewriteDataFilesAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        // Validate options
        self.validate(table)?;

        // Plan the compaction
        let planner = RewriteDataFilesPlanner::new(table, &self.options)
            .with_case_sensitive(self.case_sensitive);
        let planner = match &self.partition_filter {
            Some(filter) => planner.with_partition_filter(filter),
            None => planner,
        };
        let plan = planner.plan().await?;

        // If no files to rewrite, this is a no-op (success with empty result)
        if plan.file_groups.is_empty() {
            return Ok(ActionCommit::new(vec![], vec![]));
        }

        // Execution requires a query engine (like DataFusion) to read data files,
        // apply deletes, and write new files. The iceberg crate provides planning
        // and commit infrastructure; execution must be done externally.
        //
        // For direct usage, see the planner/committer workflow:
        //
        // ```ignore
        // // 1. Plan
        // let planner = RewriteDataFilesPlanner::new(&table, &options);
        // let plan = planner.plan().await?;
        //
        // // 2. Execute each group (using your query engine)
        // let mut results = Vec::new();
        // for group in &plan.file_groups {
        //     // Read data files, apply deletes, write new files
        //     let new_files = your_engine.execute_rewrite(group).await?;
        //     results.push(FileGroupRewriteResult {
        //         group_id: group.group_id,
        //         new_data_files: new_files,
        //     });
        // }
        //
        // // 3. Commit atomically
        // let committer = RewriteDataFilesCommitter::new(&table, plan);
        // let (action_commit, result) = committer.commit(results).await?;
        // ```
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            format!(
                "rewrite_data_files action found {} file groups ({} files, {} bytes) to compact, \
                 but built-in execution is not yet implemented. \
                 Use RewriteDataFilesPlanner and RewriteDataFilesCommitter directly with \
                 an external query engine (like DataFusion) to execute the rewrites.",
                plan.file_groups.len(),
                plan.total_data_files,
                plan.total_bytes
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_options() {
        let action = RewriteDataFilesAction::new();

        assert_eq!(action.options.target_file_size_bytes, 512 * 1024 * 1024);
        assert_eq!(
            action.options.min_file_size_bytes,
            (512.0 * 1024.0 * 1024.0 * 0.75) as u64
        );
        assert_eq!(
            action.options.max_file_size_bytes,
            (512.0 * 1024.0 * 1024.0 * 1.80) as u64
        );
        assert_eq!(action.options.min_input_files, 5);
        assert!(!action.options.partial_progress_enabled);
        assert!(action.options.use_starting_sequence_number);
        assert!(!action.options.remove_dangling_deletes);
    }

    #[test]
    fn test_builder_pattern() {
        let action = RewriteDataFilesAction::new()
            .target_file_size_bytes(128 * 1024 * 1024)
            .min_input_files(10)
            .max_concurrent_file_group_rewrites(4)
            .partial_progress_enabled(true)
            .remove_dangling_deletes(true);

        assert_eq!(action.options.target_file_size_bytes, 128 * 1024 * 1024);
        assert_eq!(action.options.min_input_files, 10);
        assert_eq!(action.options.max_concurrent_file_group_rewrites, Some(4));
        assert!(action.options.partial_progress_enabled);
        assert!(action.options.remove_dangling_deletes);
    }

    #[test]
    fn test_derived_size_thresholds() {
        // When only target is set, derived values should be computed
        let action = RewriteDataFilesAction::new().target_file_size_bytes(100 * 1024 * 1024);

        assert_eq!(
            action.options.min_file_size_bytes,
            (100.0 * 1024.0 * 1024.0 * 0.75) as u64
        );
        assert_eq!(
            action.options.max_file_size_bytes,
            (100.0 * 1024.0 * 1024.0 * 1.80) as u64
        );
    }

    #[test]
    fn test_explicit_size_thresholds_preserved() {
        // When explicitly set, derived values should not override
        let action = RewriteDataFilesAction::new()
            .min_file_size_bytes(50 * 1024 * 1024)
            .max_file_size_bytes(200 * 1024 * 1024)
            .target_file_size_bytes(128 * 1024 * 1024);

        // Explicit values preserved even after setting target
        assert_eq!(action.options.min_file_size_bytes, 50 * 1024 * 1024);
        assert_eq!(action.options.max_file_size_bytes, 200 * 1024 * 1024);
    }

    #[test]
    fn test_strategy_selection() {
        let action = RewriteDataFilesAction::new();
        assert!(matches!(action.strategy, RewriteStrategy::BinPack));

        let action = RewriteDataFilesAction::new().sort();
        assert!(matches!(action.strategy, RewriteStrategy::Sort { .. }));

        let action = RewriteDataFilesAction::new().z_order(vec!["col1".to_string()]);
        assert!(matches!(action.strategy, RewriteStrategy::ZOrder { .. }));
    }
}
