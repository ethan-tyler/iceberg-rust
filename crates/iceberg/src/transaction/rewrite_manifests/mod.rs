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

//! This module provides infrastructure for manifest rewriting/consolidation.
//!
//! The `rewrite_manifests` action consolidates small manifests into larger,
//! optimally-sized manifests while preserving data file references and
//! clustering files by partition. This is the Rust-native implementation
//! of Iceberg's `rewrite_manifests` action.
//!
//! # Problem Solved
//!
//! Frequent or streaming writes create many small manifest files:
//!
//! - **Query Planning Degradation**: Each manifest must be read during planning
//! - **Metadata I/O Amplification**: Thousands of small manifests = thousands of reads
//! - **Partition Alignment Issues**: Files scattered across manifests prevent pruning
//! - **Cost Inefficiency**: Object storage costs scale with request count
//!
//! # Solution
//!
//! Manifest rewriting:
//!
//! 1. Loads all entries from selected manifests
//! 2. Sorts entries by partition (for optimal query pruning)
//! 3. Writes entries into new, target-sized manifests
//! 4. Atomically commits the change
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────┐
//! │                    RewriteManifestsAction                           │
//! │  ┌─────────────────────────────────────────────────────────────┐   │
//! │  │  Builder: spec_id, rewrite_if, target_size                  │   │
//! │  └─────────────────────────────────────────────────────────────┘   │
//! │                              │                                      │
//! │                              ▼                                      │
//! │  ┌─────────────────────────────────────────────────────────────┐   │
//! │  │  Planning: Load manifests, filter, load entries             │   │
//! │  └─────────────────────────────────────────────────────────────┘   │
//! │                              │                                      │
//! │                              ▼                                      │
//! │  ┌─────────────────────────────────────────────────────────────┐   │
//! │  │  Rewrite: Sort entries, batch, write new manifests          │   │
//! │  └─────────────────────────────────────────────────────────────┘   │
//! │                              │                                      │
//! │                              ▼                                      │
//! │  ┌─────────────────────────────────────────────────────────────┐   │
//! │  │  Commit: Validate, atomic metadata update                   │   │
//! │  └─────────────────────────────────────────────────────────────┘   │
//! └─────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Use Cases
//!
//! - **Streaming Pipelines**: Consolidate frequent micro-batch writes
//! - **High-Frequency ETL**: Optimize hourly/minute-level data loads
//! - **Large Tables**: Reduce manifest count for faster planning
//! - **Cost Optimization**: Reduce metadata storage and API calls
//!
//! # Example
//!
//! ```ignore
//! use iceberg::transaction::{Transaction, ApplyTransactionAction};
//!
//! let tx = Transaction::new(&table);
//!
//! // Basic manifest rewrite
//! let action = tx.rewrite_manifests();
//! let tx = action.apply(tx)?;
//! let table = tx.commit(&catalog).await?;
//!
//! // With custom configuration
//! let tx = Transaction::new(&table);
//! let action = tx.rewrite_manifests()
//!     .target_size_bytes(16 * 1024 * 1024)  // 16 MB target
//!     .rewrite_if(|m| m.manifest_length < 5 * 1024 * 1024);  // Only small manifests
//! let tx = action.apply(tx)?;
//! let table = tx.commit(&catalog).await?;
//! ```
//!
//! # Atomicity
//!
//! The operation is atomic: manifests are only replaced on successful commit.
//! On failure, original manifests remain unchanged.
//!
//! # Format Version Support
//!
//! - **V1 tables**: Data manifests only
//! - **V2+ tables**: Data and delete manifests
//!
//! # Configuration
//!
//! Key options in [`RewriteManifestsOptions`]:
//!
//! - `target_manifest_size_bytes`: Target output manifest size (default: 8 MB)
//! - `spec_id`: Limit to specific partition spec
//! - `rewrite_delete_manifests`: Include delete manifests (default: true)
//! - `min_manifest_size_bytes`: Minimum size for candidates (default: 0)
//! - `max_entries_in_memory`: Max entries to hold in memory (default: 1,000,000; 0 disables)

mod options;
mod planner;
mod result;
mod rewriter;

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
pub use options::{
    DEFAULT_MAX_ENTRIES_IN_MEMORY, DEFAULT_TARGET_MANIFEST_SIZE_BYTES, RewriteManifestsOptions,
};
pub use planner::RewriteManifestsPlanner;
pub(crate) use planner::generate_manifest_path;
pub use result::RewriteManifestsResult;
pub(crate) use rewriter::ManifestRewriter;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{
    DataFileFormat, FormatVersion, MAIN_BRANCH, ManifestContentType, ManifestFile,
    ManifestListWriter, ManifestStatus, Operation, Snapshot, SnapshotReference, Summary,
};
use crate::table::Table;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

/// Predicate for selecting which manifests to rewrite.
///
/// The predicate receives a reference to each [`ManifestFile`] and returns
/// `true` if the manifest should be included in the rewrite operation.
///
/// # Example
///
/// ```ignore
/// // Rewrite manifests smaller than 5 MB
/// let predicate: ManifestPredicate = Arc::new(|m| m.manifest_length < 5 * 1024 * 1024);
/// ```
pub type ManifestPredicate = Arc<dyn Fn(&ManifestFile) -> bool + Send + Sync>;

/// Action for consolidating manifests using size-based batching.
///
/// This action combines small manifests into larger, target-sized manifests
/// while preserving data file references and clustering files by partition
/// for optimal query pruning.
///
/// # Configuration
///
/// Use builder methods to configure the action:
///
/// - `target_size_bytes()`: Set target manifest size
/// - `spec_id()`: Limit to specific partition spec
/// - `rewrite_if()`: Custom predicate for manifest selection
/// - `rewrite_delete_manifests()`: Include/exclude delete manifests
///
/// # Example
///
/// ```ignore
/// let tx = Transaction::new(&table);
/// let action = tx.rewrite_manifests()
///     .target_size_bytes(16 * 1024 * 1024)
///     .rewrite_if_smaller_than(10 * 1024 * 1024);
/// let tx = action.apply(tx)?;
/// let table = tx.commit(&catalog).await?;
/// ```
#[derive(Clone)]
pub struct RewriteManifestsAction {
    options: RewriteManifestsOptions,
    predicate: Option<ManifestPredicate>,
}

impl std::fmt::Debug for RewriteManifestsAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RewriteManifestsAction")
            .field("options", &self.options)
            .field("predicate", &self.predicate.is_some())
            .finish()
    }
}

impl Default for RewriteManifestsAction {
    fn default() -> Self {
        Self::new()
    }
}

impl RewriteManifestsAction {
    /// Create a new rewrite manifests action with default options.
    pub fn new() -> Self {
        Self {
            options: RewriteManifestsOptions::default(),
            predicate: None,
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Configuration Methods
    // ═══════════════════════════════════════════════════════════════════════

    /// Set target manifest size in bytes.
    ///
    /// Output manifests will be approximately this size.
    ///
    /// Default: 8 MB
    pub fn target_size_bytes(mut self, size: u64) -> Self {
        self.options.target_manifest_size_bytes = size;
        self
    }

    /// Rewrite manifests for a specific partition spec ID.
    ///
    /// When set, only manifests for this partition spec are rewritten.
    /// This is useful for tables that have undergone partition evolution.
    pub fn spec_id(mut self, spec_id: i32) -> Self {
        self.options.spec_id = Some(spec_id);
        self
    }

    /// Enable or disable delete manifest rewriting.
    ///
    /// When enabled (default), both data manifests and delete manifests
    /// are consolidated. When disabled, only data manifests are rewritten.
    pub fn rewrite_delete_manifests(mut self, enabled: bool) -> Self {
        self.options.rewrite_delete_manifests = enabled;
        self
    }

    /// Set minimum manifest size threshold for rewrite.
    ///
    /// When set to a non-zero value, only manifests smaller than this threshold
    /// are candidates for rewrite. When combined with a custom predicate, both
    /// conditions must match.
    pub fn min_manifest_size_bytes(mut self, size: u64) -> Self {
        self.options.min_manifest_size_bytes = size;
        self
    }

    /// Set the maximum number of manifest entries to hold in memory.
    ///
    /// Set to 0 to disable the limit.
    pub fn max_entries_in_memory(mut self, max_entries: u64) -> Self {
        self.options.max_entries_in_memory = if max_entries == 0 {
            None
        } else {
            Some(max_entries)
        };
        self
    }

    /// Disable the in-memory entry limit for manifest rewrite.
    pub fn disable_entry_limit(mut self) -> Self {
        self.options.max_entries_in_memory = None;
        self
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Predicate Methods
    // ═══════════════════════════════════════════════════════════════════════

    /// Filter which manifests to rewrite using a custom predicate.
    ///
    /// The predicate is called for each manifest and should return `true`
    /// if the manifest should be included in the rewrite operation.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Only rewrite manifests smaller than 10 MB
    /// let action = tx.rewrite_manifests()
    ///     .rewrite_if(|m| m.manifest_length < 10 * 1024 * 1024);
    /// ```
    pub fn rewrite_if<F>(mut self, predicate: F) -> Self
    where F: Fn(&ManifestFile) -> bool + Send + Sync + 'static {
        self.predicate = Some(Arc::new(predicate));
        self
    }

    /// Convenience: rewrite manifests smaller than the given size.
    ///
    /// Equivalent to `rewrite_if(|m| m.manifest_length < size_bytes)`.
    pub fn rewrite_if_smaller_than(self, size_bytes: u64) -> Self {
        self.rewrite_if(move |m| {
            u64::try_from(m.manifest_length)
                .map(|len| len < size_bytes)
                .unwrap_or(false)
        })
    }

    /// Convenience: rewrite manifests that have any deleted files tracked.
    ///
    /// This is useful for cleaning up manifests that contain references
    /// to deleted files that are no longer needed.
    pub fn rewrite_if_has_deletes(self) -> Self {
        self.rewrite_if(|m| m.has_deleted_files())
    }

    /// Get the options for this action.
    pub fn options(&self) -> &RewriteManifestsOptions {
        &self.options
    }
}

const META_ROOT_PATH: &str = "metadata";

#[async_trait]
impl TransactionAction for RewriteManifestsAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        // V3 format version requires row-lineage support (row-range + row-id stability)
        // which is not yet implemented. Reject V3 tables until properly supported.
        if table.metadata().format_version() == FormatVersion::V3 {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Rewrite manifests is not supported for V3 tables: row-lineage requirements not yet implemented",
            ));
        }

        // Validate table has a current snapshot
        let snapshot = table.metadata().current_snapshot().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Table has no snapshot; cannot rewrite manifests",
            )
        })?;

        // Load manifest list from current snapshot
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), &table.metadata_ref())
            .await?;

        let all_manifests: Vec<ManifestFile> = manifest_list.entries().to_vec();

        let max_entries_in_memory = self.options.max_entries_in_memory;
        let mut entry_count: u64 = 0;

        // Create planner and filter manifests
        let planner = RewriteManifestsPlanner::new(&self.options);

        // Separate data and delete manifests
        let data_manifests: Vec<ManifestFile> = all_manifests
            .iter()
            .filter(|m| m.content == ManifestContentType::Data)
            .cloned()
            .collect();
        let delete_manifests: Vec<ManifestFile> = all_manifests
            .iter()
            .filter(|m| m.content == ManifestContentType::Deletes)
            .cloned()
            .collect();

        // Filter data manifests using planner
        let data_manifests_to_rewrite =
            planner.filter_manifests(&data_manifests, self.predicate.as_ref());

        // Filter delete manifests if enabled
        let delete_manifests_to_rewrite = if self.options.rewrite_delete_manifests {
            planner.filter_manifests(&delete_manifests, self.predicate.as_ref())
        } else {
            vec![]
        };

        // Track which manifests are being rewritten (by path)
        let rewritten_manifest_paths: HashSet<&str> = data_manifests_to_rewrite
            .iter()
            .chain(delete_manifests_to_rewrite.iter())
            .map(|m| m.manifest_path.as_str())
            .collect();

        // If nothing to rewrite, return no-op
        if rewritten_manifest_paths.is_empty() {
            return Ok(ActionCommit::new(vec![], vec![]));
        }

        // Unchanged manifests (not being rewritten)
        let unchanged_manifests: Vec<ManifestFile> = all_manifests
            .into_iter()
            .filter(|m| !rewritten_manifest_paths.contains(m.manifest_path.as_str()))
            .collect();

        // Generate snapshot ID for the rewrite commit
        let snapshot_id = Self::generate_unique_snapshot_id(table);

        // Load entries from data manifests to rewrite
        let mut data_entries = Vec::new();
        for manifest in &data_manifests_to_rewrite {
            let loaded = manifest.load_manifest(table.file_io()).await?;
            for entry in loaded.entries() {
                // Only include existing/added files (not deleted)
                if entry.status != ManifestStatus::Deleted {
                    entry_count += 1;
                    if let Some(max_entries) = max_entries_in_memory
                        && entry_count > max_entries
                    {
                        return Err(Error::new(
                            ErrorKind::PreconditionFailed,
                            format!(
                                "Rewrite manifests exceeded in-memory entry limit ({entry_count} > {max_entries}). \
Increase with RewriteManifestsAction::max_entries_in_memory(), the table property commit.manifest.rewrite.max-entries, \
or disable with RewriteManifestsAction::disable_entry_limit()."
                            ),
                        ));
                    }
                    data_entries.push(entry.clone());
                }
            }
        }

        // Load entries from delete manifests to rewrite
        let mut delete_entries = Vec::new();
        for manifest in &delete_manifests_to_rewrite {
            let loaded = manifest.load_manifest(table.file_io()).await?;
            for entry in loaded.entries() {
                if entry.status != ManifestStatus::Deleted {
                    entry_count += 1;
                    if let Some(max_entries) = max_entries_in_memory
                        && entry_count > max_entries
                    {
                        return Err(Error::new(
                            ErrorKind::PreconditionFailed,
                            format!(
                                "Rewrite manifests exceeded in-memory entry limit ({entry_count} > {max_entries}). \
Increase with RewriteManifestsAction::max_entries_in_memory(), the table property commit.manifest.rewrite.max-entries, \
or disable with RewriteManifestsAction::disable_entry_limit()."
                            ),
                        ));
                    }
                    delete_entries.push(entry.clone());
                }
            }
        }

        // Track file counts (before == after)
        let original_file_count: u64 = (data_entries.len() + delete_entries.len()) as u64;

        // Sort entries by partition for optimal clustering
        let sorted_data_entries = planner.sort_entries_by_partition(data_entries);
        let sorted_delete_entries = planner.sort_entries_by_partition(delete_entries);

        // Create rewriter
        let mut rewriter = ManifestRewriter::new(
            table.metadata().location().to_string(),
            table.metadata().format_version(),
            snapshot_id,
            self.options.clone(),
        );

        // Write new manifests grouped by partition spec ID
        let schema = table.metadata().current_schema().clone();
        let mut new_manifests = Vec::new();
        let mut new_file_count: u64 = 0;

        // Data manifests
        let mut data_entries_iter = sorted_data_entries.into_iter().peekable();
        while let Some(entry) = data_entries_iter.next() {
            let spec_id = entry.data_file.partition_spec_id;
            let mut entries_for_spec = vec![entry];
            while let Some(next) = data_entries_iter.peek() {
                if next.data_file.partition_spec_id != spec_id {
                    break;
                }
                entries_for_spec.push(data_entries_iter.next().unwrap());
            }

            let partition_spec = table
                .metadata()
                .partition_spec_by_id(spec_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Unknown partition spec id: {spec_id}"),
                    )
                })?
                .clone();

            let batches = planner.batch_entries(entries_for_spec);
            new_file_count += batches.iter().map(|b| b.len() as u64).sum::<u64>();
            let manifests = rewriter
                .write_manifests(
                    table.file_io(),
                    batches,
                    ManifestContentType::Data,
                    schema.clone(),
                    partition_spec.clone(),
                )
                .await?;
            new_manifests.extend(manifests);
        }

        // Delete manifests
        let mut delete_entries_iter = sorted_delete_entries.into_iter().peekable();
        while let Some(entry) = delete_entries_iter.next() {
            let spec_id = entry.data_file.partition_spec_id;
            let mut entries_for_spec = vec![entry];
            while let Some(next) = delete_entries_iter.peek() {
                if next.data_file.partition_spec_id != spec_id {
                    break;
                }
                entries_for_spec.push(delete_entries_iter.next().unwrap());
            }

            let partition_spec = table
                .metadata()
                .partition_spec_by_id(spec_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Unknown partition spec id: {spec_id}"),
                    )
                })?
                .clone();

            let batches = planner.batch_entries(entries_for_spec);
            new_file_count += batches.iter().map(|b| b.len() as u64).sum::<u64>();
            let manifests = rewriter
                .write_manifests(
                    table.file_io(),
                    batches,
                    ManifestContentType::Deletes,
                    schema.clone(),
                    partition_spec.clone(),
                )
                .await?;
            new_manifests.extend(manifests);
        }

        // Validate file counts (before == after)
        if original_file_count != new_file_count {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "File count mismatch after rewrite: original={original_file_count}, new={new_file_count}"
                ),
            ));
        }

        // Combine unchanged and new manifests
        let all_new_manifests: Vec<ManifestFile> = unchanged_manifests
            .into_iter()
            .chain(new_manifests)
            .collect();

        // Write manifest list
        let next_seq_num = table.metadata().next_sequence_number();
        let manifest_list_path = format!(
            "{}/{}/snap-{}-0-{}.{}",
            table.metadata().location(),
            META_ROOT_PATH,
            snapshot_id,
            rewriter.commit_uuid(),
            DataFileFormat::Avro
        );

        let mut manifest_list_writer = match table.metadata().format_version() {
            FormatVersion::V1 => ManifestListWriter::v1(
                table.file_io().new_output(&manifest_list_path)?,
                snapshot_id,
                table.metadata().current_snapshot_id(),
            ),
            FormatVersion::V2 => ManifestListWriter::v2(
                table.file_io().new_output(&manifest_list_path)?,
                snapshot_id,
                table.metadata().current_snapshot_id(),
                next_seq_num,
            ),
            FormatVersion::V3 => {
                let first_row_id = table.metadata().next_row_id();
                ManifestListWriter::v3(
                    table.file_io().new_output(&manifest_list_path)?,
                    snapshot_id,
                    table.metadata().current_snapshot_id(),
                    next_seq_num,
                    Some(first_row_id),
                )
            }
        };

        manifest_list_writer.add_manifests(all_new_manifests.into_iter())?;
        manifest_list_writer.close().await?;

        // Build summary for the snapshot
        let summary = Summary {
            operation: Operation::Replace,
            additional_properties: Default::default(),
        };

        // Create new snapshot
        let commit_ts = chrono::Utc::now().timestamp_millis();
        let new_snapshot = Snapshot::builder()
            .with_manifest_list(manifest_list_path)
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(table.metadata().current_snapshot_id())
            .with_sequence_number(next_seq_num)
            .with_summary(summary)
            .with_schema_id(table.metadata().current_schema_id())
            .with_timestamp_ms(commit_ts)
            .build();

        // Create table updates
        let current_main_ref = table.metadata().refs().get(MAIN_BRANCH).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Missing {MAIN_BRANCH} branch reference in table metadata"),
            )
        })?;
        if !current_main_ref.is_branch() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("{MAIN_BRANCH} reference is not a branch"),
            ));
        }

        let main_ref = SnapshotReference::new(snapshot_id, current_main_ref.retention.clone());

        let updates = vec![
            TableUpdate::AddSnapshot {
                snapshot: new_snapshot,
            },
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
                reference: main_ref,
            },
        ];

        // Create requirements
        let requirements = vec![
            TableRequirement::UuidMatch {
                uuid: table.metadata().uuid(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: table.metadata().current_snapshot_id(),
            },
        ];

        Ok(ActionCommit::new(updates, requirements))
    }
}

impl RewriteManifestsAction {
    /// Generate a unique snapshot ID that doesn't conflict with existing snapshots.
    fn generate_unique_snapshot_id(table: &Table) -> i64 {
        let generate_random_id = || -> i64 {
            let (lhs, rhs) = Uuid::new_v4().as_u64_pair();
            let snapshot_id = (lhs ^ rhs) as i64;
            if snapshot_id < 0 {
                -snapshot_id
            } else {
                snapshot_id
            }
        };

        let mut snapshot_id = generate_random_id();
        while table
            .metadata()
            .snapshots()
            .any(|s| s.snapshot_id() == snapshot_id)
        {
            snapshot_id = generate_random_id();
        }
        snapshot_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_action_default() {
        let action = RewriteManifestsAction::new();
        assert_eq!(
            action.options.target_manifest_size_bytes,
            DEFAULT_TARGET_MANIFEST_SIZE_BYTES
        );
        assert!(action.options.spec_id.is_none());
        assert!(action.options.rewrite_delete_manifests);
        assert!(action.predicate.is_none());
        assert_eq!(
            action.options.max_entries_in_memory,
            Some(DEFAULT_MAX_ENTRIES_IN_MEMORY)
        );
    }

    #[test]
    fn test_builder_target_size() {
        let action = RewriteManifestsAction::new().target_size_bytes(16 * 1024 * 1024);

        assert_eq!(action.options.target_manifest_size_bytes, 16 * 1024 * 1024);
    }

    #[test]
    fn test_builder_spec_id() {
        let action = RewriteManifestsAction::new().spec_id(42);

        assert_eq!(action.options.spec_id, Some(42));
    }

    #[test]
    fn test_builder_rewrite_delete_manifests() {
        let action = RewriteManifestsAction::new().rewrite_delete_manifests(false);

        assert!(!action.options.rewrite_delete_manifests);
    }

    #[test]
    fn test_builder_min_manifest_size() {
        let action = RewriteManifestsAction::new().min_manifest_size_bytes(1024 * 1024);

        assert_eq!(action.options.min_manifest_size_bytes, 1024 * 1024);
    }

    #[test]
    fn test_builder_max_entries_in_memory() {
        let action = RewriteManifestsAction::new().max_entries_in_memory(5000);

        assert_eq!(action.options.max_entries_in_memory, Some(5000));
    }

    #[test]
    fn test_builder_disable_entry_limit() {
        let action = RewriteManifestsAction::new().disable_entry_limit();

        assert!(action.options.max_entries_in_memory.is_none());
    }

    #[test]
    fn test_builder_predicate() {
        let action =
            RewriteManifestsAction::new().rewrite_if(|m| m.manifest_length < 5 * 1024 * 1024);

        assert!(action.predicate.is_some());
    }

    #[test]
    fn test_builder_chaining() {
        let action = RewriteManifestsAction::new()
            .target_size_bytes(16 * 1024 * 1024)
            .spec_id(1)
            .rewrite_delete_manifests(false)
            .min_manifest_size_bytes(1024 * 1024)
            .max_entries_in_memory(100)
            .rewrite_if_smaller_than(10 * 1024 * 1024);

        assert_eq!(action.options.target_manifest_size_bytes, 16 * 1024 * 1024);
        assert_eq!(action.options.spec_id, Some(1));
        assert!(!action.options.rewrite_delete_manifests);
        assert_eq!(action.options.min_manifest_size_bytes, 1024 * 1024);
        assert_eq!(action.options.max_entries_in_memory, Some(100));
        assert!(action.predicate.is_some());
    }

    #[tokio::test]
    async fn test_commit_table_no_snapshot() {
        // Table with no current snapshot should error
        let table = crate::transaction::tests::make_v2_minimal_table();
        assert!(table.metadata().current_snapshot_id().is_none());

        let result = Arc::new(RewriteManifestsAction::new()).commit(&table).await;
        let err = match result {
            Ok(_) => panic!("expected error for table with no snapshot"),
            Err(e) => e,
        };
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("no snapshot"));
    }

    #[tokio::test]
    async fn test_commit_v3_unsupported() {
        // V3 format version requires row-lineage support which is not yet implemented
        let table = make_v3_minimal_table();
        assert_eq!(
            table.metadata().format_version(),
            crate::spec::FormatVersion::V3
        );

        let result = Arc::new(RewriteManifestsAction::new()).commit(&table).await;
        let err = match result {
            Ok(_) => panic!("expected error for V3 format version"),
            Err(e) => e,
        };
        assert_eq!(err.kind(), ErrorKind::FeatureUnsupported);
        assert!(err.message().contains("V3"));
    }

    /// Create a V3 minimal table for testing format version checks.
    fn make_v3_minimal_table() -> crate::table::Table {
        use std::fs::File;
        use std::io::BufReader;

        use crate::TableIdent;
        use crate::io::FileIOBuilder;
        use crate::spec::TableMetadata;
        use crate::table::Table;

        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV3ValidMinimal.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap()
    }
}

/// Integration tests that create actual manifest files and test the full commit flow.
#[cfg(test)]
mod integration_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::*;
    use crate::TableIdent;
    use crate::io::FileIOBuilder;
    use crate::spec::{
        DataContentType, DataFile, DataFileFormat, ManifestListWriter, ManifestWriterBuilder,
        NestedField, PartitionSpec, PrimitiveType, Schema, SnapshotRetention, SortOrder,
        StructType, TableMetadata, Type,
    };
    use crate::table::Table;

    /// Create a test schema with a single int field.
    fn test_schema() -> Arc<Schema> {
        Arc::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::optional(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                ))])
                .build()
                .unwrap(),
        )
    }

    /// Create a test partition spec (unpartitioned).
    fn test_partition_spec() -> Arc<PartitionSpec> {
        Arc::new(PartitionSpec::unpartition_spec())
    }

    /// Create a test data file.
    fn test_data_file(path: &str) -> DataFile {
        DataFile {
            content: DataContentType::Data,
            file_path: path.to_string(),
            file_format: DataFileFormat::Parquet,
            partition: crate::spec::Struct::empty(),
            record_count: 100,
            file_size_in_bytes: 1024,
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
            sort_order_id: None,
            partition_spec_id: 0,
            first_row_id: None,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        }
    }

    /// Create a table with actual manifest files on disk for end-to-end testing.
    async fn create_table_with_manifests(
        tmp_dir: &TempDir,
    ) -> (Table, Vec<ManifestFile>, crate::io::FileIO) {
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let io = FileIOBuilder::new_fs_io().build().unwrap();

        let schema = test_schema();
        let partition_spec = test_partition_spec();

        // Create metadata directory
        std::fs::create_dir_all(tmp_dir.path().join("metadata")).unwrap();

        // Write manifest files (2 small manifests that should be consolidated)
        let mut manifests = Vec::new();

        for i in 0..2 {
            let manifest_path = format!("{table_location}/metadata/manifest-{i}.avro");
            let output = io.new_output(&manifest_path).unwrap();

            let mut writer = ManifestWriterBuilder::new(
                output,
                Some(1), // snapshot_id
                None,    // key_metadata
                schema.clone(),
                (*partition_spec).clone(),
            )
            .build_v2_data();

            // Add test entries using add_file
            writer
                .add_file(test_data_file(&format!("data/file-{i}-a.parquet")), 1)
                .unwrap();
            writer
                .add_file(test_data_file(&format!("data/file-{i}-b.parquet")), 1)
                .unwrap();

            let manifest = writer.write_manifest_file().await.unwrap();
            manifests.push(manifest);
        }

        // Write manifest list
        let manifest_list_path = format!("{table_location}/metadata/snap-1-0-uuid.avro");
        let mut manifest_list_writer = ManifestListWriter::v2(
            io.new_output(&manifest_list_path).unwrap(),
            1,    // snapshot_id
            None, // parent_snapshot_id
            1,    // sequence_number
        );
        manifest_list_writer
            .add_manifests(manifests.clone().into_iter())
            .unwrap();
        manifest_list_writer.close().await.unwrap();

        // Create snapshot
        let snapshot = Snapshot::builder()
            .with_snapshot_id(1)
            .with_sequence_number(1)
            .with_timestamp_ms(1000)
            .with_manifest_list(manifest_list_path)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build();

        // Create table metadata
        let metadata = TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: uuid::Uuid::new_v4(),
            location: table_location.clone(),
            last_sequence_number: 1,
            last_updated_ms: 1000,
            last_column_id: 1,
            current_schema_id: 0,
            schemas: HashMap::from([(0, schema)]),
            partition_specs: HashMap::from([(0, partition_spec)]),
            default_spec: Arc::new(PartitionSpec::unpartition_spec()),
            default_partition_type: StructType::new(vec![]),
            last_partition_id: -1,
            properties: HashMap::new(),
            current_snapshot_id: Some(1),
            snapshots: HashMap::from([(1, Arc::new(snapshot))]),
            snapshot_log: vec![],
            sort_orders: HashMap::from([(0, Arc::new(SortOrder::unsorted_order()))]),
            metadata_log: vec![],
            default_sort_order_id: 0,
            refs: HashMap::from([(
                MAIN_BRANCH.to_string(),
                SnapshotReference::new(1, SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                }),
            )]),
            statistics: HashMap::new(),
            partition_statistics: HashMap::new(),
            encryption_keys: HashMap::new(),
            next_row_id: 0,
        };

        let table = Table::builder()
            .metadata(metadata)
            .metadata_location(format!("{table_location}/metadata/v1.json"))
            .identifier(TableIdent::from_strs(["db", "table"]).unwrap())
            .file_io(io.clone())
            .build()
            .unwrap();

        (table, manifests, io)
    }

    #[tokio::test]
    async fn test_commit_end_to_end_with_actual_manifests() {
        // Create table with real manifest files
        let tmp_dir = TempDir::new().unwrap();
        let (table, original_manifests, io) = create_table_with_manifests(&tmp_dir).await;

        // Verify initial state
        assert_eq!(original_manifests.len(), 2);
        assert_eq!(table.metadata().current_snapshot_id(), Some(1));

        // Execute rewrite action
        let action = Arc::new(RewriteManifestsAction::new());
        let result = action.commit(&table).await;

        // Verify commit succeeds
        let mut commit = match result {
            Ok(c) => c,
            Err(e) => panic!("Expected successful commit, got error: {e}"),
        };

        // Verify updates
        let updates = commit.take_updates();
        assert_eq!(
            updates.len(),
            2,
            "Expected 2 updates (AddSnapshot + SetSnapshotRef)"
        );

        // First update should be AddSnapshot
        match &updates[0] {
            TableUpdate::AddSnapshot { snapshot } => {
                assert_ne!(
                    snapshot.snapshot_id(),
                    1,
                    "New snapshot should have different ID"
                );
                assert_eq!(snapshot.parent_snapshot_id(), Some(1));
                assert_eq!(snapshot.summary().operation, Operation::Replace);
            }
            other => panic!("Expected AddSnapshot, got {other:?}"),
        }

        // Second update should be SetSnapshotRef for main
        match &updates[1] {
            TableUpdate::SetSnapshotRef {
                ref_name,
                reference,
            } => {
                assert_eq!(ref_name, MAIN_BRANCH);
                assert!(reference.is_branch());
            }
            other => panic!("Expected SetSnapshotRef, got {other:?}"),
        }

        // Verify requirements
        let requirements = commit.take_requirements();
        assert_eq!(requirements.len(), 2, "Expected 2 requirements");

        // Verify UuidMatch requirement
        let has_uuid_match = requirements.iter().any(|r| {
            matches!(r, TableRequirement::UuidMatch { uuid } if uuid == &table.metadata().uuid())
        });
        assert!(has_uuid_match, "Should have UuidMatch requirement");

        // Verify RefSnapshotIdMatch requirement
        let has_ref_match = requirements.iter().any(|r| {
            matches!(
                r,
                TableRequirement::RefSnapshotIdMatch { r#ref, snapshot_id }
                if r#ref == MAIN_BRANCH && snapshot_id == &Some(1)
            )
        });
        assert!(has_ref_match, "Should have RefSnapshotIdMatch requirement");

        // Verify the new manifest list was written
        if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            let manifest_list_path = snapshot.manifest_list();
            assert!(
                io.new_input(manifest_list_path)
                    .unwrap()
                    .exists()
                    .await
                    .unwrap(),
                "New manifest list should exist"
            );
        }
    }

    #[tokio::test]
    async fn test_commit_no_op_when_nothing_to_rewrite() {
        // Create table with real manifest files
        let tmp_dir = TempDir::new().unwrap();
        let (table, _original_manifests, _io) = create_table_with_manifests(&tmp_dir).await;

        // Use spec_id filter that matches nothing
        let action = Arc::new(RewriteManifestsAction::new().spec_id(999)); // Non-existent spec

        let result = action.commit(&table).await;

        // Should return no-op (empty updates/requirements)
        let mut commit = result.expect("No-op should not error");
        assert!(
            commit.take_updates().is_empty(),
            "No-op should have no updates"
        );
        assert!(
            commit.take_requirements().is_empty(),
            "No-op should have no requirements"
        );
    }

    #[tokio::test]
    async fn test_commit_preserves_file_count() {
        // Create table with real manifest files
        let tmp_dir = TempDir::new().unwrap();
        let (table, original_manifests, io) = create_table_with_manifests(&tmp_dir).await;

        // Count original files - for added entries it's added_files_count
        let original_file_count: usize = original_manifests
            .iter()
            .map(|m| m.added_files_count.unwrap_or(0) as usize)
            .sum();
        assert_eq!(
            original_file_count, 4,
            "Should have 4 files total (2 per manifest)"
        );

        // Execute rewrite
        let action = Arc::new(RewriteManifestsAction::new());
        let mut commit = action.commit(&table).await.expect("Commit should succeed");

        // Verify the new snapshot exists and we can check file counts
        let updates = commit.take_updates();
        if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            let manifest_list = snapshot
                .load_manifest_list(&io, &table.metadata_ref())
                .await
                .expect("Should load manifest list");

            let new_file_count: usize = manifest_list
                .entries()
                .iter()
                .map(|m| m.existing_files_count.unwrap_or(0) as usize)
                .sum();

            assert_eq!(
                new_file_count, original_file_count,
                "File count should be preserved after rewrite"
            );
        }
    }

    #[tokio::test]
    async fn test_commit_respects_entry_limit() {
        let tmp_dir = TempDir::new().unwrap();
        let (table, _original_manifests, _io) = create_table_with_manifests(&tmp_dir).await;

        let action = Arc::new(RewriteManifestsAction::new().max_entries_in_memory(1));
        let err = action
            .commit(&table)
            .await
            .expect_err("Expected entry limit to be enforced");

        assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
        assert!(err.message().contains("entry limit"));
    }
}
