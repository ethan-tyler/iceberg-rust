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

//! Snapshot expiration for Apache Iceberg tables.
//!
//! This module provides infrastructure for safely removing old snapshots from
//! Iceberg tables. Snapshot expiration is essential for production table maintenance:
//!
//! - **Storage Cost Control**: Remove snapshots no longer needed for time travel
//! - **Metadata Performance**: Keep table metadata small and fast to load
//! - **Compliance**: Support data retention policies with configurable windows
//!
//! # File Deletion Modes
//!
//! The implementation supports three modes:
//!
//! 1. **Metadata only** (default): Removes snapshots from table metadata only.
//!    Orphaned files remain and can be cleaned up separately.
//!
//! 2. **Dry-run**: Computes what would be deleted without making changes.
//!    Useful for previewing the impact of expiration.
//!
//! 3. **Full cleanup**: Deletes unreferenced files after metadata commit.
//!    Requires explicit opt-in via `delete_files(true)`.
//!
//! # Architecture
//!
//! The implementation follows the Apache Iceberg specification for snapshot retention:
//!
//! 1. **Retention Policy Evaluation**: Determine which snapshots are protected by
//!    branches, tags, and retention settings
//! 2. **Ref Expiration**: Remove non-main refs that exceed `max-ref-age-ms`
//! 3. **Cleanup Planning**: Identify unreferenced files for deletion
//! 4. **Metadata Commit**: Atomically remove expired snapshots and refs
//! 5. **File Cleanup**: Delete unreferenced files (only after metadata commit succeeds)
//!
//! # Safety
//!
//! The implementation includes critical safety guardrails:
//!
//! - Never deletes files still referenced by retained snapshots or refs
//! - Files are only deleted AFTER metadata commit succeeds
//! - Respects branch and tag references
//! - The main branch never expires
//! - Uses table requirements for optimistic concurrency control
//!
//! # Example: Metadata-only expiration
//!
//! ```ignore
//! use chrono::{Duration, Utc};
//! use iceberg::transaction::{Transaction, ApplyTransactionAction};
//!
//! // Expire snapshots older than 7 days, keep at least 10
//! let tx = Transaction::new(&table);
//! let action = tx.expire_snapshots()
//!     .older_than(Utc::now() - Duration::days(7))
//!     .retain_last(10);
//! let tx = action.apply(tx)?;
//! let table = tx.commit(&catalog).await?;
//! ```
//!
//! # Example: Full cleanup with file deletion
//!
//! ```ignore
//! use chrono::{Duration, Utc};
//! use iceberg::transaction::{
//!     Transaction, ApplyTransactionAction,
//!     expire_snapshots::{execute_cleanup_plan}
//! };
//!
//! // Create action with file deletion enabled
//! let tx = Transaction::new(&table);
//! let action = tx.expire_snapshots()
//!     .older_than(Utc::now() - Duration::days(7))
//!     .retain_last(10)
//!     .delete_files(true);
//!
//! // Plan the cleanup BEFORE committing
//! let cleanup_plan = action.plan_cleanup(&table).await?;
//!
//! // Commit metadata changes
//! let tx = action.apply(tx)?;
//! let table = tx.commit(&catalog).await?;
//!
//! // Execute file cleanup AFTER commit succeeds
//! let cleanup_result = execute_cleanup_plan(
//!     table.file_io(),
//!     &cleanup_plan,
//!     None, // Optional progress callback
//! ).await?;
//!
//! println!("Deleted {} files", cleanup_result.total_deleted_files());
//! ```

mod cleanup;
mod result;
mod retention;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
pub use cleanup::{
    CleanupExecutionResult, CleanupPlan, compute_cleanup_plan, execute_cleanup_plan,
};
pub use result::{ExpireProgressCallback, ExpireProgressEvent, ExpireSnapshotsResult};
pub use retention::{
    RetentionPolicy, RetentionResult, compute_retained_snapshots, compute_retention,
};

use crate::catalog::TableRequirement;
use crate::error::{Error, ErrorKind, Result};
use crate::spec::TableMetadata;
use crate::table::Table;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Catalog, TableUpdate};

/// Level of cleanup to perform after expiring snapshots.
///
/// This enum is reserved for future configuration. Use `delete_files` and
/// `dry_run` on [`ExpireSnapshotsAction`] to control cleanup behavior today.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CleanupLevel {
    /// Delete snapshots from metadata only, no file deletion.
    #[default]
    None,

    /// Delete snapshots from metadata, delete only manifest files (not data files).
    MetadataOnly,

    /// Delete snapshots from metadata, delete unreferenced data and manifest files.
    Full,
}

/// Options for file deletion during snapshot expiration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ExpireOptions {
    /// If true, compute what would be deleted but don't actually delete anything.
    /// Returns the same result structure with counts of what would be deleted.
    pub dry_run: bool,

    /// If true, delete unreferenced files after expiring snapshot metadata.
    /// When false (default), only metadata is updated.
    ///
    /// **Safety**: Files are only deleted AFTER the metadata commit succeeds.
    /// If the metadata commit fails, no files are deleted.
    pub delete_files: bool,
}

impl ExpireOptions {
    /// Create options for dry-run mode (no changes made).
    pub fn dry_run() -> Self {
        Self {
            dry_run: true,
            delete_files: false,
        }
    }

    /// Create options for full cleanup (metadata + file deletion).
    pub fn with_file_deletion() -> Self {
        Self {
            dry_run: false,
            delete_files: true,
        }
    }
}

/// Planned snapshot expiration and file cleanup.
#[derive(Debug, Clone)]
pub struct ExpireSnapshotsPlan {
    expired_snapshot_ids: Vec<i64>,
    expired_ref_names: Vec<String>,
    cleanup_plan: CleanupPlan,
    requirements: Vec<TableRequirement>,
}

impl ExpireSnapshotsPlan {
    /// Snapshot IDs that will be expired.
    pub fn expired_snapshot_ids(&self) -> &[i64] {
        &self.expired_snapshot_ids
    }

    /// Snapshot refs (branches/tags) that will be removed.
    pub fn expired_ref_names(&self) -> &[String] {
        &self.expired_ref_names
    }

    /// File cleanup plan derived from expired snapshots.
    pub fn cleanup_plan(&self) -> &CleanupPlan {
        &self.cleanup_plan
    }

    /// Returns true if there are no snapshot or ref changes to apply.
    pub fn is_empty(&self) -> bool {
        self.expired_snapshot_ids.is_empty() && self.expired_ref_names.is_empty()
    }

    fn build_updates(&self) -> Vec<TableUpdate> {
        let mut updates = Vec::new();

        for ref_name in &self.expired_ref_names {
            updates.push(TableUpdate::RemoveSnapshotRef {
                ref_name: ref_name.clone(),
            });
        }

        if !self.expired_snapshot_ids.is_empty() {
            updates.push(TableUpdate::RemoveSnapshots {
                snapshot_ids: self.expired_snapshot_ids.clone(),
            });
        }

        updates
    }

    fn action_commit(&self) -> ActionCommit {
        ActionCommit::new(self.build_updates(), self.requirements.clone())
    }

    fn build_result(
        &self,
        options: ExpireOptions,
        duration: Duration,
        cleanup_result: Option<&CleanupExecutionResult>,
    ) -> ExpireSnapshotsResult {
        let mut result = ExpireSnapshotsResult {
            deleted_snapshots_count: self.expired_snapshot_ids.len() as u64,
            deleted_refs_count: self.expired_ref_names.len() as u64,
            duration_ms: duration.as_millis() as u64,
            ..Default::default()
        };

        if options.delete_files {
            if let Some(cleanup_result) = cleanup_result {
                result.deleted_data_files_count = cleanup_result.deleted_data_files;
                result.deleted_position_delete_files_count =
                    cleanup_result.deleted_position_delete_files;
                result.deleted_equality_delete_files_count =
                    cleanup_result.deleted_equality_delete_files;
                result.deleted_manifest_files_count = cleanup_result.deleted_manifest_files;
                result.deleted_manifest_list_files_count =
                    cleanup_result.deleted_manifest_list_files;
            } else {
                result.deleted_data_files_count = self.cleanup_plan.data_files.len() as u64;
                result.deleted_position_delete_files_count =
                    self.cleanup_plan.position_delete_files.len() as u64;
                result.deleted_equality_delete_files_count =
                    self.cleanup_plan.equality_delete_files.len() as u64;
                result.deleted_manifest_files_count = self.cleanup_plan.manifest_files.len() as u64;
                result.deleted_manifest_list_files_count =
                    self.cleanup_plan.manifest_list_files.len() as u64;
            }

            result.total_bytes_freed = self.cleanup_plan.total_bytes;
        }

        result
    }
}

/// Action for expiring old snapshots from an Iceberg table.
///
/// This action removes snapshots that are no longer needed for time travel
/// or branch/tag references. It can also remove expired refs (branches/tags
/// that exceed their `max-ref-age-ms` setting).
///
/// # File Deletion Modes
///
/// - **Metadata only** (default): Removes snapshots from table metadata only.
///   Orphaned files can be cleaned up separately.
/// - **Dry-run**: Computes what would be deleted without making changes.
/// - **Full cleanup**: Deletes unreferenced files after metadata commit.
///
/// # Configuration
///
/// Use builder methods to customize expiration behavior:
///
/// - `older_than()`: Expire snapshots older than a timestamp
/// - `retain_last()`: Keep at least N most recent snapshots per branch
/// - `expire_snapshot_id()`: Explicitly expire specific snapshots by ID
/// - `delete_files(true)`: Enable file deletion (explicit opt-in required)
///
/// # Safety
///
/// The action validates that:
/// - The current snapshot cannot be expired
/// - Snapshots referenced by active branches/tags are protected
/// - Explicitly requested snapshot IDs exist in the table
/// - Uses `TableRequirement::UuidMatch` for optimistic concurrency
/// - Files are only deleted AFTER metadata commit succeeds
pub struct ExpireSnapshotsAction {
    /// Expire snapshots older than this timestamp.
    older_than: Option<DateTime<Utc>>,

    /// Retain at least this many snapshots per branch.
    retain_last: Option<u32>,

    /// Explicitly expire these snapshot IDs.
    snapshot_ids_to_expire: HashSet<i64>,

    /// Use table properties for retention policy.
    use_table_properties: bool,

    /// Whether to delete files (requires explicit opt-in).
    delete_files: bool,

    /// Dry-run mode (compute but don't execute).
    dry_run: bool,
}

impl ExpireSnapshotsAction {
    /// Creates a new expire snapshots action with default settings.
    pub(crate) fn new() -> Self {
        Self {
            older_than: None,
            retain_last: None,
            snapshot_ids_to_expire: HashSet::new(),
            use_table_properties: false,
            delete_files: false,
            dry_run: false,
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Snapshot Selection
    // ═══════════════════════════════════════════════════════════════════════

    /// Expire snapshots older than the given timestamp.
    ///
    /// Snapshots with a timestamp before this value are candidates for expiration,
    /// subject to branch/tag protection and `retain_last` settings.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use chrono::{Duration, Utc};
    ///
    /// // Expire snapshots older than 7 days
    /// let action = tx.expire_snapshots()
    ///     .older_than(Utc::now() - Duration::days(7));
    /// ```
    #[must_use]
    pub fn older_than(mut self, timestamp: DateTime<Utc>) -> Self {
        self.older_than = Some(timestamp);
        self
    }

    /// Retain at least N most recent snapshots per branch.
    ///
    /// This setting provides a safety net to ensure recent snapshots are
    /// preserved regardless of age. It applies per-branch, so each branch
    /// will retain its own N most recent ancestors.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Keep at least 10 snapshots even if older than the cutoff
    /// let action = tx.expire_snapshots()
    ///     .older_than(Utc::now() - Duration::days(7))
    ///     .retain_last(10);
    /// ```
    #[must_use]
    pub fn retain_last(mut self, count: u32) -> Self {
        self.retain_last = Some(count);
        self
    }

    /// Expire a specific snapshot by ID.
    ///
    /// Multiple calls can be chained to expire multiple specific snapshots.
    /// The operation will fail if the snapshot ID does not exist or is
    /// protected by a branch or tag.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let action = tx.expire_snapshots()
    ///     .expire_snapshot_id(12345678901234)
    ///     .expire_snapshot_id(23456789012345);
    /// ```
    #[must_use]
    pub fn expire_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_ids_to_expire.insert(snapshot_id);
        self
    }

    /// Use table properties for retention policy.
    ///
    /// When enabled, reads retention settings from table properties:
    /// - `history.expire.max-snapshot-age-ms`: Maximum snapshot age
    /// - `history.expire.min-snapshots-to-keep`: Minimum snapshots to retain
    /// - `history.expire.max-ref-age-ms`: Maximum age for refs
    ///
    /// These properties provide defaults that can be overridden by
    /// explicit `older_than()` and `retain_last()` calls.
    #[must_use]
    pub fn use_table_properties(mut self, enabled: bool) -> Self {
        self.use_table_properties = enabled;
        self
    }

    // ═══════════════════════════════════════════════════════════════════════
    // File Deletion Options
    // ═══════════════════════════════════════════════════════════════════════

    /// Enable file deletion after expiring snapshots.
    ///
    /// When enabled, unreferenced files (data files, delete files, manifests,
    /// and manifest lists) will be deleted after the metadata commit succeeds.
    ///
    /// **Important**: This is an explicit opt-in. By default, only metadata
    /// is updated and orphaned files must be cleaned up separately.
    ///
    /// File deletion is performed by [`Self::execute`] or by running
    /// [`execute_cleanup_plan`] with a plan created by [`Self::plan_cleanup`].
    ///
    /// # Safety
    ///
    /// - Files are only deleted AFTER the metadata commit succeeds
    /// - Never deletes files referenced by retained snapshots or refs
    /// - If the metadata commit fails, no files are deleted
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Delete unreferenced files after expiring snapshots
    /// let action = tx.expire_snapshots()
    ///     .older_than(Utc::now() - Duration::days(7))
    ///     .delete_files(true);
    /// ```
    #[must_use]
    pub fn delete_files(mut self, enabled: bool) -> Self {
        self.delete_files = enabled;
        self
    }

    /// Enable dry-run mode.
    ///
    /// When enabled, computes what would be expired/deleted but makes no
    /// changes. The returned result shows the counts of what would be
    /// affected.
    ///
    /// Useful for previewing the impact of expiration before committing.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Preview what would be deleted
    /// let action = tx.expire_snapshots()
    ///     .older_than(Utc::now() - Duration::days(7))
    ///     .delete_files(true)
    ///     .dry_run(true);
    /// ```
    #[must_use]
    pub fn dry_run(mut self, enabled: bool) -> Self {
        self.dry_run = enabled;
        self
    }

    /// Get the current options for this action.
    pub fn options(&self) -> ExpireOptions {
        ExpireOptions {
            dry_run: self.dry_run,
            delete_files: self.delete_files,
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Cleanup Planning
    // ═══════════════════════════════════════════════════════════════════════

    /// Plan snapshot expiration and file cleanup.
    ///
    /// The plan contains:
    /// - Snapshot IDs to expire
    /// - Snapshot refs to remove
    /// - File cleanup plan for unreferenced files
    ///
    /// The plan is tied to the current table state via optimistic requirements.
    /// If the table changes before executing, the commit will fail safely.
    pub async fn plan(&self, table: &Table) -> Result<ExpireSnapshotsPlan> {
        let metadata = table.metadata();
        let file_io = table.file_io();
        let now = Utc::now();

        // Validate configuration against current metadata
        self.validate(metadata)?;

        // Get retention policy
        let policy = if self.use_table_properties {
            RetentionPolicy::from_table_properties(metadata.properties())
        } else {
            RetentionPolicy::default()
        };

        // Compute retention (which snapshots to keep, which refs expired)
        let retention_result = compute_retention(metadata, &policy, now)?;

        // Compute snapshots to expire
        let to_expire =
            self.compute_snapshots_to_expire(metadata, &retention_result, &policy, now)?;

        let cleanup_plan = if to_expire.is_empty() {
            CleanupPlan::default()
        } else {
            compute_cleanup_plan(
                file_io,
                metadata,
                &to_expire,
                &retention_result.retained_snapshot_ids,
                None, // Progress callback not needed for planning
            )
            .await?
        };

        let requirements = if to_expire.is_empty() && retention_result.expired_ref_names.is_empty()
        {
            Vec::new()
        } else {
            self.build_requirements(metadata)
        };

        Ok(ExpireSnapshotsPlan {
            expired_snapshot_ids: to_expire,
            expired_ref_names: retention_result.expired_ref_names,
            cleanup_plan,
            requirements,
        })
    }

    /// Plan file cleanup for this expiration action.
    ///
    /// This method computes which files would be deleted if file cleanup is
    /// enabled. Call this BEFORE committing the transaction if you want to
    /// delete files after the commit succeeds.
    ///
    /// The cleanup plan identifies unreferenced:
    /// - Manifest list files
    /// - Manifest files
    /// - Data files
    /// - Position delete files
    /// - Equality delete files
    ///
    /// # Safety
    ///
    /// The plan only includes files that will be unreferenced after expiration.
    /// Files still referenced by retained snapshots or refs are never included.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let action = tx.expire_snapshots()
    ///     .older_than(Utc::now() - Duration::days(7))
    ///     .delete_files(true);
    ///
    /// // Plan cleanup before commit
    /// let cleanup_plan = action.plan_cleanup(&table).await?;
    ///
    /// // Commit metadata
    /// let tx = action.apply(tx)?;
    /// let table = tx.commit(&catalog).await?;
    ///
    /// // Execute cleanup after successful commit
    /// let result = execute_cleanup_plan(table.file_io(), &cleanup_plan, None).await?;
    /// ```
    pub async fn plan_cleanup(&self, table: &Table) -> Result<CleanupPlan> {
        Ok(self.plan(table).await?.cleanup_plan().clone())
    }

    /// Execute snapshot expiration using a precomputed plan.
    ///
    /// The plan must be derived from the same table state. If the table changes
    /// before commit, the operation fails safely due to optimistic requirements.
    pub async fn execute_with_plan(
        &self,
        table: &Table,
        catalog: &dyn Catalog,
        plan: ExpireSnapshotsPlan,
        options: ExpireOptions,
    ) -> Result<ExpireSnapshotsResult> {
        let start = Instant::now();

        if plan.is_empty() {
            return Ok(ExpireSnapshotsResult::empty(start.elapsed()));
        }

        if options.dry_run {
            return Ok(plan.build_result(options, start.elapsed(), None));
        }

        if table.readonly() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "Cannot expire snapshots on a readonly table",
            ));
        }

        let action_commit = plan.action_commit();
        action_commit
            .commit_to_catalog(table.identifier().clone(), catalog)
            .await?;

        if !options.delete_files {
            return Ok(plan.build_result(options, start.elapsed(), None));
        }

        let cleanup_result =
            execute_cleanup_plan(table.file_io(), plan.cleanup_plan(), None).await?;

        Ok(plan.build_result(options, start.elapsed(), Some(&cleanup_result)))
    }

    /// Execute snapshot expiration using the action's configured options.
    pub async fn execute(
        &self,
        table: &Table,
        catalog: &dyn Catalog,
    ) -> Result<ExpireSnapshotsResult> {
        let plan = self.plan(table).await?;
        self.execute_with_plan(table, catalog, plan, self.options())
            .await
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Internal Helpers
    // ═══════════════════════════════════════════════════════════════════════

    /// Determine which snapshots should be expired based on configuration.
    fn compute_snapshots_to_expire(
        &self,
        metadata: &TableMetadata,
        retention_result: &RetentionResult,
        policy: &RetentionPolicy,
        now: DateTime<Utc>,
    ) -> Result<Vec<i64>> {
        let retained_ids = &retention_result.retained_snapshot_ids;

        // Determine snapshots to expire based on configuration
        let mut to_expire: Vec<i64> = if !self.snapshot_ids_to_expire.is_empty() {
            // Explicit IDs provided - validate they exist and can be expired
            for id in &self.snapshot_ids_to_expire {
                // Check snapshot exists
                if metadata.snapshot_by_id(*id).is_none() {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Snapshot {id} does not exist"),
                    ));
                }

                // Check snapshot is not protected
                if retained_ids.contains(id) {
                    // Find which ref protects this snapshot
                    for (ref_name, snap_ref) in metadata.refs() {
                        if snap_ref.snapshot_id == *id {
                            return Err(Error::new(
                                ErrorKind::DataInvalid,
                                format!(
                                    "Cannot expire snapshot {id}: still referenced by '{ref_name}'",
                                ),
                            ));
                        }
                    }
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Cannot expire snapshot {id}: protected by retention policy",),
                    ));
                }
            }
            self.snapshot_ids_to_expire.iter().copied().collect()
        } else if let Some(older_than) = self.older_than {
            // Time-based expiration
            let cutoff_ms = older_than.timestamp_millis();
            metadata
                .snapshots()
                .filter(|s| {
                    s.timestamp_ms() < cutoff_ms && !retained_ids.contains(&s.snapshot_id())
                })
                .map(|s| s.snapshot_id())
                .collect()
        } else if self.use_table_properties {
            // Use policy age from table properties
            let cutoff = now - policy.max_snapshot_age;
            let cutoff_ms = cutoff.timestamp_millis();
            metadata
                .snapshots()
                .filter(|s| {
                    s.timestamp_ms() < cutoff_ms && !retained_ids.contains(&s.snapshot_id())
                })
                .map(|s| s.snapshot_id())
                .collect()
        } else {
            // No expiration criteria specified
            return Ok(vec![]);
        };

        // Apply retain_last if specified (additional protection beyond policy)
        if let Some(retain_count) = self.retain_last {
            let additional_protected = self.compute_retain_last_protected(metadata, retain_count);
            to_expire.retain(|id| !additional_protected.contains(id));
        }

        // Sort for deterministic behavior
        to_expire.sort();

        Ok(to_expire)
    }

    /// Compute additional protected snapshots from retain_last setting.
    fn compute_retain_last_protected(
        &self,
        metadata: &TableMetadata,
        retain_count: u32,
    ) -> HashSet<i64> {
        let mut protected = HashSet::new();

        // For each branch, protect the N most recent ancestors
        for snap_ref in metadata.refs().values() {
            if !snap_ref.is_branch() {
                continue;
            }

            let mut current_id = Some(snap_ref.snapshot_id);
            for _ in 0..retain_count {
                match current_id {
                    Some(id) => {
                        protected.insert(id);
                        current_id = metadata
                            .snapshot_by_id(id)
                            .and_then(|s| s.parent_snapshot_id());
                    }
                    None => break,
                }
            }
        }

        protected
    }

    /// Validate the expiration configuration.
    fn validate(&self, metadata: &TableMetadata) -> Result<()> {
        // Check that we're not trying to expire the current snapshot
        if let Some(current_id) = metadata.current_snapshot_id()
            && self.snapshot_ids_to_expire.contains(&current_id)
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Cannot expire current snapshot {current_id}"),
            ));
        }

        Ok(())
    }

    /// Build table requirements for optimistic concurrency control.
    fn build_requirements(&self, metadata: &TableMetadata) -> Vec<TableRequirement> {
        let mut requirements = vec![
            // Ensure table hasn't changed since we read it
            TableRequirement::UuidMatch {
                uuid: metadata.uuid(),
            },
        ];

        // For each ref, ensure its snapshot hasn't changed
        // This prevents expiring snapshots that became protected by concurrent updates
        for (ref_name, snap_ref) in metadata.refs() {
            requirements.push(TableRequirement::RefSnapshotIdMatch {
                r#ref: ref_name.clone(),
                snapshot_id: Some(snap_ref.snapshot_id),
            });
        }

        requirements
    }
}

#[async_trait]
impl TransactionAction for ExpireSnapshotsAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let metadata = table.metadata();

        // Validate configuration
        self.validate(metadata)?;

        let now = Utc::now();

        // Get retention policy
        let policy = if self.use_table_properties {
            RetentionPolicy::from_table_properties(metadata.properties())
        } else {
            RetentionPolicy::default()
        };

        // Compute retention (which snapshots to keep, which refs expired)
        let retention_result = compute_retention(metadata, &policy, now)?;

        // Compute snapshots to expire
        let to_expire =
            self.compute_snapshots_to_expire(metadata, &retention_result, &policy, now)?;

        // Build table updates
        let mut updates = Vec::new();

        // Add RemoveSnapshotRef for expired refs
        for ref_name in &retention_result.expired_ref_names {
            updates.push(TableUpdate::RemoveSnapshotRef {
                ref_name: ref_name.clone(),
            });
        }

        // Add RemoveSnapshots for expired snapshots
        if !to_expire.is_empty() {
            updates.push(TableUpdate::RemoveSnapshots {
                snapshot_ids: to_expire,
            });
        }

        if updates.is_empty() {
            // Nothing to expire - return no-op
            return Ok(ActionCommit::new(vec![], vec![]));
        }

        // Build requirements for optimistic concurrency
        let requirements = self.build_requirements(metadata);

        Ok(ActionCommit::new(updates, requirements))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_expire_snapshots_action_builder() {
        let now = Utc::now();

        let action = ExpireSnapshotsAction::new()
            .older_than(now)
            .retain_last(10)
            .expire_snapshot_id(12345)
            .use_table_properties(true);

        assert_eq!(action.older_than, Some(now));
        assert_eq!(action.retain_last, Some(10));
        assert!(action.snapshot_ids_to_expire.contains(&12345));
        assert!(action.use_table_properties);
    }

    #[test]
    fn test_cleanup_level_default() {
        assert_eq!(CleanupLevel::default(), CleanupLevel::None);
    }

    #[test]
    fn test_expire_snapshots_action_defaults() {
        let action = ExpireSnapshotsAction::new();

        assert!(action.older_than.is_none());
        assert!(action.retain_last.is_none());
        assert!(action.snapshot_ids_to_expire.is_empty());
        assert!(!action.use_table_properties);
        assert!(!action.delete_files);
        assert!(!action.dry_run);
    }

    #[test]
    fn test_expire_snapshots_multiple_ids() {
        let action = ExpireSnapshotsAction::new()
            .expire_snapshot_id(1)
            .expire_snapshot_id(2)
            .expire_snapshot_id(3);

        assert_eq!(action.snapshot_ids_to_expire.len(), 3);
        assert!(action.snapshot_ids_to_expire.contains(&1));
        assert!(action.snapshot_ids_to_expire.contains(&2));
        assert!(action.snapshot_ids_to_expire.contains(&3));
    }

    #[test]
    fn test_expire_snapshots_file_deletion_options() {
        let action = ExpireSnapshotsAction::new()
            .delete_files(true)
            .dry_run(true);

        assert!(action.delete_files);
        assert!(action.dry_run);

        let options = action.options();
        assert!(options.delete_files);
        assert!(options.dry_run);
    }

    #[test]
    fn test_expire_options_constructors() {
        let dry_run = ExpireOptions::dry_run();
        assert!(dry_run.dry_run);
        assert!(!dry_run.delete_files);

        let with_deletion = ExpireOptions::with_file_deletion();
        assert!(!with_deletion.dry_run);
        assert!(with_deletion.delete_files);

        let default_opts = ExpireOptions::default();
        assert!(!default_opts.dry_run);
        assert!(!default_opts.delete_files);
    }

    #[test]
    fn test_expire_snapshots_plan_build_result_with_deletes() {
        let plan = ExpireSnapshotsPlan {
            expired_snapshot_ids: vec![1, 2],
            expired_ref_names: vec!["branch".to_string()],
            cleanup_plan: CleanupPlan {
                manifest_list_files: vec!["ml".to_string()],
                manifest_files: vec!["m1".to_string(), "m2".to_string()],
                data_files: vec!["d1".to_string()],
                position_delete_files: vec!["pd1".to_string()],
                equality_delete_files: vec!["ed1".to_string(), "ed2".to_string()],
                total_bytes: 42,
            },
            requirements: vec![],
        };

        let result = plan.build_result(
            ExpireOptions {
                dry_run: true,
                delete_files: true,
            },
            Duration::from_secs(1),
            None,
        );

        assert_eq!(result.deleted_snapshots_count, 2);
        assert_eq!(result.deleted_refs_count, 1);
        assert_eq!(result.deleted_manifest_list_files_count, 1);
        assert_eq!(result.deleted_manifest_files_count, 2);
        assert_eq!(result.deleted_data_files_count, 1);
        assert_eq!(result.deleted_position_delete_files_count, 1);
        assert_eq!(result.deleted_equality_delete_files_count, 2);
        assert_eq!(result.total_bytes_freed, 42);
        assert!(result.has_deleted_files());
    }
}
