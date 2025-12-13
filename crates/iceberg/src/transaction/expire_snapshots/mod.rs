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
//! # Current Limitations
//!
//! **This implementation performs metadata-only expiration.** It removes snapshots
//! and refs from table metadata but does NOT delete orphaned data files, manifests,
//! or manifest lists. File cleanup must be done separately using a dedicated
//! orphan file removal tool.
//!
//! File deletion will be added in a future release.
//!
//! # Architecture
//!
//! The implementation follows the Apache Iceberg specification for snapshot retention:
//!
//! 1. **Retention Policy Evaluation**: Determine which snapshots are protected by
//!    branches, tags, and retention settings
//! 2. **Ref Expiration**: Remove non-main refs that exceed `max-ref-age-ms`
//! 3. **Metadata Commit**: Atomically remove expired snapshots and refs
//!
//! # Safety
//!
//! The implementation includes critical safety guardrails:
//!
//! - Never deletes files still referenced by retained snapshots
//! - Respects branch and tag references
//! - The main branch never expires
//! - Uses table requirements for optimistic concurrency control
//!
//! # Example
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

mod result;
mod retention;

pub use result::ExpireSnapshotsResult;
pub use retention::{RetentionPolicy, RetentionResult, compute_retained_snapshots, compute_retention};

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::catalog::TableRequirement;
use crate::error::{Error, ErrorKind, Result};
use crate::spec::TableMetadata;
use crate::table::Table;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::TableUpdate;

/// Level of cleanup to perform after expiring snapshots.
///
/// **Note:** Currently only `None` is implemented. `Full` and `MetadataOnly`
/// will return `FeatureUnsupported` errors until file deletion is implemented.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CleanupLevel {
    /// Delete snapshots from metadata only, no file deletion.
    /// This is currently the only implemented option.
    #[default]
    None,

    /// Delete snapshots from metadata, delete only manifest files (not data files).
    /// **Not yet implemented** - will return `FeatureUnsupported`.
    MetadataOnly,

    /// Delete snapshots from metadata, delete unreferenced data and manifest files.
    /// **Not yet implemented** - will return `FeatureUnsupported`.
    Full,
}

/// Action for expiring old snapshots from an Iceberg table.
///
/// This action removes snapshots that are no longer needed for time travel
/// or branch/tag references. It can also remove expired refs (branches/tags
/// that exceed their `max-ref-age-ms` setting).
///
/// # Current Limitations
///
/// - **Metadata only**: Does not delete orphaned files (data, manifests, etc.)
/// - **No dry-run**: Dry run mode is not yet implemented
/// - **No file cleanup levels**: `CleanupLevel::Full` and `MetadataOnly` are not implemented
///
/// # Configuration
///
/// Use builder methods to customize expiration behavior:
///
/// - `older_than()`: Expire snapshots older than a timestamp
/// - `retain_last()`: Keep at least N most recent snapshots per branch
/// - `expire_snapshot_id()`: Explicitly expire specific snapshots by ID
///
/// # Safety
///
/// The action validates that:
/// - The current snapshot cannot be expired
/// - Snapshots referenced by active branches/tags are protected
/// - Explicitly requested snapshot IDs exist in the table
/// - Uses `TableRequirement::UuidMatch` for optimistic concurrency
pub struct ExpireSnapshotsAction {
    /// Expire snapshots older than this timestamp.
    older_than: Option<DateTime<Utc>>,

    /// Retain at least this many snapshots per branch.
    retain_last: Option<u32>,

    /// Explicitly expire these snapshot IDs.
    snapshot_ids_to_expire: HashSet<i64>,

    /// Use table properties for retention policy.
    use_table_properties: bool,
}

impl ExpireSnapshotsAction {
    /// Creates a new expire snapshots action with default settings.
    pub(crate) fn new() -> Self {
        Self {
            older_than: None,
            retain_last: None,
            snapshot_ids_to_expire: HashSet::new(),
            use_table_properties: false,
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
                        format!("Snapshot {} does not exist", id),
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
                                    "Cannot expire snapshot {}: still referenced by '{}'",
                                    id, ref_name
                                ),
                            ));
                        }
                    }
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Cannot expire snapshot {}: protected by retention policy",
                            id
                        ),
                    ));
                }
            }
            self.snapshot_ids_to_expire.iter().copied().collect()
        } else if let Some(older_than) = self.older_than {
            // Time-based expiration
            let cutoff_ms = older_than.timestamp_millis();
            metadata
                .snapshots()
                .filter(|s| s.timestamp_ms() < cutoff_ms && !retained_ids.contains(&s.snapshot_id()))
                .map(|s| s.snapshot_id())
                .collect()
        } else if self.use_table_properties {
            // Use policy age from table properties
            let cutoff = now - policy.max_snapshot_age;
            let cutoff_ms = cutoff.timestamp_millis();
            metadata
                .snapshots()
                .filter(|s| s.timestamp_ms() < cutoff_ms && !retained_ids.contains(&s.snapshot_id()))
                .map(|s| s.snapshot_id())
                .collect()
        } else {
            // No expiration criteria specified
            return Ok(vec![]);
        };

        // Apply retain_last if specified (additional protection beyond policy)
        if let Some(retain_count) = self.retain_last {
            let additional_protected =
                self.compute_retain_last_protected(metadata, retain_count);
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
        for (_ref_name, snap_ref) in metadata.refs() {
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
        if let Some(current_id) = metadata.current_snapshot_id() {
            if self.snapshot_ids_to_expire.contains(&current_id) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("Cannot expire current snapshot {}", current_id),
                ));
            }
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
        let to_expire = self.compute_snapshots_to_expire(metadata, &retention_result, &policy, now)?;

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
}
