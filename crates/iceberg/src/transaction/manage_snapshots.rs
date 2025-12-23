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

//! Snapshot management operations for rollback and snapshot reference updates.
//!
//! This module provides the [`ManageSnapshotsAction`] which enables operations to
//! change the current snapshot of a table without creating new snapshots. This
//! includes rollback operations and setting arbitrary snapshots as current.
//!
//! # API Overview
//!
//! The `ManageSnapshotsAction` supports three main operations:
//!
//! - [`rollback_to_snapshot`](ManageSnapshotsAction::rollback_to_snapshot): Roll back to a
//!   specific snapshot ID. Validates that the target is an ancestor of the current snapshot.
//! - [`rollback_to_timestamp`](ManageSnapshotsAction::rollback_to_timestamp): Roll back to the
//!   last snapshot before a given timestamp. Also validates ancestry.
//! - [`set_current_snapshot`](ManageSnapshotsAction::set_current_snapshot): Set any valid
//!   snapshot as current without ancestry validation. Useful for branch operations.
//!
//! # Semantic Differences
//!
//! | Operation | Ancestor Validation | Use Case |
//! |-----------|---------------------|----------|
//! | `rollback_to_snapshot` | Required | Safe rollback along linear history |
//! | `rollback_to_timestamp` | Required | Time-based recovery |
//! | `set_current_snapshot` | Not required | Branch operations, cherry-picking |
//!
//! # Example
//!
//! ```rust,ignore
//! use iceberg::transaction::{Transaction, ApplyTransactionAction};
//!
//! // Rollback to a specific snapshot
//! let tx = Transaction::new(&table);
//! let tx = tx.manage_snapshots()
//!     .rollback_to_snapshot(previous_snapshot_id)?
//!     .apply(tx)?;
//! let table = tx.commit(&catalog).await?;
//!
//! // Rollback to timestamp
//! let tx = Transaction::new(&table);
//! let yesterday = chrono::Utc::now() - chrono::Duration::days(1);
//! let tx = tx.manage_snapshots()
//!     .rollback_to_timestamp(yesterday)?
//!     .apply(tx)?;
//! let table = tx.commit(&catalog).await?;
//!
//! // Set current snapshot (no ancestry validation)
//! let tx = Transaction::new(&table);
//! let tx = tx.manage_snapshots()
//!     .set_current_snapshot(any_snapshot_id)?
//!     .apply(tx)?;
//! let table = tx.commit(&catalog).await?;
//! ```
//!
//! # Compatibility
//!
//! This implementation follows the semantics of the Java Iceberg `ManageSnapshots`
//! interface, ensuring cross-engine compatibility with Spark, Trino, and other
//! Iceberg implementations.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::spec::{MAIN_BRANCH, SnapshotReference, SnapshotRetention, SnapshotUtil};
use crate::table::Table;
use crate::transaction::action::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, Result, TableRequirement, TableUpdate};

/// Operation type for snapshot management.
///
/// Determines whether ancestry validation is performed during commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManageSnapshotsOperation {
    /// Rollback to ancestor snapshot (validates ancestry).
    ///
    /// This operation ensures the target snapshot is in the direct ancestry
    /// of the current snapshot, preventing accidental jumps to unrelated
    /// snapshots.
    Rollback,

    /// Set current snapshot without ancestry validation.
    ///
    /// This operation allows setting any valid snapshot as current,
    /// enabling advanced workflows like cherry-picking and branch merging.
    SetCurrent,
}

/// Builder for snapshot management operations.
///
/// This action enables changing the current snapshot of a table without
/// creating new snapshots. It supports rollback operations (which validate
/// ancestry) and direct snapshot setting (which does not).
///
/// # Builder Pattern
///
/// Create an action via [`Transaction::manage_snapshots()`](crate::transaction::Transaction::manage_snapshots),
/// configure it with one of the operation methods, optionally specify a branch,
/// then apply to the transaction.
///
/// # Example
///
/// ```rust,ignore
/// use iceberg::transaction::{Transaction, ApplyTransactionAction};
///
/// let tx = Transaction::new(&table);
/// let action = tx.manage_snapshots()
///     .rollback_to_snapshot(123456789)?;
/// let tx = action.apply(tx)?;
/// let table = tx.commit(&catalog).await?;
/// ```
#[derive(Debug)]
pub struct ManageSnapshotsAction {
    operation: Option<ManageSnapshotsOperation>,
    target_snapshot_id: Option<i64>,
    target_timestamp: Option<DateTime<Utc>>,
    target_ref: Option<String>,
}

impl ManageSnapshotsAction {
    /// Create a new ManageSnapshots action builder.
    pub fn new() -> Self {
        Self {
            operation: None,
            target_snapshot_id: None,
            target_timestamp: None,
            target_ref: None,
        }
    }

    /// Roll back the table's state to a specific snapshot.
    ///
    /// The target snapshot must be an ancestor of the current snapshot.
    /// This operation does not create a new snapshot; it simply updates
    /// the current snapshot reference to point to the target.
    ///
    /// # Arguments
    ///
    /// * `snapshot_id` - The snapshot ID to roll back to
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - An operation has already been specified (conflicting operations)
    /// - At commit time: if the snapshot does not exist
    /// - At commit time: if the snapshot is not an ancestor of the current snapshot
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let action = tx.manage_snapshots()
    ///     .rollback_to_snapshot(123456789)?;
    /// ```
    pub fn rollback_to_snapshot(mut self, snapshot_id: i64) -> Result<Self> {
        self.validate_no_conflicting_operation()?;
        self.operation = Some(ManageSnapshotsOperation::Rollback);
        self.target_snapshot_id = Some(snapshot_id);
        Ok(self)
    }

    /// Roll back the table's data to the last snapshot before the given timestamp.
    ///
    /// Finds the most recent snapshot with a timestamp strictly less than the
    /// provided timestamp and rolls back to that snapshot. The target snapshot
    /// must be an ancestor of the current snapshot.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The timestamp to roll back to
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - An operation has already been specified (conflicting operations)
    /// - At commit time: if no snapshot exists before the timestamp
    /// - At commit time: if the found snapshot is not an ancestor of current
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use chrono::{Utc, Duration};
    ///
    /// let yesterday = Utc::now() - Duration::days(1);
    /// let action = tx.manage_snapshots()
    ///     .rollback_to_timestamp(yesterday)?;
    /// ```
    pub fn rollback_to_timestamp(mut self, timestamp: DateTime<Utc>) -> Result<Self> {
        self.validate_no_conflicting_operation()?;
        self.operation = Some(ManageSnapshotsOperation::Rollback);
        self.target_timestamp = Some(timestamp);
        Ok(self)
    }

    /// Set the current snapshot to a specific snapshot ID.
    ///
    /// Unlike rollback operations, this does NOT require the target snapshot
    /// to be an ancestor of the current snapshot. This enables advanced
    /// workflows like cherry-picking and branch merging.
    ///
    /// # Arguments
    ///
    /// * `snapshot_id` - The snapshot ID to set as current
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - An operation has already been specified (conflicting operations)
    /// - At commit time: if the snapshot does not exist
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let action = tx.manage_snapshots()
    ///     .set_current_snapshot(123456789)?;
    /// ```
    pub fn set_current_snapshot(mut self, snapshot_id: i64) -> Result<Self> {
        self.validate_no_conflicting_operation()?;
        self.operation = Some(ManageSnapshotsOperation::SetCurrent);
        self.target_snapshot_id = Some(snapshot_id);
        Ok(self)
    }

    /// Set the branch to operate on.
    ///
    /// By default, operations target the "main" branch. Use this method
    /// to operate on a different branch.
    ///
    /// # Arguments
    ///
    /// * `branch_name` - The name of the branch to update
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let action = tx.manage_snapshots()
    ///     .rollback_to_snapshot(123456789)?
    ///     .on_branch("feature");
    /// ```
    pub fn on_branch(mut self, branch_name: impl Into<String>) -> Self {
        self.target_ref = Some(branch_name.into());
        self
    }

    /// Validate that no operation has already been specified.
    fn validate_no_conflicting_operation(&self) -> Result<()> {
        if self.operation.is_some() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "ManageSnapshots does not allow conflicting operations. \
                 Only one of rollback_to_snapshot, rollback_to_timestamp, \
                 or set_current_snapshot can be called.",
            ));
        }
        Ok(())
    }

    /// Validate that the target snapshot exists in the table metadata.
    fn validate_snapshot_exists(table: &Table, snapshot_id: i64) -> Result<()> {
        let metadata = table.metadata();

        if metadata.snapshot_by_id(snapshot_id).is_none() {
            let available_ids: Vec<_> = metadata.snapshots().map(|s| s.snapshot_id()).collect();
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Snapshot {snapshot_id} not found in table metadata. Available snapshots: {available_ids:?}",
                ),
            ));
        }
        Ok(())
    }

    /// Find the snapshot ID for a given timestamp.
    fn find_snapshot_before_timestamp(
        table: &Table,
        timestamp: DateTime<Utc>,
        ref_name: &str,
    ) -> Result<i64> {
        let metadata = table.metadata();
        let timestamp_ms = timestamp.timestamp_millis();

        let current_snapshot_id = Self::current_snapshot_id_for_ref(table, ref_name)?;
        let ancestor_snapshots = SnapshotUtil::ancestor_ids(metadata, current_snapshot_id)
            .into_iter()
            .filter_map(|id| metadata.snapshot_by_id(id));

        ancestor_snapshots
            .filter(|s| s.timestamp_ms() < timestamp_ms)
            .max_by_key(|s| s.timestamp_ms())
            .map(|s| s.snapshot_id())
            .ok_or_else(|| {
                let earliest = SnapshotUtil::ancestor_ids(metadata, current_snapshot_id)
                    .into_iter()
                    .filter_map(|id| metadata.snapshot_by_id(id))
                    .map(|s| s.timestamp_ms())
                    .min();

                let earliest_str = earliest
                    .and_then(DateTime::from_timestamp_millis)
                    .map(|dt| dt.to_rfc3339())
                    .unwrap_or_else(|| "N/A".to_string());

                let timestamp_str = timestamp.to_rfc3339();
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "No snapshot found before timestamp {timestamp_str} on branch '{ref_name}'. Earliest snapshot is at {earliest_str}",
                    ),
                )
            })
    }

    fn current_snapshot_id_for_ref(table: &Table, ref_name: &str) -> Result<i64> {
        let metadata = table.metadata();

        let current_snapshot_id = if ref_name == MAIN_BRANCH {
            metadata.current_snapshot_id()
        } else {
            let snapshot_ref = metadata.refs().get(ref_name).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Cannot rollback: branch '{ref_name}' has no current snapshot"),
                )
            })?;

            if !snapshot_ref.is_branch() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot rollback: reference '{ref_name}' is not a branch (tags are immutable)",
                    ),
                ));
            }

            Some(snapshot_ref.snapshot_id)
        };

        current_snapshot_id.ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Cannot rollback: branch '{ref_name}' has no current snapshot"),
            )
        })
    }

    /// Validate that the target is an ancestor of the current snapshot.
    fn validate_ancestry(table: &Table, target_snapshot_id: i64, ref_name: &str) -> Result<()> {
        let metadata = table.metadata();
        let current_snapshot_id = Self::current_snapshot_id_for_ref(table, ref_name)?;

        if !SnapshotUtil::is_ancestor_of(metadata, current_snapshot_id, target_snapshot_id) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot rollback to snapshot {target_snapshot_id}: not an ancestor of current snapshot {current_snapshot_id} \
                     on branch '{ref_name}'. Use set_current_snapshot() to set arbitrary snapshots.",
                ),
            ));
        }

        Ok(())
    }
}

impl Default for ManageSnapshotsAction {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TransactionAction for ManageSnapshotsAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        // Validate operation was specified
        let operation = self.operation.ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "No operation specified. Call rollback_to_snapshot, \
                 rollback_to_timestamp, or set_current_snapshot.",
            )
        })?;

        let metadata = table.metadata();

        // Determine target ref (default to main)
        let ref_name = self
            .target_ref
            .clone()
            .unwrap_or_else(|| MAIN_BRANCH.to_string());

        // Resolve target snapshot ID
        let target_snapshot_id = match (self.target_snapshot_id, self.target_timestamp) {
            (Some(id), None) => id,
            (None, Some(ts)) => Self::find_snapshot_before_timestamp(table, ts, &ref_name)?,
            (Some(_), Some(_)) => {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Cannot specify both snapshot_id and timestamp",
                ));
            }
            (None, None) => {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "No target snapshot or timestamp specified",
                ));
            }
        };

        // Validate snapshot exists
        Self::validate_snapshot_exists(table, target_snapshot_id)?;

        // Determine expected ref snapshot for optimistic concurrency.
        let expected_ref_snapshot_id = metadata.refs().get(&ref_name).map(|r| r.snapshot_id);

        // Validate reference type (tags are immutable and must not be updated).
        if let Some(existing_ref) = metadata.refs().get(&ref_name) {
            if !existing_ref.is_branch() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot manage snapshots on reference '{ref_name}': reference is not a branch (tags are immutable)",
                    ),
                ));
            }

            // No-op: avoid metadata churn when the ref already points at the target snapshot.
            if existing_ref.snapshot_id == target_snapshot_id {
                return Ok(ActionCommit::new(vec![], vec![]));
            }
        }

        // For rollback operations, validate ancestry
        if operation == ManageSnapshotsOperation::Rollback {
            Self::validate_ancestry(table, target_snapshot_id, &ref_name)?;
        }

        // Preserve branch retention settings if the branch exists.
        let retention = metadata
            .refs()
            .get(&ref_name)
            .map(|r| r.retention.clone())
            .unwrap_or_else(|| SnapshotRetention::branch(None, None, None));

        // Create the SetSnapshotRef update
        let update = TableUpdate::SetSnapshotRef {
            ref_name: ref_name.clone(),
            reference: SnapshotReference::new(target_snapshot_id, retention),
        };

        let requirements = vec![
            TableRequirement::UuidMatch {
                uuid: metadata.uuid(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: ref_name,
                snapshot_id: expected_ref_snapshot_id,
            },
        ];

        Ok(ActionCommit::new(vec![update], requirements))
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// CreateBranchAction
// ═══════════════════════════════════════════════════════════════════════════

/// Action for creating a new branch reference.
///
/// A branch is a mutable named reference that points to a snapshot. Unlike tags,
/// branches can be updated to point to new snapshots as data is written.
///
/// # Example
///
/// ```rust,ignore
/// use iceberg::transaction::{Transaction, ApplyTransactionAction};
///
/// // Create a branch from the current snapshot
/// let tx = Transaction::new(&table);
/// let tx = tx.create_branch()
///     .name("audit-branch")
///     .apply(tx)?;
/// let table = tx.commit(&catalog).await?;
///
/// // Create a branch from a specific snapshot
/// let tx = Transaction::new(&table);
/// let tx = tx.create_branch()
///     .name("audit-branch")
///     .from_snapshot(snapshot_id)
///     .apply(tx)?;
/// let table = tx.commit(&catalog).await?;
/// ```
#[derive(Debug)]
pub struct CreateBranchAction {
    branch_name: Option<String>,
    snapshot_id: Option<i64>,
    min_snapshots_to_keep: Option<i32>,
    max_snapshot_age_ms: Option<i64>,
    max_ref_age_ms: Option<i64>,
}

impl CreateBranchAction {
    /// Create a new CreateBranchAction.
    pub fn new() -> Self {
        Self {
            branch_name: None,
            snapshot_id: None,
            min_snapshots_to_keep: None,
            max_snapshot_age_ms: None,
            max_ref_age_ms: None,
        }
    }

    /// Set the name of the branch to create.
    ///
    /// # Arguments
    ///
    /// * `name` - The branch name. Cannot be "main" (use fast-forward instead).
    #[must_use]
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.branch_name = Some(name.into());
        self
    }

    /// Set the snapshot ID the branch should point to.
    ///
    /// If not specified, defaults to the current snapshot.
    #[must_use]
    pub fn from_snapshot(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    /// Set the minimum number of snapshots to keep for this branch.
    #[must_use]
    pub fn min_snapshots_to_keep(mut self, count: i32) -> Self {
        self.min_snapshots_to_keep = Some(count);
        self
    }

    /// Set the maximum snapshot age for this branch.
    #[must_use]
    pub fn max_snapshot_age_ms(mut self, age_ms: i64) -> Self {
        self.max_snapshot_age_ms = Some(age_ms);
        self
    }

    /// Set the maximum ref age for this branch.
    #[must_use]
    pub fn max_ref_age_ms(mut self, age_ms: i64) -> Self {
        self.max_ref_age_ms = Some(age_ms);
        self
    }
}

impl Default for CreateBranchAction {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TransactionAction for CreateBranchAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let branch_name = self.branch_name.as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Branch name is required. Call .name() before applying.",
            )
        })?;

        // Cannot create a branch named "main" - it's special
        if branch_name == MAIN_BRANCH {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot create a branch named 'main'. The main branch is managed automatically.",
            ));
        }

        let metadata = table.metadata();

        // Check if ref already exists
        if metadata.refs().contains_key(branch_name) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Reference '{branch_name}' already exists"),
            ));
        }

        // Resolve snapshot ID (default to current snapshot)
        let snapshot_id = match self.snapshot_id {
            Some(id) => {
                // Validate snapshot exists
                if metadata.snapshot_by_id(id).is_none() {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Snapshot {id} does not exist"),
                    ));
                }
                id
            }
            None => metadata.current_snapshot_id().ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "No current snapshot. Specify a snapshot ID with .from_snapshot().",
                )
            })?,
        };

        let retention = SnapshotRetention::branch(
            self.min_snapshots_to_keep,
            self.max_snapshot_age_ms,
            self.max_ref_age_ms,
        );

        let update = TableUpdate::SetSnapshotRef {
            ref_name: branch_name.clone(),
            reference: SnapshotReference::new(snapshot_id, retention),
        };

        let requirements = vec![
            TableRequirement::UuidMatch {
                uuid: metadata.uuid(),
            },
            // Ensure ref doesn't exist (will fail if created concurrently)
            TableRequirement::RefSnapshotIdMatch {
                r#ref: branch_name.clone(),
                snapshot_id: None,
            },
        ];

        Ok(ActionCommit::new(vec![update], requirements))
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// CreateTagAction
// ═══════════════════════════════════════════════════════════════════════════

/// Action for creating a new tag reference.
///
/// A tag is an immutable named reference that points to a specific snapshot.
/// Once created, tags cannot be updated to point to different snapshots.
///
/// # Example
///
/// ```rust,ignore
/// use iceberg::transaction::{Transaction, ApplyTransactionAction};
///
/// // Create a tag from the current snapshot
/// let tx = Transaction::new(&table);
/// let tx = tx.create_tag()
///     .name("release-v1.0")
///     .apply(tx)?;
/// let table = tx.commit(&catalog).await?;
///
/// // Create a tag from a specific snapshot
/// let tx = Transaction::new(&table);
/// let tx = tx.create_tag()
///     .name("release-v1.0")
///     .from_snapshot(snapshot_id)
///     .apply(tx)?;
/// let table = tx.commit(&catalog).await?;
/// ```
#[derive(Debug)]
pub struct CreateTagAction {
    tag_name: Option<String>,
    snapshot_id: Option<i64>,
    max_ref_age_ms: Option<i64>,
}

impl CreateTagAction {
    /// Create a new CreateTagAction.
    pub fn new() -> Self {
        Self {
            tag_name: None,
            snapshot_id: None,
            max_ref_age_ms: None,
        }
    }

    /// Set the name of the tag to create.
    #[must_use]
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.tag_name = Some(name.into());
        self
    }

    /// Set the snapshot ID the tag should point to.
    ///
    /// If not specified, defaults to the current snapshot.
    #[must_use]
    pub fn from_snapshot(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    /// Set the maximum ref age for this tag.
    #[must_use]
    pub fn max_ref_age_ms(mut self, age_ms: i64) -> Self {
        self.max_ref_age_ms = Some(age_ms);
        self
    }
}

impl Default for CreateTagAction {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TransactionAction for CreateTagAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let tag_name = self.tag_name.as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Tag name is required. Call .name() before applying.",
            )
        })?;

        // Cannot create a tag named "main"
        if tag_name == MAIN_BRANCH {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot create a tag named 'main'. The main branch is a branch, not a tag.",
            ));
        }

        let metadata = table.metadata();

        // Check if ref already exists
        if metadata.refs().contains_key(tag_name) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Reference '{tag_name}' already exists"),
            ));
        }

        // Resolve snapshot ID (default to current snapshot)
        let snapshot_id = match self.snapshot_id {
            Some(id) => {
                // Validate snapshot exists
                if metadata.snapshot_by_id(id).is_none() {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Snapshot {id} does not exist"),
                    ));
                }
                id
            }
            None => metadata.current_snapshot_id().ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "No current snapshot. Specify a snapshot ID with .from_snapshot().",
                )
            })?,
        };

        let retention = SnapshotRetention::Tag {
            max_ref_age_ms: self.max_ref_age_ms,
        };

        let update = TableUpdate::SetSnapshotRef {
            ref_name: tag_name.clone(),
            reference: SnapshotReference::new(snapshot_id, retention),
        };

        let requirements = vec![
            TableRequirement::UuidMatch {
                uuid: metadata.uuid(),
            },
            // Ensure ref doesn't exist (will fail if created concurrently)
            TableRequirement::RefSnapshotIdMatch {
                r#ref: tag_name.clone(),
                snapshot_id: None,
            },
        ];

        Ok(ActionCommit::new(vec![update], requirements))
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// FastForwardAction
// ═══════════════════════════════════════════════════════════════════════════

/// Action for fast-forwarding a branch to match another reference's snapshot.
///
/// This is the core operation for Write-Audit-Publish (WAP) workflows:
/// 1. Create a branch for staging changes
/// 2. Write and audit data on the branch
/// 3. Fast-forward main to the branch's snapshot
///
/// # Semantics
///
/// Fast-forward updates the target branch to point to the same snapshot as
/// the source reference. Both branches and tags can be used as the source.
/// The target must be an existing branch (tags are immutable).
///
/// # Example
///
/// ```rust,ignore
/// use iceberg::transaction::{Transaction, ApplyTransactionAction};
///
/// // Fast-forward main to match a staging branch
/// let tx = Transaction::new(&table);
/// let tx = tx.fast_forward()
///     .target("main")
///     .source("staging")
///     .apply(tx)?;
/// let table = tx.commit(&catalog).await?;
/// ```
#[derive(Debug)]
pub struct FastForwardAction {
    target_ref: Option<String>,
    source_ref: Option<String>,
}

impl FastForwardAction {
    /// Create a new FastForwardAction.
    pub fn new() -> Self {
        Self {
            target_ref: None,
            source_ref: None,
        }
    }

    /// Set the target branch to update.
    ///
    /// The target must be an existing branch.
    #[must_use]
    pub fn target(mut self, ref_name: impl Into<String>) -> Self {
        self.target_ref = Some(ref_name.into());
        self
    }

    /// Set the source reference to fast-forward to.
    ///
    /// Can be a branch or tag. The target branch will be updated to point
    /// to the same snapshot as this reference.
    #[must_use]
    pub fn source(mut self, ref_name: impl Into<String>) -> Self {
        self.source_ref = Some(ref_name.into());
        self
    }
}

impl Default for FastForwardAction {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TransactionAction for FastForwardAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let target_ref = self.target_ref.as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Target ref is required. Call .target() before applying.",
            )
        })?;

        let source_ref = self.source_ref.as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Source ref is required. Call .source() before applying.",
            )
        })?;

        let metadata = table.metadata();

        // Get source reference
        let source = metadata.refs().get(source_ref).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Source reference '{source_ref}' does not exist"),
            )
        })?;

        // Get target reference
        let target = metadata.refs().get(target_ref).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Target reference '{target_ref}' does not exist"),
            )
        })?;

        // Target must be a branch (tags are immutable)
        if !target.is_branch() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot fast-forward '{target_ref}': target is a tag (tags are immutable)",
                ),
            ));
        }

        // No-op if already pointing to the same snapshot
        if target.snapshot_id == source.snapshot_id {
            return Ok(ActionCommit::new(vec![], vec![]));
        }

        // Preserve target's retention settings
        let update = TableUpdate::SetSnapshotRef {
            ref_name: target_ref.clone(),
            reference: SnapshotReference::new(source.snapshot_id, target.retention.clone()),
        };

        let requirements = vec![
            TableRequirement::UuidMatch {
                uuid: metadata.uuid(),
            },
            // Ensure target hasn't changed (optimistic locking)
            TableRequirement::RefSnapshotIdMatch {
                r#ref: target_ref.clone(),
                snapshot_id: Some(target.snapshot_id),
            },
            // Ensure source hasn't changed
            TableRequirement::RefSnapshotIdMatch {
                r#ref: source_ref.clone(),
                snapshot_id: Some(source.snapshot_id),
            },
        ];

        Ok(ActionCommit::new(vec![update], requirements))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use as_any::Downcast;
    use chrono::DateTime;

    use super::*;
    use crate::TableIdent;
    use crate::io::FileIOBuilder;
    use crate::spec::{
        FormatVersion, NestedField, Operation, PartitionSpec, PrimitiveType, Schema, Snapshot,
        SortOrder, StructType, Summary, TableMetadata, Type,
    };
    use crate::table::Table;
    use crate::transaction::Transaction;
    use crate::transaction::action::ApplyTransactionAction;

    /// Create a test table with snapshot chain: s1 -> s2 -> s3 (current)
    fn create_test_table_with_chain() -> Table {
        let s1 = Snapshot::builder()
            .with_snapshot_id(1)
            .with_sequence_number(1)
            .with_timestamp_ms(1000)
            .with_manifest_list("s3://bucket/metadata/snap-1.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build();

        let s2 = Snapshot::builder()
            .with_snapshot_id(2)
            .with_parent_snapshot_id(Some(1))
            .with_sequence_number(2)
            .with_timestamp_ms(2000)
            .with_manifest_list("s3://bucket/metadata/snap-2.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build();

        let s3 = Snapshot::builder()
            .with_snapshot_id(3)
            .with_parent_snapshot_id(Some(2))
            .with_sequence_number(3)
            .with_timestamp_ms(3000)
            .with_manifest_list("s3://bucket/metadata/snap-3.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build();

        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap();

        let metadata = TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: uuid::Uuid::new_v4(),
            location: "s3://bucket/table".to_string(),
            last_sequence_number: 3,
            last_updated_ms: 3000,
            last_column_id: 1,
            current_schema_id: 0,
            schemas: HashMap::from([(0, Arc::new(schema))]),
            partition_specs: HashMap::new(),
            default_spec: Arc::new(PartitionSpec::unpartition_spec()),
            default_partition_type: StructType::new(vec![]),
            last_partition_id: -1,
            properties: HashMap::new(),
            current_snapshot_id: Some(3),
            snapshots: HashMap::from([(1, Arc::new(s1)), (2, Arc::new(s2)), (3, Arc::new(s3))]),
            snapshot_log: vec![],
            sort_orders: HashMap::from([(0, Arc::new(SortOrder::unsorted_order()))]),
            metadata_log: vec![],
            default_sort_order_id: 0,
            refs: HashMap::from([(
                MAIN_BRANCH.to_string(),
                SnapshotReference::new(3, SnapshotRetention::Branch {
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

        Table::builder()
            .metadata(metadata)
            .metadata_location("s3://bucket/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["db", "table"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap()
    }

    /// Create test table with branch: s1 -> s2 -> s3 (main) and s1 -> s4 (not on main)
    fn create_test_table_with_branch() -> Table {
        let table = create_test_table_with_chain();

        // Add s4 as a branch from s1
        let s4 = Snapshot::builder()
            .with_snapshot_id(4)
            .with_parent_snapshot_id(Some(1))
            .with_sequence_number(4)
            .with_timestamp_ms(2500)
            .with_manifest_list("s3://bucket/metadata/snap-4.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build();

        // Get mutable access to metadata and add the snapshot
        let mut metadata = (*table.metadata()).clone();
        metadata.snapshots.insert(4, Arc::new(s4));

        table.with_metadata(Arc::new(metadata))
    }

    #[test]
    fn test_rollback_to_ancestor_snapshot() {
        let _table = create_test_table_with_chain();
        let action = ManageSnapshotsAction::new()
            .rollback_to_snapshot(2)
            .unwrap();

        assert_eq!(action.operation, Some(ManageSnapshotsOperation::Rollback));
        assert_eq!(action.target_snapshot_id, Some(2));
    }

    #[test]
    fn test_set_current_snapshot() {
        let _table = create_test_table_with_chain();
        let action = ManageSnapshotsAction::new()
            .set_current_snapshot(1)
            .unwrap();

        assert_eq!(action.operation, Some(ManageSnapshotsOperation::SetCurrent));
        assert_eq!(action.target_snapshot_id, Some(1));
    }

    #[test]
    fn test_rollback_to_timestamp() {
        let timestamp = DateTime::from_timestamp_millis(1500).unwrap();
        let action = ManageSnapshotsAction::new()
            .rollback_to_timestamp(timestamp)
            .unwrap();

        assert_eq!(action.operation, Some(ManageSnapshotsOperation::Rollback));
        assert_eq!(action.target_timestamp, Some(timestamp));
    }

    #[test]
    fn test_on_branch() {
        let action = ManageSnapshotsAction::new()
            .rollback_to_snapshot(1)
            .unwrap()
            .on_branch("feature");

        assert_eq!(action.target_ref, Some("feature".to_string()));
    }

    #[test]
    fn test_conflicting_operations_error() {
        let result = ManageSnapshotsAction::new()
            .rollback_to_snapshot(1)
            .unwrap()
            .set_current_snapshot(2);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message().contains("conflicting operations"));
    }

    #[test]
    fn test_apply_to_transaction() {
        let table = create_test_table_with_chain();
        let tx = Transaction::new(&table);

        let action = ManageSnapshotsAction::new()
            .rollback_to_snapshot(2)
            .unwrap();

        let tx = action.apply(tx).unwrap();
        assert_eq!(tx.actions.len(), 1);

        // Verify action was stored
        let stored = (*tx.actions[0])
            .downcast_ref::<ManageSnapshotsAction>()
            .expect("Action should be ManageSnapshotsAction");
        assert_eq!(stored.target_snapshot_id, Some(2));
    }

    #[tokio::test]
    async fn test_commit_rollback_to_ancestor() {
        let table = create_test_table_with_chain();
        let action = Arc::new(
            ManageSnapshotsAction::new()
                .rollback_to_snapshot(2)
                .unwrap(),
        );

        let result = action.commit(&table).await;
        assert!(result.is_ok());

        let mut commit = result.unwrap();
        let updates = commit.take_updates();
        let requirements = commit.take_requirements();
        assert_eq!(updates.len(), 1);
        assert_eq!(requirements.len(), 2);

        match &updates[0] {
            TableUpdate::SetSnapshotRef {
                ref_name,
                reference,
            } => {
                assert_eq!(ref_name, MAIN_BRANCH);
                assert_eq!(reference.snapshot_id, 2);
            }
            _ => panic!("Expected SetSnapshotRef update"),
        }

        assert!(matches!(
            requirements[0],
            TableRequirement::UuidMatch { .. }
        ));
        assert_eq!(requirements[1], TableRequirement::RefSnapshotIdMatch {
            r#ref: MAIN_BRANCH.to_string(),
            snapshot_id: Some(3),
        });
    }

    #[tokio::test]
    async fn test_commit_rollback_to_non_ancestor_fails() {
        let table = create_test_table_with_branch();
        let action = Arc::new(
            ManageSnapshotsAction::new()
                .rollback_to_snapshot(4) // s4 is not ancestor of s3
                .unwrap(),
        );

        let result = action.commit(&table).await;
        let err = result.expect_err("Expected error");
        assert!(err.message().contains("not an ancestor"));
    }

    #[tokio::test]
    async fn test_commit_set_current_snapshot_no_ancestry_check() {
        let table = create_test_table_with_branch();
        let action = Arc::new(
            ManageSnapshotsAction::new()
                .set_current_snapshot(4) // s4 is not ancestor, but should work
                .unwrap(),
        );

        let result = action.commit(&table).await;
        assert!(result.is_ok());

        let mut commit = result.unwrap();
        let updates = commit.take_updates();
        assert_eq!(updates.len(), 1);

        match &updates[0] {
            TableUpdate::SetSnapshotRef { reference, .. } => {
                assert_eq!(reference.snapshot_id, 4);
            }
            _ => panic!("Expected SetSnapshotRef update"),
        }
    }

    #[tokio::test]
    async fn test_commit_rollback_to_timestamp() {
        let table = create_test_table_with_chain();
        // Timestamp between s1 and s2 should find s1
        let timestamp = DateTime::from_timestamp_millis(1500).unwrap();
        let action = Arc::new(
            ManageSnapshotsAction::new()
                .rollback_to_timestamp(timestamp)
                .unwrap(),
        );

        let result = action.commit(&table).await;
        assert!(result.is_ok());

        let mut commit = result.unwrap();
        let updates = commit.take_updates();
        match &updates[0] {
            TableUpdate::SetSnapshotRef { reference, .. } => {
                assert_eq!(reference.snapshot_id, 1);
            }
            _ => panic!("Expected SetSnapshotRef update"),
        }
    }

    #[tokio::test]
    async fn test_commit_rollback_to_timestamp_ignores_non_ancestor_snapshots() {
        // Table contains snapshot 4 at timestamp 2500, branched from snapshot 1.
        // The main branch chain is 1 -> 2 -> 3 (current).
        // For timestamp 2600, the closest snapshot in the main ancestry is 2 (timestamp 2000),
        // not 4 (timestamp 2500, not an ancestor).
        let table = create_test_table_with_branch();
        let timestamp = DateTime::from_timestamp_millis(2600).unwrap();
        let action = Arc::new(
            ManageSnapshotsAction::new()
                .rollback_to_timestamp(timestamp)
                .unwrap(),
        );

        let mut commit = action.commit(&table).await.unwrap();
        let updates = commit.take_updates();
        match &updates[0] {
            TableUpdate::SetSnapshotRef { reference, .. } => {
                assert_eq!(reference.snapshot_id, 2);
            }
            _ => panic!("Expected SetSnapshotRef update"),
        }
    }

    #[tokio::test]
    async fn test_commit_rollback_to_timestamp_on_branch_selects_ancestor() {
        // Feature branch points to snapshot 4 (timestamp 2500), whose only ancestor is snapshot 1.
        // For timestamp 2200, the closest valid snapshot in that branch ancestry is 1, not 2.
        let table = create_test_table_with_branch();
        let mut metadata = (*table.metadata()).clone();
        metadata.refs.insert(
            "feature".to_string(),
            SnapshotReference::new(4, SnapshotRetention::branch(None, None, None)),
        );
        let table = table.with_metadata(Arc::new(metadata));

        let timestamp = DateTime::from_timestamp_millis(2200).unwrap();
        let action = Arc::new(
            ManageSnapshotsAction::new()
                .rollback_to_timestamp(timestamp)
                .unwrap()
                .on_branch("feature"),
        );

        let mut commit = action.commit(&table).await.unwrap();
        let updates = commit.take_updates();
        match &updates[0] {
            TableUpdate::SetSnapshotRef {
                ref_name,
                reference,
            } => {
                assert_eq!(ref_name, "feature");
                assert_eq!(reference.snapshot_id, 1);
            }
            _ => panic!("Expected SetSnapshotRef update"),
        }
    }

    #[tokio::test]
    async fn test_commit_rollback_to_timestamp_before_all_snapshots() {
        let table = create_test_table_with_chain();
        let timestamp = DateTime::from_timestamp_millis(500).unwrap();
        let action = Arc::new(
            ManageSnapshotsAction::new()
                .rollback_to_timestamp(timestamp)
                .unwrap(),
        );

        let result = action.commit(&table).await;
        let err = result.expect_err("Expected error");
        assert!(err.message().contains("No snapshot found before timestamp"));
    }

    #[tokio::test]
    async fn test_commit_snapshot_not_found() {
        let table = create_test_table_with_chain();
        let action = Arc::new(
            ManageSnapshotsAction::new()
                .rollback_to_snapshot(999) // Non-existent
                .unwrap(),
        );

        let result = action.commit(&table).await;
        let err = result.expect_err("Expected error");
        assert!(err.message().contains("Snapshot 999 not found"));
    }

    #[tokio::test]
    async fn test_commit_no_operation_specified() {
        let table = create_test_table_with_chain();
        let action = Arc::new(ManageSnapshotsAction::new());

        let result = action.commit(&table).await;
        let err = result.expect_err("Expected error");
        assert!(err.message().contains("No operation specified"));
    }

    #[tokio::test]
    async fn test_commit_with_branch() {
        let table = create_test_table_with_chain();
        let action = Arc::new(
            ManageSnapshotsAction::new()
                .set_current_snapshot(2)
                .unwrap()
                .on_branch("feature"),
        );

        let result = action.commit(&table).await;
        assert!(result.is_ok());

        let mut commit = result.unwrap();
        let updates = commit.take_updates();
        let requirements = commit.take_requirements();
        match &updates[0] {
            TableUpdate::SetSnapshotRef {
                ref_name,
                reference,
            } => {
                assert_eq!(ref_name, "feature");
                assert_eq!(reference.snapshot_id, 2);
            }
            _ => panic!("Expected SetSnapshotRef update"),
        }

        assert!(matches!(
            requirements[0],
            TableRequirement::UuidMatch { .. }
        ));
        assert_eq!(requirements[1], TableRequirement::RefSnapshotIdMatch {
            r#ref: "feature".to_string(),
            snapshot_id: None,
        });
    }

    #[tokio::test]
    async fn test_commit_noop_when_snapshot_unchanged() {
        let table = create_test_table_with_chain();
        let action = Arc::new(
            ManageSnapshotsAction::new()
                .set_current_snapshot(3)
                .unwrap(),
        );

        let mut commit = action.commit(&table).await.unwrap();
        assert!(commit.take_updates().is_empty());
        assert!(commit.take_requirements().is_empty());
    }

    #[tokio::test]
    async fn test_commit_preserves_branch_retention_settings() {
        let table = create_test_table_with_chain();

        let mut metadata = (*table.metadata()).clone();
        let retention = SnapshotRetention::branch(Some(7), Some(123), Some(456));
        metadata.refs.insert(
            "feature".to_string(),
            SnapshotReference::new(3, retention.clone()),
        );
        let table = table.with_metadata(Arc::new(metadata));

        let action = Arc::new(
            ManageSnapshotsAction::new()
                .set_current_snapshot(2)
                .unwrap()
                .on_branch("feature"),
        );

        let mut commit = action.commit(&table).await.unwrap();
        let updates = commit.take_updates();
        match &updates[0] {
            TableUpdate::SetSnapshotRef {
                ref_name,
                reference,
            } => {
                assert_eq!(ref_name, "feature");
                assert_eq!(reference.snapshot_id, 2);
                assert_eq!(reference.retention, retention);
            }
            _ => panic!("Expected SetSnapshotRef update"),
        }
    }

    #[tokio::test]
    async fn test_commit_on_tag_fails() {
        let table = create_test_table_with_chain();

        let mut metadata = (*table.metadata()).clone();
        metadata.refs.insert(
            "tag-1".to_string(),
            SnapshotReference::new(2, SnapshotRetention::Tag {
                max_ref_age_ms: None,
            }),
        );
        let table = table.with_metadata(Arc::new(metadata));

        let action = Arc::new(
            ManageSnapshotsAction::new()
                .set_current_snapshot(1)
                .unwrap()
                .on_branch("tag-1"),
        );

        let err = action.commit(&table).await.expect_err("Expected error");
        assert!(err.message().contains("not a branch"));
    }
}

/// Integration tests that use MockCatalog to test the full transaction commit flow.
#[cfg(test)]
mod integration_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use as_any::Downcast;
    use chrono::DateTime;

    use super::*;
    use crate::TableIdent;
    use crate::catalog::MockCatalog;
    use crate::io::FileIOBuilder;
    use crate::spec::{
        FormatVersion, NestedField, Operation, PartitionSpec, PrimitiveType, Schema, Snapshot,
        SnapshotRetention, SortOrder, StructType, Summary, TableMetadata, Type,
    };
    use crate::table::Table;
    use crate::transaction::Transaction;
    use crate::transaction::action::ApplyTransactionAction;

    /// Create a test table with snapshot chain: s1 -> s2 -> s3 (current)
    fn create_test_table_with_chain() -> Table {
        let s1 = Snapshot::builder()
            .with_snapshot_id(1)
            .with_sequence_number(1)
            .with_timestamp_ms(1000)
            .with_manifest_list("s3://bucket/metadata/snap-1.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build();

        let s2 = Snapshot::builder()
            .with_snapshot_id(2)
            .with_parent_snapshot_id(Some(1))
            .with_sequence_number(2)
            .with_timestamp_ms(2000)
            .with_manifest_list("s3://bucket/metadata/snap-2.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build();

        let s3 = Snapshot::builder()
            .with_snapshot_id(3)
            .with_parent_snapshot_id(Some(2))
            .with_sequence_number(3)
            .with_timestamp_ms(3000)
            .with_manifest_list("s3://bucket/metadata/snap-3.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build();

        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap();

        let metadata = TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: uuid::Uuid::new_v4(),
            location: "s3://bucket/table".to_string(),
            last_sequence_number: 3,
            last_updated_ms: 3000,
            last_column_id: 1,
            current_schema_id: 0,
            schemas: HashMap::from([(0, Arc::new(schema))]),
            partition_specs: HashMap::new(),
            default_spec: Arc::new(PartitionSpec::unpartition_spec()),
            default_partition_type: StructType::new(vec![]),
            last_partition_id: -1,
            properties: HashMap::new(),
            current_snapshot_id: Some(3),
            snapshots: HashMap::from([(1, Arc::new(s1)), (2, Arc::new(s2)), (3, Arc::new(s3))]),
            snapshot_log: vec![],
            sort_orders: HashMap::from([(0, Arc::new(SortOrder::unsorted_order()))]),
            metadata_log: vec![],
            default_sort_order_id: 0,
            refs: HashMap::from([(
                MAIN_BRANCH.to_string(),
                SnapshotReference::new(3, SnapshotRetention::Branch {
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

        Table::builder()
            .metadata(metadata)
            .metadata_location("s3://bucket/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["db", "table"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap()
    }

    /// Create test table with branch: s1 -> s2 -> s3 (main) and s1 -> s4 (not on main)
    fn create_test_table_with_branch() -> Table {
        let table = create_test_table_with_chain();

        // Add s4 as a branch from s1
        let s4 = Snapshot::builder()
            .with_snapshot_id(4)
            .with_parent_snapshot_id(Some(1))
            .with_sequence_number(4)
            .with_timestamp_ms(2500)
            .with_manifest_list("s3://bucket/metadata/snap-4.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build();

        // Get mutable access to metadata and add the snapshot
        let mut metadata = (*table.metadata()).clone();
        metadata.snapshots.insert(4, Arc::new(s4));

        table.with_metadata(Arc::new(metadata))
    }

    /// Create a table with the ref updated to the new snapshot ID (simulating catalog commit).
    fn create_updated_table(original: &Table, new_snapshot_id: i64) -> Table {
        let mut metadata = (*original.metadata()).clone();
        metadata.refs.insert(
            MAIN_BRANCH.to_string(),
            SnapshotReference::new(new_snapshot_id, SnapshotRetention::Branch {
                min_snapshots_to_keep: None,
                max_snapshot_age_ms: None,
                max_ref_age_ms: None,
            }),
        );
        original.clone().with_metadata(Arc::new(metadata))
    }

    fn setup_mock_catalog_for_rollback(table: Table, target_snapshot_id: i64) -> MockCatalog {
        let load_table = table.clone();
        let updated_table = create_updated_table(&table, target_snapshot_id);

        let mut mock_catalog = MockCatalog::new();

        mock_catalog.expect_load_table().returning_st(move |_| {
            let t = load_table.clone();
            Box::pin(async move { Ok(t) })
        });

        mock_catalog
            .expect_update_table()
            .times(1)
            .returning_st(move |_commit| {
                let t = updated_table.clone();
                Box::pin(async move { Ok(t) })
            });

        mock_catalog
    }

    #[tokio::test]
    async fn test_rollback_to_snapshot_via_transaction_commit() {
        let table = create_test_table_with_chain();
        let mock_catalog = setup_mock_catalog_for_rollback(table.clone(), 2);

        // Build transaction with rollback action
        let tx = Transaction::new(&table);
        let action = tx.manage_snapshots().rollback_to_snapshot(2).unwrap();
        let tx = action.apply(tx).unwrap();

        // Commit through the catalog
        let result = tx.commit(&mock_catalog).await;
        assert!(
            result.is_ok(),
            "Transaction commit should succeed: {result:?}"
        );

        let updated_table = result.unwrap();
        // The mock catalog returns the updated table with snapshot 2 as current
        let main_ref = updated_table.metadata().refs().get(MAIN_BRANCH).unwrap();
        assert_eq!(main_ref.snapshot_id, 2);
    }

    #[tokio::test]
    async fn test_rollback_to_timestamp_via_transaction_commit() {
        let table = create_test_table_with_chain();
        // Timestamp of 1500ms should find snapshot 1 (timestamp 1000ms)
        let mock_catalog = setup_mock_catalog_for_rollback(table.clone(), 1);

        let tx = Transaction::new(&table);
        let timestamp = DateTime::from_timestamp_millis(1500).unwrap();
        let action = tx
            .manage_snapshots()
            .rollback_to_timestamp(timestamp)
            .unwrap();
        let tx = action.apply(tx).unwrap();

        let result = tx.commit(&mock_catalog).await;
        assert!(
            result.is_ok(),
            "Transaction commit should succeed: {result:?}"
        );

        let updated_table = result.unwrap();
        let main_ref = updated_table.metadata().refs().get(MAIN_BRANCH).unwrap();
        assert_eq!(main_ref.snapshot_id, 1);
    }

    #[tokio::test]
    async fn test_set_current_snapshot_via_transaction_commit() {
        let table = create_test_table_with_chain();
        let mock_catalog = setup_mock_catalog_for_rollback(table.clone(), 1);

        let tx = Transaction::new(&table);
        let action = tx.manage_snapshots().set_current_snapshot(1).unwrap();
        let tx = action.apply(tx).unwrap();

        let result = tx.commit(&mock_catalog).await;
        assert!(
            result.is_ok(),
            "Transaction commit should succeed: {result:?}"
        );

        let updated_table = result.unwrap();
        let main_ref = updated_table.metadata().refs().get(MAIN_BRANCH).unwrap();
        assert_eq!(main_ref.snapshot_id, 1);
    }

    #[tokio::test]
    async fn test_rollback_to_non_ancestor_fails_before_commit() {
        // Create table with an extra snapshot not in ancestor chain
        let table = create_test_table_with_chain();
        let s4 = Snapshot::builder()
            .with_snapshot_id(4)
            .with_parent_snapshot_id(Some(1)) // branched from s1, not ancestor of s3
            .with_sequence_number(4)
            .with_timestamp_ms(2500)
            .with_manifest_list("s3://bucket/metadata/snap-4.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build();

        let mut metadata = (*table.metadata()).clone();
        metadata.snapshots.insert(4, Arc::new(s4));
        let table = table.with_metadata(Arc::new(metadata));

        // MockCatalog - load_table still needs to be mocked for validation path
        let load_table = table.clone();
        let mut mock_catalog = MockCatalog::new();
        mock_catalog.expect_load_table().returning_st(move |_| {
            let t = load_table.clone();
            Box::pin(async move { Ok(t) })
        });
        mock_catalog.expect_update_table().times(0); // Should not be called due to validation failure

        let tx = Transaction::new(&table);
        let action = tx.manage_snapshots().rollback_to_snapshot(4).unwrap();
        let tx = action.apply(tx).unwrap();

        let result = tx.commit(&mock_catalog).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message().contains("not an ancestor"));
    }

    #[tokio::test]
    async fn test_rollback_on_new_branch_via_transaction() {
        let table = create_test_table_with_chain();

        // Create a table with feature branch pointing to snapshot 2
        let updated_table = {
            let mut metadata = (*table.metadata()).clone();
            metadata.refs.insert(
                "feature".to_string(),
                SnapshotReference::new(2, SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                }),
            );
            table.clone().with_metadata(Arc::new(metadata))
        };

        let load_table = table.clone();
        let mut mock_catalog = MockCatalog::new();
        mock_catalog.expect_load_table().returning_st(move |_| {
            let t = load_table.clone();
            Box::pin(async move { Ok(t) })
        });

        mock_catalog
            .expect_update_table()
            .times(1)
            .returning_st(move |_commit| {
                let t = updated_table.clone();
                Box::pin(async move { Ok(t) })
            });

        // Create feature branch pointing to snapshot 2
        let tx = Transaction::new(&table);
        let action = tx
            .manage_snapshots()
            .set_current_snapshot(2)
            .unwrap()
            .on_branch("feature");
        let tx = action.apply(tx).unwrap();

        let result = tx.commit(&mock_catalog).await;
        assert!(
            result.is_ok(),
            "Transaction commit should succeed: {result:?}"
        );

        let updated_table = result.unwrap();
        let feature_ref = updated_table.metadata().refs().get("feature").unwrap();
        assert_eq!(feature_ref.snapshot_id, 2);
    }

    #[tokio::test]
    async fn test_noop_rollback_skips_commit() {
        let table = create_test_table_with_chain();

        // MockCatalog - load_table needs to be mocked
        let load_table = table.clone();
        let mut mock_catalog = MockCatalog::new();
        mock_catalog.expect_load_table().returning_st(move |_| {
            let t = load_table.clone();
            Box::pin(async move { Ok(t) })
        });
        mock_catalog.expect_update_table().times(0); // No-op should not trigger update

        // Try to "rollback" to current snapshot (no-op)
        let tx = Transaction::new(&table);
        let action = tx.manage_snapshots().set_current_snapshot(3).unwrap();
        let tx = action.apply(tx).unwrap();

        // With no-op optimization, this should return the original table
        // without calling update_table
        let result = tx.commit(&mock_catalog).await;
        assert!(result.is_ok(), "No-op commit should succeed: {result:?}");
    }

    // ═══════════════════════════════════════════════════════════════════════
    // CreateBranchAction Tests
    // ═══════════════════════════════════════════════════════════════════════

    #[test]
    fn test_create_branch_action_builder() {
        let action = CreateBranchAction::new()
            .name("staging")
            .from_snapshot(1)
            .min_snapshots_to_keep(10)
            .max_snapshot_age_ms(86400000)
            .max_ref_age_ms(604800000);

        assert_eq!(action.branch_name, Some("staging".to_string()));
        assert_eq!(action.snapshot_id, Some(1));
        assert_eq!(action.min_snapshots_to_keep, Some(10));
        assert_eq!(action.max_snapshot_age_ms, Some(86400000));
        assert_eq!(action.max_ref_age_ms, Some(604800000));
    }

    #[tokio::test]
    async fn test_create_branch_from_current_snapshot() {
        let table = create_test_table_with_chain();
        let action = Arc::new(CreateBranchAction::new().name("staging"));

        let result = action.commit(&table).await;
        assert!(result.is_ok());

        let mut commit = result.unwrap();
        let updates = commit.take_updates();
        let requirements = commit.take_requirements();

        assert_eq!(updates.len(), 1);
        match &updates[0] {
            TableUpdate::SetSnapshotRef {
                ref_name,
                reference,
            } => {
                assert_eq!(ref_name, "staging");
                assert_eq!(reference.snapshot_id, 3); // Current snapshot
                assert!(reference.is_branch());
            }
            _ => panic!("Expected SetSnapshotRef update"),
        }

        assert_eq!(requirements.len(), 2);
        assert!(matches!(
            requirements[0],
            TableRequirement::UuidMatch { .. }
        ));
        assert_eq!(requirements[1], TableRequirement::RefSnapshotIdMatch {
            r#ref: "staging".to_string(),
            snapshot_id: None,
        });
    }

    #[tokio::test]
    async fn test_create_branch_from_specific_snapshot() {
        let table = create_test_table_with_chain();
        let action = Arc::new(CreateBranchAction::new().name("staging").from_snapshot(2));

        let result = action.commit(&table).await;
        assert!(result.is_ok());

        let mut commit = result.unwrap();
        let updates = commit.take_updates();

        match &updates[0] {
            TableUpdate::SetSnapshotRef { reference, .. } => {
                assert_eq!(reference.snapshot_id, 2);
            }
            _ => panic!("Expected SetSnapshotRef update"),
        }
    }

    #[tokio::test]
    async fn test_create_branch_name_required() {
        let table = create_test_table_with_chain();
        let action = Arc::new(CreateBranchAction::new().from_snapshot(1));

        let result = action.commit(&table).await;
        let err = result.expect_err("Expected error");
        assert!(err.message().contains("Branch name is required"));
    }

    #[tokio::test]
    async fn test_create_branch_cannot_use_main() {
        let table = create_test_table_with_chain();
        let action = Arc::new(CreateBranchAction::new().name("main"));

        let result = action.commit(&table).await;
        let err = result.expect_err("Expected error");
        assert!(err.message().contains("Cannot create a branch named 'main'"));
    }

    #[tokio::test]
    async fn test_create_branch_already_exists() {
        let table = create_test_table_with_chain();

        // Add an existing branch
        let mut metadata = (*table.metadata()).clone();
        metadata.refs.insert(
            "staging".to_string(),
            SnapshotReference::new(2, SnapshotRetention::branch(None, None, None)),
        );
        let table = table.with_metadata(Arc::new(metadata));

        let action = Arc::new(CreateBranchAction::new().name("staging"));

        let result = action.commit(&table).await;
        let err = result.expect_err("Expected error");
        assert!(err.message().contains("already exists"));
    }

    #[tokio::test]
    async fn test_create_branch_snapshot_not_found() {
        let table = create_test_table_with_chain();
        let action = Arc::new(CreateBranchAction::new().name("staging").from_snapshot(999));

        let result = action.commit(&table).await;
        let err = result.expect_err("Expected error");
        assert!(err.message().contains("Snapshot 999 does not exist"));
    }

    // ═══════════════════════════════════════════════════════════════════════
    // CreateTagAction Tests
    // ═══════════════════════════════════════════════════════════════════════

    #[test]
    fn test_create_tag_action_builder() {
        let action = CreateTagAction::new()
            .name("release-v1.0")
            .from_snapshot(1)
            .max_ref_age_ms(604800000);

        assert_eq!(action.tag_name, Some("release-v1.0".to_string()));
        assert_eq!(action.snapshot_id, Some(1));
        assert_eq!(action.max_ref_age_ms, Some(604800000));
    }

    #[tokio::test]
    async fn test_create_tag_from_current_snapshot() {
        let table = create_test_table_with_chain();
        let action = Arc::new(CreateTagAction::new().name("release-v1.0"));

        let result = action.commit(&table).await;
        assert!(result.is_ok());

        let mut commit = result.unwrap();
        let updates = commit.take_updates();

        assert_eq!(updates.len(), 1);
        match &updates[0] {
            TableUpdate::SetSnapshotRef {
                ref_name,
                reference,
            } => {
                assert_eq!(ref_name, "release-v1.0");
                assert_eq!(reference.snapshot_id, 3); // Current snapshot
                assert!(!reference.is_branch()); // Tags are not branches
            }
            _ => panic!("Expected SetSnapshotRef update"),
        }
    }

    #[tokio::test]
    async fn test_create_tag_from_specific_snapshot() {
        let table = create_test_table_with_chain();
        let action = Arc::new(CreateTagAction::new().name("release-v1.0").from_snapshot(1));

        let result = action.commit(&table).await;
        assert!(result.is_ok());

        let mut commit = result.unwrap();
        let updates = commit.take_updates();

        match &updates[0] {
            TableUpdate::SetSnapshotRef { reference, .. } => {
                assert_eq!(reference.snapshot_id, 1);
            }
            _ => panic!("Expected SetSnapshotRef update"),
        }
    }

    #[tokio::test]
    async fn test_create_tag_name_required() {
        let table = create_test_table_with_chain();
        let action = Arc::new(CreateTagAction::new().from_snapshot(1));

        let result = action.commit(&table).await;
        let err = result.expect_err("Expected error");
        assert!(err.message().contains("Tag name is required"));
    }

    #[tokio::test]
    async fn test_create_tag_cannot_use_main() {
        let table = create_test_table_with_chain();
        let action = Arc::new(CreateTagAction::new().name("main"));

        let result = action.commit(&table).await;
        let err = result.expect_err("Expected error");
        assert!(err.message().contains("Cannot create a tag named 'main'"));
    }

    // ═══════════════════════════════════════════════════════════════════════
    // FastForwardAction Tests
    // ═══════════════════════════════════════════════════════════════════════

    #[test]
    fn test_fast_forward_action_builder() {
        let action = FastForwardAction::new().target("main").source("staging");

        assert_eq!(action.target_ref, Some("main".to_string()));
        assert_eq!(action.source_ref, Some("staging".to_string()));
    }

    /// Create a table with main at s3 and staging at s4
    fn create_test_table_with_staging_branch() -> Table {
        let table = create_test_table_with_branch();

        // Add staging branch pointing to s4
        let mut metadata = (*table.metadata()).clone();
        metadata.refs.insert(
            "staging".to_string(),
            SnapshotReference::new(4, SnapshotRetention::branch(None, None, None)),
        );
        table.with_metadata(Arc::new(metadata))
    }

    #[tokio::test]
    async fn test_fast_forward_main_to_staging() {
        let table = create_test_table_with_staging_branch();
        let action = Arc::new(FastForwardAction::new().target("main").source("staging"));

        let result = action.commit(&table).await;
        assert!(result.is_ok());

        let mut commit = result.unwrap();
        let updates = commit.take_updates();
        let requirements = commit.take_requirements();

        assert_eq!(updates.len(), 1);
        match &updates[0] {
            TableUpdate::SetSnapshotRef {
                ref_name,
                reference,
            } => {
                assert_eq!(ref_name, "main");
                assert_eq!(reference.snapshot_id, 4); // Now points to staging's snapshot
                assert!(reference.is_branch());
            }
            _ => panic!("Expected SetSnapshotRef update"),
        }

        // Should have optimistic locking requirements
        assert_eq!(requirements.len(), 3);
        assert!(matches!(
            requirements[0],
            TableRequirement::UuidMatch { .. }
        ));
        // Target ref should match original snapshot
        assert_eq!(requirements[1], TableRequirement::RefSnapshotIdMatch {
            r#ref: "main".to_string(),
            snapshot_id: Some(3),
        });
        // Source ref should match current snapshot
        assert_eq!(requirements[2], TableRequirement::RefSnapshotIdMatch {
            r#ref: "staging".to_string(),
            snapshot_id: Some(4),
        });
    }

    #[tokio::test]
    async fn test_fast_forward_noop_when_same_snapshot() {
        let table = create_test_table_with_chain();

        // Add staging branch pointing to same snapshot as main (3)
        let mut metadata = (*table.metadata()).clone();
        metadata.refs.insert(
            "staging".to_string(),
            SnapshotReference::new(3, SnapshotRetention::branch(None, None, None)),
        );
        let table = table.with_metadata(Arc::new(metadata));

        let action = Arc::new(FastForwardAction::new().target("main").source("staging"));

        let result = action.commit(&table).await;
        assert!(result.is_ok());

        let mut commit = result.unwrap();
        // Should be a no-op
        assert!(commit.take_updates().is_empty());
        assert!(commit.take_requirements().is_empty());
    }

    #[tokio::test]
    async fn test_fast_forward_target_required() {
        let table = create_test_table_with_staging_branch();
        let action = Arc::new(FastForwardAction::new().source("staging"));

        let result = action.commit(&table).await;
        let err = result.expect_err("Expected error");
        assert!(err.message().contains("Target ref is required"));
    }

    #[tokio::test]
    async fn test_fast_forward_source_required() {
        let table = create_test_table_with_staging_branch();
        let action = Arc::new(FastForwardAction::new().target("main"));

        let result = action.commit(&table).await;
        let err = result.expect_err("Expected error");
        assert!(err.message().contains("Source ref is required"));
    }

    #[tokio::test]
    async fn test_fast_forward_target_not_found() {
        let table = create_test_table_with_staging_branch();
        let action = Arc::new(FastForwardAction::new().target("nonexistent").source("staging"));

        let result = action.commit(&table).await;
        let err = result.expect_err("Expected error");
        assert!(err.message().contains("does not exist"));
    }

    #[tokio::test]
    async fn test_fast_forward_source_not_found() {
        let table = create_test_table_with_chain();
        let action = Arc::new(FastForwardAction::new().target("main").source("nonexistent"));

        let result = action.commit(&table).await;
        let err = result.expect_err("Expected error");
        assert!(err.message().contains("does not exist"));
    }

    #[tokio::test]
    async fn test_fast_forward_cannot_update_tag() {
        let table = create_test_table_with_chain();

        // Add a tag
        let mut metadata = (*table.metadata()).clone();
        metadata.refs.insert(
            "release-v1.0".to_string(),
            SnapshotReference::new(2, SnapshotRetention::Tag { max_ref_age_ms: None }),
        );
        metadata.refs.insert(
            "staging".to_string(),
            SnapshotReference::new(3, SnapshotRetention::branch(None, None, None)),
        );
        let table = table.with_metadata(Arc::new(metadata));

        let action = Arc::new(FastForwardAction::new().target("release-v1.0").source("staging"));

        let result = action.commit(&table).await;
        let err = result.expect_err("Expected error");
        assert!(err.message().contains("tags are immutable"));
    }

    #[tokio::test]
    async fn test_fast_forward_from_tag_to_branch() {
        let table = create_test_table_with_chain();

        // Add a tag and an additional branch
        let mut metadata = (*table.metadata()).clone();
        metadata.refs.insert(
            "release-v1.0".to_string(),
            SnapshotReference::new(2, SnapshotRetention::Tag { max_ref_age_ms: None }),
        );
        metadata.refs.insert(
            "target-branch".to_string(),
            SnapshotReference::new(3, SnapshotRetention::branch(None, None, None)),
        );
        let table = table.with_metadata(Arc::new(metadata));

        // Fast-forward branch to tag's snapshot (should work - tags can be source)
        let action = Arc::new(FastForwardAction::new().target("target-branch").source("release-v1.0"));

        let result = action.commit(&table).await;
        assert!(result.is_ok());

        let mut commit = result.unwrap();
        let updates = commit.take_updates();

        match &updates[0] {
            TableUpdate::SetSnapshotRef { reference, .. } => {
                assert_eq!(reference.snapshot_id, 2); // Tag's snapshot
            }
            _ => panic!("Expected SetSnapshotRef update"),
        }
    }

    #[test]
    fn test_apply_create_branch_to_transaction() {
        let table = create_test_table_with_chain();
        let tx = Transaction::new(&table);

        let action = tx.create_branch().name("staging");
        let tx = action.apply(tx).unwrap();

        assert_eq!(tx.actions.len(), 1);
        (*tx.actions[0])
            .downcast_ref::<CreateBranchAction>()
            .expect("Action should be CreateBranchAction");
    }

    #[test]
    fn test_apply_create_tag_to_transaction() {
        let table = create_test_table_with_chain();
        let tx = Transaction::new(&table);

        let action = tx.create_tag().name("release-v1.0");
        let tx = action.apply(tx).unwrap();

        assert_eq!(tx.actions.len(), 1);
        (*tx.actions[0])
            .downcast_ref::<CreateTagAction>()
            .expect("Action should be CreateTagAction");
    }

    #[test]
    fn test_apply_fast_forward_to_transaction() {
        let table = create_test_table_with_chain();
        let tx = Transaction::new(&table);

        let action = tx.fast_forward().target("main").source("staging");
        let tx = action.apply(tx).unwrap();

        assert_eq!(tx.actions.len(), 1);
        (*tx.actions[0])
            .downcast_ref::<FastForwardAction>()
            .expect("Action should be FastForwardAction");
    }
}
