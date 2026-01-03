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

//! This module contains transaction api.
//!
//! The transaction API enables changes to be made to an existing table.
//!
//! Note that this may also have side effects, such as producing new manifest
//! files.
//!
//! Below is a basic example using the "fast-append" action:
//!
//! ```ignore
//! use iceberg::transaction::{ApplyTransactionAction, Transaction};
//! use iceberg::Catalog;
//!
//! // Create a transaction.
//! let tx = Transaction::new(my_table);
//!
//! // Create a `FastAppendAction` which will not rewrite or append
//! // to existing metadata. This will create a new manifest.
//! let action = tx.fast_append().add_data_files(my_data_files);
//!
//! // Apply the fast-append action to the given transaction, returning
//! // the newly updated `Transaction`.
//! let tx = action.apply(tx).unwrap();
//!
//!
//! // End the transaction by committing to an `iceberg::Catalog`
//! // implementation. This will cause a table update to occur.
//! let table = tx
//!     .commit(&some_catalog_impl)
//!     .await
//!     .unwrap();
//! ```

/// The `ApplyTransactionAction` trait provides an `apply` method
/// that allows users to apply a transaction action to a `Transaction`.
mod action;

pub use action::*;
pub use evolve_partition::EvolvePartitionAction;
pub use expire_snapshots::{
    CleanupExecutionResult, CleanupLevel, CleanupPlan, ExpireOptions, ExpireProgressCallback,
    ExpireProgressEvent, ExpireSnapshotsAction, ExpireSnapshotsPlan, ExpireSnapshotsResult,
    RetentionPolicy, compute_cleanup_plan, execute_cleanup_plan,
};
pub use insert_overwrite::{InsertOverwriteAction, InsertOverwriteMode};
pub use manage_snapshots::{
    CreateBranchAction, CreateTagAction, FastForwardAction, ManageSnapshotsAction,
    ManageSnapshotsOperation,
};
pub use overwrite::OverwriteAction;
pub use remove_orphan_files::{
    FileUriNormalizer, NormalizedUri, OrphanFileInfo, OrphanFileType, OrphanFilesProgressCallback,
    OrphanFilesProgressEvent, PrefixMismatchMode, ReferenceFileCollector, RemoveOrphanFilesAction,
    RemoveOrphanFilesResult,
};
pub use replace_partitions::ReplacePartitionsAction;
pub use rewrite_data_files::{
    FileGroup, FileGroupFailure, FileGroupResult, FileGroupRewriteResult, RewriteDataFilesAction,
    RewriteDataFilesCommitter, RewriteDataFilesOptions, RewriteDataFilesPlan,
    RewriteDataFilesPlanner, RewriteDataFilesResult, RewriteJobOrder, RewriteStrategy,
};
pub use rewrite_manifests::{
    DEFAULT_MAX_ENTRIES_IN_MEMORY, DEFAULT_TARGET_MANIFEST_SIZE_BYTES, ManifestPredicate,
    RewriteManifestsAction, RewriteManifestsOptions, RewriteManifestsPlanner,
    RewriteManifestsResult,
};
// Internal-only exports for rewrite_manifests implementation (used by action commit)
#[allow(unused_imports)]
pub(crate) use rewrite_manifests::{ManifestRewriter, generate_manifest_path};
pub use rewrite_position_delete_files::RewritePositionDeleteFilesAction;
pub use row_delta::RowDeltaAction;
pub use update_schema::UpdateSchemaAction;
mod append;
#[cfg(test)]
mod concurrent;
mod delete;
mod evolve_partition;
pub mod expire_snapshots;
mod insert_overwrite;
mod manage_snapshots;
mod overwrite;
pub mod remove_orphan_files;
mod replace_partitions;
pub mod rewrite_data_files;
pub mod rewrite_manifests;
/// Action for rewriting position delete files.
pub mod rewrite_position_delete_files;
mod row_delta;
mod snapshot;
mod sort_order;
mod update_location;
mod update_properties;
mod update_schema;
mod update_statistics;
mod upgrade_format_version;

use std::sync::Arc;
use std::time::Duration;

use backon::{BackoffBuilder, ExponentialBackoff, ExponentialBuilder, RetryableWithContext};

use crate::error::Result;
use crate::spec::TableProperties;
use crate::table::Table;
use crate::transaction::action::BoxedTransactionAction;
use crate::transaction::append::FastAppendAction;
use crate::transaction::delete::DeleteAction;
use crate::transaction::sort_order::ReplaceSortOrderAction;
use crate::transaction::update_location::UpdateLocationAction;
use crate::transaction::update_properties::UpdatePropertiesAction;
use crate::transaction::update_statistics::UpdateStatisticsAction;
use crate::transaction::upgrade_format_version::UpgradeFormatVersionAction;
use crate::{Catalog, Error, ErrorKind, TableCommit, TableRequirement, TableUpdate};

/// Table transaction.
#[derive(Clone)]
pub struct Transaction {
    table: Table,
    actions: Vec<BoxedTransactionAction>,
}

impl Transaction {
    /// Creates a new transaction.
    pub fn new(table: &Table) -> Self {
        Self {
            table: table.clone(),
            actions: vec![],
        }
    }

    fn update_table_metadata(table: Table, updates: &[TableUpdate]) -> Result<Table> {
        let mut metadata_builder = table.metadata().clone().into_builder(None);
        for update in updates {
            metadata_builder = update.clone().apply(metadata_builder)?;
        }

        Ok(table.with_metadata(Arc::new(metadata_builder.build()?.metadata)))
    }

    /// Applies an [`ActionCommit`] to the given [`Table`], returning a new [`Table`] with updated metadata.
    /// Also appends any derived [`TableUpdate`]s and [`TableRequirement`]s to the provided vectors.
    fn apply(
        table: Table,
        mut action_commit: ActionCommit,
        existing_updates: &mut Vec<TableUpdate>,
        existing_requirements: &mut Vec<TableRequirement>,
    ) -> Result<Table> {
        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();

        for requirement in &requirements {
            requirement.check(Some(table.metadata()))?;
        }

        let updated_table = Self::update_table_metadata(table, &updates)?;

        existing_updates.extend(updates);
        existing_requirements.extend(requirements);

        Ok(updated_table)
    }

    /// Sets table to a new version.
    pub fn upgrade_table_version(&self) -> UpgradeFormatVersionAction {
        UpgradeFormatVersionAction::new()
    }

    /// Update table's property.
    pub fn update_table_properties(&self) -> UpdatePropertiesAction {
        UpdatePropertiesAction::new()
    }

    /// Update table schema.
    pub fn update_schema(&self) -> UpdateSchemaAction {
        UpdateSchemaAction::new()
    }

    /// Creates a fast append action.
    pub fn fast_append(&self) -> FastAppendAction {
        FastAppendAction::new()
    }

    /// Creates a delete action for committing delete files.
    ///
    /// This action supports both position delete files and equality delete files.
    /// Use this to commit delete files produced by `PositionDeleteFileWriter` or
    /// `EqualityDeleteFileWriter`.
    pub fn delete(&self) -> DeleteAction {
        DeleteAction::new()
    }

    /// Creates a row delta action for atomic row-level operations.
    ///
    /// This action enables atomic commits that combine data file additions and
    /// delete file additions in a single snapshot. Use this for:
    /// - UPDATE operations (delete old rows + insert updated rows)
    /// - MERGE operations (combined inserts, updates, deletes)
    ///
    /// Unlike separate `fast_append()` and `delete()` operations, `row_delta()`
    /// ensures atomicity - either all changes are committed or none are.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let tx = Transaction::new(&table);
    /// let action = tx.row_delta()
    ///     .add_data_files(new_rows)
    ///     .add_position_delete_files(deleted_rows);
    /// let tx = action.apply(tx)?;
    /// let table = tx.commit(&catalog).await?;
    /// ```
    pub fn row_delta(&self) -> RowDeltaAction {
        RowDeltaAction::new()
    }

    /// Creates a replace partitions action for dynamic partition replacement.
    ///
    /// This action implements Hive-compatible INSERT OVERWRITE semantics:
    /// partitions are determined from the added files' partition values,
    /// and ALL existing files in those partitions are atomically replaced.
    ///
    /// # Use Cases
    ///
    /// - Dynamic ETL pipelines where partitions are determined at runtime
    /// - Idempotent partition refresh operations
    /// - INSERT OVERWRITE with dynamic partition mode
    ///
    /// # Example
    ///
    /// ```ignore
    /// use iceberg::transaction::{Transaction, ApplyTransactionAction};
    ///
    /// let tx = Transaction::new(&table);
    /// let action = tx.replace_partitions()
    ///     .add_data_files(new_files);
    /// let tx = action.apply(tx)?;
    /// let table = tx.commit(&catalog).await?;
    /// ```
    ///
    /// # Partition Evolution
    ///
    /// Only files with the same partition spec ID as the current default
    /// partition spec are replaced. Files written with older partition specs
    /// are preserved.
    pub fn replace_partitions(&self) -> ReplacePartitionsAction {
        ReplacePartitionsAction::new()
    }

    /// Creates an overwrite action for filter-based data replacement.
    ///
    /// This action implements INSERT OVERWRITE semantics with an explicit filter:
    /// files matching the filter are deleted and replaced with new files.
    /// Unlike `replace_partitions()` where partitions are dynamically determined
    /// from added files, this action takes an explicit row filter.
    ///
    /// # Use Cases
    ///
    /// - INSERT OVERWRITE with explicit WHERE clause
    /// - Targeted data correction/replacement
    /// - Conditional bulk delete operations
    ///
    /// # Example
    ///
    /// ```ignore
    /// use iceberg::expr::Reference;
    /// use iceberg::transaction::{Transaction, ApplyTransactionAction};
    ///
    /// let tx = Transaction::new(&table);
    /// // Delete all data where date = '2024-01-01' and add new files
    /// let filter = Reference::new("date").equal_to(Datum::string("2024-01-01"));
    /// let action = tx.overwrite()
    ///     .overwrite_filter(filter)
    ///     .add_data_files(new_files);
    /// let tx = action.apply(tx)?;
    /// let table = tx.commit(&catalog).await?;
    /// ```
    ///
    /// # Filter Semantics
    ///
    /// The filter is projected to partition predicates using `InclusiveProjection`,
    /// meaning files are deleted if their partition **could contain** matching rows.
    pub fn overwrite(&self) -> OverwriteAction {
        OverwriteAction::new()
    }

    /// Creates an insert overwrite action with dynamic or static semantics.
    ///
    /// By default this uses dynamic overwrite semantics (replace only partitions touched
    /// by the incoming data). Use `static_overwrite(filter)` to replace partitions matching
    /// an explicit filter, even if there are no incoming rows for them.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use iceberg::expr::Reference;
    /// use iceberg::transaction::{Transaction, ApplyTransactionAction};
    ///
    /// let tx = Transaction::new(&table);
    /// let action = tx.insert_overwrite()
    ///     .add_data_files(new_files);
    /// let tx = action.apply(tx)?;
    ///
    /// let filter = Reference::new("date").equal_to(Datum::string("2024-01-01"));
    /// let action = tx.insert_overwrite()
    ///     .static_overwrite(filter)
    ///     .add_data_files(new_files);
    /// let tx = action.apply(tx)?;
    /// ```
    pub fn insert_overwrite(&self) -> InsertOverwriteAction {
        InsertOverwriteAction::new()
    }

    /// Creates replace sort order action.
    pub fn replace_sort_order(&self) -> ReplaceSortOrderAction {
        ReplaceSortOrderAction::new()
    }

    /// Set the location of table
    pub fn update_location(&self) -> UpdateLocationAction {
        UpdateLocationAction::new()
    }

    /// Update the statistics of table
    pub fn update_statistics(&self) -> UpdateStatisticsAction {
        UpdateStatisticsAction::new()
    }

    /// Creates a partition evolution action.
    ///
    /// This action allows adding a new partition spec to the table and optionally
    /// setting it as the default for new writes. Existing data files retain their
    /// original partition spec.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use iceberg::spec::{Transform, UnboundPartitionSpec};
    /// use iceberg::transaction::{Transaction, ApplyTransactionAction};
    ///
    /// let tx = Transaction::new(&table);
    ///
    /// // Add a new partition spec and set it as default
    /// let new_spec = UnboundPartitionSpec::builder()
    ///     .add_partition_field(1, "year", Transform::Year)?
    ///     .build();
    ///
    /// let action = tx.evolve_partition_spec()
    ///     .add_spec(new_spec)
    ///     .set_default();
    /// let tx = action.apply(tx)?;
    /// let table = tx.commit(&catalog).await?;
    /// ```
    pub fn evolve_partition_spec(&self) -> EvolvePartitionAction {
        EvolvePartitionAction::new()
    }

    /// Creates a rewrite data files action for table compaction.
    ///
    /// This action consolidates small data files into larger, target-sized files
    /// using bin-packing. It is the Rust-native implementation of Iceberg's
    /// `rewrite_data_files` maintenance action.
    ///
    /// # Use Cases
    ///
    /// - Consolidate small files from streaming ingestion
    /// - Reduce query planning overhead from high file count
    /// - Merge position/equality deletes into data files (V2 tables)
    /// - Optimize storage costs by reducing object store API calls
    ///
    /// # Example
    ///
    /// ```ignore
    /// use iceberg::transaction::{Transaction, ApplyTransactionAction};
    ///
    /// let tx = Transaction::new(&table);
    ///
    /// // Basic compaction with default settings
    /// let action = tx.rewrite_data_files();
    /// let tx = action.apply(tx)?;
    /// let table = tx.commit(&catalog).await?;
    ///
    /// // Compaction with custom settings
    /// let tx = Transaction::new(&table);
    /// let action = tx.rewrite_data_files()
    ///     .target_file_size_bytes(128 * 1024 * 1024)  // 128 MB target
    ///     .min_input_files(10)                         // Require more files
    ///     .max_concurrent_file_group_rewrites(4)       // Limit parallelism
    ///     .remove_dangling_deletes(true);              // Clean up deletes
    /// let tx = action.apply(tx)?;
    /// let table = tx.commit(&catalog).await?;
    /// ```
    ///
    /// # Atomicity
    ///
    /// The operation is atomic: files are only replaced on successful commit.
    /// On failure, original files remain unchanged.
    ///
    /// # Format Version Support
    ///
    /// - **V1 tables**: Simple bin-packing of data files
    /// - **V2+ tables**: Full compaction with delete file reconciliation
    pub fn rewrite_data_files(&self) -> RewriteDataFilesAction {
        RewriteDataFilesAction::new()
    }

    /// Creates a rewrite manifests action for manifest consolidation.
    ///
    /// This action consolidates small manifests into larger, optimally-sized
    /// manifests while preserving data file references and clustering files
    /// by partition for optimal query pruning.
    ///
    /// # Use Cases
    ///
    /// - Consolidate manifests from frequent streaming writes
    /// - Reduce query planning overhead from manifest count
    /// - Optimize metadata storage and API costs
    /// - Improve partition pruning effectiveness
    ///
    /// # Example
    ///
    /// ```ignore
    /// use iceberg::transaction::{Transaction, ApplyTransactionAction};
    ///
    /// let tx = Transaction::new(&table);
    ///
    /// // Build manifest rewrite action with default settings
    /// let action = tx.rewrite_manifests();
    /// let tx = action.apply(tx)?;
    /// // let table = tx.commit(&catalog).await?;
    ///
    /// // Custom configuration
    /// let tx = Transaction::new(&table);
    /// let action = tx.rewrite_manifests()
    ///     .target_size_bytes(16 * 1024 * 1024)  // 16 MB target
    ///     .rewrite_if_smaller_than(5 * 1024 * 1024);  // Only small manifests
    /// let tx = action.apply(tx)?;
    /// // let table = tx.commit(&catalog).await?;
    /// ```
    ///
    /// # Atomicity
    ///
    /// The operation is atomic: manifests are only replaced on successful commit.
    /// On failure, original manifests remain unchanged.
    ///
    /// # Format Version Support
    ///
    /// - **V1 tables**: Data manifests only
    /// - **V2+ tables**: Data and delete manifests (configurable)
    pub fn rewrite_manifests(&self) -> RewriteManifestsAction {
        RewriteManifestsAction::new()
    }

    /// Creates a rewrite position delete files action.
    pub fn rewrite_position_delete_files(&self) -> RewritePositionDeleteFilesAction {
        RewritePositionDeleteFilesAction::new()
    }

    /// Creates an expire snapshots action for table maintenance.
    ///
    /// This action removes old snapshots that are no longer needed for time travel
    /// or branch/tag references, and optionally deletes the orphaned data files
    /// and manifest files.
    ///
    /// # Use Cases
    ///
    /// - Remove snapshots older than a retention period
    /// - Clean up storage after data deletion operations
    /// - Reduce metadata size for faster query planning
    /// - Comply with data retention policies
    ///
    /// # Example
    ///
    /// ```ignore
    /// use chrono::{Duration, Utc};
    /// use iceberg::transaction::{Transaction, ApplyTransactionAction};
    ///
    /// let tx = Transaction::new(&table);
    ///
    /// // Expire snapshots older than 7 days, keep at least 10
    /// let action = tx.expire_snapshots()
    ///     .older_than(Utc::now() - Duration::days(7))
    ///     .retain_last(10);
    /// let tx = action.apply(tx)?;
    /// let table = tx.commit(&catalog).await?;
    ///
    /// // Dry run to preview what would be deleted
    /// let tx = Transaction::new(&table);
    /// let action = tx.expire_snapshots()
    ///     .older_than(Utc::now() - Duration::days(7))
    ///     .dry_run(true);
    /// let tx = action.apply(tx)?;
    /// ```
    ///
    /// # Safety
    ///
    /// The action protects:
    /// - The current snapshot
    /// - Snapshots referenced by branches and tags
    /// - Minimum number of snapshots per branch (configurable)
    ///
    /// # Cleanup Behavior
    ///
    /// - Default: metadata-only expiration (no file deletion)
    /// - `delete_files(true)`: enable file cleanup planning/execution
    ///
    /// File deletion is performed via [`ExpireSnapshotsAction::execute`] or by
    /// creating a cleanup plan and running [`execute_cleanup_plan`] after commit.
    pub fn expire_snapshots(&self) -> ExpireSnapshotsAction {
        ExpireSnapshotsAction::new()
    }

    /// Creates a snapshot management action for rollback and snapshot operations.
    ///
    /// This action enables changing the current snapshot of a table without
    /// creating new snapshots. It supports:
    /// - **Rollback operations**: Revert to a previous snapshot (validates ancestry)
    /// - **Set current snapshot**: Set any snapshot as current (no ancestry validation)
    ///
    /// # Use Cases
    ///
    /// - **Disaster Recovery**: Revert to last known good state after data corruption
    /// - **Pipeline Debugging**: Roll back and re-run pipeline stages
    /// - **Branch Operations**: Switch between snapshots from different branches
    ///
    /// # Example
    ///
    /// ```ignore
    /// use iceberg::transaction::{Transaction, ApplyTransactionAction};
    ///
    /// // Rollback to a specific snapshot (validates ancestry)
    /// let tx = Transaction::new(&table);
    /// let tx = tx.manage_snapshots()
    ///     .rollback_to_snapshot(previous_snapshot_id)?
    ///     .apply(tx)?;
    /// let table = tx.commit(&catalog).await?;
    ///
    /// // Rollback to timestamp
    /// let tx = Transaction::new(&table);
    /// let yesterday = chrono::Utc::now() - chrono::Duration::days(1);
    /// let tx = tx.manage_snapshots()
    ///     .rollback_to_timestamp(yesterday)?
    ///     .apply(tx)?;
    /// let table = tx.commit(&catalog).await?;
    ///
    /// // Set current snapshot (no ancestry validation)
    /// let tx = Transaction::new(&table);
    /// let tx = tx.manage_snapshots()
    ///     .set_current_snapshot(any_snapshot_id)?
    ///     .apply(tx)?;
    /// let table = tx.commit(&catalog).await?;
    /// ```
    ///
    /// # Semantic Differences
    ///
    /// | Operation | Ancestor Validation | Use Case |
    /// |-----------|---------------------|----------|
    /// | `rollback_to_snapshot` | Required | Safe rollback along linear history |
    /// | `rollback_to_timestamp` | Required | Time-based recovery |
    /// | `set_current_snapshot` | Not required | Branch operations, cherry-picking |
    pub fn manage_snapshots(&self) -> ManageSnapshotsAction {
        ManageSnapshotsAction::new()
    }

    /// Creates a branch from the current or specified snapshot.
    ///
    /// A branch is a mutable named reference that can be updated to point to
    /// new snapshots. This is useful for Write-Audit-Publish (WAP) workflows
    /// where changes are staged on a branch before being promoted to main.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use iceberg::transaction::{Transaction, ApplyTransactionAction};
    ///
    /// // Create a branch from the current snapshot
    /// let tx = Transaction::new(&table);
    /// let tx = tx.create_branch()
    ///     .name("staging")
    ///     .apply(tx)?;
    /// let table = tx.commit(&catalog).await?;
    ///
    /// // Create a branch from a specific snapshot
    /// let tx = Transaction::new(&table);
    /// let tx = tx.create_branch()
    ///     .name("staging")
    ///     .from_snapshot(snapshot_id)
    ///     .apply(tx)?;
    /// let table = tx.commit(&catalog).await?;
    /// ```
    pub fn create_branch(&self) -> CreateBranchAction {
        CreateBranchAction::new()
    }

    /// Creates a tag from the current or specified snapshot.
    ///
    /// A tag is an immutable named reference that always points to the same
    /// snapshot. Use tags to mark important points like releases or audits.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use iceberg::transaction::{Transaction, ApplyTransactionAction};
    ///
    /// // Create a tag from the current snapshot
    /// let tx = Transaction::new(&table);
    /// let tx = tx.create_tag()
    ///     .name("release-v1.0")
    ///     .apply(tx)?;
    /// let table = tx.commit(&catalog).await?;
    /// ```
    pub fn create_tag(&self) -> CreateTagAction {
        CreateTagAction::new()
    }

    /// Fast-forwards a branch to match another reference's snapshot.
    ///
    /// This is the key operation for completing Write-Audit-Publish workflows:
    /// after staging changes on a branch and auditing them, use fast-forward
    /// to atomically update main to the branch's snapshot.
    ///
    /// # Example
    ///
    /// ```ignore
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
    pub fn fast_forward(&self) -> FastForwardAction {
        FastForwardAction::new()
    }

    /// Commit transaction.
    pub async fn commit(self, catalog: &dyn Catalog) -> Result<Table> {
        if self.actions.is_empty() {
            // nothing to commit
            return Ok(self.table);
        }

        let table_props =
            TableProperties::try_from(self.table.metadata().properties()).map_err(|e| {
                Error::new(ErrorKind::DataInvalid, "Invalid table properties").with_source(e)
            })?;

        let backoff = Self::build_backoff(table_props)?;
        let tx = self;

        (|mut tx: Transaction| async {
            let result = tx.do_commit(catalog).await;
            (tx, result)
        })
        .retry(backoff)
        .sleep(tokio::time::sleep)
        .context(tx)
        .when(|e| e.retryable())
        .await
        .1
    }

    fn build_backoff(props: TableProperties) -> Result<ExponentialBackoff> {
        Ok(ExponentialBuilder::new()
            .with_min_delay(Duration::from_millis(props.commit_min_retry_wait_ms))
            .with_max_delay(Duration::from_millis(props.commit_max_retry_wait_ms))
            .with_total_delay(Some(Duration::from_millis(
                props.commit_total_retry_timeout_ms,
            )))
            .with_max_times(props.commit_num_retries)
            .with_factor(2.0)
            .build())
    }

    async fn do_commit(&mut self, catalog: &dyn Catalog) -> Result<Table> {
        let refreshed = catalog.load_table(self.table.identifier()).await?;

        if self.table.metadata() != refreshed.metadata()
            || self.table.metadata_location() != refreshed.metadata_location()
        {
            // current base is stale, use refreshed as base and re-apply transaction actions
            self.table = refreshed.clone();
        }

        let mut current_table = self.table.clone();
        let mut existing_updates: Vec<TableUpdate> = vec![];
        let mut existing_requirements: Vec<TableRequirement> = vec![];

        for action in &self.actions {
            let action_commit = Arc::clone(action).commit(&current_table).await?;
            // apply action commit to current_table
            current_table = Self::apply(
                current_table,
                action_commit,
                &mut existing_updates,
                &mut existing_requirements,
            )?;
        }

        // If there are no updates, skip the catalog write to avoid metadata churn.
        // This can happen when actions are no-ops (e.g., empty delete action).
        if existing_updates.is_empty() {
            return Ok(current_table);
        }

        let table_commit = TableCommit::builder()
            .ident(self.table.identifier().to_owned())
            .updates(existing_updates)
            .requirements(existing_requirements)
            .build();

        catalog.update_table(table_commit).await
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::BufReader;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use crate::catalog::MockCatalog;
    use crate::io::FileIOBuilder;
    use crate::spec::TableMetadata;
    use crate::table::Table;
    use crate::transaction::{ApplyTransactionAction, Transaction};
    use crate::{Catalog, Error, ErrorKind, TableCreation, TableIdent};

    pub fn make_v1_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV1Valid.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/00001-11111111-1111-1111-1111-111111111111.metadata.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap()
    }

    pub fn make_v2_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2Valid.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/00001-11111111-1111-1111-1111-111111111111.metadata.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap()
    }

    pub fn make_v2_minimal_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2ValidMinimal.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/00001-11111111-1111-1111-1111-111111111111.metadata.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap()
    }

    pub(crate) async fn make_v3_minimal_table_in_catalog(catalog: &impl Catalog) -> Table {
        let table_ident =
            TableIdent::from_strs([format!("ns1-{}", uuid::Uuid::new_v4()), "test1".to_string()])
                .unwrap();

        catalog
            .create_namespace(table_ident.namespace(), HashMap::new())
            .await
            .unwrap();

        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV3ValidMinimal.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let base_metadata = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        let table_creation = TableCreation::builder()
            .schema((**base_metadata.current_schema()).clone())
            .partition_spec((**base_metadata.default_partition_spec()).clone())
            .sort_order((**base_metadata.default_sort_order()).clone())
            .name(table_ident.name().to_string())
            .format_version(crate::spec::FormatVersion::V3)
            .build();

        catalog
            .create_table(table_ident.namespace(), table_creation)
            .await
            .unwrap()
    }

    /// Helper function to create a test table with retry properties
    pub(super) fn setup_test_table(num_retries: &str) -> Table {
        let table = make_v2_table();

        // Set retry properties
        let mut props = HashMap::new();
        props.insert("commit.retry.min-wait-ms".to_string(), "10".to_string());
        props.insert("commit.retry.max-wait-ms".to_string(), "100".to_string());
        props.insert(
            "commit.retry.total-timeout-ms".to_string(),
            "1000".to_string(),
        );
        props.insert(
            "commit.retry.num-retries".to_string(),
            num_retries.to_string(),
        );

        // Update table properties
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .set_properties(props)
            .unwrap()
            .build()
            .unwrap()
            .metadata;

        table.with_metadata(Arc::new(metadata))
    }

    /// Helper function to create a transaction with a simple update action
    fn create_test_transaction(table: &Table) -> Transaction {
        let tx = Transaction::new(table);
        tx.update_table_properties()
            .set("test.key".to_string(), "test.value".to_string())
            .apply(tx)
            .unwrap()
    }

    /// Helper function to set up a mock catalog with retryable errors
    fn setup_mock_catalog_with_retryable_errors(
        success_after_attempts: Option<u32>,
        expected_calls: usize,
    ) -> MockCatalog {
        let mut mock_catalog = MockCatalog::new();

        mock_catalog
            .expect_load_table()
            .returning_st(|_| Box::pin(async move { Ok(make_v2_table()) }));

        let attempts = AtomicU32::new(0);
        mock_catalog
            .expect_update_table()
            .times(expected_calls)
            .returning_st(move |_| {
                if let Some(success_after_attempts) = success_after_attempts {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    if attempts.load(Ordering::SeqCst) <= success_after_attempts {
                        Box::pin(async move {
                            Err(
                                Error::new(ErrorKind::CatalogCommitConflicts, "Commit conflict")
                                    .with_retryable(true),
                            )
                        })
                    } else {
                        Box::pin(async move { Ok(make_v2_table()) })
                    }
                } else {
                    // Always fail with retryable error
                    Box::pin(async move {
                        Err(
                            Error::new(ErrorKind::CatalogCommitConflicts, "Commit conflict")
                                .with_retryable(true),
                        )
                    })
                }
            });

        mock_catalog
    }

    /// Helper function to set up a mock catalog with non-retryable error
    fn setup_mock_catalog_with_non_retryable_error() -> MockCatalog {
        let mut mock_catalog = MockCatalog::new();

        mock_catalog
            .expect_load_table()
            .returning_st(|_| Box::pin(async move { Ok(make_v2_table()) }));

        mock_catalog
            .expect_update_table()
            .times(1) // Should only be called once since error is not retryable
            .returning_st(move |_| {
                Box::pin(async move {
                    Err(Error::new(ErrorKind::Unexpected, "Non-retryable error")
                        .with_retryable(false))
                })
            });

        mock_catalog
    }

    #[tokio::test]
    async fn test_commit_retryable_error() {
        // Create a test table with retry properties
        let table = setup_test_table("3");

        // Create a transaction with a simple update action
        let tx = create_test_transaction(&table);

        // Create a mock catalog that fails twice then succeeds
        let mock_catalog = setup_mock_catalog_with_retryable_errors(Some(2), 3);

        // Commit the transaction
        let result = tx.commit(&mock_catalog).await;

        // Verify the result
        assert!(result.is_ok(), "Transaction should eventually succeed");
    }

    #[tokio::test]
    async fn test_commit_non_retryable_error() {
        // Create a test table with retry properties
        let table = setup_test_table("3");

        // Create a transaction with a simple update action
        let tx = create_test_transaction(&table);

        // Create a mock catalog that fails with non-retryable error
        let mock_catalog = setup_mock_catalog_with_non_retryable_error();

        // Commit the transaction
        let result = tx.commit(&mock_catalog).await;

        // Verify the result
        assert!(result.is_err(), "Transaction should fail immediately");
        if let Err(err) = result {
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert_eq!(err.message(), "Non-retryable error");
            assert!(!err.retryable(), "Error should not be retryable");
        }
    }

    #[tokio::test]
    async fn test_commit_max_retries_exceeded() {
        // Create a test table with retry properties (only allow 2 retries)
        let table = setup_test_table("2");

        // Create a transaction with a simple update action
        let tx = create_test_transaction(&table);

        // Create a mock catalog that always fails with retryable error
        let mock_catalog = setup_mock_catalog_with_retryable_errors(None, 3); // Initial attempt + 2 retries = 3 total attempts

        // Commit the transaction
        let result = tx.commit(&mock_catalog).await;

        // Verify the result
        assert!(result.is_err(), "Transaction should fail after max retries");
        if let Err(err) = result {
            assert_eq!(err.kind(), ErrorKind::CatalogCommitConflicts);
            assert_eq!(err.message(), "Commit conflict");
            assert!(err.retryable(), "Error should be retryable");
        }
    }
}

#[cfg(test)]
mod test_row_lineage {
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, Struct,
    };
    use crate::transaction::tests::make_v3_minimal_table_in_catalog;
    use crate::transaction::{ApplyTransactionAction, Transaction};

    #[tokio::test]
    async fn test_fast_append_with_row_lineage() {
        // Helper function to create a data file with specified number of rows
        fn file_with_rows(record_count: u64) -> DataFile {
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(format!("test/{record_count}.parquet"))
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(100)
                .record_count(record_count)
                .partition(Struct::from_iter([Some(Literal::long(0))]))
                .partition_spec_id(0)
                .build()
                .unwrap()
        }
        let catalog = new_memory_catalog().await;

        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Check initial state - next_row_id should be 0
        assert_eq!(table.metadata().next_row_id(), 0);

        // First fast append with 30 rows
        let tx = Transaction::new(&table);
        let data_file_30 = file_with_rows(30);
        let action = tx.fast_append().add_data_files(vec![data_file_30]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Check snapshot and table state after first append
        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.first_row_id(), Some(0));
        assert_eq!(table.metadata().next_row_id(), 30);

        // Check written manifest for first_row_id
        let manifest_list = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        assert_eq!(manifest_list.entries().len(), 1);
        let manifest_file = &manifest_list.entries()[0];
        assert_eq!(manifest_file.first_row_id, Some(0));

        // Second fast append with 17 and 11 rows
        let tx = Transaction::new(&table);
        let data_file_17 = file_with_rows(17);
        let data_file_11 = file_with_rows(11);
        let action = tx
            .fast_append()
            .add_data_files(vec![data_file_17, data_file_11]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Check snapshot and table state after second append
        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.first_row_id(), Some(30));
        assert_eq!(table.metadata().next_row_id(), 30 + 17 + 11);

        // Check written manifest for first_row_id
        let manifest_list = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert_eq!(manifest_list.entries().len(), 2);
        let manifest_file = &manifest_list.entries()[1];
        assert_eq!(manifest_file.first_row_id, Some(30));
    }
}
