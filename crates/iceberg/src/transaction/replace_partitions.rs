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

//! This module provides `ReplacePartitionsAction` for dynamic partition replacement.
//!
//! `ReplacePartitionsAction` implements Hive-compatible dynamic partition replacement
//! semantics. Partitions are determined from the added files' partition values, and
//! ALL existing files in those partitions are atomically replaced.
//!
//! # Use Cases
//!
//! - Dynamic ETL pipelines where partitions are determined at runtime
//! - Hive-compatible INSERT OVERWRITE semantics
//! - Idempotent partition refresh operations
//!
//! # Example
//!
//! ```ignore
//! use iceberg::transaction::{Transaction, ApplyTransactionAction};
//!
//! let tx = Transaction::new(&table);
//! let action = tx.replace_partitions()
//!     .add_data_files(vec![file_for_jan_01, file_for_jan_02]);
//! let tx = action.apply(tx)?;
//! let table = tx.commit(&catalog).await?;
//! ```
//!
//! # Atomicity
//!
//! The operation is atomic: partitions are only replaced on successful commit.
//! On failure, no files are deleted from existing partitions.
//!
//! # Partition Evolution
//!
//! Only files written with the **current default partition spec** are considered for
//! replacement. Files written with older partition specs (from before a partition
//! evolution) are preserved and remain in the table.
//!
//! This conservative behavior ensures that:
//! - Historical data written under previous partition schemes is not accidentally deleted
//! - Mixed partition specs can coexist safely during migration periods
//!
//! If you need to replace data across multiple partition specs, consider using
//! `OverwriteAction` with an explicit row filter instead.
//!
//! # Unpartitioned Tables
//!
//! For unpartitioned tables (partition spec with no fields), the partition value is
//! an empty struct. This means **all existing data files will be replaced**, effectively
//! performing a full table overwrite. Use with caution on unpartitioned tables.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::{Error, ErrorKind, Result};
use crate::spec::{
    DataFile, ManifestContentType, ManifestEntry, ManifestFile, ManifestStatus, Operation,
    PartitionSpecRef, Struct,
};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};

/// Action for dynamic partition replacement.
///
/// Partitions to replace are determined from the added files' partition values.
/// All existing files in those partitions are atomically replaced.
///
/// This action is commonly used for:
/// - INSERT OVERWRITE operations with dynamic partition mode
/// - ETL pipelines that refresh specific partitions
/// - Idempotent data refresh patterns
///
/// # Partition Matching
///
/// Only files with the same partition spec ID as the current default partition spec
/// are considered for replacement. Files written with older partition specs
/// (partition evolution) are preserved.
///
/// # Conflict Detection
///
/// For concurrent operations, you can enable conflict detection to ensure the
/// partitions being replaced haven't been modified since your read snapshot:
///
/// ```ignore
/// let action = tx.replace_partitions()
///     .add_data_files(new_files)
///     .validate_from_snapshot(read_snapshot_id)
///     .validate_no_conflicting_data()
///     .validate_no_conflicting_deletes();
/// ```
///
/// This will fail the commit if any concurrent operation has added or deleted
/// files in the partitions being replaced.
pub struct ReplacePartitionsAction {
    /// Data files to add (partition values extracted from these)
    added_data_files: Vec<DataFile>,

    /// Whether to check for duplicate files
    check_duplicate: bool,

    /// Optional commit UUID
    commit_uuid: Option<Uuid>,

    /// Optional key metadata for manifest files
    key_metadata: Option<Vec<u8>>,

    /// Snapshot summary properties
    snapshot_properties: HashMap<String, String>,

    /// Snapshot ID to validate conflicts from (for optimistic concurrency)
    validate_from_snapshot_id: Option<i64>,

    /// Whether to validate no conflicting data was added since validate_from_snapshot
    validate_no_conflicting_data: bool,

    /// Whether to validate no conflicting deletes were added since validate_from_snapshot
    validate_no_conflicting_deletes: bool,
}

impl ReplacePartitionsAction {
    pub(crate) fn new() -> Self {
        Self {
            added_data_files: Vec::new(),
            check_duplicate: true,
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            validate_from_snapshot_id: None,
            validate_no_conflicting_data: false,
            validate_no_conflicting_deletes: false,
        }
    }

    /// Add a data file to the table.
    ///
    /// The file's partition values determine which partition to replace.
    pub fn add_data_file(mut self, file: DataFile) -> Self {
        self.added_data_files.push(file);
        self
    }

    /// Add multiple data files to the table.
    pub fn add_data_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(files);
        self
    }

    /// Set whether to check for duplicate files.
    pub fn with_check_duplicate(mut self, check: bool) -> Self {
        self.check_duplicate = check;
        self
    }

    /// Set commit UUID for the snapshot.
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set key metadata for manifest files.
    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }

    /// Set snapshot summary properties.
    pub fn set_snapshot_properties(mut self, properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = properties;
        self
    }

    /// Set the snapshot ID to validate conflicts from.
    ///
    /// When conflict detection is enabled, this specifies the "read snapshot" -
    /// the point in time when your query read the table. Any concurrent modifications
    /// to the partitions being replaced since this snapshot will cause a conflict.
    ///
    /// This must be called before `validate_no_conflicting_data()` or
    /// `validate_no_conflicting_deletes()` to enable conflict detection.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Capture the current snapshot when reading
    /// let read_snapshot_id = table.metadata().current_snapshot_id();
    ///
    /// // ... process data ...
    ///
    /// // Later, when committing, validate no conflicts occurred
    /// let action = tx.replace_partitions()
    ///     .add_data_files(new_files)
    ///     .validate_from_snapshot(read_snapshot_id)
    ///     .validate_no_conflicting_data();
    /// ```
    pub fn validate_from_snapshot(mut self, snapshot_id: i64) -> Self {
        self.validate_from_snapshot_id = Some(snapshot_id);
        self
    }

    /// Enable validation that no conflicting data files were added.
    ///
    /// When enabled, the commit will fail if any data files were added to the
    /// partitions being replaced since the snapshot set by `validate_from_snapshot()`.
    ///
    /// This prevents the "lost update" problem where concurrent writers add data
    /// to partitions that this operation is about to overwrite.
    ///
    /// # Panics
    ///
    /// This method requires `validate_from_snapshot()` to be called first.
    /// The validation is performed during `commit()`.
    pub fn validate_no_conflicting_data(mut self) -> Self {
        self.validate_no_conflicting_data = true;
        self
    }

    /// Enable validation that no conflicting delete files were added.
    ///
    /// When enabled, the commit will fail if any delete files (position or equality)
    /// were added targeting the partitions being replaced since the snapshot set
    /// by `validate_from_snapshot()`.
    ///
    /// This ensures consistency when concurrent operations are deleting rows
    /// from partitions that this operation is about to completely replace.
    ///
    /// # Panics
    ///
    /// This method requires `validate_from_snapshot()` to be called first.
    /// The validation is performed during `commit()`.
    pub fn validate_no_conflicting_deletes(mut self) -> Self {
        self.validate_no_conflicting_deletes = true;
        self
    }

    /// Extract unique partition values from added files.
    fn extract_partition_values(&self, partition_spec: &PartitionSpecRef) -> HashSet<Struct> {
        self.added_data_files
            .iter()
            .filter(|f| f.partition_spec_id == partition_spec.spec_id())
            .map(|f| f.partition().clone())
            .collect()
    }

    /// Find all snapshots between start_snapshot_id and current_snapshot_id (exclusive of start).
    ///
    /// Returns snapshots in order from oldest to newest.
    fn find_snapshots_since(
        table: &Table,
        start_snapshot_id: i64,
        current_snapshot_id: Option<i64>,
    ) -> Result<Vec<i64>> {
        let Some(current_id) = current_snapshot_id else {
            // No current snapshot means no commits since start
            return Ok(vec![]);
        };

        if start_snapshot_id == current_id {
            // Same snapshot, no changes
            return Ok(vec![]);
        }

        // Walk backwards from current to find the path to start
        let mut snapshot_ids = Vec::new();
        let mut current = current_id;

        loop {
            if current == start_snapshot_id {
                // Found the start, we're done
                break;
            }

            snapshot_ids.push(current);

            let snapshot = table.metadata().snapshot_by_id(current).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Snapshot {} not found in table metadata", current),
                )
            })?;

            match snapshot.parent_snapshot_id() {
                Some(parent_id) => {
                    current = parent_id;
                }
                None => {
                    // Reached the root without finding start_snapshot_id
                    // This means start_snapshot_id is not an ancestor of current
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "validate_from_snapshot {} is not an ancestor of current snapshot {}",
                            start_snapshot_id, current_id
                        ),
                    ));
                }
            }
        }

        // Reverse to get oldest-to-newest order
        snapshot_ids.reverse();
        Ok(snapshot_ids)
    }

    /// Validate that no conflicting data files were added in the given snapshots.
    async fn validate_no_conflicting_data_in_snapshots(
        table: &Table,
        snapshot_ids: &[i64],
        partitions_to_replace: &HashSet<Struct>,
        partition_spec: &PartitionSpecRef,
    ) -> Result<()> {
        for &snapshot_id in snapshot_ids {
            let snapshot = table
                .metadata()
                .snapshot_by_id(snapshot_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Snapshot {} not found", snapshot_id),
                    )
                })?;

            let manifest_list = snapshot
                .load_manifest_list(table.file_io(), &table.metadata_ref())
                .await?;

            for manifest_file in manifest_list.entries() {
                // Only check data manifests with matching partition spec
                if manifest_file.content != ManifestContentType::Data {
                    continue;
                }
                if manifest_file.partition_spec_id != partition_spec.spec_id() {
                    continue;
                }

                let manifest = manifest_file.load_manifest(table.file_io()).await?;

                for entry in manifest.entries() {
                    // Only check ADDED entries (new files in this snapshot)
                    if entry.status() != ManifestStatus::Added {
                        continue;
                    }

                    // Check if the snapshot_id matches (only entries added in this snapshot)
                    if entry.snapshot_id() != Some(snapshot_id) {
                        continue;
                    }

                    // Check if partition conflicts
                    if partitions_to_replace.contains(entry.data_file().partition()) {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!(
                                "Conflicting data file added in snapshot {}: {} in partition {:?}",
                                snapshot_id,
                                entry.data_file().file_path(),
                                entry.data_file().partition()
                            ),
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    /// Validate that no conflicting delete files were added in the given snapshots.
    async fn validate_no_conflicting_deletes_in_snapshots(
        table: &Table,
        snapshot_ids: &[i64],
        partitions_to_replace: &HashSet<Struct>,
        partition_spec: &PartitionSpecRef,
    ) -> Result<()> {
        for &snapshot_id in snapshot_ids {
            let snapshot = table
                .metadata()
                .snapshot_by_id(snapshot_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Snapshot {} not found", snapshot_id),
                    )
                })?;

            let manifest_list = snapshot
                .load_manifest_list(table.file_io(), &table.metadata_ref())
                .await?;

            for manifest_file in manifest_list.entries() {
                // Only check delete manifests with matching partition spec
                if manifest_file.content != ManifestContentType::Deletes {
                    continue;
                }
                if manifest_file.partition_spec_id != partition_spec.spec_id() {
                    continue;
                }

                let manifest = manifest_file.load_manifest(table.file_io()).await?;

                for entry in manifest.entries() {
                    // Only check ADDED entries (new delete files in this snapshot)
                    if entry.status() != ManifestStatus::Added {
                        continue;
                    }

                    // Check if the snapshot_id matches (only entries added in this snapshot)
                    if entry.snapshot_id() != Some(snapshot_id) {
                        continue;
                    }

                    // Check if partition conflicts
                    if partitions_to_replace.contains(entry.data_file().partition()) {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!(
                                "Conflicting delete file added in snapshot {}: {} in partition {:?}",
                                snapshot_id,
                                entry.data_file().file_path(),
                                entry.data_file().partition()
                            ),
                        ));
                    }
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl TransactionAction for ReplacePartitionsAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        // Empty action is a no-op
        if self.added_data_files.is_empty() {
            return Ok(ActionCommit::new(vec![], vec![]));
        }

        let partition_spec = table.metadata().default_partition_spec();

        // Extract partitions to replace from added files
        let partitions_to_replace = self.extract_partition_values(&partition_spec);

        if partitions_to_replace.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "No partition values found in added files matching current partition spec",
            ));
        }

        // Perform conflict detection if enabled
        if self.validate_no_conflicting_data || self.validate_no_conflicting_deletes {
            let validate_from_snapshot_id = self.validate_from_snapshot_id.ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "validate_from_snapshot() must be called before validate_no_conflicting_data() or validate_no_conflicting_deletes()",
                )
            })?;

            // Find all snapshots committed since the validation snapshot
            let snapshots_since = Self::find_snapshots_since(
                table,
                validate_from_snapshot_id,
                table.metadata().current_snapshot_id(),
            )?;

            // Validate no conflicting data files
            if self.validate_no_conflicting_data {
                Self::validate_no_conflicting_data_in_snapshots(
                    table,
                    &snapshots_since,
                    &partitions_to_replace,
                    &partition_spec,
                )
                .await?;
            }

            // Validate no conflicting delete files
            if self.validate_no_conflicting_deletes {
                Self::validate_no_conflicting_deletes_in_snapshots(
                    table,
                    &snapshots_since,
                    &partitions_to_replace,
                    &partition_spec,
                )
                .await?;
            }
        }

        // Create snapshot producer
        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
            vec![], // No delete files for replace partitions
        );

        // Validate added data files
        snapshot_producer.validate_added_data_files()?;

        // Check for duplicate files
        if self.check_duplicate {
            snapshot_producer.validate_duplicate_files().await?;
        }

        // Create operation with partition replacement logic
        let operation = ReplacePartitionsOperation {
            partitions_to_replace,
            partition_spec: partition_spec.clone(),
        };

        snapshot_producer
            .commit(operation, DefaultManifestProcess)
            .await
    }
}

/// Operation implementation for partition replacement.
struct ReplacePartitionsOperation {
    partitions_to_replace: HashSet<Struct>,
    partition_spec: PartitionSpecRef,
}

impl SnapshotProduceOperation for ReplacePartitionsOperation {
    fn operation(&self) -> Operation {
        // Use Overwrite operation for dynamic partition replacement.
        // Per the Iceberg spec:
        // - "overwrite": replaces data in the table in a logical way; used for INSERT OVERWRITE
        // - "replace": replaces files without changing the logical data (e.g., compaction)
        Operation::Overwrite
    }

    async fn delete_entries(
        &self,
        snapshot_producer: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        let Some(snapshot) = snapshot_producer.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_producer.table.file_io(),
                &snapshot_producer.table.metadata_ref(),
            )
            .await?;

        let mut entries_to_delete = Vec::new();

        for manifest_file in manifest_list.entries() {
            // Only process data manifests
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }

            // Skip manifests that don't match current partition spec (partition evolution)
            if manifest_file.partition_spec_id != self.partition_spec.spec_id() {
                continue;
            }

            let manifest = manifest_file
                .load_manifest(snapshot_producer.table.file_io())
                .await?;

            for entry in manifest.entries() {
                // Only consider ADDED or EXISTING entries (not already DELETED)
                if entry.status() == ManifestStatus::Deleted {
                    continue;
                }

                // Check if this file's partition matches any partition we're replacing
                if self.partitions_to_replace.contains(entry.data_file().partition()) {
                    // Create a delete entry for this file, preserving sequence info
                    let delete_entry = ManifestEntry::builder()
                        .status(ManifestStatus::Deleted)
                        .snapshot_id_opt(entry.snapshot_id())
                        .sequence_number_opt(entry.sequence_number())
                        .file_sequence_number_opt(entry.file_sequence_number)
                        .data_file(entry.data_file().clone())
                        .build();
                    entries_to_delete.push(delete_entry);
                }
            }
        }

        Ok(entries_to_delete)
    }

    async fn existing_manifest(
        &self,
        snapshot_producer: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let Some(snapshot) = snapshot_producer.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_producer.table.file_io(),
                &snapshot_producer.table.metadata_ref(),
            )
            .await?;

        // Include existing manifests that:
        // 1. Are delete manifests (always include - position/equality deletes)
        // 2. Are data manifests with different partition spec (partition evolution)
        // 3. Are data manifests that have NO files in partitions being replaced
        //
        // IMPORTANT: If a manifest has ANY files in replaced partitions, do NOT include it.
        // The files to keep will be written to a new manifest via existing_entries().
        let mut result = Vec::new();

        for manifest_file in manifest_list.entries() {
            // Always include delete manifests
            if manifest_file.content == ManifestContentType::Deletes {
                result.push(manifest_file.clone());
                continue;
            }

            // Include data manifests with different partition spec
            if manifest_file.partition_spec_id != self.partition_spec.spec_id() {
                result.push(manifest_file.clone());
                continue;
            }

            // For data manifests with current partition spec, check if ANY files
            // are in partitions being replaced
            let manifest = manifest_file
                .load_manifest(snapshot_producer.table.file_io())
                .await?;

            let has_files_to_replace = manifest.entries().iter().any(|entry| {
                entry.status() != ManifestStatus::Deleted
                    && self
                        .partitions_to_replace
                        .contains(entry.data_file().partition())
            });

            // Only include if manifest has NO files in replaced partitions
            if !has_files_to_replace {
                result.push(manifest_file.clone());
            }
        }

        Ok(result)
    }

    async fn existing_entries(
        &self,
        snapshot_producer: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        let Some(snapshot) = snapshot_producer.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_producer.table.file_io(),
                &snapshot_producer.table.metadata_ref(),
            )
            .await?;

        let mut entries_to_keep = Vec::new();

        for manifest_file in manifest_list.entries() {
            // Skip non-data manifests
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }

            // Skip manifests with different partition spec
            if manifest_file.partition_spec_id != self.partition_spec.spec_id() {
                continue;
            }

            let manifest = manifest_file
                .load_manifest(snapshot_producer.table.file_io())
                .await?;

            // Check if this manifest has any files in replaced partitions
            let has_files_to_replace = manifest.entries().iter().any(|entry| {
                entry.status() != ManifestStatus::Deleted
                    && self
                        .partitions_to_replace
                        .contains(entry.data_file().partition())
            });

            // Only process manifests that have files being replaced (mixed manifests)
            if !has_files_to_replace {
                continue;
            }

            // Collect entries that should be kept (not in replaced partitions)
            for entry in manifest.entries() {
                if entry.status() != ManifestStatus::Deleted
                    && !self
                        .partitions_to_replace
                        .contains(entry.data_file().partition())
                {
                    // Clone the inner ManifestEntry from the Arc
                    entries_to_keep.push(entry.as_ref().clone());
                }
            }
        }

        Ok(entries_to_keep)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::spec::{DataContentType, DataFileBuilder, DataFileFormat, Literal, Struct};
    use crate::transaction::tests::make_v2_minimal_table;
    use crate::transaction::{Transaction, TransactionAction};
    use crate::{TableRequirement, TableUpdate};

    #[tokio::test]
    async fn test_replace_partitions_empty_noop() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.replace_partitions();

        // Empty replace partitions should succeed with no-op
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        assert!(action_commit.take_updates().is_empty());
    }

    #[tokio::test]
    async fn test_replace_partitions_single_file() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let action = tx.replace_partitions().add_data_files(vec![data_file]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        // Should produce a snapshot
        assert_eq!(updates.len(), 2);
        assert!(matches!(updates[0], TableUpdate::AddSnapshot { .. }));
    }

    #[tokio::test]
    async fn test_replace_partitions_operation_is_overwrite() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let action = tx.replace_partitions().add_data_files(vec![data_file]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            panic!("Expected AddSnapshot update");
        };

        // Operation should be Overwrite for partition replacement
        // Per Iceberg spec, "overwrite" is for INSERT OVERWRITE operations
        assert_eq!(
            snapshot.summary().operation,
            crate::spec::Operation::Overwrite
        );
    }

    #[tokio::test]
    async fn test_replace_partitions_requirements() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let action = tx.replace_partitions().add_data_files(vec![data_file]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let requirements = action_commit.take_requirements();

        // Should have UUID match and snapshot ref match requirements
        assert_eq!(requirements.len(), 2);
        assert!(matches!(
            requirements[0],
            TableRequirement::UuidMatch { .. }
        ));
        assert!(matches!(
            requirements[1],
            TableRequirement::RefSnapshotIdMatch { .. }
        ));
    }

    #[tokio::test]
    async fn test_replace_partitions_with_snapshot_properties() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let mut properties = std::collections::HashMap::new();
        properties.insert("replace.source".to_string(), "etl_pipeline".to_string());

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let action = tx
            .replace_partitions()
            .add_data_files(vec![data_file])
            .set_snapshot_properties(properties);

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            panic!("Expected AddSnapshot update");
        };

        assert_eq!(
            snapshot
                .summary()
                .additional_properties
                .get("replace.source")
                .unwrap(),
            "etl_pipeline"
        );
    }

    #[tokio::test]
    async fn test_replace_partitions_multiple_files_same_partition() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let partition_value = Struct::from_iter([Some(Literal::long(300))]);

        let data_file1 = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(partition_value.clone())
            .build()
            .unwrap();

        let data_file2 = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data-2.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(150)
            .record_count(15)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(partition_value)
            .build()
            .unwrap();

        let action = tx
            .replace_partitions()
            .add_data_files(vec![data_file1, data_file2]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        // Should produce a single snapshot with both files
        assert_eq!(updates.len(), 2);
        assert!(matches!(updates[0], TableUpdate::AddSnapshot { .. }));
    }

    #[tokio::test]
    async fn test_replace_partitions_multiple_partitions() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file1 = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            .build()
            .unwrap();

        let data_file2 = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data-2.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(150)
            .record_count(15)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(200))]))
            .build()
            .unwrap();

        let action = tx
            .replace_partitions()
            .add_data_files(vec![data_file1, data_file2]);
        let result = Arc::new(action).commit(&table).await;

        // Should succeed with multiple partitions
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_replace_partitions_no_existing_snapshot() {
        // Table with no current snapshot (brand new table)
        // Should just add files, no deletes
        let table = make_v2_minimal_table();

        // Verify table has no current snapshot
        assert!(table.metadata().current_snapshot().is_none());

        let tx = Transaction::new(&table);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let action = tx.replace_partitions().add_data_files(vec![data_file]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        // Should succeed and create first snapshot
        assert_eq!(updates.len(), 2);
        assert!(matches!(updates[0], TableUpdate::AddSnapshot { .. }));

        // Verify the snapshot
        let snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            panic!("Expected AddSnapshot update");
        };

        // Should have no parent (first snapshot)
        assert!(snapshot.parent_snapshot_id().is_none());
    }

    #[tokio::test]
    async fn test_replace_partitions_validates_partition_spec() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        // Create a data file with a different partition spec ID
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data-wrong-spec.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(999) // Non-existent spec ID
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let action = tx.replace_partitions().add_data_files(vec![data_file]);
        let result = Arc::new(action).commit(&table).await;

        // Should fail validation - partition spec doesn't match table default
        assert!(result.is_err());
    }

    // ==================== Conflict Detection Tests ====================

    #[tokio::test]
    async fn test_validate_from_snapshot_requires_snapshot_id() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        // Enable validation without setting validate_from_snapshot
        let action = tx
            .replace_partitions()
            .add_data_files(vec![data_file])
            .validate_no_conflicting_data();

        let result = Arc::new(action).commit(&table).await;

        // Should fail because validate_from_snapshot was not called
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err
            .message()
            .contains("validate_from_snapshot() must be called"));
    }

    #[tokio::test]
    async fn test_validate_from_snapshot_no_changes() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        // Table has no current snapshot, so we use a fake snapshot ID
        // This should fail because there's no snapshot to validate from
        // when the table is empty
        let action = tx
            .replace_partitions()
            .add_data_files(vec![data_file])
            .validate_from_snapshot(12345) // Non-existent snapshot
            .validate_no_conflicting_data();

        let result = Arc::new(action).commit(&table).await;

        // Should succeed because there's no current snapshot
        // (validate_from_snapshot with no current snapshot means no conflicts possible)
        // Actually, the find_snapshots_since will return empty if current is None
        assert!(result.is_ok());
    }

    #[test]
    fn test_find_snapshots_since_same_snapshot() {
        use super::ReplacePartitionsAction;

        let table = make_v2_minimal_table();

        // When start and current are the same, should return empty
        let result = ReplacePartitionsAction::find_snapshots_since(&table, 123, Some(123));
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_find_snapshots_since_no_current() {
        use super::ReplacePartitionsAction;

        let table = make_v2_minimal_table();

        // When there's no current snapshot, should return empty
        let result = ReplacePartitionsAction::find_snapshots_since(&table, 123, None);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_validation_methods_are_chainable() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        // All validation methods should be chainable
        let action = tx
            .replace_partitions()
            .add_data_files(vec![data_file])
            .validate_from_snapshot(12345)
            .validate_no_conflicting_data()
            .validate_no_conflicting_deletes();

        // Action should compile and be usable
        // (The actual validation will fail because snapshot doesn't exist,
        // but that's expected - we're just testing the API is chainable)
        let _ = Arc::new(action);
    }
}
