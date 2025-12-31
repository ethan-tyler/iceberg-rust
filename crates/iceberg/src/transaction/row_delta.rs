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

//! This module provides `RowDeltaAction` for atomic row-level mutations.
//!
//! `RowDeltaAction` enables atomic commits that combine data file additions
//! and delete file additions in a single snapshot. This is used for:
//! - UPDATE operations (delete old rows + insert updated rows)
//! - MERGE operations (combined inserts, updates, deletes)
//!
//! Unlike separate `fast_append()` and `delete()` operations, `RowDeltaAction`
//! ensures atomicity - either all changes are committed or none are.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{
    DataFile, ManifestContentType, ManifestEntry, ManifestFile, ManifestStatus, Operation, Struct,
};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind};

/// RowDeltaAction is a transaction action for atomically committing both
/// data files and delete files in a single snapshot.
///
/// This action is used for row-level operations like UPDATE and MERGE where
/// we need to atomically:
/// 1. Mark existing rows as deleted (via position or equality delete files)
/// 2. Add new/updated rows (via data files)
///
/// # Example
///
/// ```ignore
/// use iceberg::transaction::{ApplyTransactionAction, Transaction};
///
/// let tx = Transaction::new(&table);
/// let action = tx.row_delta()
///     .add_data_files(new_data_files)
///     .add_position_delete_files(delete_files);
/// let tx = action.apply(tx)?;
/// let table = tx.commit(&catalog).await?;
/// ```
pub struct RowDeltaAction {
    check_duplicate: bool,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,

    // Files to commit atomically
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,

    // Conflict detection configuration
    validate_from_snapshot_id: Option<i64>,
    validate_no_conflicting_data: bool,
    validate_no_conflicting_deletes: bool,
}

impl RowDeltaAction {
    pub(crate) fn new() -> Self {
        Self {
            check_duplicate: true,
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            added_data_files: vec![],
            added_delete_files: vec![],
            validate_from_snapshot_id: None,
            validate_no_conflicting_data: false,
            validate_no_conflicting_deletes: false,
        }
    }

    /// Set whether to check for duplicate files.
    pub fn with_check_duplicate(mut self, v: bool) -> Self {
        self.check_duplicate = v;
        self
    }

    /// Add data files (new/updated rows) to the snapshot.
    ///
    /// These files contain the new row data. For UPDATE operations,
    /// these represent the updated rows with their new values.
    pub fn add_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(data_files);
        self
    }

    /// Add a single data file to the snapshot.
    pub fn add_data_file(mut self, data_file: DataFile) -> Self {
        self.added_data_files.push(data_file);
        self
    }

    /// Add position delete files to the snapshot.
    ///
    /// Position delete files identify rows to delete by file path and row position.
    /// These are typically used in copy-on-write UPDATE operations.
    pub fn add_position_delete_files(
        mut self,
        delete_files: impl IntoIterator<Item = DataFile>,
    ) -> Self {
        self.added_delete_files.extend(delete_files);
        self
    }

    /// Add a single position delete file to the snapshot.
    pub fn add_position_delete_file(mut self, delete_file: DataFile) -> Self {
        self.added_delete_files.push(delete_file);
        self
    }

    /// Add equality delete files to the snapshot.
    ///
    /// Equality delete files identify rows to delete by matching column values.
    /// The files must have `equality_ids` set to specify which columns to match.
    pub fn add_equality_delete_files(
        mut self,
        delete_files: impl IntoIterator<Item = DataFile>,
    ) -> Self {
        self.added_delete_files.extend(delete_files);
        self
    }

    /// Add a single equality delete file to the snapshot.
    pub fn add_equality_delete_file(mut self, delete_file: DataFile) -> Self {
        self.added_delete_files.push(delete_file);
        self
    }

    /// Set the baseline snapshot ID for conflict detection.
    ///
    /// When set, the action will validate that no conflicting files have been
    /// added to the table since this snapshot. This enables optimistic concurrency
    /// control for row-level operations.
    pub fn validate_from_snapshot(mut self, snapshot_id: i64) -> Self {
        self.validate_from_snapshot_id = Some(snapshot_id);
        self
    }

    /// Enable validation that no conflicting data files have been added.
    ///
    /// When enabled, the commit will fail if any data files have been added
    /// to affected partitions since the baseline snapshot.
    pub fn validate_no_conflicting_data_files(mut self) -> Self {
        self.validate_no_conflicting_data = true;
        self
    }

    /// Enable validation that no conflicting delete files have been added.
    ///
    /// When enabled, the commit will fail if any delete files have been added
    /// to affected partitions since the baseline snapshot.
    pub fn validate_no_conflicting_delete_files(mut self) -> Self {
        self.validate_no_conflicting_deletes = true;
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
    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }

    /// Collect the set of partitions affected by this operation.
    ///
    /// This includes partitions from both data files and delete files being added.
    fn collect_affected_partitions(&self) -> HashSet<Struct> {
        let mut partitions = HashSet::new();

        for file in &self.added_data_files {
            partitions.insert(file.partition().clone());
        }

        for file in &self.added_delete_files {
            partitions.insert(file.partition().clone());
        }

        partitions
    }

    /// Validate that no conflicting files have been added since the baseline snapshot.
    ///
    /// This method walks the snapshot history from the current snapshot back to the
    /// baseline snapshot, checking for any files added to affected partitions.
    async fn validate_no_conflicts(&self, table: &Table) -> Result<()> {
        // If no baseline is set, skip conflict detection
        let Some(baseline_id) = self.validate_from_snapshot_id else {
            return Ok(());
        };

        // If neither validation flag is enabled, skip
        if !self.validate_no_conflicting_data && !self.validate_no_conflicting_deletes {
            return Ok(());
        }

        let affected_partitions = self.collect_affected_partitions();
        if affected_partitions.is_empty() {
            return Ok(());
        }

        // Walk snapshot history from current back to baseline
        let mut current_snapshot_id = table.metadata().current_snapshot_id();

        while let Some(snap_id) = current_snapshot_id {
            // Stop when we reach the baseline
            if snap_id == baseline_id {
                break;
            }

            let snapshot = table.metadata().snapshot_by_id(snap_id).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Snapshot {snap_id} not found in metadata during conflict detection"),
                )
            })?;

            // Load the manifest list for this snapshot
            let manifest_list = snapshot
                .load_manifest_list(table.file_io(), &table.metadata_ref())
                .await?;

            // Check each manifest for conflicting files
            for manifest in manifest_list.entries() {
                // Skip manifests with no added files
                if !manifest.has_added_files() {
                    continue;
                }

                // Determine if we should check this manifest based on content type
                let should_check = match manifest.content {
                    ManifestContentType::Data => self.validate_no_conflicting_data,
                    ManifestContentType::Deletes => self.validate_no_conflicting_deletes,
                };

                if !should_check {
                    continue;
                }

                // Load manifest entries and check for conflicts
                let loaded_manifest = manifest.load_manifest(table.file_io()).await?;

                for entry in loaded_manifest.entries() {
                    // Only check added files
                    if entry.status() != ManifestStatus::Added {
                        continue;
                    }

                    // Check if this file's partition is in our affected set
                    if affected_partitions.contains(entry.data_file().partition()) {
                        let conflict_type = match manifest.content {
                            ManifestContentType::Data => "data file",
                            ManifestContentType::Deletes => "delete file",
                        };

                        return Err(Error::new(
                            ErrorKind::CatalogCommitConflicts,
                            format!(
                                "Conflicting {} added in snapshot {} for partition {:?}",
                                conflict_type,
                                snap_id,
                                entry.data_file().partition()
                            ),
                        )
                        .with_context(
                            "conflicting_file",
                            entry.data_file().file_path().to_string(),
                        )
                        .with_retryable(true));
                    }
                }
            }

            // Move to parent snapshot
            current_snapshot_id = snapshot.parent_snapshot_id();
        }

        Ok(())
    }
}

#[async_trait]
impl TransactionAction for RowDeltaAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        // If no files to commit, return no-op
        if self.added_data_files.is_empty() && self.added_delete_files.is_empty() {
            return Ok(ActionCommit::new(vec![], vec![]));
        }

        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
            self.added_delete_files.clone(),
        );

        // Validate added files
        if !self.added_data_files.is_empty() {
            snapshot_producer.validate_added_data_files()?;
        }
        if !self.added_delete_files.is_empty() {
            snapshot_producer.validate_added_delete_files()?;
        }

        // Check for duplicate files
        if self.check_duplicate {
            snapshot_producer.validate_duplicate_files().await?;
        }

        // Validate no conflicting files have been added since baseline
        self.validate_no_conflicts(table).await?;

        let operation = RowDeltaOperation::new(
            !self.added_data_files.is_empty(),
            !self.added_delete_files.is_empty(),
        );

        snapshot_producer
            .commit(operation, DefaultManifestProcess)
            .await
    }
}

/// Operation implementation for RowDelta snapshots.
///
struct RowDeltaOperation {
    has_data_files: bool,
    has_delete_files: bool,
}

impl RowDeltaOperation {
    fn new(has_data_files: bool, has_delete_files: bool) -> Self {
        Self {
            has_data_files,
            has_delete_files,
        }
    }
}

impl SnapshotProduceOperation for RowDeltaOperation {
    fn operation(&self) -> Operation {
        match (self.has_data_files, self.has_delete_files) {
            (true, false) => Operation::Append,
            (false, true) => Operation::Delete,
            (true, true) => Operation::Overwrite,
            (false, false) => Operation::Append,
        }
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        // RowDelta is additive - no entries to mark as deleted
        Ok(vec![])
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        // Include all existing manifests (both data and delete)
        // RowDelta adds new manifests alongside existing ones
        let Some(snapshot) = snapshot_produce.current_snapshot()? else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.table.file_io(),
                &snapshot_produce.table.metadata_ref(),
            )
            .await?;

        Ok(manifest_list.entries().to_vec())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, MAIN_BRANCH, Operation, Struct,
    };
    use crate::transaction::tests::{make_v1_table, make_v2_minimal_table};
    use crate::transaction::{Transaction, TransactionAction};
    use crate::{TableRequirement, TableUpdate};

    #[tokio::test]
    async fn test_row_delta_empty_files_noop() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.row_delta();

        // Empty row delta should succeed with no-op
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        assert!(action_commit.take_updates().is_empty());
    }

    #[tokio::test]
    async fn test_row_delta_data_only() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let action = tx.row_delta().add_data_files(vec![data_file]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        assert!(matches!(
            (&updates[0], &updates[1]),
            (
                TableUpdate::AddSnapshot { snapshot },
                TableUpdate::SetSnapshotRef { reference, ref_name }
            ) if snapshot.snapshot_id() == reference.snapshot_id && ref_name == MAIN_BRANCH
        ));
    }

    #[tokio::test]
    async fn test_row_delta_delete_only() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let delete_file = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/delete-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let action = tx.row_delta().add_position_delete_files(vec![delete_file]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let snapshot = match &updates[0] {
            TableUpdate::AddSnapshot { snapshot } => snapshot,
            _ => panic!("Expected AddSnapshot update"),
        };
        assert_eq!(snapshot.summary().operation, Operation::Delete);

        assert_eq!(updates.len(), 2);
        assert!(matches!(updates[0], TableUpdate::AddSnapshot { .. }));
    }

    #[tokio::test]
    async fn test_row_delta_data_and_delete_files() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let delete_file = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/delete-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(50)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let action = tx
            .row_delta()
            .add_data_files(vec![data_file])
            .add_position_delete_files(vec![delete_file]);

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();

        let snapshot = match &updates[0] {
            TableUpdate::AddSnapshot { snapshot } => snapshot,
            _ => panic!("Expected AddSnapshot update"),
        };
        assert_eq!(snapshot.summary().operation, Operation::Overwrite);

        // Check that we got a snapshot update
        assert!(matches!(
            (&updates[0], &updates[1]),
            (
                TableUpdate::AddSnapshot { snapshot },
                TableUpdate::SetSnapshotRef { reference, ref_name }
            ) if snapshot.snapshot_id() == reference.snapshot_id && ref_name == MAIN_BRANCH
        ));

        // Check requirements
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
    async fn test_row_delta_snapshot_operation_is_append() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let action = tx.row_delta().add_data_files(vec![data_file]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            panic!("Expected AddSnapshot update");
        };

        assert_eq!(snapshot.summary().operation, Operation::Append);
    }

    #[tokio::test]
    async fn test_row_delta_reject_v1_table() {
        let table = make_v1_table();
        let tx = Transaction::new(&table);

        let delete_file = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/delete-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::empty())
            .build()
            .unwrap();

        let action = tx.row_delta().add_position_delete_files(vec![delete_file]);
        let result = Arc::new(action).commit(&table).await;

        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(
            err.message()
                .contains("Delete files are not supported in format version 1")
        );
    }

    #[tokio::test]
    async fn test_row_delta_reject_data_content_in_delete_files() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        // Try to add a data file as a delete file
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let action = tx.row_delta().add_position_delete_files(vec![data_file]);
        let result = Arc::new(action).commit(&table).await;

        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(
            err.message()
                .contains("Data content type is not allowed for delete files")
        );
    }

    #[tokio::test]
    async fn test_row_delta_equality_delete_requires_equality_ids() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        // Equality delete without equality_ids
        let equality_delete = DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path("test/eq-delete-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            // NOT setting equality_ids
            .build()
            .unwrap();

        let action = tx
            .row_delta()
            .add_equality_delete_files(vec![equality_delete]);
        let result = Arc::new(action).commit(&table).await;

        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(
            err.message()
                .contains("Equality delete files must have equality_ids set")
        );
    }

    #[tokio::test]
    async fn test_row_delta_with_equality_deletes() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let equality_delete = DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path("test/eq-delete-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .equality_ids(Some(vec![1, 2]))
            .build()
            .unwrap();

        let action = tx
            .row_delta()
            .add_equality_delete_files(vec![equality_delete]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        assert_eq!(updates.len(), 2);
        assert!(matches!(updates[0], TableUpdate::AddSnapshot { .. }));
    }

    #[tokio::test]
    async fn test_row_delta_set_snapshot_properties() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let mut properties = HashMap::new();
        properties.insert("custom.key".to_string(), "custom.value".to_string());

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let action = tx
            .row_delta()
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
                .get("custom.key")
                .unwrap(),
            "custom.value"
        );
    }

    #[tokio::test]
    async fn test_row_delta_manifest_content_types() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let delete_file = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/delete-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(50)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let action = tx
            .row_delta()
            .add_data_files(vec![data_file])
            .add_position_delete_files(vec![delete_file]);

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            panic!("Expected AddSnapshot update");
        };

        // Load manifest list and verify we have both data and delete manifests
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        // Should have two manifests - one for data, one for deletes
        assert_eq!(manifest_list.entries().len(), 2);

        let has_data_manifest = manifest_list
            .entries()
            .iter()
            .any(|m| m.content == crate::spec::ManifestContentType::Data);
        let has_delete_manifest = manifest_list
            .entries()
            .iter()
            .any(|m| m.content == crate::spec::ManifestContentType::Deletes);

        assert!(has_data_manifest, "Should have a data manifest");
        assert!(has_delete_manifest, "Should have a delete manifest");
    }

    #[tokio::test]
    async fn test_row_delta_sequence_numbers_consistent() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let delete_file = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/delete-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(50)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let action = tx
            .row_delta()
            .add_data_files(vec![data_file])
            .add_position_delete_files(vec![delete_file]);

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            panic!("Expected AddSnapshot update");
        };

        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        // All manifests should have the same sequence number
        let seq_num = snapshot.sequence_number();
        for manifest in manifest_list.entries() {
            assert_eq!(
                manifest.sequence_number, seq_num,
                "All manifests should have the same sequence number"
            );
        }
    }

    #[tokio::test]
    async fn test_row_delta_intra_batch_duplicate_data_files() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file1 = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data-same.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let data_file2 = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data-same.parquet".to_string()) // Same path!
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let action = tx.row_delta().add_data_files(vec![data_file1, data_file2]);
        let result = Arc::new(action).commit(&table).await;

        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(
            err.message()
                .contains("Cannot add duplicate data files in the same batch")
        );
    }

    #[tokio::test]
    async fn test_row_delta_intra_batch_duplicate_delete_files() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let delete_file1 = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/delete-same.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let delete_file2 = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/delete-same.parquet".to_string()) // Same path!
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let action = tx
            .row_delta()
            .add_position_delete_files(vec![delete_file1, delete_file2]);
        let result = Arc::new(action).commit(&table).await;

        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(
            err.message()
                .contains("Cannot add duplicate delete files in the same batch")
        );
    }

    #[tokio::test]
    async fn test_row_delta_unknown_partition_spec_id() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        // Use a partition spec ID that doesn't match the table default
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(999) // Non-existent spec ID
            .partition(Struct::empty())
            .build()
            .unwrap();

        let action = tx.row_delta().add_data_files(vec![data_file]);
        let result = Arc::new(action).commit(&table).await;

        assert!(result.is_err());
        let err = result.err().unwrap();
        // Data file validation checks that partition spec exists in table metadata
        assert!(
            err.message()
                .contains("Data file references unknown partition spec"),
            "Expected error about unknown partition spec, got: {}",
            err.message()
        );
    }

    #[tokio::test]
    async fn test_row_delta_no_conflict_without_baseline() {
        // Without a baseline snapshot, conflict detection should be skipped
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        // Enable conflict detection flags but don't set baseline
        let action = tx
            .row_delta()
            .add_data_files(vec![data_file])
            .validate_no_conflicting_data_files()
            .validate_no_conflicting_delete_files();

        // Should succeed because no baseline is set
        let result = Arc::new(action).commit(&table).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_row_delta_no_conflict_at_baseline() {
        // When current snapshot equals baseline, no conflict should be detected
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        // Use a non-existent baseline (table has no snapshots)
        // Since current_snapshot_id is None, the while loop won't execute
        let action = tx
            .row_delta()
            .add_data_files(vec![data_file])
            .validate_from_snapshot(12345) // Non-existent snapshot
            .validate_no_conflicting_data_files();

        // Should succeed because there's no current snapshot to walk from
        let result = Arc::new(action).commit(&table).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_row_delta_conflict_detection_flags_disabled() {
        // With baseline set but flags disabled, conflict detection should be skipped
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/data-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        // Set baseline but don't enable validation flags
        let action = tx
            .row_delta()
            .add_data_files(vec![data_file])
            .validate_from_snapshot(12345);
        // NOT calling validate_no_conflicting_data_files() or validate_no_conflicting_delete_files()

        // Should succeed because validation flags are not enabled
        let result = Arc::new(action).commit(&table).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_collect_affected_partitions() {
        // Test that affected partitions are collected correctly
        let partition1 = Struct::from_iter([Some(Literal::long(100))]);
        let partition2 = Struct::from_iter([Some(Literal::long(200))]);
        let partition3 = Struct::from_iter([Some(Literal::long(100))]); // Same as partition1

        let action = super::RowDeltaAction::new()
            .add_data_file(
                DataFileBuilder::default()
                    .content(DataContentType::Data)
                    .file_path("test/data-1.parquet".to_string())
                    .file_format(DataFileFormat::Parquet)
                    .file_size_in_bytes(100)
                    .record_count(1)
                    .partition_spec_id(0)
                    .partition(partition1.clone())
                    .build()
                    .unwrap(),
            )
            .add_position_delete_file(
                DataFileBuilder::default()
                    .content(DataContentType::PositionDeletes)
                    .file_path("test/delete-1.parquet".to_string())
                    .file_format(DataFileFormat::Parquet)
                    .file_size_in_bytes(50)
                    .record_count(1)
                    .partition_spec_id(0)
                    .partition(partition2.clone())
                    .build()
                    .unwrap(),
            )
            .add_data_file(
                DataFileBuilder::default()
                    .content(DataContentType::Data)
                    .file_path("test/data-2.parquet".to_string())
                    .file_format(DataFileFormat::Parquet)
                    .file_size_in_bytes(100)
                    .record_count(1)
                    .partition_spec_id(0)
                    .partition(partition3) // Same partition as partition1
                    .build()
                    .unwrap(),
            );

        let affected = action.collect_affected_partitions();

        // Should have 2 unique partitions (partition1 == partition3)
        assert_eq!(affected.len(), 2);
        assert!(affected.contains(&partition1));
        assert!(affected.contains(&partition2));
    }
}
