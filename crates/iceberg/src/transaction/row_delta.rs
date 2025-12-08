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

//! This module provides `RowDeltaAction` for atomic row-level operations.
//!
//! `RowDeltaAction` enables atomic commits containing both data files and delete files
//! in a single snapshot. This is essential for operations like UPDATE and MERGE that
//! need to atomically:
//! - Add new data files (inserted or updated rows)
//! - Add position delete files (marking original rows as deleted)
//!
//! # Atomicity Guarantee
//!
//! Unlike chaining `FastAppendAction` and `DeleteAction` (which would produce two
//! separate snapshots), `RowDeltaAction` produces a **single snapshot** containing
//! both data and delete manifests. This ensures readers never see a partial state
//! where deletes are visible but new data is not.
//!
//! # Example
//!
//! ```ignore
//! use iceberg::transaction::{Transaction, ApplyTransactionAction};
//!
//! let tx = Transaction::new(&table);
//! let action = tx.row_delta()
//!     .add_data_files(vec![updated_row_file, inserted_row_file])
//!     .add_delete_files(vec![position_delete_file]);
//! let tx = action.apply(tx)?;
//! let table = tx.commit(&catalog).await?;
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{DataFile, ManifestEntry, ManifestFile, Operation};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};

/// RowDeltaAction is a transaction action for atomically committing both data files
/// and delete files in a single snapshot.
///
/// This action is used for row-level operations like UPDATE and MERGE that need to:
/// 1. Add new data files containing updated/inserted rows
/// 2. Add position delete files marking original rows as deleted
///
/// Both file types are committed in a single atomic snapshot, ensuring readers
/// never see a partial state.
pub struct RowDeltaAction {
    check_duplicate: bool,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
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
        }
    }

    /// Set whether to check for duplicate files.
    pub fn with_check_duplicate(mut self, v: bool) -> Self {
        self.check_duplicate = v;
        self
    }

    /// Add data files to the snapshot.
    ///
    /// These files should contain `DataContentType::Data` and represent
    /// new or updated rows.
    pub fn add_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(data_files);
        self
    }

    /// Add delete files to the snapshot.
    ///
    /// These files should contain `DataContentType::PositionDeletes` or
    /// `DataContentType::EqualityDeletes` and mark rows for deletion.
    pub fn add_delete_files(mut self, delete_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_delete_files.extend(delete_files);
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
}

#[async_trait]
impl TransactionAction for RowDeltaAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        // If both are empty, this is a no-op
        if self.added_data_files.is_empty() && self.added_delete_files.is_empty() {
            return Ok(ActionCommit::new(vec![], vec![]));
        }

        // Create snapshot producer with BOTH data files AND delete files
        // This is the key difference from FastAppendAction and DeleteAction
        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
            self.added_delete_files.clone(),
        );

        // Validate data files (content type, partition spec, etc.)
        if !self.added_data_files.is_empty() {
            snapshot_producer.validate_added_data_files()?;
        }

        // Validate delete files (format version, content type, partition spec, etc.)
        if !self.added_delete_files.is_empty() {
            snapshot_producer.validate_added_delete_files()?;
        }

        // Check for duplicate files
        if self.check_duplicate {
            snapshot_producer.validate_duplicate_files().await?;
        }

        // Commit with Overwrite operation since we're modifying existing data
        snapshot_producer
            .commit(RowDeltaOperation, DefaultManifestProcess)
            .await
    }
}

/// Operation type for RowDelta commits.
///
/// Uses `Operation::Overwrite` to indicate that this snapshot modifies
/// existing table data (deletes rows and adds new rows).
struct RowDeltaOperation;

impl SnapshotProduceOperation for RowDeltaOperation {
    fn operation(&self) -> Operation {
        // Use Overwrite because RowDelta modifies existing data:
        // - Deletes mark existing rows as removed
        // - Data files contain replacement/new rows
        Operation::Overwrite
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        // RowDelta doesn't directly delete manifest entries;
        // it adds position delete files that readers will apply
        Ok(vec![])
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        // Include all existing manifests from the current snapshot
        // The new data and delete manifests will be added on top
        let Some(snapshot) = snapshot_produce.table.metadata().current_snapshot() else {
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
        DataContentType, DataFileBuilder, DataFileFormat, Literal, ManifestContentType,
        MAIN_BRANCH, Struct,
    };
    use crate::transaction::tests::make_v2_minimal_table;
    use crate::transaction::{Transaction, TransactionAction};
    use crate::{TableRequirement, TableUpdate};

    #[tokio::test]
    async fn test_row_delta_empty_noop() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.row_delta();

        // Empty row delta should succeed with no-op
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        assert!(action_commit.take_updates().is_empty());
    }

    #[tokio::test]
    async fn test_row_delta_data_files_only() {
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

        let action = tx.row_delta().add_data_files(vec![data_file]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        // Should produce a snapshot
        assert_eq!(updates.len(), 2);
        assert!(matches!(updates[0], TableUpdate::AddSnapshot { .. }));
    }

    #[tokio::test]
    async fn test_row_delta_delete_files_only() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let delete_file = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/delete-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(5)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let action = tx.row_delta().add_delete_files(vec![delete_file]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        // Should produce a snapshot
        assert_eq!(updates.len(), 2);
        assert!(matches!(updates[0], TableUpdate::AddSnapshot { .. }));
    }

    /// Critical test: Verify that RowDelta produces a SINGLE snapshot
    /// with BOTH data and delete manifests.
    #[tokio::test]
    async fn test_row_delta_single_snapshot_with_both_file_types() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        // Create a data file (new/updated rows)
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

        // Create a position delete file (marking original rows as deleted)
        let delete_file = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/delete-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(50)
            .record_count(5)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        // Add both data and delete files in single action
        let action = tx
            .row_delta()
            .add_data_files(vec![data_file])
            .add_delete_files(vec![delete_file]);

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();

        // CRITICAL: Must be exactly ONE AddSnapshot update (not two!)
        let snapshot_count = updates
            .iter()
            .filter(|u| matches!(u, TableUpdate::AddSnapshot { .. }))
            .count();
        assert_eq!(
            snapshot_count, 1,
            "RowDelta must produce exactly ONE snapshot, not multiple"
        );

        // Verify snapshot structure
        assert!(matches!(
            (&updates[0], &updates[1]),
            (
                TableUpdate::AddSnapshot { snapshot },
                TableUpdate::SetSnapshotRef { reference, ref_name }
            ) if snapshot.snapshot_id() == reference.snapshot_id && ref_name == MAIN_BRANCH
        ));

        // Verify requirements
        assert_eq!(requirements.len(), 2);
        assert!(matches!(
            requirements[0],
            TableRequirement::UuidMatch { .. }
        ));
        assert!(matches!(
            requirements[1],
            TableRequirement::RefSnapshotIdMatch { .. }
        ));

        // Load manifest list and verify BOTH manifest types are present
        let snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            panic!("Expected AddSnapshot update");
        };

        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        // Should have exactly 2 manifests: one for data, one for deletes
        assert_eq!(
            manifest_list.entries().len(),
            2,
            "Should have both data and delete manifests"
        );

        let has_data_manifest = manifest_list
            .entries()
            .iter()
            .any(|m| m.content == ManifestContentType::Data);
        let has_delete_manifest = manifest_list
            .entries()
            .iter()
            .any(|m| m.content == ManifestContentType::Deletes);

        assert!(has_data_manifest, "Snapshot must have data manifest");
        assert!(has_delete_manifest, "Snapshot must have delete manifest");
    }

    #[tokio::test]
    async fn test_row_delta_operation_is_overwrite() {
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

        let delete_file = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/delete-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(50)
            .record_count(5)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let action = tx
            .row_delta()
            .add_data_files(vec![data_file])
            .add_delete_files(vec![delete_file]);

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            panic!("Expected AddSnapshot update");
        };

        // Operation should be Overwrite for row-level updates
        assert_eq!(
            snapshot.summary().operation,
            crate::spec::Operation::Overwrite
        );
    }

    #[tokio::test]
    async fn test_row_delta_rejects_v1_table_delete_files() {
        use crate::transaction::tests::make_v1_table;

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

        let action = tx.row_delta().add_delete_files(vec![delete_file]);
        let result = Arc::new(action).commit(&table).await;

        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(
            err.message()
                .contains("Delete files are not supported in format version 1")
        );
    }

    #[tokio::test]
    async fn test_row_delta_rejects_data_content_in_delete_files() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        // Try to add a data file as a delete file (wrong content type)
        let wrong_file = DataFileBuilder::default()
            .content(DataContentType::Data) // Wrong! Should be PositionDeletes
            .file_path("test/wrong-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let action = tx.row_delta().add_delete_files(vec![wrong_file]);
        let result = Arc::new(action).commit(&table).await;

        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(
            err.message()
                .contains("Data content type is not allowed for delete files")
        );
    }

    #[tokio::test]
    async fn test_row_delta_with_snapshot_properties() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let mut properties = HashMap::new();
        properties.insert("merge.source".to_string(), "cdc_stream".to_string());
        properties.insert("merge.batch.id".to_string(), "12345".to_string());

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
                .get("merge.source")
                .unwrap(),
            "cdc_stream"
        );
        assert_eq!(
            snapshot
                .summary()
                .additional_properties
                .get("merge.batch.id")
                .unwrap(),
            "12345"
        );
    }
}
