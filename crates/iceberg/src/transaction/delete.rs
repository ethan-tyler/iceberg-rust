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

//! This module provides `DeleteAction`.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{DataFile, ManifestFile, Operation};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};

/// DeleteAction is a transaction action for committing delete files to a table.
pub struct DeleteAction {
    check_duplicate: bool,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_delete_files: Vec<DataFile>,
}

impl DeleteAction {
    pub(crate) fn new() -> Self {
        Self {
            check_duplicate: true,
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            added_delete_files: vec![],
        }
    }

    /// Set whether to check duplicate files.
    pub fn with_check_duplicate(mut self, v: bool) -> Self {
        self.check_duplicate = v;
        self
    }

    /// Add delete files to the snapshot.
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
impl TransactionAction for DeleteAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        if self.added_delete_files.is_empty() {
            return Ok(ActionCommit::new(vec![], vec![]));
        }

        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            vec![],
            self.added_delete_files.clone(),
        );

        snapshot_producer.validate_added_delete_files()?;

        if self.check_duplicate {
            snapshot_producer.validate_duplicate_files().await?;
        }

        snapshot_producer
            .commit(DeleteOperation, DefaultManifestProcess)
            .await
    }
}

struct DeleteOperation;

impl SnapshotProduceOperation for DeleteOperation {
    fn operation(&self) -> Operation {
        Operation::Delete
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<crate::spec::ManifestEntry>> {
        Ok(vec![])
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
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
        DataContentType, DataFileBuilder, DataFileFormat, Literal, MAIN_BRANCH, Struct,
    };
    use crate::transaction::tests::{make_v1_table, make_v2_minimal_table};
    use crate::transaction::{Transaction, TransactionAction};
    use crate::{TableRequirement, TableUpdate};

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

    #[tokio::test]
    async fn test_reject_delete_files_for_v1_table() {
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

        let action = tx.delete().add_delete_files(vec![delete_file]);
        let result = Arc::new(action).commit(&table).await;

        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(
            err.message()
                .contains("Delete files are not supported in format version 1")
        );
    }

    #[tokio::test]
    async fn test_empty_delete_files_noop() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.delete().add_delete_files(vec![]);

        // Empty delete files should succeed with no-op
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        // ActionCommit doesn't have a public way to check contents directly,
        // but we can check that it succeeds (no-op returns Ok)
        assert!(action_commit.take_updates().is_empty());
    }

    #[tokio::test]
    async fn test_add_position_delete_files() {
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

        let action = tx.delete().add_delete_files(vec![delete_file]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();

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
    async fn test_reject_data_content_type() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        // Try to add a data file as delete file
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

        let action = tx.delete().add_delete_files(vec![data_file]);
        let result = Arc::new(action).commit(&table).await;

        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(
            err.message()
                .contains("Data content type is not allowed for delete files")
        );
    }

    #[tokio::test]
    async fn test_equality_delete_requires_equality_ids() {
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

        let action = tx.delete().add_delete_files(vec![equality_delete]);
        let result = Arc::new(action).commit(&table).await;

        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(
            err.message()
                .contains("Equality delete files must have equality_ids set")
        );
    }

    #[tokio::test]
    async fn test_reject_puffin_delete_for_v2_table() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let delete_file = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/delete-1.puffin".to_string())
            .file_format(DataFileFormat::Puffin)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .referenced_data_file(Some("test/data-1.parquet".to_string()))
            .content_offset(Some(10))
            .content_size_in_bytes(Some(20))
            .build()
            .unwrap();

        let action = tx.delete().add_delete_files(vec![delete_file]);
        let result = Arc::new(action).commit(&table).await;

        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err
            .message()
            .contains("Deletion vectors are only supported in format version 3"));
    }

    #[tokio::test]
    async fn test_reject_parquet_position_delete_for_v3_table() {
        let table = make_v3_minimal_table();
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

        let action = tx.delete().add_delete_files(vec![delete_file]);
        let result = Arc::new(action).commit(&table).await;

        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err
            .message()
            .contains("Position delete files must use Puffin format in format version 3"));
    }

    #[tokio::test]
    async fn test_reject_puffin_delete_missing_referenced_data_file() {
        let table = make_v3_minimal_table();
        let tx = Transaction::new(&table);

        let delete_file = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/delete-1.puffin".to_string())
            .file_format(DataFileFormat::Puffin)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .content_offset(Some(10))
            .content_size_in_bytes(Some(20))
            .build()
            .unwrap();

        let action = tx.delete().add_delete_files(vec![delete_file]);
        let result = Arc::new(action).commit(&table).await;

        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err
            .message()
            .contains("Deletion vectors require referenced_data_file"));
    }

    #[tokio::test]
    async fn test_reject_puffin_delete_missing_content_offset() {
        let table = make_v3_minimal_table();
        let tx = Transaction::new(&table);

        let delete_file = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/delete-2.puffin".to_string())
            .file_format(DataFileFormat::Puffin)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .referenced_data_file(Some("test/data-1.parquet".to_string()))
            .content_size_in_bytes(Some(20))
            .build()
            .unwrap();

        let action = tx.delete().add_delete_files(vec![delete_file]);
        let result = Arc::new(action).commit(&table).await;

        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.message().contains("Deletion vectors require content_offset"));
    }

    #[tokio::test]
    async fn test_reject_puffin_delete_missing_content_size() {
        let table = make_v3_minimal_table();
        let tx = Transaction::new(&table);

        let delete_file = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/delete-3.puffin".to_string())
            .file_format(DataFileFormat::Puffin)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .referenced_data_file(Some("test/data-1.parquet".to_string()))
            .content_offset(Some(10))
            .build()
            .unwrap();

        let action = tx.delete().add_delete_files(vec![delete_file]);
        let result = Arc::new(action).commit(&table).await;

        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(
            err.message()
                .contains("Deletion vectors require content_size_in_bytes")
        );
    }

    #[tokio::test]
    async fn test_reject_duplicate_deletion_vectors_for_same_data_file() {
        let table = make_v3_minimal_table();
        let tx = Transaction::new(&table);

        let delete_file_1 = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/delete-dup-1.puffin".to_string())
            .file_format(DataFileFormat::Puffin)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .referenced_data_file(Some("test/data-1.parquet".to_string()))
            .content_offset(Some(10))
            .content_size_in_bytes(Some(20))
            .build()
            .unwrap();

        let delete_file_2 = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/delete-dup-2.puffin".to_string())
            .file_format(DataFileFormat::Puffin)
            .file_size_in_bytes(120)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .referenced_data_file(Some("test/data-1.parquet".to_string()))
            .content_offset(Some(30))
            .content_size_in_bytes(Some(40))
            .build()
            .unwrap();

        let action = tx.delete().add_delete_files(vec![delete_file_1, delete_file_2]);
        let result = Arc::new(action).commit(&table).await;

        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.message().contains(
            "Deletion vectors must not target the same referenced_data_file more than once"
        ));
    }

    #[tokio::test]
    async fn test_reject_equality_delete_duplicate_equality_ids() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let equality_delete = DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path("test/eq-delete-dup.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .equality_ids(Some(vec![1, 1]))
            .build()
            .unwrap();

        let action = tx.delete().add_delete_files(vec![equality_delete]);
        let result = Arc::new(action).commit(&table).await;

        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err
            .message()
            .contains("Equality delete files must have unique equality_ids"));
    }

    #[tokio::test]
    async fn test_reject_equality_delete_content_offset() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let equality_delete = DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path("test/eq-delete-offset.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .equality_ids(Some(vec![1]))
            .content_offset(Some(10))
            .build()
            .unwrap();

        let action = tx.delete().add_delete_files(vec![equality_delete]);
        let result = Arc::new(action).commit(&table).await;

        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err
            .message()
            .contains("Equality delete files must not set content_offset or content_size_in_bytes"));
    }

    #[tokio::test]
    async fn test_reject_equality_delete_unknown_equality_id() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let equality_delete = DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path("test/eq-delete-unknown.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .equality_ids(Some(vec![999]))
            .build()
            .unwrap();

        let action = tx.delete().add_delete_files(vec![equality_delete]);
        let result = Arc::new(action).commit(&table).await;

        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err
            .message()
            .contains("Equality delete files reference unknown field id: 999"));
    }

    #[tokio::test]
    async fn test_add_equality_delete_files() {
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
            .equality_ids(Some(vec![1, 2])) // Set equality_ids
            .build()
            .unwrap();

        let action = tx.delete().add_delete_files(vec![equality_delete]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        // Should succeed with snapshot
        assert_eq!(updates.len(), 2);
        assert!(matches!(updates[0], TableUpdate::AddSnapshot { .. }));
    }

    #[tokio::test]
    async fn test_snapshot_operation_is_delete() {
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

        let action = tx.delete().add_delete_files(vec![delete_file]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        // Check snapshot operation is Delete
        let snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            panic!("Expected AddSnapshot update");
        };

        assert_eq!(snapshot.summary().operation, crate::spec::Operation::Delete);
    }

    #[tokio::test]
    async fn test_set_snapshot_properties() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let mut properties = HashMap::new();
        properties.insert("custom.key".to_string(), "custom.value".to_string());

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

        let action = tx
            .delete()
            .add_delete_files(vec![delete_file])
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
    async fn test_multiple_delete_files() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let delete_file1 = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/delete-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let delete_file2 = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/delete-2.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(200)
            .record_count(2)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let action = tx
            .delete()
            .add_delete_files(vec![delete_file1, delete_file2]);

        let result = Arc::new(action).commit(&table).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_mixed_delete_types() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let position_delete = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/pos-delete-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let equality_delete = DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path("test/eq-delete-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .equality_ids(Some(vec![1]))
            .build()
            .unwrap();

        let action = tx
            .delete()
            .add_delete_files(vec![position_delete, equality_delete]);

        let result = Arc::new(action).commit(&table).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_delete_manifest_content_type() {
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

        let action = tx.delete().add_delete_files(vec![delete_file]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            panic!("Expected AddSnapshot update");
        };

        // Load manifest list and check manifest content type
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        // Should have exactly one manifest (delete manifest)
        assert_eq!(manifest_list.entries().len(), 1);

        // Check that it's a delete manifest
        let manifest_entry = &manifest_list.entries()[0];
        assert_eq!(
            manifest_entry.content,
            crate::spec::ManifestContentType::Deletes
        );
    }

    #[tokio::test]
    async fn test_intra_batch_duplicate_delete_files() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        // Add the same delete file twice in a single batch
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
            .delete()
            .add_delete_files(vec![delete_file1, delete_file2]);
        let result = Arc::new(action).commit(&table).await;

        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(
            err.message()
                .contains("Cannot add duplicate delete files in the same batch")
        );
    }

    #[tokio::test]
    async fn test_unknown_partition_spec_id() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        // Use a partition spec ID that doesn't exist in the table
        let delete_file = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/delete-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(999) // Non-existent spec ID
            .partition(Struct::empty())
            .build()
            .unwrap();

        let action = tx.delete().add_delete_files(vec![delete_file]);
        let result = Arc::new(action).commit(&table).await;

        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(
            err.message()
                .contains("Delete file references unknown partition spec id: 999")
        );
    }

    #[tokio::test]
    async fn test_sequence_number_assigned() {
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

        let action = tx.delete().add_delete_files(vec![delete_file]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            panic!("Expected AddSnapshot update");
        };

        // Load manifest list
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        let manifest_entry = &manifest_list.entries()[0];

        // Manifest should have inherited sequence number
        assert_eq!(manifest_entry.sequence_number, snapshot.sequence_number());

        // Load manifest and check entries have correct sequence number
        let manifest = manifest_entry.load_manifest(table.file_io()).await.unwrap();
        assert_eq!(manifest.entries().len(), 1);

        let entry_seq_num = manifest.entries()[0]
            .sequence_number()
            .expect("Sequence number should be inherited when loading");
        assert_eq!(entry_seq_num, snapshot.sequence_number());
    }
}
