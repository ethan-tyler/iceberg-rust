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

//! This module provides `InsertOverwriteAction` for Spark-aligned INSERT OVERWRITE semantics.
//!
//! `InsertOverwriteAction` supports two modes:
//! - **Dynamic**: replace only partitions touched by incoming data (ReplacePartitionsAction)
//! - **Static**: replace partitions matching a filter regardless of incoming data (OverwriteAction)
//!
//! # Example
//!
//! ```ignore
//! use iceberg::expr::Reference;
//! use iceberg::transaction::{Transaction, ApplyTransactionAction};
//!
//! let tx = Transaction::new(&table);
//! // Dynamic overwrite (partitions derived from added files)
//! let action = tx.insert_overwrite()
//!     .add_data_files(new_files);
//! let tx = action.apply(tx)?;
//! let table = tx.commit(&catalog).await?;
//!
//! // Static overwrite (explicit filter)
//! let filter = Reference::new("date").equal_to(Datum::string("2024-01-01"));
//! let action = tx.insert_overwrite()
//!     .static_overwrite(filter)
//!     .add_data_files(new_files);
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::{Error, ErrorKind, Result};
use crate::expr::Predicate;
use crate::spec::DataFile;
use crate::table::Table;
use crate::transaction::{
    ActionCommit, OverwriteAction, ReplacePartitionsAction, TransactionAction,
};

/// INSERT OVERWRITE modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOverwriteMode {
    /// Replace only partitions touched by incoming data.
    Dynamic,
    /// Replace partitions matching the static filter, regardless of incoming data.
    Static,
}

/// Action for Spark-aligned INSERT OVERWRITE semantics.
pub struct InsertOverwriteAction {
    mode: InsertOverwriteMode,
    partition_filter: Option<Predicate>,
    added_data_files: Vec<DataFile>,
    check_duplicate: bool,
    case_sensitive: bool,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    validate_from_snapshot_id: Option<i64>,
    validate_no_conflicting_data: bool,
    validate_no_conflicting_deletes: bool,
    validate_added_files_match_filter: bool,
}

impl InsertOverwriteAction {
    pub(crate) fn new() -> Self {
        Self {
            mode: InsertOverwriteMode::Dynamic,
            partition_filter: None,
            added_data_files: Vec::new(),
            check_duplicate: true,
            case_sensitive: true,
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            validate_from_snapshot_id: None,
            validate_no_conflicting_data: false,
            validate_no_conflicting_deletes: false,
            validate_added_files_match_filter: false,
        }
    }

    /// Switch to dynamic overwrite mode (default).
    pub fn dynamic_overwrite(mut self) -> Self {
        self.mode = InsertOverwriteMode::Dynamic;
        self.partition_filter = None;
        self
    }

    /// Switch to static overwrite mode using a row filter.
    pub fn static_overwrite(mut self, filter: Predicate) -> Self {
        self.mode = InsertOverwriteMode::Static;
        self.partition_filter = Some(filter);
        self
    }

    /// Add a data file to the table.
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

    /// Set whether to use case-sensitive matching when binding the filter.
    ///
    /// This only affects static overwrite mode.
    pub fn with_case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.case_sensitive = case_sensitive;
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
    pub fn validate_from_snapshot(mut self, snapshot_id: i64) -> Self {
        self.validate_from_snapshot_id = Some(snapshot_id);
        self
    }

    /// Enable validation that no conflicting data files were added.
    pub fn validate_no_conflicting_data(mut self) -> Self {
        self.validate_no_conflicting_data = true;
        self
    }

    /// Enable validation that no conflicting delete files were added.
    pub fn validate_no_conflicting_deletes(mut self) -> Self {
        self.validate_no_conflicting_deletes = true;
        self
    }

    /// Enable validation that added files match the overwrite filter.
    ///
    /// This only applies to static overwrite mode.
    pub fn validate_added_files_match_filter(mut self) -> Self {
        self.validate_added_files_match_filter = true;
        self
    }
}

#[async_trait]
impl TransactionAction for InsertOverwriteAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        match self.mode {
            InsertOverwriteMode::Dynamic => {
                let mut action = ReplacePartitionsAction::new()
                    .add_data_files(self.added_data_files.clone())
                    .with_check_duplicate(self.check_duplicate);

                if let Some(commit_uuid) = self.commit_uuid {
                    action = action.set_commit_uuid(commit_uuid);
                }
                if let Some(ref key_metadata) = self.key_metadata {
                    action = action.set_key_metadata(key_metadata.clone());
                }
                if !self.snapshot_properties.is_empty() {
                    action = action.set_snapshot_properties(self.snapshot_properties.clone());
                }
                if let Some(snapshot_id) = self.validate_from_snapshot_id {
                    action = action.validate_from_snapshot(snapshot_id);
                }
                if self.validate_no_conflicting_data {
                    action = action.validate_no_conflicting_data();
                }
                if self.validate_no_conflicting_deletes {
                    action = action.validate_no_conflicting_deletes();
                }

                Arc::new(action).commit(table).await
            }
            InsertOverwriteMode::Static => {
                let filter = self.partition_filter.clone().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Static insert overwrite requires a partition filter",
                    )
                })?;

                let mut action = OverwriteAction::new()
                    .overwrite_filter(filter)
                    .add_data_files(self.added_data_files.clone())
                    .with_check_duplicate(self.check_duplicate)
                    .with_case_sensitive(self.case_sensitive);

                if let Some(commit_uuid) = self.commit_uuid {
                    action = action.set_commit_uuid(commit_uuid);
                }
                if let Some(ref key_metadata) = self.key_metadata {
                    action = action.set_key_metadata(key_metadata.clone());
                }
                if !self.snapshot_properties.is_empty() {
                    action = action.set_snapshot_properties(self.snapshot_properties.clone());
                }
                if let Some(snapshot_id) = self.validate_from_snapshot_id {
                    action = action.validate_from_snapshot(snapshot_id);
                }
                if self.validate_no_conflicting_data {
                    action = action.validate_no_conflicting_data();
                }
                if self.validate_no_conflicting_deletes {
                    action = action.validate_no_conflicting_deletes();
                }
                if self.validate_added_files_match_filter {
                    action = action.validate_added_files_match_filter();
                }

                Arc::new(action).commit(table).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use crate::expr::Reference;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Datum, Literal, ManifestContentType,
        ManifestStatus, Struct,
    };
    use crate::transaction::tests::make_v2_minimal_table;
    use crate::transaction::{Transaction, TransactionAction};

    fn make_data_file(path: &str, partition_value: i64, spec_id: i32) -> crate::spec::DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(spec_id)
            .partition(Struct::from_iter([Some(Literal::long(partition_value))]))
            .build()
            .unwrap()
    }

    async fn collect_manifest_paths(
        table: &crate::table::Table,
    ) -> (HashSet<String>, HashSet<String>) {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), &table.metadata_ref())
            .await
            .unwrap();

        let mut deleted_paths = HashSet::new();
        let mut added_paths = HashSet::new();

        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                let path = entry.data_file().file_path().to_string();
                match entry.status() {
                    ManifestStatus::Deleted => {
                        deleted_paths.insert(path);
                    }
                    _ => {
                        added_paths.insert(path);
                    }
                }
            }
        }

        (deleted_paths, added_paths)
    }

    #[tokio::test]
    async fn test_insert_overwrite_dynamic_replaces_only_touched_partitions() {
        let table = make_v2_minimal_table();
        let spec_id = table.metadata().default_partition_spec_id();

        let file_x_100 = make_data_file("s3://bucket/table/data/x=100/file1.parquet", 100, spec_id);
        let file_x_200 = make_data_file("s3://bucket/table/data/x=200/file2.parquet", 200, spec_id);

        let tx = Transaction::new(&table);
        let append = tx
            .fast_append()
            .add_data_files(vec![file_x_100.clone(), file_x_200.clone()]);
        let mut append_commit = Arc::new(append).commit(&table).await.unwrap();
        let table =
            Transaction::update_table_metadata(table, &append_commit.take_updates()).unwrap();

        let file_x_100_new = make_data_file(
            "s3://bucket/table/data/x=100/file1-new.parquet",
            100,
            spec_id,
        );

        let tx = Transaction::new(&table);
        let overwrite = tx
            .insert_overwrite()
            .add_data_files(vec![file_x_100_new.clone()]);
        let mut overwrite_commit = Arc::new(overwrite).commit(&table).await.unwrap();
        let table =
            Transaction::update_table_metadata(table, &overwrite_commit.take_updates()).unwrap();

        let (deleted_paths, added_paths) = collect_manifest_paths(&table).await;

        assert!(deleted_paths.contains(file_x_100.file_path()));
        assert!(!deleted_paths.contains(file_x_200.file_path()));
        assert!(added_paths.contains(file_x_100_new.file_path()));
        assert!(!added_paths.contains(file_x_100.file_path()));
    }

    #[tokio::test]
    async fn test_insert_overwrite_static_replaces_partition_without_data() {
        let table = make_v2_minimal_table();
        let spec_id = table.metadata().default_partition_spec_id();

        let file_x_100 = make_data_file("s3://bucket/table/data/x=100/file1.parquet", 100, spec_id);
        let file_x_200 = make_data_file("s3://bucket/table/data/x=200/file2.parquet", 200, spec_id);

        let tx = Transaction::new(&table);
        let append = tx
            .fast_append()
            .add_data_files(vec![file_x_100.clone(), file_x_200.clone()]);
        let mut append_commit = Arc::new(append).commit(&table).await.unwrap();
        let table =
            Transaction::update_table_metadata(table, &append_commit.take_updates()).unwrap();

        let filter = Reference::new("x").equal_to(Datum::long(100));
        let tx = Transaction::new(&table);
        let overwrite = tx.insert_overwrite().static_overwrite(filter);
        let mut overwrite_commit = Arc::new(overwrite).commit(&table).await.unwrap();
        let table =
            Transaction::update_table_metadata(table, &overwrite_commit.take_updates()).unwrap();

        let (deleted_paths, _added_paths) = collect_manifest_paths(&table).await;

        assert!(deleted_paths.contains(file_x_100.file_path()));
        assert!(!deleted_paths.contains(file_x_200.file_path()));
    }
}
