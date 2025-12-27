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

//! Incremental scan API for CDC and change detection.
//!
//! This module provides an API for computing the differences between two snapshots,
//! enabling Change Data Capture (CDC) use cases. The incremental scan returns the
//! files that were added and removed between snapshots.
//!
//! # Example
//!
//! ```rust,ignore
//! use iceberg::scan::IncrementalScanBuilder;
//!
//! // Get changes between two snapshots
//! let changes = table.incremental_scan(from_snapshot_id, to_snapshot_id)
//!     .build()?
//!     .changes()
//!     .await?;
//!
//! // Process added data files
//! for data_file in changes.added_data_files() {
//!     println!("Added: {}", data_file.file_path());
//! }
//!
//! // Process removed data files
//! for data_file in changes.removed_data_files() {
//!     println!("Removed: {}", data_file.file_path());
//! }
//! ```
//!
//! # Supported Commit Types
//!
//! The incremental scan correctly handles different commit types:
//!
//! - **Append commits**: Returns only added data files
//! - **Row-delta commits**: Returns added data files and delete files
//! - **Rewrite/compaction commits**: Returns both removed and added files

use std::collections::HashSet;
use std::sync::Arc;

use crate::io::object_cache::ObjectCache;
use crate::spec::{
    DataContentType, DataFile, ManifestContentType, ManifestStatus, SnapshotUtil, TableMetadataRef,
};
use crate::table::Table;
use crate::{Error, ErrorKind, Result};

/// Builder for creating an incremental scan between two snapshots.
///
/// An incremental scan computes the difference between two snapshots, returning
/// the files that were added and removed. This is useful for CDC use cases.
pub struct IncrementalScanBuilder<'a> {
    table: &'a Table,
    from_snapshot_id: i64,
    to_snapshot_id: i64,
}

impl<'a> IncrementalScanBuilder<'a> {
    /// Create a new incremental scan builder.
    ///
    /// # Arguments
    ///
    /// * `table` - The table to scan
    /// * `from_snapshot_id` - The starting snapshot (exclusive)
    /// * `to_snapshot_id` - The ending snapshot (inclusive)
    pub fn new(table: &'a Table, from_snapshot_id: i64, to_snapshot_id: i64) -> Self {
        Self {
            table,
            from_snapshot_id,
            to_snapshot_id,
        }
    }

    /// Build the incremental scan.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `from_snapshot_id` is not an ancestor of `to_snapshot_id`
    /// - Either snapshot does not exist in the table metadata
    pub fn build(self) -> Result<IncrementalScan> {
        let metadata = self.table.metadata_ref();

        // Validate that both snapshots exist
        if metadata.snapshot_by_id(self.from_snapshot_id).is_none() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "From snapshot {} not found in table metadata",
                    self.from_snapshot_id
                ),
            ));
        }

        if metadata.snapshot_by_id(self.to_snapshot_id).is_none() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "To snapshot {} not found in table metadata",
                    self.to_snapshot_id
                ),
            ));
        }

        // Validate ancestry: from_snapshot must be an ancestor of to_snapshot
        if self.from_snapshot_id != self.to_snapshot_id
            && !SnapshotUtil::is_ancestor_of(&metadata, self.to_snapshot_id, self.from_snapshot_id)
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Snapshot {} is not an ancestor of snapshot {}",
                    self.from_snapshot_id, self.to_snapshot_id
                ),
            ));
        }

        Ok(IncrementalScan {
            object_cache: self.table.object_cache(),
            metadata,
            from_snapshot_id: self.from_snapshot_id,
            to_snapshot_id: self.to_snapshot_id,
        })
    }
}

/// An incremental scan that computes the difference between two snapshots.
#[derive(Debug)]
pub struct IncrementalScan {
    object_cache: Arc<ObjectCache>,
    metadata: TableMetadataRef,
    from_snapshot_id: i64,
    to_snapshot_id: i64,
}

impl IncrementalScan {
    /// Compute the changes between the two snapshots.
    ///
    /// This method walks the snapshot ancestry from `to_snapshot_id` back to
    /// `from_snapshot_id` and collects all files that were added or removed
    /// in each intermediate snapshot.
    ///
    /// # Returns
    ///
    /// Returns an `IncrementalChanges` struct containing:
    /// - Added data files
    /// - Removed data files
    /// - Added delete files
    /// - Removed delete files
    pub async fn changes(&self) -> Result<IncrementalChanges> {
        // If same snapshot, return empty changes
        if self.from_snapshot_id == self.to_snapshot_id {
            return Ok(IncrementalChanges::empty());
        }

        // Get all snapshot IDs between from and to (exclusive of from, inclusive of to)
        let snapshot_ids =
            SnapshotUtil::ancestor_ids_between(&self.metadata, self.from_snapshot_id, self.to_snapshot_id);

        if snapshot_ids.is_empty() {
            return Ok(IncrementalChanges::empty());
        }

        let mut added_data_files = Vec::new();
        let mut removed_data_files = Vec::new();
        let mut added_delete_files = Vec::new();
        let mut removed_delete_files = Vec::new();

        // Process each snapshot in the range.
        let snapshot_ids_set: HashSet<i64> = snapshot_ids.iter().copied().collect();
        let mut seen_manifest_paths = HashSet::new();

        for snapshot_id in &snapshot_ids {
            let snapshot = self
                .metadata
                .snapshot_by_id(*snapshot_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Snapshot {} not found", snapshot_id),
                    )
                })?;

            // Load the manifest list for this snapshot.
            let manifest_list = self
                .object_cache
                .get_manifest_list(snapshot, &self.metadata)
                .await?;

            // Only scan manifests added in this snapshot to avoid duplicate processing.
            for manifest_file in manifest_list.entries() {
                if manifest_file.added_snapshot_id != *snapshot_id {
                    continue;
                }

                if !manifest_file.has_added_files() && !manifest_file.has_deleted_files() {
                    continue;
                }

                if !seen_manifest_paths.insert(manifest_file.manifest_path.clone()) {
                    continue;
                }

                let manifest = self.object_cache.get_manifest(manifest_file).await?;

                for entry in manifest.entries() {
                    // Only process entries that were changed in the snapshot range.
                    let entry_snapshot_id =
                        entry.snapshot_id.unwrap_or(manifest_file.added_snapshot_id);
                    if !snapshot_ids_set.contains(&entry_snapshot_id) {
                        continue;
                    }

                    let data_file = entry.data_file().clone();
                    let is_data_file = entry.content_type() == DataContentType::Data;
                    let content_type = manifest_file.content;

                    match entry.status {
                        ManifestStatus::Added => {
                            if is_data_file && content_type == ManifestContentType::Data {
                                added_data_files.push(data_file);
                            } else if content_type == ManifestContentType::Deletes {
                                added_delete_files.push(data_file);
                            }
                        }
                        ManifestStatus::Deleted => {
                            if is_data_file && content_type == ManifestContentType::Data {
                                removed_data_files.push(data_file);
                            } else if content_type == ManifestContentType::Deletes {
                                removed_delete_files.push(data_file);
                            }
                        }
                        ManifestStatus::Existing => {
                            // Existing entries are not changes.
                        }
                    }
                }
            }
        }

        Ok(IncrementalChanges {
            added_data_files,
            removed_data_files,
            added_delete_files,
            removed_delete_files,
        })
    }
}

/// The result of an incremental scan, containing all file changes between snapshots.
///
/// This struct represents the complete set of changes between two snapshots,
/// categorized by whether files were added or removed, and by file type (data or delete).
#[derive(Debug, Clone, Default)]
pub struct IncrementalChanges {
    added_data_files: Vec<DataFile>,
    removed_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
    removed_delete_files: Vec<DataFile>,
}

impl IncrementalChanges {
    /// Create an empty IncrementalChanges.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Get the data files that were added in the snapshot range.
    pub fn added_data_files(&self) -> &[DataFile] {
        &self.added_data_files
    }

    /// Get the data files that were removed in the snapshot range.
    pub fn removed_data_files(&self) -> &[DataFile] {
        &self.removed_data_files
    }

    /// Get the delete files that were added in the snapshot range.
    pub fn added_delete_files(&self) -> &[DataFile] {
        &self.added_delete_files
    }

    /// Get the delete files that were removed in the snapshot range.
    pub fn removed_delete_files(&self) -> &[DataFile] {
        &self.removed_delete_files
    }

    /// Get the total number of added files (data + delete).
    pub fn added_files_count(&self) -> usize {
        self.added_data_files.len() + self.added_delete_files.len()
    }

    /// Get the total number of removed files (data + delete).
    pub fn removed_files_count(&self) -> usize {
        self.removed_data_files.len() + self.removed_delete_files.len()
    }

    /// Check if there are any changes.
    pub fn is_empty(&self) -> bool {
        self.added_data_files.is_empty()
            && self.removed_data_files.is_empty()
            && self.added_delete_files.is_empty()
            && self.removed_delete_files.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::fs;

    use super::*;
    use crate::io::{FileIO, FileIOBuilder};
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion, ManifestEntry,
        ManifestFile, ManifestListWriter, ManifestStatus, ManifestWriterBuilder, NestedField,
        Operation, PartitionSpec, PrimitiveType, Schema, Snapshot, Struct, Summary, TableMetadata,
        Type,
    };
    use tempfile::TempDir;

    #[test]
    fn test_incremental_changes_empty() {
        let changes = IncrementalChanges::empty();
        assert!(changes.is_empty());
        assert_eq!(changes.added_files_count(), 0);
        assert_eq!(changes.removed_files_count(), 0);
    }

    #[test]
    fn test_incremental_changes_with_files() {
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_format(DataFileFormat::Parquet)
            .file_path("data/file1.parquet".to_string())
            .file_size_in_bytes(1024)
            .record_count(100)
            .partition_spec_id(0)
            .partition(Struct::empty())
            .build()
            .unwrap();

        let changes = IncrementalChanges {
            added_data_files: vec![data_file.clone()],
            removed_data_files: vec![],
            added_delete_files: vec![],
            removed_delete_files: vec![],
        };

        assert!(!changes.is_empty());
        assert_eq!(changes.added_files_count(), 1);
        assert_eq!(changes.removed_files_count(), 0);
        assert_eq!(changes.added_data_files().len(), 1);
        assert_eq!(
            changes.added_data_files()[0].file_path(),
            "data/file1.parquet"
        );
    }

    /// Create test table metadata with a chain of snapshots: s1 -> s2 -> s3
    fn create_test_metadata_with_chain() -> TableMetadata {
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

        TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: uuid::Uuid::new_v4(),
            location: "s3://bucket/table".to_string(),
            last_sequence_number: 3,
            last_updated_ms: 3000,
            last_column_id: 1,
            current_schema_id: 0,
            schemas: HashMap::from([(0, Arc::new(schema.clone()))]),
            partition_specs: HashMap::new(),
            default_spec: Arc::new(PartitionSpec::unpartition_spec()),
            default_partition_type: crate::spec::StructType::new(vec![]),
            last_partition_id: -1,
            properties: HashMap::new(),
            current_snapshot_id: Some(3),
            snapshots: HashMap::from([
                (1, Arc::new(s1)),
                (2, Arc::new(s2)),
                (3, Arc::new(s3)),
            ]),
            snapshot_log: vec![],
            sort_orders: HashMap::new(),
            metadata_log: vec![],
            default_sort_order_id: 0,
            refs: HashMap::new(),
            statistics: HashMap::new(),
            partition_statistics: HashMap::new(),
            encryption_keys: HashMap::new(),
            next_row_id: 0,
        }
    }

    fn create_test_table() -> Table {
        let metadata = create_test_metadata_with_chain();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();

        Table::builder()
            .file_io(file_io)
            .metadata(Arc::new(metadata))
            .identifier(crate::TableIdent::new(
                crate::NamespaceIdent::from_strs(&["test"]).unwrap(),
                "test_table".to_string(),
            ))
            .build()
            .unwrap()
    }

    #[test]
    fn test_incremental_scan_builder_validates_from_snapshot_exists() {
        let table = create_test_table();

        let result = table.incremental_scan(999, 3).build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("999"));
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_incremental_scan_builder_validates_to_snapshot_exists() {
        let table = create_test_table();

        let result = table.incremental_scan(1, 999).build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("999"));
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_incremental_scan_builder_validates_ancestry() {
        let table = create_test_table();

        // s1 is not an ancestor of s1 when from=s3 to=s1 (reversed order)
        let result = table.incremental_scan(3, 1).build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("not an ancestor"));
    }

    #[test]
    fn test_incremental_scan_builder_same_snapshot() {
        let table = create_test_table();

        // Same snapshot should be valid
        let result = table.incremental_scan(2, 2).build();
        assert!(result.is_ok());
    }

    #[test]
    fn test_incremental_scan_builder_valid_range() {
        let table = create_test_table();

        // Valid range: s1 is ancestor of s3
        let result = table.incremental_scan(1, 3).build();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_incremental_scan_changes_by_commit_type() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().join("table");
        let metadata_dir = table_location.join("metadata");
        fs::create_dir_all(&metadata_dir).unwrap();

        let file_io = FileIO::from_path(table_location.to_str().unwrap())
            .unwrap()
            .build()
            .unwrap();

        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                ))])
                .build()
                .unwrap(),
        );
        let partition_spec = PartitionSpec::unpartition_spec();

        let data_file1 = build_data_file(
            &format!("{}/data/a1.parquet", table_location.to_str().unwrap()),
            DataContentType::Data,
        );
        let data_file2 = build_data_file(
            &format!("{}/data/a2.parquet", table_location.to_str().unwrap()),
            DataContentType::Data,
        );
        let data_file3 = build_data_file(
            &format!("{}/data/a3.parquet", table_location.to_str().unwrap()),
            DataContentType::Data,
        );
        let delete_file1 = build_data_file(
            &format!("{}/data/d1.parquet", table_location.to_str().unwrap()),
            DataContentType::PositionDeletes,
        );

        // Snapshot 1: empty manifest list (base)
        let manifest_list1_path = metadata_dir.join("manifest-list-1.avro");
        write_manifest_list(
            &file_io,
            manifest_list1_path.to_str().unwrap(),
            1,
            None,
            1,
            vec![],
        )
        .await;

        // Snapshot 2: append commit (adds data_file1)
        let manifest2_path = metadata_dir.join("manifest-2.avro");
        let mut writer2 = ManifestWriterBuilder::new(
            file_io.new_output(manifest2_path.to_str().unwrap()).unwrap(),
            Some(2),
            None,
            schema.clone(),
            partition_spec.clone(),
        )
        .build_v2_data();
        writer2
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(data_file1.clone())
                    .build(),
            )
            .unwrap();
        let manifest2 = assign_manifest_sequence(writer2.write_manifest_file().await.unwrap(), 2);

        let manifest_list2_path = metadata_dir.join("manifest-list-2.avro");
        write_manifest_list(
            &file_io,
            manifest_list2_path.to_str().unwrap(),
            2,
            Some(1),
            2,
            vec![manifest2.clone()],
        )
        .await;

        // Snapshot 3: row-delta commit (adds data_file2 + delete_file1)
        let manifest3_data_path = metadata_dir.join("manifest-3-data.avro");
        let mut writer3_data = ManifestWriterBuilder::new(
            file_io
                .new_output(manifest3_data_path.to_str().unwrap())
                .unwrap(),
            Some(3),
            None,
            schema.clone(),
            partition_spec.clone(),
        )
        .build_v2_data();
        writer3_data
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(data_file2.clone())
                    .build(),
            )
            .unwrap();
        let manifest3_data =
            assign_manifest_sequence(writer3_data.write_manifest_file().await.unwrap(), 3);

        let manifest3_delete_path = metadata_dir.join("manifest-3-delete.avro");
        let mut writer3_delete = ManifestWriterBuilder::new(
            file_io
                .new_output(manifest3_delete_path.to_str().unwrap())
                .unwrap(),
            Some(3),
            None,
            schema.clone(),
            partition_spec.clone(),
        )
        .build_v2_deletes();
        writer3_delete
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(delete_file1.clone())
                    .build(),
            )
            .unwrap();
        let manifest3_delete =
            assign_manifest_sequence(writer3_delete.write_manifest_file().await.unwrap(), 3);

        let manifest_list3_path = metadata_dir.join("manifest-list-3.avro");
        write_manifest_list(
            &file_io,
            manifest_list3_path.to_str().unwrap(),
            3,
            Some(2),
            3,
            vec![manifest2.clone(), manifest3_data.clone(), manifest3_delete.clone()],
        )
        .await;

        // Snapshot 4: rewrite/compaction (adds data_file3, removes data_file1)
        let manifest4_path = metadata_dir.join("manifest-4.avro");
        let mut writer4 = ManifestWriterBuilder::new(
            file_io.new_output(manifest4_path.to_str().unwrap()).unwrap(),
            Some(4),
            None,
            schema.clone(),
            partition_spec.clone(),
        )
        .build_v2_data();
        writer4
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(data_file3.clone())
                    .build(),
            )
            .unwrap();
        writer4
            .add_delete_file(data_file1.clone(), 2, Some(2))
            .unwrap();
        let manifest4 = assign_manifest_sequence(writer4.write_manifest_file().await.unwrap(), 4);

        let manifest_list4_path = metadata_dir.join("manifest-list-4.avro");
        write_manifest_list(
            &file_io,
            manifest_list4_path.to_str().unwrap(),
            4,
            Some(3),
            4,
            vec![
                manifest2.clone(),
                manifest3_data.clone(),
                manifest3_delete.clone(),
                manifest4.clone(),
            ],
        )
        .await;

        let s1 = Snapshot::builder()
            .with_snapshot_id(1)
            .with_sequence_number(1)
            .with_timestamp_ms(1000)
            .with_manifest_list(manifest_list1_path.to_str().unwrap())
            .with_schema_id(0)
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
            .with_manifest_list(manifest_list2_path.to_str().unwrap())
            .with_schema_id(0)
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
            .with_manifest_list(manifest_list3_path.to_str().unwrap())
            .with_schema_id(0)
            .with_summary(Summary {
                operation: Operation::RowDelta,
                additional_properties: HashMap::new(),
            })
            .build();

        let s4 = Snapshot::builder()
            .with_snapshot_id(4)
            .with_parent_snapshot_id(Some(3))
            .with_sequence_number(4)
            .with_timestamp_ms(4000)
            .with_manifest_list(manifest_list4_path.to_str().unwrap())
            .with_schema_id(0)
            .with_summary(Summary {
                operation: Operation::Replace,
                additional_properties: HashMap::new(),
            })
            .build();

        let metadata = TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: uuid::Uuid::new_v4(),
            location: table_location.to_str().unwrap().to_string(),
            last_sequence_number: 4,
            last_updated_ms: 4000,
            last_column_id: 1,
            current_schema_id: 0,
            schemas: HashMap::from([(0, schema.clone())]),
            partition_specs: HashMap::from([(0, Arc::new(partition_spec.clone()))]),
            default_spec: Arc::new(partition_spec.clone()),
            default_partition_type: crate::spec::StructType::new(vec![]),
            last_partition_id: -1,
            properties: HashMap::new(),
            current_snapshot_id: Some(4),
            snapshots: HashMap::from([
                (1, Arc::new(s1)),
                (2, Arc::new(s2)),
                (3, Arc::new(s3)),
                (4, Arc::new(s4)),
            ]),
            snapshot_log: vec![],
            sort_orders: HashMap::new(),
            metadata_log: vec![],
            default_sort_order_id: 0,
            refs: HashMap::new(),
            statistics: HashMap::new(),
            partition_statistics: HashMap::new(),
            encryption_keys: HashMap::new(),
            next_row_id: 0,
        };

        let table = Table::builder()
            .file_io(file_io)
            .metadata(Arc::new(metadata))
            .identifier(crate::TableIdent::new(
                crate::NamespaceIdent::from_strs(&["test"]).unwrap(),
                "test_table".to_string(),
            ))
            .build()
            .unwrap();

        let append_changes = table
            .incremental_scan(1, 2)
            .build()
            .unwrap()
            .changes()
            .await
            .unwrap();
        assert_eq!(
            file_paths(append_changes.added_data_files()),
            HashSet::from([data_file1.file_path().to_string()])
        );
        assert!(append_changes.removed_data_files().is_empty());
        assert!(append_changes.added_delete_files().is_empty());
        assert!(append_changes.removed_delete_files().is_empty());

        let row_delta_changes = table
            .incremental_scan(2, 3)
            .build()
            .unwrap()
            .changes()
            .await
            .unwrap();
        assert_eq!(
            file_paths(row_delta_changes.added_data_files()),
            HashSet::from([data_file2.file_path().to_string()])
        );
        assert_eq!(
            file_paths(row_delta_changes.added_delete_files()),
            HashSet::from([delete_file1.file_path().to_string()])
        );
        assert!(row_delta_changes.removed_data_files().is_empty());
        assert!(row_delta_changes.removed_delete_files().is_empty());

        let rewrite_changes = table
            .incremental_scan(3, 4)
            .build()
            .unwrap()
            .changes()
            .await
            .unwrap();
        assert_eq!(
            file_paths(rewrite_changes.added_data_files()),
            HashSet::from([data_file3.file_path().to_string()])
        );
        assert_eq!(
            file_paths(rewrite_changes.removed_data_files()),
            HashSet::from([data_file1.file_path().to_string()])
        );
        assert!(rewrite_changes.added_delete_files().is_empty());
        assert!(rewrite_changes.removed_delete_files().is_empty());
    }

    fn build_data_file(path: &str, content: DataContentType) -> DataFile {
        DataFileBuilder::default()
            .content(content)
            .file_format(DataFileFormat::Parquet)
            .file_path(path.to_string())
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::empty())
            .build()
            .unwrap()
    }

    fn assign_manifest_sequence(mut manifest: ManifestFile, sequence_number: i64) -> ManifestFile {
        manifest.sequence_number = sequence_number;
        manifest.min_sequence_number = sequence_number;
        manifest
    }

    async fn write_manifest_list(
        file_io: &FileIO,
        path: &str,
        snapshot_id: i64,
        parent_snapshot_id: Option<i64>,
        sequence_number: i64,
        manifests: Vec<ManifestFile>,
    ) {
        let mut writer = ManifestListWriter::v2(
            file_io.new_output(path).unwrap(),
            snapshot_id,
            parent_snapshot_id,
            sequence_number,
        );

        if !manifests.is_empty() {
            writer.add_manifests(manifests.into_iter()).unwrap();
        }
        writer.close().await.unwrap();
    }

    fn file_paths(files: &[DataFile]) -> HashSet<String> {
        files
            .iter()
            .map(|file| file.file_path().to_string())
            .collect()
    }
}
