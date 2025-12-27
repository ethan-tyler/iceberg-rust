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

//! Manifest rewriting execution logic.
//!
//! This module provides the `ManifestRewriter` struct which handles the actual
//! writing of manifest files during a rewrite operation. It takes batched
//! manifest entries and writes them to new manifest files.

use std::ops::RangeFrom;

use uuid::Uuid;

use super::options::RewriteManifestsOptions;
use super::planner::generate_manifest_path;
use crate::error::Result;
use crate::io::FileIO;
use crate::spec::{
    FormatVersion, ManifestContentType, ManifestEntry, ManifestEntryRef, ManifestFile,
    ManifestWriterBuilder, PartitionSpecRef, SchemaRef,
};
use crate::{Error, ErrorKind};

/// Rewriter for creating new manifests during a rewrite operation.
///
/// The rewriter takes batched manifest entries and writes them to new
/// manifest files, handling path generation and writer configuration.
#[derive(Debug)]
pub struct ManifestRewriter {
    /// UUID for this commit operation
    commit_uuid: Uuid,
    /// Table location for manifest path generation
    table_location: String,
    /// Format version of the table
    format_version: FormatVersion,
    /// Snapshot ID for the new manifests
    snapshot_id: i64,
    /// Counter for generating unique manifest indices
    manifest_counter: RangeFrom<u64>,
    /// Options for the rewrite operation
    #[allow(dead_code)]
    options: RewriteManifestsOptions,
}

impl ManifestRewriter {
    /// Create a new manifest rewriter.
    ///
    /// # Arguments
    ///
    /// * `table_location` - The table's base location
    /// * `format_version` - The table's format version
    /// * `snapshot_id` - The snapshot ID for the new manifests
    /// * `options` - Options for the rewrite operation
    pub fn new(
        table_location: String,
        format_version: FormatVersion,
        snapshot_id: i64,
        options: RewriteManifestsOptions,
    ) -> Self {
        Self {
            commit_uuid: Uuid::new_v4(),
            table_location,
            format_version,
            snapshot_id,
            manifest_counter: (0..),
            options,
        }
    }

    /// Get the commit UUID for this rewriter.
    pub fn commit_uuid(&self) -> &Uuid {
        &self.commit_uuid
    }

    /// Generate the next manifest path.
    fn next_manifest_path(&mut self, content: ManifestContentType) -> String {
        let index = self.manifest_counter.next().unwrap();
        generate_manifest_path(&self.table_location, &self.commit_uuid, index, content)
    }

    /// Write a batch of entries to a new manifest file.
    ///
    /// # Arguments
    ///
    /// * `file_io` - The file I/O interface
    /// * `entries` - The manifest entries to write
    /// * `content` - The manifest content type (Data or Deletes)
    /// * `schema` - The table schema
    /// * `partition_spec` - The partition spec for the entries
    ///
    /// # Returns
    ///
    /// The written manifest file metadata.
    pub async fn write_manifest(
        &mut self,
        file_io: &FileIO,
        entries: Vec<ManifestEntryRef>,
        content: ManifestContentType,
        schema: SchemaRef,
        partition_spec: PartitionSpecRef,
    ) -> Result<ManifestFile> {
        // V1 format does not support delete manifests
        if self.format_version == FormatVersion::V1 && content == ManifestContentType::Deletes {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Delete manifests are not supported in format version 1",
            ));
        }

        let manifest_path = self.next_manifest_path(content);
        let output = file_io.new_output(&manifest_path)?;

        let builder = ManifestWriterBuilder::new(
            output,
            Some(self.snapshot_id),
            None, // key_metadata
            schema,
            partition_spec.as_ref().clone(),
        );

        let mut writer = match self.format_version {
            FormatVersion::V1 => builder.build_v1(),
            FormatVersion::V2 => match content {
                ManifestContentType::Data => builder.build_v2_data(),
                ManifestContentType::Deletes => builder.build_v2_deletes(),
            },
            FormatVersion::V3 => match content {
                ManifestContentType::Data => builder.build_v3_data(),
                ManifestContentType::Deletes => builder.build_v3_deletes(),
            },
        };

        // Add all entries as existing entries (they're being rewritten, not added)
        for entry_ref in entries {
            // Convert Arc<ManifestEntry> to ManifestEntry for the writer
            let entry = ManifestEntry {
                status: entry_ref.status,
                snapshot_id: entry_ref.snapshot_id,
                sequence_number: entry_ref.sequence_number,
                file_sequence_number: entry_ref.file_sequence_number,
                data_file: entry_ref.data_file.clone(),
            };
            writer.add_existing_entry(entry)?;
        }

        writer.write_manifest_file().await
    }

    /// Write multiple batches of entries to new manifest files.
    ///
    /// # Arguments
    ///
    /// * `file_io` - The file I/O interface
    /// * `batches` - The batched manifest entries to write
    /// * `content` - The manifest content type (Data or Deletes)
    /// * `schema` - The table schema
    /// * `partition_spec` - The partition spec for the entries
    ///
    /// # Returns
    ///
    /// A vector of written manifest file metadata.
    pub async fn write_manifests(
        &mut self,
        file_io: &FileIO,
        batches: Vec<Vec<ManifestEntryRef>>,
        content: ManifestContentType,
        schema: SchemaRef,
        partition_spec: PartitionSpecRef,
    ) -> Result<Vec<ManifestFile>> {
        let mut manifests = Vec::with_capacity(batches.len());

        for batch in batches {
            let manifest = self
                .write_manifest(
                    file_io,
                    batch,
                    content,
                    schema.clone(),
                    partition_spec.clone(),
                )
                .await?;
            manifests.push(manifest);
        }

        Ok(manifests)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::{
        DataContentType, DataFile, DataFileFormat, ManifestStatus, NestedField, PartitionSpec,
        PrimitiveType, Schema, Struct, Type,
    };

    /// Create a test schema with a single int field.
    fn test_schema() -> SchemaRef {
        Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::optional(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::optional(
                        2,
                        "date",
                        Type::Primitive(PrimitiveType::Date),
                    )),
                ])
                .build()
                .unwrap(),
        )
    }

    /// Create a test partition spec.
    fn test_partition_spec(schema: SchemaRef) -> PartitionSpecRef {
        Arc::new(
            PartitionSpec::builder(schema)
                .with_spec_id(0)
                .build()
                .unwrap(),
        )
    }

    /// Create a test manifest entry.
    fn test_entry(path: &str, partition: Struct, spec_id: i32) -> ManifestEntryRef {
        Arc::new(ManifestEntry {
            status: ManifestStatus::Existing,
            snapshot_id: Some(1),
            sequence_number: Some(1),
            file_sequence_number: Some(1),
            data_file: DataFile {
                content: DataContentType::Data,
                file_path: path.to_string(),
                file_format: DataFileFormat::Parquet,
                partition,
                record_count: 1000,
                file_size_in_bytes: 10_000,
                column_sizes: HashMap::new(),
                value_counts: HashMap::new(),
                null_value_counts: HashMap::new(),
                nan_value_counts: HashMap::new(),
                lower_bounds: HashMap::new(),
                upper_bounds: HashMap::new(),
                key_metadata: None,
                split_offsets: None,
                equality_ids: None,
                sort_order_id: None,
                partition_spec_id: spec_id,
                first_row_id: None,
                referenced_data_file: None,
                content_offset: None,
                content_size_in_bytes: None,
            },
        })
    }

    #[test]
    fn test_rewriter_creation() {
        let options = RewriteManifestsOptions::default();
        let rewriter = ManifestRewriter::new(
            "s3://bucket/warehouse/db/table".to_string(),
            FormatVersion::V2,
            12345,
            options,
        );

        // Verify the rewriter has a valid commit UUID
        assert!(!rewriter.commit_uuid().is_nil());
    }

    #[test]
    fn test_rewriter_generates_unique_paths() {
        let options = RewriteManifestsOptions::default();
        let mut rewriter = ManifestRewriter::new(
            "s3://bucket/warehouse/db/table".to_string(),
            FormatVersion::V2,
            12345,
            options,
        );

        let path1 = rewriter.next_manifest_path(ManifestContentType::Data);
        let path2 = rewriter.next_manifest_path(ManifestContentType::Data);
        let path3 = rewriter.next_manifest_path(ManifestContentType::Deletes);

        // All paths should be unique
        assert_ne!(path1, path2);
        assert_ne!(path2, path3);

        // All paths should contain the commit UUID
        let uuid_str = rewriter.commit_uuid().to_string();
        assert!(path1.contains(&uuid_str));
        assert!(path2.contains(&uuid_str));
        assert!(path3.contains(&uuid_str));

        // Paths should have incrementing indices
        assert!(path1.contains("-m0"));
        assert!(path2.contains("-m1"));
        assert!(path3.contains("-m2"));
    }

    #[tokio::test]
    async fn test_write_manifest() {
        // Setup temp directory and file IO
        let tmp_dir = TempDir::new().unwrap();
        let io = FileIOBuilder::new_fs_io().build().unwrap();

        let options = RewriteManifestsOptions::default();
        let mut rewriter = ManifestRewriter::new(
            tmp_dir.path().to_str().unwrap().to_string(),
            FormatVersion::V2,
            12345,
            options,
        );

        let schema = test_schema();
        let partition_spec = test_partition_spec(schema.clone());

        let partition = Struct::empty();
        let entries = vec![
            test_entry("file1.parquet", partition.clone(), 0),
            test_entry("file2.parquet", partition.clone(), 0),
        ];

        // Write manifest
        let manifest = rewriter
            .write_manifest(
                &io,
                entries,
                ManifestContentType::Data,
                schema,
                partition_spec,
            )
            .await
            .expect("Should write manifest successfully");

        // Verify manifest metadata
        assert!(
            manifest
                .manifest_path
                .contains(&rewriter.commit_uuid().to_string())
        );
        assert!(manifest.manifest_length > 0);
        assert_eq!(manifest.content, ManifestContentType::Data);
        assert_eq!(manifest.added_snapshot_id, 12345);
        assert_eq!(manifest.existing_files_count, Some(2));
    }

    #[tokio::test]
    async fn test_write_delete_manifest_v1_unsupported() {
        let tmp_dir = TempDir::new().unwrap();
        let io = FileIOBuilder::new_fs_io().build().unwrap();

        let options = RewriteManifestsOptions::default();
        let mut rewriter = ManifestRewriter::new(
            tmp_dir.path().to_str().unwrap().to_string(),
            FormatVersion::V1,
            12345,
            options,
        );

        let schema = test_schema();
        let partition_spec = test_partition_spec(schema.clone());

        let entries = vec![test_entry("file1.parquet", Struct::empty(), 0)];

        let err = rewriter
            .write_manifest(
                &io,
                entries,
                ManifestContentType::Deletes,
                schema,
                partition_spec,
            )
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::FeatureUnsupported);
    }

    #[tokio::test]
    async fn test_write_multiple_manifests() {
        // Setup temp directory and file IO
        let tmp_dir = TempDir::new().unwrap();
        let io = FileIOBuilder::new_fs_io().build().unwrap();

        let options = RewriteManifestsOptions::default();
        let mut rewriter = ManifestRewriter::new(
            tmp_dir.path().to_str().unwrap().to_string(),
            FormatVersion::V2,
            12345,
            options,
        );

        let schema = test_schema();
        let partition_spec = test_partition_spec(schema.clone());

        let partition = Struct::empty();
        let batches = vec![
            vec![
                test_entry("file1.parquet", partition.clone(), 0),
                test_entry("file2.parquet", partition.clone(), 0),
            ],
            vec![
                test_entry("file3.parquet", partition.clone(), 0),
                test_entry("file4.parquet", partition.clone(), 0),
            ],
        ];

        // Write manifests
        let manifests = rewriter
            .write_manifests(
                &io,
                batches,
                ManifestContentType::Data,
                schema,
                partition_spec,
            )
            .await
            .expect("Should write manifests successfully");

        // Should have 2 manifests
        assert_eq!(manifests.len(), 2);

        // Each manifest should have 2 entries
        for manifest in &manifests {
            assert_eq!(manifest.existing_files_count, Some(2));
        }

        // Manifests should have unique paths
        assert_ne!(manifests[0].manifest_path, manifests[1].manifest_path);
    }

    #[tokio::test]
    async fn test_write_manifest_v1_deletes_error() {
        // V1 format does not support delete manifests
        let tmp_dir = TempDir::new().unwrap();
        let io = FileIOBuilder::new_fs_io().build().unwrap();

        let options = RewriteManifestsOptions::default();
        let mut rewriter = ManifestRewriter::new(
            tmp_dir.path().to_str().unwrap().to_string(),
            FormatVersion::V1, // V1 format
            12345,
            options,
        );

        let schema = test_schema();
        let partition_spec = test_partition_spec(schema.clone());

        let partition = Struct::empty();
        let entries = vec![test_entry("file1.parquet", partition, 0)];

        // Should fail with FeatureUnsupported error
        let result = rewriter
            .write_manifest(
                &io,
                entries,
                ManifestContentType::Deletes, // Delete manifest on V1
                schema,
                partition_spec,
            )
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), crate::ErrorKind::FeatureUnsupported);
        assert!(err.message().contains("Delete manifests are not supported"));
    }
}
