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

//! Result types for the remove orphan files operation.

use std::sync::Arc;

use chrono::{DateTime, Utc};

/// Progress event emitted during the remove orphan files operation.
///
/// These events allow callers to track progress for large tables where the
/// operation may take significant time.
#[derive(Debug, Clone)]
pub enum OrphanFilesProgressEvent {
    /// Started collecting referenced files from snapshots.
    CollectingReferences {
        /// Number of snapshots to process.
        snapshot_count: usize,
    },
    /// Started listing files in storage.
    ListingFiles {
        /// Location being scanned.
        location: String,
    },
    /// Finished listing files.
    FilesListed {
        /// Total files found in storage.
        total_files: usize,
    },
    /// Identified orphan files eligible for deletion.
    OrphansIdentified {
        /// Number of orphan files found.
        orphan_count: usize,
        /// Total bytes of orphan files.
        total_bytes: u64,
    },
    /// Started deleting orphan files.
    DeletingFiles {
        /// Total files to delete.
        total: usize,
    },
    /// Progress update during deletion.
    DeletionProgress {
        /// Files deleted so far.
        deleted: usize,
        /// Total files to delete.
        total: usize,
    },
    /// Operation complete.
    Complete {
        /// Total files deleted.
        deleted_count: u64,
        /// Total bytes reclaimed.
        bytes_reclaimed: u64,
    },
}

/// Callback function for receiving progress events.
///
/// The callback is invoked synchronously during the operation. Keep the
/// callback fast to avoid slowing down the operation.
///
/// # Example
///
/// ```ignore
/// use iceberg::transaction::remove_orphan_files::OrphanFilesProgressEvent;
///
/// let callback: OrphanFilesProgressCallback = Arc::new(|event| {
///     match event {
///         OrphanFilesProgressEvent::OrphansIdentified { orphan_count, .. } => {
///             println!("Found {} orphan files", orphan_count);
///         }
///         OrphanFilesProgressEvent::DeletionProgress { deleted, total } => {
///             println!("Deleted {}/{} files", deleted, total);
///         }
///         _ => {}
///     }
/// });
/// ```
pub type OrphanFilesProgressCallback =
    Arc<dyn Fn(OrphanFilesProgressEvent) + Send + Sync + 'static>;

/// Result of the remove orphan files operation.
#[derive(Debug, Clone)]
pub struct RemoveOrphanFilesResult {
    /// Orphan files that were identified as eligible for deletion.
    ///
    /// In `dry_run` mode, these are the files that would be deleted.
    pub orphan_files: Vec<OrphanFileInfo>,
    /// Number of data files deleted.
    pub deleted_data_files_count: u64,
    /// Number of delete files deleted (position or equality deletes).
    pub deleted_delete_files_count: u64,
    /// Number of manifest files deleted.
    pub deleted_manifest_files_count: u64,
    /// Number of manifest list files deleted.
    pub deleted_manifest_list_files_count: u64,
    /// Number of metadata files deleted.
    pub deleted_metadata_files_count: u64,
    /// Number of other files deleted.
    pub deleted_other_files_count: u64,
    /// Total bytes reclaimed (estimated from file sizes).
    pub bytes_reclaimed: u64,
    /// Whether this was a dry run (no files actually deleted).
    pub dry_run: bool,
    /// Duration of the operation.
    pub duration: std::time::Duration,
}

impl Default for RemoveOrphanFilesResult {
    fn default() -> Self {
        Self {
            orphan_files: Vec::new(),
            deleted_data_files_count: 0,
            deleted_delete_files_count: 0,
            deleted_manifest_files_count: 0,
            deleted_manifest_list_files_count: 0,
            deleted_metadata_files_count: 0,
            deleted_other_files_count: 0,
            bytes_reclaimed: 0,
            dry_run: false,
            duration: std::time::Duration::ZERO,
        }
    }
}

impl RemoveOrphanFilesResult {
    /// Total number of files deleted (or would be deleted in dry-run).
    pub fn total_deleted_files(&self) -> u64 {
        self.deleted_data_files_count
            + self.deleted_delete_files_count
            + self.deleted_manifest_files_count
            + self.deleted_manifest_list_files_count
            + self.deleted_metadata_files_count
            + self.deleted_other_files_count
    }
}

/// Information about an orphan file.
#[derive(Debug, Clone)]
pub struct OrphanFileInfo {
    /// Full path to the file.
    pub path: String,
    /// Type of file (data, delete, manifest, etc.).
    pub file_type: OrphanFileType,
    /// File size in bytes.
    pub size_bytes: u64,
    /// Last modification timestamp.
    pub last_modified: Option<DateTime<Utc>>,
}

/// Type of orphan file based on file extension and location.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrphanFileType {
    /// Data file (.parquet)
    DataFile,
    /// Position delete file
    PositionDeleteFile,
    /// Equality delete file
    EqualityDeleteFile,
    /// Manifest file (.avro in manifest directory)
    ManifestFile,
    /// Manifest list file (.avro in metadata directory)
    ManifestListFile,
    /// Metadata file (.json/.metadata.json)
    MetadataFile,
    /// Statistics file
    StatisticsFile,
    /// Other/unknown file type
    Other,
}

impl OrphanFileType {
    /// Infer the file type from a path.
    ///
    /// This uses heuristics based on file extension and path components:
    /// - `.parquet` in `/data/` -> DataFile
    /// - `.avro` in `/metadata/` containing `snap-` -> ManifestListFile
    /// - `.avro` otherwise -> ManifestFile
    /// - `.metadata.json` or `.json` in `/metadata/` -> MetadataFile
    /// - `.stats` or in `/statistics/` -> StatisticsFile
    pub fn from_path(path: &str) -> Self {
        let lower_path = path.to_lowercase();

        // Check for metadata files first
        if lower_path.ends_with(".metadata.json")
            || (lower_path.ends_with(".json") && lower_path.contains("/metadata/"))
        {
            return Self::MetadataFile;
        }

        // Check for statistics files
        if lower_path.ends_with(".stats") || lower_path.contains("/statistics/") {
            return Self::StatisticsFile;
        }

        // Check for manifest list files (avro files with snap- prefix in metadata)
        if lower_path.ends_with(".avro") {
            if lower_path.contains("/metadata/") && lower_path.contains("snap-") {
                return Self::ManifestListFile;
            }
            return Self::ManifestFile;
        }

        // Check for parquet files
        if lower_path.ends_with(".parquet") {
            // Delete files typically have "delete" in their path
            if lower_path.contains("-delete-") || lower_path.contains("/deletes/") {
                // Position deletes are more common, default to that
                // In practice, we can't distinguish without reading the file
                return Self::PositionDeleteFile;
            }
            return Self::DataFile;
        }

        Self::Other
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_orphan_file_type_from_path_data_file() {
        assert_eq!(
            OrphanFileType::from_path("s3://bucket/db/table/data/00000-0-abc.parquet"),
            OrphanFileType::DataFile
        );
    }

    #[test]
    fn test_orphan_file_type_from_path_delete_file() {
        assert_eq!(
            OrphanFileType::from_path("s3://bucket/db/table/data/00000-0-abc-delete-00001.parquet"),
            OrphanFileType::PositionDeleteFile
        );
    }

    #[test]
    fn test_orphan_file_type_from_path_manifest_list() {
        assert_eq!(
            OrphanFileType::from_path("s3://bucket/db/table/metadata/snap-1234567890-1-abc.avro"),
            OrphanFileType::ManifestListFile
        );
    }

    #[test]
    fn test_orphan_file_type_from_path_manifest() {
        assert_eq!(
            OrphanFileType::from_path("s3://bucket/db/table/metadata/abc-m0.avro"),
            OrphanFileType::ManifestFile
        );
    }

    #[test]
    fn test_orphan_file_type_from_path_metadata() {
        assert_eq!(
            OrphanFileType::from_path("s3://bucket/db/table/metadata/v1.metadata.json"),
            OrphanFileType::MetadataFile
        );
    }

    #[test]
    fn test_orphan_file_type_from_path_other() {
        assert_eq!(
            OrphanFileType::from_path("s3://bucket/db/table/unknown.txt"),
            OrphanFileType::Other
        );
    }

    #[test]
    fn test_result_total_deleted() {
        let result = RemoveOrphanFilesResult {
            deleted_data_files_count: 10,
            deleted_delete_files_count: 5,
            deleted_manifest_files_count: 2,
            deleted_manifest_list_files_count: 1,
            deleted_metadata_files_count: 1,
            deleted_other_files_count: 3,
            ..Default::default()
        };
        assert_eq!(result.total_deleted_files(), 22);
    }
}
