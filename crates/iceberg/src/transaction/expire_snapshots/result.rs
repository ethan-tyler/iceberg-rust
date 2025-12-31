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

//! Result types for snapshot expiration.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Progress event emitted during expire snapshots operation.
#[derive(Debug, Clone)]
pub enum ExpireProgressEvent {
    /// Analyzing a snapshot for files to delete.
    AnalyzingSnapshot {
        /// Index of the snapshot being analyzed (0-based).
        index: usize,
        /// ID of the snapshot being analyzed.
        snapshot_id: i64,
    },
    /// Files have been identified for deletion.
    FilesIdentified {
        /// Number of data files to delete.
        data_files: usize,
        /// Number of position delete files to delete.
        position_delete_files: usize,
        /// Number of equality delete files to delete.
        equality_delete_files: usize,
        /// Number of manifest files to delete.
        manifest_files: usize,
        /// Number of manifest list files to delete.
        manifest_list_files: usize,
    },
    /// Deleting files.
    DeletingFiles {
        /// Current file being deleted (1-based).
        current: usize,
        /// Total number of files to delete.
        total: usize,
    },
}

/// Callback for progress events during expire snapshots.
pub type ExpireProgressCallback = Arc<dyn Fn(ExpireProgressEvent) + Send + Sync>;

/// Result of an expire_snapshots operation.
///
/// This struct contains statistics about what was deleted during expiration.
/// In dry-run mode, it shows what would be deleted without making changes.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExpireSnapshotsResult {
    /// Number of snapshots expired (removed from metadata).
    pub deleted_snapshots_count: u64,

    /// Number of refs removed (branches/tags that expired).
    pub deleted_refs_count: u64,

    /// Number of data files deleted.
    pub deleted_data_files_count: u64,

    /// Number of position delete files deleted.
    pub deleted_position_delete_files_count: u64,

    /// Number of equality delete files deleted.
    pub deleted_equality_delete_files_count: u64,

    /// Number of manifest files deleted.
    pub deleted_manifest_files_count: u64,

    /// Number of manifest list files deleted.
    pub deleted_manifest_list_files_count: u64,

    /// Total bytes freed by file deletion (estimated).
    pub total_bytes_freed: u64,

    /// Execution duration in milliseconds.
    pub duration_ms: u64,
}

impl ExpireSnapshotsResult {
    /// Create an empty result (no snapshots expired).
    pub fn empty(duration: Duration) -> Self {
        Self {
            duration_ms: duration.as_millis() as u64,
            ..Default::default()
        }
    }

    /// Create a result for metadata-only expiration (no file deletion).
    pub fn metadata_only(
        deleted_snapshots_count: u64,
        deleted_refs_count: u64,
        duration: Duration,
    ) -> Self {
        Self {
            deleted_snapshots_count,
            deleted_refs_count,
            duration_ms: duration.as_millis() as u64,
            ..Default::default()
        }
    }

    /// Total number of files deleted (data + delete + manifest + manifest list).
    pub fn total_files_deleted(&self) -> u64 {
        self.deleted_data_files_count
            + self.deleted_position_delete_files_count
            + self.deleted_equality_delete_files_count
            + self.deleted_manifest_files_count
            + self.deleted_manifest_list_files_count
    }

    /// Returns true if the operation made any changes.
    pub fn has_changes(&self) -> bool {
        self.deleted_snapshots_count > 0 || self.deleted_refs_count > 0
    }

    /// Returns true if any files were deleted.
    pub fn has_deleted_files(&self) -> bool {
        self.total_files_deleted() > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expire_snapshots_result_default() {
        let result = ExpireSnapshotsResult::default();

        assert_eq!(result.deleted_snapshots_count, 0);
        assert_eq!(result.deleted_refs_count, 0);
        assert_eq!(result.total_files_deleted(), 0);
        assert!(!result.has_changes());
        assert!(!result.has_deleted_files());
    }

    #[test]
    fn test_expire_snapshots_result_empty() {
        let result = ExpireSnapshotsResult::empty(Duration::from_secs(5));

        assert_eq!(result.deleted_snapshots_count, 0);
        assert_eq!(result.duration_ms, 5000);
        assert!(!result.has_changes());
    }

    #[test]
    fn test_expire_snapshots_result_metadata_only() {
        let result = ExpireSnapshotsResult::metadata_only(5, 2, Duration::from_millis(1500));

        assert_eq!(result.deleted_snapshots_count, 5);
        assert_eq!(result.deleted_refs_count, 2);
        assert_eq!(result.duration_ms, 1500);
        assert!(result.has_changes());
        assert!(!result.has_deleted_files());
    }

    #[test]
    fn test_expire_snapshots_result_serialization() {
        let result = ExpireSnapshotsResult::metadata_only(2, 1, Duration::from_secs(1));

        let json = serde_json::to_string(&result).unwrap();
        let parsed: ExpireSnapshotsResult = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed, result);
    }
}
