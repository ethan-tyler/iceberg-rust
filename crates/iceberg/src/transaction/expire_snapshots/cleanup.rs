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

//! File cleanup for snapshot expiration.
//!
//! This module handles the identification and deletion of files that become
//! unreferenced after snapshots are expired. It works by traversing manifests
//! (NOT by scanning storage).
//!
//! # What Gets Deleted
//!
//! 1. **Manifest list files** - From expired snapshots
//! 2. **Manifest files** - No longer referenced by any retained snapshot
//! 3. **Data/Delete files** - Referenced only by expired snapshots (not in retained snapshots)
//!
//! # Safety
//!
//! Files are only deleted if they are not referenced by any retained snapshot.
//! This is determined by building a "live file set" from retained snapshots.

use std::collections::HashSet;

use super::result::{ExpireProgressCallback, ExpireProgressEvent};
use crate::Result;
use crate::io::FileIO;
use crate::spec::{DataContentType, TableMetadata};

/// Files identified for cleanup after snapshot expiration.
#[derive(Debug, Clone, Default)]
pub struct CleanupPlan {
    /// Manifest list file paths to delete.
    pub manifest_list_files: Vec<String>,
    /// Manifest file paths to delete.
    pub manifest_files: Vec<String>,
    /// Data file paths to delete.
    pub data_files: Vec<String>,
    /// Position delete file paths to delete.
    pub position_delete_files: Vec<String>,
    /// Equality delete file paths to delete.
    pub equality_delete_files: Vec<String>,
    /// Total bytes that will be freed (estimated).
    pub total_bytes: u64,
}

impl CleanupPlan {
    /// Returns true if there are no files to clean up.
    pub fn is_empty(&self) -> bool {
        self.manifest_list_files.is_empty()
            && self.manifest_files.is_empty()
            && self.data_files.is_empty()
            && self.position_delete_files.is_empty()
            && self.equality_delete_files.is_empty()
    }

    /// Total number of files to delete.
    pub fn total_files(&self) -> usize {
        self.manifest_list_files.len()
            + self.manifest_files.len()
            + self.data_files.len()
            + self.position_delete_files.len()
            + self.equality_delete_files.len()
    }
}

/// Statistics about cleanup execution.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CleanupExecutionResult {
    /// Number of data files successfully deleted.
    pub deleted_data_files: u64,
    /// Number of position delete files successfully deleted.
    pub deleted_position_delete_files: u64,
    /// Number of equality delete files successfully deleted.
    pub deleted_equality_delete_files: u64,
    /// Number of manifest files successfully deleted.
    pub deleted_manifest_files: u64,
    /// Number of manifest list files successfully deleted.
    pub deleted_manifest_list_files: u64,
}

impl CleanupExecutionResult {
    /// Total number of files successfully deleted.
    pub fn total_deleted_files(&self) -> u64 {
        self.deleted_data_files
            + self.deleted_position_delete_files
            + self.deleted_equality_delete_files
            + self.deleted_manifest_files
            + self.deleted_manifest_list_files
    }
}

/// Compute which files can be safely deleted after expiring snapshots.
///
/// # Algorithm
///
/// 1. Build "live file set" from retained snapshots (manifests + data/delete files)
/// 2. For each expired snapshot:
///    - Add manifest list to delete list
///    - For each manifest not in live set, add to delete list
///    - For each file referenced by expired snapshots but not in live set, add to delete list
///
/// # Arguments
///
/// * `file_io` - FileIO for reading manifest files
/// * `metadata` - Table metadata
/// * `expired_snapshot_ids` - IDs of snapshots being expired
/// * `retained_snapshot_ids` - IDs of snapshots that will be retained
/// * `progress_callback` - Optional progress callback
///
/// # Returns
///
/// A `CleanupPlan` containing all files that can be safely deleted.
pub async fn compute_cleanup_plan(
    file_io: &FileIO,
    metadata: &TableMetadata,
    expired_snapshot_ids: &[i64],
    retained_snapshot_ids: &HashSet<i64>,
    progress_callback: Option<&ExpireProgressCallback>,
) -> Result<CleanupPlan> {
    if expired_snapshot_ids.is_empty() {
        return Ok(CleanupPlan::default());
    }

    // Step 1: Build live file set from retained snapshots
    // This includes both data files and delete files (position/equality)
    let live_manifests = build_live_manifest_set(file_io, metadata, retained_snapshot_ids).await?;
    let live_files = build_live_file_set(file_io, metadata, retained_snapshot_ids).await?;

    // Use sets to avoid duplicates and keep byte estimates correct.
    let mut manifest_list_files = HashSet::new();
    let mut manifest_files = HashSet::new();
    let mut data_files = HashSet::new();
    let mut position_delete_files = HashSet::new();
    let mut equality_delete_files = HashSet::new();
    let mut scanned_manifests = HashSet::new();
    let mut total_bytes = 0u64;

    // Step 2: Analyze expired snapshots
    for (idx, &snapshot_id) in expired_snapshot_ids.iter().enumerate() {
        if let Some(callback) = progress_callback {
            callback(ExpireProgressEvent::AnalyzingSnapshot {
                index: idx,
                snapshot_id,
            });
        }

        let Some(snapshot) = metadata.snapshot_by_id(snapshot_id) else {
            continue;
        };

        // Add manifest list from expired snapshot
        manifest_list_files.insert(snapshot.manifest_list().to_string());

        // Load and analyze manifests
        let manifest_list = snapshot.load_manifest_list(file_io, metadata).await?;

        for manifest_file in manifest_list.entries() {
            let manifest_path = &manifest_file.manifest_path;

            // If manifest is not in live set, it can be deleted.
            if !live_manifests.contains(manifest_path)
                && manifest_files.insert(manifest_path.clone())
                && manifest_file.manifest_length > 0
            {
                total_bytes = total_bytes.saturating_add(manifest_file.manifest_length as u64);
            }

            if !scanned_manifests.insert(manifest_path.clone()) {
                continue;
            }

            // Collect data and delete files referenced by expired snapshots.
            let manifest = manifest_file.load_manifest(file_io).await?;
            for entry in manifest.entries() {
                let file_path = entry.file_path();
                if live_files.contains(file_path) {
                    continue;
                }

                let inserted = match entry.content_type() {
                    DataContentType::Data => data_files.insert(file_path.to_string()),
                    DataContentType::PositionDeletes => {
                        position_delete_files.insert(file_path.to_string())
                    }
                    DataContentType::EqualityDeletes => {
                        equality_delete_files.insert(file_path.to_string())
                    }
                };
                if inserted {
                    total_bytes = total_bytes.saturating_add(entry.file_size_in_bytes());
                }
            }
        }
    }

    let mut plan = CleanupPlan {
        manifest_list_files: manifest_list_files.into_iter().collect(),
        manifest_files: manifest_files.into_iter().collect(),
        data_files: data_files.into_iter().collect(),
        position_delete_files: position_delete_files.into_iter().collect(),
        equality_delete_files: equality_delete_files.into_iter().collect(),
        total_bytes,
    };
    plan.manifest_list_files.sort();
    plan.manifest_files.sort();
    plan.data_files.sort();
    plan.position_delete_files.sort();
    plan.equality_delete_files.sort();

    if let Some(callback) = progress_callback {
        callback(ExpireProgressEvent::FilesIdentified {
            data_files: plan.data_files.len(),
            position_delete_files: plan.position_delete_files.len(),
            equality_delete_files: plan.equality_delete_files.len(),
            manifest_files: plan.manifest_files.len(),
            manifest_list_files: plan.manifest_list_files.len(),
        });
    }

    Ok(plan)
}

/// Build set of manifest paths referenced by retained snapshots.
async fn build_live_manifest_set(
    file_io: &FileIO,
    metadata: &TableMetadata,
    retained_snapshot_ids: &HashSet<i64>,
) -> Result<HashSet<String>> {
    let mut live_manifests = HashSet::new();

    for &snapshot_id in retained_snapshot_ids {
        let Some(snapshot) = metadata.snapshot_by_id(snapshot_id) else {
            continue;
        };

        let manifest_list = snapshot.load_manifest_list(file_io, metadata).await?;
        for manifest_file in manifest_list.entries() {
            live_manifests.insert(manifest_file.manifest_path.clone());
        }
    }

    Ok(live_manifests)
}

/// Build set of file paths that are "alive" in retained snapshots.
///
/// A file is "alive" if it appears with status ADDED or EXISTING
/// in any manifest of any retained snapshot. This includes both
/// data files and delete files (position and equality).
async fn build_live_file_set(
    file_io: &FileIO,
    metadata: &TableMetadata,
    retained_snapshot_ids: &HashSet<i64>,
) -> Result<HashSet<String>> {
    let mut live_files = HashSet::new();

    for &snapshot_id in retained_snapshot_ids {
        let Some(snapshot) = metadata.snapshot_by_id(snapshot_id) else {
            continue;
        };

        let manifest_list = snapshot.load_manifest_list(file_io, metadata).await?;
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(file_io).await?;
            for entry in manifest.entries() {
                // Files that are ADDED or EXISTING are live (includes data and delete files)
                if entry.is_alive() {
                    live_files.insert(entry.file_path().to_string());
                }
            }
        }
    }

    Ok(live_files)
}

/// Execute the cleanup plan by deleting files.
///
/// # Arguments
///
/// * `file_io` - FileIO for deleting files
/// * `plan` - The cleanup plan to execute
/// * `progress_callback` - Optional progress callback
///
/// # Returns
///
/// Statistics about which files were successfully deleted.
pub async fn execute_cleanup_plan(
    file_io: &FileIO,
    plan: &CleanupPlan,
    progress_callback: Option<&ExpireProgressCallback>,
) -> Result<CleanupExecutionResult> {
    let total_files = plan.total_files();
    let mut result = CleanupExecutionResult::default();
    let mut current = 0usize;

    // Delete in order: data files, delete files, manifests, manifest lists
    // (manifest lists should be deleted last as they reference manifests)

    for file_path in &plan.data_files {
        current += 1;
        if let Some(callback) = progress_callback {
            callback(ExpireProgressEvent::DeletingFiles {
                current,
                total: total_files,
            });
        }
        // Ignore errors for individual file deletions (file might already be gone)
        if file_io.delete(file_path).await.is_ok() {
            result.deleted_data_files += 1;
        }
    }

    for file_path in &plan.position_delete_files {
        current += 1;
        if let Some(callback) = progress_callback {
            callback(ExpireProgressEvent::DeletingFiles {
                current,
                total: total_files,
            });
        }
        if file_io.delete(file_path).await.is_ok() {
            result.deleted_position_delete_files += 1;
        }
    }

    for file_path in &plan.equality_delete_files {
        current += 1;
        if let Some(callback) = progress_callback {
            callback(ExpireProgressEvent::DeletingFiles {
                current,
                total: total_files,
            });
        }
        if file_io.delete(file_path).await.is_ok() {
            result.deleted_equality_delete_files += 1;
        }
    }

    for file_path in &plan.manifest_files {
        current += 1;
        if let Some(callback) = progress_callback {
            callback(ExpireProgressEvent::DeletingFiles {
                current,
                total: total_files,
            });
        }
        if file_io.delete(file_path).await.is_ok() {
            result.deleted_manifest_files += 1;
        }
    }

    for file_path in &plan.manifest_list_files {
        current += 1;
        if let Some(callback) = progress_callback {
            callback(ExpireProgressEvent::DeletingFiles {
                current,
                total: total_files,
            });
        }
        if file_io.delete(file_path).await.is_ok() {
            result.deleted_manifest_list_files += 1;
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cleanup_plan_empty() {
        let plan = CleanupPlan::default();
        assert!(plan.is_empty());
        assert_eq!(plan.total_files(), 0);
    }

    #[test]
    fn test_cleanup_plan_total_files() {
        let plan = CleanupPlan {
            manifest_list_files: vec!["a".to_string()],
            manifest_files: vec!["b".to_string(), "c".to_string()],
            data_files: vec!["d".to_string(), "e".to_string(), "f".to_string()],
            position_delete_files: vec!["pd1".to_string()],
            equality_delete_files: vec!["ed1".to_string(), "ed2".to_string()],
            total_bytes: 1000,
        };
        assert!(!plan.is_empty());
        assert_eq!(plan.total_files(), 9); // 1 + 2 + 3 + 1 + 2 = 9
    }

    #[test]
    fn test_cleanup_plan_with_delete_files() {
        let plan = CleanupPlan {
            manifest_list_files: vec![],
            manifest_files: vec![],
            data_files: vec![],
            position_delete_files: vec!["pd1".to_string()],
            equality_delete_files: vec!["ed1".to_string()],
            total_bytes: 0,
        };
        assert!(!plan.is_empty());
        assert_eq!(plan.total_files(), 2);
    }

    #[test]
    fn test_cleanup_plan_deduplication() {
        let mut plan = CleanupPlan {
            manifest_list_files: vec!["a".to_string(), "a".to_string()],
            manifest_files: vec!["b".to_string(), "b".to_string(), "c".to_string()],
            data_files: vec!["d".to_string()],
            position_delete_files: vec![],
            equality_delete_files: vec![],
            total_bytes: 0,
        };

        // Simulate deduplication
        plan.manifest_list_files.sort();
        plan.manifest_list_files.dedup();
        plan.manifest_files.sort();
        plan.manifest_files.dedup();

        assert_eq!(plan.manifest_list_files.len(), 1);
        assert_eq!(plan.manifest_files.len(), 2);
    }

    #[test]
    fn test_cleanup_execution_result_total() {
        let result = CleanupExecutionResult {
            deleted_data_files: 5,
            deleted_position_delete_files: 2,
            deleted_equality_delete_files: 1,
            deleted_manifest_files: 3,
            deleted_manifest_list_files: 1,
        };
        assert_eq!(result.total_deleted_files(), 12);
    }
}
