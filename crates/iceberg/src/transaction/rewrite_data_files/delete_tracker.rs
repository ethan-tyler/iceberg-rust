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

//! Delete file reference tracking for dangling delete cleanup.
//!
//! After compaction, delete files that no longer reference any data files
//! (because all referenced data was compacted) can be removed. This module
//! provides `DeleteTracker` to identify such "dangling" delete files.
//!
//! # Position Deletes vs Equality Deletes
//!
//! - **Position deletes** reference specific data files via the `referenced_data_file`
//!   field (when present) or by partition (when the field is absent).
//! - **Equality deletes** apply to all files in a partition and cannot become
//!   dangling unless the entire partition is removed.
//!
//! # Usage
//!
//! ```ignore
//! let mut tracker = DeleteTracker::new();
//!
//! // Add delete files found during planning
//! for delete_entry in delete_entries {
//!     tracker.add_delete(delete_entry.clone());
//! }
//!
//! // After compaction, find dangling deletes
//! let rewritten_files: HashSet<String> = plan.file_groups
//!     .iter()
//!     .flat_map(|g| g.data_files.iter().map(|e| e.file_path().to_string()))
//!     .collect();
//!
//! let remaining_files: HashSet<String> = get_remaining_data_files(&table);
//!
//! let dangling = tracker.find_dangling_deletes(&rewritten_files, &remaining_files);
//! ```

use std::collections::{HashMap, HashSet};

use crate::spec::{DataContentType, ManifestEntryRef, Struct};
use crate::{Error, ErrorKind, Result};

/// Tracks which data files each delete file references.
///
/// Used to identify dangling delete files after compaction.
#[derive(Debug, Default, Clone)]
pub(crate) struct DeleteTracker {
    /// Position deletes indexed by their referenced data file path.
    /// Only populated when the position delete has `referenced_data_file` set.
    position_deletes_by_ref: HashMap<String, Vec<DeleteFileRef>>,

    /// Position deletes indexed by (partition, spec_id).
    /// Populated when position delete does NOT have `referenced_data_file` set.
    /// These apply to all files in the partition.
    position_deletes_by_partition: HashMap<PartitionKey, Vec<DeleteFileRef>>,

    /// Equality deletes indexed by (partition, spec_id).
    /// Equality deletes always apply to all files in a partition.
    equality_deletes_by_partition: HashMap<PartitionKey, Vec<DeleteFileRef>>,

    /// All delete file entries, keyed by path for lookup.
    all_deletes: HashMap<String, ManifestEntryRef>,

    /// Mapping of partition spec ID to whether that spec is unpartitioned.
    /// Used to correctly derive partition keys for files from evolved tables
    /// with mixed partition specs.
    spec_unpartitioned: HashMap<i32, bool>,
}

/// Key for partition-based lookup: (partition_value, partition_spec_id).
///
/// Note: This tuple alias predates the `partition_filter::PartitionKey` struct.
/// The struct uses `(spec_id, partition)` ordering while this uses `(partition, spec_id)`.
/// Consider unifying to use the struct in a future refactor for consistency.
type PartitionKey = (Option<Struct>, i32);

/// Reference to a delete file with its path.
#[derive(Debug, Clone)]
struct DeleteFileRef {
    /// Path to the delete file.
    path: String,
}

impl DeleteTracker {
    /// Create a new delete tracker with spec unpartitioned mapping.
    ///
    /// # Arguments
    ///
    /// * `spec_unpartitioned` - Mapping of partition spec ID to whether that spec is unpartitioned.
    pub fn new(spec_unpartitioned: HashMap<i32, bool>) -> Self {
        Self {
            spec_unpartitioned,
            ..Default::default()
        }
    }

    /// Add a delete file entry to the tracker.
    ///
    /// Derives `is_unpartitioned` from the entry's partition spec ID to correctly
    /// handle tables with evolved partition specs.
    ///
    /// # Arguments
    ///
    /// * `entry` - A delete file manifest entry
    ///
    /// # Errors
    ///
    /// Returns an error if the entry references an unknown partition spec ID,
    /// which indicates table metadata corruption or inconsistency.
    pub fn add_delete(&mut self, entry: ManifestEntryRef) -> Result<()> {
        let content_type = entry.data_file.content_type();
        let path = entry.data_file.file_path.clone();
        let spec_id = entry.data_file.partition_spec_id;

        // Derive is_unpartitioned from the entry's spec_id, failing if unknown
        let is_unpartitioned = self
            .spec_unpartitioned
            .get(&spec_id)
            .copied()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Delete file '{path}' references unknown partition spec ID {spec_id}. This may indicate table metadata corruption."),
                )
            })?;

        let partition_key = if is_unpartitioned {
            (None, spec_id)
        } else {
            (Some(entry.data_file.partition().clone()), spec_id)
        };

        let delete_ref = DeleteFileRef { path: path.clone() };

        match content_type {
            DataContentType::PositionDeletes => {
                // Check if this position delete references a specific file
                if let Some(ref_path) = entry.data_file.referenced_data_file() {
                    self.position_deletes_by_ref
                        .entry(ref_path)
                        .or_default()
                        .push(delete_ref);
                } else {
                    // No specific file reference - applies to all files in partition
                    self.position_deletes_by_partition
                        .entry(partition_key)
                        .or_default()
                        .push(delete_ref);
                }
            }
            DataContentType::EqualityDeletes => {
                self.equality_deletes_by_partition
                    .entry(partition_key)
                    .or_default()
                    .push(delete_ref);
            }
            DataContentType::Data => {
                // Data files shouldn't be added to delete tracker
            }
        }

        // Store the full entry for later retrieval
        self.all_deletes.insert(path, entry);

        Ok(())
    }

    /// Find position deletes that no longer reference any remaining data files.
    ///
    /// A position delete is "dangling" if:
    /// 1. It has `referenced_data_file` set, AND
    /// 2. That referenced file is in the `rewritten_files` set, AND
    /// 3. That referenced file is NOT in `remaining_files`
    ///
    /// Position deletes without `referenced_data_file` (partition-scoped) are
    /// more complex to track and are NOT marked as dangling by this method.
    ///
    /// # Arguments
    ///
    /// * `rewritten_files` - Set of data file paths that were rewritten (compacted)
    /// * `remaining_files` - Set of data file paths that still exist in the table
    ///
    /// # Returns
    ///
    /// Vector of manifest entries for dangling position delete files.
    pub fn find_dangling_position_deletes(
        &self,
        rewritten_files: &HashSet<String>,
        remaining_files: &HashSet<String>,
    ) -> Vec<ManifestEntryRef> {
        let mut dangling = Vec::new();

        // Check position deletes with specific file references
        for (ref_path, delete_refs) in &self.position_deletes_by_ref {
            // The delete is dangling if:
            // - The referenced file was rewritten (is in the compaction set)
            // - AND the referenced file no longer exists in the table
            if rewritten_files.contains(ref_path) && !remaining_files.contains(ref_path) {
                for delete_ref in delete_refs {
                    if let Some(entry) = self.all_deletes.get(&delete_ref.path) {
                        dangling.push(entry.clone());
                    }
                }
            }
        }

        // Note: Position deletes without referenced_data_file (partition-scoped)
        // are harder to determine as dangling because they apply to ALL files
        // in the partition. They only become dangling when ALL data files in
        // the partition are rewritten. For simplicity, we don't handle these
        // in the initial implementation.

        dangling
    }

    /// Check if there are any tracked delete files.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.all_deletes.is_empty()
    }

    /// Get the count of tracked delete files.
    #[allow(dead_code)]
    pub fn delete_count(&self) -> usize {
        self.all_deletes.len()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::spec::{
        DataContentType, DataFile, DataFileFormat, Literal, ManifestEntry, ManifestStatus, Struct,
    };

    /// Create spec_unpartitioned mapping for a partitioned table (spec_id 1 is partitioned).
    fn partitioned_spec_mapping() -> HashMap<i32, bool> {
        HashMap::from([(1, false)])
    }

    /// Create spec_unpartitioned mapping for an unpartitioned table (spec_id 1 is unpartitioned).
    fn unpartitioned_spec_mapping() -> HashMap<i32, bool> {
        HashMap::from([(1, true)])
    }

    fn test_delete_entry(
        path: &str,
        content_type: DataContentType,
        partition: Struct,
        referenced_file: Option<&str>,
    ) -> ManifestEntryRef {
        Arc::new(ManifestEntry {
            status: ManifestStatus::Added,
            snapshot_id: Some(1),
            sequence_number: Some(1),
            file_sequence_number: Some(1),
            data_file: DataFile {
                content: content_type,
                file_path: path.to_string(),
                file_format: DataFileFormat::Parquet,
                partition,
                record_count: 100,
                file_size_in_bytes: 1000,
                column_sizes: std::collections::HashMap::new(),
                value_counts: std::collections::HashMap::new(),
                null_value_counts: std::collections::HashMap::new(),
                nan_value_counts: std::collections::HashMap::new(),
                lower_bounds: std::collections::HashMap::new(),
                upper_bounds: std::collections::HashMap::new(),
                key_metadata: None,
                split_offsets: None,
                equality_ids: None,
                sort_order_id: None,
                partition_spec_id: 1,
                first_row_id: None,
                referenced_data_file: referenced_file.map(|s| s.to_string()),
                content_offset: None,
                content_size_in_bytes: None,
            },
        })
    }

    #[test]
    fn test_delete_tracker_identifies_dangling_position_delete() {
        let mut tracker = DeleteTracker::new(partitioned_spec_mapping());

        // Position delete referencing specific file
        let pos_delete = test_delete_entry(
            "delete1.parquet",
            DataContentType::PositionDeletes,
            Struct::from_iter([Some(Literal::date(19737))]),
            Some("data1.parquet"), // References data1.parquet
        );
        tracker.add_delete(pos_delete).unwrap();

        // After compacting data1.parquet, it no longer exists
        let rewritten_files: HashSet<String> =
            vec!["data1.parquet".to_string()].into_iter().collect();
        let remaining_files: HashSet<String> = HashSet::new(); // No files remain

        let dangling = tracker.find_dangling_position_deletes(&rewritten_files, &remaining_files);

        assert_eq!(dangling.len(), 1);
        assert_eq!(dangling[0].data_file.file_path, "delete1.parquet");
    }

    #[test]
    fn test_delete_tracker_keeps_active_position_delete() {
        let mut tracker = DeleteTracker::new(partitioned_spec_mapping());

        // Position delete referencing data1.parquet
        let pos_delete = test_delete_entry(
            "delete1.parquet",
            DataContentType::PositionDeletes,
            Struct::from_iter([Some(Literal::date(19737))]),
            Some("data1.parquet"),
        );
        tracker.add_delete(pos_delete).unwrap();

        // data1.parquet was NOT rewritten, still exists
        let rewritten_files: HashSet<String> =
            vec!["data2.parquet".to_string()].into_iter().collect();
        let remaining_files: HashSet<String> =
            vec!["data1.parquet".to_string()].into_iter().collect();

        let dangling = tracker.find_dangling_position_deletes(&rewritten_files, &remaining_files);

        // Delete still applies to data1.parquet, not dangling
        assert_eq!(dangling.len(), 0);
    }

    #[test]
    fn test_delete_tracker_keeps_delete_when_file_still_exists() {
        let mut tracker = DeleteTracker::new(partitioned_spec_mapping());

        // Position delete referencing data1.parquet
        let pos_delete = test_delete_entry(
            "delete1.parquet",
            DataContentType::PositionDeletes,
            Struct::from_iter([Some(Literal::date(19737))]),
            Some("data1.parquet"),
        );
        tracker.add_delete(pos_delete).unwrap();

        // data1.parquet was in the rewrite set but still exists
        // (e.g., rewrite was partial or file was duplicated)
        let rewritten_files: HashSet<String> =
            vec!["data1.parquet".to_string()].into_iter().collect();
        let remaining_files: HashSet<String> =
            vec!["data1.parquet".to_string()].into_iter().collect();

        let dangling = tracker.find_dangling_position_deletes(&rewritten_files, &remaining_files);

        // File still exists, delete not dangling
        assert_eq!(dangling.len(), 0);
    }

    #[test]
    fn test_delete_tracker_multiple_deletes_same_file() {
        let mut tracker = DeleteTracker::new(partitioned_spec_mapping());

        // Two position deletes referencing the same data file
        let pos_delete1 = test_delete_entry(
            "delete1.parquet",
            DataContentType::PositionDeletes,
            Struct::from_iter([Some(Literal::date(19737))]),
            Some("data1.parquet"),
        );
        let pos_delete2 = test_delete_entry(
            "delete2.parquet",
            DataContentType::PositionDeletes,
            Struct::from_iter([Some(Literal::date(19737))]),
            Some("data1.parquet"),
        );
        tracker.add_delete(pos_delete1).unwrap();
        tracker.add_delete(pos_delete2).unwrap();

        // data1.parquet was rewritten and no longer exists
        let rewritten_files: HashSet<String> =
            vec!["data1.parquet".to_string()].into_iter().collect();
        let remaining_files: HashSet<String> = HashSet::new();

        let dangling = tracker.find_dangling_position_deletes(&rewritten_files, &remaining_files);

        // Both deletes should be dangling
        assert_eq!(dangling.len(), 2);
        let paths: HashSet<&str> = dangling
            .iter()
            .map(|e| e.data_file.file_path.as_str())
            .collect();
        assert!(paths.contains("delete1.parquet"));
        assert!(paths.contains("delete2.parquet"));
    }

    #[test]
    fn test_delete_tracker_position_delete_without_ref_not_dangling() {
        let mut tracker = DeleteTracker::new(partitioned_spec_mapping());

        // Position delete WITHOUT referenced_data_file (partition-scoped)
        let pos_delete = test_delete_entry(
            "delete1.parquet",
            DataContentType::PositionDeletes,
            Struct::from_iter([Some(Literal::date(19737))]),
            None, // No referenced file - applies to all in partition
        );
        tracker.add_delete(pos_delete).unwrap();

        // Even if some data files were rewritten
        let rewritten_files: HashSet<String> =
            vec!["data1.parquet".to_string()].into_iter().collect();
        let remaining_files: HashSet<String> = HashSet::new();

        let dangling = tracker.find_dangling_position_deletes(&rewritten_files, &remaining_files);

        // Partition-scoped deletes are not tracked as dangling in this implementation
        assert_eq!(dangling.len(), 0);
    }

    #[test]
    fn test_delete_tracker_equality_delete_not_dangling() {
        let mut tracker = DeleteTracker::new(partitioned_spec_mapping());

        // Equality delete (always partition-scoped)
        let eq_delete = test_delete_entry(
            "eq_delete.parquet",
            DataContentType::EqualityDeletes,
            Struct::from_iter([Some(Literal::date(19737))]),
            None,
        );
        tracker.add_delete(eq_delete).unwrap();

        let rewritten_files: HashSet<String> =
            vec!["data1.parquet".to_string()].into_iter().collect();
        let remaining_files: HashSet<String> = HashSet::new();

        let dangling = tracker.find_dangling_position_deletes(&rewritten_files, &remaining_files);

        // Equality deletes are not tracked as dangling
        assert_eq!(dangling.len(), 0);
    }

    #[test]
    fn test_delete_tracker_is_empty() {
        let tracker = DeleteTracker::new(partitioned_spec_mapping());
        assert!(tracker.is_empty());
        assert_eq!(tracker.delete_count(), 0);

        let mut tracker = DeleteTracker::new(unpartitioned_spec_mapping());
        let pos_delete = test_delete_entry(
            "delete1.parquet",
            DataContentType::PositionDeletes,
            Struct::empty(),
            Some("data1.parquet"),
        );
        tracker.add_delete(pos_delete).unwrap();

        assert!(!tracker.is_empty());
        assert_eq!(tracker.delete_count(), 1);
    }

    #[test]
    fn test_delete_tracker_unpartitioned_table() {
        let mut tracker = DeleteTracker::new(unpartitioned_spec_mapping());

        // Position delete in unpartitioned table
        let pos_delete = test_delete_entry(
            "delete1.parquet",
            DataContentType::PositionDeletes,
            Struct::empty(),
            Some("data1.parquet"),
        );
        tracker.add_delete(pos_delete).unwrap();

        let rewritten_files: HashSet<String> =
            vec!["data1.parquet".to_string()].into_iter().collect();
        let remaining_files: HashSet<String> = HashSet::new();

        let dangling = tracker.find_dangling_position_deletes(&rewritten_files, &remaining_files);

        assert_eq!(dangling.len(), 1);
    }

    #[test]
    fn test_delete_tracker_rejects_unknown_spec_id() {
        // Empty spec mapping - no specs registered
        let mut tracker = DeleteTracker::new(HashMap::new());

        // Delete file with spec_id 1 (not in mapping)
        let pos_delete = test_delete_entry(
            "delete1.parquet",
            DataContentType::PositionDeletes,
            Struct::from_iter([Some(Literal::date(19737))]),
            Some("data1.parquet"),
        );

        let result = tracker.add_delete(pos_delete);

        // Should fail because spec_id 1 is not in the mapping
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("unknown partition spec ID 1"));
    }
}
