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

//! File group types and bin-packing algorithm for compaction.
//!
//! A `FileGroup` represents a set of files within a single partition that will
//! be compacted together into new, target-sized files. The bin-packing algorithm
//! groups files to maximize efficiency while respecting size constraints.

use crate::spec::{ManifestEntryRef, Struct};

use super::options::RewriteJobOrder;

/// A group of files to be compacted together.
///
/// Files within a group share the same partition and will be rewritten into
/// new, optimally-sized output files. The group tracks both data files and
/// their associated delete files (for V2 tables).
///
/// # Metadata Preservation
///
/// This struct uses `ManifestEntry` rather than `DataFile` to preserve
/// important metadata like sequence numbers, snapshot IDs, and status
/// which are needed during the commit phase.
#[derive(Debug, Clone)]
pub struct FileGroup {
    /// Unique identifier for this group within the compaction plan.
    pub group_id: u32,

    /// Partition this group belongs to.
    ///
    /// `None` for unpartitioned tables. For partitioned tables, all files
    /// in the group share this partition value.
    pub partition: Option<Struct>,

    /// Partition spec ID for this group's files.
    ///
    /// All files in the group should have the same partition spec.
    pub partition_spec_id: i32,

    /// Data files to be compacted in this group.
    ///
    /// These are the input files that will be read and rewritten.
    /// Uses `ManifestEntryRef` (Arc) to avoid cloning large metadata.
    pub data_files: Vec<ManifestEntryRef>,

    /// Position delete files associated with data files in this group.
    ///
    /// For V2 tables, these deletes must be applied during compaction.
    /// Empty for V1 tables.
    pub position_delete_files: Vec<ManifestEntryRef>,

    /// Equality delete files associated with data files in this group.
    ///
    /// For V2 tables, these deletes must be applied during compaction.
    /// Empty for V1 tables.
    pub equality_delete_files: Vec<ManifestEntryRef>,

    /// Total size in bytes of all data files in this group.
    pub total_bytes: u64,

    /// Total number of records across all data files.
    pub total_records: u64,
}

impl FileGroup {
    /// Create a new file group with an initial data file.
    pub fn new(group_id: u32, partition: Option<Struct>, partition_spec_id: i32) -> Self {
        Self {
            group_id,
            partition,
            partition_spec_id,
            data_files: Vec::new(),
            position_delete_files: Vec::new(),
            equality_delete_files: Vec::new(),
            total_bytes: 0,
            total_records: 0,
        }
    }

    /// Create a new file group with an initial data file.
    pub fn with_file(
        group_id: u32,
        partition: Option<Struct>,
        partition_spec_id: i32,
        entry: ManifestEntryRef,
    ) -> Self {
        let total_bytes = entry.file_size_in_bytes();
        let total_records = entry.data_file.record_count();

        Self {
            group_id,
            partition,
            partition_spec_id,
            data_files: vec![entry],
            position_delete_files: Vec::new(),
            equality_delete_files: Vec::new(),
            total_bytes,
            total_records,
        }
    }

    /// Add a data file to this group.
    pub fn add_data_file(&mut self, entry: ManifestEntryRef) {
        self.total_bytes += entry.file_size_in_bytes();
        self.total_records += entry.data_file.record_count();
        self.data_files.push(entry);
    }

    /// Check if this group has capacity for an additional file.
    pub fn has_capacity_for(&self, file_size: u64, max_group_size: u64) -> bool {
        self.total_bytes + file_size <= max_group_size
    }

    /// Number of data files in this group.
    #[inline]
    pub fn file_count(&self) -> usize {
        self.data_files.len()
    }

    /// Total delete files associated with this group.
    #[inline]
    pub fn delete_file_count(&self) -> usize {
        self.position_delete_files.len() + self.equality_delete_files.len()
    }

    /// Check if this group has any associated delete files.
    #[inline]
    pub fn has_delete_files(&self) -> bool {
        !self.position_delete_files.is_empty() || !self.equality_delete_files.is_empty()
    }

    /// Get all file paths in this group (data files only).
    pub fn file_paths(&self) -> Vec<&str> {
        self.data_files
            .iter()
            .map(|e| e.data_file.file_path.as_str())
            .collect()
    }
}

/// Groups files into compaction units using the First-Fit Decreasing bin-packing algorithm.
///
/// The FFD algorithm provides a good approximation for the bin-packing problem:
/// 1. Sort files by size in descending order
/// 2. For each file, place it in the first group that has capacity
/// 3. Create a new group if no existing group has capacity
///
/// This typically achieves results within 11/9 OPT + 6/9 of the optimal solution.
///
/// # Arguments
///
/// * `entries` - Data file entries to group (should all be from same partition)
/// * `partition` - Partition value for all files (None for unpartitioned)
/// * `partition_spec_id` - Partition spec ID for all files
/// * `max_group_size` - Maximum bytes per group
/// * `starting_group_id` - Starting ID for new groups
///
/// # Returns
///
/// Vector of file groups, each containing files that fit within `max_group_size`.
pub fn bin_pack_files(
    mut entries: Vec<ManifestEntryRef>,
    partition: Option<Struct>,
    partition_spec_id: i32,
    max_group_size: u64,
    starting_group_id: u32,
) -> Vec<FileGroup> {
    if entries.is_empty() {
        return Vec::new();
    }

    // Step 1: Sort by file size descending (First-Fit Decreasing)
    entries.sort_by(|a, b| {
        b.file_size_in_bytes()
            .cmp(&a.file_size_in_bytes())
    });

    // Step 2: Bin-pack using First-Fit
    let mut groups: Vec<FileGroup> = Vec::new();
    let mut next_group_id = starting_group_id;

    for entry in entries {
        let file_size = entry.file_size_in_bytes();

        // Find first group with capacity
        let target_group = groups
            .iter_mut()
            .find(|g| g.has_capacity_for(file_size, max_group_size));

        match target_group {
            Some(group) => {
                group.add_data_file(entry);
            }
            None => {
                // Create new group
                let new_group = FileGroup::with_file(
                    next_group_id,
                    partition.clone(),
                    partition_spec_id,
                    entry,
                );
                groups.push(new_group);
                next_group_id += 1;
            }
        }
    }

    groups
}

/// Orders file groups according to the specified strategy.
///
/// This affects which groups are processed first during execution,
/// allowing users to prioritize based on their needs.
pub fn order_groups(mut groups: Vec<FileGroup>, order: &RewriteJobOrder) -> Vec<FileGroup> {
    match order {
        RewriteJobOrder::None => {
            // No ordering, return as-is
        }
        RewriteJobOrder::BytesAsc => {
            groups.sort_by_key(|g| g.total_bytes);
        }
        RewriteJobOrder::BytesDesc => {
            groups.sort_by_key(|g| std::cmp::Reverse(g.total_bytes));
        }
        RewriteJobOrder::FilesAsc => {
            groups.sort_by_key(|g| g.data_files.len());
        }
        RewriteJobOrder::FilesDesc => {
            groups.sort_by_key(|g| std::cmp::Reverse(g.data_files.len()));
        }
    }
    groups
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, ManifestEntry, ManifestStatus,
    };

    /// Helper to create a test manifest entry with specified size
    fn test_entry(file_path: &str, size_bytes: u64, record_count: u64) -> ManifestEntryRef {
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(file_path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(size_bytes)
            .record_count(record_count)
            .partition(Struct::from_iter([Some(Literal::long(0))]))
            .partition_spec_id(0)
            .build()
            .unwrap();

        Arc::new(ManifestEntry {
            status: ManifestStatus::Added,
            snapshot_id: Some(1),
            sequence_number: Some(1),
            file_sequence_number: Some(1),
            data_file,
        })
    }

    #[test]
    fn test_file_group_new() {
        let group = FileGroup::new(0, None, 0);
        assert_eq!(group.group_id, 0);
        assert_eq!(group.file_count(), 0);
        assert_eq!(group.total_bytes, 0);
        assert!(!group.has_delete_files());
    }

    #[test]
    fn test_file_group_with_file() {
        let entry = test_entry("data/file1.parquet", 100_000, 1000);
        let group = FileGroup::with_file(1, None, 0, entry);

        assert_eq!(group.group_id, 1);
        assert_eq!(group.file_count(), 1);
        assert_eq!(group.total_bytes, 100_000);
        assert_eq!(group.total_records, 1000);
    }

    #[test]
    fn test_file_group_add_data_file() {
        let mut group = FileGroup::new(0, None, 0);

        group.add_data_file(test_entry("file1.parquet", 50_000, 500));
        assert_eq!(group.file_count(), 1);
        assert_eq!(group.total_bytes, 50_000);

        group.add_data_file(test_entry("file2.parquet", 30_000, 300));
        assert_eq!(group.file_count(), 2);
        assert_eq!(group.total_bytes, 80_000);
    }

    #[test]
    fn test_file_group_has_capacity() {
        let entry = test_entry("file1.parquet", 60_000, 600);
        let group = FileGroup::with_file(0, None, 0, entry);

        // Max size 100KB, current 60KB, should have room for 40KB
        assert!(group.has_capacity_for(40_000, 100_000));
        assert!(group.has_capacity_for(40_001, 100_000) == false);
        assert!(group.has_capacity_for(30_000, 100_000));
    }

    #[test]
    fn test_bin_pack_empty() {
        let groups = bin_pack_files(vec![], None, 0, 100_000, 0);
        assert!(groups.is_empty());
    }

    #[test]
    fn test_bin_pack_single_file() {
        let entries = vec![test_entry("file1.parquet", 50_000, 500)];
        let groups = bin_pack_files(entries, None, 0, 100_000, 0);

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].file_count(), 1);
        assert_eq!(groups[0].total_bytes, 50_000);
    }

    #[test]
    fn test_bin_pack_files_fit_one_group() {
        // Three files totaling 80KB should fit in one 100KB group
        let entries = vec![
            test_entry("file1.parquet", 30_000, 300),
            test_entry("file2.parquet", 25_000, 250),
            test_entry("file3.parquet", 25_000, 250),
        ];
        let groups = bin_pack_files(entries, None, 0, 100_000, 0);

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].file_count(), 3);
        assert_eq!(groups[0].total_bytes, 80_000);
    }

    #[test]
    fn test_bin_pack_files_multiple_groups() {
        // Files too large to fit in one group
        let entries = vec![
            test_entry("file1.parquet", 60_000, 600),
            test_entry("file2.parquet", 50_000, 500),
            test_entry("file3.parquet", 40_000, 400),
        ];
        let groups = bin_pack_files(entries, None, 0, 100_000, 0);

        // FFD: 60KB -> group0, 50KB -> group1 (60+50>100), 40KB -> group0 (60+40=100)
        assert_eq!(groups.len(), 2);

        // Verify total bytes across groups
        let total: u64 = groups.iter().map(|g| g.total_bytes).sum();
        assert_eq!(total, 150_000);
    }

    #[test]
    fn test_bin_pack_first_fit_decreasing() {
        // FFD should sort descending and pack optimally
        // Files: 70KB, 50KB, 40KB, 30KB, 20KB, 10KB = 220KB total
        // With 100KB max: optimal is 3 groups
        let entries = vec![
            test_entry("small.parquet", 10_000, 100),
            test_entry("medium-small.parquet", 20_000, 200),
            test_entry("medium.parquet", 30_000, 300),
            test_entry("large-medium.parquet", 40_000, 400),
            test_entry("large.parquet", 50_000, 500),
            test_entry("largest.parquet", 70_000, 700),
        ];
        let groups = bin_pack_files(entries, None, 0, 100_000, 0);

        // FFD packing:
        // - 70KB first -> group0
        // - 50KB -> group1 (70+50>100)
        // - 40KB -> group1 (50+40=90) ✓
        // - 30KB -> group0 (70+30=100) ✓
        // - 20KB -> group2 (100+20>100, 90+20>100)
        // - 10KB -> group1 (90+10=100) ✓
        // Result: 3 groups with (100KB, 100KB, 20KB)
        assert_eq!(groups.len(), 3);

        // Verify total
        let total: u64 = groups.iter().map(|g| g.total_bytes).sum();
        assert_eq!(total, 220_000);
    }

    #[test]
    fn test_bin_pack_preserves_partition() {
        let partition = Struct::from_iter([Some(Literal::string("2024-01-01"))]);
        let entries = vec![test_entry("file1.parquet", 50_000, 500)];

        let groups = bin_pack_files(entries, Some(partition.clone()), 1, 100_000, 0);

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].partition, Some(partition));
        assert_eq!(groups[0].partition_spec_id, 1);
    }

    #[test]
    fn test_bin_pack_starting_group_id() {
        let entries = vec![
            test_entry("file1.parquet", 60_000, 600),
            test_entry("file2.parquet", 60_000, 600),
        ];
        let groups = bin_pack_files(entries, None, 0, 100_000, 10);

        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].group_id, 10);
        assert_eq!(groups[1].group_id, 11);
    }

    #[test]
    fn test_order_groups_none() {
        let groups = vec![
            create_test_group(0, 100_000, 5),
            create_test_group(1, 50_000, 10),
            create_test_group(2, 75_000, 3),
        ];
        let ordered = order_groups(groups.clone(), &RewriteJobOrder::None);

        // Should preserve original order
        assert_eq!(ordered[0].group_id, 0);
        assert_eq!(ordered[1].group_id, 1);
        assert_eq!(ordered[2].group_id, 2);
    }

    #[test]
    fn test_order_groups_bytes_asc() {
        let groups = vec![
            create_test_group(0, 100_000, 5),
            create_test_group(1, 50_000, 10),
            create_test_group(2, 75_000, 3),
        ];
        let ordered = order_groups(groups, &RewriteJobOrder::BytesAsc);

        assert_eq!(ordered[0].total_bytes, 50_000);
        assert_eq!(ordered[1].total_bytes, 75_000);
        assert_eq!(ordered[2].total_bytes, 100_000);
    }

    #[test]
    fn test_order_groups_bytes_desc() {
        let groups = vec![
            create_test_group(0, 100_000, 5),
            create_test_group(1, 50_000, 10),
            create_test_group(2, 75_000, 3),
        ];
        let ordered = order_groups(groups, &RewriteJobOrder::BytesDesc);

        assert_eq!(ordered[0].total_bytes, 100_000);
        assert_eq!(ordered[1].total_bytes, 75_000);
        assert_eq!(ordered[2].total_bytes, 50_000);
    }

    #[test]
    fn test_order_groups_files_asc() {
        let groups = vec![
            create_test_group(0, 100_000, 5),
            create_test_group(1, 50_000, 10),
            create_test_group(2, 75_000, 3),
        ];
        let ordered = order_groups(groups, &RewriteJobOrder::FilesAsc);

        assert_eq!(ordered[0].file_count(), 3);
        assert_eq!(ordered[1].file_count(), 5);
        assert_eq!(ordered[2].file_count(), 10);
    }

    #[test]
    fn test_order_groups_files_desc() {
        let groups = vec![
            create_test_group(0, 100_000, 5),
            create_test_group(1, 50_000, 10),
            create_test_group(2, 75_000, 3),
        ];
        let ordered = order_groups(groups, &RewriteJobOrder::FilesDesc);

        assert_eq!(ordered[0].file_count(), 10);
        assert_eq!(ordered[1].file_count(), 5);
        assert_eq!(ordered[2].file_count(), 3);
    }

    /// Helper to create a test group with specified metrics
    fn create_test_group(group_id: u32, total_bytes: u64, file_count: usize) -> FileGroup {
        let mut group = FileGroup::new(group_id, None, 0);
        let per_file_size = total_bytes / file_count as u64;

        for i in 0..file_count {
            group.add_data_file(test_entry(
                &format!("file{i}.parquet"),
                per_file_size,
                100,
            ));
        }
        group
    }
}
