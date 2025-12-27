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

//! Planning logic for the rewrite manifests action.
//!
//! The planner is responsible for:
//! 1. Loading manifest files from the current snapshot
//! 2. Filtering manifests by content type (data vs delete)
//! 3. Filtering manifests by partition spec ID (if specified)
//! 4. Applying user predicate for manifest selection
//! 5. Loading manifest entries from selected manifests
//! 6. Sorting entries by partition for optimal clustering

use std::cmp::Ordering;

use uuid::Uuid;

use super::ManifestPredicate;
use super::options::RewriteManifestsOptions;
use crate::spec::{
    DataFileFormat, Literal, ManifestContentType, ManifestEntryRef, ManifestFile, Struct,
};

/// Compare two partition structs lexicographically without string allocation.
///
/// This function compares partition values field by field, using the natural
/// ordering of `PrimitiveLiteral` values. Null values sort before non-null values.
///
/// # Arguments
///
/// * `a` - First partition struct
/// * `b` - Second partition struct
///
/// # Returns
///
/// `Ordering::Less`, `Ordering::Equal`, or `Ordering::Greater` based on lexicographic comparison.
fn compare_partitions(a: &Struct, b: &Struct) -> Ordering {
    for (field_a, field_b) in a.iter().zip(b.iter()) {
        let ord = compare_optional_literals(field_a, field_b);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    // If all compared fields are equal, compare by length (shorter is less)
    a.iter().len().cmp(&b.iter().len())
}

/// Compare two optional literal values.
///
/// None (null) values sort before Some values.
fn compare_optional_literals(a: Option<&Literal>, b: Option<&Literal>) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(lit_a), Some(lit_b)) => compare_literals(lit_a, lit_b),
    }
}

/// Compare two literal values.
///
/// For primitives, uses the natural `PartialOrd` ordering.
/// For complex types (struct, list, map), falls back to Debug string comparison.
fn compare_literals(a: &Literal, b: &Literal) -> Ordering {
    match (a, b) {
        (Literal::Primitive(prim_a), Literal::Primitive(prim_b)) => {
            // PrimitiveLiteral implements PartialOrd
            prim_a.partial_cmp(prim_b).unwrap_or(Ordering::Equal)
        }
        // For complex types, fall back to Debug representation
        // (rare in partition values, but handle gracefully)
        _ => format!("{a:?}").cmp(&format!("{b:?}")),
    }
}

/// Generate a unique manifest file path for a rewrite operation.
///
/// The path follows the Iceberg convention:
/// `{table_location}/metadata/{commit_uuid}-m{index}.avro`
///
/// # Arguments
///
/// * `table_location` - The table's base location
/// * `commit_uuid` - A unique UUID for this commit operation
/// * `index` - The manifest index within this commit
/// * `_content` - The manifest content type (for future use in path naming)
///
/// # Returns
///
/// A unique manifest file path as a String.
pub(crate) fn generate_manifest_path(
    table_location: &str,
    commit_uuid: &Uuid,
    index: u64,
    _content: ManifestContentType,
) -> String {
    format!(
        "{}/{}/{}-m{}.{}",
        table_location,
        "metadata",
        commit_uuid,
        index,
        DataFileFormat::Avro
    )
}

/// Planner for the rewrite manifests operation.
///
/// The planner analyzes the current table state and produces a plan
/// describing which manifests should be consolidated and how entries
/// should be reorganized.
#[derive(Debug)]
pub struct RewriteManifestsPlanner<'a> {
    options: &'a RewriteManifestsOptions,
}

impl<'a> RewriteManifestsPlanner<'a> {
    /// Create a new planner with the given options.
    pub fn new(options: &'a RewriteManifestsOptions) -> Self {
        Self { options }
    }

    /// Get the options for this planner.
    pub fn options(&self) -> &RewriteManifestsOptions {
        self.options
    }

    /// Filter manifests based on options and an optional predicate.
    ///
    /// Filtering is applied in this order:
    /// 1. Partition spec ID (if `options.spec_id` is set)
    /// 2. Content type (exclude delete manifests if `options.rewrite_delete_manifests` is false)
    /// 3. Minimum manifest size (if `options.min_manifest_size_bytes` > 0, only smaller manifests are included)
    /// 4. User predicate (if provided)
    ///
    /// # Arguments
    ///
    /// * `manifests` - Slice of manifest files to filter
    /// * `predicate` - Optional user predicate for custom filtering
    ///
    /// # Returns
    ///
    /// A vector of references to manifests that pass all filters.
    pub fn filter_manifests<'b>(
        &self,
        manifests: &'b [ManifestFile],
        predicate: Option<&ManifestPredicate>,
    ) -> Vec<&'b ManifestFile> {
        manifests
            .iter()
            .filter(|m| {
                // Filter by spec_id if specified
                if let Some(spec_id) = self.options.spec_id
                    && m.partition_spec_id != spec_id
                {
                    return false;
                }

                // Filter out delete manifests if not enabled
                if !self.options.rewrite_delete_manifests
                    && m.content == ManifestContentType::Deletes
                {
                    return false;
                }

                // Filter by minimum manifest size threshold
                // Only include manifests smaller than the threshold for rewriting
                if self.options.min_manifest_size_bytes > 0 {
                    let manifest_size = u64::try_from(m.manifest_length).unwrap_or(0);
                    if manifest_size >= self.options.min_manifest_size_bytes {
                        return false;
                    }
                }

                // Apply user predicate if provided
                if let Some(pred) = predicate
                    && !pred(m)
                {
                    return false;
                }

                true
            })
            .collect()
    }

    /// Sort manifest entries by partition for optimal clustering.
    ///
    /// Entries are sorted by (partition_spec_id, partition_value) to group
    /// files from the same partition together. This clustering improves
    /// query performance by enabling more effective partition pruning.
    ///
    /// # Arguments
    ///
    /// * `entries` - Vector of manifest entries to sort
    ///
    /// # Returns
    ///
    /// A sorted vector of manifest entries.
    ///
    /// # Performance
    ///
    /// Uses a non-allocating comparator for primitive partition values.
    /// Complex partition types (struct, list, map) fall back to Debug
    /// string comparison, which is rare in practice.
    pub fn sort_entries_by_partition(
        &self,
        mut entries: Vec<ManifestEntryRef>,
    ) -> Vec<ManifestEntryRef> {
        entries.sort_by(|a, b| {
            // First compare by partition spec ID
            let spec_cmp = a
                .data_file
                .partition_spec_id
                .cmp(&b.data_file.partition_spec_id);
            if spec_cmp != Ordering::Equal {
                return spec_cmp;
            }
            // Then compare by partition values (non-allocating)
            compare_partitions(a.data_file.partition(), b.data_file.partition())
        });
        entries
    }

    /// Batch manifest entries into groups based on target manifest size.
    ///
    /// Entries are grouped into batches where each batch will be written
    /// to a separate manifest file. The batching uses estimated entry sizes
    /// to determine how many entries fit in the target manifest size.
    ///
    /// # Arguments
    ///
    /// * `entries` - Vector of manifest entries to batch
    ///
    /// # Returns
    ///
    /// A vector of batches, where each batch is a vector of manifest entries.
    pub fn batch_entries(&self, entries: Vec<ManifestEntryRef>) -> Vec<Vec<ManifestEntryRef>> {
        if entries.is_empty() {
            return vec![];
        }

        // Estimate entry size based on average file path length + fixed overhead
        let avg_entry_size = self.estimate_entry_size(&entries);
        let entries_per_batch =
            (self.options.target_manifest_size_bytes / avg_entry_size).max(1) as usize;

        // Split entries into batches
        entries
            .chunks(entries_per_batch)
            .map(|chunk| chunk.to_vec())
            .collect()
    }

    /// Estimate the average size of a manifest entry in bytes.
    ///
    /// This is used to calculate how many entries fit in a target-sized manifest.
    fn estimate_entry_size(&self, entries: &[ManifestEntryRef]) -> u64 {
        if entries.is_empty() {
            return 1024; // Default estimate
        }

        // Sum up path lengths
        let total_path_len: usize = entries.iter().map(|e| e.data_file.file_path().len()).sum();

        let avg_path_len = total_path_len / entries.len();

        // Estimated size: path length + partition data + stats + overhead
        // Typical overhead: ~200 bytes for metadata, bounds, etc.
        (avg_path_len + 200) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{ManifestContentType, ManifestFile};

    /// Create a test manifest file with configurable properties.
    fn test_manifest(
        path: &str,
        length: i64,
        spec_id: i32,
        content: ManifestContentType,
    ) -> ManifestFile {
        ManifestFile {
            manifest_path: path.to_string(),
            manifest_length: length,
            partition_spec_id: spec_id,
            content,
            sequence_number: 1,
            min_sequence_number: 1,
            added_snapshot_id: 1,
            added_files_count: Some(10),
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: Some(1000),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: None,
            key_metadata: None,
            first_row_id: None,
        }
    }

    #[test]
    fn test_planner_creation() {
        // Test that we can create a RewriteManifestsPlanner with options
        let options = crate::transaction::RewriteManifestsOptions::default();
        let planner = RewriteManifestsPlanner::new(&options);

        // Verify the planner stores the options
        assert_eq!(
            planner.options().target_manifest_size_bytes,
            crate::transaction::DEFAULT_TARGET_MANIFEST_SIZE_BYTES
        );
    }

    #[test]
    fn test_planner_with_spec_id() {
        let options = RewriteManifestsOptions {
            spec_id: Some(42),
            ..Default::default()
        };

        let planner = RewriteManifestsPlanner::new(&options);

        assert_eq!(planner.options().spec_id, Some(42));
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Manifest Filtering Tests
    // ═══════════════════════════════════════════════════════════════════════

    #[test]
    fn test_filter_manifests_by_spec_id() {
        // When spec_id is set in options, only manifests with that spec_id should pass
        let options = RewriteManifestsOptions {
            spec_id: Some(1),
            ..Default::default()
        };

        let planner = RewriteManifestsPlanner::new(&options);

        let manifests = vec![
            test_manifest("m1.avro", 1000, 1, ManifestContentType::Data),
            test_manifest("m2.avro", 1000, 2, ManifestContentType::Data),
            test_manifest("m3.avro", 1000, 1, ManifestContentType::Data),
        ];

        let filtered = planner.filter_manifests(&manifests, None);

        assert_eq!(filtered.len(), 2);
        assert!(filtered.iter().all(|m| m.partition_spec_id == 1));
    }

    #[test]
    fn test_filter_manifests_no_spec_id_filter() {
        // When spec_id is None, all manifests should pass (no spec filtering)
        let options = RewriteManifestsOptions::default();
        assert!(options.spec_id.is_none());

        let planner = RewriteManifestsPlanner::new(&options);

        let manifests = vec![
            test_manifest("m1.avro", 1000, 1, ManifestContentType::Data),
            test_manifest("m2.avro", 1000, 2, ManifestContentType::Data),
            test_manifest("m3.avro", 1000, 3, ManifestContentType::Data),
        ];

        let filtered = planner.filter_manifests(&manifests, None);

        assert_eq!(filtered.len(), 3);
    }

    #[test]
    fn test_filter_manifests_separates_data_and_delete() {
        // The filter should separate data manifests from delete manifests
        let options = RewriteManifestsOptions::default();
        let planner = RewriteManifestsPlanner::new(&options);

        let manifests = vec![
            test_manifest("data1.avro", 1000, 1, ManifestContentType::Data),
            test_manifest("delete1.avro", 1000, 1, ManifestContentType::Deletes),
            test_manifest("data2.avro", 1000, 1, ManifestContentType::Data),
            test_manifest("delete2.avro", 1000, 1, ManifestContentType::Deletes),
        ];

        // When rewrite_delete_manifests is true (default), both should be returned
        let filtered = planner.filter_manifests(&manifests, None);
        let data_count = filtered
            .iter()
            .filter(|m| m.content == ManifestContentType::Data)
            .count();
        let delete_count = filtered
            .iter()
            .filter(|m| m.content == ManifestContentType::Deletes)
            .count();

        assert_eq!(data_count, 2);
        assert_eq!(delete_count, 2);
    }

    #[test]
    fn test_filter_manifests_excludes_delete_when_disabled() {
        // When rewrite_delete_manifests is false, delete manifests should be excluded
        let options = RewriteManifestsOptions {
            rewrite_delete_manifests: false,
            ..Default::default()
        };

        let planner = RewriteManifestsPlanner::new(&options);

        let manifests = vec![
            test_manifest("data1.avro", 1000, 1, ManifestContentType::Data),
            test_manifest("delete1.avro", 1000, 1, ManifestContentType::Deletes),
            test_manifest("data2.avro", 1000, 1, ManifestContentType::Data),
        ];

        let filtered = planner.filter_manifests(&manifests, None);

        assert_eq!(filtered.len(), 2);
        assert!(
            filtered
                .iter()
                .all(|m| m.content == ManifestContentType::Data)
        );
    }

    #[test]
    fn test_filter_manifests_with_predicate() {
        // A custom predicate should filter manifests
        use std::sync::Arc;

        let options = RewriteManifestsOptions::default();
        let planner = RewriteManifestsPlanner::new(&options);

        let manifests = vec![
            test_manifest("small1.avro", 1000, 1, ManifestContentType::Data),
            test_manifest("large1.avro", 10_000_000, 1, ManifestContentType::Data), // 10 MB
            test_manifest("small2.avro", 2000, 1, ManifestContentType::Data),
            test_manifest("large2.avro", 20_000_000, 1, ManifestContentType::Data), // 20 MB
        ];

        // Predicate: only include manifests smaller than 5 MB
        let predicate: super::ManifestPredicate = Arc::new(|m| m.manifest_length < 5_000_000);

        let filtered = planner.filter_manifests(&manifests, Some(&predicate));

        assert_eq!(filtered.len(), 2);
        assert!(filtered.iter().all(|m| m.manifest_length < 5_000_000));
    }

    #[test]
    fn test_filter_manifests_with_predicate_and_spec_id() {
        // Both predicate and spec_id should be combined
        use std::sync::Arc;

        let options = RewriteManifestsOptions {
            spec_id: Some(1),
            ..Default::default()
        };

        let planner = RewriteManifestsPlanner::new(&options);

        let manifests = vec![
            test_manifest("small_spec1.avro", 1000, 1, ManifestContentType::Data),
            test_manifest("large_spec1.avro", 10_000_000, 1, ManifestContentType::Data),
            test_manifest("small_spec2.avro", 1000, 2, ManifestContentType::Data), // Different spec
            test_manifest("small_spec1_2.avro", 2000, 1, ManifestContentType::Data),
        ];

        // Predicate: only small manifests
        let predicate: super::ManifestPredicate = Arc::new(|m| m.manifest_length < 5_000_000);

        let filtered = planner.filter_manifests(&manifests, Some(&predicate));

        // Should only match: small_spec1.avro and small_spec1_2.avro
        // (spec_id=1 AND size < 5MB)
        assert_eq!(filtered.len(), 2);
        assert!(filtered.iter().all(|m| m.partition_spec_id == 1));
        assert!(filtered.iter().all(|m| m.manifest_length < 5_000_000));
    }

    #[test]
    fn test_filter_manifests_by_min_size() {
        // When min_manifest_size_bytes is set, only smaller manifests should be included
        let options = RewriteManifestsOptions {
            min_manifest_size_bytes: 5_000_000, // 5 MB threshold
            ..Default::default()
        };

        let planner = RewriteManifestsPlanner::new(&options);

        let manifests = vec![
            test_manifest("small1.avro", 1000, 1, ManifestContentType::Data),
            test_manifest("large1.avro", 10_000_000, 1, ManifestContentType::Data), // 10 MB - excluded
            test_manifest("medium.avro", 4_999_999, 1, ManifestContentType::Data), // Just under threshold
            test_manifest("exact.avro", 5_000_000, 1, ManifestContentType::Data), // Exactly threshold - excluded
        ];

        let filtered = planner.filter_manifests(&manifests, None);

        // Should only include manifests smaller than 5 MB
        assert_eq!(filtered.len(), 2);
        assert!(filtered.iter().all(|m| m.manifest_length < 5_000_000));
    }

    #[test]
    fn test_filter_manifests_no_min_size_includes_all() {
        // When min_manifest_size_bytes is 0 (default), all manifests should be included
        let options = RewriteManifestsOptions::default();
        assert_eq!(options.min_manifest_size_bytes, 0);

        let planner = RewriteManifestsPlanner::new(&options);

        let manifests = vec![
            test_manifest("small.avro", 1000, 1, ManifestContentType::Data),
            test_manifest("large.avro", 100_000_000, 1, ManifestContentType::Data), // 100 MB
        ];

        let filtered = planner.filter_manifests(&manifests, None);

        // Both should be included since min_manifest_size_bytes is 0
        assert_eq!(filtered.len(), 2);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Entry Sorting Tests
    // ═══════════════════════════════════════════════════════════════════════

    use std::sync::Arc;

    use crate::spec::{
        DataContentType, DataFile, DataFileFormat, Literal, ManifestEntry, ManifestEntryRef,
        ManifestStatus, Struct,
    };

    /// Create a test manifest entry with a given partition value.
    fn test_entry(path: &str, partition: Struct, spec_id: i32) -> ManifestEntryRef {
        Arc::new(ManifestEntry {
            status: ManifestStatus::Added,
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
                partition_spec_id: spec_id,
                first_row_id: None,
                referenced_data_file: None,
                content_offset: None,
                content_size_in_bytes: None,
            },
        })
    }

    #[test]
    fn test_sort_entries_by_partition() {
        // Entries should be sorted by (partition_spec_id, partition_value)
        // so entries with the same partition are grouped together
        let options = RewriteManifestsOptions::default();
        let planner = RewriteManifestsPlanner::new(&options);

        // Create entries with different partitions (using date values)
        let partition_a = Struct::from_iter([Some(Literal::date(19737))]); // 2024-01-15
        let partition_b = Struct::from_iter([Some(Literal::date(19738))]); // 2024-01-16
        let partition_c = Struct::from_iter([Some(Literal::date(19739))]); // 2024-01-17

        let entries = vec![
            test_entry("file1.parquet", partition_b.clone(), 1),
            test_entry("file2.parquet", partition_a.clone(), 1),
            test_entry("file3.parquet", partition_c.clone(), 1),
            test_entry("file4.parquet", partition_a.clone(), 1),
            test_entry("file5.parquet", partition_b.clone(), 1),
        ];

        let sorted = planner.sort_entries_by_partition(entries);

        // After sorting, entries with the same partition should be adjacent
        // partition_a (2 files), partition_b (2 files), partition_c (1 file)
        assert_eq!(sorted.len(), 5);

        // Check that entries are grouped by partition
        // First two should have partition_a (19737)
        assert_eq!(sorted[0].data_file.partition(), &partition_a);
        assert_eq!(sorted[1].data_file.partition(), &partition_a);

        // Next two should have partition_b (19738)
        assert_eq!(sorted[2].data_file.partition(), &partition_b);
        assert_eq!(sorted[3].data_file.partition(), &partition_b);

        // Last one should have partition_c (19739)
        assert_eq!(sorted[4].data_file.partition(), &partition_c);
    }

    #[test]
    fn test_sort_entries_by_partition_with_multiple_specs() {
        // Entries should be grouped by spec_id first, then by partition
        let options = RewriteManifestsOptions::default();
        let planner = RewriteManifestsPlanner::new(&options);

        let partition_a = Struct::from_iter([Some(Literal::date(19737))]);
        let partition_b = Struct::from_iter([Some(Literal::date(19738))]);

        let entries = vec![
            test_entry("file1.parquet", partition_a.clone(), 2), // spec 2
            test_entry("file2.parquet", partition_a.clone(), 1), // spec 1
            test_entry("file3.parquet", partition_b.clone(), 2), // spec 2
            test_entry("file4.parquet", partition_b.clone(), 1), // spec 1
        ];

        let sorted = planner.sort_entries_by_partition(entries);

        // Should be sorted by (spec_id, partition):
        // spec 1, partition_a -> spec 1, partition_b -> spec 2, partition_a -> spec 2, partition_b
        assert_eq!(sorted[0].data_file.partition_spec_id, 1);
        assert_eq!(sorted[0].data_file.partition(), &partition_a);

        assert_eq!(sorted[1].data_file.partition_spec_id, 1);
        assert_eq!(sorted[1].data_file.partition(), &partition_b);

        assert_eq!(sorted[2].data_file.partition_spec_id, 2);
        assert_eq!(sorted[2].data_file.partition(), &partition_a);

        assert_eq!(sorted[3].data_file.partition_spec_id, 2);
        assert_eq!(sorted[3].data_file.partition(), &partition_b);
    }

    #[test]
    fn test_sort_entries_empty() {
        let options = RewriteManifestsOptions::default();
        let planner = RewriteManifestsPlanner::new(&options);

        let entries: Vec<ManifestEntryRef> = vec![];
        let sorted = planner.sort_entries_by_partition(entries);

        assert!(sorted.is_empty());
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Entry Batching Tests (Phase 2.8.3)
    // ═══════════════════════════════════════════════════════════════════════

    #[test]
    fn test_batch_entries_single_batch() {
        // When entries fit in a single manifest, return one batch
        let options = RewriteManifestsOptions {
            target_manifest_size_bytes: 1_000_000, // 1 MB
            ..Default::default()
        };
        let planner = RewriteManifestsPlanner::new(&options);

        let partition = Struct::from_iter([Some(Literal::date(19737))]);
        let entries = vec![
            test_entry("file1.parquet", partition.clone(), 1),
            test_entry("file2.parquet", partition.clone(), 1),
            test_entry("file3.parquet", partition.clone(), 1),
        ];

        let batches = planner.batch_entries(entries);

        // All entries should fit in one batch
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 3);
    }

    #[test]
    fn test_batch_entries_multiple_batches() {
        // With a small target size, entries should be split into multiple batches
        // Each test_entry has ~14 char path ("file1.parquet") + 200 overhead = ~214 bytes
        // Target size 500 bytes = ~2 entries per batch
        let options = RewriteManifestsOptions {
            target_manifest_size_bytes: 500,
            ..Default::default()
        };
        let planner = RewriteManifestsPlanner::new(&options);

        let partition = Struct::from_iter([Some(Literal::date(19737))]);
        let entries = vec![
            test_entry("file1.parquet", partition.clone(), 1),
            test_entry("file2.parquet", partition.clone(), 1),
            test_entry("file3.parquet", partition.clone(), 1),
            test_entry("file4.parquet", partition.clone(), 1),
            test_entry("file5.parquet", partition.clone(), 1),
        ];

        let batches = planner.batch_entries(entries);

        // Should split into multiple batches (at least 2)
        assert!(batches.len() >= 2);

        // Total entries should be preserved
        let total_entries: usize = batches.iter().map(|b| b.len()).sum();
        assert_eq!(total_entries, 5);
    }

    #[test]
    fn test_batch_entries_empty() {
        let options = RewriteManifestsOptions::default();
        let planner = RewriteManifestsPlanner::new(&options);

        let entries: Vec<ManifestEntryRef> = vec![];
        let batches = planner.batch_entries(entries);

        assert!(batches.is_empty());
    }

    #[test]
    fn test_batch_entries_preserves_order() {
        // Batching should preserve the order of entries
        let options = RewriteManifestsOptions {
            target_manifest_size_bytes: 500, // Small to create multiple batches
            ..Default::default()
        };
        let planner = RewriteManifestsPlanner::new(&options);

        let partition = Struct::from_iter([Some(Literal::date(19737))]);
        let entries = vec![
            test_entry("a.parquet", partition.clone(), 1),
            test_entry("b.parquet", partition.clone(), 1),
            test_entry("c.parquet", partition.clone(), 1),
            test_entry("d.parquet", partition.clone(), 1),
        ];

        let batches = planner.batch_entries(entries);

        // Flatten batches and verify order is preserved
        let flattened: Vec<_> = batches
            .into_iter()
            .flatten()
            .map(|e| e.data_file.file_path.clone())
            .collect();

        assert_eq!(flattened, vec![
            "a.parquet",
            "b.parquet",
            "c.parquet",
            "d.parquet"
        ]);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Manifest Path Generation Tests (Phase 2.8.3)
    // ═══════════════════════════════════════════════════════════════════════

    #[test]
    fn test_generate_manifest_path() {
        use uuid::Uuid;

        let table_location = "s3://bucket/warehouse/db/table";
        let commit_uuid = Uuid::new_v4();

        let path = generate_manifest_path(
            table_location,
            &commit_uuid,
            0, // index
            crate::spec::ManifestContentType::Data,
        );

        // Path should follow format: {location}/metadata/{uuid}-m{index}.avro
        assert!(path.starts_with(table_location));
        assert!(path.contains("/metadata/"));
        assert!(path.contains(&commit_uuid.to_string()));
        assert!(path.ends_with(".avro"));
    }

    #[test]
    fn test_generate_manifest_path_unique_indices() {
        use uuid::Uuid;

        let table_location = "s3://bucket/warehouse/db/table";
        let commit_uuid = Uuid::new_v4();

        let path0 = generate_manifest_path(
            table_location,
            &commit_uuid,
            0,
            crate::spec::ManifestContentType::Data,
        );
        let path1 = generate_manifest_path(
            table_location,
            &commit_uuid,
            1,
            crate::spec::ManifestContentType::Data,
        );

        // Paths should be different
        assert_ne!(path0, path1);

        // Both should contain the index
        assert!(path0.contains("-m0"));
        assert!(path1.contains("-m1"));
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Partition Comparison Tests (Performance Optimization)
    // ═══════════════════════════════════════════════════════════════════════

    use std::cmp::Ordering;

    #[test]
    fn test_compare_partitions_equal() {
        let partition_a = Struct::from_iter([Some(Literal::date(19737))]);
        let partition_b = Struct::from_iter([Some(Literal::date(19737))]);

        assert_eq!(
            compare_partitions(&partition_a, &partition_b),
            Ordering::Equal
        );
    }

    #[test]
    fn test_compare_partitions_less() {
        let partition_a = Struct::from_iter([Some(Literal::date(19737))]);
        let partition_b = Struct::from_iter([Some(Literal::date(19738))]);

        assert_eq!(
            compare_partitions(&partition_a, &partition_b),
            Ordering::Less
        );
    }

    #[test]
    fn test_compare_partitions_greater() {
        let partition_a = Struct::from_iter([Some(Literal::date(19738))]);
        let partition_b = Struct::from_iter([Some(Literal::date(19737))]);

        assert_eq!(
            compare_partitions(&partition_a, &partition_b),
            Ordering::Greater
        );
    }

    #[test]
    fn test_compare_partitions_multi_field() {
        // Multi-field partitions should compare lexicographically
        let partition_a = Struct::from_iter([Some(Literal::date(19737)), Some(Literal::int(1))]);
        let partition_b = Struct::from_iter([Some(Literal::date(19737)), Some(Literal::int(2))]);

        assert_eq!(
            compare_partitions(&partition_a, &partition_b),
            Ordering::Less
        );
    }

    #[test]
    fn test_compare_partitions_null_handling() {
        // Null values should sort before non-null
        let partition_a = Struct::from_iter([None::<Literal>]);
        let partition_b = Struct::from_iter([Some(Literal::date(19737))]);

        assert_eq!(
            compare_partitions(&partition_a, &partition_b),
            Ordering::Less
        );
    }

    #[test]
    fn test_compare_partitions_empty() {
        // Empty partitions should be equal
        let partition_a = Struct::empty();
        let partition_b = Struct::empty();

        assert_eq!(
            compare_partitions(&partition_a, &partition_b),
            Ordering::Equal
        );
    }
}
