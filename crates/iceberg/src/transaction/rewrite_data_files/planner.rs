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

//! Planning logic for the rewrite data files action.
//!
//! The planner is responsible for:
//! 1. Loading manifest entries from the current snapshot
//! 2. Filtering files by partition predicate (if provided)
//! 3. Selecting candidate files based on size and delete thresholds
//! 4. Grouping files by partition
//! 5. Applying bin-packing to create file groups
//! 6. Filtering groups that don't meet minimum requirements
//! 7. Loading associated delete files (V2 tables)

use std::collections::HashMap;

use super::FilterStats;
use super::delete_tracker::DeleteTracker;
use super::file_group::{FileGroup, bin_pack_files, order_groups};
use super::options::RewriteDataFilesOptions;
use super::partition_filter::{MultiSpecPartitionFilter, PartitionFilterEvaluator, PartitionKey};
use crate::error::{Error, ErrorKind, Result};
use crate::expr::Predicate;
use crate::spec::{
    DataContentType, FormatVersion, ManifestContentType, ManifestEntryRef, ManifestStatus,
    PartitionSpecRef, Struct,
};
use crate::table::Table;

type PartitionEntryGroups = HashMap<(Option<Struct>, i32), Vec<ManifestEntryRef>>;

/// Result of the planning phase.
#[derive(Debug, Clone)]
pub struct RewriteDataFilesPlan {
    /// File groups to be rewritten.
    pub file_groups: Vec<FileGroup>,

    /// Sequence number to use for new files.
    ///
    /// For MoR tables, this is the starting sequence number captured
    /// at plan time to ensure correct delete ordering.
    pub starting_sequence_number: i64,

    /// Total number of data files that will be rewritten.
    pub total_data_files: u64,

    /// Total bytes in files that will be rewritten.
    pub total_bytes: u64,

    /// Delete file tracker for dangling delete removal.
    ///
    /// Populated when `remove_dangling_deletes` option is enabled.
    /// Used by the committer to identify delete files that can be removed
    /// after compaction.
    pub(super) delete_tracker: Option<DeleteTracker>,
}

/// Result of planning with filter diagnostics.
///
/// Returned by [`RewriteDataFilesPlanner::plan_with_stats`] to provide both
/// the plan and diagnostics about partition filter evaluation.
#[derive(Debug, Clone)]
pub struct RewriteDataFilesPlanningResult {
    /// The compaction plan.
    pub plan: RewriteDataFilesPlan,

    /// Partition filter diagnostics.
    ///
    /// Contains counters for fail-open behavior during filtering.
    /// Check `fail_open_partitions > 0` to detect degraded filtering.
    pub filter_stats: FilterStats,
}

/// Planner for the rewrite data files operation.
///
/// The planner analyzes the current table state and produces a plan
/// describing which files should be compacted together.
pub struct RewriteDataFilesPlanner<'a> {
    table: &'a Table,
    options: &'a RewriteDataFilesOptions,
    /// Optional partition filter to limit which partitions to compact.
    partition_filter: Option<&'a Predicate>,
    /// Case sensitivity for partition filter field matching.
    case_sensitive: bool,
}

impl<'a> RewriteDataFilesPlanner<'a> {
    /// Create a new planner.
    pub fn new(table: &'a Table, options: &'a RewriteDataFilesOptions) -> Self {
        Self {
            table,
            options,
            partition_filter: None,
            case_sensitive: true,
        }
    }

    /// Set a partition filter to limit which partitions are compacted.
    ///
    /// The predicate is projected to partition values using inclusive projection,
    /// meaning it will include any partition that *might* contain matching data.
    ///
    /// # Partition Evolution (Multi-Spec Evaluation)
    ///
    /// When a table has multiple partition specs (due to partition evolution),
    /// each file is evaluated against its own partition spec. The predicate is
    /// projected separately to each spec, enabling correct filtering across
    /// evolved tables.
    ///
    /// **Fail-open semantics:** If the predicate cannot be projected to a spec
    /// (incompatible transforms or missing fields), or if evaluation errors
    /// occur, files in those partitions are included rather than excluded.
    /// This ensures correctness at the cost of potentially compacting more
    /// files than strictly necessary.
    ///
    /// Use [`Self::plan_with_stats`] to get diagnostics about fail-open behavior.
    #[must_use]
    pub fn with_partition_filter(mut self, filter: &'a Predicate) -> Self {
        self.partition_filter = Some(filter);
        self
    }

    /// Set case sensitivity for partition filter field matching.
    ///
    /// When `true` (default), field names in the filter predicate must match
    /// column names exactly. When `false`, field matching is case-insensitive.
    #[must_use]
    pub fn with_case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.case_sensitive = case_sensitive;
        self
    }

    /// Execute the planning phase with filter diagnostics.
    ///
    /// This method:
    /// 1. Validates the table has a current snapshot
    /// 2. Loads all data file entries from manifests
    /// 3. Filters by partition predicate (if provided) using multi-spec evaluation
    /// 4. Selects candidates based on size thresholds
    /// 5. Counts fail_open_files among candidates
    /// 6. Groups files by partition
    /// 7. Applies bin-packing within partitions
    /// 8. Filters groups below minimum thresholds
    /// 9. Orders groups according to configuration
    /// 10. Loads delete files for V2 tables
    ///
    /// # Returns
    ///
    /// A `RewriteDataFilesPlanningResult` containing file groups to process
    /// and filter diagnostics, or an error if planning fails.
    pub async fn plan_with_stats(&self) -> Result<RewriteDataFilesPlanningResult> {
        // Step 1: Validate snapshot exists
        let snapshot = self.table.metadata().current_snapshot().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Cannot rewrite data files: table has no current snapshot",
            )
        })?;

        let starting_sequence_number = snapshot.sequence_number();

        // Step 2: Load all data file entries
        let all_entries = self.load_data_files().await?;

        if all_entries.is_empty() {
            return Ok(RewriteDataFilesPlanningResult {
                plan: RewriteDataFilesPlan {
                    file_groups: vec![],
                    starting_sequence_number,
                    total_data_files: 0,
                    total_bytes: 0,
                    delete_tracker: None,
                },
                filter_stats: FilterStats::default(),
            });
        }

        // Step 3: Filter by partition predicate (if provided)
        // Use internal method to get both filtered entries and the filter for stats
        let (filtered_entries, partition_filter) =
            self.filter_by_partition_internal(all_entries)?;

        if filtered_entries.is_empty() {
            return Ok(RewriteDataFilesPlanningResult {
                plan: RewriteDataFilesPlan {
                    file_groups: vec![],
                    starting_sequence_number,
                    total_data_files: 0,
                    total_bytes: 0,
                    delete_tracker: None,
                },
                filter_stats: partition_filter.map(|f| f.stats()).unwrap_or_default(),
            });
        }

        // Step 4: Load delete index (if delete_file_threshold is set)
        // This must happen before candidate selection so we can count deletes
        let delete_index = self.load_delete_index().await?;

        // Step 5: Select candidates based on size thresholds and delete count
        let candidates = self.select_candidates(filtered_entries, delete_index.as_ref())?;

        // Step 5b: Count fail_open_files among candidates
        // Build spec_map for PartitionKey construction
        if let Some(ref filter) = partition_filter {
            let spec_map: HashMap<i32, PartitionSpecRef> = self
                .table
                .metadata()
                .partition_specs_iter()
                .map(|spec| (spec.spec_id(), spec.clone()))
                .collect();

            for candidate in &candidates {
                let spec_id = candidate.data_file.partition_spec_id;
                if let Some(spec) = spec_map.get(&spec_id) {
                    let key = PartitionKey::from_manifest_entry(candidate, spec);
                    if filter.is_fail_open(&key) {
                        filter.increment_fail_open_files();
                    }
                }
            }
        }

        if candidates.is_empty() {
            return Ok(RewriteDataFilesPlanningResult {
                plan: RewriteDataFilesPlan {
                    file_groups: vec![],
                    starting_sequence_number,
                    total_data_files: 0,
                    total_bytes: 0,
                    delete_tracker: None,
                },
                filter_stats: partition_filter.map(|f| f.stats()).unwrap_or_default(),
            });
        }

        // Step 6: Group by partition
        let partition_groups = self.group_by_partition(candidates)?;

        // Step 7: Bin-pack within partitions (deterministic order)
        let mut file_groups = self.bin_pack_partitions(partition_groups);

        // Step 8: Filter groups below thresholds
        file_groups = self.filter_valid_groups(file_groups);

        if file_groups.is_empty() {
            return Ok(RewriteDataFilesPlanningResult {
                plan: RewriteDataFilesPlan {
                    file_groups: vec![],
                    starting_sequence_number,
                    total_data_files: 0,
                    total_bytes: 0,
                    delete_tracker: None,
                },
                filter_stats: partition_filter.map(|f| f.stats()).unwrap_or_default(),
            });
        }

        // Step 9: Order groups
        file_groups = order_groups(file_groups, &self.options.rewrite_job_order);

        // Step 10: Load delete files into file groups (V2 only)
        // Also build delete tracker if remove_dangling_deletes is enabled
        let delete_tracker = if self.table.metadata().format_version() >= FormatVersion::V2 {
            self.load_delete_files(&mut file_groups).await?;

            // Build delete tracker for dangling delete removal
            if self.options.remove_dangling_deletes {
                Some(self.build_delete_tracker(&file_groups)?)
            } else {
                None
            }
        } else {
            None
        };

        // Compute totals
        let total_data_files: u64 = file_groups.iter().map(|g| g.file_count() as u64).sum();
        let total_bytes: u64 = file_groups.iter().map(|g| g.total_bytes).sum();

        Ok(RewriteDataFilesPlanningResult {
            plan: RewriteDataFilesPlan {
                file_groups,
                starting_sequence_number,
                total_data_files,
                total_bytes,
                delete_tracker,
            },
            filter_stats: partition_filter.map(|f| f.stats()).unwrap_or_default(),
        })
    }

    /// Execute the planning phase.
    ///
    /// This is a convenience method that delegates to [`Self::plan_with_stats`] and
    /// discards the filter diagnostics. Use `plan_with_stats` when you need
    /// visibility into partition filter evaluation behavior.
    ///
    /// # Returns
    ///
    /// A `RewriteDataFilesPlan` containing file groups to process,
    /// or an error if planning fails.
    pub async fn plan(&self) -> Result<RewriteDataFilesPlan> {
        self.plan_with_stats().await.map(|result| result.plan)
    }

    /// Build a delete tracker from the delete files loaded into file groups.
    ///
    /// Called when `remove_dangling_deletes` is enabled to track which data files
    /// each delete file references.
    ///
    /// # Errors
    ///
    /// Returns an error if any delete file references an unknown partition spec ID.
    fn build_delete_tracker(&self, file_groups: &[FileGroup]) -> Result<DeleteTracker> {
        // Build mapping of spec_id -> is_unpartitioned for all partition specs
        let spec_unpartitioned: HashMap<i32, bool> = self
            .table
            .metadata()
            .partition_specs_iter()
            .map(|spec| (spec.spec_id(), spec.is_unpartitioned()))
            .collect();

        let mut tracker = DeleteTracker::new(spec_unpartitioned);

        for group in file_groups {
            // Add position deletes
            for delete_entry in &group.position_delete_files {
                tracker.add_delete(delete_entry.clone())?;
            }
            // Add equality deletes
            for delete_entry in &group.equality_delete_files {
                tracker.add_delete(delete_entry.clone())?;
            }
        }

        Ok(tracker)
    }

    /// Load all data file entries from the current snapshot's manifests.
    async fn load_data_files(&self) -> Result<Vec<ManifestEntryRef>> {
        let snapshot = self
            .table
            .metadata()
            .current_snapshot()
            .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "No current snapshot"))?;

        let manifest_list = snapshot
            .load_manifest_list(self.table.file_io(), self.table.metadata())
            .await?;

        let mut entries = Vec::new();

        for manifest_file in manifest_list.entries() {
            // Only load data manifests
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }

            let manifest = manifest_file.load_manifest(self.table.file_io()).await?;

            for entry in manifest.entries() {
                // Only include live files (ADDED or EXISTING)
                // Ignore DELETED entries
                if entry.status == ManifestStatus::Deleted {
                    continue;
                }

                // Only include data files
                if entry.data_file.content_type() != DataContentType::Data {
                    continue;
                }

                entries.push(entry.clone());
            }
        }

        Ok(entries)
    }

    /// Filter entries by partition predicate using multi-spec evaluation.
    ///
    /// Creates a `MultiSpecPartitionFilter` from the partition filter predicate
    /// and uses it to filter entries. Each entry is evaluated against its own
    /// partition spec. Fail-open semantics apply for unevaluable specs.
    ///
    /// Returns the filtered entries and optionally the filter (for stats collection).
    fn filter_by_partition_internal(
        &self,
        entries: Vec<ManifestEntryRef>,
    ) -> Result<(Vec<ManifestEntryRef>, Option<MultiSpecPartitionFilter>)> {
        match &self.partition_filter {
            Some(predicate) => {
                // Build spec_id -> PartitionSpec map
                let spec_map: HashMap<i32, PartitionSpecRef> = self
                    .table
                    .metadata()
                    .partition_specs_iter()
                    .map(|spec| (spec.spec_id(), spec.clone()))
                    .collect();

                // Create multi-spec filter
                let filter = MultiSpecPartitionFilter::try_new(
                    predicate,
                    self.table.metadata(),
                    self.case_sensitive,
                )?;

                let filtered = filter_entries_with_multi_spec(entries, Some(&filter), &spec_map)?;

                Ok((filtered, Some(filter)))
            }
            None => Ok((entries, None)), // No filter, return all entries
        }
    }

    /// Select candidate files for rewriting based on size thresholds and delete count.
    ///
    /// If `delete_index` is provided and `delete_file_threshold` is set, files with
    /// enough associated deletes will be candidates even if their size is within range.
    ///
    /// # Errors
    ///
    /// Returns an error if any entry references an unknown partition spec ID.
    fn select_candidates(
        &self,
        entries: Vec<ManifestEntryRef>,
        delete_index: Option<&DeleteIndex>,
    ) -> Result<Vec<ManifestEntryRef>> {
        let mut candidates = Vec::new();
        for entry in entries {
            // delete_count_for_file derives is_unpartitioned from the entry's spec_id
            let delete_count = match delete_index {
                Some(idx) => idx.delete_count_for_file(&entry)?,
                None => 0,
            };
            if is_candidate_with_deletes(&entry, self.options, delete_count) {
                candidates.push(entry);
            }
        }
        Ok(candidates)
    }

    /// Load delete files into an index for threshold-based candidate selection.
    ///
    /// Only loads delete files when `delete_file_threshold` is set, as the index
    /// is only needed for delete-count-based candidate selection.
    async fn load_delete_index(&self) -> Result<Option<DeleteIndex>> {
        // Only load if delete threshold is configured
        if self.options.delete_file_threshold.is_none() {
            return Ok(None);
        }

        let snapshot = self
            .table
            .metadata()
            .current_snapshot()
            .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "No current snapshot"))?;

        let manifest_list = snapshot
            .load_manifest_list(self.table.file_io(), self.table.metadata())
            .await?;

        // Build mapping of spec_id -> is_unpartitioned for all partition specs
        let spec_unpartitioned: HashMap<i32, bool> = self
            .table
            .metadata()
            .partition_specs_iter()
            .map(|spec| (spec.spec_id(), spec.is_unpartitioned()))
            .collect();

        let mut index = DeleteIndex::new(spec_unpartitioned);

        for manifest_file in manifest_list.entries() {
            // Only process delete manifests
            if manifest_file.content != ManifestContentType::Deletes {
                continue;
            }

            let manifest = manifest_file.load_manifest(self.table.file_io()).await?;

            for entry in manifest.entries() {
                // Skip deleted entries
                if entry.status == ManifestStatus::Deleted {
                    continue;
                }

                index.add_delete(entry.clone())?;
            }
        }

        Ok(Some(index))
    }

    /// Group candidate files by their partition value.
    ///
    /// Derives `is_unpartitioned` per entry's spec_id to correctly handle tables
    /// with evolved partition specs.
    ///
    /// # Errors
    ///
    /// Returns an error if any entry references an unknown partition spec ID.
    fn group_by_partition(&self, entries: Vec<ManifestEntryRef>) -> Result<PartitionEntryGroups> {
        let mut groups: PartitionEntryGroups = HashMap::new();

        // Build mapping of spec_id -> is_unpartitioned for all partition specs
        let spec_unpartitioned: HashMap<i32, bool> = self
            .table
            .metadata()
            .partition_specs_iter()
            .map(|spec| (spec.spec_id(), spec.is_unpartitioned()))
            .collect();

        for entry in entries {
            let spec_id = entry.data_file.partition_spec_id;

            // Derive is_unpartitioned from the entry's spec_id, failing if unknown
            let is_unpartitioned = spec_unpartitioned.get(&spec_id).copied().ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Data file '{}' references unknown partition spec ID {spec_id}. \
                             This may indicate table metadata corruption.",
                        entry.data_file.file_path
                    ),
                )
            })?;

            let partition = if is_unpartitioned {
                None
            } else {
                Some(entry.data_file.partition().clone())
            };

            groups.entry((partition, spec_id)).or_default().push(entry);
        }

        Ok(groups)
    }

    /// Apply bin-packing to partition groups.
    ///
    /// Sorts partition groups by (spec_id, partition_repr) for deterministic
    /// iteration order to ensure group_id assignments are consistent across
    /// runs with the same input.
    fn bin_pack_partitions(
        &self,
        partition_groups: HashMap<(Option<Struct>, i32), Vec<ManifestEntryRef>>,
    ) -> Vec<FileGroup> {
        // Convert to a Vec and sort for deterministic ordering.
        // We use (spec_id, partition_repr) as the sort key where partition_repr
        // is the Debug string representation of the partition struct.
        // This is stable across process runs (unlike DefaultHasher which is randomly seeded).
        let mut sorted_entries: Vec<_> = partition_groups
            .into_iter()
            .map(|((partition, spec_id), entries)| {
                // Use partition's Debug representation for stable lexicographic ordering
                let partition_repr = partition
                    .as_ref()
                    .map(|p| format!("{p:?}"))
                    .unwrap_or_default();

                ((spec_id, partition_repr), partition, entries)
            })
            .collect();

        // Sort by (spec_id, partition_repr) for deterministic order
        // String comparison is stable across runs
        sorted_entries.sort_by(|a, b| a.0.cmp(&b.0));

        let mut all_groups = Vec::new();
        let mut group_id = 0u32;

        for ((spec_id, _repr), partition, entries) in sorted_entries {
            let groups = bin_pack_files(
                entries,
                partition,
                spec_id,
                self.options.max_file_group_size_bytes,
                group_id,
            );

            group_id += groups.len() as u32;
            all_groups.extend(groups);
        }

        all_groups
    }

    /// Filter out groups that don't meet minimum requirements.
    fn filter_valid_groups(&self, groups: Vec<FileGroup>) -> Vec<FileGroup> {
        groups
            .into_iter()
            .filter(|group| {
                // Must have minimum number of input files
                if group.file_count() >= self.options.min_input_files as usize {
                    return true;
                }

                // OR total size exceeds target (single large file that needs splitting)
                if group.total_bytes > self.options.target_file_size_bytes {
                    return true;
                }

                false
            })
            .collect()
    }

    /// Load delete files for file groups (V2 tables only).
    ///
    /// Associates delete files with file groups based on matching partition
    /// AND partition_spec_id to correctly handle partition evolution.
    async fn load_delete_files(&self, groups: &mut [FileGroup]) -> Result<()> {
        // Build a map of (partition, spec_id) -> group indices for efficient lookup
        let mut partition_to_indices: HashMap<(Option<Struct>, i32), Vec<usize>> = HashMap::new();

        for (idx, group) in groups.iter().enumerate() {
            partition_to_indices
                .entry((group.partition.clone(), group.partition_spec_id))
                .or_default()
                .push(idx);
        }

        // Build mapping of spec_id -> is_unpartitioned for all partition specs.
        // This must be derived from each entry's own spec_id to correctly handle
        // partition evolution where a table may contain both partitioned and
        // unpartitioned specs.
        let spec_unpartitioned: HashMap<i32, bool> = self
            .table
            .metadata()
            .partition_specs_iter()
            .map(|spec| (spec.spec_id(), spec.is_unpartitioned()))
            .collect();

        // Load delete manifests
        let snapshot = self
            .table
            .metadata()
            .current_snapshot()
            .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "No current snapshot"))?;

        let manifest_list = snapshot
            .load_manifest_list(self.table.file_io(), self.table.metadata())
            .await?;

        for manifest_file in manifest_list.entries() {
            // Only process delete manifests
            if manifest_file.content != ManifestContentType::Deletes {
                continue;
            }

            let manifest = manifest_file.load_manifest(self.table.file_io()).await?;

            for entry in manifest.entries() {
                // Skip deleted entries
                if entry.status == ManifestStatus::Deleted {
                    continue;
                }

                let partition = entry.data_file.partition();
                let spec_id = entry.data_file.partition_spec_id;
                let content_type = entry.data_file.content_type();

                // Find group indices with matching partition AND spec_id
                // For unpartitioned specs, delete files apply to all groups with matching spec_id.
                let is_unpartitioned =
                    spec_unpartitioned.get(&spec_id).copied().ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!(
                                "Delete file '{}' references unknown partition spec ID {}. \
                             This may indicate table metadata corruption.",
                                entry.data_file.file_path, spec_id
                            ),
                        )
                    })?;

                let lookup_key = if is_unpartitioned {
                    (None, spec_id)
                } else {
                    (Some(partition.clone()), spec_id)
                };

                if let Some(indices) = partition_to_indices.get(&lookup_key) {
                    for &idx in indices {
                        match content_type {
                            DataContentType::PositionDeletes => {
                                groups[idx].position_delete_files.push(entry.clone());
                            }
                            DataContentType::EqualityDeletes => {
                                groups[idx].equality_delete_files.push(entry.clone());
                            }
                            DataContentType::Data => {
                                // Skip data files in delete manifest (shouldn't happen)
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Delete Index for Threshold-based Candidate Selection
// ═══════════════════════════════════════════════════════════════════════════

/// Index of delete files for counting deletes per data file.
///
/// Used when `delete_file_threshold` is set to determine which data files
/// have too many associated delete files and should be compacted.
#[derive(Debug, Default)]
struct DeleteIndex {
    /// Position deletes indexed by referenced data file path.
    /// When a position delete has `referenced_data_file` set, it only applies
    /// to that specific file.
    position_deletes_by_file: HashMap<String, Vec<ManifestEntryRef>>,

    /// Position deletes indexed by (partition, spec_id).
    /// Position deletes without `referenced_data_file` apply to all files
    /// in the same partition.
    position_deletes_by_partition: HashMap<(Option<Struct>, i32), Vec<ManifestEntryRef>>,

    /// Equality deletes indexed by (partition, spec_id).
    /// Equality deletes apply to all files in the same partition.
    equality_deletes_by_partition: HashMap<(Option<Struct>, i32), Vec<ManifestEntryRef>>,

    /// Global equality deletes (unpartitioned).
    /// These apply to all data files regardless of partition.
    global_equality_deletes: Vec<ManifestEntryRef>,

    /// Mapping of partition spec ID to whether that spec is unpartitioned.
    /// Used to correctly derive partition keys for files from evolved tables
    /// with mixed partition specs.
    spec_unpartitioned: HashMap<i32, bool>,
}

impl DeleteIndex {
    /// Create a new delete index with spec unpartitioned mapping.
    ///
    /// # Arguments
    ///
    /// * `spec_unpartitioned` - Mapping of partition spec ID to whether that spec is unpartitioned.
    fn new(spec_unpartitioned: HashMap<i32, bool>) -> Self {
        Self {
            spec_unpartitioned,
            ..Default::default()
        }
    }

    /// Add a delete file entry to the index.
    ///
    /// Derives `is_unpartitioned` from the entry's partition spec ID to correctly
    /// handle tables with evolved partition specs.
    ///
    /// # Errors
    ///
    /// Returns an error if the entry references an unknown partition spec ID,
    /// which indicates table metadata corruption or inconsistency.
    fn add_delete(&mut self, entry: ManifestEntryRef) -> Result<()> {
        let content_type = entry.data_file.content_type();
        let spec_id = entry.data_file.partition_spec_id;

        // Derive is_unpartitioned from the entry's spec_id, failing if unknown
        let is_unpartitioned = self
            .spec_unpartitioned
            .get(&spec_id)
            .copied()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Delete file '{}' references unknown partition spec ID {}. \
                     This may indicate table metadata corruption.",
                        entry.data_file.file_path, spec_id
                    ),
                )
            })?;

        let partition = if is_unpartitioned {
            None
        } else {
            Some(entry.data_file.partition().clone())
        };

        match content_type {
            DataContentType::PositionDeletes => {
                // Check if this position delete references a specific file
                if let Some(ref_path) = entry.data_file.referenced_data_file() {
                    self.position_deletes_by_file
                        .entry(ref_path)
                        .or_default()
                        .push(entry);
                } else {
                    // No specific file reference - applies to all files in partition
                    self.position_deletes_by_partition
                        .entry((partition, spec_id))
                        .or_default()
                        .push(entry);
                }
            }
            DataContentType::EqualityDeletes => {
                // Equality deletes with empty partition are global
                if is_unpartitioned {
                    self.global_equality_deletes.push(entry);
                } else {
                    self.equality_deletes_by_partition
                        .entry((partition, spec_id))
                        .or_default()
                        .push(entry);
                }
            }
            DataContentType::Data => {
                // Data files shouldn't appear in delete manifests
            }
        }

        Ok(())
    }

    /// Get the count of delete files that affect a data file.
    ///
    /// Derives `is_unpartitioned` from the entry's partition spec ID to correctly
    /// handle tables with evolved partition specs.
    ///
    /// # Errors
    ///
    /// Returns an error if the entry references an unknown partition spec ID,
    /// which indicates table metadata corruption or inconsistency.
    fn delete_count_for_file(&self, entry: &ManifestEntryRef) -> Result<usize> {
        let file_path = &entry.data_file.file_path;
        let spec_id = entry.data_file.partition_spec_id;

        // Derive is_unpartitioned from the entry's spec_id, failing if unknown
        let is_unpartitioned = self
                .spec_unpartitioned
                .get(&spec_id)
                .copied()
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Data file '{file_path}' references unknown partition spec ID {spec_id}. This may indicate table metadata corruption."),
                    )
                })?;

        let partition = if is_unpartitioned {
            None
        } else {
            Some(entry.data_file.partition().clone())
        };

        let mut count = 0;

        // Position deletes referencing this specific file
        if let Some(deletes) = self.position_deletes_by_file.get(file_path) {
            count += deletes.len();
        }

        // Position deletes for this partition (not referencing specific files)
        if let Some(deletes) = self
            .position_deletes_by_partition
            .get(&(partition.clone(), spec_id))
        {
            count += deletes.len();
        }

        // Equality deletes for this partition
        if let Some(deletes) = self
            .equality_deletes_by_partition
            .get(&(partition, spec_id))
        {
            count += deletes.len();
        }

        // Global equality deletes (only for unpartitioned tables, but count anyway)
        count += self.global_equality_deletes.len();

        Ok(count)
    }
}

/// Check if a file is a candidate based on size OR delete count.
///
/// A file is a candidate if any of these conditions are true:
/// - Its size is below `min_file_size_bytes` (too small, needs compaction)
/// - Its size exceeds `max_file_size_bytes` (too large, needs splitting)
/// - Its delete count meets or exceeds `delete_file_threshold` (too many deletes)
fn is_candidate_with_deletes(
    entry: &ManifestEntryRef,
    options: &RewriteDataFilesOptions,
    delete_count: usize,
) -> bool {
    let size = entry.file_size_in_bytes();

    // Size-based: too small
    if size < options.min_file_size_bytes {
        return true;
    }

    // Size-based: too large (could be split)
    if size > options.max_file_size_bytes {
        return true;
    }

    // Delete-based: too many associated deletes
    if options
        .delete_file_threshold
        .is_some_and(|threshold| delete_count >= threshold as usize)
    {
        return true;
    }

    false
}

/// Filter entries using a multi-spec partition filter.
///
/// This helper evaluates each entry against the filter using its correct
/// partition spec. Fail-open semantics apply when evaluation isn't possible.
///
/// # Arguments
///
/// * `entries` - Manifest entries to filter
/// * `filter` - Multi-spec filter (if None, all entries pass through)
/// * `spec_map` - Mapping of spec_id to PartitionSpec for building PartitionKey
///
/// # Errors
///
/// Returns an error if any entry references an unknown partition spec ID.
fn filter_entries_with_multi_spec(
    entries: Vec<ManifestEntryRef>,
    filter: Option<&MultiSpecPartitionFilter>,
    spec_map: &HashMap<i32, PartitionSpecRef>,
) -> Result<Vec<ManifestEntryRef>> {
    let filter = match filter {
        None => return Ok(entries),
        Some(f) => f,
    };

    let mut filtered = Vec::with_capacity(entries.len());

    for entry in entries {
        let spec_id = entry.data_file.partition_spec_id;

        // Lookup the spec for this entry
        let spec = spec_map.get(&spec_id).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Data file '{}' references unknown partition spec ID {}. \
                     This may indicate table metadata corruption.",
                    entry.data_file.file_path, spec_id
                ),
            )
        })?;

        let key = PartitionKey::from_manifest_entry(&entry, spec);

        if filter.matches(&key)? {
            filtered.push(entry);
        }
    }

    Ok(filtered)
}

/// Filter entries by partition predicate (legacy single-spec version).
///
/// This is a helper function used by the planner to filter entries
/// before grouping. If no evaluator is provided, all entries pass through.
///
/// **Partition Evolution Safety:** When an entry's partition_spec_id doesn't match
/// the evaluator's spec_id, the entry is included by default (conservative behavior).
/// This ensures we don't incorrectly exclude entries from tables with evolved partitions.
#[allow(dead_code)] // Kept for backward compatibility, superseded by filter_entries_with_multi_spec
fn filter_entries_by_partition(
    entries: Vec<ManifestEntryRef>,
    evaluator: Option<&PartitionFilterEvaluator>,
) -> Result<Vec<ManifestEntryRef>> {
    match evaluator {
        None => Ok(entries),
        Some(eval) => {
            let mut filtered = Vec::with_capacity(entries.len());
            for entry in entries {
                // Check if this entry's spec_id matches the evaluator's spec
                // If not, include the entry by default (conservative behavior)
                if !eval.can_evaluate(entry.data_file.partition_spec_id) {
                    // Entry has a different partition spec - include it to be safe
                    filtered.push(entry);
                    continue;
                }

                let partition = entry.data_file.partition();
                if eval.matches(partition)? {
                    filtered.push(entry);
                }
            }
            Ok(filtered)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::expr::Reference;
    use crate::spec::{
        DataContentType, DataFile, DataFileFormat, Datum, Literal, ManifestEntry, ManifestStatus,
        NestedField, PartitionSpec, PrimitiveType, Schema, Transform, Type, UnboundPartitionField,
    };

    // Note: Full integration tests for the planner require a complete table setup
    // with manifests, which is complex. These tests focus on the plan struct and
    // document expected behavior. See integration tests for end-to-end testing.

    fn create_test_date_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "date",
                Type::Primitive(PrimitiveType::Date),
            ))])
            .build()
            .unwrap()
    }

    fn create_test_date_partition_spec(schema: &Schema) -> PartitionSpec {
        PartitionSpec::builder(schema.clone())
            .with_spec_id(1)
            .add_unbound_field(
                UnboundPartitionField::builder()
                    .source_id(1)
                    .name("date".to_string())
                    .field_id(1000)
                    .transform(Transform::Identity)
                    .build(),
            )
            .unwrap()
            .build()
            .unwrap()
    }

    fn test_entry_with_partition(path: &str, size: u64, partition: Struct) -> ManifestEntryRef {
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
                file_size_in_bytes: size,
                column_sizes: std::collections::HashMap::new(),
                value_counts: std::collections::HashMap::new(),
                null_value_counts: std::collections::HashMap::new(),
                nan_value_counts: std::collections::HashMap::new(),
                lower_bounds: std::collections::HashMap::new(),
                upper_bounds: std::collections::HashMap::new(),
                key_metadata: None,
                split_offsets: vec![],
                equality_ids: None,
                sort_order_id: None,
                partition_spec_id: 1,
                first_row_id: None,
                referenced_data_file: None,
                content_offset: None,
                content_size_in_bytes: None,
            },
        })
    }

    #[test]
    fn test_filter_by_partition_with_predicate() {
        let schema = create_test_date_schema();
        let partition_spec = create_test_date_partition_spec(&schema);

        // Create mock entries with different partitions
        // 2024-01-15 (days since epoch = 19737)
        let entry1 = test_entry_with_partition(
            "file1.parquet",
            50_000,
            Struct::from_iter([Some(Literal::date(19737))]),
        );
        // 2024-01-16 (days since epoch = 19738)
        let entry2 = test_entry_with_partition(
            "file2.parquet",
            50_000,
            Struct::from_iter([Some(Literal::date(19738))]),
        );

        let entries = vec![entry1, entry2];

        // Filter for date = '2024-01-15'
        let predicate =
            Reference::new("date").equal_to(Datum::date_from_str("2024-01-15").unwrap());

        let evaluator = PartitionFilterEvaluator::try_new(
            &predicate,
            &Arc::new(schema),
            &Arc::new(partition_spec),
            true,
        )
        .unwrap();

        let filtered = filter_entries_by_partition(entries, Some(&evaluator)).unwrap();

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].data_file.file_path, "file1.parquet");
    }

    #[test]
    fn test_filter_by_partition_without_evaluator() {
        // When no evaluator is provided, all entries should pass through
        let entry1 = test_entry_with_partition(
            "file1.parquet",
            50_000,
            Struct::from_iter([Some(Literal::date(19737))]),
        );
        let entry2 = test_entry_with_partition(
            "file2.parquet",
            50_000,
            Struct::from_iter([Some(Literal::date(19738))]),
        );

        let entries = vec![entry1, entry2];
        let filtered = filter_entries_by_partition(entries, None).unwrap();

        assert_eq!(filtered.len(), 2);
    }

    fn test_entry_with_partition_and_spec(
        path: &str,
        size: u64,
        partition: Struct,
        spec_id: i32,
    ) -> ManifestEntryRef {
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
                file_size_in_bytes: size,
                column_sizes: std::collections::HashMap::new(),
                value_counts: std::collections::HashMap::new(),
                null_value_counts: std::collections::HashMap::new(),
                nan_value_counts: std::collections::HashMap::new(),
                lower_bounds: std::collections::HashMap::new(),
                upper_bounds: std::collections::HashMap::new(),
                key_metadata: None,
                split_offsets: vec![],
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
    fn test_filter_by_partition_with_evolved_spec_includes_different_specs() {
        // Test that entries with a different spec_id are included (not filtered out)
        // This is conservative behavior to handle partition evolution safely
        let schema = create_test_date_schema();
        let partition_spec = create_test_date_partition_spec(&schema);

        // Entry matching spec_id=1 and partition date 2024-01-15 (should match filter)
        let entry1 = test_entry_with_partition_and_spec(
            "file1.parquet",
            50_000,
            Struct::from_iter([Some(Literal::date(19737))]),
            1, // Matches the evaluator's spec
        );
        // Entry matching spec_id=1 but partition date 2024-01-16 (should NOT match filter)
        let entry2 = test_entry_with_partition_and_spec(
            "file2.parquet",
            50_000,
            Struct::from_iter([Some(Literal::date(19738))]),
            1, // Matches the evaluator's spec
        );
        // Entry with different spec_id=2 (should be INCLUDED regardless of partition value)
        let entry3 = test_entry_with_partition_and_spec(
            "file3.parquet",
            50_000,
            Struct::from_iter([Some(Literal::date(19738))]), // Different date
            2, // Different spec - can't evaluate, so include
        );

        let entries = vec![entry1, entry2, entry3];

        // Filter for date = '2024-01-15'
        let predicate =
            Reference::new("date").equal_to(Datum::date_from_str("2024-01-15").unwrap());

        let evaluator = PartitionFilterEvaluator::try_new(
            &predicate,
            &Arc::new(schema),
            &Arc::new(partition_spec),
            true,
        )
        .unwrap();

        let filtered = filter_entries_by_partition(entries, Some(&evaluator)).unwrap();

        // Should include:
        // - file1 (matches spec and filter)
        // - file3 (different spec, included conservatively)
        // Should exclude:
        // - file2 (matches spec but doesn't match filter)
        assert_eq!(filtered.len(), 2);
        let paths: Vec<&str> = filtered
            .iter()
            .map(|e| e.data_file.file_path.as_str())
            .collect();
        assert!(paths.contains(&"file1.parquet"));
        assert!(paths.contains(&"file3.parquet"));
        assert!(!paths.contains(&"file2.parquet"));
    }

    #[test]
    fn test_rewrite_plan_empty() {
        let plan = RewriteDataFilesPlan {
            file_groups: vec![],
            starting_sequence_number: 0,
            total_data_files: 0,
            total_bytes: 0,
            delete_tracker: None,
        };

        assert!(plan.file_groups.is_empty());
        assert_eq!(plan.total_data_files, 0);
        assert_eq!(plan.total_bytes, 0);
    }

    #[test]
    fn test_rewrite_plan_with_files() {
        let plan = RewriteDataFilesPlan {
            file_groups: vec![], // Would contain actual FileGroups in real usage
            starting_sequence_number: 42,
            total_data_files: 100,
            total_bytes: 1024 * 1024 * 1024, // 1 GB
            delete_tracker: None,
        };

        assert_eq!(plan.starting_sequence_number, 42);
        assert_eq!(plan.total_data_files, 100);
        assert_eq!(plan.total_bytes, 1024 * 1024 * 1024);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Delete-aware Candidate Selection Tests
    // ═══════════════════════════════════════════════════════════════════════

    fn test_entry(path: &str, size: u64) -> ManifestEntryRef {
        Arc::new(ManifestEntry {
            status: ManifestStatus::Added,
            snapshot_id: Some(1),
            sequence_number: Some(1),
            file_sequence_number: Some(1),
            data_file: DataFile {
                content: DataContentType::Data,
                file_path: path.to_string(),
                file_format: DataFileFormat::Parquet,
                partition: Struct::empty(),
                record_count: 1000,
                file_size_in_bytes: size,
                column_sizes: std::collections::HashMap::new(),
                value_counts: std::collections::HashMap::new(),
                null_value_counts: std::collections::HashMap::new(),
                nan_value_counts: std::collections::HashMap::new(),
                lower_bounds: std::collections::HashMap::new(),
                upper_bounds: std::collections::HashMap::new(),
                key_metadata: None,
                split_offsets: vec![],
                equality_ids: None,
                sort_order_id: None,
                partition_spec_id: 1,
                first_row_id: None,
                referenced_data_file: None,
                content_offset: None,
                content_size_in_bytes: None,
            },
        })
    }

    #[test]
    fn test_is_candidate_with_delete_threshold() {
        let options = RewriteDataFilesOptions {
            delete_file_threshold: Some(3),
            ..Default::default()
        };

        // File is the right size (not a size-based candidate)
        // Default min = 384MB, max = 921.6MB, so 400MB is within range
        let entry = test_entry("file1.parquet", 400 * 1024 * 1024);

        // Without delete info, not a candidate
        assert!(!is_candidate_with_deletes(&entry, &options, 0));

        // With 2 deletes (below threshold), not a candidate
        assert!(!is_candidate_with_deletes(&entry, &options, 2));

        // With 3 deletes (at threshold), IS a candidate
        assert!(is_candidate_with_deletes(&entry, &options, 3));

        // With 5 deletes (above threshold), IS a candidate
        assert!(is_candidate_with_deletes(&entry, &options, 5));
    }

    #[test]
    fn test_is_candidate_size_based() {
        let options = RewriteDataFilesOptions::default();
        // Default: min = 384MB, max = 921.6MB

        // Too small - is a candidate
        let small_entry = test_entry("small.parquet", 100 * 1024 * 1024); // 100MB
        assert!(is_candidate_with_deletes(&small_entry, &options, 0));

        // Right size - not a candidate
        let right_size_entry = test_entry("right.parquet", 400 * 1024 * 1024); // 400MB
        assert!(!is_candidate_with_deletes(&right_size_entry, &options, 0));

        // Too large - is a candidate
        let large_entry = test_entry("large.parquet", 1024 * 1024 * 1024); // 1GB
        assert!(is_candidate_with_deletes(&large_entry, &options, 0));
    }

    #[test]
    fn test_is_candidate_without_delete_threshold() {
        let options = RewriteDataFilesOptions::default();
        // delete_file_threshold is None by default

        // Right-sized file with lots of deletes - NOT a candidate (threshold disabled)
        let entry = test_entry("file1.parquet", 400 * 1024 * 1024);
        assert!(!is_candidate_with_deletes(&entry, &options, 100));
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Delete Index Tests
    // ═══════════════════════════════════════════════════════════════════════

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
                split_offsets: vec![],
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

    /// Create spec_unpartitioned mapping for a partitioned table (spec_id 1 is partitioned).
    fn partitioned_spec_mapping() -> HashMap<i32, bool> {
        HashMap::from([(1, false)])
    }

    #[test]
    fn test_delete_index_position_delete_by_file() {
        let mut index = DeleteIndex::new(partitioned_spec_mapping());

        // Position delete referencing specific file
        let pos_delete = test_delete_entry(
            "delete1.parquet",
            DataContentType::PositionDeletes,
            Struct::from_iter([Some(Literal::date(19737))]),
            Some("data1.parquet"),
        );
        index.add_delete(pos_delete).unwrap();

        // Data file that is referenced
        let data_entry = test_entry_with_partition(
            "data1.parquet",
            100 * 1024 * 1024,
            Struct::from_iter([Some(Literal::date(19737))]),
        );
        assert_eq!(index.delete_count_for_file(&data_entry).unwrap(), 1);

        // Data file that is NOT referenced
        let other_entry = test_entry_with_partition(
            "data2.parquet",
            100 * 1024 * 1024,
            Struct::from_iter([Some(Literal::date(19737))]),
        );
        assert_eq!(index.delete_count_for_file(&other_entry).unwrap(), 0);
    }

    #[test]
    fn test_delete_index_position_delete_by_partition() {
        let mut index = DeleteIndex::new(partitioned_spec_mapping());

        // Position delete without referenced file - applies to all in partition
        let pos_delete = test_delete_entry(
            "delete1.parquet",
            DataContentType::PositionDeletes,
            Struct::from_iter([Some(Literal::date(19737))]),
            None, // No referenced file
        );
        index.add_delete(pos_delete).unwrap();

        // Data file in same partition
        let data_entry = test_entry_with_partition(
            "data1.parquet",
            100 * 1024 * 1024,
            Struct::from_iter([Some(Literal::date(19737))]),
        );
        assert_eq!(index.delete_count_for_file(&data_entry).unwrap(), 1);

        // Data file in different partition
        let other_entry = test_entry_with_partition(
            "data2.parquet",
            100 * 1024 * 1024,
            Struct::from_iter([Some(Literal::date(19738))]), // Different partition
        );
        assert_eq!(index.delete_count_for_file(&other_entry).unwrap(), 0);
    }

    #[test]
    fn test_delete_index_equality_delete() {
        let mut index = DeleteIndex::new(partitioned_spec_mapping());

        // Equality delete - applies to all files in partition
        let eq_delete = test_delete_entry(
            "delete1.parquet",
            DataContentType::EqualityDeletes,
            Struct::from_iter([Some(Literal::date(19737))]),
            None,
        );
        index.add_delete(eq_delete).unwrap();

        // Data file in same partition
        let data_entry = test_entry_with_partition(
            "data1.parquet",
            100 * 1024 * 1024,
            Struct::from_iter([Some(Literal::date(19737))]),
        );
        assert_eq!(index.delete_count_for_file(&data_entry).unwrap(), 1);

        // Data file in different partition
        let other_entry = test_entry_with_partition(
            "data2.parquet",
            100 * 1024 * 1024,
            Struct::from_iter([Some(Literal::date(19738))]),
        );
        assert_eq!(index.delete_count_for_file(&other_entry).unwrap(), 0);
    }

    #[test]
    fn test_delete_index_combined_deletes() {
        let mut index = DeleteIndex::new(partitioned_spec_mapping());

        let partition = Struct::from_iter([Some(Literal::date(19737))]);

        // Add multiple types of deletes
        // 1. Position delete referencing specific file
        let pos_delete1 = test_delete_entry(
            "pos_delete1.parquet",
            DataContentType::PositionDeletes,
            partition.clone(),
            Some("data1.parquet"),
        );
        index.add_delete(pos_delete1).unwrap();

        // 2. Position delete for partition
        let pos_delete2 = test_delete_entry(
            "pos_delete2.parquet",
            DataContentType::PositionDeletes,
            partition.clone(),
            None,
        );
        index.add_delete(pos_delete2).unwrap();

        // 3. Equality delete for partition
        let eq_delete = test_delete_entry(
            "eq_delete.parquet",
            DataContentType::EqualityDeletes,
            partition.clone(),
            None,
        );
        index.add_delete(eq_delete).unwrap();

        // Data file should have all 3 deletes
        let data_entry = test_entry_with_partition("data1.parquet", 100 * 1024 * 1024, partition);
        assert_eq!(index.delete_count_for_file(&data_entry).unwrap(), 3);
    }

    #[test]
    fn test_delete_index_rejects_unknown_spec_id() {
        // Empty spec mapping - no specs registered
        let mut index = DeleteIndex::new(HashMap::new());

        // Delete file with spec_id 1 (not in mapping)
        let pos_delete = test_delete_entry(
            "delete1.parquet",
            DataContentType::PositionDeletes,
            Struct::from_iter([Some(Literal::date(19737))]),
            Some("data1.parquet"),
        );

        let result = index.add_delete(pos_delete);

        // Should fail because spec_id 1 is not in the mapping
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("unknown partition spec ID 1"));
    }

    #[test]
    fn test_delete_index_delete_count_rejects_unknown_spec_id() {
        let index = DeleteIndex::new(HashMap::new());

        // Data file with spec_id 1 (not in mapping)
        let data_entry = test_entry_with_partition(
            "data1.parquet",
            100 * 1024 * 1024,
            Struct::from_iter([Some(Literal::date(19737))]),
        );

        let result = index.delete_count_for_file(&data_entry);

        // Should fail because spec_id 1 is not in the mapping
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("unknown partition spec ID 1"));
    }

    // Integration tests for the planner require a table with manifests.
    // See crates/iceberg/tests/ for comprehensive integration tests.
}
