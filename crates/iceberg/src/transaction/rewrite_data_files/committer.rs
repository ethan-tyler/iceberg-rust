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

//! Commit logic for the rewrite data files action.
//!
//! The committer takes completed file groups (after execution) and produces
//! the snapshot commit that atomically replaces old files with new files.

use std::collections::HashMap;

use uuid::Uuid;

use crate::error::{Error, ErrorKind, Result};
use crate::spec::{DataFile, ManifestEntry, ManifestFile, Operation};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::ActionCommit;

use super::file_group::FileGroup;
use super::planner::RewriteDataFilesPlan;
use super::result::RewriteDataFilesResult;

/// Result of rewriting a single file group.
#[derive(Debug, Clone)]
pub struct FileGroupRewriteResult {
    /// The group ID that was rewritten.
    pub group_id: u32,

    /// New data files produced by rewriting this group.
    ///
    /// These files contain the compacted data from all input files
    /// with any position/equality deletes applied.
    pub new_data_files: Vec<DataFile>,
}

/// Committer for the rewrite data files action.
///
/// The committer handles the final phase of compaction: taking the rewritten
/// files and producing an atomic snapshot that replaces old files with new.
///
/// # Usage
///
/// ```ignore
/// // 1. Plan the compaction
/// let plan = planner.plan().await?;
///
/// // 2. Execute rewrites (using your query engine)
/// let mut results = Vec::new();
/// for group in &plan.file_groups {
///     let new_files = execute_rewrite(group).await?;
///     results.push(FileGroupRewriteResult {
///         group_id: group.group_id,
///         new_data_files: new_files,
///     });
/// }
///
/// // 3. Commit all changes atomically
/// let committer = RewriteDataFilesCommitter::new(&table, plan);
/// let action_commit = committer.commit(results).await?;
/// ```
pub struct RewriteDataFilesCommitter<'a> {
    table: &'a Table,
    plan: RewriteDataFilesPlan,
    commit_uuid: Uuid,
    snapshot_properties: HashMap<String, String>,
}

impl<'a> RewriteDataFilesCommitter<'a> {
    /// Create a new committer for the given plan.
    pub fn new(table: &'a Table, plan: RewriteDataFilesPlan) -> Self {
        Self {
            table,
            plan,
            commit_uuid: Uuid::now_v7(),
            snapshot_properties: HashMap::new(),
        }
    }

    /// Set the commit UUID.
    #[must_use]
    pub fn with_commit_uuid(mut self, uuid: Uuid) -> Self {
        self.commit_uuid = uuid;
        self
    }

    /// Set snapshot summary properties.
    #[must_use]
    pub fn with_snapshot_properties(mut self, properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = properties;
        self
    }

    /// Plan commit batches for partial progress mode.
    ///
    /// Divides file groups into roughly equal batches based on `max_commits`.
    /// Returns a vector of batch assignments, where each batch contains the
    /// group IDs to be committed together.
    ///
    /// # Arguments
    ///
    /// * `max_commits` - Maximum number of commits to make
    ///
    /// # Returns
    ///
    /// Vector of batches, where each batch is a vector of group IDs.
    pub fn plan_batches(&self, max_commits: u32) -> Vec<Vec<u32>> {
        if self.plan.file_groups.is_empty() || max_commits == 0 {
            return vec![];
        }

        let num_groups = self.plan.file_groups.len();
        let groups_per_batch = (num_groups as f64 / max_commits as f64).ceil() as usize;
        let groups_per_batch = groups_per_batch.max(1);

        self.plan
            .file_groups
            .chunks(groups_per_batch)
            .map(|chunk| chunk.iter().map(|g| g.group_id).collect())
            .collect()
    }

    /// Commit a batch of file group results (partial progress mode).
    ///
    /// This method is used when `partial_progress_enabled` is true to commit
    /// groups incrementally. Unlike `commit()`, this method:
    ///
    /// - Only validates and commits the specified groups
    /// - Does not require all planned groups to have results
    /// - Returns successfully even if other groups are pending
    ///
    /// # Arguments
    ///
    /// * `results` - Results for the groups in this batch
    /// * `batch_group_ids` - Group IDs that should be in this batch
    ///
    /// # Workflow
    ///
    /// ```ignore
    /// let batches = committer.plan_batches(options.partial_progress_max_commits);
    ///
    /// for (batch_idx, batch_group_ids) in batches.iter().enumerate() {
    ///     // Execute this batch's groups
    ///     let batch_results = execute_groups(&plan, batch_group_ids).await?;
    ///
    ///     // Commit this batch
    ///     let (commit, result) = committer.commit_batch(batch_results, batch_group_ids).await?;
    ///
    ///     // Apply commit to catalog
    ///     catalog.update_table(commit).await?;
    ///
    ///     // Refresh table reference for next batch
    ///     table = catalog.load_table(&table_id).await?;
    ///     committer = RewriteDataFilesCommitter::new(&table, plan.clone());
    /// }
    /// ```
    ///
    /// # Note
    ///
    /// After committing a batch, the table reference is stale. The caller must
    /// refresh the table from the catalog before committing the next batch.
    pub async fn commit_batch(
        &self,
        results: Vec<FileGroupRewriteResult>,
        batch_group_ids: &[u32],
    ) -> Result<(ActionCommit, RewriteDataFilesResult)> {
        // Validate that results match the batch
        self.validate_batch_results(&results, batch_group_ids)?;

        // Collect files to delete and add for this batch only
        let (files_to_delete, files_to_add) = self.collect_batch_file_changes(&results, batch_group_ids)?;

        // If nothing changed, return empty commit
        if files_to_delete.is_empty() && files_to_add.is_empty() {
            return Ok((
                ActionCommit::new(vec![], vec![]),
                RewriteDataFilesResult::empty(),
            ));
        }

        // Build statistics for result
        let rewritten_data_files_count = files_to_delete.len() as u64;
        let added_data_files_count = files_to_add.len() as u64;
        let rewritten_bytes: u64 = files_to_delete
            .iter()
            .map(|e| e.file_size_in_bytes())
            .sum();
        let added_bytes: u64 = files_to_add
            .iter()
            .map(|f| f.file_size_in_bytes)
            .sum();

        // Find dangling delete files for this batch
        let dangling_deletes = self.find_dangling_deletes(&files_to_delete);
        let removed_delete_files_count = dangling_deletes.len() as u64;

        // Validate new files
        self.validate_new_data_files(&files_to_add)?;

        // Create snapshot producer with a new UUID for this batch
        let batch_uuid = Uuid::now_v7();
        let snapshot_producer = SnapshotProducer::new(
            self.table,
            batch_uuid,
            None,
            self.snapshot_properties.clone(),
            files_to_add,
            vec![],
        );

        // Create the operation
        let operation = ReplaceDataFilesOperation {
            files_to_delete,
            delete_files_to_remove: dangling_deletes,
            starting_sequence_number: self.plan.starting_sequence_number,
        };

        // Commit
        let action_commit = snapshot_producer
            .commit(operation, DefaultManifestProcess)
            .await?;

        let result = RewriteDataFilesResult {
            rewritten_data_files_count,
            added_data_files_count,
            rewritten_bytes,
            added_bytes,
            file_groups_count: batch_group_ids.len() as u64,
            failed_file_groups_count: 0,
            file_group_results: vec![],
            failed_file_groups: vec![],
            removed_delete_files_count,
            duration_ms: 0,
            commits_count: 1, // This batch = 1 commit
        };

        Ok((action_commit, result))
    }

    /// Validate that batch results match the expected batch groups.
    fn validate_batch_results(
        &self,
        results: &[FileGroupRewriteResult],
        batch_group_ids: &[u32],
    ) -> Result<()> {
        let expected_ids: std::collections::HashSet<u32> = batch_group_ids.iter().copied().collect();
        let result_ids: std::collections::HashSet<u32> = results.iter().map(|r| r.group_id).collect();

        // Check for missing groups
        let missing: Vec<u32> = expected_ids.difference(&result_ids).copied().collect();
        if !missing.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Missing rewrite results for batch groups: {missing:?}. \
                     All groups in batch must have results."
                ),
            ));
        }

        // Check for unexpected groups
        let unexpected: Vec<u32> = result_ids.difference(&expected_ids).copied().collect();
        if !unexpected.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Unexpected rewrite results for groups: {unexpected:?}. \
                     These groups were not in this batch."
                ),
            ));
        }

        Ok(())
    }

    /// Collect file changes for a specific batch of groups.
    fn collect_batch_file_changes(
        &self,
        results: &[FileGroupRewriteResult],
        batch_group_ids: &[u32],
    ) -> Result<(Vec<ManifestEntry>, Vec<DataFile>)> {
        let batch_ids: std::collections::HashSet<u32> = batch_group_ids.iter().copied().collect();

        // Collect files to delete from the batch's file groups
        let mut files_to_delete = Vec::new();
        for group in &self.plan.file_groups {
            if batch_ids.contains(&group.group_id) {
                for entry in &group.data_files {
                    files_to_delete.push(entry.as_ref().clone());
                }
            }
        }

        // Collect all new files from batch results
        let mut files_to_add = Vec::new();
        for result in results {
            files_to_add.extend(result.new_data_files.iter().cloned());
        }

        Ok((files_to_delete, files_to_add))
    }

    /// Commit the rewrite results, producing an atomic snapshot.
    ///
    /// This method:
    /// 1. Validates that all planned groups have results
    /// 2. Collects all files to delete (from plan) and add (from results)
    /// 3. Identifies dangling delete files (if `remove_dangling_deletes` was enabled)
    /// 4. Produces a single atomic snapshot with the replacement
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Results don't match planned groups
    /// - No files were rewritten
    /// - Snapshot production fails
    pub async fn commit(
        self,
        results: Vec<FileGroupRewriteResult>,
    ) -> Result<(ActionCommit, RewriteDataFilesResult)> {
        // Validate results match plan
        self.validate_results(&results)?;

        // Collect files to delete and add
        let (files_to_delete, files_to_add) = self.collect_file_changes(&results)?;

        // If nothing changed, return empty commit
        if files_to_delete.is_empty() && files_to_add.is_empty() {
            return Ok((
                ActionCommit::new(vec![], vec![]),
                RewriteDataFilesResult::empty(),
            ));
        }

        // Build statistics for result
        let rewritten_data_files_count = files_to_delete.len() as u64;
        let added_data_files_count = files_to_add.len() as u64;
        let rewritten_bytes: u64 = files_to_delete
            .iter()
            .map(|e| e.file_size_in_bytes())
            .sum();
        let added_bytes: u64 = files_to_add
            .iter()
            .map(|f| f.file_size_in_bytes)
            .sum();

        // Find dangling delete files (if delete tracker is available)
        let dangling_deletes = self.find_dangling_deletes(&files_to_delete);
        let removed_delete_files_count = dangling_deletes.len() as u64;

        // Validate new files before creating snapshot producer
        // Note: We use custom validation instead of SnapshotProducer::validate_added_data_files()
        // because that method enforces files MUST use the default partition spec, which is too
        // restrictive for compaction. Compaction can process files from evolved partition specs.
        self.validate_new_data_files(&files_to_add)?;

        // Create snapshot producer
        let snapshot_producer = SnapshotProducer::new(
            self.table,
            self.commit_uuid,
            None, // key_metadata
            self.snapshot_properties,
            files_to_add,
            vec![], // No delete files (position/equality deletes were applied during rewrite)
        );

        // Create the operation with files to delete (data files + dangling delete files)
        let operation = ReplaceDataFilesOperation {
            files_to_delete,
            delete_files_to_remove: dangling_deletes,
            starting_sequence_number: self.plan.starting_sequence_number,
        };

        // Commit
        let action_commit = snapshot_producer
            .commit(operation, DefaultManifestProcess)
            .await?;

        let result = RewriteDataFilesResult {
            rewritten_data_files_count,
            added_data_files_count,
            rewritten_bytes,
            added_bytes,
            file_groups_count: self.plan.file_groups.len() as u64,
            failed_file_groups_count: 0,
            file_group_results: vec![], // Per-group results populated by execution engine
            failed_file_groups: vec![],
            removed_delete_files_count,
            duration_ms: 0,   // Caller should time the operation
            commits_count: 1, // Single commit (partial progress not yet implemented)
        };

        Ok((action_commit, result))
    }

    /// Find dangling delete files that can be removed after compaction.
    ///
    /// A delete file is "dangling" when all data files it references have been
    /// rewritten (compacted). Position deletes with `referenced_data_file` set
    /// are candidates; partition-scoped deletes are more complex and not handled.
    fn find_dangling_deletes(&self, files_to_delete: &[ManifestEntry]) -> Vec<ManifestEntry> {
        // If no delete tracker, dangling delete removal wasn't enabled
        let Some(ref tracker) = self.plan.delete_tracker else {
            return vec![];
        };

        // Build set of data file paths being rewritten
        let rewritten_files: std::collections::HashSet<String> = files_to_delete
            .iter()
            .map(|e| e.file_path().to_string())
            .collect();

        // For simplicity, we assume all rewritten files are gone and no files remain
        // in the affected partitions. This is a conservative assumption for the initial
        // implementation - in practice, we would need to load remaining files from
        // manifests not touched by this compaction.
        //
        // The remaining_files set would contain data files that still exist in the
        // table after compaction. Since we don't have access to that information
        // here without additional manifest scanning, we use an empty set.
        let remaining_files: std::collections::HashSet<String> = std::collections::HashSet::new();

        // Find position deletes whose referenced files are all being rewritten
        let dangling_refs = tracker.find_dangling_position_deletes(&rewritten_files, &remaining_files);

        // Convert ManifestEntryRef to ManifestEntry
        dangling_refs
            .into_iter()
            .map(|entry_ref| entry_ref.as_ref().clone())
            .collect()
    }

    /// Validate that results match the plan.
    fn validate_results(&self, results: &[FileGroupRewriteResult]) -> Result<()> {
        // Check for duplicate group IDs in results
        let mut seen_ids: std::collections::HashSet<u32> = std::collections::HashSet::new();
        let mut duplicates: Vec<u32> = Vec::new();
        for result in results {
            if !seen_ids.insert(result.group_id) {
                duplicates.push(result.group_id);
            }
        }
        if !duplicates.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Duplicate rewrite results for groups: {:?}. \
                     Each group must have exactly one result.",
                    duplicates
                ),
            ));
        }

        // Build a set of expected group IDs
        let expected_ids: std::collections::HashSet<u32> = self
            .plan
            .file_groups
            .iter()
            .map(|g| g.group_id)
            .collect();

        // Check for missing groups
        let missing: Vec<u32> = expected_ids.difference(&seen_ids).copied().collect();
        if !missing.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Missing rewrite results for groups: {:?}. \
                     All planned groups must have results.",
                    missing
                ),
            ));
        }

        // Check for unexpected groups
        let unexpected: Vec<u32> = seen_ids.difference(&expected_ids).copied().collect();
        if !unexpected.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Unexpected rewrite results for groups: {:?}. \
                     These groups were not in the plan.",
                    unexpected
                ),
            ));
        }

        Ok(())
    }

    /// Collect files to delete (from plan) and add (from results).
    fn collect_file_changes(
        &self,
        results: &[FileGroupRewriteResult],
    ) -> Result<(Vec<ManifestEntry>, Vec<DataFile>)> {
        // Collect all files to delete from the plan's file groups
        let mut files_to_delete = Vec::new();
        for group in &self.plan.file_groups {
            for entry in &group.data_files {
                // Convert to ManifestEntry for deletion
                // The entry already has the correct status from the manifest
                files_to_delete.push(entry.as_ref().clone());
            }
        }

        // Collect all new files from results
        let mut files_to_add = Vec::new();
        for result in results {
            files_to_add.extend(result.new_data_files.iter().cloned());
        }

        Ok((files_to_delete, files_to_add))
    }

    /// Validate new data files for compaction.
    ///
    /// Unlike `SnapshotProducer::validate_added_data_files()`, this allows files
    /// with any valid partition spec (not just the default), since compaction
    /// can process files from evolved partition specs.
    fn validate_new_data_files(&self, files: &[DataFile]) -> Result<()> {
        use crate::spec::DataContentType;

        for file in files {
            // Must be data content type
            if file.content_type() != DataContentType::Data {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Compacted file must have Data content type, got {:?}: {}",
                        file.content_type(),
                        file.file_path
                    ),
                ));
            }

            // Partition spec must exist in the table
            let partition_spec = self
                .table
                .metadata()
                .partition_spec_by_id(file.partition_spec_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Compacted file references unknown partition spec id {}: {}",
                            file.partition_spec_id, file.file_path
                        ),
                    )
                })?;

            // Find a compatible schema for this partition spec
            // (may be historical for evolved tables)
            let partition_type = self
                .find_compatible_partition_type(&partition_spec)
                .map_err(|e| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Failed to compute partition type for spec {}: {}",
                            file.partition_spec_id, e
                        ),
                    )
                })?;

            // Validate partition value field count
            if file.partition.fields().len() != partition_type.fields().len() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Compacted file partition value has {} fields but spec {} requires {}: {}",
                        file.partition.fields().len(),
                        file.partition_spec_id,
                        partition_type.fields().len(),
                        file.file_path
                    ),
                ));
            }

            // Validate partition value type compatibility
            for (value, field) in file.partition.fields().iter().zip(partition_type.fields()) {
                let field_type = field.field_type.as_primitive_type().ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        format!(
                            "Partition field {} should be primitive type: {}",
                            field.name, file.file_path
                        ),
                    )
                })?;

                if let Some(literal) = value {
                    let prim_literal = literal.as_primitive_literal().ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!(
                                "Partition value for field {} is not a primitive literal: {}",
                                field.name, file.file_path
                            ),
                        )
                    })?;

                    if !field_type.compatible(&prim_literal) {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!(
                                "Partition value for field {} is not compatible with type {:?}: {}",
                                field.name, field_type, file.file_path
                            ),
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    /// Find a partition type compatible with the given partition spec.
    ///
    /// For evolved tables, old partition specs may reference fields not in the
    /// current schema. This method searches historical schemas to find one
    /// that can compute the partition type.
    fn find_compatible_partition_type(
        &self,
        partition_spec: &crate::spec::PartitionSpecRef,
    ) -> Result<crate::spec::StructType> {
        // Try current schema first
        let current_schema = self.table.metadata().current_schema();
        if let Ok(partition_type) = partition_spec.partition_type(current_schema) {
            return Ok(partition_type);
        }

        // Search historical schemas
        for schema in self.table.metadata().schemas_iter() {
            if let Ok(partition_type) = partition_spec.partition_type(schema) {
                return Ok(partition_type);
            }
        }

        Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Cannot compute partition type for spec {}: no schema contains all referenced fields",
                partition_spec.spec_id()
            ),
        ))
    }
}

/// Operation for replacing data files during compaction.
///
/// This operation marks the old files as DELETED and adds the new files.
/// It uses Operation::Replace to indicate files are being replaced.
struct ReplaceDataFilesOperation {
    /// Data files to mark as deleted.
    files_to_delete: Vec<ManifestEntry>,
    /// Delete files to remove (dangling deletes after compaction).
    delete_files_to_remove: Vec<ManifestEntry>,
    /// Starting sequence number from plan time.
    /// Used to set data_sequence_number on new manifest entries for MoR correctness.
    starting_sequence_number: i64,
}

impl SnapshotProduceOperation for ReplaceDataFilesOperation {
    fn operation(&self) -> Operation {
        // Use Replace operation to indicate compaction/rewrite
        Operation::Replace
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        // Return entries to mark as deleted (both data files and dangling delete files)
        // The status will be set to DELETED by the snapshot producer
        let mut entries = self.files_to_delete.clone();
        entries.extend(self.delete_files_to_remove.iter().cloned());
        Ok(entries)
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        // Get all manifests from current snapshot
        let Some(snapshot) = snapshot_produce.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.table.file_io(),
                &snapshot_produce.table.metadata_ref(),
            )
            .await?;

        // Filter out manifests that contain ONLY files we're deleting
        // We need to be careful here: if a manifest contains files we're keeping
        // AND files we're deleting, we need special handling (existing_entries)
        //
        // For now, keep all existing manifests. The deleted files will be
        // written to a new manifest with DELETED status. Iceberg readers
        // correctly handle the interplay between Added/Existing/Deleted entries.
        //
        // A more optimized implementation could:
        // 1. Identify manifests that only contain files being deleted
        // 2. Skip those manifests entirely (don't include in existing_manifest)
        // 3. Handle mixed manifests by splitting entries
        //
        // This optimization is left for a future improvement.
        Ok(manifest_list.entries().to_vec())
    }

    fn data_sequence_number(&self) -> Option<i64> {
        // Return the starting sequence number from when the compaction was planned.
        // This ensures that equality deletes written after planning will still apply
        // to the compacted files, maintaining MoR (Merge-on-Read) correctness.
        Some(self.starting_sequence_number)
    }
}

/// Execute a single file group rewrite.
///
/// This is a helper that orchestrates reading data files, applying deletes,
/// and writing new files. It requires a file writer implementation.
///
/// **Note**: This is a placeholder for the execution engine. The actual
/// implementation requires integration with a query engine (like DataFusion)
/// or custom Parquet reading/writing code.
///
/// For now, users should implement their own execution logic using the
/// planned file groups and commit the results using `RewriteDataFilesCommitter`.
#[allow(dead_code)]
pub async fn execute_file_group_rewrite(
    _table: &Table,
    _group: &FileGroup,
    _target_file_size: u64,
) -> Result<Vec<DataFile>> {
    // Placeholder for Phase 2.1.3 execution engine
    //
    // A full implementation would:
    // 1. Read all data files in the group using Arrow readers
    // 2. Apply position delete files (filter out deleted rows)
    // 3. Apply equality delete files (filter matching rows)
    // 4. Write new Parquet files at target size
    // 5. Return the new DataFile metadata
    //
    // This requires:
    // - Arrow Parquet reader integration
    // - Delete file application logic
    // - Arrow Parquet writer with size limits
    //
    // For now, this returns an error indicating execution is not implemented.
    Err(Error::new(
        ErrorKind::FeatureUnsupported,
        "Built-in file group execution is not yet implemented. \
         Use a query engine (like DataFusion) to read the files from \
         the planned groups, apply deletes, and write new files. \
         Then commit the results using RewriteDataFilesCommitter.",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, ManifestEntry, ManifestStatus,
        Struct,
    };

    /// Helper to create a test manifest entry
    fn test_entry(file_path: &str, size_bytes: u64, record_count: u64) -> Arc<ManifestEntry> {
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

    /// Helper to create a test data file
    fn test_data_file(file_path: &str, size_bytes: u64, record_count: u64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(file_path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(size_bytes)
            .record_count(record_count)
            .partition(Struct::from_iter([Some(Literal::long(0))]))
            .partition_spec_id(0)
            .build()
            .unwrap()
    }

    #[test]
    fn test_file_group_rewrite_result() {
        let result = FileGroupRewriteResult {
            group_id: 0,
            new_data_files: vec![test_data_file("new/file1.parquet", 100_000, 1000)],
        };

        assert_eq!(result.group_id, 0);
        assert_eq!(result.new_data_files.len(), 1);
    }

    #[test]
    fn test_validate_results_missing_groups() {
        use crate::io::FileIOBuilder;
        use crate::spec::TableMetadata;
        use crate::table::Table;
        use crate::TableIdent;
        use std::fs::File;
        use std::io::BufReader;

        // Create a test table
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2ValidMinimal.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let metadata = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        let table = Table::builder()
            .metadata(metadata)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap();

        // Create a plan with two groups
        let mut group0 = super::super::file_group::FileGroup::new(0, None, 0);
        group0.add_data_file(test_entry("file0.parquet", 50_000, 500));

        let mut group1 = super::super::file_group::FileGroup::new(1, None, 0);
        group1.add_data_file(test_entry("file1.parquet", 50_000, 500));

        let plan = RewriteDataFilesPlan {
            file_groups: vec![group0, group1],
            starting_sequence_number: 1,
            total_data_files: 2,
            total_bytes: 100_000,
            delete_tracker: None,
        };

        let committer = RewriteDataFilesCommitter::new(&table, plan);

        // Only provide result for group 0 (missing group 1)
        let results = vec![FileGroupRewriteResult {
            group_id: 0,
            new_data_files: vec![test_data_file("new/file0.parquet", 100_000, 1000)],
        }];

        let err = committer.validate_results(&results).unwrap_err();
        assert!(err.message().contains("Missing rewrite results"));
        assert!(err.message().contains("1"));
    }

    #[test]
    fn test_validate_results_unexpected_groups() {
        use crate::io::FileIOBuilder;
        use crate::spec::TableMetadata;
        use crate::table::Table;
        use crate::TableIdent;
        use std::fs::File;
        use std::io::BufReader;

        // Create a test table
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2ValidMinimal.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let metadata = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        let table = Table::builder()
            .metadata(metadata)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap();

        // Create a plan with one group
        let mut group0 = super::super::file_group::FileGroup::new(0, None, 0);
        group0.add_data_file(test_entry("file0.parquet", 50_000, 500));

        let plan = RewriteDataFilesPlan {
            file_groups: vec![group0],
            starting_sequence_number: 1,
            total_data_files: 1,
            total_bytes: 50_000,
            delete_tracker: None,
        };

        let committer = RewriteDataFilesCommitter::new(&table, plan);

        // Provide results for group 0 and an unexpected group 99
        let results = vec![
            FileGroupRewriteResult {
                group_id: 0,
                new_data_files: vec![test_data_file("new/file0.parquet", 100_000, 1000)],
            },
            FileGroupRewriteResult {
                group_id: 99,
                new_data_files: vec![test_data_file("new/file99.parquet", 100_000, 1000)],
            },
        ];

        let err = committer.validate_results(&results).unwrap_err();
        assert!(err.message().contains("Unexpected rewrite results"));
        assert!(err.message().contains("99"));
    }

    #[test]
    fn test_collect_file_changes() {
        use crate::io::FileIOBuilder;
        use crate::spec::TableMetadata;
        use crate::table::Table;
        use crate::TableIdent;
        use std::fs::File;
        use std::io::BufReader;

        // Create a test table
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2ValidMinimal.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let metadata = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        let table = Table::builder()
            .metadata(metadata)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap();

        // Create a plan with files to delete
        let mut group0 = super::super::file_group::FileGroup::new(0, None, 0);
        group0.add_data_file(test_entry("old/file1.parquet", 30_000, 300));
        group0.add_data_file(test_entry("old/file2.parquet", 20_000, 200));

        let plan = RewriteDataFilesPlan {
            file_groups: vec![group0],
            starting_sequence_number: 1,
            total_data_files: 2,
            total_bytes: 50_000,
            delete_tracker: None,
        };

        let committer = RewriteDataFilesCommitter::new(&table, plan);

        // Results with new files
        let results = vec![FileGroupRewriteResult {
            group_id: 0,
            new_data_files: vec![test_data_file("new/compacted.parquet", 50_000, 500)],
        }];

        let (files_to_delete, files_to_add) = committer.collect_file_changes(&results).unwrap();

        // Should have 2 files to delete
        assert_eq!(files_to_delete.len(), 2);
        assert!(files_to_delete
            .iter()
            .any(|e| e.file_path() == "old/file1.parquet"));
        assert!(files_to_delete
            .iter()
            .any(|e| e.file_path() == "old/file2.parquet"));

        // Should have 1 file to add
        assert_eq!(files_to_add.len(), 1);
        assert_eq!(files_to_add[0].file_path, "new/compacted.parquet");
    }

    #[test]
    fn test_replace_operation_type() {
        let operation = ReplaceDataFilesOperation {
            files_to_delete: vec![],
            delete_files_to_remove: vec![],
            starting_sequence_number: 1,
        };

        assert_eq!(operation.operation(), Operation::Replace);
    }

    #[test]
    fn test_validate_new_data_files_rejects_delete_content() {
        use crate::io::FileIOBuilder;
        use crate::spec::TableMetadata;
        use crate::table::Table;
        use crate::TableIdent;
        use std::fs::File;
        use std::io::BufReader;

        // Create a test table
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2ValidMinimal.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let metadata = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        let table = Table::builder()
            .metadata(metadata)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap();

        let plan = RewriteDataFilesPlan {
            file_groups: vec![],
            starting_sequence_number: 1,
            total_data_files: 0,
            total_bytes: 0,
            delete_tracker: None,
        };

        let committer = RewriteDataFilesCommitter::new(&table, plan);

        // Create a delete file (wrong content type)
        let delete_file = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/delete.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition(Struct::from_iter([Some(Literal::long(0))]))
            .partition_spec_id(0)
            .build()
            .unwrap();

        let err = committer
            .validate_new_data_files(&[delete_file])
            .unwrap_err();
        assert!(err.message().contains("Data content type"));
    }

    #[test]
    fn test_validate_new_data_files_accepts_valid_file() {
        use crate::io::FileIOBuilder;
        use crate::spec::TableMetadata;
        use crate::table::Table;
        use crate::TableIdent;
        use std::fs::File;
        use std::io::BufReader;

        // Create a test table
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2ValidMinimal.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let metadata = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        let table = Table::builder()
            .metadata(metadata)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap();

        let plan = RewriteDataFilesPlan {
            file_groups: vec![],
            starting_sequence_number: 1,
            total_data_files: 0,
            total_bytes: 0,
            delete_tracker: None,
        };

        let committer = RewriteDataFilesCommitter::new(&table, plan);

        // Create a valid data file
        let data_file = test_data_file("test/data.parquet", 100_000, 1000);

        // Should succeed
        committer.validate_new_data_files(&[data_file]).unwrap();
    }

    #[test]
    fn test_validate_results_rejects_duplicate_groups() {
        use crate::io::FileIOBuilder;
        use crate::spec::TableMetadata;
        use crate::table::Table;
        use crate::TableIdent;
        use std::fs::File;
        use std::io::BufReader;

        // Create a test table
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2ValidMinimal.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let metadata = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        let table = Table::builder()
            .metadata(metadata)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap();

        // Create a plan with one group
        let mut group0 = super::super::file_group::FileGroup::new(0, None, 0);
        group0.add_data_file(test_entry("file0.parquet", 50_000, 500));

        let plan = RewriteDataFilesPlan {
            file_groups: vec![group0],
            starting_sequence_number: 1,
            total_data_files: 1,
            total_bytes: 50_000,
            delete_tracker: None,
        };

        let committer = RewriteDataFilesCommitter::new(&table, plan);

        // Provide duplicate results for group 0
        let results = vec![
            FileGroupRewriteResult {
                group_id: 0,
                new_data_files: vec![test_data_file("new/file0a.parquet", 50_000, 500)],
            },
            FileGroupRewriteResult {
                group_id: 0, // Duplicate!
                new_data_files: vec![test_data_file("new/file0b.parquet", 50_000, 500)],
            },
        ];

        let err = committer.validate_results(&results).unwrap_err();
        assert!(err.message().contains("Duplicate rewrite results"));
        assert!(err.message().contains("0"));
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Batch Commit Tests
    // ═══════════════════════════════════════════════════════════════════════

    #[test]
    fn test_plan_batches_empty() {
        use crate::io::FileIOBuilder;
        use crate::spec::TableMetadata;
        use crate::table::Table;
        use crate::TableIdent;
        use std::fs::File;
        use std::io::BufReader;

        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2ValidMinimal.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let metadata = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        let table = Table::builder()
            .metadata(metadata)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap();

        let plan = RewriteDataFilesPlan {
            file_groups: vec![],
            starting_sequence_number: 1,
            total_data_files: 0,
            total_bytes: 0,
            delete_tracker: None,
        };

        let committer = RewriteDataFilesCommitter::new(&table, plan);
        let batches = committer.plan_batches(5);
        assert!(batches.is_empty());
    }

    #[test]
    fn test_plan_batches_single_batch() {
        use crate::io::FileIOBuilder;
        use crate::spec::TableMetadata;
        use crate::table::Table;
        use crate::TableIdent;
        use std::fs::File;
        use std::io::BufReader;

        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2ValidMinimal.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let metadata = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        let table = Table::builder()
            .metadata(metadata)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap();

        // 3 groups, max 10 commits -> all in one batch
        let mut group0 = super::super::file_group::FileGroup::new(0, None, 0);
        group0.add_data_file(test_entry("file0.parquet", 50_000, 500));
        let mut group1 = super::super::file_group::FileGroup::new(1, None, 0);
        group1.add_data_file(test_entry("file1.parquet", 50_000, 500));
        let mut group2 = super::super::file_group::FileGroup::new(2, None, 0);
        group2.add_data_file(test_entry("file2.parquet", 50_000, 500));

        let plan = RewriteDataFilesPlan {
            file_groups: vec![group0, group1, group2],
            starting_sequence_number: 1,
            total_data_files: 3,
            total_bytes: 150_000,
            delete_tracker: None,
        };

        let committer = RewriteDataFilesCommitter::new(&table, plan);
        let batches = committer.plan_batches(10);

        // With 3 groups and max 10 commits, each group gets its own batch
        // (ceil(3/10) = 1 group per batch)
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0], vec![0]);
        assert_eq!(batches[1], vec![1]);
        assert_eq!(batches[2], vec![2]);
    }

    #[test]
    fn test_plan_batches_multiple_batches() {
        use crate::io::FileIOBuilder;
        use crate::spec::TableMetadata;
        use crate::table::Table;
        use crate::TableIdent;
        use std::fs::File;
        use std::io::BufReader;

        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2ValidMinimal.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let metadata = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        let table = Table::builder()
            .metadata(metadata)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap();

        // 6 groups, max 3 commits -> 2 groups per batch
        let groups: Vec<_> = (0..6)
            .map(|i| {
                let mut g = super::super::file_group::FileGroup::new(i, None, 0);
                g.add_data_file(test_entry(&format!("file{i}.parquet"), 50_000, 500));
                g
            })
            .collect();

        let plan = RewriteDataFilesPlan {
            file_groups: groups,
            starting_sequence_number: 1,
            total_data_files: 6,
            total_bytes: 300_000,
            delete_tracker: None,
        };

        let committer = RewriteDataFilesCommitter::new(&table, plan);
        let batches = committer.plan_batches(3);

        // With 6 groups and max 3 commits, should get 3 batches of 2 groups each
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0], vec![0, 1]);
        assert_eq!(batches[1], vec![2, 3]);
        assert_eq!(batches[2], vec![4, 5]);
    }

    #[test]
    fn test_validate_batch_results_missing_group() {
        use crate::io::FileIOBuilder;
        use crate::spec::TableMetadata;
        use crate::table::Table;
        use crate::TableIdent;
        use std::fs::File;
        use std::io::BufReader;

        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2ValidMinimal.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let metadata = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        let table = Table::builder()
            .metadata(metadata)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap();

        let mut group0 = super::super::file_group::FileGroup::new(0, None, 0);
        group0.add_data_file(test_entry("file0.parquet", 50_000, 500));
        let mut group1 = super::super::file_group::FileGroup::new(1, None, 0);
        group1.add_data_file(test_entry("file1.parquet", 50_000, 500));

        let plan = RewriteDataFilesPlan {
            file_groups: vec![group0, group1],
            starting_sequence_number: 1,
            total_data_files: 2,
            total_bytes: 100_000,
            delete_tracker: None,
        };

        let committer = RewriteDataFilesCommitter::new(&table, plan);

        // Only provide result for group 0, but batch expects both 0 and 1
        let results = vec![FileGroupRewriteResult {
            group_id: 0,
            new_data_files: vec![test_data_file("new/file0.parquet", 50_000, 500)],
        }];

        let err = committer
            .validate_batch_results(&results, &[0, 1])
            .unwrap_err();
        assert!(err.message().contains("Missing rewrite results"));
    }

    #[test]
    fn test_validate_batch_results_unexpected_group() {
        use crate::io::FileIOBuilder;
        use crate::spec::TableMetadata;
        use crate::table::Table;
        use crate::TableIdent;
        use std::fs::File;
        use std::io::BufReader;

        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2ValidMinimal.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let metadata = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        let table = Table::builder()
            .metadata(metadata)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap();

        let mut group0 = super::super::file_group::FileGroup::new(0, None, 0);
        group0.add_data_file(test_entry("file0.parquet", 50_000, 500));

        let plan = RewriteDataFilesPlan {
            file_groups: vec![group0],
            starting_sequence_number: 1,
            total_data_files: 1,
            total_bytes: 50_000,
            delete_tracker: None,
        };

        let committer = RewriteDataFilesCommitter::new(&table, plan);

        // Provide result for groups 0 and 99, but batch only expects 0
        let results = vec![
            FileGroupRewriteResult {
                group_id: 0,
                new_data_files: vec![test_data_file("new/file0.parquet", 50_000, 500)],
            },
            FileGroupRewriteResult {
                group_id: 99, // Not in batch
                new_data_files: vec![test_data_file("new/file99.parquet", 50_000, 500)],
            },
        ];

        let err = committer
            .validate_batch_results(&results, &[0])
            .unwrap_err();
        assert!(err.message().contains("Unexpected rewrite results"));
    }

    #[test]
    fn test_collect_batch_file_changes() {
        use crate::io::FileIOBuilder;
        use crate::spec::TableMetadata;
        use crate::table::Table;
        use crate::TableIdent;
        use std::fs::File;
        use std::io::BufReader;

        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2ValidMinimal.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let metadata = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        let table = Table::builder()
            .metadata(metadata)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap();

        // Two groups with files
        let mut group0 = super::super::file_group::FileGroup::new(0, None, 0);
        group0.add_data_file(test_entry("old/file0.parquet", 30_000, 300));
        let mut group1 = super::super::file_group::FileGroup::new(1, None, 0);
        group1.add_data_file(test_entry("old/file1.parquet", 20_000, 200));

        let plan = RewriteDataFilesPlan {
            file_groups: vec![group0, group1],
            starting_sequence_number: 1,
            total_data_files: 2,
            total_bytes: 50_000,
            delete_tracker: None,
        };

        let committer = RewriteDataFilesCommitter::new(&table, plan);

        // Result for group 0 only (batch contains only group 0)
        let results = vec![FileGroupRewriteResult {
            group_id: 0,
            new_data_files: vec![test_data_file("new/compacted0.parquet", 30_000, 300)],
        }];

        let (files_to_delete, files_to_add) =
            committer.collect_batch_file_changes(&results, &[0]).unwrap();

        // Only files from group 0
        assert_eq!(files_to_delete.len(), 1);
        assert_eq!(files_to_delete[0].file_path(), "old/file0.parquet");

        assert_eq!(files_to_add.len(), 1);
        assert_eq!(files_to_add[0].file_path, "new/compacted0.parquet");
    }
}
