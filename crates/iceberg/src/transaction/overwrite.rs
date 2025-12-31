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

//! This module provides `OverwriteAction` for filter-based data replacement.
//!
//! `OverwriteAction` implements INSERT OVERWRITE semantics where files are
//! deleted based on an explicit row filter predicate. Unlike `ReplacePartitionsAction`
//! where partitions are dynamically determined from added files, `OverwriteAction`
//! takes an explicit filter that determines which existing data to delete.
//!
//! # Use Cases
//!
//! - INSERT OVERWRITE with explicit WHERE clause
//! - Targeted data correction/replacement
//! - Conditional bulk delete operations
//!
//! # Example
//!
//! ```ignore
//! use iceberg::expr::Reference;
//! use iceberg::transaction::{Transaction, ApplyTransactionAction};
//!
//! let tx = Transaction::new(&table);
//! // Delete all data where date = '2024-01-01' and add new files
//! let filter = Reference::new("date").equal_to(Datum::string("2024-01-01"));
//! let action = tx.overwrite()
//!     .overwrite_filter(filter)
//!     .add_data_files(vec![new_file_for_jan_01]);
//! let tx = action.apply(tx)?;
//! let table = tx.commit(&catalog).await?;
//! ```
//!
//! # Filter Semantics
//!
//! The overwrite filter is a **row-level predicate** that specifies which data
//! to delete. It is projected to partition predicates using `InclusiveProjection`,
//! meaning:
//! - Files whose partition values **could contain** matching rows are deleted
//! - This is a conservative (safe) approach - it may delete more files than strictly necessary
//!
//! For example, if your filter is `date = '2024-01-01'` on a table partitioned by month:
//! - All files in the `2024-01` partition will be deleted (since they could contain Jan 1st data)
//!
//! # Atomicity
//!
//! The operation is atomic: files are only deleted on successful commit.
//! On failure, no files are deleted.
//!
//! # Conflict Detection
//!
//! For concurrent operations, you can enable conflict detection to ensure the
//! data being overwritten hasn't been modified since your read snapshot:
//!
//! ```ignore
//! let action = tx.overwrite()
//!     .overwrite_filter(filter)
//!     .add_data_files(new_files)
//!     .validate_from_snapshot(read_snapshot_id)  // Required for conflict checks
//!     .validate_no_conflicting_data()
//!     .validate_no_conflicting_deletes();
//! ```
//!
//! **Important**: Conflict checks are only performed when `validate_from_snapshot()`
//! is called. Without it, `validate_no_conflicting_data()` and
//! `validate_no_conflicting_deletes()` will cause an error at commit time.
//!
//! # Partition Evolution
//!
//! The overwrite filter is projected against every partition spec referenced by
//! manifests in the current snapshot. Files written with older partition specs
//! are still evaluated and deleted when their partitions could match the filter,
//! preventing stale data after partition evolution.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::{Error, ErrorKind, Result};
use crate::expr::visitors::expression_evaluator::ExpressionEvaluator;
use crate::expr::visitors::inclusive_projection::InclusiveProjection;
use crate::expr::{Bind, BoundPredicate, Predicate};
use crate::spec::{
    DataFile, ManifestContentType, ManifestEntry, ManifestFile, ManifestStatus, Operation,
    PartitionSpecRef, Schema,
};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};

/// Action for filter-based data overwrite.
///
/// Files matching the overwrite filter are deleted and replaced with new files.
/// The filter is a row-level predicate that is projected to partition predicates.
///
/// This action is commonly used for:
/// - INSERT OVERWRITE operations with explicit WHERE clause
/// - Targeted data correction where you know exactly which rows to replace
/// - Conditional delete-and-replace operations
///
/// # Filter Projection
///
/// The row filter is projected to a partition filter using `InclusiveProjection`.
/// This means files are deleted if their partition **could contain** rows matching
/// the filter, which is a safe (conservative) approach.
///
/// # Conflict Detection
///
/// For concurrent operations, you can enable conflict detection:
///
/// ```ignore
/// let action = tx.overwrite()
///     .overwrite_filter(filter)
///     .add_data_files(new_files)
///     .validate_from_snapshot(read_snapshot_id)
///     .validate_no_conflicting_data()
///     .validate_no_conflicting_deletes();
/// ```
pub struct OverwriteAction {
    /// The row filter that determines which files to delete
    overwrite_filter: Option<Predicate>,

    /// Data files to add
    added_data_files: Vec<DataFile>,

    /// Whether to check for duplicate files
    check_duplicate: bool,

    /// Whether to use case-sensitive matching for filter binding
    case_sensitive: bool,

    /// Optional commit UUID
    commit_uuid: Option<Uuid>,

    /// Optional key metadata for manifest files
    key_metadata: Option<Vec<u8>>,

    /// Snapshot summary properties
    snapshot_properties: HashMap<String, String>,

    /// Snapshot ID to validate conflicts from (for optimistic concurrency)
    validate_from_snapshot_id: Option<i64>,

    /// Whether to validate no conflicting data was added since validate_from_snapshot
    validate_no_conflicting_data: bool,

    /// Whether to validate no conflicting deletes were added since validate_from_snapshot
    validate_no_conflicting_deletes: bool,

    /// Whether to validate that added files match the overwrite filter (idempotency check)
    validate_added_files_match_filter: bool,
}

impl OverwriteAction {
    pub(crate) fn new() -> Self {
        Self {
            overwrite_filter: None,
            added_data_files: Vec::new(),
            check_duplicate: true,
            case_sensitive: true,
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            validate_from_snapshot_id: None,
            validate_no_conflicting_data: false,
            validate_no_conflicting_deletes: false,
            validate_added_files_match_filter: false,
        }
    }

    /// Set the row filter for determining which files to delete.
    ///
    /// The filter is a row-level predicate that will be projected to partition
    /// predicates. Files whose partitions could contain matching rows will be deleted.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Delete all data where date = '2024-01-01'
    /// let filter = Reference::new("date").equal_to(Datum::string("2024-01-01"));
    /// let action = tx.overwrite().overwrite_filter(filter);
    /// ```
    pub fn overwrite_filter(mut self, filter: Predicate) -> Self {
        self.overwrite_filter = Some(filter);
        self
    }

    /// Add a data file to the table.
    pub fn add_data_file(mut self, file: DataFile) -> Self {
        self.added_data_files.push(file);
        self
    }

    /// Add multiple data files to the table.
    pub fn add_data_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(files);
        self
    }

    /// Set whether to check for duplicate files.
    pub fn with_check_duplicate(mut self, check: bool) -> Self {
        self.check_duplicate = check;
        self
    }

    /// Set whether to use case-sensitive matching when binding the filter.
    pub fn with_case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.case_sensitive = case_sensitive;
        self
    }

    /// Set commit UUID for the snapshot.
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set key metadata for manifest files.
    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }

    /// Set snapshot summary properties.
    pub fn set_snapshot_properties(mut self, properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = properties;
        self
    }

    /// Set the snapshot ID to validate conflicts from.
    ///
    /// When conflict detection is enabled, this specifies the "read snapshot" -
    /// the point in time when your query read the table. Any concurrent modifications
    /// to files matching the filter since this snapshot will cause a conflict.
    pub fn validate_from_snapshot(mut self, snapshot_id: i64) -> Self {
        self.validate_from_snapshot_id = Some(snapshot_id);
        self
    }

    /// Enable validation that no conflicting data files were added.
    ///
    /// When enabled, the commit will fail if any data files were added to
    /// partitions matching the filter since the snapshot set by `validate_from_snapshot()`.
    pub fn validate_no_conflicting_data(mut self) -> Self {
        self.validate_no_conflicting_data = true;
        self
    }

    /// Enable validation that no conflicting delete files were added.
    ///
    /// When enabled, the commit will fail if any delete files were added
    /// targeting partitions matching the filter since the snapshot set by
    /// `validate_from_snapshot()`.
    pub fn validate_no_conflicting_deletes(mut self) -> Self {
        self.validate_no_conflicting_deletes = true;
        self
    }

    /// Enable validation that added files match the overwrite filter.
    ///
    /// When enabled, the commit will fail if any added files have partition
    /// values that don't match the overwrite filter. This is an idempotency
    /// check that catches user errors where files are added to partitions
    /// outside the overwrite scope.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Overwrite data where x = 100
    /// let filter = Reference::new("x").equal_to(Datum::long(100));
    /// let action = tx.overwrite()
    ///     .overwrite_filter(filter)
    ///     .add_data_file(file_for_x_100)  // OK
    ///     .validate_added_files_match_filter();  // Will fail if file is not in x=100
    /// ```
    pub fn validate_added_files_match_filter(mut self) -> Self {
        self.validate_added_files_match_filter = true;
        self
    }

    /// Bind the row filter to the schema and project it to a partition filter.
    fn create_partition_filter(
        &self,
        schema: &Arc<Schema>,
        partition_spec: &PartitionSpecRef,
        case_sensitive: bool,
    ) -> Result<BoundPredicate> {
        let filter = self.overwrite_filter.as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Overwrite filter is required for OverwriteAction",
            )
        })?;

        // Bind the row filter to the schema
        let bound_filter = filter.bind(schema.clone(), case_sensitive)?;

        // Project to partition filter using InclusiveProjection
        let partition_type = partition_spec.partition_type(schema)?;
        let partition_fields = partition_type.fields().to_owned();
        let partition_schema = Arc::new(
            Schema::builder()
                .with_schema_id(partition_spec.spec_id())
                .with_fields(partition_fields)
                .build()?,
        );

        let mut inclusive_projection = InclusiveProjection::new(partition_spec.clone());
        let projected_filter = inclusive_projection.project(&bound_filter)?;

        // Rewrite NOT and bind to partition schema
        projected_filter
            .rewrite_not()
            .bind(partition_schema, case_sensitive)
    }

    /// Create partition filters for all partition specs in the table.
    ///
    /// The row filter is bound once per spec and projected using that spec's
    /// transforms so that manifests written under older specs can be evaluated
    /// safely.
    fn create_partition_filters_for_all_specs(
        &self,
        table: &Table,
        schema: &Arc<Schema>,
        case_sensitive: bool,
    ) -> Result<HashMap<i32, BoundPredicate>> {
        let mut filters = HashMap::new();
        for spec in table.metadata().partition_specs_iter() {
            let spec_id = spec.spec_id();
            let filter = self.create_partition_filter(schema, spec, case_sensitive)?;
            filters.insert(spec_id, filter);
        }
        Ok(filters)
    }

    /// Find all snapshots between start_snapshot_id and current_snapshot_id (exclusive of start).
    fn find_snapshots_since(
        table: &Table,
        start_snapshot_id: i64,
        current_snapshot_id: Option<i64>,
    ) -> Result<Vec<i64>> {
        let Some(current_id) = current_snapshot_id else {
            return Ok(vec![]);
        };

        if start_snapshot_id == current_id {
            return Ok(vec![]);
        }

        let mut snapshot_ids = Vec::new();
        let mut current = current_id;

        loop {
            if current == start_snapshot_id {
                break;
            }

            snapshot_ids.push(current);

            let snapshot = table.metadata().snapshot_by_id(current).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Snapshot {current} not found in table metadata"),
                )
            })?;

            match snapshot.parent_snapshot_id() {
                Some(parent_id) => {
                    current = parent_id;
                }
                None => {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "validate_from_snapshot {start_snapshot_id} is not an ancestor of current snapshot {current_id}"
                        ),
                    ));
                }
            }
        }

        snapshot_ids.reverse();
        Ok(snapshot_ids)
    }

    /// Validate that no conflicting data files were added in the given snapshots.
    async fn validate_no_conflicting_data_in_snapshots(
        table: &Table,
        snapshot_ids: &[i64],
        partition_filters: &HashMap<i32, BoundPredicate>,
    ) -> Result<()> {
        for &snapshot_id in snapshot_ids {
            let snapshot = table
                .metadata()
                .snapshot_by_id(snapshot_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Snapshot {snapshot_id} not found"),
                    )
                })?;

            let manifest_list = snapshot
                .load_manifest_list(table.file_io(), &table.metadata_ref())
                .await?;

            for manifest_file in manifest_list.entries() {
                if manifest_file.content != ManifestContentType::Data {
                    continue;
                }

                let spec_id = manifest_file.partition_spec_id;
                let partition_filter = partition_filters.get(&spec_id).ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Manifest '{}' references partition spec id {} which does not exist in table metadata",
                            manifest_file.manifest_path, spec_id
                        ),
                    )
                })?;
                let evaluator = ExpressionEvaluator::new(partition_filter.clone());

                let manifest = manifest_file.load_manifest(table.file_io()).await?;

                for entry in manifest.entries() {
                    if entry.status() != ManifestStatus::Added {
                        continue;
                    }
                    if entry.snapshot_id() != Some(snapshot_id) {
                        continue;
                    }

                    // Check if file matches the overwrite filter
                    if evaluator.eval(entry.data_file())? {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!(
                                "Conflicting data file added in snapshot {snapshot_id}: {} in partition {:?}",
                                entry.data_file().file_path(),
                                entry.data_file().partition()
                            ),
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    /// Validate that no conflicting delete files were added in the given snapshots.
    async fn validate_no_conflicting_deletes_in_snapshots(
        table: &Table,
        snapshot_ids: &[i64],
        partition_filters: &HashMap<i32, BoundPredicate>,
    ) -> Result<()> {
        for &snapshot_id in snapshot_ids {
            let snapshot = table
                .metadata()
                .snapshot_by_id(snapshot_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Snapshot {snapshot_id} not found"),
                    )
                })?;

            let manifest_list = snapshot
                .load_manifest_list(table.file_io(), &table.metadata_ref())
                .await?;

            for manifest_file in manifest_list.entries() {
                if manifest_file.content != ManifestContentType::Deletes {
                    continue;
                }

                let spec_id = manifest_file.partition_spec_id;
                let partition_filter = partition_filters.get(&spec_id).ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Manifest '{}' references partition spec id {} which does not exist in table metadata",
                            manifest_file.manifest_path, spec_id
                        ),
                    )
                })?;
                let evaluator = ExpressionEvaluator::new(partition_filter.clone());

                let manifest = manifest_file.load_manifest(table.file_io()).await?;

                for entry in manifest.entries() {
                    if entry.status() != ManifestStatus::Added {
                        continue;
                    }
                    if entry.snapshot_id() != Some(snapshot_id) {
                        continue;
                    }

                    // Check if file matches the overwrite filter
                    if evaluator.eval(entry.data_file())? {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!(
                                "Conflicting delete file added in snapshot {}: {} in partition {:?}",
                                snapshot_id,
                                entry.data_file().file_path(),
                                entry.data_file().partition()
                            ),
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    /// Validate that all added files match the partition filter.
    ///
    /// This is an idempotency check to ensure added files are in partitions
    /// that would be affected by the overwrite filter.
    fn validate_added_files_match_partition_filter(
        added_files: &[DataFile],
        partition_filter: &BoundPredicate,
    ) -> Result<()> {
        let evaluator = ExpressionEvaluator::new(partition_filter.clone());

        for file in added_files {
            if !evaluator.eval(file)? {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Added file does not match overwrite filter: {} in partition {:?}. \
                        Files added during overwrite must be in partitions matching the filter.",
                        file.file_path(),
                        file.partition()
                    ),
                ));
            }
        }

        Ok(())
    }
}

#[async_trait]
impl TransactionAction for OverwriteAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        // Require a filter for OverwriteAction
        if self.overwrite_filter.is_none() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Overwrite filter is required. Use ReplacePartitionsAction for dynamic partition replacement.",
            ));
        }

        let schema = table.metadata().current_schema();
        let partition_spec = table.metadata().default_partition_spec();

        // Create partition filters for all specs so we can evaluate manifests written under
        // older specs as well.
        let partition_filters =
            self.create_partition_filters_for_all_specs(table, schema, self.case_sensitive)?;

        let default_spec_id = partition_spec.spec_id();
        let default_partition_filter =
            partition_filters.get(&default_spec_id).ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "Default partition spec id {default_spec_id} missing from partition filters"
                    ),
                )
            })?;

        // Perform conflict detection if enabled
        if self.validate_no_conflicting_data || self.validate_no_conflicting_deletes {
            let validate_from_snapshot_id = self.validate_from_snapshot_id.ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "validate_from_snapshot() must be called before validate_no_conflicting_data() or validate_no_conflicting_deletes()",
                )
            })?;

            let snapshots_since = Self::find_snapshots_since(
                table,
                validate_from_snapshot_id,
                table.metadata().current_snapshot_id(),
            )?;

            if self.validate_no_conflicting_data {
                Self::validate_no_conflicting_data_in_snapshots(
                    table,
                    &snapshots_since,
                    &partition_filters,
                )
                .await?;
            }

            if self.validate_no_conflicting_deletes {
                Self::validate_no_conflicting_deletes_in_snapshots(
                    table,
                    &snapshots_since,
                    &partition_filters,
                )
                .await?;
            }
        }

        // Validate that added files match the partition filter (idempotency check)
        if self.validate_added_files_match_filter && !self.added_data_files.is_empty() {
            Self::validate_added_files_match_partition_filter(
                &self.added_data_files,
                default_partition_filter,
            )?;
        }

        // Create snapshot producer
        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
            vec![], // No delete files for overwrite
        );

        // Validate added data files
        snapshot_producer.validate_added_data_files()?;

        // Check for duplicate files
        if self.check_duplicate {
            snapshot_producer.validate_duplicate_files().await?;
        }

        // Create operation with filter-based deletion logic
        let operation = OverwriteOperation {
            partition_filters,
            has_added_files: !self.added_data_files.is_empty(),
        };

        snapshot_producer
            .commit(operation, DefaultManifestProcess)
            .await
    }
}

/// Operation implementation for filter-based overwrite.
struct OverwriteOperation {
    partition_filters: HashMap<i32, BoundPredicate>,
    has_added_files: bool,
}

impl SnapshotProduceOperation for OverwriteOperation {
    fn operation(&self) -> Operation {
        if self.has_added_files {
            Operation::Overwrite
        } else {
            Operation::Delete
        }
    }

    async fn delete_entries(
        &self,
        snapshot_producer: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        let Some(snapshot) = snapshot_producer.current_snapshot()? else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_producer.table.file_io(),
                &snapshot_producer.table.metadata_ref(),
            )
            .await?;

        let mut entries_to_delete = Vec::new();

        for manifest_file in manifest_list.entries() {
            // Only process data manifests
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }

            let spec_id = manifest_file.partition_spec_id;
            let partition_filter = self.partition_filters.get(&spec_id).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Manifest '{}' references partition spec id {} which does not exist in table metadata",
                        manifest_file.manifest_path, spec_id
                    ),
                )
            })?;
            let evaluator = ExpressionEvaluator::new(partition_filter.clone());

            let manifest = manifest_file
                .load_manifest(snapshot_producer.table.file_io())
                .await?;

            for entry in manifest.entries() {
                // Only consider ADDED or EXISTING entries (not already DELETED)
                if entry.status() == ManifestStatus::Deleted {
                    continue;
                }

                // Check if this file matches the overwrite filter
                if evaluator.eval(entry.data_file())? {
                    // Create a delete entry for this file
                    let delete_entry = ManifestEntry::builder()
                        .status(ManifestStatus::Deleted)
                        .snapshot_id_opt(entry.snapshot_id())
                        .sequence_number_opt(entry.sequence_number())
                        .file_sequence_number_opt(entry.file_sequence_number)
                        .data_file(entry.data_file().clone())
                        .build();
                    entries_to_delete.push(delete_entry);
                }
            }
        }

        Ok(entries_to_delete)
    }

    async fn existing_manifest(
        &self,
        snapshot_producer: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let Some(snapshot) = snapshot_producer.current_snapshot()? else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_producer.table.file_io(),
                &snapshot_producer.table.metadata_ref(),
            )
            .await?;

        let mut result = Vec::new();

        for manifest_file in manifest_list.entries() {
            // Always include delete manifests
            if manifest_file.content == ManifestContentType::Deletes {
                result.push(manifest_file.clone());
                continue;
            }

            if manifest_file.content != ManifestContentType::Data {
                result.push(manifest_file.clone());
                continue;
            }

            let spec_id = manifest_file.partition_spec_id;
            let partition_filter = self.partition_filters.get(&spec_id).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Manifest '{}' references partition spec id {} which does not exist in table metadata",
                        manifest_file.manifest_path, spec_id
                    ),
                )
            })?;
            let evaluator = ExpressionEvaluator::new(partition_filter.clone());

            let manifest = manifest_file
                .load_manifest(snapshot_producer.table.file_io())
                .await?;

            let mut has_files_to_delete = false;
            for entry in manifest.entries() {
                if entry.status() == ManifestStatus::Deleted {
                    continue;
                }
                if evaluator.eval(entry.data_file())? {
                    has_files_to_delete = true;
                    break;
                }
            }

            if !has_files_to_delete {
                result.push(manifest_file.clone());
            }
        }

        Ok(result)
    }

    async fn existing_entries(
        &self,
        snapshot_producer: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        let Some(snapshot) = snapshot_producer.current_snapshot()? else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_producer.table.file_io(),
                &snapshot_producer.table.metadata_ref(),
            )
            .await?;

        let mut entries_to_keep = Vec::new();

        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }

            let spec_id = manifest_file.partition_spec_id;
            let partition_filter = self.partition_filters.get(&spec_id).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Manifest '{}' references partition spec id {} which does not exist in table metadata",
                        manifest_file.manifest_path, spec_id
                    ),
                )
            })?;
            let evaluator = ExpressionEvaluator::new(partition_filter.clone());

            let manifest = manifest_file
                .load_manifest(snapshot_producer.table.file_io())
                .await?;

            let mut has_files_to_delete = false;
            for entry in manifest.entries() {
                if entry.status() == ManifestStatus::Deleted {
                    continue;
                }
                if evaluator.eval(entry.data_file())? {
                    has_files_to_delete = true;
                    break;
                }
            }

            if !has_files_to_delete {
                continue;
            }

            for entry in manifest.entries() {
                if entry.status() == ManifestStatus::Deleted {
                    continue;
                }
                if !evaluator.eval(entry.data_file())? {
                    entries_to_keep.push(entry.as_ref().clone());
                }
            }
        }

        Ok(entries_to_keep)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::expr::Reference;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Datum, Literal, ManifestContentType,
        ManifestStatus, Struct, Transform, UnboundPartitionSpec,
    };
    use crate::transaction::tests::make_v2_minimal_table;
    use crate::transaction::{Transaction, TransactionAction};
    use crate::{TableRequirement, TableUpdate};

    #[tokio::test]
    async fn test_overwrite_requires_filter() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.overwrite();

        // Should fail without filter
        let result = Arc::new(action).commit(&table).await;
        assert!(result.is_err());
        match result {
            Err(err) => assert!(err.to_string().contains("Overwrite filter is required")),
            Ok(_) => panic!("Expected error"),
        }
    }

    #[tokio::test]
    async fn test_overwrite_with_filter_and_file() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        // Test schema has x, y, z (all long), partitioned by x
        let partition_value = Struct::from_iter([Some(Literal::long(100))]);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("s3://bucket/table/data/x=100/file1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(1024)
            .record_count(100)
            .partition(partition_value.clone())
            .build()
            .unwrap();

        let filter = Reference::new("x").equal_to(Datum::long(100));

        let action = tx
            .overwrite()
            .overwrite_filter(filter)
            .add_data_file(data_file);

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();

        // Should produce AddSnapshot update
        assert!(!updates.is_empty());
        let has_snapshot_update = updates.iter().any(|u| {
            matches!(
                u,
                TableUpdate::AddSnapshot { .. } | TableUpdate::SetSnapshotRef { .. }
            )
        });
        assert!(has_snapshot_update, "Expected snapshot updates");

        // Should have table requirements
        assert!(!requirements.is_empty());
        let has_uuid_requirement = requirements
            .iter()
            .any(|r| matches!(r, TableRequirement::UuidMatch { .. }));
        assert!(has_uuid_requirement, "Expected UUID match requirement");
    }

    #[tokio::test]
    async fn test_overwrite_with_always_true_filter() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        // Test schema has x, y, z (all long), partitioned by x
        let partition_value = Struct::from_iter([Some(Literal::long(100))]);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("s3://bucket/table/data/x=100/file1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(1024)
            .record_count(100)
            .partition(partition_value)
            .build()
            .unwrap();

        // AlwaysTrue filter - matches all files
        let filter = crate::expr::Predicate::AlwaysTrue;

        let action = tx
            .overwrite()
            .overwrite_filter(filter)
            .add_data_file(data_file);

        let result = Arc::new(action).commit(&table).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_overwrite_multiple_files() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        // Test schema has x, y, z (all long), partitioned by x
        let partition_value = Struct::from_iter([Some(Literal::long(100))]);

        let data_file1 = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("s3://bucket/table/data/x=100/file1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(1024)
            .record_count(100)
            .partition(partition_value.clone())
            .build()
            .unwrap();

        let data_file2 = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("s3://bucket/table/data/x=100/file2.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(2048)
            .record_count(200)
            .partition(partition_value)
            .build()
            .unwrap();

        let filter = Reference::new("x").equal_to(Datum::long(100));

        let action = tx
            .overwrite()
            .overwrite_filter(filter)
            .add_data_files(vec![data_file1, data_file2]);

        let result = Arc::new(action).commit(&table).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_overwrite_case_insensitive() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        // Test schema has x, y, z (all long), partitioned by x
        let partition_value = Struct::from_iter([Some(Literal::long(100))]);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("s3://bucket/table/data/x=100/file1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(1024)
            .record_count(100)
            .partition(partition_value)
            .build()
            .unwrap();

        // Use uppercase column name with case-insensitive matching
        let filter = Reference::new("X").equal_to(Datum::long(100));

        let action = tx
            .overwrite()
            .overwrite_filter(filter)
            .with_case_sensitive(false)
            .add_data_file(data_file);

        let result = Arc::new(action).commit(&table).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_overwrite_conflict_detection_requires_snapshot() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        // Test schema has x, y, z (all long), partitioned by x
        let partition_value = Struct::from_iter([Some(Literal::long(100))]);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("s3://bucket/table/data/x=100/file1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(1024)
            .record_count(100)
            .partition(partition_value)
            .build()
            .unwrap();

        let filter = Reference::new("x").equal_to(Datum::long(100));

        // Enable conflict detection without setting validate_from_snapshot
        let action = tx
            .overwrite()
            .overwrite_filter(filter)
            .add_data_file(data_file)
            .validate_no_conflicting_data();

        let result = Arc::new(action).commit(&table).await;
        assert!(result.is_err());
        match result {
            Err(err) => assert!(err.to_string().contains("validate_from_snapshot")),
            Ok(_) => panic!("Expected error"),
        }
    }

    #[tokio::test]
    async fn test_overwrite_validate_added_files_match_filter_passes() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        // Test schema has x, y, z (all long), partitioned by x
        // File partition matches filter (x = 100)
        let partition_value = Struct::from_iter([Some(Literal::long(100))]);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("s3://bucket/table/data/x=100/file1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(1024)
            .record_count(100)
            .partition(partition_value)
            .build()
            .unwrap();

        let filter = Reference::new("x").equal_to(Datum::long(100));

        // Enable idempotency check - should pass because file is in x=100
        let action = tx
            .overwrite()
            .overwrite_filter(filter)
            .add_data_file(data_file)
            .validate_added_files_match_filter();

        let result = Arc::new(action).commit(&table).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_overwrite_validate_added_files_match_filter_fails() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        // Test schema has x, y, z (all long), partitioned by x
        // File partition does NOT match filter (x = 200, but filter is x = 100)
        let partition_value = Struct::from_iter([Some(Literal::long(200))]);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("s3://bucket/table/data/x=200/file1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(1024)
            .record_count(100)
            .partition(partition_value)
            .build()
            .unwrap();

        let filter = Reference::new("x").equal_to(Datum::long(100));

        // Enable idempotency check - should fail because file is in x=200, not x=100
        let action = tx
            .overwrite()
            .overwrite_filter(filter)
            .add_data_file(data_file)
            .validate_added_files_match_filter();

        let result = Arc::new(action).commit(&table).await;
        assert!(result.is_err());
        match result {
            Err(err) => assert!(
                err.to_string()
                    .contains("Added file does not match overwrite filter")
            ),
            Ok(_) => panic!("Expected error"),
        }
    }

    #[tokio::test]
    async fn test_overwrite_filter_on_non_partitioned_column() {
        // When filtering on a column that is NOT part of the partition spec,
        // the filter projects to AlwaysTrue (matches all partitions).
        // This is correct behavior: we can't know which partitions contain
        // rows with y > 50, so we must consider all of them.
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        // Test schema has x, y, z (all long), partitioned by x
        // File is in partition x=100
        let partition_value = Struct::from_iter([Some(Literal::long(100))]);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("s3://bucket/table/data/x=100/file1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(1024)
            .record_count(100)
            .partition(partition_value)
            .build()
            .unwrap();

        // Filter on y (non-partitioned column) - projects to AlwaysTrue
        let filter = Reference::new("y").greater_than(Datum::long(50));

        let action = tx
            .overwrite()
            .overwrite_filter(filter)
            .add_data_file(data_file);

        // Should succeed - filter projects to AlwaysTrue which matches all partitions
        let result = Arc::new(action).commit(&table).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_overwrite_deletes_files_written_under_older_specs() {
        // Start with a table partitioned by x (spec 0)
        let table = make_v2_minimal_table();

        // Append two files under the original spec: x=100 and x=200
        let file_x_100 = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("s3://bucket/table/data/x=100/file1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            .partition_spec_id(0)
            .build()
            .unwrap();

        let file_x_200 = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("s3://bucket/table/data/x=200/file2.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition(Struct::from_iter([Some(Literal::long(200))]))
            .partition_spec_id(0)
            .build()
            .unwrap();

        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .add_data_files(vec![file_x_100.clone(), file_x_200.clone()]);

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let table =
            Transaction::update_table_metadata(table, &action_commit.take_updates()).unwrap();

        // Evolve partition spec to partition by y instead (spec 1, set as default)
        let new_spec = UnboundPartitionSpec::builder()
            .add_partition_field(2, "y", Transform::Identity)
            .unwrap()
            .build();

        let tx = Transaction::new(&table);
        let evolve = tx.evolve_partition_spec().add_spec(new_spec).set_default();
        let mut evolve_commit = Arc::new(evolve).commit(&table).await.unwrap();
        let table =
            Transaction::update_table_metadata(table, &evolve_commit.take_updates()).unwrap();

        // Append a file under the new spec (y=5)
        let new_spec_id = table.metadata().default_partition_spec_id();
        assert_ne!(new_spec_id, 0);

        let file_y_5 = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("s3://bucket/table/data/y=5/file3.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition(Struct::from_iter([Some(Literal::long(5))]))
            .partition_spec_id(new_spec_id)
            .build()
            .unwrap();

        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![file_y_5.clone()]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let table =
            Transaction::update_table_metadata(table, &action_commit.take_updates()).unwrap();

        // Overwrite where x = 100. This should delete:
        // - file_x_100 (written under spec 0)
        // - file_y_5   (written under spec 1, filter projects to AlwaysTrue)
        // and keep file_x_200.
        let filter = Reference::new("x").equal_to(Datum::long(100));
        let tx = Transaction::new(&table);
        let overwrite = tx.overwrite().overwrite_filter(filter);
        let mut overwrite_commit = Arc::new(overwrite).commit(&table).await.unwrap();
        let table =
            Transaction::update_table_metadata(table, &overwrite_commit.take_updates()).unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), &table.metadata_ref())
            .await
            .unwrap();

        let mut deleted_paths = Vec::new();
        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                if entry.status() == ManifestStatus::Deleted {
                    deleted_paths.push(entry.data_file().file_path().to_string());
                }
            }
        }

        assert!(deleted_paths.contains(&file_x_100.file_path().to_string()));
        assert!(deleted_paths.contains(&file_y_5.file_path().to_string()));
        assert!(!deleted_paths.contains(&file_x_200.file_path().to_string()));
    }
}
