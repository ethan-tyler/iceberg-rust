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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::ops::RangeFrom;

use uuid::Uuid;

use crate::error::Result;
use crate::spec::{
    DataContentType, DataFile, DataFileFormat, FormatVersion, MAIN_BRANCH, ManifestContentType,
    ManifestEntry, ManifestFile, ManifestListWriter, ManifestWriter, ManifestWriterBuilder,
    Operation, Snapshot, SnapshotReference, SnapshotRetention, SnapshotSummaryCollector, Struct,
    StructType, Summary, TableProperties, update_snapshot_summaries,
};
use crate::table::Table;
use crate::transaction::ActionCommit;
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

const META_ROOT_PATH: &str = "metadata";

/// Groups data files by their partition spec ID.
///
/// This is used when committing files from evolved tables where files may
/// have been written under different partition specs. Each group will be
/// written to a separate manifest to comply with the Iceberg specification
/// that "a manifest stores files for a single partition spec."
///
/// Uses BTreeMap for deterministic iteration order (sorted by spec_id),
/// ensuring consistent manifest numbering across runs.
fn group_files_by_spec(files: Vec<DataFile>) -> BTreeMap<i32, Vec<DataFile>> {
    let mut groups: BTreeMap<i32, Vec<DataFile>> = BTreeMap::new();
    for file in files {
        groups.entry(file.partition_spec_id).or_default().push(file);
    }
    groups
}

/// Groups manifest entries by their data file's partition spec ID.
///
/// This is used when writing deleted or existing entries from evolved tables
/// where entries may reference files from different partition specs.
/// Each group will be written to a separate manifest.
///
/// Uses BTreeMap for deterministic iteration order (sorted by spec_id),
/// ensuring consistent manifest numbering across runs.
fn group_entries_by_spec(entries: Vec<ManifestEntry>) -> BTreeMap<i32, Vec<ManifestEntry>> {
    let mut groups: BTreeMap<i32, Vec<ManifestEntry>> = BTreeMap::new();
    for entry in entries {
        let spec_id = entry.data_file().partition_spec_id;
        groups.entry(spec_id).or_default().push(entry);
    }
    groups
}

/// A trait that defines how different table operations produce new snapshots.
///
/// `SnapshotProduceOperation` is used by [`SnapshotProducer`] to customize snapshot creation
/// based on the type of operation being performed (e.g., `Append`, `Overwrite`, `Delete`, etc.).
/// Each operation type implements this trait to specify:
/// - Which operation type to record in the snapshot summary
/// - Which existing manifest files should be included in the new snapshot
/// - Which manifest entries should be marked as deleted
///
/// # Usage in Snapshot Creation
///
/// This trait is used during the snapshot creation process in [`SnapshotProducer::commit()`]:
///
/// 1. **Operation Type Recording**: The `operation()` method determines which operation type
///    (e.g., `Operation::Append`, `Operation::Overwrite`) is recorded in the snapshot summary.
///    This metadata helps track what kind of change was made to the table.
///
/// 2. **Manifest File Selection**: The `existing_manifest()` method determines which existing
///    manifest files from the current snapshot should be carried forward to the new snapshot.
///    For example:
///    - An `Append` operation typically includes all existing manifests plus new ones
///    - An `Overwrite` operation might exclude manifests for partitions being overwritten
///
/// 3. **Delete Entry Processing**: The `delete_entries()` method specifies which manifest
///    entries should be marked as deleted. This is used by operations like `ReplacePartitions`
///    and `Overwrite` to atomically remove existing data files while adding new ones.
pub(crate) trait SnapshotProduceOperation: Send + Sync {
    /// Returns the operation type that will be recorded in the snapshot summary.
    ///
    /// This determines what kind of operation is being performed (e.g., `Append`, `Overwrite`),
    /// which is stored in the snapshot metadata for tracking and auditing purposes.
    fn operation(&self) -> Operation;

    /// Returns manifest entries that should be marked as deleted in the new snapshot.
    ///
    /// These entries represent data files being removed from the table. They will be
    /// written to a new manifest with status=DELETED to track the removal.
    fn delete_entries(
        &self,
        snapshot_produce: &SnapshotProducer,
    ) -> impl Future<Output = Result<Vec<ManifestEntry>>> + Send;

    /// Returns existing manifest files that should be included in the new snapshot.
    ///
    /// This method determines which manifest files from the current snapshot should be
    /// carried forward to the new snapshot. The selection depends on the operation type:
    ///
    /// - **Append operations**: Typically include all existing manifests
    /// - **Overwrite operations**: May exclude manifests for partitions being overwritten
    /// - **Delete operations**: May exclude manifests for partitions being deleted
    fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> impl Future<Output = Result<Vec<ManifestFile>>> + Send;

    /// Returns manifest entries that should be carried forward as EXISTING status.
    ///
    /// This is used when a manifest contains a mix of files: some to keep and some to replace.
    /// Instead of including the original manifest (which would cause both Added and Deleted
    /// entries for the same file), we return only the entries we want to keep.
    /// These will be written to a new manifest with status=EXISTING.
    ///
    /// Default implementation returns empty (most operations don't need this).
    fn existing_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> impl Future<Output = Result<Vec<ManifestEntry>>> + Send {
        async { Ok(vec![]) }
    }

    /// Returns the data sequence number to use for added files.
    ///
    /// For most operations, this returns `None` which means the sequence number
    /// will be inherited from the manifest list at commit time (the normal V2 behavior).
    ///
    /// For compaction operations with `use_starting_sequence_number=true`, this returns
    /// the sequence number from when the compaction was planned. This ensures that
    /// equality deletes written after planning will still apply to the compacted files,
    /// maintaining MoR (Merge-on-Read) correctness.
    ///
    /// Default implementation returns `None`.
    fn data_sequence_number(&self) -> Option<i64> {
        None
    }

    fn delete_sequence_number(&self) -> Option<i64> {
        None
    }
}

pub(crate) struct DefaultManifestProcess;

impl ManifestProcess for DefaultManifestProcess {
    fn process_manifests(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
        manifests: Vec<ManifestFile>,
    ) -> Vec<ManifestFile> {
        manifests
    }
}

pub(crate) trait ManifestProcess: Send + Sync {
    fn process_manifests(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
        manifests: Vec<ManifestFile>,
    ) -> Vec<ManifestFile>;
}

pub(crate) struct SnapshotProducer<'a> {
    pub(crate) table: &'a Table,
    target_ref: Option<String>,
    snapshot_id: i64,
    commit_uuid: Uuid,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
    // A counter used to generate unique manifest file names.
    // It starts from 0 and increments for each new manifest file.
    // Note: This counter is limited to the range of (0..u64::MAX).
    manifest_counter: RangeFrom<u64>,
}

impl<'a> SnapshotProducer<'a> {
    pub(crate) fn new(
        table: &'a Table,
        commit_uuid: Uuid,
        key_metadata: Option<Vec<u8>>,
        snapshot_properties: HashMap<String, String>,
        added_data_files: Vec<DataFile>,
        added_delete_files: Vec<DataFile>,
    ) -> Self {
        Self {
            table,
            target_ref: None,
            snapshot_id: Self::generate_unique_snapshot_id(table),
            commit_uuid,
            key_metadata,
            snapshot_properties,
            added_data_files,
            added_delete_files,
            manifest_counter: (0..),
        }
    }

    pub(crate) fn with_target_ref(mut self, target_ref: impl Into<String>) -> Self {
        self.target_ref = Some(target_ref.into());
        self
    }

    pub(crate) fn current_snapshot_id(&self) -> Result<Option<i64>> {
        Ok(self.resolve_target_ref()?.base_snapshot_id)
    }

    pub(crate) fn current_snapshot(&self) -> Result<Option<&Snapshot>> {
        Ok(self
            .current_snapshot_id()?
            .and_then(|id| self.table.metadata().snapshot_by_id(id))
            .map(|arc| arc.as_ref()))
    }

    /// Validates added data files.
    ///
    /// For tables with partition evolution, data files may use any valid partition spec
    /// from the table metadata (not just the default spec). This enables operations on
    /// tables where files were written under different partition schemes.
    pub(crate) fn validate_added_data_files(&self) -> Result<()> {
        for data_file in &self.added_data_files {
            if data_file.content_type() != DataContentType::Data {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Only data content type is allowed for fast append",
                ));
            }
            // Validate that the data file references a valid partition spec from table metadata
            let partition_spec = self
                .table
                .metadata()
                .partition_spec_by_id(data_file.partition_spec_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Data file references unknown partition spec: {}",
                            data_file.partition_spec_id
                        ),
                    )
                })?;
            // Validate partition value against the file's actual partition type
            let partition_type = self
                .find_compatible_partition_type(partition_spec)
                .map_err(|e| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Failed to compute partition type for spec {}: {}",
                            data_file.partition_spec_id, e
                        ),
                    )
                })?;
            Self::validate_partition_value(data_file.partition(), &partition_type)?;
        }

        Ok(())
    }

    pub(crate) fn validate_added_delete_files(&self) -> Result<()> {
        if self.table.metadata().format_version() == FormatVersion::V1
            && !self.added_delete_files.is_empty()
        {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Delete files are not supported in format version 1. Upgrade the table to format version 2 or later.",
            ));
        }

        for delete_file in &self.added_delete_files {
            match delete_file.content_type() {
                DataContentType::PositionDeletes => {}
                DataContentType::EqualityDeletes => {
                    if delete_file.equality_ids().is_none_or(|ids| ids.is_empty()) {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            "Equality delete files must have equality_ids set",
                        ));
                    }
                }
                DataContentType::Data => {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "Data content type is not allowed for delete files",
                    ));
                }
            }
            let partition_spec = self
                .table
                .metadata()
                .partition_spec_by_id(delete_file.partition_spec_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Delete file references unknown partition spec id: {}",
                            delete_file.partition_spec_id
                        ),
                    )
                })?;
            let partition_type = self
                .find_compatible_partition_type(partition_spec)
                .map_err(|e| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Failed to compute partition type for spec {}: {}",
                            delete_file.partition_spec_id, e
                        ),
                    )
                })?;
            Self::validate_partition_value(delete_file.partition(), &partition_type)?;
        }

        Ok(())
    }

    pub(crate) async fn validate_duplicate_files(&self) -> Result<()> {
        let mut seen_data_files: HashSet<&str> = HashSet::new();
        let mut intra_batch_data_duplicates = Vec::new();
        for data_file in &self.added_data_files {
            if !seen_data_files.insert(data_file.file_path.as_str()) {
                intra_batch_data_duplicates.push(data_file.file_path.clone());
            }
        }
        if !intra_batch_data_duplicates.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot add duplicate data files in the same batch: {}",
                    intra_batch_data_duplicates.join(", ")
                ),
            ));
        }

        let mut seen_delete_files: HashSet<&str> = HashSet::new();
        let mut intra_batch_delete_duplicates = Vec::new();
        for delete_file in &self.added_delete_files {
            if !seen_delete_files.insert(delete_file.file_path.as_str()) {
                intra_batch_delete_duplicates.push(delete_file.file_path.clone());
            }
        }
        if !intra_batch_delete_duplicates.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot add duplicate delete files in the same batch: {}",
                    intra_batch_delete_duplicates.join(", ")
                ),
            ));
        }

        let new_data_files = seen_data_files;
        let new_delete_files = seen_delete_files;

        let mut duplicate_data_files = Vec::new();
        let mut duplicate_delete_files = Vec::new();

        if let Some(current_snapshot) = self.current_snapshot()? {
            let manifest_list = current_snapshot
                .load_manifest_list(self.table.file_io(), &self.table.metadata_ref())
                .await?;
            for manifest_list_entry in manifest_list.entries() {
                let manifest = manifest_list_entry
                    .load_manifest(self.table.file_io())
                    .await?;
                for entry in manifest.entries() {
                    let file_path = entry.file_path();
                    if entry.is_alive() {
                        if new_data_files.contains(file_path) {
                            duplicate_data_files.push(file_path.to_string());
                        }
                        if new_delete_files.contains(file_path) {
                            duplicate_delete_files.push(file_path.to_string());
                        }
                    }
                }
            }
        }

        if !duplicate_data_files.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot add data files that are already referenced by table, files: {}",
                    duplicate_data_files.join(", ")
                ),
            ));
        }

        if !duplicate_delete_files.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot add delete files that are already referenced by table, files: {}",
                    duplicate_delete_files.join(", ")
                ),
            ));
        }

        Ok(())
    }

    fn generate_unique_snapshot_id(table: &Table) -> i64 {
        let generate_random_id = || -> i64 {
            let (lhs, rhs) = Uuid::new_v4().as_u64_pair();
            let snapshot_id = (lhs ^ rhs) as i64;
            if snapshot_id < 0 {
                -snapshot_id
            } else {
                snapshot_id
            }
        };
        let mut snapshot_id = generate_random_id();

        while table
            .metadata()
            .snapshots()
            .any(|s| s.snapshot_id() == snapshot_id)
        {
            snapshot_id = generate_random_id();
        }
        snapshot_id
    }

    /// Creates a manifest writer for a specific partition spec.
    ///
    /// This is used when writing manifests for files that belong to a non-default
    /// partition spec, which can happen in tables with partition evolution.
    ///
    /// For evolved tables where the partition spec references fields not present
    /// in the current schema, this method finds a compatible historical schema.
    fn new_manifest_writer_for_spec(
        &mut self,
        partition_spec: &crate::spec::PartitionSpecRef,
        content: ManifestContentType,
    ) -> Result<ManifestWriter> {
        // Find a schema compatible with this partition spec.
        // For evolved tables, old specs may reference fields not in the current schema.
        let compatible_schema = self.find_compatible_schema(partition_spec)?;

        let new_manifest_path = format!(
            "{}/{}/{}-m{}.{}",
            self.table.metadata().location(),
            META_ROOT_PATH,
            self.commit_uuid,
            self.manifest_counter.next().unwrap(),
            DataFileFormat::Avro
        );
        let output_file = self.table.file_io().new_output(new_manifest_path)?;
        let builder = ManifestWriterBuilder::new(
            output_file,
            Some(self.snapshot_id),
            self.key_metadata.clone(),
            compatible_schema,
            partition_spec.as_ref().clone(),
        );
        match self.table.metadata().format_version() {
            FormatVersion::V1 => Ok(builder.build_v1()),
            FormatVersion::V2 => match content {
                ManifestContentType::Data => Ok(builder.build_v2_data()),
                ManifestContentType::Deletes => Ok(builder.build_v2_deletes()),
            },
            FormatVersion::V3 => match content {
                ManifestContentType::Data => Ok(builder.build_v3_data()),
                ManifestContentType::Deletes => Ok(builder.build_v3_deletes()),
            },
        }
    }

    // Check if the partition value is compatible with the partition type.
    fn validate_partition_value(
        partition_value: &Struct,
        partition_type: &StructType,
    ) -> Result<()> {
        if partition_value.fields().len() != partition_type.fields().len() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Partition value is not compatible with partition type",
            ));
        }

        for (value, field) in partition_value.fields().iter().zip(partition_type.fields()) {
            let field = field.field_type.as_primitive_type().ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Partition field should only be primitive type.",
                )
            })?;
            if let Some(value) = value
                && !field.compatible(&value.as_primitive_literal().unwrap())
            {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Partition value is not compatible partition type",
                ));
            }
        }
        Ok(())
    }

    fn find_compatible_partition_type(
        &self,
        partition_spec: &crate::spec::PartitionSpecRef,
    ) -> Result<StructType> {
        let current_schema = self.table.metadata().current_schema();
        if let Ok(partition_type) = partition_spec.partition_type(current_schema) {
            return Ok(partition_type);
        }

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

    /// Finds a schema compatible with the given partition spec.
    ///
    /// For evolved tables, old partition specs may reference fields that no longer
    /// exist in the current schema. This method finds a schema that can compute
    /// the partition type for the given spec.
    ///
    /// Returns the current schema if compatible, otherwise searches historical schemas.
    fn find_compatible_schema(
        &self,
        partition_spec: &crate::spec::PartitionSpecRef,
    ) -> Result<crate::spec::SchemaRef> {
        let current_schema = self.table.metadata().current_schema();
        if partition_spec.partition_type(current_schema).is_ok() {
            return Ok(current_schema.clone());
        }

        for schema in self.table.metadata().schemas_iter() {
            if partition_spec.partition_type(schema).is_ok() {
                return Ok(schema.clone());
            }
        }

        Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Cannot find compatible schema for partition spec {}: no schema contains all referenced fields",
                partition_spec.spec_id()
            ),
        ))
    }

    /// Write manifest files for added data files, grouping by partition spec.
    ///
    /// For tables with partition evolution, files may have different partition specs.
    /// Each spec's files are written to a separate manifest to comply with the Iceberg
    /// specification that "a manifest stores files for a single partition spec."
    ///
    /// # Arguments
    ///
    /// * `data_sequence_number` - Optional sequence number to use for the added entries.
    ///   If `Some`, this sequence number will be set on each manifest entry (used for
    ///   compaction operations where `use_starting_sequence_number=true`).
    ///   If `None`, the sequence number will be inherited from the manifest list at
    ///   commit time (normal V2+ behavior).
    async fn write_added_manifests(
        &mut self,
        data_sequence_number: Option<i64>,
    ) -> Result<Vec<ManifestFile>> {
        let added_data_files = std::mem::take(&mut self.added_data_files);
        if added_data_files.is_empty() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "No added data files found when write an added manifest file",
            ));
        }

        let snapshot_id = self.snapshot_id;
        let format_version = self.table.metadata().format_version();

        // Group files by partition spec for per-spec manifest writing
        let file_groups = group_files_by_spec(added_data_files);

        let mut manifests = Vec::with_capacity(file_groups.len());
        for (spec_id, files) in file_groups {
            let partition_spec = self
                .table
                .metadata()
                .partition_spec_by_id(spec_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Data file references unknown partition spec: {spec_id}"),
                    )
                })?;

            let manifest_entries = files.into_iter().map(|data_file| {
                let builder = ManifestEntry::builder()
                    .status(crate::spec::ManifestStatus::Added)
                    .data_file(data_file);
                if format_version == FormatVersion::V1 {
                    builder.snapshot_id(snapshot_id).build()
                } else {
                    // For format version > 1, we set the snapshot id at the inherited time
                    // to avoid rewriting the manifest file when commit fails.
                    // However, for compaction with use_starting_sequence_number=true,
                    // we must set the sequence number explicitly to ensure MoR correctness.
                    if let Some(seq_num) = data_sequence_number {
                        builder.sequence_number(seq_num).build()
                    } else {
                        builder.build()
                    }
                }
            });

            let mut writer =
                self.new_manifest_writer_for_spec(partition_spec, ManifestContentType::Data)?;
            for entry in manifest_entries {
                writer.add_entry(entry)?;
            }
            manifests.push(writer.write_manifest_file().await?);
        }

        Ok(manifests)
    }

    /// Write manifest files for added delete files, grouping by partition spec.
    ///
    /// For tables with partition evolution, position delete files inherit the partition
    /// spec of the data file they reference. Each spec's delete files are written to
    /// a separate manifest to comply with the Iceberg specification.
    async fn write_delete_manifests(
        &mut self,
        delete_sequence_number: Option<i64>,
    ) -> Result<Vec<ManifestFile>> {
        let added_delete_files = std::mem::take(&mut self.added_delete_files);
        if added_delete_files.is_empty() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "No added delete files found when write a delete manifest file",
            ));
        }

        let snapshot_id = self.snapshot_id;
        let format_version = self.table.metadata().format_version();

        // Group delete files by partition spec for per-spec manifest writing
        let file_groups = group_files_by_spec(added_delete_files);

        let mut manifests = Vec::with_capacity(file_groups.len());
        for (spec_id, files) in file_groups {
            let partition_spec = self
                .table
                .metadata()
                .partition_spec_by_id(spec_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Delete file references unknown partition spec: {spec_id}"),
                    )
                })?;

            let manifest_entries = files.into_iter().map(|delete_file| {
                let builder = ManifestEntry::builder()
                    .status(crate::spec::ManifestStatus::Added)
                    .data_file(delete_file);
                if format_version == FormatVersion::V1 {
                    builder.snapshot_id(snapshot_id).build()
                } else if let Some(seq_num) = delete_sequence_number {
                    builder.sequence_number(seq_num).build()
                } else {
                    builder.build()
                }
            });

            let mut writer =
                self.new_manifest_writer_for_spec(partition_spec, ManifestContentType::Deletes)?;
            for entry in manifest_entries {
                writer.add_entry(entry)?;
            }
            manifests.push(writer.write_manifest_file().await?);
        }

        Ok(manifests)
    }

    /// Write manifest files for deleted data file entries (files being removed from the table).
    ///
    /// This is used by operations like `ReplacePartitions` and `Overwrite` that need to
    /// mark existing data files as deleted in a new manifest.
    ///
    /// For tables with partition evolution, entries may reference files from different
    /// partition specs. Each spec's entries are written to a separate manifest.
    pub(crate) async fn write_deleted_data_manifests(
        &mut self,
        deleted_entries: Vec<ManifestEntry>,
    ) -> Result<Vec<ManifestFile>> {
        if deleted_entries.is_empty() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "No deleted entries to write",
            ));
        }

        let (data_entries, delete_entries): (Vec<_>, Vec<_>) = deleted_entries
            .into_iter()
            .partition(|entry| entry.content_type() == DataContentType::Data);

        let mut manifests = Vec::new();

        if !data_entries.is_empty() {
            let entry_groups = group_entries_by_spec(data_entries);
            for (spec_id, entries) in entry_groups {
                let partition_spec = self
                    .table
                    .metadata()
                    .partition_spec_by_id(spec_id)
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!("Deleted entry references unknown partition spec: {spec_id}"),
                        )
                    })?;

                let mut writer =
                    self.new_manifest_writer_for_spec(partition_spec, ManifestContentType::Data)?;
                for entry in entries {
                    writer.add_delete_entry(entry)?;
                }
                manifests.push(writer.write_manifest_file().await?);
            }
        }

        if !delete_entries.is_empty() {
            let entry_groups = group_entries_by_spec(delete_entries);
            for (spec_id, entries) in entry_groups {
                let partition_spec = self
                    .table
                    .metadata()
                    .partition_spec_by_id(spec_id)
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!("Deleted entry references unknown partition spec: {spec_id}"),
                        )
                    })?;

                let mut writer = self
                    .new_manifest_writer_for_spec(partition_spec, ManifestContentType::Deletes)?;
                for entry in entries {
                    writer.add_delete_entry(entry)?;
                }
                manifests.push(writer.write_manifest_file().await?);
            }
        }

        Ok(manifests)
    }

    /// Write manifest files for existing data file entries (files being carried forward).
    ///
    /// This is used when a manifest contains a mix of files: some to keep and some to replace.
    /// The entries to keep are written to new manifests with status=EXISTING.
    ///
    /// For tables with partition evolution, entries may reference files from different
    /// partition specs. Each spec's entries are written to a separate manifest.
    pub(crate) async fn write_existing_data_manifests(
        &mut self,
        existing_entries: Vec<ManifestEntry>,
    ) -> Result<Vec<ManifestFile>> {
        if existing_entries.is_empty() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "No existing entries to write",
            ));
        }

        let (data_entries, delete_entries): (Vec<_>, Vec<_>) = existing_entries
            .into_iter()
            .partition(|entry| entry.content_type() == DataContentType::Data);

        let mut manifests = Vec::new();

        if !data_entries.is_empty() {
            let entry_groups = group_entries_by_spec(data_entries);
            for (spec_id, entries) in entry_groups {
                let partition_spec = self
                    .table
                    .metadata()
                    .partition_spec_by_id(spec_id)
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!("Existing entry references unknown partition spec: {spec_id}"),
                        )
                    })?;

                let mut writer =
                    self.new_manifest_writer_for_spec(partition_spec, ManifestContentType::Data)?;
                for entry in entries {
                    writer.add_existing_entry(entry)?;
                }
                manifests.push(writer.write_manifest_file().await?);
            }
        }

        if !delete_entries.is_empty() {
            let entry_groups = group_entries_by_spec(delete_entries);
            for (spec_id, entries) in entry_groups {
                let partition_spec = self
                    .table
                    .metadata()
                    .partition_spec_by_id(spec_id)
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!("Existing entry references unknown partition spec: {spec_id}"),
                        )
                    })?;

                let mut writer = self
                    .new_manifest_writer_for_spec(partition_spec, ManifestContentType::Deletes)?;
                for entry in entries {
                    writer.add_existing_entry(entry)?;
                }
                manifests.push(writer.write_manifest_file().await?);
            }
        }

        Ok(manifests)
    }

    async fn manifest_file<OP: SnapshotProduceOperation, MP: ManifestProcess>(
        &mut self,
        snapshot_produce_operation: &OP,
        manifest_process: &MP,
    ) -> Result<Vec<ManifestFile>> {
        // Get entries to delete (files being removed from the table)
        let deleted_entries = snapshot_produce_operation.delete_entries(self).await?;
        let has_deleted_entries = !deleted_entries.is_empty();

        // Get existing entries (files being carried forward from partially affected manifests)
        let existing_entries = snapshot_produce_operation.existing_entries(self).await?;
        let has_existing_entries = !existing_entries.is_empty();

        // Assert current snapshot producer contains new content to add to new snapshot.
        //
        // TODO: Allowing snapshot property setup with no added data files is a workaround.
        // We should clean it up after all necessary actions are supported.
        // For details, please refer to https://github.com/apache/iceberg-rust/issues/1548
        let has_data_files = !self.added_data_files.is_empty();
        let has_delete_files = !self.added_delete_files.is_empty();
        let has_properties = !self.snapshot_properties.is_empty();

        if !has_data_files
            && !has_delete_files
            && !has_properties
            && !has_deleted_entries
            && !has_existing_entries
        {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "No added data files, delete files, deleted entries, existing entries, or snapshot properties found when write a manifest file",
            ));
        }

        let existing_manifests = snapshot_produce_operation.existing_manifest(self).await?;
        let mut manifest_files = existing_manifests;

        // Process existing data file entries (files being carried forward from mixed manifests).
        // Entries are grouped by partition spec, with separate manifests per spec.
        if has_existing_entries {
            let existing_manifests = self.write_existing_data_manifests(existing_entries).await?;
            manifest_files.extend(existing_manifests);
        }

        // Process deleted data file entries (files being removed from the table).
        // Entries are grouped by partition spec, with separate manifests per spec.
        if has_deleted_entries {
            let deleted_manifests = self.write_deleted_data_manifests(deleted_entries).await?;
            manifest_files.extend(deleted_manifests);
        }

        // Process added data file entries.
        // Files are grouped by partition spec, with separate manifests per spec.
        // For compaction operations, the data sequence number may be explicitly set
        // to maintain MoR correctness (use_starting_sequence_number=true).
        if has_data_files {
            let data_sequence_number = snapshot_produce_operation.data_sequence_number();
            let added_manifests = self.write_added_manifests(data_sequence_number).await?;
            manifest_files.extend(added_manifests);
        }

        // Process added delete file entries (position/equality deletes).
        // Delete files are grouped by partition spec, with separate manifests per spec.
        if has_delete_files {
            let delete_sequence_number = snapshot_produce_operation.delete_sequence_number();
            let delete_manifests = self.write_delete_manifests(delete_sequence_number).await?;
            manifest_files.extend(delete_manifests);
        }

        let manifest_files = manifest_process.process_manifests(self, manifest_files);
        Ok(manifest_files)
    }

    // Returns a `Summary` of the current snapshot
    fn summary<OP: SnapshotProduceOperation>(
        &self,
        snapshot_produce_operation: &OP,
    ) -> Result<Summary> {
        let mut summary_collector = SnapshotSummaryCollector::default();
        let table_metadata = self.table.metadata_ref();

        let partition_summary_limit = if let Some(limit) = table_metadata
            .properties()
            .get(TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT)
        {
            if let Ok(limit) = limit.parse::<u64>() {
                limit
            } else {
                TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT
            }
        } else {
            TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT
        };

        summary_collector.set_partition_summary_limit(partition_summary_limit);

        // Add data files to summary collector, using each file's actual partition spec
        // and a schema compatible with that spec (for evolved tables with historical schemas)
        for data_file in &self.added_data_files {
            let partition_spec = table_metadata
                .partition_spec_by_id(data_file.partition_spec_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Data file references unknown partition spec: {}",
                            data_file.partition_spec_id
                        ),
                    )
                })?;
            let compatible_schema = self.find_compatible_schema(partition_spec)?;
            summary_collector.add_file(
                data_file,
                compatible_schema.clone(),
                partition_spec.clone(),
            );
        }

        // Add delete files to summary collector, using each file's actual partition spec
        // and a schema compatible with that spec (for evolved tables with historical schemas)
        for delete_file in &self.added_delete_files {
            let partition_spec = table_metadata
                .partition_spec_by_id(delete_file.partition_spec_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Delete file references unknown partition spec: {}",
                            delete_file.partition_spec_id
                        ),
                    )
                })?;
            let compatible_schema = self.find_compatible_schema(partition_spec)?;
            summary_collector.add_file(
                delete_file,
                compatible_schema.clone(),
                partition_spec.clone(),
            );
        }

        let previous_snapshot = table_metadata
            .snapshot_by_id(self.snapshot_id)
            .and_then(|snapshot| snapshot.parent_snapshot_id())
            .and_then(|parent_id| table_metadata.snapshot_by_id(parent_id));

        let mut additional_properties = summary_collector.build();
        additional_properties.extend(self.snapshot_properties.clone());

        let summary = Summary {
            operation: snapshot_produce_operation.operation(),
            additional_properties,
        };

        update_snapshot_summaries(
            summary,
            previous_snapshot.map(|s| s.summary()),
            snapshot_produce_operation.operation() == Operation::Overwrite,
        )
    }

    fn generate_manifest_list_file_path(&self, attempt: i64) -> String {
        format!(
            "{}/{}/snap-{}-{}-{}.{}",
            self.table.metadata().location(),
            META_ROOT_PATH,
            self.snapshot_id,
            attempt,
            self.commit_uuid,
            DataFileFormat::Avro
        )
    }

    /// Finished building the action and return the [`ActionCommit`] to the transaction.
    pub(crate) async fn commit<OP: SnapshotProduceOperation, MP: ManifestProcess>(
        mut self,
        snapshot_produce_operation: OP,
        process: MP,
    ) -> Result<ActionCommit> {
        // Clone target_ref early to avoid borrow conflicts with mutable self borrows later
        let target_ref = self
            .target_ref
            .clone()
            .unwrap_or_else(|| MAIN_BRANCH.to_string());
        let TargetRefInfo {
            base_snapshot_id,
            expected_snapshot_id,
            retention,
            ..
        } = self.resolve_target_ref()?;

        let manifest_list_path = self.generate_manifest_list_file_path(0);
        let next_seq_num = self.table.metadata().next_sequence_number();
        let first_row_id = self.table.metadata().next_row_id();
        let mut manifest_list_writer = match self.table.metadata().format_version() {
            FormatVersion::V1 => ManifestListWriter::v1(
                self.table
                    .file_io()
                    .new_output(manifest_list_path.clone())?,
                self.snapshot_id,
                base_snapshot_id,
            ),
            FormatVersion::V2 => ManifestListWriter::v2(
                self.table
                    .file_io()
                    .new_output(manifest_list_path.clone())?,
                self.snapshot_id,
                base_snapshot_id,
                next_seq_num,
            ),
            FormatVersion::V3 => ManifestListWriter::v3(
                self.table
                    .file_io()
                    .new_output(manifest_list_path.clone())?,
                self.snapshot_id,
                base_snapshot_id,
                next_seq_num,
                Some(first_row_id),
            ),
        };

        // Calling self.summary() before self.manifest_file() is important because self.added_data_files
        // will be set to an empty vec after self.manifest_file() returns, resulting in an empty summary
        // being generated.
        let summary = self.summary(&snapshot_produce_operation).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "Failed to create snapshot summary.").with_source(err)
        })?;

        let new_manifests = self
            .manifest_file(&snapshot_produce_operation, &process)
            .await?;

        manifest_list_writer.add_manifests(new_manifests.into_iter())?;
        let writer_next_row_id = manifest_list_writer.next_row_id();
        manifest_list_writer.close().await?;

        let commit_ts = chrono::Utc::now().timestamp_millis();
        let new_snapshot = Snapshot::builder()
            .with_manifest_list(manifest_list_path)
            .with_snapshot_id(self.snapshot_id)
            .with_parent_snapshot_id(base_snapshot_id)
            .with_sequence_number(next_seq_num)
            .with_summary(summary)
            .with_schema_id(self.table.metadata().current_schema_id())
            .with_timestamp_ms(commit_ts);

        let new_snapshot = if let Some(writer_next_row_id) = writer_next_row_id {
            let assigned_rows = writer_next_row_id - self.table.metadata().next_row_id();
            new_snapshot
                .with_row_range(first_row_id, assigned_rows)
                .build()
        } else {
            new_snapshot.build()
        };

        let updates = vec![
            TableUpdate::AddSnapshot {
                snapshot: new_snapshot,
            },
            TableUpdate::SetSnapshotRef {
                ref_name: target_ref.to_string(),
                reference: SnapshotReference::new(self.snapshot_id, retention),
            },
        ];

        let requirements = vec![
            TableRequirement::UuidMatch {
                uuid: self.table.metadata().uuid(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: target_ref.to_string(),
                snapshot_id: expected_snapshot_id,
            },
        ];

        Ok(ActionCommit::new(updates, requirements))
    }

    fn resolve_target_ref(&self) -> Result<TargetRefInfo> {
        let metadata = self.table.metadata();
        let target_ref = self.target_ref.as_deref().unwrap_or(MAIN_BRANCH);

        if target_ref == MAIN_BRANCH {
            let retention = metadata
                .refs()
                .get(MAIN_BRANCH)
                .map(|r| {
                    if !r.is_branch() {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!(
                                "Cannot write to reference '{MAIN_BRANCH}': reference is not a branch",
                            ),
                        ));
                    }
                    Ok(r.retention.clone())
                })
                .transpose()?
                .unwrap_or_else(|| SnapshotRetention::branch(None, None, None));

            let ref_snapshot_id = metadata.refs().get(MAIN_BRANCH).map(|r| r.snapshot_id);
            let expected_snapshot_id = ref_snapshot_id.or_else(|| metadata.current_snapshot_id());

            return Ok(TargetRefInfo {
                base_snapshot_id: expected_snapshot_id,
                expected_snapshot_id,
                retention,
            });
        }

        if let Some(snapshot_ref) = metadata.refs().get(target_ref) {
            if !snapshot_ref.is_branch() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("Cannot write to reference '{target_ref}': reference is not a branch",),
                ));
            }

            return Ok(TargetRefInfo {
                base_snapshot_id: Some(snapshot_ref.snapshot_id),
                expected_snapshot_id: Some(snapshot_ref.snapshot_id),
                retention: snapshot_ref.retention.clone(),
            });
        }

        Ok(TargetRefInfo {
            base_snapshot_id: metadata.current_snapshot_id(),
            expected_snapshot_id: None,
            retention: SnapshotRetention::branch(None, None, None),
        })
    }
}

struct TargetRefInfo {
    base_snapshot_id: Option<i64>,
    expected_snapshot_id: Option<i64>,
    retention: SnapshotRetention,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{DataContentType, DataFileBuilder, DataFileFormat, ManifestStatus, Struct};

    fn make_data_file_with_spec(spec_id: i32, path: &str) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(spec_id)
            .partition(Struct::empty())
            .build()
            .unwrap()
    }

    fn make_manifest_entry_with_spec(spec_id: i32, path: &str) -> ManifestEntry {
        let data_file = make_data_file_with_spec(spec_id, path);
        ManifestEntry::builder()
            .status(ManifestStatus::Added)
            .data_file(data_file)
            .build()
    }

    #[test]
    fn test_group_files_by_spec_empty() {
        let files: Vec<DataFile> = vec![];
        let groups = group_files_by_spec(files);
        assert!(groups.is_empty());
    }

    #[test]
    fn test_group_files_by_spec_single_spec() {
        let files = vec![
            make_data_file_with_spec(0, "file1.parquet"),
            make_data_file_with_spec(0, "file2.parquet"),
        ];
        let groups = group_files_by_spec(files);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[&0].len(), 2);
    }

    #[test]
    fn test_group_files_by_spec_multiple_specs() {
        let files = vec![
            make_data_file_with_spec(0, "file1.parquet"),
            make_data_file_with_spec(1, "file2.parquet"),
            make_data_file_with_spec(0, "file3.parquet"),
            make_data_file_with_spec(2, "file4.parquet"),
        ];
        let groups = group_files_by_spec(files);

        assert_eq!(groups.len(), 3);
        assert_eq!(groups[&0].len(), 2);
        assert_eq!(groups[&1].len(), 1);
        assert_eq!(groups[&2].len(), 1);
    }

    #[test]
    fn test_group_files_by_spec_deterministic_order() {
        // BTreeMap should produce deterministic iteration order sorted by spec_id
        let files = vec![
            make_data_file_with_spec(2, "file1.parquet"),
            make_data_file_with_spec(0, "file2.parquet"),
            make_data_file_with_spec(1, "file3.parquet"),
        ];
        let groups = group_files_by_spec(files);

        // Verify iteration order is sorted by spec_id
        let spec_ids: Vec<i32> = groups.keys().copied().collect();
        assert_eq!(spec_ids, vec![0, 1, 2]);
    }

    #[test]
    fn test_group_entries_by_spec_empty() {
        let entries: Vec<ManifestEntry> = vec![];
        let groups = group_entries_by_spec(entries);
        assert!(groups.is_empty());
    }

    #[test]
    fn test_group_entries_by_spec_single_spec() {
        let entries = vec![
            make_manifest_entry_with_spec(0, "file1.parquet"),
            make_manifest_entry_with_spec(0, "file2.parquet"),
        ];
        let groups = group_entries_by_spec(entries);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[&0].len(), 2);
    }

    #[test]
    fn test_group_entries_by_spec_multiple_specs() {
        let entries = vec![
            make_manifest_entry_with_spec(0, "file1.parquet"),
            make_manifest_entry_with_spec(1, "file2.parquet"),
            make_manifest_entry_with_spec(0, "file3.parquet"),
        ];
        let groups = group_entries_by_spec(entries);

        assert_eq!(groups.len(), 2);
        assert_eq!(groups[&0].len(), 2);
        assert_eq!(groups[&1].len(), 1);
    }

    #[test]
    fn test_group_entries_by_spec_deterministic_order() {
        // BTreeMap should produce deterministic iteration order sorted by spec_id
        let entries = vec![
            make_manifest_entry_with_spec(2, "file1.parquet"),
            make_manifest_entry_with_spec(0, "file2.parquet"),
            make_manifest_entry_with_spec(1, "file3.parquet"),
        ];
        let groups = group_entries_by_spec(entries);

        // Verify iteration order is sorted by spec_id
        let spec_ids: Vec<i32> = groups.keys().copied().collect();
        assert_eq!(spec_ids, vec![0, 1, 2]);
    }
}
