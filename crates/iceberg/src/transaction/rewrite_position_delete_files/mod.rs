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

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use async_trait::async_trait;
use futures::TryStreamExt;
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use crate::arrow::ArrowReader;
use crate::error::Result;
use crate::spec::{
    DataContentType, DataFile, DataFileFormat, FormatVersion, ManifestContentType, ManifestEntry,
    ManifestFile, ManifestStatus, Operation, PartitionKey, Struct,
};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::writer::base_writer::position_delete_writer::{
    PositionDeleteFileWriterBuilder, PositionDeleteWriterConfig, position_delete_schema,
};
use crate::writer::file_writer::ParquetWriterBuilder;
use crate::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use crate::writer::{IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind};

/// Action to rewrite position delete files.
pub struct RewritePositionDeleteFilesAction {
    check_duplicate: bool,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
}

impl RewritePositionDeleteFilesAction {
    pub(crate) fn new() -> Self {
        Self {
            check_duplicate: true,
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
        }
    }

    /// Sets whether duplicate position deletes should be checked.
    pub fn with_check_duplicate(mut self, v: bool) -> Self {
        self.check_duplicate = v;
        self
    }

    /// Sets the commit UUID for the rewrite action.
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Sets key metadata for encrypted file handling.
    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }

    /// Sets snapshot properties for the commit.
    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }
}

#[async_trait]
impl TransactionAction for RewritePositionDeleteFilesAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        if table.metadata().format_version() == FormatVersion::V1 {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Position delete files are not supported in format version 1",
            ));
        }

        let Some(snapshot) = table.metadata().current_snapshot() else {
            return Ok(ActionCommit::new(vec![], vec![]));
        };

        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await?;

        let mut position_delete_entries: Vec<ManifestEntry> = Vec::new();

        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Deletes {
                continue;
            }

            let manifest = manifest_file.load_manifest(table.file_io()).await?;
            for entry in manifest.entries() {
                if entry.status == ManifestStatus::Deleted {
                    continue;
                }
                if entry.content_type() != DataContentType::PositionDeletes {
                    continue;
                }
                if entry.file_format() != DataFileFormat::Parquet {
                    return Err(Error::new(
                        ErrorKind::FeatureUnsupported,
                        "rewrite position delete files only supports Parquet delete files",
                    ));
                }
                position_delete_entries.push(entry.as_ref().clone());
            }
        }

        if position_delete_entries.is_empty() {
            return Ok(ActionCommit::new(vec![], vec![]));
        }

        let spec_unpartitioned: HashMap<i32, bool> = table
            .metadata()
            .partition_specs_iter()
            .map(|spec| (spec.spec_id(), spec.is_unpartitioned()))
            .collect();

        let mut partition_groups: HashMap<(Option<Struct>, i32), Vec<ManifestEntry>> =
            HashMap::new();

        for entry in position_delete_entries {
            if entry.sequence_number.is_none() || entry.file_sequence_number.is_none() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Position delete manifest entry is missing sequence numbers",
                ));
            }

            let spec_id = entry.data_file.partition_spec_id;
            let is_unpartitioned = spec_unpartitioned.get(&spec_id).copied().ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Delete file '{}' references unknown partition spec ID {spec_id}",
                        entry.data_file.file_path
                    ),
                )
            })?;

            let partition = if is_unpartitioned {
                None
            } else {
                Some(entry.data_file.partition().clone())
            };

            partition_groups
                .entry((partition, spec_id))
                .or_default()
                .push(entry);
        }

        let mut sorted_groups: Vec<_> = partition_groups
            .into_iter()
            .map(|((partition, spec_id), entries)| {
                let partition_repr = partition
                    .as_ref()
                    .map(|p| format!("{p:?}"))
                    .unwrap_or_default();
                ((spec_id, partition_repr), partition, entries)
            })
            .collect();
        sorted_groups.sort_by(|a, b| a.0.cmp(&b.0));

        let commit_uuid = self.commit_uuid.unwrap_or_else(Uuid::now_v7);

        let delete_config = PositionDeleteWriterConfig::new();
        let delete_schema = delete_config.delete_schema().clone();
        let iceberg_delete_schema = Arc::new(position_delete_schema(
            table.metadata().current_schema_id(),
        )?);

        let parquet_writer_builder =
            ParquetWriterBuilder::new(WriterProperties::default(), iceberg_delete_schema);
        let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
        let file_name_generator =
            DefaultFileNameGenerator::new("pos-delete".to_string(), None, DataFileFormat::Parquet);

        let mut added_delete_files: Vec<DataFile> = Vec::new();
        let mut deleted_entries: Vec<ManifestEntry> = Vec::new();
        let mut delete_sequence_number: Option<i64> = None;

        for ((spec_id, _repr), partition, entries) in sorted_groups {
            if entries.len() <= 1 {
                continue;
            }

            for entry in &entries {
                let seq = entry.sequence_number.ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Position delete manifest entry is missing sequence number",
                    )
                })?;
                delete_sequence_number = Some(delete_sequence_number.map_or(seq, |m| m.max(seq)));
            }

            let mut deletes: Vec<(String, i64)> = Vec::new();
            for entry in &entries {
                deletes.extend(read_position_delete_file(table, entry.file_path()).await?);
            }

            deletes.sort_by(|(a_path, a_pos), (b_path, b_pos)| {
                a_path.cmp(b_path).then_with(|| a_pos.cmp(b_pos))
            });
            deletes.dedup();

            let Some(partition_spec) = table.metadata().partition_spec_by_id(spec_id) else {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("Unknown partition spec ID {spec_id}"),
                ));
            };

            let partition_key = partition.map(|partition| {
                PartitionKey::new(
                    partition_spec.as_ref().clone(),
                    table.metadata().current_schema().clone(),
                    partition,
                )
            });

            let file_paths: Vec<String> = deletes.iter().map(|(p, _)| p.clone()).collect();
            let positions: Vec<i64> = deletes.iter().map(|(_, p)| *p).collect();

            let record_batch = RecordBatch::try_new(delete_schema.clone(), vec![
                Arc::new(StringArray::from(file_paths)),
                Arc::new(Int64Array::from(positions)),
            ])
            .map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Failed to build position delete record batch",
                )
                .with_source(e)
            })?;

            let rolling_builder = RollingFileWriterBuilder::new_with_default_file_size(
                parquet_writer_builder.clone(),
                table.file_io().clone(),
                location_generator.clone(),
                file_name_generator.clone(),
            );

            let mut writer =
                PositionDeleteFileWriterBuilder::new(rolling_builder, delete_config.clone())
                    .build(partition_key)
                    .await?;

            writer.write(record_batch).await?;
            let new_files = writer.close().await?;

            added_delete_files.extend(new_files);
            deleted_entries.extend(entries);
        }

        if added_delete_files.is_empty() {
            return Ok(ActionCommit::new(vec![], vec![]));
        }

        let snapshot_producer = SnapshotProducer::new(
            table,
            commit_uuid,
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            vec![],
            added_delete_files,
        );

        snapshot_producer.validate_added_delete_files()?;

        if self.check_duplicate {
            snapshot_producer.validate_duplicate_files().await?;
        }

        snapshot_producer
            .commit(
                RewritePositionDeleteFilesOperation {
                    deleted_entries,
                    delete_sequence_number,
                },
                DefaultManifestProcess,
            )
            .await
    }
}

struct RewritePositionDeleteFilesOperation {
    deleted_entries: Vec<ManifestEntry>,
    delete_sequence_number: Option<i64>,
}

impl SnapshotProduceOperation for RewritePositionDeleteFilesOperation {
    fn operation(&self) -> Operation {
        Operation::Replace
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        Ok(self.deleted_entries.clone())
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let Some(snapshot) = snapshot_produce.current_snapshot()? else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.table.file_io(),
                &snapshot_produce.table.metadata_ref(),
            )
            .await?;

        Ok(manifest_list.entries().to_vec())
    }

    fn delete_sequence_number(&self) -> Option<i64> {
        self.delete_sequence_number
    }
}

async fn read_position_delete_file(table: &Table, file_path: &str) -> Result<Vec<(String, i64)>> {
    let record_batch_stream_builder = ArrowReader::create_parquet_record_batch_stream_builder(
        file_path,
        table.file_io().clone(),
        false,
        None,
    )
    .await?;

    let record_batch_stream = record_batch_stream_builder
        .build()?
        .map_err(|e| Error::new(ErrorKind::Unexpected, format!("{e}")));

    let batches: Vec<RecordBatch> = record_batch_stream.try_collect().await?;
    let mut deletes: Vec<(String, i64)> = Vec::new();

    for batch in batches {
        if batch.num_columns() != 2 {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "rewrite position delete files expects delete files with exactly two columns",
            ));
        }

        let file_path_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Position delete file_path column must be Utf8",
                )
            })?;

        let pos_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Position delete pos column must be Int64",
                )
            })?;

        for i in 0..batch.num_rows() {
            if file_path_array.is_null(i) || pos_array.is_null(i) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Position delete file contains null values",
                ));
            }

            let pos = pos_array.value(i);
            if pos < 0 {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Position delete file contains negative positions",
                ));
            }

            deletes.push((file_path_array.value(i).to_string(), pos));
        }
    }

    Ok(deletes)
}
