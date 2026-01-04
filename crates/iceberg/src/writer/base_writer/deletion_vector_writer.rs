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

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::delete_vector::DeleteVector;
use crate::io::FileIO;
use crate::puffin::{Blob, CompressionCodec, PuffinReader, PuffinWriter, DELETION_VECTOR_V1};
use crate::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, PartitionKey,
};
use crate::writer::base_writer::position_delete_writer::{
    PositionDeleteWriterConfig, POSITION_DELETE_FILE_PATH_FIELD_ID, POSITION_DELETE_POS_FIELD_ID,
};
use crate::writer::file_writer::location_generator::{FileNameGenerator, LocationGenerator};
use crate::writer::{IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

const PUFFIN_PROP_REFERENCED_DATA_FILE: &str = "referenced-data-file";
const PUFFIN_PROP_CARDINALITY: &str = "cardinality";
const PUFFIN_DELETE_VECTOR_SNAPSHOT_ID: i64 = -1;
const PUFFIN_DELETE_VECTOR_SEQUENCE_NUMBER: i64 = -1;

/// Builder for [`DeletionVectorWriter`].
#[derive(Clone, Debug)]
pub struct DeletionVectorWriterBuilder<L: LocationGenerator, F: FileNameGenerator> {
    file_io: FileIO,
    location_generator: L,
    file_name_generator: F,
    config: PositionDeleteWriterConfig,
}

impl<L, F> DeletionVectorWriterBuilder<L, F>
where
    L: LocationGenerator,
    F: FileNameGenerator,
{
    /// Creates a new builder with the given configuration.
    pub fn new(
        file_io: FileIO,
        location_generator: L,
        file_name_generator: F,
        config: PositionDeleteWriterConfig,
    ) -> Self {
        Self {
            file_io,
            location_generator,
            file_name_generator,
            config,
        }
    }
}

#[async_trait::async_trait]
impl<L, F> IcebergWriterBuilder for DeletionVectorWriterBuilder<L, F>
where
    L: LocationGenerator,
    F: FileNameGenerator,
{
    type R = DeletionVectorWriter<L, F>;

    async fn build(&self, partition_key: Option<PartitionKey>) -> Result<Self::R> {
        Ok(DeletionVectorWriter {
            file_io: self.file_io.clone(),
            location_generator: self.location_generator.clone(),
            file_name_generator: self.file_name_generator.clone(),
            config: self.config.clone(),
            partition_key,
            delete_vectors: HashMap::new(),
        })
    }
}

/// Writer for Puffin-based deletion vector files.
#[derive(Debug)]
pub struct DeletionVectorWriter<L: LocationGenerator, F: FileNameGenerator> {
    file_io: FileIO,
    location_generator: L,
    file_name_generator: F,
    config: PositionDeleteWriterConfig,
    partition_key: Option<PartitionKey>,
    delete_vectors: HashMap<String, DeleteVector>,
}

impl<L, F> DeletionVectorWriter<L, F>
where
    L: LocationGenerator,
    F: FileNameGenerator,
{
    fn validate_schema(&self, batch: &RecordBatch) -> Result<()> {
        let expected = self.config.delete_schema();
        let actual = batch.schema();

        if actual.fields().len() < 2 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Position delete batch must have at least 2 columns (file_path, pos), got {}",
                    actual.fields().len()
                ),
            ));
        }

        let file_path_field = actual.field(0);
        let expected_file_path_id = POSITION_DELETE_FILE_PATH_FIELD_ID.to_string();
        let file_path_field_id = file_path_field
            .metadata()
            .get(PARQUET_FIELD_ID_META_KEY)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Position delete file_path column must have field ID metadata (expected {POSITION_DELETE_FILE_PATH_FIELD_ID})"
                    ),
                )
            })?;
        if file_path_field_id != &expected_file_path_id {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Position delete file_path field ID must be {expected_file_path_id}, got {file_path_field_id}"
                ),
            ));
        }

        let pos_field = actual.field(1);
        let pos_field_id = pos_field.metadata().get(PARQUET_FIELD_ID_META_KEY).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Position delete pos column must have field ID metadata (expected {POSITION_DELETE_POS_FIELD_ID})"
                ),
            )
        })?;
        let expected_pos_id = POSITION_DELETE_POS_FIELD_ID.to_string();
        if pos_field_id != &expected_pos_id {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Position delete pos field ID must be {expected_pos_id}, got {pos_field_id}"
                ),
            ));
        }

        if expected.fields().len() != actual.fields().len() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Position delete batch schema does not match expected schema",
            ));
        }

        Ok(())
    }

    async fn write_delete_vector_file(
        &self,
        referenced_data_file: &str,
        delete_vector: &DeleteVector,
    ) -> Result<DataFile> {
        let file_name = self.file_name_generator.generate_file_name();
        let file_path =
            self.location_generator
                .generate_location(self.partition_key.as_ref(), &file_name);
        let output_file = self.file_io.new_output(file_path.clone())?;

        let mut writer = PuffinWriter::new(&output_file, HashMap::new(), false).await?;
        let blob = Blob::builder()
            .r#type(DELETION_VECTOR_V1.to_string())
            .fields(Vec::new())
            .snapshot_id(PUFFIN_DELETE_VECTOR_SNAPSHOT_ID)
            .sequence_number(PUFFIN_DELETE_VECTOR_SEQUENCE_NUMBER)
            .data(delete_vector.to_bytes()?)
            .properties(HashMap::from([
                (
                    PUFFIN_PROP_REFERENCED_DATA_FILE.to_string(),
                    referenced_data_file.to_string(),
                ),
                (
                    PUFFIN_PROP_CARDINALITY.to_string(),
                    delete_vector.len().to_string(),
                ),
            ]))
            .build();
        writer.add(blob, CompressionCodec::None).await?;
        writer.close().await?;

        let input_file = output_file.to_input_file();
        let file_size = input_file.metadata().await?.size;
        let puffin_reader = PuffinReader::new(input_file);
        let file_metadata = puffin_reader.file_metadata().await?;
        let blob_metadata = file_metadata.blobs.first().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Deletion vector Puffin file did not contain any blobs",
            )
        })?;

        let mut builder = DataFileBuilder::default();
        builder
            .content(DataContentType::PositionDeletes)
            .file_path(file_path)
            .file_format(DataFileFormat::Puffin)
            .file_size_in_bytes(file_size)
            .record_count(delete_vector.len())
            .referenced_data_file(Some(referenced_data_file.to_string()))
            .content_offset(Some(blob_metadata.offset as i64))
            .content_size_in_bytes(Some(blob_metadata.length as i64));

        if let Some(pk) = self.partition_key.as_ref() {
            builder.partition(pk.data().clone());
            builder.partition_spec_id(pk.spec().spec_id());
        }

        builder.build().map_err(|err| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Failed to build deletion vector data file: {err}"),
            )
        })
    }
}

#[async_trait::async_trait]
impl<L, F> IcebergWriter for DeletionVectorWriter<L, F>
where
    L: LocationGenerator,
    F: FileNameGenerator,
{
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.validate_schema(&batch)?;

        let file_path_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "First column (file_path) must be StringArray",
                )
            })?;
        let pos_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Second column (pos) must be Int64Array",
                )
            })?;

        for i in 0..file_path_array.len() {
            if file_path_array.is_null(i) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Position delete file_path at row {i} is null, but must be non-null"
                    ),
                ));
            }
            if pos_array.is_null(i) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("Position delete pos at row {i} is null, but must be non-null"),
                ));
            }

            let pos = pos_array.value(i);
            if pos < 0 {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Position delete pos at row {i} is negative ({pos}), must be >= 0"
                    ),
                ));
            }

            let path = file_path_array.value(i);
            let delete_vector = self
                .delete_vectors
                .entry(path.to_string())
                .or_default();
            delete_vector.insert(pos as u64);
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        let delete_vectors = std::mem::take(&mut self.delete_vectors);
        if delete_vectors.is_empty() {
            return Ok(Vec::new());
        }

        let mut outputs = Vec::with_capacity(delete_vectors.len());
        let mut sorted: Vec<_> = delete_vectors.into_iter().collect();
        sorted.sort_by(|left, right| left.0.cmp(&right.0));

        for (referenced_data_file, delete_vector) in sorted {
            if delete_vector.len() == 0 {
                continue;
            }
            outputs.push(
                self.write_delete_vector_file(&referenced_data_file, &delete_vector)
                    .await?,
            );
        }

        Ok(outputs)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
    use tempfile::TempDir;

    use super::*;
    use crate::io::FileIOBuilder;
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use crate::writer::{IcebergWriter, IcebergWriterBuilder};

    #[tokio::test]
    async fn test_deletion_vector_writer_writes_puffin_files() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("delete-vector".to_string(), None, DataFileFormat::Puffin);
        let config = PositionDeleteWriterConfig::new();
        let delete_schema = config.delete_schema().clone();

        let builder = DeletionVectorWriterBuilder::new(
            file_io.clone(),
            location_gen,
            file_name_gen,
            config,
        );
        let mut writer = builder.build(None).await?;

        let file_paths = Arc::new(StringArray::from(vec![
            "data-1.parquet",
            "data-1.parquet",
            "data-2.parquet",
        ]));
        let positions = Arc::new(Int64Array::from(vec![1, 3, 2]));
        let batch = RecordBatch::try_new(delete_schema, vec![file_paths, positions]).unwrap();

        writer.write(batch).await?;
        let delete_files = writer.close().await?;
        assert_eq!(delete_files.len(), 2);

        for delete_file in delete_files {
            assert_eq!(delete_file.file_format(), DataFileFormat::Puffin);
            assert_eq!(delete_file.content_type(), DataContentType::PositionDeletes);
            assert!(delete_file.content_offset().is_some());
            assert!(delete_file.content_size_in_bytes().is_some());
            let referenced = delete_file.referenced_data_file().unwrap();

            let input_file = file_io.new_input(delete_file.file_path.clone()).unwrap();
            let puffin_reader = PuffinReader::new(input_file);
            let file_metadata = puffin_reader.file_metadata().await.unwrap().clone();
            let blob_metadata = file_metadata.blobs.first().unwrap();
            let blob = puffin_reader.blob(blob_metadata).await.unwrap();
            let delete_vector = DeleteVector::from_bytes(blob.data()).unwrap();
            let mut positions: Vec<u64> = delete_vector.iter().collect();
            positions.sort();

            if referenced == "data-1.parquet" {
                assert_eq!(positions, vec![1, 3]);
                assert_eq!(delete_file.record_count, 2);
            } else if referenced == "data-2.parquet" {
                assert_eq!(positions, vec![2]);
                assert_eq!(delete_file.record_count, 1);
            } else {
                panic!("Unexpected referenced data file: {referenced}");
            }
        }

        Ok(())
    }
}
