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

//! Task-level writer for position delete files with partition support.
//!
//! This module provides `PositionDeleteTaskWriter` which handles writing position
//! delete files for partitioned tables. Unlike the regular `TaskWriter` which
//! computes partition keys from record batch data, this writer receives partition
//! keys from `FileScanTask` metadata (the partition of the data file being deleted).
//!
//! # Usage
//!
//! ```rust,ignore
//! // For partitioned tables
//! let writer = PositionDeleteTaskWriter::try_new(builder, schema, partition_spec)?;
//!
//! // Write position deletes with partition context from FileScanTask
//! writer.write("s3://bucket/data/file.parquet", &[0, 5, 10], Some(partition_key)).await?;
//!
//! // Close and collect delete files
//! let delete_files = writer.close().await?;
//! ```

use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use iceberg::spec::{DataFile, PartitionKey, PartitionSpecRef, SchemaRef};
use iceberg::writer::base_writer::position_delete_writer::{
    PositionDeleteFileWriterBuilder, PositionDeleteWriterConfig,
};
use iceberg::writer::file_writer::location_generator::{FileNameGenerator, LocationGenerator};
use iceberg::writer::file_writer::FileWriterBuilder;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Error, ErrorKind, Result};

/// Task-level writer for position delete files with partition support.
///
/// This writer handles position delete files for both partitioned and unpartitioned
/// tables. For partitioned tables, it uses a `FanoutWriter` to route position deletes
/// to partition-specific writers based on the partition key from `FileScanTask`.
///
/// # Key Difference from TaskWriter
///
/// Unlike `TaskWriter` which computes partition keys from incoming `RecordBatch` data,
/// `PositionDeleteTaskWriter` receives partition keys directly from caller (extracted
/// from `FileScanTask` metadata). This is because position deletes reference rows in
/// existing data files, and must use the SAME partition spec/values as those data files.
///
/// # Partition Evolution
///
/// When a table undergoes partition evolution, data files may have different partition
/// specs. This writer handles this by:
/// 1. Accepting partition keys with their original partition spec (from FileScanTask)
/// 2. Using FanoutWriter to create separate delete files per partition
/// 3. Each delete file gets the correct `partition_spec_id` from its partition key
pub struct PositionDeleteTaskWriter<B: FileWriterBuilder, L: LocationGenerator, F: FileNameGenerator>
{
    writer: SupportedDeleteWriter<B, L, F>,
    config: PositionDeleteWriterConfig,
}

/// Internal enum to hold different writer types based on partitioning strategy.
enum SupportedDeleteWriter<B: FileWriterBuilder, L: LocationGenerator, F: FileNameGenerator> {
    /// Writer for unpartitioned tables - single writer, no partition routing
    Unpartitioned(
        iceberg::writer::base_writer::position_delete_writer::PositionDeleteFileWriter<B, L, F>,
    ),
    /// Writer for partitioned tables - routes to partition-specific writers
    Fanout(FanoutWriter<PositionDeleteFileWriterBuilder<B, L, F>>),
}

impl<B, L, F> PositionDeleteTaskWriter<B, L, F>
where
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
{
    /// Create a new `PositionDeleteTaskWriter`.
    ///
    /// # Parameters
    ///
    /// * `writer_builder` - Builder for creating underlying position delete writers
    /// * `_schema` - The Iceberg schema reference (reserved for future use)
    /// * `partition_spec` - The partition specification reference
    ///
    /// # Returns
    ///
    /// Returns a new `PositionDeleteTaskWriter` instance, or an error if creation fails.
    ///
    /// # Writer Selection
    ///
    /// - If `partition_spec` is unpartitioned: creates a single position delete writer
    /// - If `partition_spec` is partitioned: creates a FanoutWriter for multi-partition support
    pub async fn try_new(
        writer_builder: PositionDeleteFileWriterBuilder<B, L, F>,
        _schema: SchemaRef,
        partition_spec: PartitionSpecRef,
    ) -> Result<Self> {
        let config = PositionDeleteWriterConfig::new();

        let writer = if partition_spec.is_unpartitioned() {
            // For unpartitioned tables, create a single writer with no partition key
            let inner = writer_builder.build(None).await?;
            SupportedDeleteWriter::Unpartitioned(inner)
        } else {
            // For partitioned tables, use FanoutWriter to handle multiple partitions
            SupportedDeleteWriter::Fanout(FanoutWriter::new(writer_builder))
        };

        Ok(Self { writer, config })
    }

    /// Write position deletes for a data file.
    ///
    /// # Parameters
    ///
    /// * `file_path` - Path to the data file being deleted from
    /// * `positions` - Row positions (0-indexed) to delete within the file
    /// * `partition_key` - Partition key for the data file (from `FileScanTask`).
    ///   Must be `Some` for partitioned tables, `None` for unpartitioned tables.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, or an error if the write fails.
    ///
    /// # Errors
    ///
    /// - Returns error if `partition_key` is `Some` for unpartitioned writer
    /// - Returns error if `partition_key` is `None` for partitioned writer
    /// - Returns error if underlying writer fails
    pub async fn write(
        &mut self,
        file_path: &str,
        positions: &[i64],
        partition_key: Option<PartitionKey>,
    ) -> Result<()> {
        if positions.is_empty() {
            return Ok(());
        }

        // Create RecordBatch with position delete schema
        let batch = self.create_delete_batch(file_path, positions)?;

        match &mut self.writer {
            SupportedDeleteWriter::Unpartitioned(writer) => {
                if partition_key.is_some() {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "Cannot write partitioned position deletes to unpartitioned writer",
                    ));
                }
                writer.write(batch).await
            }
            SupportedDeleteWriter::Fanout(writer) => {
                let pk = partition_key.ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Partition key required for partitioned position delete writer",
                    )
                })?;
                writer.write(pk, batch).await
            }
        }
    }

    /// Close the writer and return all position delete files.
    ///
    /// This consumes the writer to prevent further use after closing.
    ///
    /// # Returns
    ///
    /// Returns a `Vec<DataFile>` containing all written position delete files,
    /// each with:
    /// - `content_type`: `DataContentType::PositionDeletes`
    /// - `partition`: Partition values (if partitioned)
    /// - `partition_spec_id`: Spec ID from the partition key (if partitioned)
    /// - `referenced_data_file`: Set if all deletes in the file reference a single data file
    pub async fn close(self) -> Result<Vec<DataFile>> {
        match self.writer {
            SupportedDeleteWriter::Unpartitioned(mut writer) => writer.close().await,
            SupportedDeleteWriter::Fanout(writer) => writer.close().await,
        }
    }

    /// Create a RecordBatch with position delete schema from file path and positions.
    fn create_delete_batch(&self, file_path: &str, positions: &[i64]) -> Result<RecordBatch> {
        let delete_schema = self.config.delete_schema().clone();

        // Create file_path column (same value repeated for each position)
        let file_paths: Vec<&str> = vec![file_path; positions.len()];
        let file_path_array = Arc::new(StringArray::from(file_paths));

        // Create positions column
        let positions_array = Arc::new(Int64Array::from(positions.to_vec()));

        RecordBatch::try_new(delete_schema, vec![file_path_array, positions_array]).map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Failed to create position delete batch: {}", e),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use super::*;
    use iceberg::io::FileIOBuilder;
    use iceberg::spec::{
        DataContentType, DataFileFormat, Literal, NestedField, PartitionSpec, PrimitiveType,
        Schema, Struct, Type,
    };
    use iceberg::writer::base_writer::position_delete_writer::{
        POSITION_DELETE_FILE_PATH_FIELD_ID, POSITION_DELETE_POS_FIELD_ID,
    };
    use iceberg::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use iceberg::writer::file_writer::ParquetWriterBuilder;

    fn create_test_schema() -> Result<Arc<Schema>> {
        Ok(Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(3, "region", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])
                .build()?,
        ))
    }

    fn create_iceberg_delete_schema() -> Result<Arc<Schema>> {
        Ok(Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(
                        POSITION_DELETE_FILE_PATH_FIELD_ID,
                        "file_path",
                        Type::Primitive(PrimitiveType::String),
                    )
                    .into(),
                    NestedField::required(
                        POSITION_DELETE_POS_FIELD_ID,
                        "pos",
                        Type::Primitive(PrimitiveType::Long),
                    )
                    .into(),
                ])
                .build()?,
        ))
    }

    fn create_writer_builder(
        temp_dir: &TempDir,
    ) -> Result<
        PositionDeleteFileWriterBuilder<
            ParquetWriterBuilder,
            DefaultLocationGenerator,
            DefaultFileNameGenerator,
        >,
    > {
        let file_io = FileIOBuilder::new_fs_io().build()?;
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test-delete".to_string(), None, DataFileFormat::Parquet);

        let iceberg_delete_schema = create_iceberg_delete_schema()?;
        let parquet_writer_builder =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), iceberg_delete_schema);

        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            file_io,
            location_gen,
            file_name_gen,
        );

        let config = PositionDeleteWriterConfig::new();
        Ok(PositionDeleteFileWriterBuilder::new(
            rolling_writer_builder,
            config,
        ))
    }

    #[tokio::test]
    async fn test_unpartitioned_position_deletes() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let schema = create_test_schema()?;
        let partition_spec = Arc::new(PartitionSpec::builder(schema.clone()).build()?);

        let writer_builder = create_writer_builder(&temp_dir)?;
        let mut task_writer =
            PositionDeleteTaskWriter::try_new(writer_builder, schema, partition_spec).await?;

        // Write position deletes without partition key
        task_writer
            .write("s3://bucket/data/file1.parquet", &[0, 5, 10], None)
            .await?;
        task_writer
            .write("s3://bucket/data/file1.parquet", &[15, 20], None)
            .await?;

        let delete_files = task_writer.close().await?;

        assert!(!delete_files.is_empty());
        let delete_file = &delete_files[0];
        assert_eq!(delete_file.content_type(), DataContentType::PositionDeletes);
        assert_eq!(delete_file.record_count(), 5); // 3 + 2 positions
        assert_eq!(
            delete_file.referenced_data_file(),
            Some("s3://bucket/data/file1.parquet".to_string())
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_partitioned_position_deletes_single_partition() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let schema = create_test_schema()?;

        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(1)
                .add_partition_field("region", "region", iceberg::spec::Transform::Identity)?
                .build()?,
        );

        let writer_builder = create_writer_builder(&temp_dir)?;
        let mut task_writer = PositionDeleteTaskWriter::try_new(
            writer_builder,
            schema.clone(),
            partition_spec.clone(),
        )
        .await?;

        // Create partition key for "US" region
        let partition_value = Struct::from_iter([Some(Literal::string("US"))]);
        let partition_key =
            PartitionKey::new((*partition_spec).clone(), schema.clone(), partition_value.clone());

        // Write position deletes with partition key
        task_writer
            .write(
                "s3://bucket/data/region=US/file1.parquet",
                &[0, 5, 10],
                Some(partition_key.clone()),
            )
            .await?;
        task_writer
            .write(
                "s3://bucket/data/region=US/file2.parquet",
                &[3],
                Some(partition_key),
            )
            .await?;

        let delete_files = task_writer.close().await?;

        assert!(!delete_files.is_empty());
        let delete_file = &delete_files[0];
        assert_eq!(delete_file.content_type(), DataContentType::PositionDeletes);
        assert_eq!(delete_file.record_count(), 4);
        assert_eq!(delete_file.partition(), &partition_value);
        // Multiple data files referenced, so no single referenced_data_file
        assert_eq!(delete_file.referenced_data_file(), None);

        Ok(())
    }

    #[tokio::test]
    async fn test_partitioned_position_deletes_multiple_partitions() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let schema = create_test_schema()?;

        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(1)
                .add_partition_field("region", "region", iceberg::spec::Transform::Identity)?
                .build()?,
        );

        let writer_builder = create_writer_builder(&temp_dir)?;
        let mut task_writer = PositionDeleteTaskWriter::try_new(
            writer_builder,
            schema.clone(),
            partition_spec.clone(),
        )
        .await?;

        // Create partition keys for different regions
        let us_partition = Struct::from_iter([Some(Literal::string("US"))]);
        let us_key = PartitionKey::new((*partition_spec).clone(), schema.clone(), us_partition);

        let eu_partition = Struct::from_iter([Some(Literal::string("EU"))]);
        let eu_key =
            PartitionKey::new((*partition_spec).clone(), schema.clone(), eu_partition.clone());

        let asia_partition = Struct::from_iter([Some(Literal::string("ASIA"))]);
        let asia_key = PartitionKey::new(
            (*partition_spec).clone(),
            schema.clone(),
            asia_partition.clone(),
        );

        // Write position deletes to different partitions (interleaved)
        task_writer
            .write(
                "s3://bucket/data/region=US/file1.parquet",
                &[0, 5],
                Some(us_key.clone()),
            )
            .await?;
        task_writer
            .write(
                "s3://bucket/data/region=EU/file1.parquet",
                &[10, 15, 20],
                Some(eu_key.clone()),
            )
            .await?;
        task_writer
            .write(
                "s3://bucket/data/region=US/file2.parquet",
                &[1],
                Some(us_key),
            )
            .await?;
        task_writer
            .write(
                "s3://bucket/data/region=ASIA/file1.parquet",
                &[0],
                Some(asia_key),
            )
            .await?;

        let delete_files = task_writer.close().await?;

        // Should have 3 delete files (one per partition)
        assert_eq!(delete_files.len(), 3);

        // Verify each delete file has correct partition
        let mut partitions_found: std::collections::HashSet<_> = std::collections::HashSet::new();
        for df in &delete_files {
            assert_eq!(df.content_type(), DataContentType::PositionDeletes);
            partitions_found.insert(df.partition().clone());
        }

        assert!(partitions_found.contains(&Struct::from_iter([Some(Literal::string("US"))])));
        assert!(partitions_found.contains(&eu_partition));
        assert!(partitions_found.contains(&asia_partition));

        Ok(())
    }

    #[tokio::test]
    async fn test_empty_positions_no_write() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let schema = create_test_schema()?;
        let partition_spec = Arc::new(PartitionSpec::builder(schema.clone()).build()?);

        let writer_builder = create_writer_builder(&temp_dir)?;
        let mut task_writer =
            PositionDeleteTaskWriter::try_new(writer_builder, schema, partition_spec).await?;

        // Write with empty positions - should be no-op
        task_writer
            .write("s3://bucket/data/file1.parquet", &[], None)
            .await?;

        let delete_files = task_writer.close().await?;

        // No files should be created for empty writes
        assert!(delete_files.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_unpartitioned_rejects_partition_key() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let schema = create_test_schema()?;
        let partition_spec = Arc::new(PartitionSpec::builder(schema.clone()).build()?);

        let writer_builder = create_writer_builder(&temp_dir)?;
        let mut task_writer = PositionDeleteTaskWriter::try_new(
            writer_builder,
            schema.clone(),
            partition_spec.clone(),
        )
        .await?;

        // Try to write with partition key to unpartitioned writer
        let partition_value = Struct::from_iter([Some(Literal::string("US"))]);
        let partition_key =
            PartitionKey::new((*partition_spec).clone(), schema.clone(), partition_value);

        let result = task_writer
            .write(
                "s3://bucket/data/file1.parquet",
                &[0],
                Some(partition_key),
            )
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("unpartitioned"));

        Ok(())
    }

    #[tokio::test]
    async fn test_partitioned_requires_partition_key() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let schema = create_test_schema()?;

        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(1)
                .add_partition_field("region", "region", iceberg::spec::Transform::Identity)?
                .build()?,
        );

        let writer_builder = create_writer_builder(&temp_dir)?;
        let mut task_writer =
            PositionDeleteTaskWriter::try_new(writer_builder, schema, partition_spec).await?;

        // Try to write without partition key to partitioned writer
        let result = task_writer
            .write("s3://bucket/data/file1.parquet", &[0], None)
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Partition key required"));

        Ok(())
    }
}
