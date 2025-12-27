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

//! Partitions metadata table implementation.
//!
//! Provides the `$partitions` metadata table that shows partition-level summaries
//! including file counts, record counts, and byte totals.
//!
//! See: <https://iceberg.apache.org/spec/#partitions>

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Date32Builder, Decimal128Builder, FixedSizeBinaryBuilder, Float32Builder,
    Float64Builder, GenericBinaryBuilder, Int32Builder, Int64Builder, PrimitiveBuilder,
    StringBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{ArrayRef, RecordBatch, StructArray};
use arrow_schema::Field;
use futures::{StreamExt, stream};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::Result;
use crate::arrow::{schema_to_arrow_schema, type_to_arrow_type};
use crate::scan::ArrowRecordBatchStream;
use crate::spec::{
    DataContentType, Literal, ManifestStatus, NestedField, PrimitiveType, StructType, Type,
};
use crate::table::Table;

/// Key for partition aggregation combining spec_id and partition values.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PartitionKey {
    spec_id: i32,
    partition: crate::spec::Struct,
}

/// Aggregated statistics for a partition.
#[derive(Debug, Clone, Default)]
struct PartitionStats {
    /// Count of records in data files
    record_count: i64,
    /// Count of data files
    file_count: i32,
    /// Total size in bytes of data files
    total_data_file_size_in_bytes: i64,
    /// Count of records in position delete files
    position_delete_record_count: i64,
    /// Count of position delete files
    position_delete_file_count: i32,
    /// Count of records in equality delete files
    equality_delete_record_count: i64,
    /// Count of equality delete files
    equality_delete_file_count: i32,
}

/// Partitions table showing partition-level summaries.
///
/// This table provides aggregated statistics per partition including:
/// - File counts (data files, position deletes, equality deletes)
/// - Record counts
/// - Total bytes
///
/// # Schema
///
/// | Column | Type | Description |
/// |--------|------|-------------|
/// | `partition` | `Struct` | Partition values |
/// | `spec_id` | `Int` | Partition spec ID |
/// | `record_count` | `Long` | Count of records in data files |
/// | `file_count` | `Int` | Count of data files |
/// | `total_data_file_size_in_bytes` | `Long` | Total size of data files in bytes |
/// | `position_delete_record_count` | `Long` | Count of records in position delete files |
/// | `position_delete_file_count` | `Int` | Count of position delete files |
/// | `equality_delete_record_count` | `Long` | Count of records in equality delete files |
/// | `equality_delete_file_count` | `Int` | Count of equality delete files |
pub struct PartitionsTable<'a> {
    table: &'a Table,
}

impl<'a> PartitionsTable<'a> {
    /// Create a new Partitions table instance.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Returns the Iceberg schema for the partitions table.
    ///
    /// The `partition` field is a struct type that is the union of all partition specs
    /// in the table. For unpartitioned tables, the partition field is omitted.
    pub fn schema(&self) -> crate::spec::Schema {
        let partition_type = self.build_unified_partition_type(self.table.metadata());

        let mut fields = Vec::new();

        // Only include partition field if table is partitioned
        if !partition_type.fields().is_empty() {
            fields.push(NestedField::required(
                1,
                "partition",
                Type::Struct(partition_type),
            ));
            fields.push(NestedField::required(
                2,
                "spec_id",
                Type::Primitive(PrimitiveType::Int),
            ));
        }

        fields.extend([
            NestedField::required(3, "record_count", Type::Primitive(PrimitiveType::Long)),
            NestedField::required(4, "file_count", Type::Primitive(PrimitiveType::Int)),
            NestedField::required(
                5,
                "total_data_file_size_in_bytes",
                Type::Primitive(PrimitiveType::Long),
            ),
            NestedField::required(
                6,
                "position_delete_record_count",
                Type::Primitive(PrimitiveType::Long),
            ),
            NestedField::required(
                7,
                "position_delete_file_count",
                Type::Primitive(PrimitiveType::Int),
            ),
            NestedField::required(
                8,
                "equality_delete_record_count",
                Type::Primitive(PrimitiveType::Long),
            ),
            NestedField::required(
                9,
                "equality_delete_file_count",
                Type::Primitive(PrimitiveType::Int),
            ),
        ]);

        crate::spec::Schema::builder()
            .with_fields(fields.into_iter().map(|f| f.into()))
            .build()
            .unwrap()
    }

    /// Scans the partitions table and returns Arrow record batches.
    pub async fn scan(&self) -> Result<ArrowRecordBatchStream> {
        let schema = schema_to_arrow_schema(&self.schema())?;
        let metadata = self.table.metadata();
        let file_io = self.table.file_io();

        let unified_partition_type = self.build_unified_partition_type(metadata);
        let is_partitioned = !unified_partition_type.fields().is_empty();

        // Aggregate statistics by partition
        let mut partition_stats: HashMap<PartitionKey, PartitionStats> = HashMap::new();

        if let Some(snapshot) = metadata.current_snapshot() {
            let manifest_list = snapshot
                .load_manifest_list(file_io, &self.table.metadata_ref())
                .await?;

            for manifest_file in manifest_list.entries() {
                let manifest = manifest_file.load_manifest(file_io).await?;

                for entry in manifest.entries() {
                    // Only include files that are currently part of the snapshot
                    if entry.status == ManifestStatus::Deleted {
                        continue;
                    }

                    let data_file = &entry.data_file;
                    let key = PartitionKey {
                        spec_id: data_file.partition_spec_id,
                        partition: data_file.partition().clone(),
                    };

                    let stats = partition_stats.entry(key).or_default();

                    match data_file.content_type() {
                        DataContentType::Data => {
                            stats.record_count += data_file.record_count() as i64;
                            stats.file_count += 1;
                            stats.total_data_file_size_in_bytes +=
                                data_file.file_size_in_bytes() as i64;
                        }
                        DataContentType::PositionDeletes => {
                            stats.position_delete_record_count += data_file.record_count() as i64;
                            stats.position_delete_file_count += 1;
                        }
                        DataContentType::EqualityDeletes => {
                            stats.equality_delete_record_count += data_file.record_count() as i64;
                            stats.equality_delete_file_count += 1;
                        }
                    }
                }
            }
        }

        // Sort partitions for deterministic output
        let mut sorted_partitions: Vec<_> = partition_stats.into_iter().collect();
        sorted_partitions.sort_by(|a, b| {
            a.0.spec_id
                .cmp(&b.0.spec_id)
                .then_with(|| format!("{:?}", a.0.partition).cmp(&format!("{:?}", b.0.partition)))
        });

        // Build arrays
        let mut partition_data: Vec<(i32, crate::spec::Struct)> = Vec::new();
        let mut spec_id = PrimitiveBuilder::<Int32Type>::new();
        let mut record_count = PrimitiveBuilder::<Int64Type>::new();
        let mut file_count = PrimitiveBuilder::<Int32Type>::new();
        let mut total_data_file_size_in_bytes = PrimitiveBuilder::<Int64Type>::new();
        let mut position_delete_record_count = PrimitiveBuilder::<Int64Type>::new();
        let mut position_delete_file_count = PrimitiveBuilder::<Int32Type>::new();
        let mut equality_delete_record_count = PrimitiveBuilder::<Int64Type>::new();
        let mut equality_delete_file_count = PrimitiveBuilder::<Int32Type>::new();

        for (key, stats) in sorted_partitions {
            partition_data.push((key.spec_id, key.partition));
            spec_id.append_value(key.spec_id);
            record_count.append_value(stats.record_count);
            file_count.append_value(stats.file_count);
            total_data_file_size_in_bytes.append_value(stats.total_data_file_size_in_bytes);
            position_delete_record_count.append_value(stats.position_delete_record_count);
            position_delete_file_count.append_value(stats.position_delete_file_count);
            equality_delete_record_count.append_value(stats.equality_delete_record_count);
            equality_delete_file_count.append_value(stats.equality_delete_file_count);
        }

        let mut columns: Vec<ArrayRef> = Vec::new();

        if is_partitioned {
            let partition_array =
                self.build_partition_array(&unified_partition_type, &partition_data, metadata)?;
            columns.push(partition_array);
            columns.push(Arc::new(spec_id.finish()));
        }

        columns.extend([
            Arc::new(record_count.finish()) as ArrayRef,
            Arc::new(file_count.finish()),
            Arc::new(total_data_file_size_in_bytes.finish()),
            Arc::new(position_delete_record_count.finish()),
            Arc::new(position_delete_file_count.finish()),
            Arc::new(equality_delete_record_count.finish()),
            Arc::new(equality_delete_file_count.finish()),
        ]);

        let batch = RecordBatch::try_new(Arc::new(schema), columns)?;
        Ok(stream::iter(vec![Ok(batch)]).boxed())
    }

    /// Finds a schema that can successfully produce the partition type for the given spec.
    fn partition_type_for_spec(
        spec: &crate::spec::PartitionSpec,
        metadata: &crate::spec::TableMetadata,
    ) -> Option<StructType> {
        // Try current schema first
        if let Ok(partition_type) = spec.partition_type(metadata.current_schema()) {
            return Some(partition_type);
        }

        // Fall back to trying all schemas in the table's history
        for schema in metadata.schemas_iter() {
            if let Ok(partition_type) = spec.partition_type(schema) {
                return Some(partition_type);
            }
        }

        None
    }

    /// Builds a unified partition type from all partition specs in the table.
    fn build_unified_partition_type(&self, metadata: &crate::spec::TableMetadata) -> StructType {
        let mut seen_field_ids = std::collections::HashSet::new();
        let mut unified_fields: Vec<Arc<NestedField>> = Vec::new();

        // Sort specs by spec_id for deterministic field ordering
        let mut specs: Vec<_> = metadata.partition_specs_iter().collect();
        specs.sort_by_key(|spec| spec.spec_id());

        for spec in specs {
            if let Some(partition_type) = Self::partition_type_for_spec(spec, metadata) {
                for field in partition_type.fields() {
                    if seen_field_ids.insert(field.id) {
                        unified_fields.push(field.clone());
                    }
                }
            }
        }

        StructType::new(unified_fields)
    }

    /// Builds an Arrow StructArray from partition values, handling partition spec evolution.
    fn build_partition_array(
        &self,
        output_partition_type: &StructType,
        partition_data: &[(i32, crate::spec::Struct)],
        metadata: &crate::spec::TableMetadata,
    ) -> Result<ArrayRef> {
        let output_fields = output_partition_type.fields();
        if output_fields.is_empty() {
            return Ok(Arc::new(StructArray::new_empty_fields(
                partition_data.len(),
                None,
            )));
        }

        let mut field_arrays: Vec<(Arc<Field>, ArrayRef)> = Vec::with_capacity(output_fields.len());

        for output_field in output_fields.iter() {
            let arrow_type = type_to_arrow_type(&output_field.field_type)?;
            let arrow_field = Arc::new(
                Field::new(
                    &output_field.name,
                    arrow_type.clone(),
                    !output_field.required,
                )
                .with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    output_field.id.to_string(),
                )])),
            );

            let array = self.build_partition_field_array_with_evolution(
                output_field,
                partition_data,
                metadata,
            )?;

            field_arrays.push((arrow_field, array));
        }

        Ok(Arc::new(StructArray::from(field_arrays)))
    }

    /// Builds an Arrow array for a single partition field, handling spec evolution.
    fn build_partition_field_array_with_evolution(
        &self,
        output_field: &NestedField,
        partition_data: &[(i32, crate::spec::Struct)],
        metadata: &crate::spec::TableMetadata,
    ) -> Result<ArrayRef> {
        let mut values: Vec<Option<Literal>> = Vec::with_capacity(partition_data.len());

        for (spec_id, partition_struct) in partition_data {
            let file_spec = metadata.partition_spec_by_id(*spec_id);

            let value = if let Some(spec) = file_spec {
                if let Some(spec_type) = Self::partition_type_for_spec(spec, metadata) {
                    let field_idx = spec_type
                        .fields()
                        .iter()
                        .position(|f| f.id == output_field.id);

                    if let Some(idx) = field_idx {
                        partition_struct.iter().nth(idx).flatten().cloned()
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            };

            values.push(value);
        }

        self.build_literal_array(&output_field.field_type, &values)
    }

    /// Builds an Arrow array from a vector of optional Literal values.
    fn build_literal_array(
        &self,
        field_type: &Type,
        values: &[Option<Literal>],
    ) -> Result<ArrayRef> {
        match field_type {
            Type::Primitive(prim) => self.build_primitive_literal_array(prim, values),
            _ => Err(crate::Error::new(
                crate::ErrorKind::FeatureUnsupported,
                "Non-primitive partition field types are not yet supported in partitions table",
            )),
        }
    }

    /// Builds an Arrow array for primitive literals.
    fn build_primitive_literal_array(
        &self,
        prim_type: &PrimitiveType,
        values: &[Option<Literal>],
    ) -> Result<ArrayRef> {
        match prim_type {
            PrimitiveType::Boolean => {
                let mut builder = BooleanBuilder::with_capacity(values.len());
                for v in values {
                    match v {
                        Some(Literal::Primitive(crate::spec::PrimitiveLiteral::Boolean(b))) => {
                            builder.append_value(*b);
                        }
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            PrimitiveType::Int => {
                let mut builder = Int32Builder::with_capacity(values.len());
                for v in values {
                    match v {
                        Some(Literal::Primitive(crate::spec::PrimitiveLiteral::Int(i))) => {
                            builder.append_value(*i);
                        }
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            PrimitiveType::Long => {
                let mut builder = Int64Builder::with_capacity(values.len());
                for v in values {
                    match v {
                        Some(Literal::Primitive(crate::spec::PrimitiveLiteral::Long(l))) => {
                            builder.append_value(*l);
                        }
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            PrimitiveType::Float => {
                let mut builder = Float32Builder::with_capacity(values.len());
                for v in values {
                    match v {
                        Some(Literal::Primitive(crate::spec::PrimitiveLiteral::Float(f))) => {
                            builder.append_value(f.0);
                        }
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            PrimitiveType::Double => {
                let mut builder = Float64Builder::with_capacity(values.len());
                for v in values {
                    match v {
                        Some(Literal::Primitive(crate::spec::PrimitiveLiteral::Double(d))) => {
                            builder.append_value(d.0);
                        }
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            PrimitiveType::Date => {
                let mut builder = Date32Builder::with_capacity(values.len());
                for v in values {
                    match v {
                        Some(Literal::Primitive(crate::spec::PrimitiveLiteral::Int(i))) => {
                            builder.append_value(*i);
                        }
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            PrimitiveType::Timestamp | PrimitiveType::Timestamptz => {
                let mut builder = TimestampMicrosecondBuilder::with_capacity(values.len());
                if matches!(prim_type, PrimitiveType::Timestamptz) {
                    builder = builder.with_timezone("+00:00");
                }
                for v in values {
                    match v {
                        Some(Literal::Primitive(crate::spec::PrimitiveLiteral::Long(l))) => {
                            builder.append_value(*l);
                        }
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            PrimitiveType::String => {
                let mut builder = StringBuilder::with_capacity(values.len(), 256);
                for v in values {
                    match v {
                        Some(Literal::Primitive(crate::spec::PrimitiveLiteral::String(s))) => {
                            builder.append_value(s);
                        }
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            PrimitiveType::Uuid => {
                let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), 16);
                for v in values {
                    match v {
                        Some(Literal::Primitive(crate::spec::PrimitiveLiteral::UInt128(u))) => {
                            builder.append_value(u.to_be_bytes())?;
                        }
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            PrimitiveType::Binary | PrimitiveType::Fixed(_) => {
                let mut builder = GenericBinaryBuilder::<i64>::new();
                for v in values {
                    match v {
                        Some(Literal::Primitive(crate::spec::PrimitiveLiteral::Binary(b))) => {
                            builder.append_value(b);
                        }
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            PrimitiveType::Decimal { precision, scale } => {
                let mut builder = Decimal128Builder::with_capacity(values.len())
                    .with_precision_and_scale(*precision as u8, *scale as i8)?;
                for v in values {
                    match v {
                        Some(Literal::Primitive(crate::spec::PrimitiveLiteral::Int128(i))) => {
                            builder.append_value(*i);
                        }
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            PrimitiveType::Time => {
                let mut builder = Int64Builder::with_capacity(values.len());
                for v in values {
                    match v {
                        Some(Literal::Primitive(crate::spec::PrimitiveLiteral::Long(l))) => {
                            builder.append_value(*l);
                        }
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs => {
                let mut builder =
                    arrow_array::builder::TimestampNanosecondBuilder::with_capacity(values.len());
                if matches!(prim_type, PrimitiveType::TimestamptzNs) {
                    builder = builder.with_timezone("+00:00");
                }
                for v in values {
                    match v {
                        Some(Literal::Primitive(crate::spec::PrimitiveLiteral::Long(l))) => {
                            builder.append_value(*l);
                        }
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::cast::AsArray;
    use expect_test::expect;
    use futures::TryStreamExt;

    use crate::scan::tests::TableTestFixture;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, ManifestEntry,
        ManifestListWriter, ManifestStatus, ManifestWriterBuilder, Struct,
    };
    use crate::test_utils::check_record_batches;

    #[tokio::test]
    async fn test_partitions_table() {
        let fixture = TableTestFixture::new();
        let current_snapshot = fixture.table.metadata().current_snapshot().unwrap();
        let current_schema = current_snapshot.schema(fixture.table.metadata()).unwrap();
        let current_partition_spec = fixture.table.metadata().default_partition_spec();

        let mut writer = ManifestWriterBuilder::new(
            fixture.next_manifest_file(),
            Some(current_snapshot.snapshot_id()),
            None,
            current_schema.clone(),
            current_partition_spec.as_ref().clone(),
        )
        .build_v2_data();

        // Add files to partition x=100 (2 data files)
        for i in 0..2 {
            writer
                .add_entry(
                    ManifestEntry::builder()
                        .status(ManifestStatus::Added)
                        .data_file(
                            DataFileBuilder::default()
                                .partition_spec_id(0)
                                .content(DataContentType::Data)
                                .file_path(format!("{}/x100_{}.parquet", &fixture.table_location, i))
                                .file_format(DataFileFormat::Parquet)
                                .file_size_in_bytes(1000)
                                .record_count(100)
                                .partition(Struct::from_iter([Some(Literal::long(100))]))
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .unwrap();
        }

        // Add files to partition x=200 (1 data file)
        writer
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::Data)
                            .file_path(format!("{}/x200.parquet", &fixture.table_location))
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(500)
                            .record_count(50)
                            .partition(Struct::from_iter([Some(Literal::long(200))]))
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();

        let data_manifest = writer.write_manifest_file().await.unwrap();

        // Add delete files to partition x=100
        let mut delete_writer = ManifestWriterBuilder::new(
            fixture.next_manifest_file(),
            Some(current_snapshot.snapshot_id()),
            None,
            current_schema.clone(),
            current_partition_spec.as_ref().clone(),
        )
        .build_v2_deletes();

        // Position delete file
        delete_writer
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::PositionDeletes)
                            .file_path(format!("{}/pos_delete.parquet", &fixture.table_location))
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(200)
                            .record_count(10)
                            .partition(Struct::from_iter([Some(Literal::long(100))]))
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();

        // Equality delete file
        delete_writer
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::EqualityDeletes)
                            .file_path(format!("{}/eq_delete.parquet", &fixture.table_location))
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(150)
                            .record_count(5)
                            .partition(Struct::from_iter([Some(Literal::long(100))]))
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();

        let delete_manifest = delete_writer.write_manifest_file().await.unwrap();

        // Write manifest list
        let mut manifest_list_write = ManifestListWriter::v2(
            fixture
                .table
                .file_io()
                .new_output(current_snapshot.manifest_list())
                .unwrap(),
            current_snapshot.snapshot_id(),
            current_snapshot.parent_snapshot_id(),
            current_snapshot.sequence_number(),
        );
        manifest_list_write
            .add_manifests(vec![data_manifest, delete_manifest].into_iter())
            .unwrap();
        manifest_list_write.close().await.unwrap();

        // Scan partitions table
        let batch_stream = fixture.table.inspect().partitions().scan().await.unwrap();
        let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2); // Two partitions: x=100 and x=200

        // Check partition x=100 stats
        let partition_col = batch.column(0).as_struct();
        let x_col = partition_col
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>();
        assert_eq!(x_col.value(0), 100);
        assert_eq!(x_col.value(1), 200);

        // Check file counts for x=100 (first row)
        let record_count_col = batch
            .column(2)
            .as_primitive::<arrow_array::types::Int64Type>();
        let file_count_col = batch
            .column(3)
            .as_primitive::<arrow_array::types::Int32Type>();
        let pos_delete_count = batch
            .column(5)
            .as_primitive::<arrow_array::types::Int64Type>();
        let eq_delete_count = batch
            .column(7)
            .as_primitive::<arrow_array::types::Int64Type>();

        // x=100: 2 data files * 100 records = 200 records
        assert_eq!(record_count_col.value(0), 200);
        assert_eq!(file_count_col.value(0), 2);
        assert_eq!(pos_delete_count.value(0), 10);
        assert_eq!(eq_delete_count.value(0), 5);

        // x=200: 1 data file * 50 records = 50 records
        assert_eq!(record_count_col.value(1), 50);
        assert_eq!(file_count_col.value(1), 1);
        assert_eq!(pos_delete_count.value(1), 0);
        assert_eq!(eq_delete_count.value(1), 0);
    }

    #[tokio::test]
    async fn test_partitions_table_schema() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        let batch_stream = fixture.table.inspect().partitions().scan().await.unwrap();

        check_record_batches(
            batch_stream.try_collect::<Vec<_>>().await.unwrap(),
            expect![[r#"
                Field { "partition": Struct("x": Int64, metadata: {"PARQUET:field_id": "1000"}), metadata: {"PARQUET:field_id": "1"} },
                Field { "spec_id": Int32, metadata: {"PARQUET:field_id": "2"} },
                Field { "record_count": Int64, metadata: {"PARQUET:field_id": "3"} },
                Field { "file_count": Int32, metadata: {"PARQUET:field_id": "4"} },
                Field { "total_data_file_size_in_bytes": Int64, metadata: {"PARQUET:field_id": "5"} },
                Field { "position_delete_record_count": Int64, metadata: {"PARQUET:field_id": "6"} },
                Field { "position_delete_file_count": Int32, metadata: {"PARQUET:field_id": "7"} },
                Field { "equality_delete_record_count": Int64, metadata: {"PARQUET:field_id": "8"} },
                Field { "equality_delete_file_count": Int32, metadata: {"PARQUET:field_id": "9"} }"#]],
            expect![[r#"
                partition: StructArray
                -- validity:
                [
                  valid,
                  valid,
                ]
                [
                -- child 0: "x" (Int64)
                PrimitiveArray<Int64>
                [
                  100,
                  300,
                ]
                ],
                spec_id: PrimitiveArray<Int32>
                [
                  0,
                  0,
                ],
                record_count: PrimitiveArray<Int64>
                [
                  1,
                  1,
                ],
                file_count: PrimitiveArray<Int32>
                [
                  1,
                  1,
                ],
                total_data_file_size_in_bytes: PrimitiveArray<Int64>
                [
                  100,
                  100,
                ],
                position_delete_record_count: PrimitiveArray<Int64>
                [
                  0,
                  0,
                ],
                position_delete_file_count: PrimitiveArray<Int32>
                [
                  0,
                  0,
                ],
                equality_delete_record_count: PrimitiveArray<Int64>
                [
                  0,
                  0,
                ],
                equality_delete_file_count: PrimitiveArray<Int32>
                [
                  0,
                  0,
                ]"#]],
            &[],
            None,
        );
    }
}
