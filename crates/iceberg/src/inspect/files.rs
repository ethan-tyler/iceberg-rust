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

//! Files metadata table implementation.
//!
//! Provides the `$files` metadata table that lists all data and delete files
//! in the current snapshot with their partition values, statistics, and metadata.
//!
//! See: <https://iceberg.apache.org/spec/#files>

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Date32Builder, Decimal128Builder, FixedSizeBinaryBuilder, Float32Builder,
    Float64Builder, GenericBinaryBuilder, GenericListBuilder, Int32Builder, Int64Builder,
    ListBuilder, MapBuilder, MapFieldNames, PrimitiveBuilder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{ArrayRef, RecordBatch, StructArray};
use arrow_schema::{DataType, Field};
use futures::{StreamExt, stream};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::Result;
use crate::arrow::{DEFAULT_MAP_FIELD_NAME, schema_to_arrow_schema, type_to_arrow_type};
use crate::scan::ArrowRecordBatchStream;
use crate::spec::{
    ListType, Literal, MAP_KEY_FIELD_NAME, MAP_VALUE_FIELD_NAME, ManifestStatus, MapType,
    NestedField, PrimitiveType, Type,
};
use crate::table::Table;

/// Files table listing all data and delete files in the current snapshot.
///
/// This table exposes detailed file-level metadata including paths, formats,
/// partition values, record counts, file sizes, and column-level statistics.
///
/// # Schema
///
/// | Column | Type | Description |
/// |--------|------|-------------|
/// | `content` | `Int` | File content type: 0=DATA, 1=POSITION_DELETES, 2=EQUALITY_DELETES |
/// | `file_path` | `String` | Full URI path to the file |
/// | `file_format` | `String` | File format: PARQUET, AVRO, ORC |
/// | `spec_id` | `Int` | Partition spec ID used for this file |
/// | `record_count` | `Long` | Number of records in the file |
/// | `file_size_in_bytes` | `Long` | File size in bytes |
/// | `column_sizes` | `Map<Int, Long>` (nullable) | Column ID to size mapping |
/// | `value_counts` | `Map<Int, Long>` (nullable) | Column ID to value count mapping |
/// | `null_value_counts` | `Map<Int, Long>` (nullable) | Column ID to null count mapping |
/// | `nan_value_counts` | `Map<Int, Long>` (nullable) | Column ID to NaN count mapping |
/// | `lower_bounds` | `Map<Int, Binary>` (nullable) | Column ID to lower bound mapping |
/// | `upper_bounds` | `Map<Int, Binary>` (nullable) | Column ID to upper bound mapping |
/// | `key_metadata` | `Binary` (nullable) | Encryption key metadata |
/// | `split_offsets` | `List<Long>` (nullable) | Split offsets for parallel reading |
/// | `equality_ids` | `List<Int>` (nullable) | Equality field IDs (for equality deletes) |
/// | `sort_order_id` | `Int` (nullable) | Sort order ID |
pub struct FilesTable<'a> {
    table: &'a Table,
}

impl<'a> FilesTable<'a> {
    /// Create a new Files table instance.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Returns the Iceberg schema for the files table.
    ///
    /// The `partition` field is a struct type that is the union of all partition specs
    /// in the table, allowing files from any spec to have their partition values represented.
    /// For tables with partition evolution, this ensures all partition fields are present.
    pub fn schema(&self) -> crate::spec::Schema {
        // Build unified partition type from all partition specs in the table
        let partition_type = self.build_unified_partition_type(self.table.metadata());

        let fields = vec![
            NestedField::required(1, "content", Type::Primitive(PrimitiveType::Int)),
            NestedField::required(2, "file_path", Type::Primitive(PrimitiveType::String)),
            NestedField::required(3, "file_format", Type::Primitive(PrimitiveType::String)),
            NestedField::required(4, "spec_id", Type::Primitive(PrimitiveType::Int)),
            // Note: partition struct field IDs come from the partition spec itself
            NestedField::required(5, "partition", Type::Struct(partition_type)),
            NestedField::required(6, "record_count", Type::Primitive(PrimitiveType::Long)),
            NestedField::required(
                7,
                "file_size_in_bytes",
                Type::Primitive(PrimitiveType::Long),
            ),
            NestedField::optional(
                8,
                "column_sizes",
                Type::Map(MapType::new(
                    Arc::new(NestedField::required(
                        9,
                        "key",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::required(
                        10,
                        "value",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                )),
            ),
            NestedField::optional(
                11,
                "value_counts",
                Type::Map(MapType::new(
                    Arc::new(NestedField::required(
                        12,
                        "key",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::required(
                        13,
                        "value",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                )),
            ),
            NestedField::optional(
                14,
                "null_value_counts",
                Type::Map(MapType::new(
                    Arc::new(NestedField::required(
                        15,
                        "key",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::required(
                        16,
                        "value",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                )),
            ),
            NestedField::optional(
                17,
                "nan_value_counts",
                Type::Map(MapType::new(
                    Arc::new(NestedField::required(
                        18,
                        "key",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::required(
                        19,
                        "value",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                )),
            ),
            NestedField::optional(
                20,
                "lower_bounds",
                Type::Map(MapType::new(
                    Arc::new(NestedField::required(
                        21,
                        "key",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::required(
                        22,
                        "value",
                        Type::Primitive(PrimitiveType::Binary),
                    )),
                )),
            ),
            NestedField::optional(
                23,
                "upper_bounds",
                Type::Map(MapType::new(
                    Arc::new(NestedField::required(
                        24,
                        "key",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::required(
                        25,
                        "value",
                        Type::Primitive(PrimitiveType::Binary),
                    )),
                )),
            ),
            NestedField::optional(26, "key_metadata", Type::Primitive(PrimitiveType::Binary)),
            NestedField::optional(
                27,
                "split_offsets",
                Type::List(ListType::new(Arc::new(NestedField::required(
                    28,
                    "element",
                    Type::Primitive(PrimitiveType::Long),
                )))),
            ),
            NestedField::optional(
                29,
                "equality_ids",
                Type::List(ListType::new(Arc::new(NestedField::required(
                    30,
                    "element",
                    Type::Primitive(PrimitiveType::Int),
                )))),
            ),
            NestedField::optional(31, "sort_order_id", Type::Primitive(PrimitiveType::Int)),
        ];

        crate::spec::Schema::builder()
            .with_fields(fields.into_iter().map(|f| f.into()))
            .build()
            .unwrap()
    }

    /// Scans the files table and returns Arrow record batches.
    pub async fn scan(&self) -> Result<ArrowRecordBatchStream> {
        let schema = schema_to_arrow_schema(&self.schema())?;
        let metadata = self.table.metadata();
        let file_io = self.table.file_io();

        // Build unified partition type from all partition specs for output schema
        // This ensures files from any partition spec have their values correctly represented
        let unified_partition_type = self.build_unified_partition_type(metadata);

        // Builders for the columns
        let mut content = PrimitiveBuilder::<Int32Type>::new();
        let mut file_path = StringBuilder::new();
        let mut file_format = StringBuilder::new();
        let mut spec_id = PrimitiveBuilder::<Int32Type>::new();
        // Store (partition_spec_id, partition_values) to handle spec evolution
        let mut partition_data: Vec<(i32, crate::spec::Struct)> = Vec::new();
        let mut record_count = PrimitiveBuilder::<Int64Type>::new();
        let mut file_size_in_bytes = PrimitiveBuilder::<Int64Type>::new();
        let mut column_sizes = self.int_long_map_builder(9, 10);
        let mut value_counts = self.int_long_map_builder(12, 13);
        let mut null_value_counts = self.int_long_map_builder(15, 16);
        let mut nan_value_counts = self.int_long_map_builder(18, 19);
        let mut lower_bounds = self.int_binary_map_builder(21, 22);
        let mut upper_bounds = self.int_binary_map_builder(24, 25);
        let mut key_metadata = GenericBinaryBuilder::<i64>::new();
        let mut split_offsets = self.long_list_builder(28);
        let mut equality_ids = self.int_list_builder(30);
        let mut sort_order_id = PrimitiveBuilder::<Int32Type>::new();

        // Get current snapshot and iterate through manifest entries
        if let Some(snapshot) = metadata.current_snapshot() {
            let manifest_list = snapshot
                .load_manifest_list(file_io, &self.table.metadata_ref())
                .await?;

            for manifest_file in manifest_list.entries() {
                let manifest = manifest_file.load_manifest(file_io).await?;

                for entry in manifest.entries() {
                    // Only include files that are currently part of the snapshot
                    // (i.e., Added or Existing, not Deleted)
                    if entry.status == ManifestStatus::Deleted {
                        continue;
                    }

                    let data_file = &entry.data_file;

                    content.append_value(data_file.content_type() as i32);
                    file_path.append_value(data_file.file_path());
                    file_format.append_value(data_file.file_format().to_string().to_uppercase());
                    spec_id.append_value(data_file.partition_spec_id);
                    partition_data
                        .push((data_file.partition_spec_id, data_file.partition().clone()));
                    record_count.append_value(data_file.record_count() as i64);
                    file_size_in_bytes.append_value(data_file.file_size_in_bytes() as i64);

                    // column_sizes
                    self.append_int_long_map(&mut column_sizes, data_file.column_sizes());

                    // value_counts
                    self.append_int_long_map(&mut value_counts, data_file.value_counts());

                    // null_value_counts
                    self.append_int_long_map(&mut null_value_counts, data_file.null_value_counts());

                    // nan_value_counts
                    self.append_int_long_map(&mut nan_value_counts, data_file.nan_value_counts());

                    // lower_bounds - need to serialize Datum to bytes
                    self.append_datum_map(&mut lower_bounds, data_file.lower_bounds())?;

                    // upper_bounds - need to serialize Datum to bytes
                    self.append_datum_map(&mut upper_bounds, data_file.upper_bounds())?;

                    // key_metadata
                    if let Some(km) = data_file.key_metadata() {
                        key_metadata.append_value(km);
                    } else {
                        key_metadata.append_null();
                    }

                    // split_offsets
                    if let Some(offsets) = data_file.split_offsets() {
                        if offsets.is_empty() {
                            split_offsets.append_null();
                        } else {
                            let values = split_offsets.values();
                            for offset in offsets {
                                values.append_value(*offset);
                            }
                            split_offsets.append(true);
                        }
                    } else {
                        split_offsets.append_null();
                    }

                    // equality_ids
                    if let Some(eq_ids) = data_file.equality_ids() {
                        let values = equality_ids.values();
                        for id in eq_ids {
                            values.append_value(id);
                        }
                        equality_ids.append(true);
                    } else {
                        equality_ids.append_null();
                    }

                    // sort_order_id
                    sort_order_id.append_option(data_file.sort_order_id());
                }
            }
        }

        // Build partition struct array, mapping each file's partition values to the unified spec
        let partition_array =
            self.build_partition_array(&unified_partition_type, &partition_data, metadata)?;

        let batch = RecordBatch::try_new(Arc::new(schema), vec![
            Arc::new(content.finish()),
            Arc::new(file_path.finish()),
            Arc::new(file_format.finish()),
            Arc::new(spec_id.finish()),
            partition_array,
            Arc::new(record_count.finish()),
            Arc::new(file_size_in_bytes.finish()),
            Arc::new(column_sizes.finish()),
            Arc::new(value_counts.finish()),
            Arc::new(null_value_counts.finish()),
            Arc::new(nan_value_counts.finish()),
            Arc::new(lower_bounds.finish()),
            Arc::new(upper_bounds.finish()),
            Arc::new(key_metadata.finish()),
            Arc::new(split_offsets.finish()),
            Arc::new(equality_ids.finish()),
            Arc::new(sort_order_id.finish()),
        ])?;

        Ok(stream::iter(vec![Ok(batch)]).boxed())
    }

    /// Finds a schema that can successfully produce the partition type for the given spec.
    ///
    /// When a table undergoes schema evolution (e.g., columns dropped), partition specs
    /// created under older schemas may reference source column IDs that no longer exist
    /// in the current schema. This method tries each schema in the table's history to
    /// find one that contains all the source columns needed by the partition spec.
    ///
    /// Returns the partition type if a suitable schema is found, None otherwise.
    fn partition_type_for_spec(
        spec: &crate::spec::PartitionSpec,
        metadata: &crate::spec::TableMetadata,
    ) -> Option<crate::spec::StructType> {
        // Try current schema first (most common case)
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
    ///
    /// When a table undergoes partition evolution, different files may be written with
    /// different partition specs. To correctly represent all files in the files metadata
    /// table, we need a partition schema that is the union of all partition fields across
    /// all specs.
    ///
    /// This method:
    /// 1. Iterates through all partition specs in the table metadata (sorted by spec_id)
    /// 2. For each spec, finds a schema that can resolve its source columns
    /// 3. Collects all partition fields by their field ID (ensuring uniqueness)
    /// 4. Returns a StructType containing all unique partition fields
    ///
    /// Fields are deduplicated by field ID - if the same field ID appears in multiple specs,
    /// only the first occurrence is kept (types should be consistent across specs for the
    /// same field ID per Iceberg spec rules).
    ///
    /// Specs are processed in spec_id order to ensure deterministic field ordering in the
    /// output schema, regardless of HashMap iteration order.
    fn build_unified_partition_type(
        &self,
        metadata: &crate::spec::TableMetadata,
    ) -> crate::spec::StructType {
        let mut seen_field_ids = std::collections::HashSet::new();
        let mut unified_fields: Vec<Arc<NestedField>> = Vec::new();

        // Sort specs by spec_id for deterministic field ordering
        let mut specs: Vec<_> = metadata.partition_specs_iter().collect();
        specs.sort_by_key(|spec| spec.spec_id());

        for spec in specs {
            if let Some(partition_type) = Self::partition_type_for_spec(spec, metadata) {
                for field in partition_type.fields() {
                    if seen_field_ids.insert(field.id) {
                        // First time seeing this field ID - add to unified type
                        unified_fields.push(field.clone());
                    }
                }
            }
        }

        crate::spec::StructType::new(unified_fields)
    }

    /// Builds an Arrow StructArray from partition values, handling partition spec evolution.
    ///
    /// The output schema is based on `output_partition_type`, which should be the unified
    /// partition type containing fields from all partition specs. For each file, we look up
    /// its actual partition spec and map values by field ID to the output schema. This
    /// correctly handles partition evolution where files may have been written with different
    /// partition specs - fields that exist in the file's spec are populated, and fields that
    /// don't exist in that spec are null.
    fn build_partition_array(
        &self,
        output_partition_type: &crate::spec::StructType,
        partition_data: &[(i32, crate::spec::Struct)],
        metadata: &crate::spec::TableMetadata,
    ) -> Result<ArrayRef> {
        let output_fields = output_partition_type.fields();
        if output_fields.is_empty() {
            // Empty partition spec - return empty struct array
            return Ok(Arc::new(StructArray::new_empty_fields(
                partition_data.len(),
                None,
            )));
        }

        // Build arrays for each output field
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
    ///
    /// For each file, looks up its partition spec and finds the corresponding field by ID,
    /// then extracts the value at that field's position in the file's partition struct.
    /// Uses `partition_type_for_spec` to find a suitable schema for specs that may have
    /// been created under older table schemas.
    fn build_partition_field_array_with_evolution(
        &self,
        output_field: &crate::spec::NestedField,
        partition_data: &[(i32, crate::spec::Struct)],
        metadata: &crate::spec::TableMetadata,
    ) -> Result<ArrayRef> {
        // Collect values for this field from all files
        let mut values: Vec<Option<Literal>> = Vec::with_capacity(partition_data.len());

        for (spec_id, partition_struct) in partition_data {
            // Look up the file's partition spec
            let file_spec = metadata.partition_spec_by_id(*spec_id);

            let value = if let Some(spec) = file_spec {
                // Get the partition type for this spec, trying historical schemas if needed
                if let Some(spec_type) = Self::partition_type_for_spec(spec, metadata) {
                    // Find the field position by field ID in this spec's partition type
                    let field_idx = spec_type
                        .fields()
                        .iter()
                        .position(|f| f.id == output_field.id);

                    if let Some(idx) = field_idx {
                        // Extract value at this position
                        partition_struct.iter().nth(idx).flatten().cloned()
                    } else {
                        // Field doesn't exist in this spec (partition evolution)
                        None
                    }
                } else {
                    None
                }
            } else {
                // Unknown spec - emit null
                None
            };

            values.push(value);
        }

        // Build the appropriate typed array
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
                "Non-primitive partition field types are not yet supported in files table",
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

    fn int_long_map_builder(
        &self,
        key_field_id: i32,
        value_field_id: i32,
    ) -> MapBuilder<PrimitiveBuilder<Int32Type>, PrimitiveBuilder<Int64Type>> {
        MapBuilder::new(
            Some(MapFieldNames {
                entry: DEFAULT_MAP_FIELD_NAME.to_string(),
                key: MAP_KEY_FIELD_NAME.to_string(),
                value: MAP_VALUE_FIELD_NAME.to_string(),
            }),
            PrimitiveBuilder::<Int32Type>::new(),
            PrimitiveBuilder::<Int64Type>::new(),
        )
        .with_keys_field(Arc::new(
            Field::new(MAP_KEY_FIELD_NAME, DataType::Int32, false).with_metadata(HashMap::from([
                (
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    key_field_id.to_string(),
                ),
            ])),
        ))
        .with_values_field(Arc::new(
            Field::new(MAP_VALUE_FIELD_NAME, DataType::Int64, false).with_metadata(HashMap::from(
                [(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    value_field_id.to_string(),
                )],
            )),
        ))
    }

    fn int_binary_map_builder(
        &self,
        key_field_id: i32,
        value_field_id: i32,
    ) -> MapBuilder<PrimitiveBuilder<Int32Type>, GenericBinaryBuilder<i64>> {
        MapBuilder::new(
            Some(MapFieldNames {
                entry: DEFAULT_MAP_FIELD_NAME.to_string(),
                key: MAP_KEY_FIELD_NAME.to_string(),
                value: MAP_VALUE_FIELD_NAME.to_string(),
            }),
            PrimitiveBuilder::<Int32Type>::new(),
            GenericBinaryBuilder::<i64>::new(),
        )
        .with_keys_field(Arc::new(
            Field::new(MAP_KEY_FIELD_NAME, DataType::Int32, false).with_metadata(HashMap::from([
                (
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    key_field_id.to_string(),
                ),
            ])),
        ))
        .with_values_field(Arc::new(
            Field::new(MAP_VALUE_FIELD_NAME, DataType::LargeBinary, false).with_metadata(
                HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    value_field_id.to_string(),
                )]),
            ),
        ))
    }

    fn long_list_builder(
        &self,
        element_field_id: i32,
    ) -> GenericListBuilder<i32, PrimitiveBuilder<Int64Type>> {
        ListBuilder::new(PrimitiveBuilder::<Int64Type>::new()).with_field(Arc::new(
            Field::new("element", DataType::Int64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                element_field_id.to_string(),
            )])),
        ))
    }

    fn int_list_builder(
        &self,
        element_field_id: i32,
    ) -> GenericListBuilder<i32, PrimitiveBuilder<Int32Type>> {
        ListBuilder::new(PrimitiveBuilder::<Int32Type>::new()).with_field(Arc::new(
            Field::new("element", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                element_field_id.to_string(),
            )])),
        ))
    }

    fn append_int_long_map(
        &self,
        builder: &mut MapBuilder<PrimitiveBuilder<Int32Type>, PrimitiveBuilder<Int64Type>>,
        map: &HashMap<i32, u64>,
    ) {
        if map.is_empty() {
            // Empty map from DataFile means stats weren't collected, emit null
            builder.append(false).unwrap();
        } else {
            // Sort keys for consistent output
            let mut entries: Vec<_> = map.iter().collect();
            entries.sort_by_key(|(k, _)| *k);

            for (key, value) in entries {
                builder.keys().append_value(*key);
                builder.values().append_value(*value as i64);
            }
            builder.append(true).unwrap();
        }
    }

    fn append_datum_map(
        &self,
        builder: &mut MapBuilder<PrimitiveBuilder<Int32Type>, GenericBinaryBuilder<i64>>,
        map: &HashMap<i32, crate::spec::Datum>,
    ) -> Result<()> {
        if map.is_empty() {
            // Empty map from DataFile means bounds weren't collected, emit null
            builder.append(false)?;
        } else {
            // Sort keys for consistent output
            let mut entries: Vec<_> = map.iter().collect();
            entries.sort_by_key(|(k, _)| *k);

            for (key, datum) in entries {
                builder.keys().append_value(*key);
                let bytes = datum.to_bytes()?;
                builder.values().append_value(&bytes);
            }
            builder.append(true)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    use expect_test::expect;
    use futures::TryStreamExt;

    use crate::scan::tests::TableTestFixture;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Datum, Literal, ManifestEntry,
        ManifestListWriter, ManifestStatus, ManifestWriterBuilder, Struct,
    };
    use crate::test_utils::check_record_batches;

    #[tokio::test]
    async fn test_files_table() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        let batch_stream = fixture.table.inspect().files().scan().await.unwrap();

        check_record_batches(
            batch_stream.try_collect::<Vec<_>>().await.unwrap(),
            expect![[r#"
                Field { "content": Int32, metadata: {"PARQUET:field_id": "1"} },
                Field { "file_path": Utf8, metadata: {"PARQUET:field_id": "2"} },
                Field { "file_format": Utf8, metadata: {"PARQUET:field_id": "3"} },
                Field { "spec_id": Int32, metadata: {"PARQUET:field_id": "4"} },
                Field { "partition": Struct("x": Int64, metadata: {"PARQUET:field_id": "1000"}), metadata: {"PARQUET:field_id": "5"} },
                Field { "record_count": Int64, metadata: {"PARQUET:field_id": "6"} },
                Field { "file_size_in_bytes": Int64, metadata: {"PARQUET:field_id": "7"} },
                Field { "column_sizes": nullable Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "9"}, "value": non-null Int64, metadata: {"PARQUET:field_id": "10"}), unsorted), metadata: {"PARQUET:field_id": "8"} },
                Field { "value_counts": nullable Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "12"}, "value": non-null Int64, metadata: {"PARQUET:field_id": "13"}), unsorted), metadata: {"PARQUET:field_id": "11"} },
                Field { "null_value_counts": nullable Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "15"}, "value": non-null Int64, metadata: {"PARQUET:field_id": "16"}), unsorted), metadata: {"PARQUET:field_id": "14"} },
                Field { "nan_value_counts": nullable Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "18"}, "value": non-null Int64, metadata: {"PARQUET:field_id": "19"}), unsorted), metadata: {"PARQUET:field_id": "17"} },
                Field { "lower_bounds": nullable Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "21"}, "value": non-null LargeBinary, metadata: {"PARQUET:field_id": "22"}), unsorted), metadata: {"PARQUET:field_id": "20"} },
                Field { "upper_bounds": nullable Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "24"}, "value": non-null LargeBinary, metadata: {"PARQUET:field_id": "25"}), unsorted), metadata: {"PARQUET:field_id": "23"} },
                Field { "key_metadata": nullable LargeBinary, metadata: {"PARQUET:field_id": "26"} },
                Field { "split_offsets": nullable List(non-null Int64, field: 'element', metadata: {"PARQUET:field_id": "28"}), metadata: {"PARQUET:field_id": "27"} },
                Field { "equality_ids": nullable List(non-null Int32, field: 'element', metadata: {"PARQUET:field_id": "30"}), metadata: {"PARQUET:field_id": "29"} },
                Field { "sort_order_id": nullable Int32, metadata: {"PARQUET:field_id": "31"} }"#]],
            expect![[r#"
                content: PrimitiveArray<Int32>
                [
                  0,
                  0,
                ],
                file_path: (skipped),
                file_format: StringArray
                [
                  "PARQUET",
                  "PARQUET",
                ],
                spec_id: PrimitiveArray<Int32>
                [
                  0,
                  0,
                ],
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
                record_count: PrimitiveArray<Int64>
                [
                  1,
                  1,
                ],
                file_size_in_bytes: PrimitiveArray<Int64>
                [
                  100,
                  100,
                ],
                column_sizes: (skipped),
                value_counts: (skipped),
                null_value_counts: (skipped),
                nan_value_counts: (skipped),
                lower_bounds: (skipped),
                upper_bounds: (skipped),
                key_metadata: LargeBinaryArray
                [
                  null,
                  null,
                ],
                split_offsets: ListArray
                [
                  null,
                  null,
                ],
                equality_ids: ListArray
                [
                  null,
                  null,
                ],
                sort_order_id: PrimitiveArray<Int32>
                [
                  null,
                  null,
                ]"#]],
            &[
                "file_path",
                "column_sizes",
                "value_counts",
                "null_value_counts",
                "nan_value_counts",
                "lower_bounds",
                "upper_bounds",
            ],
            Some("file_path"),
        );
    }

    /// Test that delete files (position deletes, equality deletes) are correctly
    /// represented in the files table with their content type and equality_ids.
    #[tokio::test]
    async fn test_files_table_with_delete_files() {
        let fixture = TableTestFixture::new();
        let current_snapshot = fixture.table.metadata().current_snapshot().unwrap();
        let current_schema = current_snapshot.schema(fixture.table.metadata()).unwrap();
        let current_partition_spec = fixture.table.metadata().default_partition_spec();

        // Write data manifest with data file
        let mut data_writer = ManifestWriterBuilder::new(
            fixture.next_manifest_file(),
            Some(current_snapshot.snapshot_id()),
            None,
            current_schema.clone(),
            current_partition_spec.as_ref().clone(),
        )
        .build_v2_data();

        // Add a data file (content = 0)
        data_writer
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::Data)
                            .file_path(format!("{}/data.parquet", &fixture.table_location))
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

        let data_manifest = data_writer.write_manifest_file().await.unwrap();

        // Write delete manifest with position delete and equality delete files
        let mut delete_writer = ManifestWriterBuilder::new(
            fixture.next_manifest_file(),
            Some(current_snapshot.snapshot_id()),
            None,
            current_schema.clone(),
            current_partition_spec.as_ref().clone(),
        )
        .build_v2_deletes();

        // Add a position delete file (content = 1)
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

        // Add an equality delete file (content = 2) with equality_ids
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
                            .equality_ids(Some(vec![1, 2])) // Delete based on columns 1 and 2
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();

        let delete_manifest = delete_writer.write_manifest_file().await.unwrap();

        // Write manifest list with both data and delete manifests
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

        // Scan and verify
        let batch_stream = fixture.table.inspect().files().scan().await.unwrap();
        let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);

        // Check content types: 0=Data, 1=PositionDeletes, 2=EqualityDeletes
        let content_col = batch
            .column(0)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(content_col.value(0), 0); // Data file
        assert_eq!(content_col.value(1), 1); // Position delete
        assert_eq!(content_col.value(2), 2); // Equality delete

        // Check equality_ids - only the equality delete file should have them
        // Column indices: 0=content, 1=file_path, 2=file_format, 3=spec_id, 4=partition,
        // 5=record_count, 6=file_size, 7=column_sizes, ..., 15=equality_ids, 16=sort_order_id
        let equality_ids_col = batch.column(15).as_list::<i32>();
        assert!(equality_ids_col.is_null(0)); // Data file - no equality ids
        assert!(equality_ids_col.is_null(1)); // Position delete - no equality ids
        assert!(!equality_ids_col.is_null(2)); // Equality delete - has equality ids

        let eq_ids = equality_ids_col.value(2);
        let eq_ids_arr = eq_ids.as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(eq_ids_arr.len(), 2);
        assert_eq!(eq_ids_arr.value(0), 1);
        assert_eq!(eq_ids_arr.value(1), 2);
    }

    /// Test that the partition column contains correct partition values for each file.
    /// The test fixture uses a partition spec with a single field "x" of type Long.
    #[tokio::test]
    async fn test_files_table_partition_values() {
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

        // Add files with different partition values
        writer
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::Data)
                            .file_path(format!("{}/part1.parquet", &fixture.table_location))
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(100)
                            .record_count(10)
                            .partition(Struct::from_iter([Some(Literal::long(100))]))
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();

        writer
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::Data)
                            .file_path(format!("{}/part2.parquet", &fixture.table_location))
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(200)
                            .record_count(20)
                            .partition(Struct::from_iter([Some(Literal::long(200))]))
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();

        // Add a file with null partition value
        writer
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::Data)
                            .file_path(format!("{}/part_null.parquet", &fixture.table_location))
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(300)
                            .record_count(30)
                            .partition(Struct::from_iter([None]))
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();

        let manifest = writer.write_manifest_file().await.unwrap();

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
            .add_manifests(vec![manifest].into_iter())
            .unwrap();
        manifest_list_write.close().await.unwrap();

        // Scan and verify partition values
        let batch_stream = fixture.table.inspect().files().scan().await.unwrap();
        let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);

        // Verify partition column (index 4) is a struct with field "x"
        let partition_col = batch.column(4).as_struct();
        assert_eq!(partition_col.num_columns(), 1);

        // Get the "x" field values
        let x_col = partition_col
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>();

        // Verify partition values: 100, 200, null
        assert_eq!(x_col.value(0), 100);
        assert_eq!(x_col.value(1), 200);
        assert!(x_col.is_null(2));
    }

    /// Test that column-level metrics (column_sizes, value_counts, null_value_counts,
    /// nan_value_counts, lower_bounds, upper_bounds) are correctly serialized.
    #[tokio::test]
    async fn test_files_table_with_metrics() {
        let fixture = TableTestFixture::new();
        let current_snapshot = fixture.table.metadata().current_snapshot().unwrap();
        let current_schema = current_snapshot.schema(fixture.table.metadata()).unwrap();
        let current_partition_spec = fixture.table.metadata().default_partition_spec();

        // Build a data file with comprehensive metrics
        let mut column_sizes = HashMap::new();
        column_sizes.insert(1, 500u64);
        column_sizes.insert(2, 300u64);

        let mut value_counts = HashMap::new();
        value_counts.insert(1, 100u64);
        value_counts.insert(2, 100u64);

        let mut null_value_counts = HashMap::new();
        null_value_counts.insert(1, 5u64);
        null_value_counts.insert(2, 10u64);

        let mut nan_value_counts = HashMap::new();
        nan_value_counts.insert(1, 0u64);
        nan_value_counts.insert(2, 2u64);

        let mut lower_bounds = HashMap::new();
        lower_bounds.insert(1, Datum::long(10));
        lower_bounds.insert(2, Datum::long(20));

        let mut upper_bounds = HashMap::new();
        upper_bounds.insert(1, Datum::long(1000));
        upper_bounds.insert(2, Datum::long(2000));

        let data_file = DataFileBuilder::default()
            .partition_spec_id(0)
            .content(DataContentType::Data)
            .file_path(format!("{}/metrics_file.parquet", &fixture.table_location))
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(800)
            .record_count(100)
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            .column_sizes(column_sizes)
            .value_counts(value_counts)
            .null_value_counts(null_value_counts)
            .nan_value_counts(nan_value_counts)
            .lower_bounds(lower_bounds)
            .upper_bounds(upper_bounds)
            .split_offsets(Some(vec![0, 400]))
            .sort_order_id(0)
            .build()
            .unwrap();

        let mut writer = ManifestWriterBuilder::new(
            fixture.next_manifest_file(),
            Some(current_snapshot.snapshot_id()),
            None,
            current_schema.clone(),
            current_partition_spec.as_ref().clone(),
        )
        .build_v2_data();

        writer
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(data_file)
                    .build(),
            )
            .unwrap();

        let data_file_manifest = writer.write_manifest_file().await.unwrap();

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
            .add_manifests(vec![data_file_manifest].into_iter())
            .unwrap();
        manifest_list_write.close().await.unwrap();

        // Scan and verify
        let batch_stream = fixture.table.inspect().files().scan().await.unwrap();
        let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);

        // Column indices after adding partition column:
        // 0=content, 1=file_path, 2=file_format, 3=spec_id, 4=partition,
        // 5=record_count, 6=file_size, 7=column_sizes, 8=value_counts, 9=null_value_counts,
        // 10=nan_value_counts, 11=lower_bounds, 12=upper_bounds, 13=key_metadata,
        // 14=split_offsets, 15=equality_ids, 16=sort_order_id

        // Verify column_sizes map (column index 7)
        let column_sizes_col = batch.column(7).as_map();
        assert!(!column_sizes_col.is_null(0));
        let cs_keys = column_sizes_col
            .keys()
            .as_primitive::<arrow_array::types::Int32Type>();
        let cs_values = column_sizes_col
            .values()
            .as_primitive::<arrow_array::types::Int64Type>();
        // Map entries: {1: 500, 2: 300}
        assert_eq!(cs_keys.len(), 2);
        assert_eq!(cs_keys.value(0), 1);
        assert_eq!(cs_values.value(0), 500);
        assert_eq!(cs_keys.value(1), 2);
        assert_eq!(cs_values.value(1), 300);

        // Verify split_offsets list (column index 14)
        let split_offsets_col = batch.column(14).as_list::<i32>();
        assert!(!split_offsets_col.is_null(0));
        let offsets = split_offsets_col.value(0);
        let offsets_arr = offsets.as_primitive::<arrow_array::types::Int64Type>();
        assert_eq!(offsets_arr.len(), 2);
        assert_eq!(offsets_arr.value(0), 0);
        assert_eq!(offsets_arr.value(1), 400);

        // Verify sort_order_id (column index 16)
        let sort_order_col = batch
            .column(16)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert!(!sort_order_col.is_null(0));
        assert_eq!(sort_order_col.value(0), 0);
    }

    /// Test that files with ManifestStatus::Deleted are filtered out.
    #[tokio::test]
    async fn test_files_table_filters_deleted_entries() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        // The fixture creates:
        // - 1 Added file (1.parquet)
        // - 1 Deleted file (2.parquet) - should be filtered out
        // - 1 Existing file (3.parquet)

        let batch_stream = fixture.table.inspect().files().scan().await.unwrap();
        let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];

        // Only 2 files should appear (Added and Existing, not Deleted)
        assert_eq!(batch.num_rows(), 2);

        // Verify the file paths don't include the deleted file
        let file_path_col = batch.column(1).as_string::<i32>();
        let paths: Vec<&str> = (0..batch.num_rows())
            .map(|i| file_path_col.value(i))
            .collect();

        // Should have 1.parquet and 3.parquet, not 2.parquet
        assert!(paths.iter().any(|p| p.ends_with("/1.parquet")));
        assert!(paths.iter().any(|p| p.ends_with("/3.parquet")));
        assert!(!paths.iter().any(|p| p.ends_with("/2.parquet")));
    }

    /// Test that empty metrics maps emit null (not empty map).
    #[tokio::test]
    async fn test_files_table_null_metrics_for_missing_stats() {
        let fixture = TableTestFixture::new();
        let current_snapshot = fixture.table.metadata().current_snapshot().unwrap();
        let current_schema = current_snapshot.schema(fixture.table.metadata()).unwrap();
        let current_partition_spec = fixture.table.metadata().default_partition_spec();

        // Create a file with NO metrics (all empty HashMaps)
        let data_file = DataFileBuilder::default()
            .partition_spec_id(0)
            .content(DataContentType::Data)
            .file_path(format!("{}/no_stats.parquet", &fixture.table_location))
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(500)
            .record_count(50)
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            // No metrics set - all HashMap fields will be empty
            .build()
            .unwrap();

        let mut writer = ManifestWriterBuilder::new(
            fixture.next_manifest_file(),
            Some(current_snapshot.snapshot_id()),
            None,
            current_schema.clone(),
            current_partition_spec.as_ref().clone(),
        )
        .build_v2_data();

        writer
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(data_file)
                    .build(),
            )
            .unwrap();

        let data_file_manifest = writer.write_manifest_file().await.unwrap();

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
            .add_manifests(vec![data_file_manifest].into_iter())
            .unwrap();
        manifest_list_write.close().await.unwrap();

        // Scan and verify null semantics
        let batch_stream = fixture.table.inspect().files().scan().await.unwrap();
        let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);

        // All metric columns should be null (not empty maps)
        // Column indices: 0-4 = content/path/format/spec_id/partition, 5-6 = record_count/file_size
        // 7=column_sizes, 8=value_counts, 9=null_value_counts, 10=nan_value_counts, 11=lower_bounds, 12=upper_bounds
        let column_sizes = batch.column(7); // column_sizes
        let value_counts = batch.column(8); // value_counts
        let null_value_counts = batch.column(9); // null_value_counts
        let nan_value_counts = batch.column(10); // nan_value_counts
        let lower_bounds = batch.column(11); // lower_bounds
        let upper_bounds = batch.column(12); // upper_bounds

        assert!(
            column_sizes.is_null(0),
            "column_sizes should be null for missing stats"
        );
        assert!(
            value_counts.is_null(0),
            "value_counts should be null for missing stats"
        );
        assert!(
            null_value_counts.is_null(0),
            "null_value_counts should be null for missing stats"
        );
        assert!(
            nan_value_counts.is_null(0),
            "nan_value_counts should be null for missing stats"
        );
        assert!(
            lower_bounds.is_null(0),
            "lower_bounds should be null for missing stats"
        );
        assert!(
            upper_bounds.is_null(0),
            "upper_bounds should be null for missing stats"
        );
    }
}
