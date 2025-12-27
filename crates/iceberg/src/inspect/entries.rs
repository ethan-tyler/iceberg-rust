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

//! Entries metadata table implementation.
//!
//! Provides the `$entries` metadata table that shows low-level manifest entries
//! for debugging purposes. Each row represents a manifest entry with full details.
//!
//! See: <https://iceberg.apache.org/spec/#entries>

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
use crate::arrow::{schema_to_arrow_schema, type_to_arrow_type};
use crate::scan::ArrowRecordBatchStream;
use crate::spec::{
    ListType, Literal, MAP_KEY_FIELD_NAME, MAP_VALUE_FIELD_NAME, MapType, NestedField,
    PrimitiveType, StructType, Type,
};
use crate::table::Table;

/// Default map field name used in Arrow maps.
const DEFAULT_MAP_FIELD_NAME: &str = "key_value";

/// Entries table showing low-level manifest entries for debugging.
///
/// This table exposes all manifest entries including their status, sequence numbers,
/// and full data file details. Unlike the files table which only shows live files,
/// the entries table shows all entries including deleted ones.
///
/// # Schema
///
/// | Column | Type | Description |
/// |--------|------|-------------|
/// | `status` | `Int` | Entry status: 0=existing, 1=added, 2=deleted |
/// | `snapshot_id` | `Long` (nullable) | Snapshot ID when entry was added/deleted |
/// | `sequence_number` | `Long` (nullable) | Data sequence number |
/// | `file_sequence_number` | `Long` (nullable) | File sequence number |
/// | `data_file` | `Struct` | Full data file information |
pub struct EntriesTable<'a> {
    table: &'a Table,
}

impl<'a> EntriesTable<'a> {
    /// Create a new Entries table instance.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Returns the Iceberg schema for the entries table.
    pub fn schema(&self) -> crate::spec::Schema {
        // Build unified partition type from all partition specs
        let partition_type = self.build_unified_partition_type(self.table.metadata());

        let data_file_fields = self.data_file_struct_fields(&partition_type);

        let fields = vec![
            NestedField::required(0, "status", Type::Primitive(PrimitiveType::Int)),
            NestedField::optional(1, "snapshot_id", Type::Primitive(PrimitiveType::Long)),
            NestedField::optional(3, "sequence_number", Type::Primitive(PrimitiveType::Long)),
            NestedField::optional(
                4,
                "file_sequence_number",
                Type::Primitive(PrimitiveType::Long),
            ),
            NestedField::required(2, "data_file", Type::Struct(data_file_fields)),
        ];

        crate::spec::Schema::builder()
            .with_fields(fields.into_iter().map(|f| f.into()))
            .build()
            .unwrap()
    }

    /// Returns the struct type for the data_file field.
    fn data_file_struct_fields(&self, partition_type: &StructType) -> StructType {
        StructType::new(vec![
            Arc::new(NestedField::required(
                134,
                "content",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::required(
                100,
                "file_path",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::required(
                101,
                "file_format",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::required(
                102,
                "partition",
                Type::Struct(partition_type.clone()),
            )),
            Arc::new(NestedField::required(
                103,
                "record_count",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::required(
                104,
                "file_size_in_bytes",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                108,
                "column_sizes",
                Type::Map(MapType::new(
                    Arc::new(NestedField::required(
                        117,
                        "key",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::required(
                        118,
                        "value",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                )),
            )),
            Arc::new(NestedField::optional(
                109,
                "value_counts",
                Type::Map(MapType::new(
                    Arc::new(NestedField::required(
                        119,
                        "key",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::required(
                        120,
                        "value",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                )),
            )),
            Arc::new(NestedField::optional(
                110,
                "null_value_counts",
                Type::Map(MapType::new(
                    Arc::new(NestedField::required(
                        121,
                        "key",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::required(
                        122,
                        "value",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                )),
            )),
            Arc::new(NestedField::optional(
                137,
                "nan_value_counts",
                Type::Map(MapType::new(
                    Arc::new(NestedField::required(
                        138,
                        "key",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::required(
                        139,
                        "value",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                )),
            )),
            Arc::new(NestedField::optional(
                125,
                "lower_bounds",
                Type::Map(MapType::new(
                    Arc::new(NestedField::required(
                        126,
                        "key",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::required(
                        127,
                        "value",
                        Type::Primitive(PrimitiveType::Binary),
                    )),
                )),
            )),
            Arc::new(NestedField::optional(
                128,
                "upper_bounds",
                Type::Map(MapType::new(
                    Arc::new(NestedField::required(
                        129,
                        "key",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::required(
                        130,
                        "value",
                        Type::Primitive(PrimitiveType::Binary),
                    )),
                )),
            )),
            Arc::new(NestedField::optional(
                131,
                "key_metadata",
                Type::Primitive(PrimitiveType::Binary),
            )),
            Arc::new(NestedField::optional(
                132,
                "split_offsets",
                Type::List(ListType::new(Arc::new(NestedField::required(
                    133,
                    "element",
                    Type::Primitive(PrimitiveType::Long),
                )))),
            )),
            Arc::new(NestedField::optional(
                135,
                "equality_ids",
                Type::List(ListType::new(Arc::new(NestedField::required(
                    136,
                    "element",
                    Type::Primitive(PrimitiveType::Int),
                )))),
            )),
            Arc::new(NestedField::optional(
                140,
                "sort_order_id",
                Type::Primitive(PrimitiveType::Int),
            )),
        ])
    }

    /// Scans the entries table and returns Arrow record batches.
    pub async fn scan(&self) -> Result<ArrowRecordBatchStream> {
        let schema = schema_to_arrow_schema(&self.schema())?;
        let metadata = self.table.metadata();
        let file_io = self.table.file_io();

        let unified_partition_type = self.build_unified_partition_type(metadata);

        // Build entry-level columns
        let mut status = PrimitiveBuilder::<Int32Type>::new();
        let mut snapshot_id = PrimitiveBuilder::<Int64Type>::new();
        let mut sequence_number = PrimitiveBuilder::<Int64Type>::new();
        let mut file_sequence_number = PrimitiveBuilder::<Int64Type>::new();

        // Build data_file struct columns
        let mut content = PrimitiveBuilder::<Int32Type>::new();
        let mut file_path = StringBuilder::new();
        let mut file_format = StringBuilder::new();
        let mut partition_data: Vec<(i32, crate::spec::Struct)> = Vec::new();
        let mut record_count = PrimitiveBuilder::<Int64Type>::new();
        let mut file_size_in_bytes = PrimitiveBuilder::<Int64Type>::new();
        let mut column_sizes = self.int_long_map_builder(117, 118);
        let mut value_counts = self.int_long_map_builder(119, 120);
        let mut null_value_counts = self.int_long_map_builder(121, 122);
        let mut nan_value_counts = self.int_long_map_builder(138, 139);
        let mut lower_bounds = self.int_binary_map_builder(126, 127);
        let mut upper_bounds = self.int_binary_map_builder(129, 130);
        let mut key_metadata = GenericBinaryBuilder::<i64>::new();
        let mut split_offsets = self.long_list_builder(133);
        let mut equality_ids = self.int_list_builder(136);
        let mut sort_order_id = PrimitiveBuilder::<Int32Type>::new();

        // Iterate through all manifest entries (including deleted ones)
        if let Some(snapshot) = metadata.current_snapshot() {
            let manifest_list = snapshot
                .load_manifest_list(file_io, &self.table.metadata_ref())
                .await?;

            for manifest_file in manifest_list.entries() {
                let manifest = manifest_file.load_manifest(file_io).await?;

                for entry in manifest.entries() {
                    // Entry-level fields
                    status.append_value(entry.status as i32);
                    snapshot_id.append_option(entry.snapshot_id);
                    sequence_number.append_option(entry.sequence_number);
                    file_sequence_number.append_option(entry.file_sequence_number);

                    // Data file fields
                    let data_file = &entry.data_file;
                    content.append_value(data_file.content_type() as i32);
                    file_path.append_value(data_file.file_path());
                    file_format.append_value(data_file.file_format().to_string().to_uppercase());
                    partition_data
                        .push((data_file.partition_spec_id, data_file.partition().clone()));
                    record_count.append_value(data_file.record_count() as i64);
                    file_size_in_bytes.append_value(data_file.file_size_in_bytes() as i64);

                    // Map columns
                    self.append_int_long_map(&mut column_sizes, data_file.column_sizes());
                    self.append_int_long_map(&mut value_counts, data_file.value_counts());
                    self.append_int_long_map(&mut null_value_counts, data_file.null_value_counts());
                    self.append_int_long_map(&mut nan_value_counts, data_file.nan_value_counts());
                    self.append_datum_map(&mut lower_bounds, data_file.lower_bounds())?;
                    self.append_datum_map(&mut upper_bounds, data_file.upper_bounds())?;

                    // Optional fields
                    if let Some(km) = data_file.key_metadata() {
                        key_metadata.append_value(km);
                    } else {
                        key_metadata.append_null();
                    }

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

                    if let Some(eq_ids) = data_file.equality_ids() {
                        let values = equality_ids.values();
                        for id in eq_ids {
                            values.append_value(id);
                        }
                        equality_ids.append(true);
                    } else {
                        equality_ids.append_null();
                    }

                    sort_order_id.append_option(data_file.sort_order_id());
                }
            }
        }

        // Build partition struct array
        let partition_array =
            self.build_partition_array(&unified_partition_type, &partition_data, metadata)?;

        // Build the data_file struct array
        let data_file_struct = self.build_data_file_struct(
            &unified_partition_type,
            content.finish(),
            file_path.finish(),
            file_format.finish(),
            partition_array,
            record_count.finish(),
            file_size_in_bytes.finish(),
            column_sizes.finish(),
            value_counts.finish(),
            null_value_counts.finish(),
            nan_value_counts.finish(),
            lower_bounds.finish(),
            upper_bounds.finish(),
            key_metadata.finish(),
            split_offsets.finish(),
            equality_ids.finish(),
            sort_order_id.finish(),
        )?;

        let batch = RecordBatch::try_new(Arc::new(schema), vec![
            Arc::new(status.finish()),
            Arc::new(snapshot_id.finish()),
            Arc::new(sequence_number.finish()),
            Arc::new(file_sequence_number.finish()),
            Arc::new(data_file_struct),
        ])?;

        Ok(stream::iter(vec![Ok(batch)]).boxed())
    }

    /// Builds the data_file struct array from individual column arrays.
    #[allow(clippy::too_many_arguments)]
    fn build_data_file_struct(
        &self,
        partition_type: &StructType,
        content: arrow_array::Int32Array,
        file_path: arrow_array::StringArray,
        file_format: arrow_array::StringArray,
        partition: ArrayRef,
        record_count: arrow_array::Int64Array,
        file_size_in_bytes: arrow_array::Int64Array,
        column_sizes: arrow_array::MapArray,
        value_counts: arrow_array::MapArray,
        null_value_counts: arrow_array::MapArray,
        nan_value_counts: arrow_array::MapArray,
        lower_bounds: arrow_array::MapArray,
        upper_bounds: arrow_array::MapArray,
        key_metadata: arrow_array::LargeBinaryArray,
        split_offsets: arrow_array::ListArray,
        equality_ids: arrow_array::ListArray,
        sort_order_id: arrow_array::Int32Array,
    ) -> Result<StructArray> {
        let data_file_type = self.data_file_struct_fields(partition_type);
        let arrow_schema = schema_to_arrow_schema(
            &crate::spec::Schema::builder()
                .with_fields(
                    data_file_type
                        .fields()
                        .iter()
                        .map(|f| f.clone())
                        .collect::<Vec<_>>(),
                )
                .build()
                .unwrap(),
        )?;

        let struct_array = StructArray::try_new(
            arrow_schema.fields().clone(),
            vec![
                Arc::new(content) as ArrayRef,
                Arc::new(file_path),
                Arc::new(file_format),
                partition,
                Arc::new(record_count),
                Arc::new(file_size_in_bytes),
                Arc::new(column_sizes),
                Arc::new(value_counts),
                Arc::new(null_value_counts),
                Arc::new(nan_value_counts),
                Arc::new(lower_bounds),
                Arc::new(upper_bounds),
                Arc::new(key_metadata),
                Arc::new(split_offsets),
                Arc::new(equality_ids),
                Arc::new(sort_order_id),
            ],
            None,
        )?;

        Ok(struct_array)
    }

    /// Finds a schema that can successfully produce the partition type for the given spec.
    fn partition_type_for_spec(
        spec: &crate::spec::PartitionSpec,
        metadata: &crate::spec::TableMetadata,
    ) -> Option<StructType> {
        if let Ok(partition_type) = spec.partition_type(metadata.current_schema()) {
            return Some(partition_type);
        }

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

    /// Builds an Arrow StructArray from partition values.
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

    /// Builds an Arrow array for a single partition field.
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
                "Non-primitive partition field types are not yet supported in entries table",
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
            builder.append(false).unwrap();
        } else {
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
            builder.append(false)?;
        } else {
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
    async fn test_entries_table() {
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

        // Add an Added entry
        writer
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .snapshot_id(current_snapshot.snapshot_id())
                    .sequence_number(1)
                    .file_sequence_number(1)
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::Data)
                            .file_path(format!("{}/added.parquet", &fixture.table_location))
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

        // Add a Deleted entry
        writer
            .add_delete_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Deleted)
                    .snapshot_id(current_snapshot.snapshot_id())
                    .sequence_number(1)
                    .file_sequence_number(0)
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::Data)
                            .file_path(format!("{}/deleted.parquet", &fixture.table_location))
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(200)
                            .record_count(20)
                            .partition(Struct::from_iter([Some(Literal::long(100))]))
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();

        // Add an Existing entry
        writer
            .add_existing_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Existing)
                    .snapshot_id(current_snapshot.snapshot_id() - 1)
                    .sequence_number(0)
                    .file_sequence_number(0)
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::Data)
                            .file_path(format!("{}/existing.parquet", &fixture.table_location))
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(500)
                            .record_count(50)
                            .partition(Struct::from_iter([Some(Literal::long(100))]))
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

        // Scan entries table
        let batch_stream = fixture.table.inspect().entries().scan().await.unwrap();
        let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        // All 3 entries should appear (including deleted)
        assert_eq!(batch.num_rows(), 3);

        // Check status column
        // Order is: Added, Deleted, Existing based on the order entries were written
        let status_col = batch
            .column(0)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(status_col.value(0), 1); // Added
        assert_eq!(status_col.value(1), 2); // Deleted
        assert_eq!(status_col.value(2), 0); // Existing

        // Check snapshot_id column
        let snapshot_id_col = batch
            .column(1)
            .as_primitive::<arrow_array::types::Int64Type>();
        assert_eq!(snapshot_id_col.value(0), current_snapshot.snapshot_id());
        assert_eq!(snapshot_id_col.value(1), current_snapshot.snapshot_id());
        assert_eq!(
            snapshot_id_col.value(2),
            current_snapshot.snapshot_id() - 1
        );

        // Check data_file struct column
        let data_file_col = batch.column(4).as_struct();

        // Check file_path within data_file struct
        let file_path_col = data_file_col.column(1).as_string::<i32>();
        assert!(file_path_col.value(0).ends_with("/added.parquet"));
        assert!(file_path_col.value(1).ends_with("/deleted.parquet"));
        assert!(file_path_col.value(2).ends_with("/existing.parquet"));
    }

    #[tokio::test]
    async fn test_entries_table_schema() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        let batch_stream = fixture.table.inspect().entries().scan().await.unwrap();

        check_record_batches(
            batch_stream.try_collect::<Vec<_>>().await.unwrap(),
            expect![[r#"
                Field { "status": Int32, metadata: {"PARQUET:field_id": "0"} },
                Field { "snapshot_id": nullable Int64, metadata: {"PARQUET:field_id": "1"} },
                Field { "sequence_number": nullable Int64, metadata: {"PARQUET:field_id": "3"} },
                Field { "file_sequence_number": nullable Int64, metadata: {"PARQUET:field_id": "4"} },
                Field { "data_file": Struct("content": non-null Int32, metadata: {"PARQUET:field_id": "134"}, "file_path": non-null Utf8, metadata: {"PARQUET:field_id": "100"}, "file_format": non-null Utf8, metadata: {"PARQUET:field_id": "101"}, "partition": non-null Struct("x": Int64, metadata: {"PARQUET:field_id": "1000"}), metadata: {"PARQUET:field_id": "102"}, "record_count": non-null Int64, metadata: {"PARQUET:field_id": "103"}, "file_size_in_bytes": non-null Int64, metadata: {"PARQUET:field_id": "104"}, "column_sizes": Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "117"}, "value": non-null Int64, metadata: {"PARQUET:field_id": "118"}), unsorted), metadata: {"PARQUET:field_id": "108"}, "value_counts": Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "119"}, "value": non-null Int64, metadata: {"PARQUET:field_id": "120"}), unsorted), metadata: {"PARQUET:field_id": "109"}, "null_value_counts": Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "121"}, "value": non-null Int64, metadata: {"PARQUET:field_id": "122"}), unsorted), metadata: {"PARQUET:field_id": "110"}, "nan_value_counts": Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "138"}, "value": non-null Int64, metadata: {"PARQUET:field_id": "139"}), unsorted), metadata: {"PARQUET:field_id": "137"}, "lower_bounds": Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "126"}, "value": non-null LargeBinary, metadata: {"PARQUET:field_id": "127"}), unsorted), metadata: {"PARQUET:field_id": "125"}, "upper_bounds": Map("key_value": non-null Struct("key": non-null Int32, metadata: {"PARQUET:field_id": "129"}, "value": non-null LargeBinary, metadata: {"PARQUET:field_id": "130"}), unsorted), metadata: {"PARQUET:field_id": "128"}, "key_metadata": LargeBinary, metadata: {"PARQUET:field_id": "131"}, "split_offsets": List(non-null Int64, field: 'element', metadata: {"PARQUET:field_id": "133"}), metadata: {"PARQUET:field_id": "132"}, "equality_ids": List(non-null Int32, field: 'element', metadata: {"PARQUET:field_id": "136"}), metadata: {"PARQUET:field_id": "135"}, "sort_order_id": Int32, metadata: {"PARQUET:field_id": "140"}), metadata: {"PARQUET:field_id": "2"} }"#]],
            expect![[r#"
                status: PrimitiveArray<Int32>
                [
                  1,
                  2,
                  0,
                ],
                snapshot_id: PrimitiveArray<Int64>
                [
                  3055729675574597004,
                  3055729675574597004,
                  3051729675574597004,
                ],
                sequence_number: (skipped),
                file_sequence_number: (skipped),
                data_file: (skipped)"#]],
            &["sequence_number", "file_sequence_number", "data_file"],
            None,
        );
    }
}
