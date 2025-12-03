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

//! Integration tests for DeleteAction.

use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use iceberg::spec::{
    DataFileFormat, ManifestContentType, NestedField, PrimitiveType, Schema, Type,
};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::base_writer::position_delete_writer::{
    POSITION_DELETE_FILE_PATH_FIELD_ID, POSITION_DELETE_POS_FIELD_ID,
    PositionDeleteFileWriterBuilder, PositionDeleteWriterConfig,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, TableCreation};
use iceberg_catalog_rest::RestCatalogBuilder;
use parquet::file::properties::WriterProperties;

use crate::get_shared_containers;
use crate::shared_tests::{random_ns, test_schema};

#[tokio::test]
async fn test_delete_action_integration() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns().await;
    let schema = test_schema();

    let table_creation = TableCreation::builder()
        .name("t_delete_test".to_string())
        .schema(schema.clone())
        .build();

    let table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    let arrow_schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_generator =
        DefaultFileNameGenerator::new("data".to_string(), None, DataFileFormat::Parquet);
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
    );
    let rolling_file_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_file_writer_builder);
    let mut data_file_writer = data_file_writer_builder.build(None).await.unwrap();

    let col1 = StringArray::from(vec![Some("foo"), Some("bar"), Some("baz"), Some("qux")]);
    let col2 = Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4)]);
    let col3 = BooleanArray::from(vec![Some(true), Some(false), Some(true), Some(false)]);
    let batch = RecordBatch::try_new(arrow_schema.clone(), vec![
        Arc::new(col1) as ArrayRef,
        Arc::new(col2) as ArrayRef,
        Arc::new(col3) as ArrayRef,
    ])
    .unwrap();
    data_file_writer.write(batch.clone()).await.unwrap();
    let data_files = data_file_writer.close().await.unwrap();
    let data_file_path = data_files[0].file_path().to_string();

    let tx = Transaction::new(&table);
    let append_action = tx.fast_append().add_data_files(data_files.clone());
    let tx = append_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    let batch_stream = table
        .scan()
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap();
    let batches: Vec<_> = batch_stream.try_collect().await.unwrap();
    let total_rows_before: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows_before, 4);

    // create position delete files
    let delete_config = PositionDeleteWriterConfig::new();
    let delete_schema = delete_config.delete_schema().clone();

    let iceberg_delete_schema = Arc::new(
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
            .build()
            .unwrap(),
    );

    let delete_file_name_generator =
        DefaultFileNameGenerator::new("delete".to_string(), None, DataFileFormat::Parquet);
    let delete_parquet_writer_builder =
        ParquetWriterBuilder::new(WriterProperties::default(), iceberg_delete_schema);
    let delete_rolling_builder = RollingFileWriterBuilder::new_with_default_file_size(
        delete_parquet_writer_builder,
        table.file_io().clone(),
        location_generator,
        delete_file_name_generator,
    );
    let mut position_delete_writer =
        PositionDeleteFileWriterBuilder::new(delete_rolling_builder, delete_config)
            .build(None)
            .await
            .unwrap();

    // delete rows at positions 1 and 3
    let file_paths = Arc::new(StringArray::from(vec![
        data_file_path.as_str(),
        data_file_path.as_str(),
    ]));
    let positions = Arc::new(Int64Array::from(vec![1i64, 3i64]));
    let delete_batch = RecordBatch::try_new(delete_schema, vec![file_paths, positions]).unwrap();

    position_delete_writer.write(delete_batch).await.unwrap();
    let delete_files = position_delete_writer.close().await.unwrap();

    assert_eq!(delete_files.len(), 1);
    assert_eq!(
        delete_files[0].content_type(),
        iceberg::spec::DataContentType::PositionDeletes
    );

    // commit delete files
    let tx = Transaction::new(&table);
    let delete_action = tx.delete().add_delete_files(delete_files.clone());
    let tx = delete_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // check manifest list
    let snapshot = table.metadata().current_snapshot().unwrap();
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();

    assert_eq!(manifest_list.entries().len(), 2);

    let mut has_data_manifest = false;
    let mut has_delete_manifest = false;
    for entry in manifest_list.entries() {
        match entry.content {
            ManifestContentType::Data => has_data_manifest = true,
            ManifestContentType::Deletes => has_delete_manifest = true,
        }
    }
    assert!(has_data_manifest);
    assert!(has_delete_manifest);

    assert_eq!(
        snapshot.summary().operation,
        iceberg::spec::Operation::Delete
    );

    // check scan includes delete file
    let scan = table.scan().build().unwrap();
    let plan: Vec<_> = scan
        .plan_files()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(!plan.is_empty());
    assert_eq!(plan[0].deletes.len(), 1);

    // check row filtering
    let batch_stream = table
        .scan()
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap();
    let batches: Vec<_> = batch_stream.try_collect().await.unwrap();
    let total_rows_after: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows_after, 2);
}
