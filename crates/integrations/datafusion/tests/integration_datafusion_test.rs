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

//! Integration tests for Iceberg Datafusion with Hive Metastore.

use std::collections::HashMap;
use std::sync::Arc;
use std::vec;

use datafusion::arrow::array::{Array, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::common::config::Dialect;
use datafusion::execution::context::SessionContext;
use datafusion::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use datafusion::prelude::{SessionConfig, col, lit};
use expect_test::expect;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    NestedField, PrimitiveType, Schema, StructType, Transform, Type, UnboundPartitionSpec,
};
use iceberg::test_utils::check_record_batches;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{
    Catalog, CatalogBuilder, MemoryCatalog, NamespaceIdent, Result, TableCreation, TableIdent,
};
use iceberg_datafusion::{IcebergCatalogProvider, IcebergTableProvider};
use tempfile::TempDir;

fn temp_path() -> String {
    let temp_dir = TempDir::new().unwrap();
    temp_dir.path().to_str().unwrap().to_string()
}

async fn get_iceberg_catalog() -> MemoryCatalog {
    MemoryCatalogBuilder::default()
        .load(
            "memory",
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), temp_path())]),
        )
        .await
        .unwrap()
}

fn get_struct_type() -> StructType {
    StructType::new(vec![
        NestedField::required(4, "s_foo1", Type::Primitive(PrimitiveType::Int)).into(),
        NestedField::required(5, "s_foo2", Type::Primitive(PrimitiveType::String)).into(),
    ])
}

async fn set_test_namespace(catalog: &MemoryCatalog, namespace: &NamespaceIdent) -> Result<()> {
    let properties = HashMap::new();

    catalog.create_namespace(namespace, properties).await?;

    Ok(())
}

fn get_table_creation(
    location: impl ToString,
    name: impl ToString,
    schema: Option<Schema>,
) -> Result<TableCreation> {
    let schema = match schema {
        None => Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "foo1", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "foo2", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()?,
        Some(schema) => schema,
    };

    let creation = TableCreation::builder()
        .location(location.to_string())
        .name(name.to_string())
        .properties(HashMap::new())
        .schema(schema)
        .build();

    Ok(creation)
}

#[tokio::test]
async fn test_provider_plan_stream_schema() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_provider_get_table_schema".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let creation = get_table_creation(temp_path(), "my_table", None)?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    let provider = ctx.catalog("catalog").unwrap();
    let schema = provider.schema("test_provider_get_table_schema").unwrap();

    let table = schema.table("my_table").await.unwrap().unwrap();
    let table_schema = table.schema();

    let expected = [("foo1", &DataType::Int32), ("foo2", &DataType::Utf8)];

    for (field, exp) in table_schema.fields().iter().zip(expected.iter()) {
        assert_eq!(field.name(), exp.0);
        assert_eq!(field.data_type(), exp.1);
        assert!(!field.is_nullable())
    }

    let df = ctx
        .sql("select foo2 from catalog.test_provider_get_table_schema.my_table")
        .await
        .unwrap();

    let task_ctx = Arc::new(df.task_ctx());
    let plan = df.create_physical_plan().await.unwrap();
    let stream = plan.execute(1, task_ctx).unwrap();

    // Ensure both the plan and the stream conform to the same schema
    assert_eq!(plan.schema(), stream.schema());
    assert_eq!(
        stream.schema().as_ref(),
        &ArrowSchema::new(vec![
            Field::new("foo2", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )]))
        ]),
    );

    Ok(())
}

#[tokio::test]
async fn test_provider_list_table_names() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_provider_list_table_names".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let creation = get_table_creation(temp_path(), "my_table", None)?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    let provider = ctx.catalog("catalog").unwrap();
    let schema = provider.schema("test_provider_list_table_names").unwrap();

    let result = schema.table_names();

    expect![[r#"
        [
            "my_table",
            "my_table$snapshots",
            "my_table$manifests",
            "my_table$history",
            "my_table$refs",
            "my_table$files",
            "my_table$properties",
        ]
    "#]]
    .assert_debug_eq(&result);

    Ok(())
}

#[tokio::test]
async fn test_provider_list_schema_names() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_provider_list_schema_names".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    let provider = ctx.catalog("catalog").unwrap();

    let expected = ["test_provider_list_schema_names"];
    let result = provider.schema_names();

    assert!(
        expected
            .iter()
            .all(|item| result.contains(&item.to_string()))
    );
    Ok(())
}

#[tokio::test]
async fn test_table_projection() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("ns".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "foo1", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "foo2", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "foo3", Type::Struct(get_struct_type())).into(),
        ])
        .build()?;
    let creation = get_table_creation(temp_path(), "t1", Some(schema))?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);
    let table_df = ctx.table("catalog.ns.t1").await.unwrap();

    let records = table_df
        .clone()
        .explain(false, false)
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(1, records.len());
    let record = &records[0];
    // the first column is plan_type, the second column plan string.
    let s = record
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(2, s.len());
    // the first row is logical_plan, the second row is physical_plan
    assert!(s.value(1).contains("projection:[foo1,foo2,foo3]"));

    // datafusion doesn't support query foo3.s_foo1, use foo3 instead
    let records = table_df
        .select_columns(&["foo1", "foo3"])
        .unwrap()
        .explain(false, false)
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(1, records.len());
    let record = &records[0];
    let s = record
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(2, s.len());
    assert!(
        s.value(1)
            .contains("IcebergTableScan projection:[foo1,foo3]")
    );

    Ok(())
}

#[tokio::test]
async fn test_table_predict_pushdown() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("ns".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "foo", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "bar", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;
    let creation = get_table_creation(temp_path(), "t1", Some(schema))?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);
    let records = ctx
        .sql("select * from catalog.ns.t1 where (foo > 1 and length(bar) = 1 ) or bar is null")
        .await
        .unwrap()
        .explain(false, false)
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(1, records.len());
    let record = &records[0];
    // the first column is plan_type, the second column plan string.
    let s = record
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(2, s.len());
    // the first row is logical_plan, the second row is physical_plan
    let expected = "predicate:[(foo > 1) OR (bar IS NULL)]";
    assert!(s.value(1).trim().contains(expected));
    Ok(())
}

#[tokio::test]
async fn test_metadata_table() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("ns".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "foo", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "bar", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;
    let creation = get_table_creation(temp_path(), "t1", Some(schema))?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);
    let snapshots = ctx
        .sql("select * from catalog.ns.t1$snapshots")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    check_record_batches(
        snapshots,
        expect![[r#"
            Field { "committed_at": Timestamp(µs, "+00:00"), metadata: {"PARQUET:field_id": "1"} },
            Field { "snapshot_id": Int64, metadata: {"PARQUET:field_id": "2"} },
            Field { "parent_id": nullable Int64, metadata: {"PARQUET:field_id": "3"} },
            Field { "operation": nullable Utf8, metadata: {"PARQUET:field_id": "4"} },
            Field { "manifest_list": nullable Utf8, metadata: {"PARQUET:field_id": "5"} },
            Field { "summary": nullable Map("key_value": non-null Struct("key": non-null Utf8, metadata: {"PARQUET:field_id": "7"}, "value": Utf8, metadata: {"PARQUET:field_id": "8"}), unsorted), metadata: {"PARQUET:field_id": "6"} }"#]],
        expect![[r#"
            committed_at: PrimitiveArray<Timestamp(µs, "+00:00")>
            [
            ],
            snapshot_id: PrimitiveArray<Int64>
            [
            ],
            parent_id: PrimitiveArray<Int64>
            [
            ],
            operation: StringArray
            [
            ],
            manifest_list: StringArray
            [
            ],
            summary: MapArray
            [
            ]"#]],
        &[],
        None,
    );

    let manifests = ctx
        .sql("select * from catalog.ns.t1$manifests")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    check_record_batches(
        manifests,
        expect![[r#"
            Field { "content": Int32, metadata: {"PARQUET:field_id": "14"} },
            Field { "path": Utf8, metadata: {"PARQUET:field_id": "1"} },
            Field { "length": Int64, metadata: {"PARQUET:field_id": "2"} },
            Field { "partition_spec_id": Int32, metadata: {"PARQUET:field_id": "3"} },
            Field { "added_snapshot_id": Int64, metadata: {"PARQUET:field_id": "4"} },
            Field { "added_data_files_count": Int32, metadata: {"PARQUET:field_id": "5"} },
            Field { "existing_data_files_count": Int32, metadata: {"PARQUET:field_id": "6"} },
            Field { "deleted_data_files_count": Int32, metadata: {"PARQUET:field_id": "7"} },
            Field { "added_delete_files_count": Int32, metadata: {"PARQUET:field_id": "15"} },
            Field { "existing_delete_files_count": Int32, metadata: {"PARQUET:field_id": "16"} },
            Field { "deleted_delete_files_count": Int32, metadata: {"PARQUET:field_id": "17"} },
            Field { "partition_summaries": List(non-null Struct("contains_null": non-null Boolean, metadata: {"PARQUET:field_id": "10"}, "contains_nan": Boolean, metadata: {"PARQUET:field_id": "11"}, "lower_bound": Utf8, metadata: {"PARQUET:field_id": "12"}, "upper_bound": Utf8, metadata: {"PARQUET:field_id": "13"}), metadata: {"PARQUET:field_id": "9"}), metadata: {"PARQUET:field_id": "8"} }"#]],
        expect![[r#"
            content: PrimitiveArray<Int32>
            [
            ],
            path: StringArray
            [
            ],
            length: PrimitiveArray<Int64>
            [
            ],
            partition_spec_id: PrimitiveArray<Int32>
            [
            ],
            added_snapshot_id: PrimitiveArray<Int64>
            [
            ],
            added_data_files_count: PrimitiveArray<Int32>
            [
            ],
            existing_data_files_count: PrimitiveArray<Int32>
            [
            ],
            deleted_data_files_count: PrimitiveArray<Int32>
            [
            ],
            added_delete_files_count: PrimitiveArray<Int32>
            [
            ],
            existing_delete_files_count: PrimitiveArray<Int32>
            [
            ],
            deleted_delete_files_count: PrimitiveArray<Int32>
            [
            ],
            partition_summaries: ListArray
            [
            ]"#]],
        &[],
        None,
    );

    // Test $history metadata table
    let history = ctx
        .sql("select * from catalog.ns.t1$history")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    check_record_batches(
        history,
        expect![[r#"
            Field { "made_current_at": Timestamp(µs, "+00:00"), metadata: {"PARQUET:field_id": "1"} },
            Field { "snapshot_id": Int64, metadata: {"PARQUET:field_id": "2"} },
            Field { "parent_id": nullable Int64, metadata: {"PARQUET:field_id": "3"} },
            Field { "is_current_ancestor": Boolean, metadata: {"PARQUET:field_id": "4"} }"#]],
        expect![[r#"
            made_current_at: PrimitiveArray<Timestamp(µs, "+00:00")>
            [
            ],
            snapshot_id: PrimitiveArray<Int64>
            [
            ],
            parent_id: PrimitiveArray<Int64>
            [
            ],
            is_current_ancestor: BooleanArray
            [
            ]"#]],
        &[],
        None,
    );

    // Test $refs metadata table
    let refs = ctx
        .sql("select * from catalog.ns.t1$refs")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    check_record_batches(
        refs,
        expect![[r#"
            Field { "name": Utf8, metadata: {"PARQUET:field_id": "1"} },
            Field { "type": Utf8, metadata: {"PARQUET:field_id": "2"} },
            Field { "snapshot_id": Int64, metadata: {"PARQUET:field_id": "3"} },
            Field { "max_ref_age_ms": nullable Int64, metadata: {"PARQUET:field_id": "4"} },
            Field { "min_snapshots_to_keep": nullable Int32, metadata: {"PARQUET:field_id": "5"} },
            Field { "max_snapshot_age_ms": nullable Int64, metadata: {"PARQUET:field_id": "6"} }"#]],
        expect![[r#"
            name: StringArray
            [
            ],
            type: StringArray
            [
            ],
            snapshot_id: PrimitiveArray<Int64>
            [
            ],
            max_ref_age_ms: PrimitiveArray<Int64>
            [
            ],
            min_snapshots_to_keep: PrimitiveArray<Int32>
            [
            ],
            max_snapshot_age_ms: PrimitiveArray<Int64>
            [
            ]"#]],
        &[],
        None,
    );

    // Test $files metadata table (empty since no data inserted yet)
    let files = ctx
        .sql("select * from catalog.ns.t1$files")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    check_record_batches(
        files,
        expect![[r#"
            Field { "content": Int32, metadata: {"PARQUET:field_id": "1"} },
            Field { "file_path": Utf8, metadata: {"PARQUET:field_id": "2"} },
            Field { "file_format": Utf8, metadata: {"PARQUET:field_id": "3"} },
            Field { "spec_id": Int32, metadata: {"PARQUET:field_id": "4"} },
            Field { "partition": Struct(), metadata: {"PARQUET:field_id": "5"} },
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
            ],
            file_path: StringArray
            [
            ],
            file_format: StringArray
            [
            ],
            spec_id: PrimitiveArray<Int32>
            [
            ],
            partition: StructArray
            -- validity:
            [
            ]
            [
            ],
            record_count: PrimitiveArray<Int64>
            [
            ],
            file_size_in_bytes: PrimitiveArray<Int64>
            [
            ],
            column_sizes: MapArray
            [
            ],
            value_counts: MapArray
            [
            ],
            null_value_counts: MapArray
            [
            ],
            nan_value_counts: MapArray
            [
            ],
            lower_bounds: MapArray
            [
            ],
            upper_bounds: MapArray
            [
            ],
            key_metadata: LargeBinaryArray
            [
            ],
            split_offsets: ListArray
            [
            ],
            equality_ids: ListArray
            [
            ],
            sort_order_id: PrimitiveArray<Int32>
            [
            ]"#]],
        &[],
        None,
    );

    Ok(())
}

/// Test $files metadata table with actual data
#[tokio::test]
async fn test_files_metadata_with_data() -> Result<()> {
    use datafusion::arrow::array::{Int32Array, Int64Array};

    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_files_meta".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let creation = get_table_creation(temp_path(), "my_table", None)?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert data into the table
    ctx.sql("INSERT INTO catalog.test_files_meta.my_table VALUES (1, 'alice'), (2, 'bob')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Refresh the catalog to see the new data
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client).await?);
    ctx.register_catalog("catalog", catalog);

    // Query $files metadata table to validate real rows (without projection to avoid schema issues)
    let files = ctx
        .sql("SELECT * FROM catalog.test_files_meta.my_table$files")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Should have exactly 1 file
    assert_eq!(files.len(), 1);
    let batch = &files[0];
    assert_eq!(batch.num_rows(), 1);

    // Verify content type is 0 (Data file) - column 0
    let content = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(content.value(0), 0); // DataContentType::Data

    // Verify file format is PARQUET - column 2
    let file_format = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(file_format.value(0), "PARQUET");

    // Verify record_count is 2 (we inserted 2 rows) - column 5
    // (Column 4 is partition struct, 5 is record_count, 6 is file_size_in_bytes)
    let record_count = batch
        .column(5)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(record_count.value(0), 2);

    // Verify file_size_in_bytes is > 0 - column 6
    let file_size = batch
        .column(6)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert!(file_size.value(0) > 0);

    Ok(())
}

#[tokio::test]
async fn test_insert_into() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_insert_into".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let creation = get_table_creation(temp_path(), "my_table", None)?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Verify table schema
    let provider = ctx.catalog("catalog").unwrap();
    let schema = provider.schema("test_insert_into").unwrap();
    let table = schema.table("my_table").await.unwrap().unwrap();
    let table_schema = table.schema();

    let expected = [("foo1", &DataType::Int32), ("foo2", &DataType::Utf8)];
    for (field, exp) in table_schema.fields().iter().zip(expected.iter()) {
        assert_eq!(field.name(), exp.0);
        assert_eq!(field.data_type(), exp.1);
        assert!(!field.is_nullable())
    }

    // Insert data into the table
    let df = ctx
        .sql("INSERT INTO catalog.test_insert_into.my_table VALUES (1, 'alan'), (2, 'turing')")
        .await
        .unwrap();

    // Verify the insert operation result
    let batches = df.collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert!(
        batch.num_rows() == 1 && batch.num_columns() == 1,
        "Results should only have one row and one column that has the number of rows inserted"
    );
    // Verify the number of rows inserted
    let rows_inserted = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(rows_inserted.value(0), 2);

    // Query the table to verify the inserted data
    let df = ctx
        .sql("SELECT * FROM catalog.test_insert_into.my_table")
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();

    // Use check_record_batches to verify the data
    check_record_batches(
        batches,
        expect![[r#"
            Field { "foo1": Int32, metadata: {"PARQUET:field_id": "1"} },
            Field { "foo2": Utf8, metadata: {"PARQUET:field_id": "2"} }"#]],
        expect![[r#"
            foo1: PrimitiveArray<Int32>
            [
              1,
              2,
            ],
            foo2: StringArray
            [
              "alan",
              "turing",
            ]"#]],
        &[],
        Some("foo1"),
    );

    Ok(())
}

fn get_nested_struct_type() -> StructType {
    // Create a nested struct type with:
    // - address: STRUCT<street: STRING, city: STRING, zip: INT>
    // - contact: STRUCT<email: STRING, phone: STRING>
    StructType::new(vec![
        NestedField::optional(
            10,
            "address",
            Type::Struct(StructType::new(vec![
                NestedField::required(11, "street", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(12, "city", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(13, "zip", Type::Primitive(PrimitiveType::Int)).into(),
            ])),
        )
        .into(),
        NestedField::optional(
            20,
            "contact",
            Type::Struct(StructType::new(vec![
                NestedField::optional(21, "email", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(22, "phone", Type::Primitive(PrimitiveType::String)).into(),
            ])),
        )
        .into(),
    ])
}

#[tokio::test]
async fn test_insert_into_nested() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_insert_nested".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;
    let table_name = "nested_table";

    // Create a schema with nested fields
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "profile", Type::Struct(get_nested_struct_type())).into(),
        ])
        .build()?;

    // Create the table with the nested schema
    let creation = get_table_creation(temp_path(), table_name, Some(schema))?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Verify table schema
    let provider = ctx.catalog("catalog").unwrap();
    let schema = provider.schema("test_insert_nested").unwrap();
    let table = schema.table("nested_table").await.unwrap().unwrap();
    let table_schema = table.schema();

    // Verify the schema has the expected structure
    assert_eq!(table_schema.fields().len(), 3);
    assert_eq!(table_schema.field(0).name(), "id");
    assert_eq!(table_schema.field(1).name(), "name");
    assert_eq!(table_schema.field(2).name(), "profile");
    assert!(matches!(
        table_schema.field(2).data_type(),
        DataType::Struct(_)
    ));

    // In DataFusion, we need to use named_struct to create struct values
    // Insert data with nested structs
    let insert_sql = r#"
    INSERT INTO catalog.test_insert_nested.nested_table
    SELECT 
        1 as id, 
        'Alice' as name,
        named_struct(
            'address', named_struct(
                'street', '123 Main St',
                'city', 'San Francisco',
                'zip', 94105
            ),
            'contact', named_struct(
                'email', 'alice@example.com',
                'phone', '555-1234'
            )
        ) as profile
    UNION ALL
    SELECT 
        2 as id, 
        'Bob' as name,
        named_struct(
            'address', named_struct(
                'street', '456 Market St',
                'city', 'San Jose',
                'zip', 95113
            ),
            'contact', named_struct(
                'email', 'bob@example.com',
                'phone', NULL
            )
        ) as profile
    "#;

    // Execute the insert
    let df = ctx.sql(insert_sql).await.unwrap();
    let batches = df.collect().await.unwrap();

    // Verify the insert operation result
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert!(batch.num_rows() == 1 && batch.num_columns() == 1);

    // Verify the number of rows inserted
    let rows_inserted = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(rows_inserted.value(0), 2);

    // Query the table to verify the inserted data
    let df = ctx
        .sql("SELECT * FROM catalog.test_insert_nested.nested_table ORDER BY id")
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();

    // Use check_record_batches to verify the data
    check_record_batches(
        batches,
        expect![[r#"
            Field { "id": Int32, metadata: {"PARQUET:field_id": "1"} },
            Field { "name": Utf8, metadata: {"PARQUET:field_id": "2"} },
            Field { "profile": nullable Struct("address": Struct("street": non-null Utf8, metadata: {"PARQUET:field_id": "6"}, "city": non-null Utf8, metadata: {"PARQUET:field_id": "7"}, "zip": non-null Int32, metadata: {"PARQUET:field_id": "8"}), metadata: {"PARQUET:field_id": "4"}, "contact": Struct("email": Utf8, metadata: {"PARQUET:field_id": "9"}, "phone": Utf8, metadata: {"PARQUET:field_id": "10"}), metadata: {"PARQUET:field_id": "5"}), metadata: {"PARQUET:field_id": "3"} }"#]],
        expect![[r#"
            id: PrimitiveArray<Int32>
            [
              1,
              2,
            ],
            name: StringArray
            [
              "Alice",
              "Bob",
            ],
            profile: StructArray
            -- validity:
            [
              valid,
              valid,
            ]
            [
            -- child 0: "address" (Struct([Field { name: "street", data_type: Utf8, metadata: {"PARQUET:field_id": "6"} }, Field { name: "city", data_type: Utf8, metadata: {"PARQUET:field_id": "7"} }, Field { name: "zip", data_type: Int32, metadata: {"PARQUET:field_id": "8"} }]))
            StructArray
            -- validity:
            [
              valid,
              valid,
            ]
            [
            -- child 0: "street" (Utf8)
            StringArray
            [
              "123 Main St",
              "456 Market St",
            ]
            -- child 1: "city" (Utf8)
            StringArray
            [
              "San Francisco",
              "San Jose",
            ]
            -- child 2: "zip" (Int32)
            PrimitiveArray<Int32>
            [
              94105,
              95113,
            ]
            ]
            -- child 1: "contact" (Struct([Field { name: "email", data_type: Utf8, nullable: true, metadata: {"PARQUET:field_id": "9"} }, Field { name: "phone", data_type: Utf8, nullable: true, metadata: {"PARQUET:field_id": "10"} }]))
            StructArray
            -- validity:
            [
              valid,
              valid,
            ]
            [
            -- child 0: "email" (Utf8)
            StringArray
            [
              "alice@example.com",
              "bob@example.com",
            ]
            -- child 1: "phone" (Utf8)
            StringArray
            [
              "555-1234",
              null,
            ]
            ]
            ]"#]],
        &[],
        Some("id"),
    );

    // Query with explicit field access to verify nested data
    let df = ctx
        .sql(
            r#"
            SELECT 
                id, 
                name,
                profile.address.street,
                profile.address.city,
                profile.address.zip,
                profile.contact.email,
                profile.contact.phone
            FROM catalog.test_insert_nested.nested_table 
            ORDER BY id
        "#,
        )
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();

    // Use check_record_batches to verify the flattened data
    check_record_batches(
        batches,
        expect![[r#"
            Field { "id": Int32, metadata: {"PARQUET:field_id": "1"} },
            Field { "name": Utf8, metadata: {"PARQUET:field_id": "2"} },
            Field { "catalog.test_insert_nested.nested_table.profile[address][street]": nullable Utf8, metadata: {"PARQUET:field_id": "6"} },
            Field { "catalog.test_insert_nested.nested_table.profile[address][city]": nullable Utf8, metadata: {"PARQUET:field_id": "7"} },
            Field { "catalog.test_insert_nested.nested_table.profile[address][zip]": nullable Int32, metadata: {"PARQUET:field_id": "8"} },
            Field { "catalog.test_insert_nested.nested_table.profile[contact][email]": nullable Utf8, metadata: {"PARQUET:field_id": "9"} },
            Field { "catalog.test_insert_nested.nested_table.profile[contact][phone]": nullable Utf8, metadata: {"PARQUET:field_id": "10"} }"#]],
        expect![[r#"
            id: PrimitiveArray<Int32>
            [
              1,
              2,
            ],
            name: StringArray
            [
              "Alice",
              "Bob",
            ],
            catalog.test_insert_nested.nested_table.profile[address][street]: StringArray
            [
              "123 Main St",
              "456 Market St",
            ],
            catalog.test_insert_nested.nested_table.profile[address][city]: StringArray
            [
              "San Francisco",
              "San Jose",
            ],
            catalog.test_insert_nested.nested_table.profile[address][zip]: PrimitiveArray<Int32>
            [
              94105,
              95113,
            ],
            catalog.test_insert_nested.nested_table.profile[contact][email]: StringArray
            [
              "alice@example.com",
              "bob@example.com",
            ],
            catalog.test_insert_nested.nested_table.profile[contact][phone]: StringArray
            [
              "555-1234",
              null,
            ]"#]],
        &[],
        Some("id"),
    );

    Ok(())
}

#[tokio::test]
async fn test_insert_into_partitioned() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_partitioned_write".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    // Create a schema with a partition column
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "category", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "value", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;

    // Create partition spec with identity transform on category
    let partition_spec = UnboundPartitionSpec::builder()
        .with_spec_id(0)
        .add_partition_field(2, "category", Transform::Identity)?
        .build();

    // Create the partitioned table
    let creation = TableCreation::builder()
        .name("partitioned_table".to_string())
        .location(temp_path())
        .schema(schema)
        .partition_spec(partition_spec)
        .properties(HashMap::new())
        .build();

    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert data with multiple partition values in a single batch
    let df = ctx
        .sql(
            r#"
            INSERT INTO catalog.test_partitioned_write.partitioned_table 
            VALUES 
                (1, 'electronics', 'laptop'),
                (2, 'electronics', 'phone'),
                (3, 'books', 'novel'),
                (4, 'books', 'textbook'),
                (5, 'clothing', 'shirt')
            "#,
        )
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    let rows_inserted = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(rows_inserted.value(0), 5);

    // Query the table to verify data
    let df = ctx
        .sql("SELECT * FROM catalog.test_partitioned_write.partitioned_table ORDER BY id")
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();

    // Verify the data - note that _partition column should NOT be present
    check_record_batches(
        batches,
        expect![[r#"
            Field { "id": Int32, metadata: {"PARQUET:field_id": "1"} },
            Field { "category": Utf8, metadata: {"PARQUET:field_id": "2"} },
            Field { "value": Utf8, metadata: {"PARQUET:field_id": "3"} }"#]],
        expect![[r#"
            id: PrimitiveArray<Int32>
            [
              1,
              2,
              3,
              4,
              5,
            ],
            category: StringArray
            [
              "electronics",
              "electronics",
              "books",
              "books",
              "clothing",
            ],
            value: StringArray
            [
              "laptop",
              "phone",
              "novel",
              "textbook",
              "shirt",
            ]"#]],
        &[],
        Some("id"),
    );

    // Verify that data files exist under correct partition paths
    let table_ident = TableIdent::new(namespace.clone(), "partitioned_table".to_string());
    let table = client.load_table(&table_ident).await?;
    let table_location = table.metadata().location();
    let file_io = table.file_io();

    // List files under each expected partition path
    let electronics_path = format!("{table_location}/data/category=electronics");
    let books_path = format!("{table_location}/data/category=books");
    let clothing_path = format!("{table_location}/data/category=clothing");

    // Verify partition directories exist and contain data files
    assert!(
        file_io.exists(&electronics_path).await?,
        "Expected partition directory: {electronics_path}"
    );
    assert!(
        file_io.exists(&books_path).await?,
        "Expected partition directory: {books_path}"
    );
    assert!(
        file_io.exists(&clothing_path).await?,
        "Expected partition directory: {clothing_path}"
    );

    Ok(())
}

// ============================================================================
// DELETE OPERATION TESTS
// ============================================================================
// These tests validate DELETE SQL operations for Sprint 1.
// They are marked #[ignore] until the delete-action feature is enabled
// after rebasing feat/delete-action onto this branch.
//
// To run these tests after enabling the feature:
//   cargo test -p iceberg-datafusion --features delete-action test_delete
// ============================================================================

/// Helper to create a table with test data for DELETE tests.
/// Returns (catalog, namespace, ctx) so tests can use programmatic delete API.
async fn setup_delete_test_table(
    namespace_name: &str,
) -> Result<(Arc<MemoryCatalog>, NamespaceIdent, SessionContext)> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new(namespace_name.to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let creation = get_table_creation(temp_path(), "delete_table", None)?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert test data: 5 rows
    ctx.sql(&format!(
        "INSERT INTO catalog.{}.delete_table VALUES \
         (1, 'alice'), (2, 'bob'), (3, 'charlie'), (4, 'diana'), (5, 'eve')",
        namespace_name
    ))
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    Ok((client, namespace, ctx))
}

/// Test DELETE with a WHERE predicate.
/// Verifies that only matching rows are deleted.
#[tokio::test]
async fn test_delete_with_predicate() -> Result<()> {
    let (catalog, namespace, ctx) = setup_delete_test_table("test_delete_predicate").await?;

    // Create provider for programmatic delete
    let provider =
        IcebergTableProvider::try_new(catalog.clone(), namespace.clone(), "delete_table").await?;

    // Delete rows where foo1 > 3 (should delete diana and eve)
    let deleted_count = provider
        .delete(&ctx.state(), Some(col("foo1").gt(lit(3))))
        .await
        .unwrap();

    assert_eq!(deleted_count, 2, "Expected 2 rows deleted");

    // Verify remaining rows
    let df = ctx
        .sql("SELECT * FROM catalog.test_delete_predicate.delete_table ORDER BY foo1")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    // Should have 3 rows remaining: alice, bob, charlie
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "Expected 3 rows remaining after delete");

    Ok(())
}

/// Test DELETE without a predicate (full table delete).
/// Verifies that all rows are deleted.
#[tokio::test]
async fn test_delete_full_table() -> Result<()> {
    let (catalog, namespace, ctx) = setup_delete_test_table("test_delete_full").await?;

    // Create provider for programmatic delete
    let provider =
        IcebergTableProvider::try_new(catalog.clone(), namespace.clone(), "delete_table").await?;

    // Delete all rows (no predicate)
    let deleted_count = provider.delete(&ctx.state(), None).await.unwrap();

    assert_eq!(deleted_count, 5, "Expected 5 rows deleted");

    // Verify table is empty
    let df = ctx
        .sql("SELECT COUNT(*) as cnt FROM catalog.test_delete_full.delete_table")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(count.value(0), 0, "Expected 0 rows after full table delete");

    Ok(())
}

/// Test DELETE with a predicate that matches no rows.
/// Verifies zero count is returned and table is unchanged.
#[tokio::test]
async fn test_delete_no_match() -> Result<()> {
    let (catalog, namespace, ctx) = setup_delete_test_table("test_delete_nomatch").await?;

    // Create provider for programmatic delete
    let provider =
        IcebergTableProvider::try_new(catalog.clone(), namespace.clone(), "delete_table").await?;

    // Delete rows that don't exist
    let deleted_count = provider
        .delete(&ctx.state(), Some(col("foo1").gt(lit(100))))
        .await
        .unwrap();

    assert_eq!(deleted_count, 0, "Expected 0 rows deleted");

    // Verify all rows still exist
    let df = ctx
        .sql("SELECT COUNT(*) as cnt FROM catalog.test_delete_nomatch.delete_table")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(count.value(0), 5, "Expected 5 rows unchanged");

    Ok(())
}

/// Test that SELECT after DELETE properly excludes deleted rows.
/// This validates read-side delete file application.
#[tokio::test]
async fn test_select_after_delete() -> Result<()> {
    let (catalog, namespace, ctx) = setup_delete_test_table("test_select_after_delete").await?;

    // Create provider for programmatic delete
    let provider =
        IcebergTableProvider::try_new(catalog.clone(), namespace.clone(), "delete_table").await?;

    // Delete specific row (bob)
    let deleted_count = provider
        .delete(&ctx.state(), Some(col("foo2").eq(lit("bob"))))
        .await
        .unwrap();
    assert_eq!(deleted_count, 1, "Expected 1 row deleted");

    // SELECT should not return bob
    let df = ctx
        .sql("SELECT foo2 FROM catalog.test_select_after_delete.delete_table ORDER BY foo1")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let names: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .flatten()
                .map(|s| s.to_string())
        })
        .collect();

    assert_eq!(names, vec!["alice", "charlie", "diana", "eve"]);
    assert!(!names.contains(&"bob".to_string()), "bob should be deleted");

    Ok(())
}

/// Test DELETE on a partitioned table with identity partition.
/// Verifies that DELETE properly handles partitioned tables by:
/// 1. Deleting rows from a specific partition
/// 2. Leaving other partitions unaffected
/// 3. Writing partition-aware position delete files
#[tokio::test]
async fn test_delete_partitioned_table() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_delete_partitioned".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    // Create schema with partition column
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "category", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "value", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;

    let partition_spec = UnboundPartitionSpec::builder()
        .with_spec_id(0)
        .add_partition_field(2, "category", Transform::Identity)?
        .build();

    let creation = TableCreation::builder()
        .name("partitioned_delete_table".to_string())
        .location(temp_path())
        .schema(schema)
        .partition_spec(partition_spec)
        .properties(HashMap::new())
        .build();

    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert data across multiple partitions
    ctx.sql(
        "INSERT INTO catalog.test_delete_partitioned.partitioned_delete_table VALUES \
         (1, 'electronics', 'laptop'), \
         (2, 'electronics', 'phone'), \
         (3, 'books', 'novel'), \
         (4, 'books', 'textbook')",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // Verify initial row count
    let df = ctx
        .sql("SELECT COUNT(*) as cnt FROM catalog.test_delete_partitioned.partitioned_delete_table")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 4, "Should have 4 rows initially");

    // Create provider for programmatic delete
    let provider = IcebergTableProvider::try_new(
        client.clone(),
        namespace.clone(),
        "partitioned_delete_table",
    )
    .await?;

    // Delete all rows in the 'electronics' partition
    let deleted_count = provider
        .delete(&ctx.state(), Some(col("category").eq(lit("electronics"))))
        .await
        .expect("DELETE on partitioned table should succeed");

    assert_eq!(
        deleted_count, 2,
        "Should delete 2 rows from electronics partition"
    );

    // Re-register with fresh catalog to see changes
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);
    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Verify total row count decreased
    let df = ctx
        .sql("SELECT COUNT(*) as cnt FROM catalog.test_delete_partitioned.partitioned_delete_table")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(
        count, 2,
        "Should have 2 rows after deleting electronics partition"
    );

    // Verify only books partition remains
    let df = ctx
        .sql("SELECT category, value FROM catalog.test_delete_partitioned.partitioned_delete_table ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let categories: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .flatten()
                .map(|s| s.to_string())
        })
        .collect();

    assert_eq!(
        categories,
        vec!["books", "books"],
        "Only books category should remain"
    );

    Ok(())
}

/// Test DELETE on an empty table.
/// Verifies graceful handling with zero count.
#[tokio::test]
async fn test_delete_empty_table() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_delete_empty".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let creation = get_table_creation(temp_path(), "empty_table", None)?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Create provider for programmatic delete
    let provider =
        IcebergTableProvider::try_new(client.clone(), namespace.clone(), "empty_table").await?;

    // DELETE from empty table
    let deleted_count = provider
        .delete(&ctx.state(), Some(col("foo1").gt(lit(0))))
        .await
        .unwrap();

    // Verify zero count (graceful handling)
    assert_eq!(deleted_count, 0, "Expected 0 rows deleted from empty table");

    Ok(())
}

// ============================================================================
// UPDATE TESTS
// ============================================================================

/// Helper to set up a test table with data for UPDATE tests.
/// Creates a table with columns (foo1: Int, foo2: String) and inserts 5 rows.
async fn setup_update_test_table(
    namespace_name: &str,
) -> Result<(Arc<MemoryCatalog>, NamespaceIdent, SessionContext)> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new(namespace_name.to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let creation = get_table_creation(temp_path(), "update_table", None)?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert test data: 5 rows
    ctx.sql(&format!(
        "INSERT INTO catalog.{}.update_table VALUES \
         (1, 'alice'), (2, 'bob'), (3, 'charlie'), (4, 'diana'), (5, 'eve')",
        namespace_name
    ))
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    Ok((client, namespace, ctx))
}

/// Test UPDATE with a WHERE predicate.
/// Verifies that only matching rows are updated.
#[tokio::test]
async fn test_update_with_predicate() -> Result<()> {
    let (catalog, namespace, ctx) = setup_update_test_table("test_update_predicate").await?;

    // Create provider for programmatic update
    let provider =
        IcebergTableProvider::try_new(catalog.clone(), namespace.clone(), "update_table").await?;

    // Update rows where foo1 > 3 (diana and eve) to have foo2 = 'updated'
    let updated_count = provider
        .update()
        .await
        .unwrap()
        .set("foo2", lit("updated"))
        .filter(col("foo1").gt(lit(3)))
        .execute(&ctx.state())
        .await
        .unwrap();

    assert_eq!(updated_count, 2, "Expected 2 rows updated");

    // Verify the updates
    let df = ctx
        .sql("SELECT foo1, foo2 FROM catalog.test_update_predicate.update_table ORDER BY foo1")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let names: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            b.column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .flatten()
                .map(|s| s.to_string())
        })
        .collect();

    // First 3 rows should be unchanged, last 2 should be 'updated'
    assert_eq!(names, vec!["alice", "bob", "charlie", "updated", "updated"]);

    Ok(())
}

/// Test UPDATE without a WHERE clause (full table update).
/// All rows should be updated.
#[tokio::test]
async fn test_update_full_table() -> Result<()> {
    let (catalog, namespace, ctx) = setup_update_test_table("test_update_full").await?;

    let provider =
        IcebergTableProvider::try_new(catalog.clone(), namespace.clone(), "update_table").await?;

    // Update all rows to have foo2 = 'all_updated'
    let updated_count = provider
        .update()
        .await
        .unwrap()
        .set("foo2", lit("all_updated"))
        .execute(&ctx.state())
        .await
        .unwrap();

    assert_eq!(updated_count, 5, "Expected 5 rows updated");

    // Verify all rows have the new value
    let df = ctx
        .sql("SELECT foo2 FROM catalog.test_update_full.update_table")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let names: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .flatten()
                .map(|s| s.to_string())
        })
        .collect();

    assert!(
        names.iter().all(|n| n == "all_updated"),
        "All rows should be 'all_updated'"
    );
    assert_eq!(names.len(), 5);

    Ok(())
}

/// Test UPDATE with multiple columns being updated.
#[tokio::test]
async fn test_update_multiple_columns() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_update_multi".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    // Create table with 3 columns
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "status", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;

    let creation = get_table_creation(temp_path(), "multi_update_table", Some(schema))?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert test data
    ctx.sql(
        "INSERT INTO catalog.test_update_multi.multi_update_table VALUES \
         (1, 'alice', 'active'), (2, 'bob', 'active')",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    let provider =
        IcebergTableProvider::try_new(client.clone(), namespace.clone(), "multi_update_table")
            .await?;

    // Update both name and status where id = 1
    let updated_count = provider
        .update()
        .await
        .unwrap()
        .set("name", lit("alice_updated"))
        .set("status", lit("inactive"))
        .filter(col("id").eq(lit(1)))
        .execute(&ctx.state())
        .await
        .unwrap();

    assert_eq!(updated_count, 1, "Expected 1 row updated");

    // Verify the updates
    let df = ctx
        .sql(
            "SELECT id, name, status FROM catalog.test_update_multi.multi_update_table ORDER BY id",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let names: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            b.column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .flatten()
                .map(|s| s.to_string())
        })
        .collect();

    let statuses: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            b.column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .flatten()
                .map(|s| s.to_string())
        })
        .collect();

    assert_eq!(names, vec!["alice_updated", "bob"]);
    assert_eq!(statuses, vec!["inactive", "active"]);

    Ok(())
}

/// Test UPDATE with an expression (col = col + something).
#[tokio::test]
async fn test_update_with_expression() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_update_expr".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    // Create table with numeric column
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "value", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()?;

    let creation = get_table_creation(temp_path(), "expr_update_table", Some(schema))?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert test data
    ctx.sql(
        "INSERT INTO catalog.test_update_expr.expr_update_table VALUES \
         (1, 10), (2, 20), (3, 30)",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    let provider =
        IcebergTableProvider::try_new(client.clone(), namespace.clone(), "expr_update_table")
            .await?;

    // Update: value = value + 100 where value > 15
    let updated_count = provider
        .update()
        .await
        .unwrap()
        .set("value", col("value") + lit(100))
        .filter(col("value").gt(lit(15)))
        .execute(&ctx.state())
        .await
        .unwrap();

    assert_eq!(
        updated_count, 2,
        "Expected 2 rows updated (value 20 and 30)"
    );

    // Verify the updates
    let df = ctx
        .sql("SELECT id, value FROM catalog.test_update_expr.expr_update_table ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let values: Vec<i32> = batches
        .iter()
        .flat_map(|b| {
            b.column(1)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int32Array>()
                .unwrap()
                .iter()
                .flatten()
        })
        .collect();

    // value=10 stays at 10, value=20 becomes 120, value=30 becomes 130
    assert_eq!(values, vec![10, 120, 130]);

    Ok(())
}

/// Test UPDATE where no rows match the predicate.
/// Should return count of 0.
#[tokio::test]
async fn test_update_no_match() -> Result<()> {
    let (catalog, namespace, ctx) = setup_update_test_table("test_update_no_match").await?;

    let provider =
        IcebergTableProvider::try_new(catalog.clone(), namespace.clone(), "update_table").await?;

    // Update with predicate that matches nothing
    let updated_count = provider
        .update()
        .await
        .unwrap()
        .set("foo2", lit("never_seen"))
        .filter(col("foo1").gt(lit(100))) // No rows have foo1 > 100
        .execute(&ctx.state())
        .await
        .unwrap();

    assert_eq!(updated_count, 0, "Expected 0 rows updated");

    // Verify data unchanged
    let df = ctx
        .sql("SELECT COUNT(*) as cnt FROM catalog.test_update_no_match.update_table")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 5, "All 5 rows should still exist");

    Ok(())
}

/// Test that SELECT after UPDATE properly shows the updated values.
/// This validates end-to-end update and read path.
#[tokio::test]
async fn test_select_after_update() -> Result<()> {
    let (catalog, namespace, ctx) = setup_update_test_table("test_select_after_update").await?;

    let provider =
        IcebergTableProvider::try_new(catalog.clone(), namespace.clone(), "update_table").await?;

    // Update bob's name to 'robert'
    let updated_count = provider
        .update()
        .await
        .unwrap()
        .set("foo2", lit("robert"))
        .filter(col("foo2").eq(lit("bob")))
        .execute(&ctx.state())
        .await
        .unwrap();

    assert_eq!(updated_count, 1, "Expected 1 row updated");

    // SELECT should show 'robert' instead of 'bob'
    let df = ctx
        .sql("SELECT foo2 FROM catalog.test_select_after_update.update_table ORDER BY foo1")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let names: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .flatten()
                .map(|s| s.to_string())
        })
        .collect();

    assert_eq!(names, vec!["alice", "robert", "charlie", "diana", "eve"]);
    assert!(
        !names.contains(&"bob".to_string()),
        "bob should be replaced with robert"
    );

    Ok(())
}

/// Test UPDATE on a partitioned table - validate partition column protection.
/// Updating partition columns is rejected with a clear error message because it would
/// require moving rows between partitions, which is complex and not supported.
#[tokio::test]
async fn test_update_partitioned_table() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_update_partitioned".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    // Create schema with partition column
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "category", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "value", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;

    let partition_spec = UnboundPartitionSpec::builder()
        .with_spec_id(0)
        .add_partition_field(2, "category", Transform::Identity)?
        .build();

    let creation = TableCreation::builder()
        .name("partitioned_update_table".to_string())
        .location(temp_path())
        .schema(schema)
        .partition_spec(partition_spec)
        .properties(HashMap::new())
        .build();

    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert data across partitions
    ctx.sql(
        "INSERT INTO catalog.test_update_partitioned.partitioned_update_table VALUES \
         (1, 'electronics', 'laptop'), \
         (2, 'electronics', 'phone')",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    let provider = IcebergTableProvider::try_new(
        client.clone(),
        namespace.clone(),
        "partitioned_update_table",
    )
    .await?;

    // Attempt to update a PARTITION column should return an error
    let result = provider
        .update()
        .await
        .unwrap()
        .set("category", lit("books")) // category is the partition column!
        .filter(col("id").eq(lit(1)))
        .execute(&ctx.state())
        .await;

    assert!(
        result.is_err(),
        "Expected error when updating partition column"
    );
    let err = result.unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("Cannot UPDATE partition column"),
        "Expected error message about partition columns, got: {}",
        err_msg
    );

    // Verify data is still intact
    let df = ctx
        .sql("SELECT COUNT(*) as cnt FROM catalog.test_update_partitioned.partitioned_update_table")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(
        count, 2,
        "All 2 rows should still exist since update was rejected"
    );

    Ok(())
}

/// Test UPDATE with self-reference (SET col = col).
/// This should be a valid no-op operation.
#[tokio::test]
async fn test_update_self_reference() -> Result<()> {
    let (catalog, namespace, ctx) = setup_update_test_table("test_update_self_ref").await?;

    let provider =
        IcebergTableProvider::try_new(catalog.clone(), namespace.clone(), "update_table").await?;

    // Update foo2 = foo2 (self-reference, effectively no change in values)
    let updated_count = provider
        .update()
        .await
        .unwrap()
        .set("foo2", col("foo2"))
        .filter(col("foo1").eq(lit(1)))
        .execute(&ctx.state())
        .await
        .unwrap();

    assert_eq!(
        updated_count, 1,
        "Expected 1 row updated (even though value is same)"
    );

    // Verify data is unchanged
    let df = ctx
        .sql("SELECT foo2 FROM catalog.test_update_self_ref.update_table WHERE foo1 = 1")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let name = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);

    assert_eq!(
        name, "alice",
        "Value should remain 'alice' after self-reference update"
    );

    Ok(())
}

/// Test UPDATE on an empty table.
/// Should return count of 0 gracefully.
#[tokio::test]
async fn test_update_empty_table() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_update_empty".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let creation = get_table_creation(temp_path(), "empty_update_table", None)?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    let provider =
        IcebergTableProvider::try_new(client.clone(), namespace.clone(), "empty_update_table")
            .await?;

    // UPDATE on empty table
    let updated_count = provider
        .update()
        .await
        .unwrap()
        .set("foo2", lit("value"))
        .execute(&ctx.state())
        .await
        .unwrap();

    assert_eq!(updated_count, 0, "Expected 0 rows updated on empty table");

    Ok(())
}

/// Test UPDATE with cross-column swap (SET a = b, b = a).
/// This verifies that all expressions are evaluated against the ORIGINAL batch,
/// not the partially-updated batch.
#[tokio::test]
async fn test_update_cross_column_swap() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_update_swap".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    // Create table with two string columns
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "first", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "second", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;

    let creation = get_table_creation(temp_path(), "swap_table", Some(schema))?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert test data
    ctx.sql(
        "INSERT INTO catalog.test_update_swap.swap_table VALUES \
         (1, 'A', 'B')",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    let provider =
        IcebergTableProvider::try_new(client.clone(), namespace.clone(), "swap_table").await?;

    // Swap: SET first = second, second = first
    // If evaluated incorrectly (first updated, then second reads updated first),
    // we'd get (1, 'B', 'B') instead of (1, 'B', 'A')
    let updated_count = provider
        .update()
        .await
        .unwrap()
        .set("first", col("second"))
        .set("second", col("first"))
        .filter(col("id").eq(lit(1)))
        .execute(&ctx.state())
        .await
        .unwrap();

    assert_eq!(updated_count, 1, "Expected 1 row updated");

    // Verify the swap worked correctly
    let df = ctx
        .sql("SELECT id, first, second FROM catalog.test_update_swap.swap_table")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let first_val = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);

    let second_val = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);

    // After swap: first should be 'B' (original second), second should be 'A' (original first)
    assert_eq!(
        first_val, "B",
        "first should be swapped to original second value"
    );
    assert_eq!(
        second_val, "A",
        "second should be swapped to original first value"
    );

    Ok(())
}

// =============================================================================
// SQL Interface Tests (Epic 2)
// These tests verify that SQL UPDATE and DELETE statements work via the
// TableProvider trait methods (dml_capabilities, delete_from, update).
// =============================================================================

#[tokio::test]
async fn test_sql_update_with_predicate() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_sql_update".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let location = temp_path();
    let creation = get_table_creation(&location, "sql_update_table", None)?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);
    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert test data
    ctx.sql(
        "INSERT INTO catalog.test_sql_update.sql_update_table VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // Execute SQL UPDATE
    let result = ctx
        .sql("UPDATE catalog.test_sql_update.sql_update_table SET foo2 = 'updated' WHERE foo1 = 2")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Verify count (should be 1 row updated)
    assert!(!result.is_empty());
    let count = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 1, "Expected 1 row to be updated");

    // Verify the data was updated correctly
    let batches = ctx
        .sql("SELECT foo1, foo2 FROM catalog.test_sql_update.sql_update_table ORDER BY foo1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let values: Vec<_> = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|v| v.unwrap().to_string())
        .collect();

    assert_eq!(values, vec!["a", "updated", "c"]);

    Ok(())
}

#[tokio::test]
async fn test_sql_update_full_table() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_sql_update_full".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let location = temp_path();
    let creation = get_table_creation(&location, "sql_update_full_table", None)?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);
    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert test data
    ctx.sql("INSERT INTO catalog.test_sql_update_full.sql_update_full_table VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Execute SQL UPDATE without WHERE clause (update all rows)
    let result = ctx
        .sql("UPDATE catalog.test_sql_update_full.sql_update_full_table SET foo2 = 'all_updated'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Verify count (should be 3 rows updated)
    let count = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 3, "Expected 3 rows to be updated");

    // Verify all data was updated
    let batches = ctx
        .sql("SELECT DISTINCT foo2 FROM catalog.test_sql_update_full.sql_update_full_table")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let value = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    assert_eq!(value, "all_updated");

    Ok(())
}

#[tokio::test]
async fn test_sql_delete_with_predicate() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_sql_delete".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let location = temp_path();
    let creation = get_table_creation(&location, "sql_delete_table", None)?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);
    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert test data
    ctx.sql(
        "INSERT INTO catalog.test_sql_delete.sql_delete_table VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // Execute SQL DELETE
    let result = ctx
        .sql("DELETE FROM catalog.test_sql_delete.sql_delete_table WHERE foo1 = 2")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Verify count (should be 1 row deleted)
    let count = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 1, "Expected 1 row to be deleted");

    // Verify the data after delete
    let batches = ctx
        .sql("SELECT foo1 FROM catalog.test_sql_delete.sql_delete_table ORDER BY foo1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "Expected 2 rows remaining after delete");

    Ok(())
}

#[tokio::test]
async fn test_sql_delete_full_table() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_sql_delete_full".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let location = temp_path();
    let creation = get_table_creation(&location, "sql_delete_full_table", None)?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);
    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert test data
    ctx.sql("INSERT INTO catalog.test_sql_delete_full.sql_delete_full_table VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Execute SQL DELETE without WHERE clause (delete all rows)
    let result = ctx
        .sql("DELETE FROM catalog.test_sql_delete_full.sql_delete_full_table")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Verify count (should be 3 rows deleted)
    let count = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 3, "Expected 3 rows to be deleted");

    // Verify table is empty
    let batches = ctx
        .sql("SELECT COUNT(*) as cnt FROM catalog.test_sql_delete_full.sql_delete_full_table")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let remaining_count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(remaining_count, 0, "Expected 0 rows after full delete");

    Ok(())
}

// =============================================================================
// Error Handling Tests (P1 Task 4)
// These tests verify proper error handling for invalid operations.
// =============================================================================

/// Tests that UPDATE with an invalid column name returns a proper error.
#[tokio::test]
async fn test_update_invalid_column() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_update_invalid_col".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let location = temp_path();
    let creation = get_table_creation(&location, "invalid_col_table", None)?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);
    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert test data
    ctx.sql("INSERT INTO catalog.test_update_invalid_col.invalid_col_table VALUES (1, 'a')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Try to UPDATE a non-existent column
    let result = ctx
        .sql("UPDATE catalog.test_update_invalid_col.invalid_col_table SET nonexistent_col = 'x'")
        .await;

    // Should fail at planning or execution time
    assert!(
        result.is_err(),
        "Expected error when updating non-existent column"
    );

    Ok(())
}

/// Tests that DELETE with invalid predicate returns a proper error.
#[tokio::test]
async fn test_delete_invalid_predicate() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_delete_invalid_pred".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let location = temp_path();
    let creation = get_table_creation(&location, "invalid_pred_table", None)?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);
    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert test data
    ctx.sql("INSERT INTO catalog.test_delete_invalid_pred.invalid_pred_table VALUES (1, 'a')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Try to DELETE with a non-existent column in predicate
    let result = ctx
        .sql("DELETE FROM catalog.test_delete_invalid_pred.invalid_pred_table WHERE bad_column = 1")
        .await;

    // Should fail at planning time
    assert!(
        result.is_err(),
        "Expected error when filtering on non-existent column"
    );

    Ok(())
}

/// Tests that UPDATE with invalid SQL syntax returns an error.
#[tokio::test]
async fn test_update_invalid_syntax() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_update_syntax".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let location = temp_path();
    let creation = get_table_creation(&location, "syntax_table", None)?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);
    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert test data
    ctx.sql("INSERT INTO catalog.test_update_syntax.syntax_table VALUES (1, 'a')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Try to UPDATE with invalid SQL syntax (missing SET value)
    let result = ctx
        .sql("UPDATE catalog.test_update_syntax.syntax_table SET foo1 =")
        .await;

    // Should fail at parsing time
    assert!(result.is_err(), "Expected error for invalid SQL syntax");

    Ok(())
}

/// Tests that SELECT after a failed UPDATE leaves data unchanged.
#[tokio::test]
async fn test_update_failure_no_side_effects() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_update_no_side".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let location = temp_path();
    let creation = get_table_creation(&location, "no_side_effects_table", None)?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);
    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert test data
    ctx.sql("INSERT INTO catalog.test_update_no_side.no_side_effects_table VALUES (1, 'original')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Try an invalid UPDATE (should fail)
    let _result = ctx
        .sql("UPDATE catalog.test_update_no_side.no_side_effects_table SET nonexistent = 'x'")
        .await;
    // We don't care if this errors at plan or execute time

    // Verify original data is unchanged
    let batches = ctx
        .sql("SELECT foo2 FROM catalog.test_update_no_side.no_side_effects_table")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let value = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);

    assert_eq!(
        value, "original",
        "Data should remain unchanged after failed UPDATE"
    );

    Ok(())
}

/// Tests that concurrent SELECT operations work correctly after UPDATE.
#[tokio::test]
async fn test_select_after_concurrent_operations() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_concurrent_select".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let location = temp_path();
    let creation = get_table_creation(&location, "concurrent_table", None)?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);
    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert test data
    ctx.sql("INSERT INTO catalog.test_concurrent_select.concurrent_table VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Execute UPDATE
    ctx.sql("UPDATE catalog.test_concurrent_select.concurrent_table SET foo2 = 'updated' WHERE foo1 = 2")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // SELECT should see updated data
    let batches = ctx
        .sql("SELECT foo2 FROM catalog.test_concurrent_select.concurrent_table WHERE foo1 = 2")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let value = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);

    assert_eq!(value, "updated");

    Ok(())
}

// ============================================================================
// MERGE TESTS
// ============================================================================

/// Helper to set up a test table with data for MERGE tests.
async fn setup_merge_test_table(
    namespace_name: &str,
) -> Result<(Arc<MemoryCatalog>, NamespaceIdent, SessionContext)> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new(namespace_name.to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let creation = get_table_creation(temp_path(), "merge_table", None)?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert initial data: [(1, "alice"), (2, "bob"), (3, "charlie")]
    ctx.sql(&format!(
        "INSERT INTO catalog.{}.merge_table VALUES \
         (1, 'alice'), \
         (2, 'bob'), \
         (3, 'charlie')",
        namespace_name
    ))
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    Ok((client, namespace, ctx))
}

/// Test MERGE classification with WHEN MATCHED UPDATE.
/// Verifies that matched rows are counted correctly.
#[tokio::test]
async fn test_merge_classification_matched_update() -> Result<()> {
    use datafusion::datasource::MemTable;
    use datafusion::prelude::col as df_col;
    use iceberg_datafusion::merge::MergeStats;

    let (catalog, namespace, ctx) = setup_merge_test_table("test_merge_matched").await?;

    // Create source DataFrame: [(2, "bob_updated"), (3, "charlie_updated")]
    // These match rows 2 and 3 in target
    let source_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
        datafusion::arrow::datatypes::Field::new(
            "id",
            datafusion::arrow::datatypes::DataType::Int32,
            false,
        ),
        datafusion::arrow::datatypes::Field::new(
            "name",
            datafusion::arrow::datatypes::DataType::Utf8,
            false,
        ),
    ]));

    let id_array = Arc::new(datafusion::arrow::array::Int32Array::from(vec![2, 3]))
        as datafusion::arrow::array::ArrayRef;
    let name_array = Arc::new(StringArray::from(vec!["bob_updated", "charlie_updated"]))
        as datafusion::arrow::array::ArrayRef;
    let source_batch = datafusion::arrow::array::RecordBatch::try_new(source_schema.clone(), vec![
        id_array, name_array,
    ])
    .unwrap();

    let source_table =
        Arc::new(MemTable::try_new(source_schema, vec![vec![source_batch]]).unwrap());
    ctx.register_table("source_data", source_table).unwrap();
    let source_df = ctx.table("source_data").await.unwrap();

    // Create provider for programmatic merge
    let provider =
        IcebergTableProvider::try_new(catalog.clone(), namespace.clone(), "merge_table").await?;

    // Execute MERGE with WHEN MATCHED THEN UPDATE *
    let stats: MergeStats = provider
        .merge(source_df)
        .await
        .unwrap()
        .on(df_col("target.foo1").eq(df_col("source.id")))
        .when_matched(None)
        .update_all()
        .execute(&ctx.state())
        .await
        .unwrap();

    // Expected classification:
    // - Row (1, "alice"): NOT_MATCHED_BY_SOURCE (target only) -> no action
    // - Row (2, "bob"): MATCHED -> UPDATE
    // - Row (3, "charlie"): MATCHED -> UPDATE
    // - No rows from source are NOT_MATCHED (all source rows matched)
    assert_eq!(
        stats.rows_updated, 2,
        "Expected 2 rows classified for UPDATE"
    );
    assert_eq!(
        stats.rows_inserted, 0,
        "Expected 0 rows classified for INSERT"
    );
    assert_eq!(
        stats.rows_deleted, 0,
        "Expected 0 rows classified for DELETE"
    );

    Ok(())
}

/// Test MERGE classification with WHEN NOT MATCHED INSERT.
/// Verifies that source-only rows are counted correctly.
#[tokio::test]
async fn test_merge_classification_not_matched_insert() -> Result<()> {
    use datafusion::datasource::MemTable;
    use datafusion::prelude::col as df_col;
    use iceberg_datafusion::merge::MergeStats;

    let (catalog, namespace, ctx) = setup_merge_test_table("test_merge_insert").await?;

    // Create source DataFrame: [(4, "diana"), (5, "eve")]
    // These do NOT match any rows in target (target has ids 1, 2, 3)
    // Source must use same column names as target for INSERT * to work
    let source_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
        datafusion::arrow::datatypes::Field::new(
            "foo1",
            datafusion::arrow::datatypes::DataType::Int32,
            false,
        ),
        datafusion::arrow::datatypes::Field::new(
            "foo2",
            datafusion::arrow::datatypes::DataType::Utf8,
            false,
        ),
    ]));

    let foo1_array = Arc::new(datafusion::arrow::array::Int32Array::from(vec![4, 5]))
        as datafusion::arrow::array::ArrayRef;
    let foo2_array =
        Arc::new(StringArray::from(vec!["diana", "eve"])) as datafusion::arrow::array::ArrayRef;
    let source_batch = datafusion::arrow::array::RecordBatch::try_new(source_schema.clone(), vec![
        foo1_array, foo2_array,
    ])
    .unwrap();

    let source_table =
        Arc::new(MemTable::try_new(source_schema, vec![vec![source_batch]]).unwrap());
    ctx.register_table("source_data2", source_table).unwrap();
    let source_df = ctx.table("source_data2").await.unwrap();

    // Create provider for programmatic merge
    let provider =
        IcebergTableProvider::try_new(catalog.clone(), namespace.clone(), "merge_table").await?;

    // Execute MERGE with WHEN NOT MATCHED THEN INSERT *
    let stats: MergeStats = provider
        .merge(source_df)
        .await
        .unwrap()
        .on(df_col("target.foo1").eq(df_col("source.foo1")))
        .when_not_matched(None)
        .insert_all()
        .execute(&ctx.state())
        .await
        .unwrap();

    // Expected classification:
    // - All 3 target rows (1,2,3): NOT_MATCHED_BY_SOURCE -> no action (no clause for this)
    // - Row (4, "diana"): NOT_MATCHED -> INSERT
    // - Row (5, "eve"): NOT_MATCHED -> INSERT
    assert_eq!(
        stats.rows_inserted, 2,
        "Expected 2 rows classified for INSERT"
    );
    assert_eq!(
        stats.rows_updated, 0,
        "Expected 0 rows classified for UPDATE"
    );
    assert_eq!(
        stats.rows_deleted, 0,
        "Expected 0 rows classified for DELETE"
    );

    Ok(())
}

/// Test MERGE classification with WHEN NOT MATCHED BY SOURCE DELETE.
/// Verifies that target-only rows are counted correctly.
#[tokio::test]
async fn test_merge_classification_not_matched_by_source_delete() -> Result<()> {
    use datafusion::datasource::MemTable;
    use datafusion::prelude::col as df_col;
    use iceberg_datafusion::merge::MergeStats;

    let (catalog, namespace, ctx) = setup_merge_test_table("test_merge_delete").await?;

    // Create source DataFrame: [(2, "bob_updated")]
    // Only row 2 matches; rows 1 and 3 are NOT_MATCHED_BY_SOURCE
    let source_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
        datafusion::arrow::datatypes::Field::new(
            "id",
            datafusion::arrow::datatypes::DataType::Int32,
            false,
        ),
        datafusion::arrow::datatypes::Field::new(
            "name",
            datafusion::arrow::datatypes::DataType::Utf8,
            false,
        ),
    ]));

    let id_array = Arc::new(datafusion::arrow::array::Int32Array::from(vec![2]))
        as datafusion::arrow::array::ArrayRef;
    let name_array =
        Arc::new(StringArray::from(vec!["bob_updated"])) as datafusion::arrow::array::ArrayRef;
    let source_batch = datafusion::arrow::array::RecordBatch::try_new(source_schema.clone(), vec![
        id_array, name_array,
    ])
    .unwrap();

    let source_table =
        Arc::new(MemTable::try_new(source_schema, vec![vec![source_batch]]).unwrap());
    ctx.register_table("source_data3", source_table).unwrap();
    let source_df = ctx.table("source_data3").await.unwrap();

    // Create provider for programmatic merge
    let provider =
        IcebergTableProvider::try_new(catalog.clone(), namespace.clone(), "merge_table").await?;

    // Execute MERGE with WHEN NOT MATCHED BY SOURCE THEN DELETE
    let stats: MergeStats = provider
        .merge(source_df)
        .await
        .unwrap()
        .on(df_col("target.foo1").eq(df_col("source.id")))
        .when_not_matched_by_source(None)
        .delete()
        .execute(&ctx.state())
        .await
        .unwrap();

    // Expected classification:
    // - Row (1, "alice"): NOT_MATCHED_BY_SOURCE -> DELETE
    // - Row (2, "bob"): MATCHED -> no action (no WHEN MATCHED clause)
    // - Row (3, "charlie"): NOT_MATCHED_BY_SOURCE -> DELETE
    assert_eq!(
        stats.rows_deleted, 2,
        "Expected 2 rows classified for DELETE"
    );
    assert_eq!(
        stats.rows_inserted, 0,
        "Expected 0 rows classified for INSERT"
    );
    assert_eq!(
        stats.rows_updated, 0,
        "Expected 0 rows classified for UPDATE"
    );

    Ok(())
}

/// Test MERGE classification with all three clause types.
/// Verifies comprehensive CDC-style MERGE behavior.
#[tokio::test]
async fn test_merge_classification_full_cdc() -> Result<()> {
    use datafusion::datasource::MemTable;
    use datafusion::prelude::col as df_col;
    use iceberg_datafusion::merge::MergeStats;

    let (catalog, namespace, ctx) = setup_merge_test_table("test_merge_cdc").await?;

    // Create source DataFrame simulating CDC data:
    // - (2, "bob_updated"): matches target row 2 -> UPDATE
    // - (4, "diana"): new row -> INSERT
    // Target rows 1 and 3 are NOT_MATCHED_BY_SOURCE -> DELETE
    // Source must use same column names as target for INSERT * to work
    let source_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
        datafusion::arrow::datatypes::Field::new(
            "foo1",
            datafusion::arrow::datatypes::DataType::Int32,
            false,
        ),
        datafusion::arrow::datatypes::Field::new(
            "foo2",
            datafusion::arrow::datatypes::DataType::Utf8,
            false,
        ),
    ]));

    let id_array = Arc::new(datafusion::arrow::array::Int32Array::from(vec![2, 4]))
        as datafusion::arrow::array::ArrayRef;
    let name_array = Arc::new(StringArray::from(vec!["bob_updated", "diana"]))
        as datafusion::arrow::array::ArrayRef;
    let source_batch = datafusion::arrow::array::RecordBatch::try_new(source_schema.clone(), vec![
        id_array, name_array,
    ])
    .unwrap();

    let source_table =
        Arc::new(MemTable::try_new(source_schema, vec![vec![source_batch]]).unwrap());
    ctx.register_table("source_data4", source_table).unwrap();
    let source_df = ctx.table("source_data4").await.unwrap();

    // Create provider for programmatic merge
    let provider =
        IcebergTableProvider::try_new(catalog.clone(), namespace.clone(), "merge_table").await?;

    // Execute full CDC MERGE:
    // - WHEN MATCHED -> UPDATE *
    // - WHEN NOT MATCHED -> INSERT *
    // - WHEN NOT MATCHED BY SOURCE -> DELETE
    let stats: MergeStats = provider
        .merge(source_df)
        .await
        .unwrap()
        .on(df_col("target.foo1").eq(df_col("source.foo1")))
        .when_matched(None)
        .update_all()
        .when_not_matched(None)
        .insert_all()
        .when_not_matched_by_source(None)
        .delete()
        .execute(&ctx.state())
        .await
        .unwrap();

    // Expected classification:
    // - Row (1, "alice"): NOT_MATCHED_BY_SOURCE -> DELETE
    // - Row (2, "bob"): MATCHED -> UPDATE
    // - Row (3, "charlie"): NOT_MATCHED_BY_SOURCE -> DELETE
    // - Row (4, "diana"): NOT_MATCHED -> INSERT
    assert_eq!(
        stats.rows_updated, 1,
        "Expected 1 row classified for UPDATE (row 2)"
    );
    assert_eq!(
        stats.rows_inserted, 1,
        "Expected 1 row classified for INSERT (row 4)"
    );
    assert_eq!(
        stats.rows_deleted, 2,
        "Expected 2 rows classified for DELETE (rows 1, 3)"
    );

    // Total affected = 4 (1 update + 1 insert + 2 deletes)
    assert_eq!(stats.total_affected(), 4, "Expected 4 total rows affected");

    Ok(())
}

/// Test MERGE E2E: verify commit persists by reloading table from catalog.
/// This is the critical test that proves MERGE actually commits atomically.
#[tokio::test]
async fn test_merge_e2e_commit_persists() -> Result<()> {
    use datafusion::datasource::MemTable;
    use datafusion::prelude::col as df_col;
    use iceberg_datafusion::merge::MergeStats;

    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_merge_e2e".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let creation = get_table_creation(temp_path(), "e2e_table", None)?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog_provider = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog_provider.clone());

    // Insert initial data: [(1, "alice"), (2, "bob"), (3, "charlie")]
    ctx.sql(&format!(
        "INSERT INTO catalog.{}.e2e_table VALUES \
         (1, 'alice'), \
         (2, 'bob'), \
         (3, 'charlie')",
        "test_merge_e2e"
    ))
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // Verify initial data
    let initial_count = ctx
        .sql("SELECT COUNT(*) as cnt FROM catalog.test_merge_e2e.e2e_table")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let initial_cnt = initial_count[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(initial_cnt, 3, "Initial table should have 3 rows");

    // Create source DataFrame for MERGE:
    // - (2, "bob_updated"): matches target -> UPDATE
    // - (4, "diana"): new row -> INSERT
    // - Rows 1 and 3: NOT_MATCHED_BY_SOURCE -> DELETE
    let source_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
        datafusion::arrow::datatypes::Field::new(
            "foo1",
            datafusion::arrow::datatypes::DataType::Int32,
            false,
        ),
        datafusion::arrow::datatypes::Field::new(
            "foo2",
            datafusion::arrow::datatypes::DataType::Utf8,
            false,
        ),
    ]));

    let id_array = Arc::new(datafusion::arrow::array::Int32Array::from(vec![2, 4]))
        as datafusion::arrow::array::ArrayRef;
    let name_array = Arc::new(StringArray::from(vec!["bob_updated", "diana"]))
        as datafusion::arrow::array::ArrayRef;
    let source_batch = datafusion::arrow::array::RecordBatch::try_new(source_schema.clone(), vec![
        id_array, name_array,
    ])
    .unwrap();

    let source_table =
        Arc::new(MemTable::try_new(source_schema, vec![vec![source_batch]]).unwrap());
    ctx.register_table("e2e_source", source_table).unwrap();
    let source_df = ctx.table("e2e_source").await.unwrap();

    // Execute MERGE
    let provider =
        IcebergTableProvider::try_new(client.clone(), namespace.clone(), "e2e_table").await?;

    let stats: MergeStats = provider
        .merge(source_df)
        .await
        .unwrap()
        .on(df_col("target.foo1").eq(df_col("source.foo1")))
        .when_matched(None)
        .update_all()
        .when_not_matched(None)
        .insert_all()
        .when_not_matched_by_source(None)
        .delete()
        .execute(&ctx.state())
        .await
        .unwrap();

    // Verify stats
    assert_eq!(stats.rows_updated, 1, "Should update 1 row (id=2)");
    assert_eq!(stats.rows_inserted, 1, "Should insert 1 row (id=4)");
    assert_eq!(stats.rows_deleted, 2, "Should delete 2 rows (ids 1, 3)");

    // ========== CRITICAL: Reload table from catalog ==========
    // This proves the commit actually persisted to the table metadata
    let fresh_provider =
        IcebergTableProvider::try_new(client.clone(), namespace.clone(), "e2e_table").await?;

    // Register the fresh provider
    ctx.deregister_table("fresh_e2e_table").ok();
    ctx.register_table("fresh_e2e_table", Arc::new(fresh_provider))
        .unwrap();

    // Query the freshly loaded table
    let result = ctx
        .sql("SELECT foo1, foo2 FROM fresh_e2e_table ORDER BY foo1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Should have 2 rows: (2, "bob_updated") and (4, "diana")
    assert_eq!(result.len(), 1, "Should have one batch");
    let batch = &result[0];
    assert_eq!(batch.num_rows(), 2, "Should have 2 rows after MERGE");

    let foo1_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int32Array>()
        .unwrap();
    let foo2_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    // Verify row 1: (2, "bob_updated")
    assert_eq!(foo1_col.value(0), 2);
    assert_eq!(foo2_col.value(0), "bob_updated");

    // Verify row 2: (4, "diana")
    assert_eq!(foo1_col.value(1), 4);
    assert_eq!(foo2_col.value(1), "diana");

    Ok(())
}

/// Test MERGE on a partitioned table.
/// Verifies that MERGE properly handles partitioned tables by:
/// 1. Updating rows in different partitions
/// 2. Inserting new rows into correct partitions
/// 3. Deleting rows from specific partitions
/// 4. Writing partition-aware data and delete files
#[tokio::test]
async fn test_merge_partitioned_table() -> Result<()> {
    use datafusion::datasource::MemTable;
    use datafusion::prelude::col as df_col;
    use iceberg_datafusion::merge::MergeStats;

    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_merge_partitioned".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    // Create schema with partition column
    // Schema: (id: Int, category: String, value: String) partitioned by category
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "category", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "value", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;

    let partition_spec = UnboundPartitionSpec::builder()
        .with_spec_id(0)
        .add_partition_field(2, "category", Transform::Identity)?
        .build();

    let creation = TableCreation::builder()
        .name("partitioned_merge_table".to_string())
        .location(temp_path())
        .schema(schema)
        .partition_spec(partition_spec)
        .properties(HashMap::new())
        .build();

    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert data across multiple partitions:
    // electronics: (1, 'electronics', 'laptop'), (2, 'electronics', 'phone')
    // books: (3, 'books', 'novel'), (4, 'books', 'textbook')
    ctx.sql(
        "INSERT INTO catalog.test_merge_partitioned.partitioned_merge_table VALUES \
         (1, 'electronics', 'laptop'), \
         (2, 'electronics', 'phone'), \
         (3, 'books', 'novel'), \
         (4, 'books', 'textbook')",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // Verify initial row count
    let df = ctx
        .sql("SELECT COUNT(*) as cnt FROM catalog.test_merge_partitioned.partitioned_merge_table")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 4, "Should have 4 rows initially");

    // Create source DataFrame for MERGE:
    // - (2, 'electronics', 'smartphone'): matches id=2 -> UPDATE value
    // - (5, 'clothing', 'shirt'): new row in new partition -> INSERT
    // - id 1 (laptop) will be deleted (NOT_MATCHED_BY_SOURCE)
    // - id 3 and 4 (books) will be deleted (NOT_MATCHED_BY_SOURCE)
    let source_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
        datafusion::arrow::datatypes::Field::new(
            "id",
            datafusion::arrow::datatypes::DataType::Int32,
            false,
        ),
        datafusion::arrow::datatypes::Field::new(
            "category",
            datafusion::arrow::datatypes::DataType::Utf8,
            false,
        ),
        datafusion::arrow::datatypes::Field::new(
            "value",
            datafusion::arrow::datatypes::DataType::Utf8,
            false,
        ),
    ]));

    let id_array = Arc::new(datafusion::arrow::array::Int32Array::from(vec![2, 5]))
        as datafusion::arrow::array::ArrayRef;
    let category_array = Arc::new(StringArray::from(vec!["electronics", "clothing"]))
        as datafusion::arrow::array::ArrayRef;
    let value_array = Arc::new(StringArray::from(vec!["smartphone", "shirt"]))
        as datafusion::arrow::array::ArrayRef;
    let source_batch = datafusion::arrow::array::RecordBatch::try_new(source_schema.clone(), vec![
        id_array,
        category_array,
        value_array,
    ])
    .unwrap();

    let source_table =
        Arc::new(MemTable::try_new(source_schema, vec![vec![source_batch]]).unwrap());
    ctx.register_table("merge_source", source_table).unwrap();
    let source_df = ctx.table("merge_source").await.unwrap();

    // Create provider for programmatic merge
    let provider =
        IcebergTableProvider::try_new(client.clone(), namespace.clone(), "partitioned_merge_table")
            .await?;

    // Execute full CDC MERGE on partitioned table:
    // - WHEN MATCHED -> UPDATE (explicit columns, excluding partition column)
    // - WHEN NOT MATCHED -> INSERT *
    // - WHEN NOT MATCHED BY SOURCE -> DELETE
    // Note: We must use explicit update() rather than update_all() because
    // update_all() doesn't allow updating partition columns
    let stats: MergeStats = provider
        .merge(source_df)
        .await
        .unwrap()
        .on(df_col("target.id").eq(df_col("source.id")))
        .when_matched(None)
        .update(vec![("value", df_col("source_value"))])
        .when_not_matched(None)
        .insert_all()
        .when_not_matched_by_source(None)
        .delete()
        .execute(&ctx.state())
        .await
        .unwrap();

    // Expected classification:
    // - Row (1, 'electronics', 'laptop'): NOT_MATCHED_BY_SOURCE -> DELETE
    // - Row (2, 'electronics', 'phone'): MATCHED -> UPDATE to 'smartphone'
    // - Row (3, 'books', 'novel'): NOT_MATCHED_BY_SOURCE -> DELETE
    // - Row (4, 'books', 'textbook'): NOT_MATCHED_BY_SOURCE -> DELETE
    // - Row (5, 'clothing', 'shirt'): NOT_MATCHED -> INSERT
    assert_eq!(
        stats.rows_updated, 1,
        "Expected 1 row classified for UPDATE (id=2)"
    );
    assert_eq!(
        stats.rows_inserted, 1,
        "Expected 1 row classified for INSERT (id=5)"
    );
    assert_eq!(
        stats.rows_deleted, 3,
        "Expected 3 rows classified for DELETE (ids 1, 3, 4)"
    );

    // Reload table from catalog to verify commit persisted
    let fresh_provider =
        IcebergTableProvider::try_new(client.clone(), namespace.clone(), "partitioned_merge_table")
            .await?;

    ctx.deregister_table("fresh_partitioned_table").ok();
    ctx.register_table("fresh_partitioned_table", Arc::new(fresh_provider))
        .unwrap();

    // Query the freshly loaded table
    let result = ctx
        .sql("SELECT id, category, value FROM fresh_partitioned_table ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Should have 2 rows:
    // (2, 'electronics', 'smartphone') - updated
    // (5, 'clothing', 'shirt') - inserted
    assert_eq!(result.len(), 1, "Should have one batch");
    let batch = &result[0];
    assert_eq!(batch.num_rows(), 2, "Should have 2 rows after MERGE");

    let id_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int32Array>()
        .unwrap();
    let category_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let value_col = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    // Verify row 1: (2, 'electronics', 'smartphone') - updated
    assert_eq!(id_col.value(0), 2);
    assert_eq!(category_col.value(0), "electronics");
    assert_eq!(value_col.value(0), "smartphone");

    // Verify row 2: (5, 'clothing', 'shirt') - inserted
    assert_eq!(id_col.value(1), 5);
    assert_eq!(category_col.value(1), "clothing");
    assert_eq!(value_col.value(1), "shirt");

    Ok(())
}

/// Test MERGE on partitioned table with UPDATE that keeps rows in same partition.
/// This is a simpler case where updates don't change the partition column.
#[tokio::test]
async fn test_merge_partitioned_update_only() -> Result<()> {
    use datafusion::datasource::MemTable;
    use datafusion::prelude::col as df_col;
    use iceberg_datafusion::merge::MergeStats;

    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_merge_partitioned_upd".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    // Create schema with partition column
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "category", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "value", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;

    let partition_spec = UnboundPartitionSpec::builder()
        .with_spec_id(0)
        .add_partition_field(2, "category", Transform::Identity)?
        .build();

    let creation = TableCreation::builder()
        .name("partitioned_update_merge".to_string())
        .location(temp_path())
        .schema(schema)
        .partition_spec(partition_spec)
        .properties(HashMap::new())
        .build();

    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert data
    ctx.sql(
        "INSERT INTO catalog.test_merge_partitioned_upd.partitioned_update_merge VALUES \
         (1, 'electronics', 'laptop'), \
         (2, 'electronics', 'phone'), \
         (3, 'books', 'novel')",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // Create source: update values for ids 1 and 3 (in different partitions)
    let source_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
        datafusion::arrow::datatypes::Field::new(
            "id",
            datafusion::arrow::datatypes::DataType::Int32,
            false,
        ),
        datafusion::arrow::datatypes::Field::new(
            "category",
            datafusion::arrow::datatypes::DataType::Utf8,
            false,
        ),
        datafusion::arrow::datatypes::Field::new(
            "value",
            datafusion::arrow::datatypes::DataType::Utf8,
            false,
        ),
    ]));

    // Source updates both electronics and books partitions
    let id_array = Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 3]))
        as datafusion::arrow::array::ArrayRef;
    let category_array = Arc::new(StringArray::from(vec!["electronics", "books"]))
        as datafusion::arrow::array::ArrayRef;
    let value_array = Arc::new(StringArray::from(vec!["gaming_laptop", "epic_novel"]))
        as datafusion::arrow::array::ArrayRef;
    let source_batch = datafusion::arrow::array::RecordBatch::try_new(source_schema.clone(), vec![
        id_array,
        category_array,
        value_array,
    ])
    .unwrap();

    let source_table =
        Arc::new(MemTable::try_new(source_schema, vec![vec![source_batch]]).unwrap());
    ctx.register_table("upd_source", source_table).unwrap();
    let source_df = ctx.table("upd_source").await.unwrap();

    let provider = IcebergTableProvider::try_new(
        client.clone(),
        namespace.clone(),
        "partitioned_update_merge",
    )
    .await?;

    // Execute MERGE with only WHEN MATCHED UPDATE
    // Note: We must use explicit update() rather than update_all() because
    // update_all() doesn't allow updating partition columns
    let stats: MergeStats = provider
        .merge(source_df)
        .await
        .unwrap()
        .on(df_col("target.id").eq(df_col("source.id")))
        .when_matched(None)
        .update(vec![("value", df_col("source_value"))])
        .execute(&ctx.state())
        .await
        .unwrap();

    // Should update 2 rows (ids 1 and 3)
    assert_eq!(stats.rows_updated, 2, "Expected 2 rows updated");
    assert_eq!(stats.rows_inserted, 0, "Expected 0 rows inserted");
    assert_eq!(stats.rows_deleted, 0, "Expected 0 rows deleted");

    // Verify data persisted correctly
    let fresh_provider = IcebergTableProvider::try_new(
        client.clone(),
        namespace.clone(),
        "partitioned_update_merge",
    )
    .await?;

    ctx.deregister_table("fresh_table").ok();
    ctx.register_table("fresh_table", Arc::new(fresh_provider))
        .unwrap();

    let result = ctx
        .sql("SELECT id, category, value FROM fresh_table ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(result.len(), 1, "Should have one batch");
    let batch = &result[0];
    assert_eq!(batch.num_rows(), 3, "Should have 3 rows after MERGE");

    let id_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int32Array>()
        .unwrap();
    let value_col = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    // Verify updates
    assert_eq!(id_col.value(0), 1);
    assert_eq!(value_col.value(0), "gaming_laptop"); // updated

    assert_eq!(id_col.value(1), 2);
    assert_eq!(value_col.value(1), "phone"); // unchanged

    assert_eq!(id_col.value(2), 3);
    assert_eq!(value_col.value(2), "epic_novel"); // updated

    Ok(())
}

/// Test MERGE with compound join keys (two-column ON condition).
/// Verifies that multi-column join conditions work correctly for matching.
#[tokio::test]
async fn test_merge_compound_join_key_two_cols() -> Result<()> {
    use datafusion::datasource::MemTable;
    use datafusion::prelude::col as df_col;
    use iceberg_datafusion::merge::MergeStats;

    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_merge_compound_key".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    // Create schema: (id, region, value) - compound key on (id, region)
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "region", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "value", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;

    let creation = TableCreation::builder()
        .name("compound_key_table".to_string())
        .location(temp_path())
        .schema(schema)
        .properties(HashMap::new())
        .build();

    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert data - same id can exist in different regions
    // (1, 'us', 'laptop_us'), (1, 'eu', 'laptop_eu'), (2, 'us', 'phone_us')
    ctx.sql(
        "INSERT INTO catalog.test_merge_compound_key.compound_key_table VALUES \
         (1, 'us', 'laptop_us'), \
         (1, 'eu', 'laptop_eu'), \
         (2, 'us', 'phone_us')",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // Verify initial row count
    let df = ctx
        .sql("SELECT COUNT(*) as cnt FROM catalog.test_merge_compound_key.compound_key_table")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 3, "Should have 3 rows initially");

    // Create source DataFrame for MERGE with compound key:
    // - (1, 'us', 'laptop_us_updated'): matches (1, 'us') -> UPDATE
    // - (3, 'us', 'tablet_us'): new compound key -> INSERT
    // Note: (1, 'eu') and (2, 'us') are NOT_MATCHED_BY_SOURCE
    let source_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
        datafusion::arrow::datatypes::Field::new(
            "id",
            datafusion::arrow::datatypes::DataType::Int32,
            false,
        ),
        datafusion::arrow::datatypes::Field::new(
            "region",
            datafusion::arrow::datatypes::DataType::Utf8,
            false,
        ),
        datafusion::arrow::datatypes::Field::new(
            "value",
            datafusion::arrow::datatypes::DataType::Utf8,
            false,
        ),
    ]));

    let id_array = Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 3]))
        as datafusion::arrow::array::ArrayRef;
    let region_array =
        Arc::new(StringArray::from(vec!["us", "us"])) as datafusion::arrow::array::ArrayRef;
    let value_array = Arc::new(StringArray::from(vec!["laptop_us_updated", "tablet_us"]))
        as datafusion::arrow::array::ArrayRef;
    let source_batch = datafusion::arrow::array::RecordBatch::try_new(source_schema.clone(), vec![
        id_array,
        region_array,
        value_array,
    ])
    .unwrap();

    let source_table =
        Arc::new(MemTable::try_new(source_schema, vec![vec![source_batch]]).unwrap());
    ctx.register_table("compound_source", source_table).unwrap();
    let source_df = ctx.table("compound_source").await.unwrap();

    // Create provider for programmatic merge
    let provider =
        IcebergTableProvider::try_new(client.clone(), namespace.clone(), "compound_key_table")
            .await?;

    // Execute MERGE with compound key: ON target.id = source.id AND target.region = source.region
    let stats: MergeStats = provider
        .merge(source_df)
        .await
        .unwrap()
        .on(df_col("target.id")
            .eq(df_col("source.id"))
            .and(df_col("target.region").eq(df_col("source.region"))))
        .when_matched(None)
        .update(vec![("value", df_col("source_value"))])
        .when_not_matched(None)
        .insert_all()
        .when_not_matched_by_source(None)
        .delete()
        .execute(&ctx.state())
        .await
        .unwrap();

    // Expected classification with compound key:
    // - (1, 'us'): MATCHED -> UPDATE
    // - (1, 'eu'): NOT_MATCHED_BY_SOURCE -> DELETE
    // - (2, 'us'): NOT_MATCHED_BY_SOURCE -> DELETE
    // - (3, 'us'): NOT_MATCHED -> INSERT
    assert_eq!(
        stats.rows_updated, 1,
        "Expected 1 row updated (id=1, region=us)"
    );
    assert_eq!(
        stats.rows_inserted, 1,
        "Expected 1 row inserted (id=3, region=us)"
    );
    assert_eq!(stats.rows_deleted, 2, "Expected 2 rows deleted");

    // Reload and verify final data
    let fresh_provider =
        IcebergTableProvider::try_new(client.clone(), namespace.clone(), "compound_key_table")
            .await?;

    ctx.deregister_table("fresh_compound").ok();
    ctx.register_table("fresh_compound", Arc::new(fresh_provider))
        .unwrap();

    let result = ctx
        .sql("SELECT id, region, value FROM fresh_compound ORDER BY id, region")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(result.len(), 1, "Should have one batch");
    let batch = &result[0];
    assert_eq!(batch.num_rows(), 2, "Should have 2 rows after MERGE");

    let id_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int32Array>()
        .unwrap();
    let region_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let value_col = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    // Row 1: (1, 'us', 'laptop_us_updated')
    assert_eq!(id_col.value(0), 1);
    assert_eq!(region_col.value(0), "us");
    assert_eq!(value_col.value(0), "laptop_us_updated");

    // Row 2: (3, 'us', 'tablet_us')
    assert_eq!(id_col.value(1), 3);
    assert_eq!(region_col.value(1), "us");
    assert_eq!(value_col.value(1), "tablet_us");

    Ok(())
}

/// Test MERGE with compound join key - simple UPDATE only scenario.
/// Verifies that compound keys correctly match rows for updates.
#[tokio::test]
async fn test_merge_compound_key_update_only() -> Result<()> {
    use datafusion::datasource::MemTable;
    use datafusion::prelude::col as df_col;
    use iceberg_datafusion::merge::MergeStats;

    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_merge_compound_upd".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    // Create schema: (id, region, value)
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "region", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "value", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;

    let creation = TableCreation::builder()
        .name("compound_upd_table".to_string())
        .location(temp_path())
        .schema(schema)
        .properties(HashMap::new())
        .build();

    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert data
    ctx.sql(
        "INSERT INTO catalog.test_merge_compound_upd.compound_upd_table VALUES \
         (1, 'us', 'old_us'), \
         (1, 'eu', 'old_eu'), \
         (2, 'us', 'old_us2')",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // Create source: update values for (1, 'us') and (2, 'us')
    let source_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
        datafusion::arrow::datatypes::Field::new(
            "id",
            datafusion::arrow::datatypes::DataType::Int32,
            false,
        ),
        datafusion::arrow::datatypes::Field::new(
            "region",
            datafusion::arrow::datatypes::DataType::Utf8,
            false,
        ),
        datafusion::arrow::datatypes::Field::new(
            "value",
            datafusion::arrow::datatypes::DataType::Utf8,
            false,
        ),
    ]));

    let id_array = Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2]))
        as datafusion::arrow::array::ArrayRef;
    let region_array =
        Arc::new(StringArray::from(vec!["us", "us"])) as datafusion::arrow::array::ArrayRef;
    let value_array = Arc::new(StringArray::from(vec!["new_us", "new_us2"]))
        as datafusion::arrow::array::ArrayRef;
    let source_batch = datafusion::arrow::array::RecordBatch::try_new(source_schema.clone(), vec![
        id_array,
        region_array,
        value_array,
    ])
    .unwrap();

    let source_table =
        Arc::new(MemTable::try_new(source_schema, vec![vec![source_batch]]).unwrap());
    ctx.register_table("upd_compound_source", source_table)
        .unwrap();
    let source_df = ctx.table("upd_compound_source").await.unwrap();

    let provider =
        IcebergTableProvider::try_new(client.clone(), namespace.clone(), "compound_upd_table")
            .await?;

    // Execute MERGE with only WHEN MATCHED UPDATE
    let stats: MergeStats = provider
        .merge(source_df)
        .await
        .unwrap()
        .on(df_col("target.id")
            .eq(df_col("source.id"))
            .and(df_col("target.region").eq(df_col("source.region"))))
        .when_matched(None)
        .update(vec![("value", df_col("source_value"))])
        .execute(&ctx.state())
        .await
        .unwrap();

    // Should update 2 rows (both us region rows matched)
    // (1, 'eu') does not match because region differs
    assert_eq!(stats.rows_updated, 2, "Expected 2 rows updated");
    assert_eq!(stats.rows_inserted, 0, "Expected 0 rows inserted");
    assert_eq!(stats.rows_deleted, 0, "Expected 0 rows deleted");

    // Verify data
    let fresh_provider =
        IcebergTableProvider::try_new(client.clone(), namespace.clone(), "compound_upd_table")
            .await?;

    ctx.deregister_table("fresh_upd").ok();
    ctx.register_table("fresh_upd", Arc::new(fresh_provider))
        .unwrap();

    let result = ctx
        .sql("SELECT id, region, value FROM fresh_upd ORDER BY id, region")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(result.len(), 1);
    let batch = &result[0];
    assert_eq!(batch.num_rows(), 3, "Should still have 3 rows");

    let id_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int32Array>()
        .unwrap();
    let region_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let value_col = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    // (1, 'eu') unchanged
    assert_eq!(id_col.value(0), 1);
    assert_eq!(region_col.value(0), "eu");
    assert_eq!(value_col.value(0), "old_eu");

    // (1, 'us') updated
    assert_eq!(id_col.value(1), 1);
    assert_eq!(region_col.value(1), "us");
    assert_eq!(value_col.value(1), "new_us");

    // (2, 'us') updated
    assert_eq!(id_col.value(2), 2);
    assert_eq!(region_col.value(2), "us");
    assert_eq!(value_col.value(2), "new_us2");

    Ok(())
}

/// Tests that Dynamic Partition Pruning (DPP) is applied when source data
/// touches only a single partition out of many.
#[tokio::test]
async fn test_merge_dpp_single_partition() -> Result<()> {
    use datafusion::datasource::MemTable;
    use datafusion::prelude::col as df_col;
    use iceberg_datafusion::merge::MergeStats;

    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_merge_dpp_single".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    // Create schema with partition column: (id: Int, region: String, value: String)
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "region", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "value", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;

    let partition_spec = UnboundPartitionSpec::builder()
        .with_spec_id(0)
        .add_partition_field(2, "region", Transform::Identity)?
        .build();

    let creation = TableCreation::builder()
        .name("dpp_single_part_table".to_string())
        .location(temp_path())
        .schema(schema)
        .partition_spec(partition_spec)
        .properties(HashMap::new())
        .build();

    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert data across 3 partitions: us, eu, asia
    ctx.sql(
        "INSERT INTO catalog.test_merge_dpp_single.dpp_single_part_table VALUES \
         (1, 'us', 'val1'), \
         (2, 'us', 'val2'), \
         (3, 'eu', 'val3'), \
         (4, 'eu', 'val4'), \
         (5, 'asia', 'val5'), \
         (6, 'asia', 'val6')",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // Create source that only touches 'us' partition (1 out of 3 partitions)
    let source_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
        datafusion::arrow::datatypes::Field::new(
            "id",
            datafusion::arrow::datatypes::DataType::Int32,
            false,
        ),
        datafusion::arrow::datatypes::Field::new(
            "region",
            datafusion::arrow::datatypes::DataType::Utf8,
            false,
        ),
        datafusion::arrow::datatypes::Field::new(
            "value",
            datafusion::arrow::datatypes::DataType::Utf8,
            false,
        ),
    ]));

    let id_array = Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 7]))
        as datafusion::arrow::array::ArrayRef;
    let region_array =
        Arc::new(StringArray::from(vec!["us", "us"])) as datafusion::arrow::array::ArrayRef;
    let value_array = Arc::new(StringArray::from(vec!["updated_val1", "new_val7"]))
        as datafusion::arrow::array::ArrayRef;
    let source_batch = datafusion::arrow::array::RecordBatch::try_new(source_schema.clone(), vec![
        id_array,
        region_array,
        value_array,
    ])
    .unwrap();

    let source_table =
        Arc::new(MemTable::try_new(source_schema, vec![vec![source_batch]]).unwrap());
    ctx.register_table("dpp_source_single", source_table)
        .unwrap();
    let source_df = ctx.table("dpp_source_single").await.unwrap();

    // Create provider and execute MERGE with join on partition column
    let provider =
        IcebergTableProvider::try_new(client.clone(), namespace.clone(), "dpp_single_part_table")
            .await?;

    let stats: MergeStats = provider
        .merge(source_df)
        .await
        .unwrap()
        .on(df_col("target.region")
            .eq(df_col("source.region"))
            .and(df_col("target.id").eq(df_col("source.id"))))
        .when_matched(None)
        .update(vec![("value", df_col("source_value"))])
        .when_not_matched(None)
        .insert_all()
        .execute(&ctx.state())
        .await
        .unwrap();

    // DPP should be applied since join includes partition column 'region'
    // and source only has 1 distinct region value ('us')
    assert!(
        stats.dpp_applied,
        "DPP should be applied when source touches single partition"
    );
    assert_eq!(
        stats.dpp_partition_count, 1,
        "Should have pruned to 1 partition"
    );
    assert_eq!(stats.rows_updated, 1, "Expected 1 row updated (id=1)");
    assert_eq!(stats.rows_inserted, 1, "Expected 1 row inserted (id=7)");

    Ok(())
}

/// Tests that DPP is disabled when source data has high cardinality
/// (exceeds dpp_max_partitions threshold).
#[tokio::test]
async fn test_merge_dpp_disabled_high_cardinality() -> Result<()> {
    use datafusion::datasource::MemTable;
    use datafusion::prelude::col as df_col;
    use iceberg_datafusion::merge::MergeStats;

    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_merge_dpp_high_card".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    // Create schema with partition column
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "category", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "value", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;

    let partition_spec = UnboundPartitionSpec::builder()
        .with_spec_id(0)
        .add_partition_field(2, "category", Transform::Identity)?
        .build();

    let creation = TableCreation::builder()
        .name("dpp_high_card_table".to_string())
        .location(temp_path())
        .schema(schema)
        .partition_spec(partition_spec)
        .properties(HashMap::new())
        .build();

    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert data across 2 partitions initially
    ctx.sql(
        "INSERT INTO catalog.test_merge_dpp_high_card.dpp_high_card_table VALUES \
         (1, 'cat_a', 'val1'), \
         (2, 'cat_b', 'val2')",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // Create source with 1001+ distinct partition values to exceed default threshold
    // Note: Default DPP_DEFAULT_MAX_PARTITIONS = 1000
    let mut id_values = Vec::new();
    let mut category_values = Vec::new();
    let mut value_values = Vec::new();
    for i in 0..1010 {
        id_values.push(i + 100);
        category_values.push(format!("cat_{}", i));
        value_values.push(format!("new_val_{}", i));
    }

    let source_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
        datafusion::arrow::datatypes::Field::new(
            "id",
            datafusion::arrow::datatypes::DataType::Int32,
            false,
        ),
        datafusion::arrow::datatypes::Field::new(
            "category",
            datafusion::arrow::datatypes::DataType::Utf8,
            false,
        ),
        datafusion::arrow::datatypes::Field::new(
            "value",
            datafusion::arrow::datatypes::DataType::Utf8,
            false,
        ),
    ]));

    let id_array = Arc::new(datafusion::arrow::array::Int32Array::from(id_values))
        as datafusion::arrow::array::ArrayRef;
    let category_array =
        Arc::new(StringArray::from(category_values)) as datafusion::arrow::array::ArrayRef;
    let value_array =
        Arc::new(StringArray::from(value_values)) as datafusion::arrow::array::ArrayRef;
    let source_batch = datafusion::arrow::array::RecordBatch::try_new(source_schema.clone(), vec![
        id_array,
        category_array,
        value_array,
    ])
    .unwrap();

    let source_table =
        Arc::new(MemTable::try_new(source_schema, vec![vec![source_batch]]).unwrap());
    ctx.register_table("dpp_source_high_card", source_table)
        .unwrap();
    let source_df = ctx.table("dpp_source_high_card").await.unwrap();

    let provider =
        IcebergTableProvider::try_new(client.clone(), namespace.clone(), "dpp_high_card_table")
            .await?;

    // Execute MERGE - join includes partition column but source has too many distinct values
    let stats: MergeStats = provider
        .merge(source_df)
        .await
        .unwrap()
        .on(df_col("target.category")
            .eq(df_col("source.category"))
            .and(df_col("target.id").eq(df_col("source.id"))))
        .when_matched(None)
        .update(vec![("value", df_col("source_value"))])
        .when_not_matched(None)
        .insert_all()
        .execute(&ctx.state())
        .await
        .unwrap();

    // DPP should NOT be applied due to high cardinality (>1000 distinct values)
    // Note: partition_count still reports the extracted count even when DPP is disabled
    assert!(
        !stats.dpp_applied,
        "DPP should be disabled when partition count exceeds threshold"
    );
    assert!(
        stats.dpp_partition_count > 1000,
        "Should report high partition count that triggered threshold"
    );

    Ok(())
}

/// Tests that DPP is not applied when the join key does not include a partition column.
#[tokio::test]
async fn test_merge_dpp_non_partition_join_key() -> Result<()> {
    use datafusion::datasource::MemTable;
    use datafusion::prelude::col as df_col;
    use iceberg_datafusion::merge::MergeStats;

    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_merge_dpp_non_part".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    // Create schema partitioned by 'region', but we'll join on 'id' (non-partition column)
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "region", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "value", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;

    let partition_spec = UnboundPartitionSpec::builder()
        .with_spec_id(0)
        .add_partition_field(2, "region", Transform::Identity)?
        .build();

    let creation = TableCreation::builder()
        .name("dpp_non_part_join_table".to_string())
        .location(temp_path())
        .schema(schema)
        .partition_spec(partition_spec)
        .properties(HashMap::new())
        .build();

    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert data across multiple partitions
    ctx.sql(
        "INSERT INTO catalog.test_merge_dpp_non_part.dpp_non_part_join_table VALUES \
         (1, 'us', 'val1'), \
         (2, 'eu', 'val2'), \
         (3, 'asia', 'val3')",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // Create source - doesn't include partition column, just id
    let source_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
        datafusion::arrow::datatypes::Field::new(
            "id",
            datafusion::arrow::datatypes::DataType::Int32,
            false,
        ),
        datafusion::arrow::datatypes::Field::new(
            "value",
            datafusion::arrow::datatypes::DataType::Utf8,
            false,
        ),
    ]));

    let id_array = Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 4]))
        as datafusion::arrow::array::ArrayRef;
    let value_array = Arc::new(StringArray::from(vec!["updated_val1", "new_val4"]))
        as datafusion::arrow::array::ArrayRef;
    let source_batch = datafusion::arrow::array::RecordBatch::try_new(source_schema.clone(), vec![
        id_array,
        value_array,
    ])
    .unwrap();

    let source_table =
        Arc::new(MemTable::try_new(source_schema, vec![vec![source_batch]]).unwrap());
    ctx.register_table("dpp_source_non_part", source_table)
        .unwrap();
    let source_df = ctx.table("dpp_source_non_part").await.unwrap();

    let provider =
        IcebergTableProvider::try_new(client.clone(), namespace.clone(), "dpp_non_part_join_table")
            .await?;

    // Execute MERGE joining only on 'id' - NOT a partition column
    let stats: MergeStats = provider
        .merge(source_df)
        .await
        .unwrap()
        .on(df_col("target.id").eq(df_col("source.id")))
        .when_matched(None)
        .update(vec![("value", df_col("source_value"))])
        .when_not_matched(None)
        .insert(vec![
            ("id", df_col("source_id")),
            ("region", lit("unknown")),
            ("value", df_col("source_value")),
        ])
        .execute(&ctx.state())
        .await
        .unwrap();

    // DPP should NOT be applied since join key 'id' is not a partition column
    assert!(
        !stats.dpp_applied,
        "DPP should not be applied when join key is not a partition column"
    );
    assert_eq!(
        stats.dpp_partition_count, 0,
        "Should report 0 partitions when DPP not applied"
    );

    // Verify the merge still works correctly
    assert_eq!(stats.rows_updated, 1, "Expected 1 row updated (id=1)");
    assert_eq!(stats.rows_inserted, 1, "Expected 1 row inserted (id=4)");

    Ok(())
}

// ==================== INSERT OVERWRITE Tests ====================

/// Test INSERT OVERWRITE on an unpartitioned table.
/// This should replace all existing data with the new data.
#[tokio::test]
async fn test_insert_overwrite_unpartitioned() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_insert_overwrite_unpartitioned".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let creation = get_table_creation(temp_path(), "my_table", None)?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    // Use SQLite dialect to support INSERT OVERWRITE syntax
    let mut config = SessionConfig::new();
    config.options_mut().sql_parser.dialect = Dialect::SQLite;
    let ctx = SessionContext::new_with_config(config);
    ctx.register_catalog("catalog", catalog);

    // Insert initial data
    ctx.sql("INSERT INTO catalog.test_insert_overwrite_unpartitioned.my_table VALUES (1, 'alice'), (2, 'bob')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Verify initial data
    let batches = ctx
        .sql("SELECT * FROM catalog.test_insert_overwrite_unpartitioned.my_table ORDER BY foo1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);

    // INSERT OVERWRITE should replace all data
    let df = ctx
        .sql("INSERT OVERWRITE catalog.test_insert_overwrite_unpartitioned.my_table VALUES (3, 'charlie'), (4, 'diana'), (5, 'eve')")
        .await
        .unwrap();

    let overwrite_result = df.collect().await.unwrap();
    assert_eq!(overwrite_result.len(), 1);
    let rows_written = overwrite_result[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(rows_written.value(0), 3, "Should report 3 rows written");

    // Query the table to verify the data was replaced
    let df = ctx
        .sql("SELECT * FROM catalog.test_insert_overwrite_unpartitioned.my_table ORDER BY foo1")
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();

    check_record_batches(
        batches,
        expect![[r#"
            Field { "foo1": Int32, metadata: {"PARQUET:field_id": "1"} },
            Field { "foo2": Utf8, metadata: {"PARQUET:field_id": "2"} }"#]],
        expect![[r#"
            foo1: PrimitiveArray<Int32>
            [
              3,
              4,
              5,
            ],
            foo2: StringArray
            [
              "charlie",
              "diana",
              "eve",
            ]"#]],
        &[],
        Some("foo1"),
    );

    Ok(())
}

/// Test INSERT OVERWRITE on a partitioned table.
/// This should only replace data in the partitions touched by the new data.
#[tokio::test]
async fn test_insert_overwrite_partitioned() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_insert_overwrite_partitioned".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    // Create a partitioned table (partitioned by foo1)
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "foo1", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "foo2", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;

    let partition_spec = UnboundPartitionSpec::builder()
        .with_spec_id(0)
        .add_partition_field(1, "foo1", Transform::Identity)?
        .build();

    let creation = TableCreation::builder()
        .name("my_table".to_string())
        .location(temp_path())
        .schema(schema)
        .partition_spec(partition_spec)
        .properties(HashMap::new())
        .build();

    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    // Use SQLite dialect to support INSERT OVERWRITE syntax
    let mut config = SessionConfig::new();
    config.options_mut().sql_parser.dialect = Dialect::SQLite;
    let ctx = SessionContext::new_with_config(config);
    ctx.register_catalog("catalog", catalog);

    // Insert initial data across multiple partitions
    // Partition foo1=1: alice
    // Partition foo1=2: bob
    // Partition foo1=3: charlie
    ctx.sql("INSERT INTO catalog.test_insert_overwrite_partitioned.my_table VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Verify initial data
    let batches = ctx
        .sql("SELECT * FROM catalog.test_insert_overwrite_partitioned.my_table ORDER BY foo1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);

    // INSERT OVERWRITE with data only in partition foo1=2
    // This should replace partition 2 but leave partitions 1 and 3 intact
    let df = ctx
        .sql("INSERT OVERWRITE catalog.test_insert_overwrite_partitioned.my_table VALUES (2, 'betty')")
        .await
        .unwrap();

    let overwrite_result = df.collect().await.unwrap();
    let rows_written = overwrite_result[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(rows_written.value(0), 1, "Should report 1 row written");

    // Query the table - partitions 1 and 3 should still have original data,
    // partition 2 should have new data
    let df = ctx
        .sql("SELECT * FROM catalog.test_insert_overwrite_partitioned.my_table ORDER BY foo1")
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();

    check_record_batches(
        batches,
        expect![[r#"
            Field { "foo1": Int32, metadata: {"PARQUET:field_id": "1"} },
            Field { "foo2": Utf8, metadata: {"PARQUET:field_id": "2"} }"#]],
        expect![[r#"
            foo1: PrimitiveArray<Int32>
            [
              1,
              2,
              3,
            ],
            foo2: StringArray
            [
              "alice",
              "betty",
              "charlie",
            ]"#]],
        &[],
        Some("foo1"),
    );

    Ok(())
}

// ============================================================================
// Partition Evolution Tests
// ============================================================================

/// Test DELETE on a table with evolved partition spec.
/// Verifies that DELETE properly handles tables with multiple partition specs by:
/// 1. Creating an unpartitioned table
/// 2. Inserting data under spec v0 (unpartitioned)
/// 3. Evolving to partitioned
/// 4. Inserting data under spec v1 (partitioned)
/// 5. Deleting rows spanning both partition specs
/// 6. Verifying correct rows are deleted
///
/// This test validates the partition evolution support implemented in the transaction layer
/// (per-spec manifest writing) and DataFusion layer (per-file spec_id serialization).
#[tokio::test]
async fn test_delete_with_partition_evolution() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_delete_partition_evolution".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    // Create schema
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "category", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "year", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()?;

    // Initial partition spec: unpartitioned
    let partition_spec_v0 = UnboundPartitionSpec::builder().with_spec_id(0).build();

    let creation = TableCreation::builder()
        .name("partition_evolution_delete_table".to_string())
        .location(temp_path())
        .schema(schema)
        .partition_spec(partition_spec_v0)
        .properties(HashMap::new())
        .build();

    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert data under partition spec v0 (unpartitioned)
    ctx.sql(
        "INSERT INTO catalog.test_delete_partition_evolution.partition_evolution_delete_table VALUES \
         (1, 'electronics', 2023), \
         (2, 'electronics', 2024), \
         (3, 'books', 2023)",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // Evolve partition spec: add partitioning by category
    let table_ident = TableIdent::new(
        namespace.clone(),
        "partition_evolution_delete_table".to_string(),
    );
    let table = client.load_table(&table_ident).await?;

    let new_spec = UnboundPartitionSpec::builder()
        .add_partition_field(2, "category", Transform::Identity)?
        .build();

    let tx = Transaction::new(&table);
    let action = tx.evolve_partition_spec().add_spec(new_spec).set_default();
    let tx = action.apply(tx)?;
    let _table = tx.commit(client.as_ref()).await?;

    // Refresh catalog to see the evolved spec
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);
    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert data under partition spec v1 (partitioned by category)
    ctx.sql(
        "INSERT INTO catalog.test_delete_partition_evolution.partition_evolution_delete_table VALUES \
         (4, 'books', 2024), \
         (5, 'electronics', 2024)",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // Verify we have 5 rows total (3 under spec v0, 2 under spec v1)
    let df = ctx
        .sql("SELECT COUNT(*) as cnt FROM catalog.test_delete_partition_evolution.partition_evolution_delete_table")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 5, "Should have 5 rows before delete");

    // Create provider for programmatic delete
    let provider = IcebergTableProvider::try_new(
        client.clone(),
        namespace.clone(),
        "partition_evolution_delete_table",
    )
    .await?;

    // Delete rows where year = 2024 (spans both partition specs)
    let deleted_count = provider
        .delete(&ctx.state(), Some(col("year").eq(lit(2024))))
        .await
        .expect("DELETE across partition specs should succeed");

    assert_eq!(
        deleted_count, 3,
        "Should delete 3 rows (id 2, 4, 5 where year=2024)"
    );

    // Re-register with fresh catalog to see changes
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);
    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Verify total row count decreased
    let df = ctx
        .sql("SELECT COUNT(*) as cnt FROM catalog.test_delete_partition_evolution.partition_evolution_delete_table")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 2, "Should have 2 rows after delete");

    // Verify correct rows remain (only 2023 data)
    let df = ctx
        .sql("SELECT id, year FROM catalog.test_delete_partition_evolution.partition_evolution_delete_table ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let ids: Vec<i32> = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int32Array>()
                .unwrap()
                .iter()
                .flatten()
        })
        .collect();

    assert_eq!(
        ids,
        vec![1, 3],
        "Only rows with id 1 and 3 (year 2023) should remain"
    );

    Ok(())
}

/// Test UPDATE on a table with evolved partition spec.
/// Verifies that UPDATE properly handles tables with multiple partition specs.
///
/// This test validates the partition evolution support implemented in the transaction layer
/// (per-spec manifest writing) and DataFusion layer (per-file spec_id serialization).
#[tokio::test]
async fn test_update_with_partition_evolution() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_update_partition_evolution".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    // Create schema
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "category", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "status", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;

    // Initial partition spec: unpartitioned
    let partition_spec_v0 = UnboundPartitionSpec::builder().with_spec_id(0).build();

    let creation = TableCreation::builder()
        .name("partition_evolution_update_table".to_string())
        .location(temp_path())
        .schema(schema)
        .partition_spec(partition_spec_v0)
        .properties(HashMap::new())
        .build();

    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert data under partition spec v0 (unpartitioned)
    ctx.sql(
        "INSERT INTO catalog.test_update_partition_evolution.partition_evolution_update_table VALUES \
         (1, 'A', 'active'), \
         (2, 'A', 'active'), \
         (3, 'B', 'active')",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // Evolve partition spec: add partitioning by category
    let table_ident = TableIdent::new(
        namespace.clone(),
        "partition_evolution_update_table".to_string(),
    );
    let table = client.load_table(&table_ident).await?;

    let new_spec = UnboundPartitionSpec::builder()
        .add_partition_field(2, "category", Transform::Identity)?
        .build();

    let tx = Transaction::new(&table);
    let action = tx.evolve_partition_spec().add_spec(new_spec).set_default();
    let tx = action.apply(tx)?;
    let _table = tx.commit(client.as_ref()).await?;

    // Refresh and insert under new spec (partitioned)
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);
    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    ctx.sql(
        "INSERT INTO catalog.test_update_partition_evolution.partition_evolution_update_table VALUES \
         (4, 'C', 'active'), \
         (5, 'C', 'active')",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // Update rows across partition specs using SQL
    let result = ctx
        .sql("UPDATE catalog.test_update_partition_evolution.partition_evolution_update_table SET status = 'inactive' WHERE id > 2")
        .await
        .expect("UPDATE across partition specs should succeed")
        .collect()
        .await
        .unwrap();

    // Verify result count
    let updated_count: u64 = result
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .iter()
                .flatten()
        })
        .sum();
    assert_eq!(updated_count, 3, "Should update 3 rows (id 3, 4, 5)");

    // Verify updates
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);
    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    let df = ctx
        .sql("SELECT id, status FROM catalog.test_update_partition_evolution.partition_evolution_update_table ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let statuses: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            b.column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .flatten()
                .map(|s| s.to_string())
        })
        .collect();

    assert_eq!(
        statuses,
        vec!["active", "active", "inactive", "inactive", "inactive"],
        "First two rows active, last three inactive"
    );

    Ok(())
}

/// Test MERGE on a table with evolved partition spec.
/// Verifies that MERGE properly handles tables with multiple partition specs.
///
/// This test validates the partition evolution support implemented in the transaction layer
/// (per-spec manifest writing) and DataFusion layer (per-file spec_id serialization).
#[tokio::test]
async fn test_merge_with_partition_evolution() -> Result<()> {
    use datafusion::arrow::array::{Int32Array as ArrowInt32Array, RecordBatch};
    use datafusion::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };
    use datafusion::datasource::MemTable;
    use datafusion::prelude::col as df_col;

    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_merge_partition_evolution".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    // Create schema
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "region", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "value", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()?;

    // Initial partition spec: unpartitioned
    let partition_spec_v0 = UnboundPartitionSpec::builder().with_spec_id(0).build();

    let creation = TableCreation::builder()
        .name("partition_evolution_merge_table".to_string())
        .location(temp_path())
        .schema(schema)
        .partition_spec(partition_spec_v0)
        .properties(HashMap::new())
        .build();

    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert data under partition spec v0 (unpartitioned)
    ctx.sql(
        "INSERT INTO catalog.test_merge_partition_evolution.partition_evolution_merge_table VALUES \
         (1, 'US', 100), \
         (2, 'US', 200), \
         (3, 'EU', 300)",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // Evolve partition spec: add partitioning by region
    let table_ident = TableIdent::new(
        namespace.clone(),
        "partition_evolution_merge_table".to_string(),
    );
    let table = client.load_table(&table_ident).await?;

    let new_spec = UnboundPartitionSpec::builder()
        .add_partition_field(2, "region", Transform::Identity)?
        .build();

    let tx = Transaction::new(&table);
    let action = tx.evolve_partition_spec().add_spec(new_spec).set_default();
    let tx = action.apply(tx)?;
    let _table = tx.commit(client.as_ref()).await?;

    // Refresh and insert under new spec (partitioned by region)
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);
    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    ctx.sql(
        "INSERT INTO catalog.test_merge_partition_evolution.partition_evolution_merge_table VALUES \
         (4, 'APAC', 400), \
         (5, 'APAC', 500)",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // Create source DataFrame for programmatic MERGE
    let source_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("region", ArrowDataType::Utf8, false),
        ArrowField::new("value", ArrowDataType::Int32, false),
    ]));

    let id_array = Arc::new(ArrowInt32Array::from(vec![1, 3, 6]));
    let region_array = Arc::new(StringArray::from(vec!["US", "EU", "EU"]));
    let value_array = Arc::new(ArrowInt32Array::from(vec![150, 350, 600]));

    let source_batch = RecordBatch::try_new(source_schema.clone(), vec![
        id_array,
        region_array,
        value_array,
    ])
    .unwrap();

    let source_table =
        Arc::new(MemTable::try_new(source_schema, vec![vec![source_batch]]).unwrap());
    ctx.register_table("merge_source_data", source_table)
        .unwrap();
    let source_df = ctx.table("merge_source_data").await.unwrap();

    // Create provider for programmatic merge
    let provider = IcebergTableProvider::try_new(
        client.clone(),
        namespace.clone(),
        "partition_evolution_merge_table",
    )
    .await?;

    // Execute MERGE across partition specs using programmatic API
    // Note: After evolving to partition by region, we can't use update_all() because
    // that would try to update the partition column. Use explicit column update instead.
    let stats = provider
        .merge(source_df)
        .await
        .unwrap()
        .on(df_col("target.id").eq(df_col("source.id")))
        .when_matched(None)
        .update(vec![("value", df_col("source_value"))])
        .when_not_matched(None)
        .insert_all()
        .execute(&ctx.state())
        .await
        .expect("MERGE across partition specs should succeed");

    assert_eq!(stats.rows_inserted, 1, "Should insert 1 row (id=6)");
    assert_eq!(stats.rows_updated, 2, "Should update 2 rows (id=1, id=3)");

    // Verify final state
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);
    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    let df = ctx
        .sql("SELECT id, value FROM catalog.test_merge_partition_evolution.partition_evolution_merge_table ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let values: Vec<i32> = batches
        .iter()
        .flat_map(|b| {
            b.column(1)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int32Array>()
                .unwrap()
                .iter()
                .flatten()
        })
        .collect();

    // id=1: updated to 150, id=2: unchanged 200, id=3: updated to 350,
    // id=4: unchanged 400, id=5: unchanged 500, id=6: inserted 600
    assert_eq!(values, vec![150, 200, 350, 400, 500, 600]);

    Ok(())
}

/// Test that position delete files inherit the partition spec of their source data file.
///
/// This validates the "Migrate Forward" semantic: when deleting rows from a data file
/// that was written under an old partition spec, the position delete file must use
/// that same partition spec, NOT the table's current default spec.
#[tokio::test]
async fn test_delete_file_inherits_source_spec() -> Result<()> {
    use iceberg::spec::ManifestContentType;

    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_delete_inherits_spec".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    // Create schema
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "category", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;

    // Initial partition spec: unpartitioned (spec_id = 0)
    let partition_spec_v0 = UnboundPartitionSpec::builder().with_spec_id(0).build();

    let creation = TableCreation::builder()
        .name("delete_inherits_spec_table".to_string())
        .location(temp_path())
        .schema(schema)
        .partition_spec(partition_spec_v0)
        .properties(HashMap::new())
        .build();

    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert data under partition spec v0 (unpartitioned)
    ctx.sql(
        "INSERT INTO catalog.test_delete_inherits_spec.delete_inherits_spec_table VALUES \
         (1, 'electronics'), \
         (2, 'books')",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // Evolve partition spec: add partitioning by category (spec_id = 1)
    let table_ident = TableIdent::new(namespace.clone(), "delete_inherits_spec_table".to_string());
    let table = client.load_table(&table_ident).await?;

    let new_spec = UnboundPartitionSpec::builder()
        .add_partition_field(2, "category", Transform::Identity)?
        .build();

    let tx = Transaction::new(&table);
    let action = tx.evolve_partition_spec().add_spec(new_spec).set_default();
    let tx = action.apply(tx)?;
    let _table = tx.commit(client.as_ref()).await?;

    // Verify default spec is now 1
    let table = client.load_table(&table_ident).await?;
    assert_eq!(
        table.metadata().default_partition_spec_id(),
        1,
        "Default spec should now be 1"
    );

    // Refresh catalog
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);
    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Create provider for programmatic delete
    let provider = IcebergTableProvider::try_new(
        client.clone(),
        namespace.clone(),
        "delete_inherits_spec_table",
    )
    .await?;

    // Delete a row from spec v0 data file (id = 1)
    // The position delete file should have spec_id = 0, not 1
    let _deleted_count = provider
        .delete(&ctx.state(), Some(col("id").eq(lit(1))))
        .await
        .expect("DELETE should succeed");

    // Load table and inspect manifests
    let table = client.load_table(&table_ident).await?;
    let snapshot = table.metadata().current_snapshot().unwrap();

    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await?;

    // Find delete manifests and verify their spec_id
    let delete_manifests: Vec<_> = manifest_list
        .entries()
        .iter()
        .filter(|m| m.content == ManifestContentType::Deletes)
        .collect();

    assert!(
        !delete_manifests.is_empty(),
        "Should have at least one delete manifest"
    );

    // All delete manifests should have partition_spec_id = 0 (inherited from source data file)
    for manifest in &delete_manifests {
        assert_eq!(
            manifest.partition_spec_id, 0,
            "Delete manifest should inherit spec_id 0 from source data file, not default spec 1"
        );
    }

    Ok(())
}

/// Test that new data files from UPDATE use the table's current default spec,
/// while position delete files inherit the source file's spec.
///
/// This validates the full "Migrate Forward" semantic for UPDATE operations:
/// - Position deletes: use the spec of the data file being deleted
/// - New data files: use the table's current default spec
#[tokio::test]
async fn test_update_writes_with_migrate_forward_semantic() -> Result<()> {
    use iceberg::spec::ManifestContentType;

    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_update_migrate_forward".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    // Create schema
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "category", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "value", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()?;

    // Initial partition spec: unpartitioned (spec_id = 0)
    let partition_spec_v0 = UnboundPartitionSpec::builder().with_spec_id(0).build();

    let creation = TableCreation::builder()
        .name("update_migrate_forward_table".to_string())
        .location(temp_path())
        .schema(schema)
        .partition_spec(partition_spec_v0)
        .properties(HashMap::new())
        .build();

    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert data under partition spec v0 (unpartitioned)
    ctx.sql(
        "INSERT INTO catalog.test_update_migrate_forward.update_migrate_forward_table VALUES \
         (1, 'electronics', 100), \
         (2, 'books', 200)",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // Evolve partition spec: add partitioning by category (spec_id = 1)
    let table_ident = TableIdent::new(
        namespace.clone(),
        "update_migrate_forward_table".to_string(),
    );
    let table = client.load_table(&table_ident).await?;

    let new_spec = UnboundPartitionSpec::builder()
        .add_partition_field(2, "category", Transform::Identity)?
        .build();

    let tx = Transaction::new(&table);
    let action = tx.evolve_partition_spec().add_spec(new_spec).set_default();
    let tx = action.apply(tx)?;
    let _table = tx.commit(client.as_ref()).await?;

    // Verify default spec is now 1
    let table = client.load_table(&table_ident).await?;
    assert_eq!(
        table.metadata().default_partition_spec_id(),
        1,
        "Default spec should now be 1"
    );

    // Refresh catalog
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);
    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Create provider for programmatic update
    let provider = IcebergTableProvider::try_new(
        client.clone(),
        namespace.clone(),
        "update_migrate_forward_table",
    )
    .await?;

    // Update a row from spec v0 data file
    // This should:
    // - Create a position delete file with spec_id = 0 (inherited)
    // - Create a new data file with spec_id = 1 (current default)
    let _update_result = provider
        .update()
        .await
        .unwrap()
        .set("value", lit(999))
        .filter(col("id").eq(lit(1)))
        .execute(&ctx.state())
        .await
        .expect("UPDATE should succeed");

    // Load table and inspect manifests from the UPDATE snapshot
    let table = client.load_table(&table_ident).await?;
    let snapshot = table.metadata().current_snapshot().unwrap();

    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await?;

    // Separate data and delete manifests from THIS snapshot
    let mut data_manifests = vec![];
    let mut delete_manifests = vec![];

    for manifest in manifest_list.entries() {
        // Only look at manifests added by the UPDATE operation (snapshot_id matches)
        if manifest.added_snapshot_id == snapshot.snapshot_id() {
            match manifest.content {
                ManifestContentType::Data => data_manifests.push(manifest),
                ManifestContentType::Deletes => delete_manifests.push(manifest),
            }
        }
    }

    // Verify delete manifests have spec_id = 0 (inherited from source)
    assert!(
        !delete_manifests.is_empty(),
        "UPDATE should create delete manifests"
    );
    for manifest in &delete_manifests {
        assert_eq!(
            manifest.partition_spec_id, 0,
            "Delete manifest should have spec_id 0 (inherited from source data file)"
        );
    }

    // Verify new data manifests have spec_id = 1 (current default)
    assert!(
        !data_manifests.is_empty(),
        "UPDATE should create new data manifests"
    );
    for manifest in &data_manifests {
        assert_eq!(
            manifest.partition_spec_id, 1,
            "New data manifest should have spec_id 1 (current default spec)"
        );
    }

    // Verify the data is correct
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);
    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    let df = ctx
        .sql("SELECT id, value FROM catalog.test_update_migrate_forward.update_migrate_forward_table ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let values: Vec<i32> = batches
        .iter()
        .flat_map(|b| {
            b.column(1)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int32Array>()
                .unwrap()
                .iter()
                .flatten()
        })
        .collect();

    // id=1: updated to 999, id=2: unchanged 200
    assert_eq!(
        values,
        vec![999, 200],
        "UPDATE should change id=1's value to 999"
    );

    Ok(())
}

/// Test DELETE on a table that evolved from partitioned to unpartitioned (de-partition).
///
/// This tests the specific code path where `default_spec.is_unpartitioned()` is true
/// but historical data files still have partition values. The delete writer must
/// preserve the original partition spec (v0) for position delete files targeting
/// those historical files.
///
/// Evolution: partitioned by category (v0) → unpartitioned (v1)
#[tokio::test]
async fn test_delete_with_departition_evolution() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_delete_departition".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    // Create schema
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "category", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "value", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()?;

    // Initial partition spec: partitioned by category (spec_id = 0)
    let partition_spec_v0 = UnboundPartitionSpec::builder()
        .with_spec_id(0)
        .add_partition_field(2, "category", Transform::Identity)?
        .build();

    let creation = TableCreation::builder()
        .name("departition_delete_table".to_string())
        .location(temp_path())
        .schema(schema)
        .partition_spec(partition_spec_v0)
        .properties(HashMap::new())
        .build();

    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert data under partition spec v0 (partitioned by category)
    ctx.sql(
        "INSERT INTO catalog.test_delete_departition.departition_delete_table VALUES \
         (1, 'electronics', 100), \
         (2, 'electronics', 200), \
         (3, 'books', 300)",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // Evolve partition spec: change to unpartitioned (spec_id = 1)
    // This is a "de-partition" evolution
    let table_ident = TableIdent::new(namespace.clone(), "departition_delete_table".to_string());
    let table = client.load_table(&table_ident).await?;

    let unpartitioned_spec = UnboundPartitionSpec::builder().build();

    let tx = Transaction::new(&table);
    let action = tx
        .evolve_partition_spec()
        .add_spec(unpartitioned_spec)
        .set_default();
    let tx = action.apply(tx)?;
    let _table = tx.commit(client.as_ref()).await?;

    // Refresh catalog to see the evolved spec
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);
    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert data under partition spec v1 (unpartitioned)
    ctx.sql(
        "INSERT INTO catalog.test_delete_departition.departition_delete_table VALUES \
         (4, 'toys', 400), \
         (5, 'books', 500)",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // Verify we have 5 rows total
    let df = ctx
        .sql("SELECT COUNT(*) as cnt FROM catalog.test_delete_departition.departition_delete_table")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 5, "Should have 5 rows before delete");

    // Create provider for programmatic delete
    let provider = IcebergTableProvider::try_new(
        client.clone(),
        namespace.clone(),
        "departition_delete_table",
    )
    .await?;

    // Delete rows from the PARTITIONED spec (v0) - specifically target electronics category
    // This exercises the path where default spec is unpartitioned but we're deleting
    // from files that have partition values
    let deleted_count = provider
        .delete(&ctx.state(), Some(col("category").eq(lit("electronics"))))
        .await
        .expect("DELETE on de-partitioned table should succeed");

    assert_eq!(
        deleted_count, 2,
        "Should delete 2 rows (id 1, 2 where category='electronics')"
    );

    // Re-register with fresh catalog to see changes
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);
    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Verify total row count decreased
    let df = ctx
        .sql("SELECT COUNT(*) as cnt FROM catalog.test_delete_departition.departition_delete_table")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 3, "Should have 3 rows after delete");

    // Verify correct rows remain
    let df = ctx
        .sql("SELECT id, category FROM catalog.test_delete_departition.departition_delete_table ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let ids: Vec<i32> = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int32Array>()
                .unwrap()
                .iter()
                .flatten()
        })
        .collect();

    assert_eq!(
        ids,
        vec![3, 4, 5],
        "Only rows with id 3, 4, 5 (non-electronics) should remain"
    );

    // Verify manifest structure: delete manifest should have spec_id = 0
    // (inheriting from the partitioned data files being deleted)
    let table = client.load_table(&table_ident).await?;
    let snapshot = table.metadata().current_snapshot().unwrap();
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await?;

    let delete_manifests: Vec<_> = manifest_list
        .entries()
        .iter()
        .filter(|m| m.content == iceberg::spec::ManifestContentType::Deletes)
        .collect();

    assert!(!delete_manifests.is_empty(), "Should have delete manifests");

    // Delete manifest should have spec_id = 0 (the original partitioned spec)
    // because the position deletes target files written under spec v0
    for manifest in &delete_manifests {
        assert_eq!(
            manifest.partition_spec_id, 0,
            "Delete manifest should have spec_id 0 (original partitioned spec)"
        );
    }

    Ok(())
}

/// Test UPDATE on a table that evolved from partitioned to unpartitioned (de-partition).
///
/// Similar to test_delete_with_departition_evolution but for UPDATE operations.
/// Verifies that the delete portion of UPDATE preserves the original partition spec.
#[tokio::test]
async fn test_update_with_departition_evolution() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_update_departition".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    // Create schema
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "category", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "value", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()?;

    // Initial partition spec: partitioned by category (spec_id = 0)
    let partition_spec_v0 = UnboundPartitionSpec::builder()
        .with_spec_id(0)
        .add_partition_field(2, "category", Transform::Identity)?
        .build();

    let creation = TableCreation::builder()
        .name("departition_update_table".to_string())
        .location(temp_path())
        .schema(schema)
        .partition_spec(partition_spec_v0)
        .properties(HashMap::new())
        .build();

    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert data under partition spec v0 (partitioned by category)
    ctx.sql(
        "INSERT INTO catalog.test_update_departition.departition_update_table VALUES \
         (1, 'electronics', 100), \
         (2, 'books', 200)",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // Evolve partition spec: change to unpartitioned (spec_id = 1)
    let table_ident = TableIdent::new(namespace.clone(), "departition_update_table".to_string());
    let table = client.load_table(&table_ident).await?;

    let unpartitioned_spec = UnboundPartitionSpec::builder().build();

    let tx = Transaction::new(&table);
    let action = tx
        .evolve_partition_spec()
        .add_spec(unpartitioned_spec)
        .set_default();
    let tx = action.apply(tx)?;
    let _table = tx.commit(client.as_ref()).await?;

    // Refresh catalog
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);
    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Update a row from the PARTITIONED spec (v0) using SQL
    let result = ctx
        .sql("UPDATE catalog.test_update_departition.departition_update_table SET value = 999 WHERE id = 1")
        .await
        .expect("UPDATE on de-partitioned table should succeed")
        .collect()
        .await
        .unwrap();

    let updated_count: u64 = result
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::UInt64Array>()
                .unwrap()
                .iter()
                .flatten()
        })
        .sum();
    assert_eq!(updated_count, 1, "Should update 1 row");

    // Re-register with fresh catalog
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);
    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Verify the update
    let df = ctx
        .sql("SELECT id, value FROM catalog.test_update_departition.departition_update_table ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let values: Vec<i32> = batches
        .iter()
        .flat_map(|b| {
            b.column(1)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int32Array>()
                .unwrap()
                .iter()
                .flatten()
        })
        .collect();

    assert_eq!(values, vec![999, 200], "id=1 should be updated to 999");

    Ok(())
}

/// Test DML operations spanning 3+ partition specs.
///
/// This tests the grouping and serialization logic across multiple spec_ids,
/// ensuring manifests are correctly written for each spec.
///
/// Evolution: unpartitioned (v0) → partitioned by category (v1) → partitioned by year (v2)
#[tokio::test]
async fn test_delete_with_three_partition_specs() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_delete_three_specs".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    // Create schema
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "category", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "year", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(4, "value", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()?;

    // Initial partition spec: unpartitioned (spec_id = 0)
    let partition_spec_v0 = UnboundPartitionSpec::builder().with_spec_id(0).build();

    let creation = TableCreation::builder()
        .name("three_specs_delete_table".to_string())
        .location(temp_path())
        .schema(schema)
        .partition_spec(partition_spec_v0)
        .properties(HashMap::new())
        .build();

    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let table_ident = TableIdent::new(namespace.clone(), "three_specs_delete_table".to_string());

    // === Phase 1: Insert under spec v0 (unpartitioned) ===
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);
    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    ctx.sql(
        "INSERT INTO catalog.test_delete_three_specs.three_specs_delete_table VALUES \
         (1, 'electronics', 2023, 100), \
         (2, 'books', 2023, 200)",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // === Phase 2: Evolve to spec v1 (partitioned by category) and insert ===
    let table = client.load_table(&table_ident).await?;
    let spec_v1 = UnboundPartitionSpec::builder()
        .add_partition_field(2, "category", Transform::Identity)?
        .build();

    let tx = Transaction::new(&table);
    let action = tx.evolve_partition_spec().add_spec(spec_v1).set_default();
    let tx = action.apply(tx)?;
    let _table = tx.commit(client.as_ref()).await?;

    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);
    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    ctx.sql(
        "INSERT INTO catalog.test_delete_three_specs.three_specs_delete_table VALUES \
         (3, 'electronics', 2024, 300), \
         (4, 'toys', 2024, 400)",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // === Phase 3: Evolve to spec v2 (partitioned by year) and insert ===
    let table = client.load_table(&table_ident).await?;
    let spec_v2 = UnboundPartitionSpec::builder()
        .add_partition_field(3, "year", Transform::Identity)?
        .build();

    let tx = Transaction::new(&table);
    let action = tx.evolve_partition_spec().add_spec(spec_v2).set_default();
    let tx = action.apply(tx)?;
    let _table = tx.commit(client.as_ref()).await?;

    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);
    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    ctx.sql(
        "INSERT INTO catalog.test_delete_three_specs.three_specs_delete_table VALUES \
         (5, 'books', 2025, 500), \
         (6, 'electronics', 2025, 600)",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // Verify we have 6 rows total across 3 specs
    let df = ctx
        .sql("SELECT COUNT(*) as cnt FROM catalog.test_delete_three_specs.three_specs_delete_table")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 6, "Should have 6 rows before delete");

    // === Delete spanning all 3 specs ===
    let provider = IcebergTableProvider::try_new(
        client.clone(),
        namespace.clone(),
        "three_specs_delete_table",
    )
    .await?;

    // Delete all electronics - this spans spec v0, v1, and v2
    let deleted_count = provider
        .delete(&ctx.state(), Some(col("category").eq(lit("electronics"))))
        .await
        .expect("DELETE across 3 partition specs should succeed");

    assert_eq!(
        deleted_count, 3,
        "Should delete 3 rows (id 1, 3, 6 where category='electronics')"
    );

    // Verify final state
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);
    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    let df = ctx
        .sql("SELECT id FROM catalog.test_delete_three_specs.three_specs_delete_table ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let ids: Vec<i32> = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int32Array>()
                .unwrap()
                .iter()
                .flatten()
        })
        .collect();

    assert_eq!(ids, vec![2, 4, 5], "Only rows 2, 4, 5 should remain");

    // Verify manifest structure has multiple spec_ids
    let table = client.load_table(&table_ident).await?;
    let snapshot = table.metadata().current_snapshot().unwrap();
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await?;

    let delete_manifests: Vec<_> = manifest_list
        .entries()
        .iter()
        .filter(|m| m.content == iceberg::spec::ManifestContentType::Deletes)
        .collect();

    // Should have delete manifests for multiple specs
    let spec_ids: std::collections::HashSet<i32> = delete_manifests
        .iter()
        .map(|m| m.partition_spec_id)
        .collect();

    assert!(
        spec_ids.len() >= 2,
        "Delete manifests should span multiple partition specs, got spec_ids: {:?}",
        spec_ids
    );

    Ok(())
}
