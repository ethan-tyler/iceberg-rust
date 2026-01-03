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

use chrono::{DateTime, Utc};
use datafusion::arrow::array::{Array, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::execution::context::SessionContext;
use datafusion::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use datafusion::prelude::{col, lit};
use expect_test::expect;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    NestedField, PrimitiveType, Schema, StructType, Transform, Type, UnboundPartitionSpec,
};
use iceberg::test_utils::check_record_batches;
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
            "my_table$partitions",
            "my_table$entries",
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
        "INSERT INTO catalog.{namespace_name}.delete_table VALUES \
         (1, 'alice'), (2, 'bob'), (3, 'charlie'), (4, 'diana'), (5, 'eve')",
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

/// Test DELETE on a partitioned table.
/// Currently, DELETE on partitioned tables returns NotImplemented error.
/// This test verifies that the error is properly returned with a clear message.
/// TODO: When partitioned table DELETE is implemented, update this test to verify actual behavior.
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

    // Create provider for programmatic delete
    let provider = IcebergTableProvider::try_new(
        client.clone(),
        namespace.clone(),
        "partitioned_delete_table",
    )
    .await?;

    // Attempt to delete from a partitioned table should return NotImplemented
    let result = provider
        .delete(&ctx.state(), Some(col("category").eq(lit("electronics"))))
        .await;

    assert!(
        result.is_err(),
        "Expected NotImplemented error for partitioned table DELETE"
    );
    let err = result.unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("partitioned tables is not yet supported"),
        "Expected error message about partitioned tables, got: {err_msg}",
    );

    // Verify data is still intact (delete did not occur)
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
        count, 4,
        "All 4 rows should still exist since delete failed"
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
        "INSERT INTO catalog.{namespace_name}.update_table VALUES \
         (1, 'alice'), (2, 'bob'), (3, 'charlie'), (4, 'diana'), (5, 'eve')",
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

/// Test UPDATE on a partitioned table.
/// Position delete files should include partition values from the data files.
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

    // Update a row in the partitioned table
    let update_count = provider
        .update()
        .await
        .unwrap()
        .set("value", lit("updated_laptop"))
        .filter(col("id").eq(lit(1)))
        .execute(&ctx.state())
        .await
        .expect("UPDATE on partitioned table should succeed");

    assert_eq!(update_count, 1, "Should have updated 1 row");

    // Re-register catalog to pick up changes
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);
    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Verify the update was applied correctly
    let df = ctx
        .sql("SELECT id, category, value FROM catalog.test_update_partitioned.partitioned_update_table ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2);

    let id_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int32Array>()
        .unwrap();
    let value_array = batch
        .column(2)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();

    // Row 1 should be updated
    assert_eq!(id_array.value(0), 1);
    assert_eq!(value_array.value(0), "updated_laptop");

    // Row 2 should be unchanged
    assert_eq!(id_array.value(1), 2);
    assert_eq!(value_array.value(1), "phone");

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

/// Test UPDATE affecting rows across multiple partitions.
/// Verifies that delete files are created per-partition and updates work correctly
/// across partition boundaries.
#[tokio::test]
async fn test_update_partitioned_cross_partition() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_update_cross_part".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    // Create schema with partition column
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "region", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "value", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()?;

    let partition_spec = UnboundPartitionSpec::builder()
        .with_spec_id(0)
        .add_partition_field(2, "region", Transform::Identity)?
        .build();

    let creation = TableCreation::builder()
        .name("cross_partition_table".to_string())
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

    // Insert data across 3 different partitions
    ctx.sql(
        "INSERT INTO catalog.test_update_cross_part.cross_partition_table VALUES \
         (1, 'US', 100), \
         (2, 'US', 200), \
         (3, 'EU', 150), \
         (4, 'EU', 250), \
         (5, 'ASIA', 300)",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    let provider =
        IcebergTableProvider::try_new(client.clone(), namespace.clone(), "cross_partition_table")
            .await?;

    // Update rows across multiple partitions (value > 150 touches US(id=2), EU(id=4), ASIA(id=5))
    let update_count = provider
        .update()
        .await
        .unwrap()
        .set("value", lit(999))
        .filter(col("value").gt(lit(150)))
        .execute(&ctx.state())
        .await
        .expect("UPDATE across partitions should succeed");

    assert_eq!(
        update_count, 3,
        "Should have updated 3 rows across partitions"
    );

    // Re-register catalog to pick up changes
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);
    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Verify all rows have correct values
    let df = ctx
        .sql("SELECT id, region, value FROM catalog.test_update_cross_part.cross_partition_table ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 5);

    let id_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int32Array>()
        .unwrap();
    let value_array = batch
        .column(2)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int32Array>()
        .unwrap();

    // Verify each row
    // id=1 (US, 100): NOT updated
    assert_eq!(id_array.value(0), 1);
    assert_eq!(value_array.value(0), 100, "id=1 should NOT be updated");

    // id=2 (US, 200): updated to 999
    assert_eq!(id_array.value(1), 2);
    assert_eq!(value_array.value(1), 999, "id=2 should be updated to 999");

    // id=3 (EU, 150): NOT updated (150 is not > 150)
    assert_eq!(id_array.value(2), 3);
    assert_eq!(value_array.value(2), 150, "id=3 should NOT be updated");

    // id=4 (EU, 250): updated to 999
    assert_eq!(id_array.value(3), 4);
    assert_eq!(value_array.value(3), 999, "id=4 should be updated to 999");

    // id=5 (ASIA, 300): updated to 999
    assert_eq!(id_array.value(4), 5);
    assert_eq!(value_array.value(4), 999, "id=5 should be updated to 999");

    Ok(())
}

/// Test UPDATE on table with year partition transform.
/// Verifies that time-based partition transforms work correctly.
#[tokio::test]
async fn test_update_partitioned_year_transform() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_update_year".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    // Create schema with date column for year partitioning
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "event_date", Type::Primitive(PrimitiveType::Date)).into(),
            NestedField::required(3, "status", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;

    // Partition by year(event_date)
    let partition_spec = UnboundPartitionSpec::builder()
        .with_spec_id(0)
        .add_partition_field(2, "event_year", Transform::Year)?
        .build();

    let creation = TableCreation::builder()
        .name("year_partitioned_table".to_string())
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

    // Insert data across multiple years using DATE literals
    // Days since epoch: 2022-01-15 = 19007, 2022-06-20 = 19163, 2023-03-10 = 19426, 2024-01-01 = 19723
    ctx.sql(
        "INSERT INTO catalog.test_update_year.year_partitioned_table VALUES \
         (1, DATE '2022-01-15', 'pending'), \
         (2, DATE '2022-06-20', 'pending'), \
         (3, DATE '2023-03-10', 'pending'), \
         (4, DATE '2024-01-01', 'pending')",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    let provider =
        IcebergTableProvider::try_new(client.clone(), namespace.clone(), "year_partitioned_table")
            .await?;

    // Update all rows to 'processed'
    let update_count = provider
        .update()
        .await
        .unwrap()
        .set("status", lit("processed"))
        .execute(&ctx.state())
        .await
        .expect("UPDATE on year-partitioned table should succeed");

    assert_eq!(update_count, 4, "Should have updated 4 rows");

    // Re-register catalog to pick up changes
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);
    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Verify all rows have 'processed' status
    let df = ctx
        .sql("SELECT id, status FROM catalog.test_update_year.year_partitioned_table ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 4);

    let status_array = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    for i in 0..4 {
        assert_eq!(
            status_array.value(i),
            "processed",
            "Row {} should have status 'processed'",
            i + 1
        );
    }

    Ok(())
}

/// Test UPDATE with multiple partitions in a single file (data inserted together).
/// This verifies the FanoutWriter correctly routes deletes to separate partition delete files.
#[tokio::test]
async fn test_update_partitioned_multiple_in_single_insert() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_update_multi_part".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "category", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "amount", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()?;

    let partition_spec = UnboundPartitionSpec::builder()
        .with_spec_id(0)
        .add_partition_field(2, "category", Transform::Identity)?
        .build();

    let creation = TableCreation::builder()
        .name("multi_part_table".to_string())
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

    // Insert first batch: category A and B
    ctx.sql(
        "INSERT INTO catalog.test_update_multi_part.multi_part_table VALUES \
         (1, 'A', 10), \
         (2, 'B', 20)",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // Insert second batch: category A and C (A appears in both batches)
    ctx.sql(
        "INSERT INTO catalog.test_update_multi_part.multi_part_table VALUES \
         (3, 'A', 30), \
         (4, 'C', 40)",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    let provider =
        IcebergTableProvider::try_new(client.clone(), namespace.clone(), "multi_part_table")
            .await?;

    // Update all category 'A' rows (spans two different data files)
    let update_count = provider
        .update()
        .await
        .unwrap()
        .set("amount", lit(999))
        .filter(col("category").eq(lit("A")))
        .execute(&ctx.state())
        .await
        .expect("UPDATE should succeed");

    assert_eq!(update_count, 2, "Should have updated 2 rows in category A");

    // Verify the updates
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);
    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    let df = ctx
        .sql("SELECT id, category, amount FROM catalog.test_update_multi_part.multi_part_table ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let batch = &batches[0];
    let amount_array = batch
        .column(2)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int32Array>()
        .unwrap();

    // id=1 (A): updated to 999
    assert_eq!(amount_array.value(0), 999, "id=1 should be updated");
    // id=2 (B): NOT updated
    assert_eq!(amount_array.value(1), 20, "id=2 should NOT be updated");
    // id=3 (A): updated to 999
    assert_eq!(amount_array.value(2), 999, "id=3 should be updated");
    // id=4 (C): NOT updated
    assert_eq!(amount_array.value(3), 40, "id=4 should NOT be updated");

    Ok(())
}

/// Test UPDATE affecting all rows in a partitioned table (full table update).
/// This stresses the FanoutWriter with all partitions being modified.
#[tokio::test]
async fn test_update_partitioned_full_table() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_update_part_full".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "region", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "processed", Type::Primitive(PrimitiveType::Boolean)).into(),
        ])
        .build()?;

    let partition_spec = UnboundPartitionSpec::builder()
        .with_spec_id(0)
        .add_partition_field(2, "region", Transform::Identity)?
        .build();

    let creation = TableCreation::builder()
        .name("full_part_table".to_string())
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

    // Insert data into multiple partitions
    ctx.sql(
        "INSERT INTO catalog.test_update_part_full.full_part_table VALUES \
         (1, 'US', false), \
         (2, 'EU', false), \
         (3, 'ASIA', false), \
         (4, 'US', false)",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    let provider =
        IcebergTableProvider::try_new(client.clone(), namespace.clone(), "full_part_table").await?;

    // Full table UPDATE (no filter) - all partitions affected
    let update_count = provider
        .update()
        .await
        .unwrap()
        .set("processed", lit(true))
        .execute(&ctx.state())
        .await
        .expect("Full table UPDATE should succeed");

    assert_eq!(update_count, 4, "Should have updated all 4 rows");

    // Verify
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);
    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Query all rows and verify processed column values directly
    let df = ctx
        .sql("SELECT id, processed FROM catalog.test_update_part_full.full_part_table ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 4);

    let processed_array = batch
        .column(1)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::BooleanArray>()
        .unwrap();

    for i in 0..4 {
        assert!(
            processed_array.value(i),
            "Row {} should have processed=true",
            i + 1
        );
    }

    Ok(())
}

// ============================================================================
// DYNAMIC PARTITION PRUNING TESTS
// ============================================================================

/// Test that queries on non-partitioned tables work correctly.
/// This verifies no regression when dynamic partition pruning is not applicable.
#[tokio::test]
async fn test_non_partitioned_table_no_regression() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_non_partitioned".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    // Create a non-partitioned table (no partition spec)
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "value", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;

    let creation = TableCreation::builder()
        .location(temp_path())
        .name("non_partitioned_table".to_string())
        .schema(schema)
        .build();

    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert some data
    ctx.sql(
        "INSERT INTO catalog.test_non_partitioned.non_partitioned_table VALUES \
         (1, 'a'), (2, 'b'), (3, 'c')",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // Query with a filter (should work without partition pruning)
    let result = ctx
        .sql(
            "SELECT id, value FROM catalog.test_non_partitioned.non_partitioned_table \
             WHERE id > 1 ORDER BY id",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Verify results: should have 2 rows (id=2 and id=3)
    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "Expected 2 rows where id > 1");

    let batch = &result[0];
    let id_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int32Array>()
        .unwrap();
    let value_array = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(id_array.value(0), 2);
    assert_eq!(value_array.value(0), "b");
    assert_eq!(id_array.value(1), 3);
    assert_eq!(value_array.value(1), "c");

    // Query all data (no filter)
    let result = ctx
        .sql("SELECT COUNT(*) as cnt FROM catalog.test_non_partitioned.non_partitioned_table")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let count = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 3, "Expected 3 total rows");

    Ok(())
}

/// Test partition pruning on a partitioned table.
/// Verifies that filters can be applied to partition pruning.
///
/// This test creates a partitioned table and queries with an IN-list filter
/// on the partition column, demonstrating partition pruning behavior.
#[tokio::test]
async fn test_partition_pruning_with_in_list() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_partition_pruning".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    // Create partitioned table
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "region", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "amount", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()?;

    let partition_spec = UnboundPartitionSpec::builder()
        .with_spec_id(0)
        .add_partition_field(2, "region", Transform::Identity)?
        .build();

    let creation = TableCreation::builder()
        .location(temp_path())
        .name("partitioned_fact".to_string())
        .schema(schema)
        .partition_spec(partition_spec)
        .build();

    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert data across 4 different partitions
    ctx.sql(
        "INSERT INTO catalog.test_partition_pruning.partitioned_fact VALUES \
         (1, 'US', 100), (2, 'US', 200), \
         (3, 'EU', 150), (4, 'EU', 250), \
         (5, 'APAC', 300), (6, 'APAC', 350), \
         (7, 'LATAM', 400), (8, 'LATAM', 450)",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // Query with IN-list filter on partition column (simulates DPP behavior)
    // Only US and EU partitions should be scanned (not APAC or LATAM)
    let result = ctx
        .sql(
            "SELECT id, region, amount \
             FROM catalog.test_partition_pruning.partitioned_fact \
             WHERE region IN ('US', 'EU') \
             ORDER BY id",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Verify correct results (only US and EU data - 4 rows)
    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4, "Expected 4 rows for US and EU regions");

    let batch = &result[0];
    let id_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int32Array>()
        .unwrap();
    let region_array = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    // Verify the results
    assert_eq!(id_array.value(0), 1);
    assert_eq!(region_array.value(0), "US");

    assert_eq!(id_array.value(1), 2);
    assert_eq!(region_array.value(1), "US");

    assert_eq!(id_array.value(2), 3);
    assert_eq!(region_array.value(2), "EU");

    assert_eq!(id_array.value(3), 4);
    assert_eq!(region_array.value(3), "EU");

    // Verify the EXPLAIN shows IcebergTableScan with the predicate pushed down
    let explain = ctx
        .sql(
            "EXPLAIN SELECT id FROM catalog.test_partition_pruning.partitioned_fact \
             WHERE region IN ('US', 'EU')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Check that EXPLAIN output mentions IcebergTableScan with predicate
    let explain_str = format!("{explain:?}");
    assert!(
        explain_str.contains("IcebergTableScan"),
        "EXPLAIN should show IcebergTableScan"
    );

    Ok(())
}

/// Test dynamic partition pruning with a hash join.
/// Verifies that runtime filters from joins are applied to partition pruning.
///
/// This test creates:
/// - A partitioned "fact" table with data across multiple partitions
/// - A small "dimension" table with filter values
/// - Executes a join that should trigger dynamic partition pruning
#[tokio::test]
async fn test_dynamic_partition_pruning_with_join() -> Result<()> {
    use datafusion::physical_plan::metrics::MetricValue;
    use iceberg_datafusion::physical_plan::IcebergTableScan;

    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_dpp_join".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    // Create partitioned fact table
    let fact_schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "partition_id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(3, "amount", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()?;

    let partition_spec = UnboundPartitionSpec::builder()
        .with_spec_id(0)
        .add_partition_field(2, "partition_id", Transform::Identity)?
        .build();

    let fact_creation = TableCreation::builder()
        .location(temp_path())
        .name("fact_table".to_string())
        .schema(fact_schema)
        .partition_spec(partition_spec)
        .build();

    iceberg_catalog
        .create_table(&namespace, fact_creation)
        .await?;

    // Create dimension table (non-partitioned)
    let dim_schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "partition_id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "partition_name", Type::Primitive(PrimitiveType::String))
                .into(),
        ])
        .build()?;

    let dim_creation = TableCreation::builder()
        .location(temp_path())
        .name("dim_table".to_string())
        .schema(dim_schema)
        .build();

    iceberg_catalog
        .create_table(&namespace, dim_creation)
        .await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new_with_config(
        datafusion::execution::context::SessionConfig::new().with_target_partitions(1),
    );
    ctx.register_catalog("catalog", catalog);

    // Insert data into fact table across 4 different partitions
    ctx.sql(
        "INSERT INTO catalog.test_dpp_join.fact_table VALUES \
         (1, 1, 100), (2, 1, 200), \
         (3, 2, 150), (4, 2, 250), \
         (5, 3, 300), (6, 3, 350), \
         (7, 4, 400), (8, 4, 450)",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // Insert data into dimension table (only 2 of 4 partitions)
    ctx.sql(
        "INSERT INTO catalog.test_dpp_join.dim_table VALUES \
         (1, 'one'), (2, 'two')",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    fn planned_tasks_for_iceberg_scans(
        plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    ) -> usize {
        let mut total = 0;
        let mut stack = vec![Arc::clone(plan)];
        while let Some(node) = stack.pop() {
            if node.name() == "IcebergTableScan"
                && let Some(metrics) = node.metrics()
            {
                for metric in metrics.iter() {
                    if let MetricValue::Count { name, count } = metric.value()
                        && name == "planned_tasks"
                    {
                        total += count.value();
                    }
                }
            }

            for child in node.children() {
                stack.push(Arc::clone(child));
            }
        }

        total
    }

    fn planned_tasks_for_fact_scan(
        plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    ) -> usize {
        let mut total = 0;
        let mut stack = vec![Arc::clone(plan)];
        while let Some(node) = stack.pop() {
            if let Some(scan) = node.as_any().downcast_ref::<IcebergTableScan>()
                && scan
                    .projection()
                    .is_some_and(|cols| cols.iter().any(|c| c == "amount"))
                && let Some(metrics) = node.metrics()
            {
                for metric in metrics.iter() {
                    if let MetricValue::Count { name, count } = metric.value()
                        && name == "planned_tasks"
                    {
                        total += count.value();
                    }
                }
            }

            for child in node.children() {
                stack.push(Arc::clone(child));
            }
        }

        total
    }

    // Baseline scan (no join): should touch all partitions/files.
    let baseline_df = ctx
        .sql("SELECT id FROM catalog.test_dpp_join.fact_table ORDER BY id")
        .await
        .unwrap();
    let baseline_task_ctx = Arc::new(baseline_df.task_ctx());
    let baseline_plan = baseline_df.create_physical_plan().await.unwrap();
    datafusion::physical_plan::collect(Arc::clone(&baseline_plan), baseline_task_ctx)
        .await
        .unwrap();
    let baseline_tasks = planned_tasks_for_iceberg_scans(&baseline_plan);
    assert!(
        baseline_tasks >= 4,
        "Expected at least 4 planned scan tasks (one per partition), got {baseline_tasks}"
    );

    // Execute a join - this should benefit from dynamic partition pruning
    let join_df = ctx
        .sql(
            "SELECT f.id, f.partition_id, f.amount, d.partition_name \
             FROM catalog.test_dpp_join.dim_table d \
             JOIN catalog.test_dpp_join.fact_table f ON f.partition_id = d.partition_id \
             ORDER BY f.id",
        )
        .await
        .unwrap();
    let join_task_ctx = Arc::new(join_df.task_ctx());
    let join_plan = join_df.clone().create_physical_plan().await.unwrap();
    let result = datafusion::physical_plan::collect(Arc::clone(&join_plan), join_task_ctx)
        .await
        .unwrap();
    let join_fact_tasks = planned_tasks_for_fact_scan(&join_plan);
    assert!(
        join_fact_tasks < baseline_tasks,
        "Expected fewer planned scan tasks with join runtime filters (baseline={baseline_tasks}, join_fact={join_fact_tasks})"
    );

    // Verify correct results (only partition_id 1 and 2 data - 4 rows)
    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 4,
        "Expected 4 rows from join (partition_id 1 and 2 only)"
    );

    let batch = &result[0];
    let id_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int32Array>()
        .unwrap();
    let partition_id_array = batch
        .column(1)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int32Array>()
        .unwrap();
    let partition_name_array = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    // Verify the join results
    assert_eq!(id_array.value(0), 1);
    assert_eq!(partition_id_array.value(0), 1);
    assert_eq!(partition_name_array.value(0), "one");

    assert_eq!(id_array.value(1), 2);
    assert_eq!(partition_id_array.value(1), 1);
    assert_eq!(partition_name_array.value(1), "one");

    assert_eq!(id_array.value(2), 3);
    assert_eq!(partition_id_array.value(2), 2);
    assert_eq!(partition_name_array.value(2), "two");

    assert_eq!(id_array.value(3), 4);
    assert_eq!(partition_id_array.value(3), 2);
    assert_eq!(partition_name_array.value(3), "two");

    // Verify the physical plan uses a hash join and the scan receives a dynamic filter.
    let records = join_df
        .explain(false, false)
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    let record = &records[0];
    let plan_str = record
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(plan_str.len(), 2);
    assert!(
        plan_str.value(1).contains("HashJoinExec"),
        "Expected a hash join physical plan"
    );
    assert!(
        plan_str.value(1).contains("dynamic_filter:[enabled]"),
        "Expected IcebergTableScan to show an enabled dynamic filter"
    );

    Ok(())
}

// =============================================================================
// Time Travel Tests
// =============================================================================

/// Test time travel using snapshot ID syntax (table@v<snapshot_id>)
///
/// This test:
/// 1. Creates a table
/// 2. Inserts initial data (creates snapshot 1)
/// 3. Inserts more data (creates snapshot 2)
/// 4. Uses @v<snapshot_id> syntax to query the first snapshot
/// 5. Verifies that the time travel query returns only the original data
#[tokio::test]
async fn test_time_travel_snapshot_id() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_time_travel_snapshot".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let creation = get_table_creation(temp_path(), "tt_table", None)?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert initial data - this creates snapshot 1
    ctx.sql("INSERT INTO catalog.test_time_travel_snapshot.tt_table VALUES (1, 'first')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Get the snapshot ID after first insert
    let table = client
        .load_table(&TableIdent::new(namespace.clone(), "tt_table".to_string()))
        .await
        .unwrap();
    let snapshot1_id = table.metadata().current_snapshot().unwrap().snapshot_id();

    // Insert more data - this creates snapshot 2
    ctx.sql("INSERT INTO catalog.test_time_travel_snapshot.tt_table VALUES (2, 'second')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Query current snapshot - should return 2 rows
    let current_df = ctx
        .sql("SELECT * FROM catalog.test_time_travel_snapshot.tt_table ORDER BY foo1")
        .await
        .unwrap();
    let current_batches = current_df.collect().await.unwrap();
    let total_rows: usize = current_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "Current snapshot should have 2 rows");

    // Query historical snapshot using @v<snapshot_id> syntax - should return 1 row
    let historical_sql = format!(
        "SELECT * FROM \"catalog\".\"test_time_travel_snapshot\".\"tt_table@v{snapshot1_id}\" ORDER BY foo1"
    );
    let historical_df = ctx.sql(&historical_sql).await.unwrap();
    let historical_batches = historical_df.collect().await.unwrap();
    let historical_rows: usize = historical_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(historical_rows, 1, "Historical snapshot should have 1 row");

    // Verify the historical data is correct
    let batch = &historical_batches[0];
    let foo1_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int32Array>()
        .unwrap();
    assert_eq!(foo1_col.value(0), 1, "Historical row should have foo1=1");

    // Query historical snapshot using VERSION AS OF syntax - should return 1 row
    let version_sql = format!(
        "SELECT * FROM catalog.test_time_travel_snapshot.tt_table VERSION AS OF {snapshot1_id} ORDER BY foo1",
    );
    let version_df = ctx.sql(&version_sql).await.unwrap();
    let version_batches = version_df.collect().await.unwrap();
    let version_rows: usize = version_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(version_rows, 1, "VERSION AS OF should return 1 row");

    let version_batch = &version_batches[0];
    let version_foo1 = version_batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int32Array>()
        .unwrap();
    assert_eq!(
        version_foo1.value(0),
        1,
        "VERSION AS OF row should have foo1=1"
    );

    Ok(())
}

/// Test time travel using timestamp syntax (table@ts<timestamp_ms>)
///
/// This test:
/// 1. Creates a table
/// 2. Records timestamp before first insert
/// 3. Inserts initial data (creates snapshot 1)
/// 4. Records timestamp between inserts
/// 5. Inserts more data (creates snapshot 2)
/// 6. Uses @ts<timestamp> syntax to query at the middle timestamp
/// 7. Verifies that the time travel query returns only the first insert's data
#[tokio::test]
async fn test_time_travel_timestamp() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_time_travel_ts".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let creation = get_table_creation(temp_path(), "ts_table", None)?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert initial data - this creates snapshot 1
    ctx.sql("INSERT INTO catalog.test_time_travel_ts.ts_table VALUES (1, 'first')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Get the timestamp of snapshot 1
    let table = client
        .load_table(&TableIdent::new(namespace.clone(), "ts_table".to_string()))
        .await
        .unwrap();
    let snapshot1_ts = table.metadata().current_snapshot().unwrap().timestamp_ms();
    let snapshot1_ts_str = DateTime::<Utc>::from_timestamp_millis(snapshot1_ts)
        .unwrap()
        .format("%Y-%m-%d %H:%M:%S%.3f")
        .to_string();

    // Small delay to ensure next snapshot has a different timestamp
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    // Insert more data - this creates snapshot 2
    ctx.sql("INSERT INTO catalog.test_time_travel_ts.ts_table VALUES (2, 'second')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Query using timestamp that matches snapshot 1 exactly
    let ts_sql = format!(
        "SELECT * FROM \"catalog\".\"test_time_travel_ts\".\"ts_table@ts{snapshot1_ts}\" ORDER BY foo1"
    );
    let ts_df = ctx.sql(&ts_sql).await.unwrap();
    let ts_batches = ts_df.collect().await.unwrap();
    let ts_rows: usize = ts_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        ts_rows, 1,
        "Query at snapshot1 timestamp should return 1 row"
    );

    // Verify the data is correct
    let batch = &ts_batches[0];
    let foo1_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int32Array>()
        .unwrap();
    assert_eq!(foo1_col.value(0), 1, "Row should have foo1=1");

    // Query using TIMESTAMP AS OF with a string literal - should return 1 row
    let timestamp_sql = format!(
        "SELECT * FROM catalog.test_time_travel_ts.ts_table TIMESTAMP AS OF '{snapshot1_ts_str}' ORDER BY foo1"
    );
    let timestamp_df = ctx.sql(&timestamp_sql).await.unwrap();
    let timestamp_batches = timestamp_df.collect().await.unwrap();
    let timestamp_rows: usize = timestamp_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(timestamp_rows, 1, "TIMESTAMP AS OF should return 1 row");

    // CTE with time travel
    let cte_sql = format!(
        "WITH historical AS (SELECT * FROM catalog.test_time_travel_ts.ts_table TIMESTAMP AS OF '{snapshot1_ts_str}') \
         SELECT count(*) FROM historical"
    );
    let cte_batches = ctx.sql(&cte_sql).await.unwrap().collect().await.unwrap();
    let cte_count = cte_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(cte_count.value(0), 1, "CTE time travel should return 1 row");

    Ok(())
}

/// Test time travel in a JOIN query
///
/// This test verifies that time travel works correctly in JOIN operations
/// by joining current data with historical data.
#[tokio::test]
async fn test_time_travel_in_join() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_time_travel_join".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let creation = get_table_creation(temp_path(), "join_table", None)?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert initial data
    ctx.sql("INSERT INTO catalog.test_time_travel_join.join_table VALUES (1, 'old_value')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Get the snapshot ID
    let table = client
        .load_table(&TableIdent::new(
            namespace.clone(),
            "join_table".to_string(),
        ))
        .await
        .unwrap();
    let snapshot1_id = table.metadata().current_snapshot().unwrap().snapshot_id();

    // Insert updated data (same foo1 key, different foo2 value)
    ctx.sql("INSERT INTO catalog.test_time_travel_join.join_table VALUES (1, 'new_value')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Join current with historical to see the change
    let join_sql = format!(
        r#"SELECT
            current.foo1,
            historical.foo2 as old_foo2,
            current.foo2 as new_foo2
        FROM catalog.test_time_travel_join.join_table AS current
        JOIN "catalog"."test_time_travel_join"."join_table@v{snapshot1_id}" AS historical
            ON current.foo1 = historical.foo1
        WHERE current.foo2 = 'new_value'
        ORDER BY current.foo1"#,
    );

    let join_df = ctx.sql(&join_sql).await.unwrap();
    let join_batches = join_df.collect().await.unwrap();
    let join_rows: usize = join_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(join_rows, 1, "Join should return 1 row");

    // Verify the joined data shows old and new values
    let batch = &join_batches[0];
    let old_foo2 = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let new_foo2 = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(old_foo2.value(0), "old_value");
    assert_eq!(new_foo2.value(0), "new_value");

    // Join using VERSION AS OF syntax
    let join_version_sql = format!(
        r#"SELECT
            current.foo1,
            historical.foo2 as old_foo2,
            current.foo2 as new_foo2
        FROM catalog.test_time_travel_join.join_table AS current
        JOIN catalog.test_time_travel_join.join_table VERSION AS OF {snapshot1_id} AS historical
            ON current.foo1 = historical.foo1
        WHERE current.foo2 = 'new_value'
        ORDER BY current.foo1"#,
    );

    let join_version_df = ctx.sql(&join_version_sql).await.unwrap();
    let join_version_batches = join_version_df.collect().await.unwrap();
    let join_version_rows: usize = join_version_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        join_version_rows, 1,
        "VERSION AS OF join should return 1 row"
    );

    Ok(())
}

/// Test time travel error handling for invalid snapshot ID
#[tokio::test]
async fn test_time_travel_invalid_snapshot_id() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_time_travel_error".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let creation = get_table_creation(temp_path(), "error_table", None)?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert some data
    ctx.sql("INSERT INTO catalog.test_time_travel_error.error_table VALUES (1, 'test')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Try to query with an invalid snapshot ID (99999 is unlikely to exist)
    let invalid_sql = "SELECT * FROM \"catalog\".\"test_time_travel_error\".\"error_table@v99999\"";
    let result = ctx.sql(invalid_sql).await;

    // Should fail with a clear error message about snapshot not found
    assert!(
        result.is_err(),
        "Query with invalid snapshot ID should fail"
    );
    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("99999") && error_msg.contains("not found"),
        "Error message should mention the invalid snapshot ID: {error_msg}"
    );

    // Try VERSION AS OF with an invalid snapshot ID
    let invalid_version_sql =
        "SELECT * FROM catalog.test_time_travel_error.error_table VERSION AS OF 99999";
    let version_result = ctx.sql(invalid_version_sql).await;
    assert!(
        version_result.is_err(),
        "VERSION AS OF with invalid snapshot ID should fail"
    );
    let version_error_msg = version_result.unwrap_err().to_string();
    assert!(
        version_error_msg.contains("99999") && version_error_msg.contains("not found"),
        "VERSION AS OF error message should mention the invalid snapshot ID: {version_error_msg}"
    );

    Ok(())
}

/// Test time travel error handling for timestamp before any snapshot
#[tokio::test]
async fn test_time_travel_timestamp_too_early() -> Result<()> {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new("test_time_travel_early".to_string());
    set_test_namespace(&iceberg_catalog, &namespace).await?;

    let creation = get_table_creation(temp_path(), "early_table", None)?;
    iceberg_catalog.create_table(&namespace, creation).await?;

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client).await?);

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert some data
    ctx.sql("INSERT INTO catalog.test_time_travel_early.early_table VALUES (1, 'test')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Try to query with a very old timestamp (epoch + 1ms)
    let early_sql = "SELECT * FROM \"catalog\".\"test_time_travel_early\".\"early_table@ts1\"";
    let result = ctx.sql(early_sql).await;

    // Should fail with a clear error message about no snapshot found
    assert!(
        result.is_err(),
        "Query with timestamp before any snapshot should fail"
    );
    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("No snapshot found"),
        "Error message should indicate no snapshot was found: {error_msg}"
    );

    // Try TIMESTAMP AS OF with a timestamp before any snapshot
    let early_ts_sql = "SELECT * FROM catalog.test_time_travel_early.early_table TIMESTAMP AS OF '1970-01-01 00:00:00.001'";
    let ts_result = ctx.sql(early_ts_sql).await;
    assert!(
        ts_result.is_err(),
        "TIMESTAMP AS OF with early timestamp should fail"
    );
    let ts_error_msg = ts_result.unwrap_err().to_string();
    assert!(
        ts_error_msg.contains("No snapshot found"),
        "TIMESTAMP AS OF error message should indicate no snapshot was found: {ts_error_msg}"
    );

    Ok(())
}
