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

//! Cross-engine partition evolution tests.
//!
//! These tests verify that iceberg-rust can correctly perform DML operations
//! (DELETE, UPDATE, MERGE) on tables created by Spark with evolved partition specs.
//!
//! Tables are provisioned by Spark in `provision.py` with:
//! - Spec v0: unpartitioned data
//! - Spec v1: partitioned data (by category, region, etc.)
//!
//! See `docs/partition-evolution/DESIGN.md` for architecture details.

use std::sync::Arc;

use datafusion::assert_batches_sorted_eq;
use datafusion::prelude::*;
use iceberg::{Catalog, CatalogBuilder, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_datafusion::IcebergTableProvider;

use crate::get_shared_containers;

/// Test DELETE on a Spark-created table with evolved partition spec.
///
/// Setup (by Spark provision.py):
/// - Table `test_partition_evolution_delete` with 5 rows
/// - Rows 1-3 written under spec v0 (unpartitioned)
/// - Rows 4-5 written under spec v1 (partitioned by category)
///
/// Test: DELETE rows where value > 300 (should delete rows 4, 5)
#[tokio::test]
async fn test_crossengine_delete_with_partition_evolution() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);

    // Verify table has evolved partition spec
    let table = client
        .load_table(&TableIdent::from_strs(["default", "test_partition_evolution_delete"]).unwrap())
        .await
        .unwrap();

    let metadata = table.metadata();
    assert!(
        metadata.partition_specs().len() >= 2,
        "Table should have at least 2 partition specs (evolved)"
    );
    assert!(
        metadata.default_partition_spec_id() > 0,
        "Default spec should be evolved (non-zero)"
    );

    let ctx = SessionContext::new();

    // Create provider for DML operations
    let provider = IcebergTableProvider::try_new(
        client.clone(),
        vec!["default".to_string()].into(),
        "test_partition_evolution_delete",
    )
    .await
    .unwrap();

    // Verify initial data (5 rows)
    let initial_df = ctx
        .read_table(Arc::new(provider.clone()))
        .unwrap()
        .select_columns(&["id", "category", "value"])
        .unwrap()
        .sort(vec![col("id").sort(true, true)])
        .unwrap();

    let initial_batches = initial_df.collect().await.unwrap();
    let expected_initial = [
        "+----+-------------+-------+",
        "| id | category    | value |",
        "+----+-------------+-------+",
        "| 1  | electronics | 100   |",
        "| 2  | electronics | 200   |",
        "| 3  | books       | 300   |",
        "| 4  | books       | 400   |",
        "| 5  | electronics | 500   |",
        "+----+-------------+-------+",
    ];
    assert_batches_sorted_eq!(expected_initial, &initial_batches);

    // Delete rows where value > 300 (spans both partition specs)
    let deleted_count = provider
        .delete(&ctx.state(), Some(col("value").gt(lit(300))))
        .await
        .expect("DELETE across partition specs should succeed");

    assert_eq!(deleted_count, 2, "Should delete 2 rows (id 4, 5)");

    // Reload table and verify
    let provider_after = IcebergTableProvider::try_new(
        client.clone(),
        vec!["default".to_string()].into(),
        "test_partition_evolution_delete",
    )
    .await
    .unwrap();

    let after_df = ctx
        .read_table(Arc::new(provider_after))
        .unwrap()
        .select_columns(&["id", "category", "value"])
        .unwrap()
        .sort(vec![col("id").sort(true, true)])
        .unwrap();

    let after_batches = after_df.collect().await.unwrap();
    let expected_after = [
        "+----+-------------+-------+",
        "| id | category    | value |",
        "+----+-------------+-------+",
        "| 1  | electronics | 100   |",
        "| 2  | electronics | 200   |",
        "| 3  | books       | 300   |",
        "+----+-------------+-------+",
    ];
    assert_batches_sorted_eq!(expected_after, &after_batches);
}

/// Test UPDATE on a Spark-created table with evolved partition spec.
///
/// Setup (by Spark provision.py):
/// - Table `test_partition_evolution_update` with 5 rows
/// - Rows 1-3 written under spec v0 (unpartitioned)
/// - Rows 4-5 written under spec v1 (partitioned by region)
///
/// Test: UPDATE status to 'completed' where id > 2 (should update rows 3, 4, 5)
#[tokio::test]
async fn test_crossengine_update_with_partition_evolution() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);

    // Verify table has evolved partition spec
    let table = client
        .load_table(&TableIdent::from_strs(["default", "test_partition_evolution_update"]).unwrap())
        .await
        .unwrap();

    let metadata = table.metadata();
    assert!(
        metadata.partition_specs().len() >= 2,
        "Table should have at least 2 partition specs (evolved)"
    );

    let ctx = SessionContext::new();

    // Create provider for DML operations
    let provider = IcebergTableProvider::try_new(
        client.clone(),
        vec!["default".to_string()].into(),
        "test_partition_evolution_update",
    )
    .await
    .unwrap();

    // Update status for rows where id > 2 (spans both partition specs)
    let update_result = provider
        .update()
        .await
        .unwrap()
        .set("status", lit("completed"))
        .filter(col("id").gt(lit(2)))
        .execute(&ctx.state())
        .await
        .expect("UPDATE across partition specs should succeed");

    assert_eq!(
        update_result.rows_updated, 3,
        "Should update 3 rows (id 3, 4, 5)"
    );

    // Reload and verify
    let provider_after = IcebergTableProvider::try_new(
        client.clone(),
        vec!["default".to_string()].into(),
        "test_partition_evolution_update",
    )
    .await
    .unwrap();

    let after_df = ctx
        .read_table(Arc::new(provider_after))
        .unwrap()
        .select_columns(&["id", "region", "status"])
        .unwrap()
        .sort(vec![col("id").sort(true, true)])
        .unwrap();

    let after_batches = after_df.collect().await.unwrap();
    let expected_after = [
        "+----+--------+-----------+",
        "| id | region | status    |",
        "+----+--------+-----------+",
        "| 1  | US     | pending   |",
        "| 2  | US     | pending   |",
        "| 3  | EU     | completed |",
        "| 4  | EU     | completed |",
        "| 5  | APAC   | completed |",
        "+----+--------+-----------+",
    ];
    assert_batches_sorted_eq!(expected_after, &after_batches);
}

/// Test MERGE on a Spark-created table with evolved partition spec.
///
/// Setup (by Spark provision.py):
/// - Table `test_partition_evolution_merge` with 5 rows
/// - Rows 1-3 written under spec v0 (unpartitioned)
/// - Rows 4-5 written under spec v1 (partitioned by region)
///
/// Test: MERGE with source that updates rows 1, 3 and inserts row 6
#[tokio::test]
async fn test_crossengine_merge_with_partition_evolution() {
    use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;

    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);

    // Verify table has evolved partition spec
    let table = client
        .load_table(&TableIdent::from_strs(["default", "test_partition_evolution_merge"]).unwrap())
        .await
        .unwrap();

    let metadata = table.metadata();
    assert!(
        metadata.partition_specs().len() >= 2,
        "Table should have at least 2 partition specs (evolved)"
    );

    let ctx = SessionContext::new();

    // Create source data for MERGE
    let source_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("amount", DataType::Int32, false),
    ]));

    let source_batch = RecordBatch::try_new(
        source_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 3, 6])),          // ids to update/insert
            Arc::new(StringArray::from(vec!["US", "EU", "EU"])), // regions
            Arc::new(Int32Array::from(vec![150, 350, 600])),    // new amounts
        ],
    )
    .unwrap();

    let source_table = Arc::new(MemTable::try_new(source_schema, vec![vec![source_batch]]).unwrap());
    ctx.register_table("merge_source", source_table).unwrap();
    let source_df = ctx.table("merge_source").await.unwrap();

    // Create provider for DML operations
    let provider = IcebergTableProvider::try_new(
        client.clone(),
        vec!["default".to_string()].into(),
        "test_partition_evolution_merge",
    )
    .await
    .unwrap();

    // Execute MERGE across partition specs
    // Note: Use explicit column update to avoid updating partition column
    let stats = provider
        .merge(source_df)
        .await
        .unwrap()
        .on(col("target.id").eq(col("source.id")))
        .when_matched(None)
        .update(vec![("amount", col("source_amount"))])
        .when_not_matched(None)
        .insert_all()
        .execute(&ctx.state())
        .await
        .expect("MERGE across partition specs should succeed");

    assert_eq!(stats.rows_updated, 2, "Should update 2 rows (id 1, 3)");
    assert_eq!(stats.rows_inserted, 1, "Should insert 1 row (id 6)");

    // Reload and verify
    let provider_after = IcebergTableProvider::try_new(
        client.clone(),
        vec!["default".to_string()].into(),
        "test_partition_evolution_merge",
    )
    .await
    .unwrap();

    let after_df = ctx
        .read_table(Arc::new(provider_after))
        .unwrap()
        .select_columns(&["id", "region", "amount"])
        .unwrap()
        .sort(vec![col("id").sort(true, true)])
        .unwrap();

    let after_batches = after_df.collect().await.unwrap();
    let expected_after = [
        "+----+--------+--------+",
        "| id | region | amount |",
        "+----+--------+--------+",
        "| 1  | US     | 150    |",
        "| 2  | US     | 200    |",
        "| 3  | EU     | 350    |",
        "| 4  | EU     | 400    |",
        "| 5  | APAC   | 500    |",
        "| 6  | EU     | 600    |",
        "+----+--------+--------+",
    ];
    assert_batches_sorted_eq!(expected_after, &after_batches);
}
