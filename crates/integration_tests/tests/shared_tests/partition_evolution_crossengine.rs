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

use bytes::Bytes;
use chrono::{Duration, Utc};
use datafusion::assert_batches_sorted_eq;
use datafusion::prelude::*;
use iceberg::spec::Operation;
use iceberg::transaction::{ApplyTransactionAction, RewriteDataFilesOptions, RewriteStrategy, Transaction};
use iceberg::{Catalog, CatalogBuilder, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_datafusion::compaction::{CompactionOptions, compact_table};
use iceberg_datafusion::IcebergTableProvider;
use iceberg_integration_tests::spark_validator::{
    ValidationType, spark_validate_distinct_with_container, spark_validate_with_container,
};
use uuid::Uuid;

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
        metadata.partition_specs_iter().count() >= 2,
        "Table should have at least 2 partition specs (evolved)"
    );
    assert!(
        metadata.default_partition_spec_id() > 0,
        "Default spec should be evolved (non-zero)"
    );

    let ctx = SessionContext::new();
    let namespace = iceberg::NamespaceIdent::new("default".to_string());

    // Create provider for DML operations
    let provider = IcebergTableProvider::try_new(
        client.clone(),
        namespace.clone(),
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
    let provider_after =
        IcebergTableProvider::try_new(client.clone(), namespace, "test_partition_evolution_delete")
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

    // Spark validation: count + distinct + metadata sanity
    let spark_container = fixture.spark_container_name();
    let count_result = spark_validate_with_container(
        &spark_container,
        "test_partition_evolution_delete",
        ValidationType::Count,
    )
    .await
    .expect("Spark count validation should succeed");
    assert_eq!(count_result.count, Some(3), "Spark count should match");

    let distinct_result = spark_validate_distinct_with_container(
        &spark_container,
        "test_partition_evolution_delete",
        "id",
    )
    .await
    .expect("Spark distinct validation should succeed");
    assert_eq!(
        distinct_result.distinct_count,
        Some(3),
        "Spark distinct count should match"
    );

    let metadata_result = spark_validate_with_container(
        &spark_container,
        "test_partition_evolution_delete",
        ValidationType::Metadata,
    )
    .await
    .expect("Spark metadata validation should succeed");
    assert!(
        metadata_result.snapshot_count.unwrap_or(0) > 0,
        "Spark should report snapshots"
    );
    assert!(
        metadata_result.file_count.unwrap_or(0) > 0,
        "Spark should report files"
    );
    assert!(
        metadata_result.manifest_count.unwrap_or(0) > 0,
        "Spark should report manifests"
    );
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
        metadata.partition_specs_iter().count() >= 2,
        "Table should have at least 2 partition specs (evolved)"
    );

    let ctx = SessionContext::new();
    let namespace = iceberg::NamespaceIdent::new("default".to_string());

    // Create provider for DML operations
    let provider = IcebergTableProvider::try_new(
        client.clone(),
        namespace.clone(),
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

    assert_eq!(update_result, 3, "Should update 3 rows (id 3, 4, 5)");

    // Reload and verify
    let provider_after =
        IcebergTableProvider::try_new(client.clone(), namespace, "test_partition_evolution_update")
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

    // Spark validation: count + distinct + metadata sanity
    let spark_container = fixture.spark_container_name();
    let count_result = spark_validate_with_container(
        &spark_container,
        "test_partition_evolution_update",
        ValidationType::Count,
    )
    .await
    .expect("Spark count validation should succeed");
    assert_eq!(count_result.count, Some(5), "Spark count should match");

    let distinct_result = spark_validate_distinct_with_container(
        &spark_container,
        "test_partition_evolution_update",
        "id",
    )
    .await
    .expect("Spark distinct validation should succeed");
    assert_eq!(
        distinct_result.distinct_count,
        Some(5),
        "Spark distinct count should match"
    );

    let metadata_result = spark_validate_with_container(
        &spark_container,
        "test_partition_evolution_update",
        ValidationType::Metadata,
    )
    .await
    .expect("Spark metadata validation should succeed");
    assert!(
        metadata_result.snapshot_count.unwrap_or(0) > 0,
        "Spark should report snapshots"
    );
    assert!(
        metadata_result.file_count.unwrap_or(0) > 0,
        "Spark should report files"
    );
    assert!(
        metadata_result.manifest_count.unwrap_or(0) > 0,
        "Spark should report manifests"
    );
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
        metadata.partition_specs_iter().count() >= 2,
        "Table should have at least 2 partition specs (evolved)"
    );

    let ctx = SessionContext::new();
    let namespace = iceberg::NamespaceIdent::new("default".to_string());

    // Create source data for MERGE
    let source_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("amount", DataType::Int32, false),
    ]));

    let source_batch = RecordBatch::try_new(source_schema.clone(), vec![
        Arc::new(Int32Array::from(vec![1, 3, 6])), // ids to update/insert
        Arc::new(StringArray::from(vec!["US", "EU", "EU"])), // regions
        Arc::new(Int32Array::from(vec![150, 350, 600])), // new amounts
    ])
    .unwrap();

    let source_table =
        Arc::new(MemTable::try_new(source_schema, vec![vec![source_batch]]).unwrap());
    ctx.register_table("merge_source", source_table).unwrap();
    let source_df = ctx.table("merge_source").await.unwrap();

    // Create provider for DML operations
    let provider = IcebergTableProvider::try_new(
        client.clone(),
        namespace.clone(),
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
    let provider_after =
        IcebergTableProvider::try_new(client.clone(), namespace, "test_partition_evolution_merge")
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

    // Spark validation: count + distinct + metadata sanity
    let spark_container = fixture.spark_container_name();
    let count_result = spark_validate_with_container(
        &spark_container,
        "test_partition_evolution_merge",
        ValidationType::Count,
    )
    .await
    .expect("Spark count validation should succeed");
    assert_eq!(count_result.count, Some(6), "Spark count should match (5 original + 1 inserted)");

    let distinct_result = spark_validate_distinct_with_container(
        &spark_container,
        "test_partition_evolution_merge",
        "id",
    )
    .await
    .expect("Spark distinct validation should succeed");
    assert_eq!(
        distinct_result.distinct_count,
        Some(6),
        "Spark distinct count should match"
    );

    let metadata_result = spark_validate_with_container(
        &spark_container,
        "test_partition_evolution_merge",
        ValidationType::Metadata,
    )
    .await
    .expect("Spark metadata validation should succeed");
    assert!(
        metadata_result.snapshot_count.unwrap_or(0) > 0,
        "Spark should report snapshots"
    );
    assert!(
        metadata_result.file_count.unwrap_or(0) > 0,
        "Spark should report files"
    );
    assert!(
        metadata_result.manifest_count.unwrap_or(0) > 0,
        "Spark should report manifests"
    );
}

// =============================================================================
// WP1 Rust -> Spark Interop Tests (Unpartitioned Tables)
// =============================================================================

/// Test DELETE with NULL semantics.
///
/// Setup (by Spark provision.py):
/// - Table `test_delete_null_semantics` with 6 rows including NULLs:
///   - (1, 'alpha', 100), (2, 'beta', NULL), (3, NULL, 300),
///   - (4, 'delta', 400), (5, NULL, NULL), (6, 'zeta', 600)
///
/// Test: DELETE rows where name IS NULL (should delete rows 3, 5)
#[tokio::test]
async fn test_crossengine_delete_with_null_semantics() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let ctx = SessionContext::new();
    let namespace = iceberg::NamespaceIdent::new("default".to_string());

    // Create provider for DML operations
    let provider = IcebergTableProvider::try_new(
        client.clone(),
        namespace.clone(),
        "test_delete_null_semantics",
    )
    .await
    .unwrap();

    // Verify initial data (6 rows)
    let initial_df = ctx
        .read_table(Arc::new(provider.clone()))
        .unwrap()
        .select_columns(&["id", "name", "value"])
        .unwrap()
        .sort(vec![col("id").sort(true, true)])
        .unwrap();

    let initial_batches = initial_df.collect().await.unwrap();
    let initial_count: usize = initial_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(initial_count, 6, "Should start with 6 rows");

    // Delete rows where name IS NULL (spans rows 3, 5)
    let deleted_count = provider
        .delete(&ctx.state(), Some(col("name").is_null()))
        .await
        .expect("DELETE with IS NULL should succeed");

    assert_eq!(deleted_count, 2, "Should delete 2 rows where name IS NULL");

    // Reload and verify
    let provider_after = IcebergTableProvider::try_new(
        client.clone(),
        namespace,
        "test_delete_null_semantics",
    )
    .await
    .unwrap();

    let after_df = ctx
        .read_table(Arc::new(provider_after))
        .unwrap()
        .select_columns(&["id", "name", "value"])
        .unwrap()
        .sort(vec![col("id").sort(true, true)])
        .unwrap();

    let after_batches = after_df.collect().await.unwrap();
    let expected_after = [
        "+----+-------+-------+",
        "| id | name  | value |",
        "+----+-------+-------+",
        "| 1  | alpha | 100   |",
        "| 2  | beta  |       |",
        "| 4  | delta | 400   |",
        "| 6  | zeta  | 600   |",
        "+----+-------+-------+",
    ];
    assert_batches_sorted_eq!(expected_after, &after_batches);

    // Spark validation: count + distinct + metadata sanity
    let spark_container = fixture.spark_container_name();
    let count_result = spark_validate_with_container(
        &spark_container,
        "test_delete_null_semantics",
        ValidationType::Count,
    )
    .await
    .expect("Spark count validation should succeed");
    assert_eq!(count_result.count, Some(4), "Spark count should match");

    let distinct_result = spark_validate_distinct_with_container(
        &spark_container,
        "test_delete_null_semantics",
        "id",
    )
    .await
    .expect("Spark distinct validation should succeed");
    assert_eq!(
        distinct_result.distinct_count,
        Some(4),
        "Spark distinct count should match"
    );

    let metadata_result = spark_validate_with_container(
        &spark_container,
        "test_delete_null_semantics",
        ValidationType::Metadata,
    )
    .await
    .expect("Spark metadata validation should succeed");
    assert!(
        metadata_result.snapshot_count.unwrap_or(0) > 0,
        "Spark should report snapshots"
    );
}

// =============================================================================
// WP1 Rust -> Spark Interop Tests (Maintenance Operations)
// =============================================================================

/// Test compaction (binpack) on a Spark-created table.
///
/// Setup (by Spark provision.py):
/// - Table `test_compaction` with 5 rows written in separate commits
///
/// Test: Run binpack compaction via iceberg-datafusion and validate Spark reads.
#[tokio::test]
async fn test_crossengine_compaction_binpack() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let table_ident = TableIdent::from_strs(["default", "test_compaction"]).unwrap();
    let table = client.load_table(&table_ident).await.unwrap();
    let pre_snapshot_count = table.metadata().snapshots().count();

    let mut rewrite_options = RewriteDataFilesOptions::default();
    rewrite_options.min_input_files = 2;

    let options = CompactionOptions::default()
        .with_rewrite_options(rewrite_options)
        .with_strategy(RewriteStrategy::BinPack);

    let result = compact_table(&table, client.clone(), Some(options))
        .await
        .expect("Compaction should succeed");

    assert!(result.has_changes(), "Compaction should rewrite data files");

    let table_after = client.load_table(&table_ident).await.unwrap();
    let post_snapshot_count = table_after.metadata().snapshots().count();
    assert!(
        post_snapshot_count > pre_snapshot_count,
        "Compaction should create a new snapshot"
    );

    let current_snapshot = table_after
        .metadata()
        .current_snapshot()
        .expect("Compaction should leave a current snapshot");
    assert_eq!(
        current_snapshot.summary().operation,
        Operation::Replace,
        "Compaction snapshot should be a replace operation"
    );

    // Spark validation: count + distinct + metadata sanity
    let spark_container = fixture.spark_container_name();
    let count_result = spark_validate_with_container(
        &spark_container,
        "test_compaction",
        ValidationType::Count,
    )
    .await
    .expect("Spark count validation should succeed");
    assert_eq!(count_result.count, Some(5), "Spark count should match");

    let distinct_result = spark_validate_distinct_with_container(
        &spark_container,
        "test_compaction",
        "id",
    )
    .await
    .expect("Spark distinct validation should succeed");
    assert_eq!(
        distinct_result.distinct_count,
        Some(5),
        "Spark distinct count should match"
    );

    let metadata_result = spark_validate_with_container(
        &spark_container,
        "test_compaction",
        ValidationType::Metadata,
    )
    .await
    .expect("Spark metadata validation should succeed");
    assert!(
        metadata_result.snapshot_count.unwrap_or(0) > 0,
        "Spark should report snapshots"
    );
    assert!(
        metadata_result.file_count.unwrap_or(0) > 0,
        "Spark should report files"
    );
    assert!(
        metadata_result.manifest_count.unwrap_or(0) > 0,
        "Spark should report manifests"
    );
}

/// Test rewrite manifests on a Spark-created table.
///
/// Setup (by Spark provision.py):
/// - Table `test_rewrite_manifests` with 3 rows across multiple commits
///
/// Test: Run rewrite_manifests and validate Spark reads.
#[tokio::test]
async fn test_crossengine_rewrite_manifests() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let table_ident = TableIdent::from_strs(["default", "test_rewrite_manifests"]).unwrap();
    let table = client.load_table(&table_ident).await.unwrap();
    let pre_snapshot_count = table.metadata().snapshots().count();

    let tx = Transaction::new(&table);
    let action = tx.rewrite_manifests().rewrite_if_smaller_than(1024 * 1024 * 1024);
    let tx = action.apply(tx).unwrap();
    let table_after = tx
        .commit(client.as_ref())
        .await
        .expect("Rewrite manifests should succeed");

    let post_snapshot_count = table_after.metadata().snapshots().count();
    assert!(
        post_snapshot_count > pre_snapshot_count,
        "Rewrite manifests should create a new snapshot"
    );

    let current_snapshot = table_after
        .metadata()
        .current_snapshot()
        .expect("Rewrite manifests should leave a current snapshot");
    assert_eq!(
        current_snapshot.summary().operation,
        Operation::Replace,
        "Rewrite manifests snapshot should be a replace operation"
    );

    // Spark validation: count + distinct + metadata sanity
    let spark_container = fixture.spark_container_name();
    let count_result = spark_validate_with_container(
        &spark_container,
        "test_rewrite_manifests",
        ValidationType::Count,
    )
    .await
    .expect("Spark count validation should succeed");
    assert_eq!(count_result.count, Some(3), "Spark count should match");

    let distinct_result = spark_validate_distinct_with_container(
        &spark_container,
        "test_rewrite_manifests",
        "id",
    )
    .await
    .expect("Spark distinct validation should succeed");
    assert_eq!(
        distinct_result.distinct_count,
        Some(3),
        "Spark distinct count should match"
    );

    let metadata_result = spark_validate_with_container(
        &spark_container,
        "test_rewrite_manifests",
        ValidationType::Metadata,
    )
    .await
    .expect("Spark metadata validation should succeed");
    assert!(
        metadata_result.snapshot_count.unwrap_or(0) > 0,
        "Spark should report snapshots"
    );
    assert!(
        metadata_result.file_count.unwrap_or(0) > 0,
        "Spark should report files"
    );
    assert!(
        metadata_result.manifest_count.unwrap_or(0) > 0,
        "Spark should report manifests"
    );
}

/// Test expire snapshots on a Spark-created table.
///
/// Setup (by Spark provision.py):
/// - Table `test_expire_snapshots` with 4 snapshots
///
/// Test: Expire snapshots using table properties and validate Spark reads.
#[tokio::test]
async fn test_crossengine_expire_snapshots() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let table_ident = TableIdent::from_strs(["default", "test_expire_snapshots"]).unwrap();
    let table = client.load_table(&table_ident).await.unwrap();

    let table = table
        .update_properties()
        .set("history.expire.max-snapshot-age-ms", "1")
        .set("history.expire.min-snapshots-to-keep", "1")
        .commit(client.as_ref())
        .await
        .expect("Updating snapshot retention properties should succeed");

    let pre_snapshot_count = table.metadata().snapshots().count();

    let tx = Transaction::new(&table);
    let action = tx.expire_snapshots().use_table_properties(true);
    let tx = action.apply(tx).unwrap();
    let table_after = tx
        .commit(client.as_ref())
        .await
        .expect("Expire snapshots should succeed");

    let post_snapshot_count = table_after.metadata().snapshots().count();
    assert!(
        post_snapshot_count < pre_snapshot_count,
        "Expire snapshots should remove older snapshots"
    );

    // Spark validation: count + distinct + metadata sanity
    let spark_container = fixture.spark_container_name();
    let count_result = spark_validate_with_container(
        &spark_container,
        "test_expire_snapshots",
        ValidationType::Count,
    )
    .await
    .expect("Spark count validation should succeed");
    assert_eq!(count_result.count, Some(4), "Spark count should match");

    let distinct_result = spark_validate_distinct_with_container(
        &spark_container,
        "test_expire_snapshots",
        "id",
    )
    .await
    .expect("Spark distinct validation should succeed");
    assert_eq!(
        distinct_result.distinct_count,
        Some(4),
        "Spark distinct count should match"
    );

    let metadata_result = spark_validate_with_container(
        &spark_container,
        "test_expire_snapshots",
        ValidationType::Metadata,
    )
    .await
    .expect("Spark metadata validation should succeed");
    assert!(
        metadata_result.snapshot_count.unwrap_or(0) > 0,
        "Spark should report snapshots"
    );
    assert!(
        metadata_result.file_count.unwrap_or(0) > 0,
        "Spark should report files"
    );
    assert!(
        metadata_result.manifest_count.unwrap_or(0) > 0,
        "Spark should report manifests"
    );
}

/// Test remove orphan files on a Spark-created table.
///
/// Setup (by Spark provision.py):
/// - Table `test_remove_orphan_files` with 1 row
///
/// Test: Create an orphan file, delete it with Rust, and validate Spark reads.
#[tokio::test]
async fn test_crossengine_remove_orphan_files() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let table_ident = TableIdent::from_strs(["default", "test_remove_orphan_files"]).unwrap();
    let table = client.load_table(&table_ident).await.unwrap();

    let table_location = table.metadata().location().trim_end_matches('/').to_string();
    let orphan_path = format!(
        "{}/data/orphan-{}.parquet",
        table_location,
        Uuid::new_v4()
    );

    let orphan_file = table.file_io().new_output(&orphan_path).unwrap();
    orphan_file
        .write(Bytes::from_static(b"orphan-data"))
        .await
        .expect("Writing orphan file should succeed");

    let exists_before = table.file_io().exists(&orphan_path).await.unwrap();
    assert!(exists_before, "Orphan file should exist before cleanup");

    let result = table
        .remove_orphan_files()
        .older_than(Utc::now() + Duration::days(1))
        .execute()
        .await
        .expect("Remove orphan files should succeed");

    assert!(
        result.orphan_files.iter().any(|f| f.path == orphan_path),
        "Orphan file should be detected"
    );
    assert!(
        result.total_deleted_files() > 0,
        "Remove orphan files should delete at least one file"
    );

    let exists_after = table.file_io().exists(&orphan_path).await.unwrap();
    assert!(!exists_after, "Orphan file should be removed");

    // Spark validation: count + distinct + metadata sanity
    let spark_container = fixture.spark_container_name();
    let count_result = spark_validate_with_container(
        &spark_container,
        "test_remove_orphan_files",
        ValidationType::Count,
    )
    .await
    .expect("Spark count validation should succeed");
    assert_eq!(count_result.count, Some(1), "Spark count should match");

    let distinct_result = spark_validate_distinct_with_container(
        &spark_container,
        "test_remove_orphan_files",
        "id",
    )
    .await
    .expect("Spark distinct validation should succeed");
    assert_eq!(
        distinct_result.distinct_count,
        Some(1),
        "Spark distinct count should match"
    );

    let metadata_result = spark_validate_with_container(
        &spark_container,
        "test_remove_orphan_files",
        ValidationType::Metadata,
    )
    .await
    .expect("Spark metadata validation should succeed");
    assert!(
        metadata_result.snapshot_count.unwrap_or(0) > 0,
        "Spark should report snapshots"
    );
    assert!(
        metadata_result.file_count.unwrap_or(0) > 0,
        "Spark should report files"
    );
    assert!(
        metadata_result.manifest_count.unwrap_or(0) > 0,
        "Spark should report manifests"
    );
}
