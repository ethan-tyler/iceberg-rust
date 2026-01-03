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
use iceberg::spec::{DataContentType, DataFile, DataFileFormat, ManifestStatus, Operation};
use iceberg::transaction::{
    ApplyTransactionAction, RewriteDataFilesOptions, RewriteStrategy, Transaction,
};
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_datafusion::IcebergTableProvider;
use iceberg_datafusion::compaction::{CompactionOptions, compact_table};
use iceberg_integration_tests::spark_validator::{
    ValidationType, spark_entries_table_with_container, spark_partitions_table_with_container,
    spark_validate_distinct_with_container, spark_validate_query_with_container,
    spark_validate_with_container,
};
use uuid::Uuid;

use crate::get_shared_containers;

async fn assert_spark_full_validation(spark_container: &str, table: &str, expected_count: i64) {
    let full_result = spark_validate_with_container(spark_container, table, ValidationType::Full)
        .await
        .expect("Spark full validation should succeed");
    assert_eq!(
        full_result.count,
        Some(expected_count),
        "Spark full count should match"
    );
    assert!(
        full_result.checksum.is_some(),
        "Spark checksum should be computed"
    );
    assert!(
        full_result.bounds.is_some(),
        "Spark bounds should be computed"
    );
}

async fn write_unpartitioned_data_files(
    table: &Table,
    file_prefix: &str,
    batches: Vec<arrow_array::RecordBatch>,
) -> Vec<DataFile> {
    use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use iceberg::writer::file_writer::ParquetWriterBuilder;
    use iceberg::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
    use parquet::file::properties::WriterProperties;

    let mut data_files = Vec::new();

    for (index, batch) in batches.into_iter().enumerate() {
        let location_generator =
            DefaultLocationGenerator::new(table.metadata().clone()).expect("location generator");
        let file_name_generator = DefaultFileNameGenerator::new(
            format!("{file_prefix}-{index}"),
            None,
            DataFileFormat::Parquet,
        );
        let parquet_writer_builder = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            table.metadata().current_schema().clone(),
        );
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            table.file_io().clone(),
            location_generator,
            file_name_generator,
        );
        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

        let mut writer = data_file_writer_builder
            .build(None)
            .await
            .expect("data file writer");
        writer.write(batch).await.expect("write data batch");
        data_files.extend(writer.close().await.expect("close data file writer"));
    }

    data_files
}

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

    assert_spark_full_validation(&spark_container, "test_partition_evolution_delete", 3).await;
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

    assert_spark_full_validation(&spark_container, "test_partition_evolution_update", 5).await;
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
    assert_eq!(
        count_result.count,
        Some(6),
        "Spark count should match (5 original + 1 inserted)"
    );

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

    assert_spark_full_validation(&spark_container, "test_partition_evolution_merge", 6).await;
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
    let provider_after =
        IcebergTableProvider::try_new(client.clone(), namespace, "test_delete_null_semantics")
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

    assert_spark_full_validation(&spark_container, "test_delete_null_semantics", 4).await;
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
    let count_result =
        spark_validate_with_container(&spark_container, "test_compaction", ValidationType::Count)
            .await
            .expect("Spark count validation should succeed");
    assert_eq!(count_result.count, Some(5), "Spark count should match");

    let distinct_result =
        spark_validate_distinct_with_container(&spark_container, "test_compaction", "id")
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

    assert_spark_full_validation(&spark_container, "test_compaction", 5).await;
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
    let action = tx
        .rewrite_manifests()
        .rewrite_if_smaller_than(1024 * 1024 * 1024);
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

    let distinct_result =
        spark_validate_distinct_with_container(&spark_container, "test_rewrite_manifests", "id")
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

    let full_result = spark_validate_with_container(
        &spark_container,
        "test_rewrite_manifests",
        ValidationType::Full,
    )
    .await
    .expect("Spark full validation should succeed");
    assert_eq!(full_result.count, Some(3), "Spark full count should match");
    assert!(
        full_result.checksum.is_some(),
        "Spark checksum should be computed"
    );
    assert!(
        full_result.bounds.is_some(),
        "Spark bounds should be computed"
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

    let distinct_result =
        spark_validate_distinct_with_container(&spark_container, "test_expire_snapshots", "id")
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

    assert_spark_full_validation(&spark_container, "test_expire_snapshots", 4).await;
}

/// Test expire snapshots with file deletion.
///
/// Setup (by Spark provision.py):
/// - Table `test_expire_snapshots_file_deletion` with 4 snapshots
///
/// Test:
/// 1. Plan cleanup to see what files would be deleted (dry-run equivalent)
/// 2. Commit expire snapshots
/// 3. Execute file cleanup
/// 4. Verify files were deleted
/// 5. Validate Spark can still read the retained data
#[tokio::test]
async fn test_crossengine_expire_snapshots_with_file_deletion() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let table_ident =
        TableIdent::from_strs(["default", "test_expire_snapshots_file_deletion"]).unwrap();
    let table = client.load_table(&table_ident).await.unwrap();

    // Configure aggressive expiration (keep only 1 snapshot)
    let table = table
        .update_properties()
        .set("history.expire.max-snapshot-age-ms", "1")
        .set("history.expire.min-snapshots-to-keep", "1")
        .commit(client.as_ref())
        .await
        .expect("Updating snapshot retention properties should succeed");

    let pre_snapshot_count = table.metadata().snapshots().count();
    assert!(
        pre_snapshot_count >= 2,
        "Table should have at least 2 snapshots for meaningful expiration test"
    );

    // Create expire action with file deletion enabled
    let tx = Transaction::new(&table);
    let action = tx
        .expire_snapshots()
        .use_table_properties(true)
        .delete_files(true);

    // Plan cleanup BEFORE committing (this is the dry-run equivalent)
    let cleanup_plan = action
        .plan_cleanup(&table)
        .await
        .expect("Planning cleanup should succeed");

    // The plan should identify files to delete (unless all snapshots share files)
    // Note: This may be empty if the remaining snapshot references all files
    println!(
        "Cleanup plan: {} manifest lists, {} manifests, {} data files",
        cleanup_plan.manifest_list_files.len(),
        cleanup_plan.manifest_files.len(),
        cleanup_plan.data_files.len()
    );

    // Commit metadata changes
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

    // Execute file cleanup AFTER successful commit
    let cleanup_result = iceberg::transaction::execute_cleanup_plan(
        table_after.file_io(),
        &cleanup_plan,
        None, // No progress callback
    )
    .await
    .expect("Executing cleanup should succeed");

    println!(
        "Cleanup result: deleted {} manifest lists, {} manifests, {} data files, {} position delete files, {} equality delete files",
        cleanup_result.deleted_manifest_list_files,
        cleanup_result.deleted_manifest_files,
        cleanup_result.deleted_data_files,
        cleanup_result.deleted_position_delete_files,
        cleanup_result.deleted_equality_delete_files
    );

    // Verify at least the manifest lists were deleted (they're always orphaned after expiration)
    // Note: Data files may still be referenced by the retained snapshot
    if !cleanup_plan.manifest_list_files.is_empty() {
        for manifest_list_path in &cleanup_plan.manifest_list_files {
            let exists = table_after
                .file_io()
                .exists(manifest_list_path)
                .await
                .unwrap_or(true);
            assert!(
                !exists,
                "Manifest list {manifest_list_path} should have been deleted"
            );
        }
    }

    // Spark validation: count + distinct + metadata sanity
    let spark_container = fixture.spark_container_name();
    let count_result = spark_validate_with_container(
        &spark_container,
        "test_expire_snapshots_file_deletion",
        ValidationType::Count,
    )
    .await
    .expect("Spark count validation should succeed after file deletion");
    assert_eq!(
        count_result.count,
        Some(4),
        "Spark count should match (all data retained)"
    );

    let distinct_result = spark_validate_distinct_with_container(
        &spark_container,
        "test_expire_snapshots_file_deletion",
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
        "test_expire_snapshots_file_deletion",
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

    assert_spark_full_validation(&spark_container, "test_expire_snapshots_file_deletion", 4).await;
}

/// Test expire snapshots dry-run fidelity (no changes or deletions).
///
/// Setup (by Spark provision.py):
/// - Table `test_expire_snapshots_dry_run` with 4 snapshots
///
/// Test:
/// 1. Plan expiration + cleanup
/// 2. Execute dry-run expiration
/// 3. Verify snapshot count unchanged and cleanup plan files still exist
#[tokio::test]
async fn test_crossengine_expire_snapshots_dry_run_fidelity() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let table_ident = TableIdent::from_strs(["default", "test_expire_snapshots_dry_run"]).unwrap();
    let table = client.load_table(&table_ident).await.unwrap();

    let table = table
        .update_properties()
        .set("history.expire.max-snapshot-age-ms", "1")
        .set("history.expire.min-snapshots-to-keep", "1")
        .commit(client.as_ref())
        .await
        .expect("Updating snapshot retention properties should succeed");

    let pre_snapshot_count = table.metadata().snapshots().count();

    let action = Transaction::new(&table)
        .expire_snapshots()
        .use_table_properties(true)
        .delete_files(true)
        .dry_run(true);

    let plan = action
        .plan(&table)
        .await
        .expect("Planning expiration should succeed");
    assert!(
        !plan.expired_snapshot_ids().is_empty(),
        "Dry-run test requires at least one expired snapshot"
    );

    let result = action
        .execute(&table, client.as_ref())
        .await
        .expect("Dry-run expiration should succeed");

    let table_after = client.load_table(&table_ident).await.unwrap();
    let post_snapshot_count = table_after.metadata().snapshots().count();
    assert_eq!(
        post_snapshot_count, pre_snapshot_count,
        "Dry-run should not expire snapshots"
    );

    let cleanup_plan = plan.cleanup_plan();
    assert_eq!(
        result.deleted_snapshots_count,
        plan.expired_snapshot_ids().len() as u64,
        "Dry-run snapshot count should match plan"
    );
    assert_eq!(
        result.deleted_refs_count,
        plan.expired_ref_names().len() as u64,
        "Dry-run ref count should match plan"
    );
    assert_eq!(
        result.deleted_manifest_list_files_count,
        cleanup_plan.manifest_list_files.len() as u64,
        "Dry-run manifest list count should match plan"
    );
    assert_eq!(
        result.deleted_manifest_files_count,
        cleanup_plan.manifest_files.len() as u64,
        "Dry-run manifest count should match plan"
    );
    assert_eq!(
        result.deleted_data_files_count,
        cleanup_plan.data_files.len() as u64,
        "Dry-run data file count should match plan"
    );
    assert_eq!(
        result.deleted_position_delete_files_count,
        cleanup_plan.position_delete_files.len() as u64,
        "Dry-run position delete count should match plan"
    );
    assert_eq!(
        result.deleted_equality_delete_files_count,
        cleanup_plan.equality_delete_files.len() as u64,
        "Dry-run equality delete count should match plan"
    );

    for path in cleanup_plan
        .manifest_list_files
        .iter()
        .chain(cleanup_plan.manifest_files.iter())
        .chain(cleanup_plan.data_files.iter())
        .chain(cleanup_plan.position_delete_files.iter())
        .chain(cleanup_plan.equality_delete_files.iter())
    {
        let exists = table_after.file_io().exists(path).await.unwrap_or(true);
        assert!(exists, "Dry-run should not delete file {path}");
    }
}

/// Test expire snapshots respects branch/tag ref safety.
///
/// Setup (by Spark provision.py):
/// - Table `test_expire_snapshots_ref_safety` with 4 snapshots
///
/// Test:
/// 1. Create a tag on an older snapshot
/// 2. Expire snapshots using table properties
/// 3. Verify the tagged snapshot remains
#[tokio::test]
async fn test_crossengine_expire_snapshots_ref_safety() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let table_ident =
        TableIdent::from_strs(["default", "test_expire_snapshots_ref_safety"]).unwrap();
    let table = client.load_table(&table_ident).await.unwrap();

    let metadata = table.metadata();
    let current_snapshot_id = metadata
        .current_snapshot_id()
        .expect("Table should have a current snapshot");
    let protected_snapshot_id = metadata
        .snapshots()
        .filter(|snapshot| snapshot.snapshot_id() != current_snapshot_id)
        .min_by_key(|snapshot| snapshot.timestamp_ms())
        .map(|snapshot| snapshot.snapshot_id())
        .expect("Table should have a non-current snapshot to protect");

    let tag_name = format!("retain-{}", Uuid::new_v4());
    let table = table
        .create_tag(&tag_name)
        .from_snapshot(protected_snapshot_id)
        .commit(client.as_ref())
        .await
        .expect("Creating tag should succeed");

    let table = table
        .update_properties()
        .set("history.expire.max-snapshot-age-ms", "1")
        .set("history.expire.min-snapshots-to-keep", "1")
        .commit(client.as_ref())
        .await
        .expect("Updating snapshot retention properties should succeed");

    let action = Transaction::new(&table)
        .expire_snapshots()
        .use_table_properties(true);
    let result = action
        .execute(&table, client.as_ref())
        .await
        .expect("Expire snapshots should succeed");

    assert!(
        result.deleted_snapshots_count > 0,
        "Expire snapshots should remove older snapshots"
    );
    assert_eq!(
        result.deleted_refs_count, 0,
        "Expire snapshots should not delete protected refs"
    );

    let table_after = client.load_table(&table_ident).await.unwrap();
    let tag_ref = table_after
        .metadata()
        .refs()
        .get(&tag_name)
        .expect("Tag should still exist after expiration");
    assert_eq!(
        tag_ref.snapshot_id, protected_snapshot_id,
        "Tag should still reference the protected snapshot"
    );
    assert!(
        table_after
            .metadata()
            .snapshot_by_id(protected_snapshot_id)
            .is_some(),
        "Protected snapshot should remain"
    );

    let spark_container = fixture.spark_container_name();
    assert_spark_full_validation(&spark_container, "test_expire_snapshots_ref_safety", 4).await;
}

/// Test remove orphan files dry-run fidelity.
///
/// Setup (by Spark provision.py):
/// - Table `test_remove_orphan_files_dry_run` with 1 row
///
/// Test:
/// 1. Create an orphan file under a dedicated subdirectory
/// 2. Run dry-run and capture orphan list
/// 3. Execute delete and verify the same files are removed
#[tokio::test]
async fn test_crossengine_remove_orphan_files_dry_run_fidelity() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let table_ident =
        TableIdent::from_strs(["default", "test_remove_orphan_files_dry_run"]).unwrap();
    let table = client.load_table(&table_ident).await.unwrap();

    let table_location = table
        .metadata()
        .location()
        .trim_end_matches('/')
        .to_string();
    let orphan_dir = format!("{}/data/orphans-{}", table_location, Uuid::new_v4());
    let orphan_path = format!("{}/orphan-{}.parquet", orphan_dir, Uuid::new_v4());

    let orphan_file = table.file_io().new_output(&orphan_path).unwrap();
    orphan_file
        .write(Bytes::from_static(b"orphan-data"))
        .await
        .expect("Writing orphan file should succeed");

    let dry_run_result = table
        .remove_orphan_files()
        .location(orphan_dir.clone())
        .older_than(Utc::now() + Duration::days(1))
        .dry_run(true)
        .execute()
        .await
        .expect("Dry-run remove orphan files should succeed");

    assert!(
        dry_run_result.dry_run,
        "Dry-run should be reported in result"
    );
    assert!(
        dry_run_result
            .orphan_files
            .iter()
            .any(|f| f.path == orphan_path),
        "Dry-run should report the orphan file"
    );
    assert!(
        dry_run_result.total_deleted_files() > 0,
        "Dry-run should report deletable files"
    );

    let exists_after_dry_run = table.file_io().exists(&orphan_path).await.unwrap();
    assert!(exists_after_dry_run, "Dry-run should not delete files");

    let delete_result = table
        .remove_orphan_files()
        .location(orphan_dir.clone())
        .older_than(Utc::now() + Duration::days(1))
        .dry_run(false)
        .execute()
        .await
        .expect("Remove orphan files should succeed");

    assert!(
        !delete_result.dry_run,
        "Delete run should not be marked dry-run"
    );

    let mut dry_paths: Vec<String> = dry_run_result
        .orphan_files
        .iter()
        .map(|f| f.path.clone())
        .collect();
    let mut delete_paths: Vec<String> = delete_result
        .orphan_files
        .iter()
        .map(|f| f.path.clone())
        .collect();
    dry_paths.sort();
    delete_paths.sort();
    assert_eq!(
        dry_paths, delete_paths,
        "Dry-run and delete runs should report the same orphan files"
    );

    let exists_after_delete = table.file_io().exists(&orphan_path).await.unwrap();
    assert!(!exists_after_delete, "Orphan file should be removed");
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

    let table_location = table
        .metadata()
        .location()
        .trim_end_matches('/')
        .to_string();
    let orphan_path = format!("{}/data/orphan-{}.parquet", table_location, Uuid::new_v4());

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

    let distinct_result =
        spark_validate_distinct_with_container(&spark_container, "test_remove_orphan_files", "id")
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

    assert_spark_full_validation(&spark_container, "test_remove_orphan_files", 1).await;
}

// =============================================================================
// WP3.1 Equality Delete Interop Tests (Spark -> Rust)
// =============================================================================
// These tests verify that iceberg-rust can correctly read tables with
// equality delete files written by Spark (via Iceberg Java API).
//
// Key acceptance criteria (from the plan):
// - Covers at least two data types (int + string)
// - Covers NULL cases in delete key columns
// - Covers multi-column equality keys
// - Validations include at least 2 of: count(*), count(distinct key), checksum

/// Test reading a table with equality deletes on an integer column.
///
/// Setup (by Spark provision.py):
/// - Table `test_equality_delete_int` with 10 rows (id 1-10)
/// - Equality delete file removes rows where id IN (3, 5, 7)
///
/// Expected: 7 rows remaining (ids: 1, 2, 4, 6, 8, 9, 10)
#[tokio::test]
async fn test_crossengine_equality_delete_int_key() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let ctx = SessionContext::new();
    let namespace = iceberg::NamespaceIdent::new("default".to_string());

    // Create provider and read the table
    let provider = IcebergTableProvider::try_new(
        client.clone(),
        namespace.clone(),
        "test_equality_delete_int",
    )
    .await
    .unwrap();

    // Query the table - equality deletes should be applied automatically
    let df = ctx
        .read_table(Arc::new(provider))
        .unwrap()
        .select_columns(&["id", "name", "value"])
        .unwrap()
        .sort(vec![col("id").sort(true, true)])
        .unwrap();

    let batches = df.collect().await.unwrap();
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

    // Verify Rust correctly applies equality deletes
    assert_eq!(
        row_count, 7,
        "Should have 7 rows after equality delete (deleted ids 3, 5, 7)"
    );

    // Verify specific rows are present (the ones NOT deleted)
    let expected = [
        "+----+-------+-------+",
        "| id | name  | value |",
        "+----+-------+-------+",
        "| 1  | alpha | 100   |",
        "| 2  | beta  | 200   |",
        "| 4  | delta | 400   |",
        "| 6  | zeta  | 600   |",
        "| 8  | theta | 800   |",
        "| 9  | iota  | 900   |",
        "| 10 | kappa | 1000  |",
        "+----+-------+-------+",
    ];
    assert_batches_sorted_eq!(expected, &batches);

    // Cross-validate with Spark: count should match
    let spark_container = fixture.spark_container_name();
    let count_result = spark_validate_with_container(
        &spark_container,
        "test_equality_delete_int",
        ValidationType::Count,
    )
    .await
    .expect("Spark count validation should succeed");
    assert_eq!(
        count_result.count,
        Some(7),
        "Spark count should match Rust count"
    );

    // Cross-validate with Spark: distinct count should match
    let distinct_result =
        spark_validate_distinct_with_container(&spark_container, "test_equality_delete_int", "id")
            .await
            .expect("Spark distinct validation should succeed");
    assert_eq!(
        distinct_result.distinct_count,
        Some(7),
        "Spark distinct count should match"
    );

    // Metadata sanity check
    let metadata_result = spark_validate_with_container(
        &spark_container,
        "test_equality_delete_int",
        ValidationType::Metadata,
    )
    .await
    .expect("Spark metadata validation should succeed");
    assert!(
        metadata_result.snapshot_count.unwrap_or(0) > 0,
        "Spark should report snapshots"
    );

    assert_spark_full_validation(&spark_container, "test_equality_delete_int", 7).await;
}

/// Test reading a table with equality deletes on a string column.
///
/// Setup (by Spark provision.py):
/// - Table `test_equality_delete_string` with 6 rows
/// - Equality delete file removes rows where name IN ('banana', 'date', 'fig')
///
/// Expected: 3 rows remaining (apple, cherry, elderberry)
#[tokio::test]
async fn test_crossengine_equality_delete_string_key() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let ctx = SessionContext::new();
    let namespace = iceberg::NamespaceIdent::new("default".to_string());

    // Create provider and read the table
    let provider = IcebergTableProvider::try_new(
        client.clone(),
        namespace.clone(),
        "test_equality_delete_string",
    )
    .await
    .unwrap();

    // Query the table - equality deletes should be applied automatically
    let df = ctx
        .read_table(Arc::new(provider))
        .unwrap()
        .select_columns(&["id", "name", "value"])
        .unwrap()
        .sort(vec![col("id").sort(true, true)])
        .unwrap();

    let batches = df.collect().await.unwrap();
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

    // Verify Rust correctly applies equality deletes
    assert_eq!(
        row_count, 3,
        "Should have 3 rows after equality delete (deleted banana, date, fig)"
    );

    // Verify specific rows are present (the ones NOT deleted)
    let expected = [
        "+----+------------+-------+",
        "| id | name       | value |",
        "+----+------------+-------+",
        "| 1  | apple      | 10    |",
        "| 3  | cherry     | 30    |",
        "| 5  | elderberry | 50    |",
        "+----+------------+-------+",
    ];
    assert_batches_sorted_eq!(expected, &batches);

    // Cross-validate with Spark: count should match
    let spark_container = fixture.spark_container_name();
    let count_result = spark_validate_with_container(
        &spark_container,
        "test_equality_delete_string",
        ValidationType::Count,
    )
    .await
    .expect("Spark count validation should succeed");
    assert_eq!(
        count_result.count,
        Some(3),
        "Spark count should match Rust count"
    );

    // Cross-validate with Spark: distinct count should match
    let distinct_result = spark_validate_distinct_with_container(
        &spark_container,
        "test_equality_delete_string",
        "name",
    )
    .await
    .expect("Spark distinct validation should succeed");
    assert_eq!(
        distinct_result.distinct_count,
        Some(3),
        "Spark distinct count should match"
    );

    assert_spark_full_validation(&spark_container, "test_equality_delete_string", 3).await;
}

/// Test reading a table with equality deletes that target NULL values.
///
/// Setup (by Spark provision.py):
/// - Table `test_equality_delete_null` with 6 rows:
///   - Row 1: (1, 'alpha', 100)
///   - Row 2: (2, NULL, 200)
///   - Row 3: (3, 'gamma', 300)
///   - Row 4: (4, NULL, 400)
///   - Row 5: (5, 'epsilon', NULL)
///   - Row 6: (6, NULL, NULL)
/// - Equality delete file removes rows where name IS NULL
///
/// Expected: 3 rows remaining (ids: 1, 3, 5)
/// Per Iceberg spec, NULL = NULL for equality delete matching.
#[tokio::test]
async fn test_crossengine_equality_delete_null_handling() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let ctx = SessionContext::new();
    let namespace = iceberg::NamespaceIdent::new("default".to_string());

    // Create provider and read the table
    let provider = IcebergTableProvider::try_new(
        client.clone(),
        namespace.clone(),
        "test_equality_delete_null",
    )
    .await
    .unwrap();

    // Query the table - equality deletes should be applied automatically
    let df = ctx
        .read_table(Arc::new(provider))
        .unwrap()
        .select_columns(&["id", "name", "value"])
        .unwrap()
        .sort(vec![col("id").sort(true, true)])
        .unwrap();

    let batches = df.collect().await.unwrap();
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

    // Verify Rust correctly applies equality deletes with NULL handling
    assert_eq!(
        row_count, 3,
        "Should have 3 rows after equality delete (deleted rows where name IS NULL)"
    );

    // Verify specific rows are present (the ones NOT deleted)
    // Note: Row 5 has value=NULL but name='epsilon' so it's NOT deleted
    let expected = [
        "+----+---------+-------+",
        "| id | name    | value |",
        "+----+---------+-------+",
        "| 1  | alpha   | 100   |",
        "| 3  | gamma   | 300   |",
        "| 5  | epsilon |       |",
        "+----+---------+-------+",
    ];
    assert_batches_sorted_eq!(expected, &batches);

    // Cross-validate with Spark: count should match
    let spark_container = fixture.spark_container_name();
    let count_result = spark_validate_with_container(
        &spark_container,
        "test_equality_delete_null",
        ValidationType::Count,
    )
    .await
    .expect("Spark count validation should succeed");
    assert_eq!(
        count_result.count,
        Some(3),
        "Spark count should match Rust count (NULL handling must be identical)"
    );

    // Cross-validate with Spark: distinct count should match
    let distinct_result =
        spark_validate_distinct_with_container(&spark_container, "test_equality_delete_null", "id")
            .await
            .expect("Spark distinct validation should succeed");
    assert_eq!(
        distinct_result.distinct_count,
        Some(3),
        "Spark distinct count should match"
    );

    assert_spark_full_validation(&spark_container, "test_equality_delete_null", 3).await;
}

/// Test reading a table with multi-column equality deletes.
///
/// Setup (by Spark provision.py):
/// - Table `test_equality_delete_multi` with 8 rows:
///   - (1, 'electronics', 'US', 100)
///   - (2, 'electronics', 'EU', 200)
///   - (3, 'books', 'US', 300)
///   - (4, 'books', 'EU', 400)
///   - (5, 'electronics', 'US', 500)
///   - (6, 'clothing', 'APAC', 600)
///   - (7, 'books', 'US', 700)
///   - (8, 'electronics', 'EU', 800)
/// - Equality delete file removes rows where (category, region) matches:
///   - ('electronics', 'US') -> deletes rows 1, 5
///   - ('books', 'EU') -> deletes row 4
///
/// Expected: 5 rows remaining (ids: 2, 3, 6, 7, 8)
#[tokio::test]
async fn test_crossengine_equality_delete_multi_column_key() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let ctx = SessionContext::new();
    let namespace = iceberg::NamespaceIdent::new("default".to_string());

    // Create provider and read the table
    let provider = IcebergTableProvider::try_new(
        client.clone(),
        namespace.clone(),
        "test_equality_delete_multi",
    )
    .await
    .unwrap();

    // Query the table - equality deletes should be applied automatically
    let df = ctx
        .read_table(Arc::new(provider))
        .unwrap()
        .select_columns(&["id", "category", "region", "value"])
        .unwrap()
        .sort(vec![col("id").sort(true, true)])
        .unwrap();

    let batches = df.collect().await.unwrap();
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

    // Verify Rust correctly applies multi-column equality deletes
    assert_eq!(
        row_count, 5,
        "Should have 5 rows after multi-column equality delete"
    );

    // Verify specific rows are present (the ones NOT deleted)
    let expected = [
        "+----+-------------+--------+-------+",
        "| id | category    | region | value |",
        "+----+-------------+--------+-------+",
        "| 2  | electronics | EU     | 200   |",
        "| 3  | books       | US     | 300   |",
        "| 6  | clothing    | APAC   | 600   |",
        "| 7  | books       | US     | 700   |",
        "| 8  | electronics | EU     | 800   |",
        "+----+-------------+--------+-------+",
    ];
    assert_batches_sorted_eq!(expected, &batches);

    // Cross-validate with Spark: count should match
    let spark_container = fixture.spark_container_name();
    let count_result = spark_validate_with_container(
        &spark_container,
        "test_equality_delete_multi",
        ValidationType::Count,
    )
    .await
    .expect("Spark count validation should succeed");
    assert_eq!(
        count_result.count,
        Some(5),
        "Spark count should match Rust count"
    );

    // Cross-validate with Spark: distinct count on id should match
    let distinct_result = spark_validate_distinct_with_container(
        &spark_container,
        "test_equality_delete_multi",
        "id",
    )
    .await
    .expect("Spark distinct validation should succeed");
    assert_eq!(
        distinct_result.distinct_count,
        Some(5),
        "Spark distinct count should match"
    );

    // Use checksum for additional validation via custom query
    let checksum_result = spark_validate_query_with_container(
        &spark_container,
        "test_equality_delete_multi",
        "SELECT sum(value) as total FROM {table}",
    )
    .await
    .expect("Spark checksum validation should succeed");
    // Expected sum: 200 + 300 + 600 + 700 + 800 = 2600
    let rows = checksum_result.rows.expect("Should have rows");
    let total = rows[0]
        .get("total")
        .and_then(|v| v.as_i64())
        .expect("Should have total");
    assert_eq!(total, 2600, "Value sum should match expected total");

    assert_spark_full_validation(&spark_container, "test_equality_delete_multi", 5).await;
}

#[tokio::test]
async fn test_crossengine_rust_equality_delete_spark_reads() {
    use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
    use iceberg::TableCreation;
    use iceberg::arrow::arrow_schema_to_schema;
    use iceberg::spec::{DataFileFormat, NestedField, PrimitiveType, Schema, Type};
    use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use iceberg::writer::base_writer::equality_delete_writer::{
        EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
    };
    use iceberg::writer::file_writer::ParquetWriterBuilder;
    use iceberg::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
    use parquet::file::properties::WriterProperties;

    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = crate::shared_tests::random_ns().await;
    let table_name = format!("rust_equality_delete_{}", Uuid::new_v4().simple());

    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "value", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .unwrap();

    let table_creation = TableCreation::builder()
        .name(table_name.clone())
        .schema(schema)
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

    let data_batch = RecordBatch::try_new(arrow_schema.clone(), vec![
        Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
        Arc::new(StringArray::from(vec![
            Some("alpha"),
            Some("beta"),
            Some("gamma"),
            Some("delta"),
            Some("epsilon"),
        ])) as ArrayRef,
        Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])) as ArrayRef,
    ])
    .unwrap();

    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(
        format!("rust-eq-data-{}", Uuid::new_v4().simple()),
        None,
        DataFileFormat::Parquet,
    );
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
    );
    let rolling_file_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_file_writer_builder);
    let mut data_file_writer = data_file_writer_builder.build(None).await.unwrap();
    data_file_writer.write(data_batch).await.unwrap();
    let data_files = data_file_writer.close().await.unwrap();

    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(data_files)
        .apply(tx)
        .unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    let equality_id = table
        .metadata()
        .current_schema()
        .field_by_name("id")
        .expect("id field should exist")
        .id;
    let equality_config = EqualityDeleteWriterConfig::new(
        vec![equality_id],
        table.metadata().current_schema().clone(),
    )
    .unwrap();
    let delete_schema =
        arrow_schema_to_schema(equality_config.projected_arrow_schema_ref()).unwrap();
    let delete_writer_builder =
        ParquetWriterBuilder::new(WriterProperties::default(), Arc::new(delete_schema));
    let delete_location_generator =
        DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let delete_file_name_generator = DefaultFileNameGenerator::new(
        format!("rust-eq-delete-{}", Uuid::new_v4().simple()),
        None,
        DataFileFormat::Parquet,
    );
    let rolling_delete_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        delete_writer_builder,
        table.file_io().clone(),
        delete_location_generator,
        delete_file_name_generator,
    );
    let mut delete_writer =
        EqualityDeleteFileWriterBuilder::new(rolling_delete_writer_builder, equality_config)
            .build(None)
            .await
            .unwrap();

    let delete_batch = RecordBatch::try_new(arrow_schema, vec![
        Arc::new(Int32Array::from(vec![2, 4])) as ArrayRef,
        Arc::new(StringArray::from(vec![Some("beta"), Some("delta")])) as ArrayRef,
        Arc::new(Int32Array::from(vec![20, 40])) as ArrayRef,
    ])
    .unwrap();

    delete_writer.write(delete_batch).await.unwrap();
    let delete_files = delete_writer.close().await.unwrap();

    let tx = Transaction::new(&table);
    let tx = tx
        .row_delta()
        .add_equality_delete_files(delete_files)
        .apply(tx)
        .unwrap();
    let _table = tx.commit(&rest_catalog).await.unwrap();

    let table_ref = format!("{}.{}", ns.name(), table_name);
    let spark_container = fixture.spark_container_name();

    let count_result =
        spark_validate_with_container(&spark_container, &table_ref, ValidationType::Count)
            .await
            .expect("Spark count validation should succeed");
    assert_eq!(count_result.count, Some(3), "Spark count should match");

    let distinct_result =
        spark_validate_distinct_with_container(&spark_container, &table_ref, "id")
            .await
            .expect("Spark distinct validation should succeed");
    assert_eq!(
        distinct_result.distinct_count,
        Some(3),
        "Spark distinct count should match"
    );

    let checksum_result = spark_validate_query_with_container(
        &spark_container,
        &table_ref,
        "SELECT sum(value) as total FROM {table}",
    )
    .await
    .expect("Spark checksum validation should succeed");
    let rows = checksum_result.rows.expect("Should have rows");
    let total = rows[0]
        .get("total")
        .and_then(|v| v.as_i64())
        .expect("Should have total");
    assert_eq!(total, 90, "Value sum should match expected total");

    assert_spark_full_validation(&spark_container, &table_ref, 3).await;
}

// =============================================================================
// WP4 INSERT OVERWRITE Interop Tests (Rust -> Spark)
// =============================================================================
// These tests verify that iceberg-rust's INSERT OVERWRITE semantics produce
// tables that Spark can correctly read.
//
// Two modes are tested:
// 1. Dynamic Overwrite (ReplacePartitionsAction): Partitions determined from added files
// 2. Static Overwrite (OverwriteAction): Partitions determined from explicit filter

/// Test Dynamic Overwrite (ReplacePartitions) - replaces only touched partitions.
///
/// Setup (by Spark provision.py):
/// - Table `test_dynamic_overwrite` partitioned by category with 6 rows across 3 partitions
///
/// Test: Replace the 'electronics' partition with new data via Rust,
///       verify untouched partitions (books, clothing) remain intact.
#[tokio::test]
async fn test_crossengine_dynamic_overwrite_partitioned() {
    use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
    use iceberg::spec::{DataFileFormat, Literal, PartitionKey, Struct};
    use iceberg::transaction::Transaction;
    use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use iceberg::writer::file_writer::ParquetWriterBuilder;
    use iceberg::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
    use parquet::file::properties::WriterProperties;

    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let table_ident = TableIdent::from_strs(["default", "test_dynamic_overwrite"]).unwrap();
    let table = client.load_table(&table_ident).await.unwrap();

    // Verify initial state: 6 rows across 3 partitions
    let ctx = SessionContext::new();
    let namespace = iceberg::NamespaceIdent::new("default".to_string());
    let provider =
        IcebergTableProvider::try_new(client.clone(), namespace.clone(), "test_dynamic_overwrite")
            .await
            .unwrap();

    let initial_df = ctx
        .read_table(Arc::new(provider))
        .unwrap()
        .aggregate(vec![], vec![datafusion::functions_aggregate::count::count(
            col("id"),
        )])
        .unwrap();
    let initial_count: i64 = initial_df
        .collect()
        .await
        .unwrap()
        .first()
        .unwrap()
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(initial_count, 6, "Should start with 6 rows");

    // Write new data files for 'electronics' partition
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_generator =
        DefaultFileNameGenerator::new("overwrite".to_string(), None, DataFileFormat::Parquet);

    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::builder().build(),
        table.metadata().current_schema().clone(),
    );
    let rolling_file_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_file_writer_builder);
    let partition_key = PartitionKey::new(
        table.metadata().default_partition_spec().as_ref().clone(),
        table.metadata().current_schema().clone(),
        Struct::from_iter([Some(Literal::string("electronics"))]),
    );
    let mut writer = data_file_writer_builder
        .build(Some(partition_key))
        .await
        .unwrap();

    // Create Arrow schema matching table schema
    let arrow_schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );

    // New data for electronics partition (replacing ids 1, 2 with new values)
    let batch = RecordBatch::try_new(arrow_schema, vec![
        Arc::new(Int32Array::from(vec![10, 20])), // new ids
        Arc::new(StringArray::from(vec!["electronics", "electronics"])),
        Arc::new(Int32Array::from(vec![1000, 2000])), // new values
    ])
    .unwrap();

    writer.write(batch).await.unwrap();
    let data_files: Vec<_> = writer.close().await.unwrap();
    assert!(!data_files.is_empty(), "Should have written data files");

    // Execute ReplacePartitions (Dynamic Overwrite)
    let tx = Transaction::new(&table);
    let action = tx.replace_partitions().add_data_files(data_files);
    let tx = action.apply(tx).unwrap();
    let _table_after = tx
        .commit(client.as_ref())
        .await
        .expect("Dynamic overwrite should succeed");

    // Verify with Rust: only electronics partition replaced
    let provider_after =
        IcebergTableProvider::try_new(client.clone(), namespace.clone(), "test_dynamic_overwrite")
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

    // Expected: new electronics (10, 20) + unchanged books (3, 4) + unchanged clothing (5, 6)
    let expected_after = [
        "+----+-------------+-------+",
        "| id | category    | value |",
        "+----+-------------+-------+",
        "| 3  | books       | 300   |",
        "| 4  | books       | 400   |",
        "| 5  | clothing    | 500   |",
        "| 6  | clothing    | 600   |",
        "| 10 | electronics | 1000  |",
        "| 20 | electronics | 2000  |",
        "+----+-------------+-------+",
    ];
    assert_batches_sorted_eq!(expected_after, &after_batches);

    // Spark validation: count should match (6 rows: 2 new electronics + 2 books + 2 clothing)
    let spark_container = fixture.spark_container_name();
    let count_result = spark_validate_with_container(
        &spark_container,
        "test_dynamic_overwrite",
        ValidationType::Count,
    )
    .await
    .expect("Spark count validation should succeed");
    assert_eq!(
        count_result.count,
        Some(6),
        "Spark count should match after dynamic overwrite"
    );

    // Verify checksum: 1000 + 2000 + 300 + 400 + 500 + 600 = 4800
    let checksum_result = spark_validate_query_with_container(
        &spark_container,
        "test_dynamic_overwrite",
        "SELECT sum(value) as total FROM {table}",
    )
    .await
    .expect("Spark checksum should succeed");
    let rows = checksum_result.rows.expect("Should have rows");
    let total = rows[0]
        .get("total")
        .and_then(|v| v.as_i64())
        .expect("Should have total");
    assert_eq!(total, 4800, "Sum should match expected total");

    assert_spark_full_validation(&spark_container, "test_dynamic_overwrite", 6).await;
}

/// Test Static Overwrite (filter-based) - replaces partitions matching filter.
///
/// Setup (by Spark provision.py):
/// - Table `test_static_overwrite` partitioned by region with 5 rows across 3 partitions
///
/// Test: Overwrite where region = 'US' with new data via Rust,
///       verify EU and APAC partitions remain intact.
#[tokio::test]
async fn test_crossengine_static_overwrite_with_filter() {
    use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
    use iceberg::expr::Reference;
    use iceberg::spec::{DataFileFormat, Datum, Literal, PartitionKey, Struct};
    use iceberg::transaction::Transaction;
    use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use iceberg::writer::file_writer::ParquetWriterBuilder;
    use iceberg::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
    use parquet::file::properties::WriterProperties;

    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let table_ident = TableIdent::from_strs(["default", "test_static_overwrite"]).unwrap();
    let table = client.load_table(&table_ident).await.unwrap();

    // Write new data files for 'US' partition
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(
        "static-overwrite".to_string(),
        None,
        DataFileFormat::Parquet,
    );

    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::builder().build(),
        table.metadata().current_schema().clone(),
    );
    let rolling_file_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_file_writer_builder);
    let partition_key = PartitionKey::new(
        table.metadata().default_partition_spec().as_ref().clone(),
        table.metadata().current_schema().clone(),
        Struct::from_iter([Some(Literal::string("US"))]),
    );
    let mut writer = data_file_writer_builder
        .build(Some(partition_key))
        .await
        .unwrap();

    // Create Arrow schema matching table schema
    let arrow_schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );

    // New data for US partition (replacing ids 1, 2 with new single row)
    let batch = RecordBatch::try_new(arrow_schema, vec![
        Arc::new(Int32Array::from(vec![100])), // new id
        Arc::new(StringArray::from(vec!["US"])),
        Arc::new(Int32Array::from(vec![9999])), // new amount
    ])
    .unwrap();

    writer.write(batch).await.unwrap();
    let data_files: Vec<_> = writer.close().await.unwrap();

    // Execute Static Overwrite with filter
    let filter = Reference::new("region").equal_to(Datum::string("US"));
    let tx = Transaction::new(&table);
    let action = tx
        .overwrite()
        .overwrite_filter(filter)
        .add_data_files(data_files);
    let tx = action.apply(tx).unwrap();
    let _table_after = tx
        .commit(client.as_ref())
        .await
        .expect("Static overwrite should succeed");

    // Verify with Rust: only US partition replaced
    let ctx = SessionContext::new();
    let namespace = iceberg::NamespaceIdent::new("default".to_string());
    let provider_after =
        IcebergTableProvider::try_new(client.clone(), namespace.clone(), "test_static_overwrite")
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

    // Expected: new US (100) + unchanged EU (3, 4) + unchanged APAC (5)
    let expected_after = [
        "+-----+--------+--------+",
        "| id  | region | amount |",
        "+-----+--------+--------+",
        "| 3   | EU     | 300    |",
        "| 4   | EU     | 400    |",
        "| 5   | APAC   | 500    |",
        "| 100 | US     | 9999   |",
        "+-----+--------+--------+",
    ];
    assert_batches_sorted_eq!(expected_after, &after_batches);

    // Spark validation
    let spark_container = fixture.spark_container_name();
    let count_result = spark_validate_with_container(
        &spark_container,
        "test_static_overwrite",
        ValidationType::Count,
    )
    .await
    .expect("Spark count validation should succeed");
    assert_eq!(
        count_result.count,
        Some(4),
        "Spark count should match after static overwrite (1 US + 2 EU + 1 APAC)"
    );

    // Verify checksum: 9999 + 300 + 400 + 500 = 11199
    let checksum_result = spark_validate_query_with_container(
        &spark_container,
        "test_static_overwrite",
        "SELECT sum(amount) as total FROM {table}",
    )
    .await
    .expect("Spark checksum should succeed");
    let rows = checksum_result.rows.expect("Should have rows");
    let total = rows[0]
        .get("total")
        .and_then(|v| v.as_i64())
        .expect("Should have total");
    assert_eq!(total, 11199, "Sum should match expected total");

    assert_spark_full_validation(&spark_container, "test_static_overwrite", 4).await;
}

/// Test Static Overwrite with empty result (delete-only operation).
///
/// Setup (by Spark provision.py):
/// - Table `test_static_overwrite_empty` with 4 rows: 2 in 'keep' partition, 2 in 'delete' partition
///
/// Test: Overwrite where category = 'delete' with NO new files (effectively deletes the partition)
#[tokio::test]
async fn test_crossengine_static_overwrite_delete_partition() {
    use iceberg::expr::Reference;
    use iceberg::spec::Datum;
    use iceberg::transaction::Transaction;

    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let table_ident = TableIdent::from_strs(["default", "test_static_overwrite_empty"]).unwrap();
    let table = client.load_table(&table_ident).await.unwrap();

    // Execute Static Overwrite with filter but NO new files
    let filter = Reference::new("category").equal_to(Datum::string("delete"));
    let tx = Transaction::new(&table);
    let action = tx.overwrite().overwrite_filter(filter);
    // Note: No add_data_files() - this effectively deletes the partition
    let tx = action.apply(tx).unwrap();
    let _table_after = tx
        .commit(client.as_ref())
        .await
        .expect("Static overwrite (delete) should succeed");

    // Verify with Rust: only 'keep' partition remains
    let ctx = SessionContext::new();
    let namespace = iceberg::NamespaceIdent::new("default".to_string());
    let provider_after = IcebergTableProvider::try_new(
        client.clone(),
        namespace.clone(),
        "test_static_overwrite_empty",
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

    // Expected: only 'keep' partition (ids 1, 2), 'delete' partition removed
    let expected_after = [
        "+----+----------+-------+",
        "| id | category | value |",
        "+----+----------+-------+",
        "| 1  | keep     | 100   |",
        "| 2  | keep     | 200   |",
        "+----+----------+-------+",
    ];
    assert_batches_sorted_eq!(expected_after, &after_batches);

    // Spark validation
    let spark_container = fixture.spark_container_name();
    let count_result = spark_validate_with_container(
        &spark_container,
        "test_static_overwrite_empty",
        ValidationType::Count,
    )
    .await
    .expect("Spark count validation should succeed");
    assert_eq!(
        count_result.count,
        Some(2),
        "Spark count should match after delete (only 'keep' partition remains)"
    );

    // Verify checksum: 100 + 200 = 300
    let checksum_result = spark_validate_query_with_container(
        &spark_container,
        "test_static_overwrite_empty",
        "SELECT sum(value) as total FROM {table}",
    )
    .await
    .expect("Spark checksum should succeed");
    let rows = checksum_result.rows.expect("Should have rows");
    let total = rows[0]
        .get("total")
        .and_then(|v| v.as_i64())
        .expect("Should have total");
    assert_eq!(total, 300, "Sum should match expected total");

    assert_spark_full_validation(&spark_container, "test_static_overwrite_empty", 2).await;
}

/// Test Dynamic Overwrite on unpartitioned table (full table replace).
///
/// Setup (by Spark provision.py):
/// - Table `test_dynamic_overwrite_unpartitioned` with 3 rows
///
/// Test: Replace ALL data with new rows via dynamic overwrite
#[tokio::test]
async fn test_crossengine_dynamic_overwrite_unpartitioned() {
    use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
    use iceberg::spec::DataFileFormat;
    use iceberg::transaction::Transaction;
    use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use iceberg::writer::file_writer::ParquetWriterBuilder;
    use iceberg::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
    use parquet::file::properties::WriterProperties;

    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let table_ident =
        TableIdent::from_strs(["default", "test_dynamic_overwrite_unpartitioned"]).unwrap();
    let table = client.load_table(&table_ident).await.unwrap();

    // Write new data files
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(
        "unpart-overwrite".to_string(),
        None,
        DataFileFormat::Parquet,
    );

    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::builder().build(),
        table.metadata().current_schema().clone(),
    );
    let rolling_file_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_file_writer_builder);
    let mut writer = data_file_writer_builder.build(None).await.unwrap();

    // Create Arrow schema matching table schema
    let arrow_schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );

    // Completely new data (replacing original alpha, beta, gamma)
    let batch = RecordBatch::try_new(arrow_schema, vec![
        Arc::new(Int32Array::from(vec![100, 200])), // new ids
        Arc::new(StringArray::from(vec!["new_a", "new_b"])),
        Arc::new(Int32Array::from(vec![5000, 6000])), // new values
    ])
    .unwrap();

    writer.write(batch).await.unwrap();
    let data_files: Vec<_> = writer.close().await.unwrap();

    // Execute ReplacePartitions (Dynamic Overwrite) on unpartitioned table
    // This should replace ALL data since unpartitioned = single implicit partition
    let tx = Transaction::new(&table);
    let action = tx.replace_partitions().add_data_files(data_files);
    let tx = action.apply(tx).unwrap();
    let _table_after = tx
        .commit(client.as_ref())
        .await
        .expect("Dynamic overwrite on unpartitioned table should succeed");

    // Verify with Rust: all original data replaced
    let ctx = SessionContext::new();
    let namespace = iceberg::NamespaceIdent::new("default".to_string());
    let provider_after = IcebergTableProvider::try_new(
        client.clone(),
        namespace.clone(),
        "test_dynamic_overwrite_unpartitioned",
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

    // Expected: only new data
    let expected_after = [
        "+-----+-------+-------+",
        "| id  | name  | value |",
        "+-----+-------+-------+",
        "| 100 | new_a | 5000  |",
        "| 200 | new_b | 6000  |",
        "+-----+-------+-------+",
    ];
    assert_batches_sorted_eq!(expected_after, &after_batches);

    // Spark validation
    let spark_container = fixture.spark_container_name();
    let count_result = spark_validate_with_container(
        &spark_container,
        "test_dynamic_overwrite_unpartitioned",
        ValidationType::Count,
    )
    .await
    .expect("Spark count validation should succeed");
    assert_eq!(
        count_result.count,
        Some(2),
        "Spark count should match after full table replace"
    );

    // Verify checksum: 5000 + 6000 = 11000
    let checksum_result = spark_validate_query_with_container(
        &spark_container,
        "test_dynamic_overwrite_unpartitioned",
        "SELECT sum(value) as total FROM {table}",
    )
    .await
    .expect("Spark checksum should succeed");
    let rows = checksum_result.rows.expect("Should have rows");
    let total = rows[0]
        .get("total")
        .and_then(|v| v.as_i64())
        .expect("Should have total");
    assert_eq!(total, 11000, "Sum should match expected total");

    assert_spark_full_validation(&spark_container, "test_dynamic_overwrite_unpartitioned", 2).await;
}

#[tokio::test]
async fn test_crossengine_metadata_tables_partitions_entries_parity() {
    use arrow_array::{
        Array, ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray, StructArray,
    };
    use arrow_schema::DataType;
    use futures::TryStreamExt;
    use iceberg::TableCreation;
    use iceberg::spec::{
        DataFileFormat, Literal, NestedField, PartitionKey, PrimitiveType, Schema, Struct,
        Transform, Type, UnboundPartitionSpec,
    };
    use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use iceberg::writer::file_writer::ParquetWriterBuilder;
    use iceberg::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
    use parquet::file::properties::WriterProperties;

    #[derive(Debug, PartialEq, Eq)]
    struct PartitionSummary {
        category: String,
        spec_id: i64,
        record_count: i64,
        file_count: i64,
        total_data_file_size_in_bytes: i64,
        position_delete_record_count: i64,
        position_delete_file_count: i64,
        equality_delete_record_count: i64,
        equality_delete_file_count: i64,
    }

    #[derive(Debug, PartialEq, Eq)]
    struct EntrySummary {
        category: String,
        status: i64,
        content: i64,
        record_count: i64,
    }

    fn struct_field_index(array: &StructArray, field_name: &str) -> usize {
        match array.data_type() {
            DataType::Struct(fields) => fields
                .iter()
                .position(|field| field.name() == field_name)
                .expect("field missing"),
            _ => panic!("expected struct"),
        }
    }

    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = crate::shared_tests::random_ns().await;
    let table_name = format!("metadata_tables_parity_{}", Uuid::new_v4().simple());

    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "category", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "value", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .unwrap();

    let unbound_partition_spec = UnboundPartitionSpec::builder()
        .add_partition_field(2, "category", Transform::Identity)
        .expect("could not add partition field")
        .build();

    let partition_spec = unbound_partition_spec
        .bind(schema.clone())
        .expect("could not bind to schema");

    let table_creation = TableCreation::builder()
        .name(table_name.clone())
        .schema(schema.clone())
        .partition_spec(partition_spec.clone())
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
        DefaultFileNameGenerator::new("metadata-parity".to_string(), None, DataFileFormat::Parquet);
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::builder().build(),
        table.metadata().current_schema().clone(),
    );
    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

    let mut data_files = Vec::new();
    for (category, ids, values) in [
        ("alpha", vec![1, 2], vec![10, 20]),
        ("beta", vec![3, 4], vec![30, 40]),
    ] {
        let partition_key = PartitionKey::new(
            partition_spec.clone(),
            table.metadata().current_schema().clone(),
            Struct::from_iter([Some(Literal::string(category))]),
        );
        let mut writer = data_file_writer_builder
            .build(Some(partition_key))
            .await
            .unwrap();

        let categories = vec![category.to_string(); ids.len()];
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![
            Arc::new(Int32Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(categories)) as ArrayRef,
            Arc::new(Int32Array::from(values)) as ArrayRef,
        ])
        .unwrap();

        writer.write(batch).await.unwrap();
        data_files.extend(writer.close().await.unwrap());
    }

    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(data_files)
        .apply(tx)
        .unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    let partition_batches: Vec<_> = table
        .inspect()
        .partitions()
        .scan()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    let mut rust_partitions = Vec::new();
    for batch in &partition_batches {
        let schema = batch.schema();
        let partition_index = schema
            .index_of("partition")
            .expect("partition column missing");
        let spec_id_index = schema.index_of("spec_id").expect("spec_id column missing");
        let record_count_index = schema
            .index_of("record_count")
            .expect("record_count column missing");
        let file_count_index = schema
            .index_of("file_count")
            .expect("file_count column missing");
        let total_size_index = schema
            .index_of("total_data_file_size_in_bytes")
            .expect("total_data_file_size_in_bytes column missing");
        let pos_delete_record_index = schema
            .index_of("position_delete_record_count")
            .expect("position_delete_record_count column missing");
        let pos_delete_file_index = schema
            .index_of("position_delete_file_count")
            .expect("position_delete_file_count column missing");
        let eq_delete_record_index = schema
            .index_of("equality_delete_record_count")
            .expect("equality_delete_record_count column missing");
        let eq_delete_file_index = schema
            .index_of("equality_delete_file_count")
            .expect("equality_delete_file_count column missing");

        let partition_col = batch
            .column(partition_index)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("partition should be struct");
        let category_index = struct_field_index(partition_col, "category");
        let category_col = partition_col
            .column(category_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("partition category should be string");

        let spec_id_col = batch
            .column(spec_id_index)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("spec_id should be int");
        let record_count_col = batch
            .column(record_count_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("record_count should be long");
        let file_count_col = batch
            .column(file_count_index)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("file_count should be int");
        let total_size_col = batch
            .column(total_size_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("total_data_file_size_in_bytes should be long");
        let pos_delete_record_col = batch
            .column(pos_delete_record_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("position_delete_record_count should be long");
        let pos_delete_file_col = batch
            .column(pos_delete_file_index)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("position_delete_file_count should be int");
        let eq_delete_record_col = batch
            .column(eq_delete_record_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("equality_delete_record_count should be long");
        let eq_delete_file_col = batch
            .column(eq_delete_file_index)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("equality_delete_file_count should be int");

        for i in 0..batch.num_rows() {
            assert!(
                !category_col.is_null(i),
                "Partition category should be populated"
            );
            rust_partitions.push(PartitionSummary {
                category: category_col.value(i).to_string(),
                spec_id: spec_id_col.value(i) as i64,
                record_count: record_count_col.value(i),
                file_count: file_count_col.value(i) as i64,
                total_data_file_size_in_bytes: total_size_col.value(i),
                position_delete_record_count: pos_delete_record_col.value(i),
                position_delete_file_count: pos_delete_file_col.value(i) as i64,
                equality_delete_record_count: eq_delete_record_col.value(i),
                equality_delete_file_count: eq_delete_file_col.value(i) as i64,
            });
        }
    }
    rust_partitions.sort_by(|a, b| {
        a.category
            .cmp(&b.category)
            .then_with(|| a.spec_id.cmp(&b.spec_id))
    });
    assert_eq!(rust_partitions.len(), 2);
    for entry in &rust_partitions {
        assert_eq!(entry.record_count, 2);
        assert!(entry.file_count > 0);
        assert_eq!(entry.position_delete_record_count, 0);
        assert_eq!(entry.position_delete_file_count, 0);
        assert_eq!(entry.equality_delete_record_count, 0);
        assert_eq!(entry.equality_delete_file_count, 0);
    }

    let spark_container = fixture.spark_container_name();
    let table_ref = format!("rest.{}.{}", ns.name(), table_name);
    let partition_result = spark_partitions_table_with_container(&spark_container, &table_ref)
        .await
        .expect("Spark partitions table should succeed");
    let spark_partition_count = partition_result
        .partition_count
        .expect("Spark partition count should be present");
    assert_eq!(spark_partition_count, 2);
    let partitions = partition_result
        .partitions
        .expect("Spark partitions should return rows");

    let mut spark_partitions = Vec::new();
    for partition in partitions {
        let partition_values = partition
            .partition
            .expect("partition values should be present");
        let category = partition_values
            .get("category")
            .and_then(|value| value.as_str())
            .expect("partition category should be present")
            .to_string();
        let spec_id = partition.spec_id.expect("spec_id should be present") as i64;
        let record_count = partition
            .record_count
            .expect("record_count should be present");
        let file_count = partition.file_count.expect("file_count should be present");
        let total_size = partition
            .total_data_file_size_in_bytes
            .expect("total_data_file_size_in_bytes should be present");
        let pos_delete_record = partition
            .position_delete_record_count
            .expect("position_delete_record_count should be present");
        let pos_delete_file = partition
            .position_delete_file_count
            .expect("position_delete_file_count should be present");
        let eq_delete_record = partition
            .equality_delete_record_count
            .expect("equality_delete_record_count should be present");
        let eq_delete_file = partition
            .equality_delete_file_count
            .expect("equality_delete_file_count should be present");
        spark_partitions.push(PartitionSummary {
            category,
            spec_id,
            record_count,
            file_count,
            total_data_file_size_in_bytes: total_size,
            position_delete_record_count: pos_delete_record,
            position_delete_file_count: pos_delete_file,
            equality_delete_record_count: eq_delete_record,
            equality_delete_file_count: eq_delete_file,
        });
    }
    spark_partitions.sort_by(|a, b| {
        a.category
            .cmp(&b.category)
            .then_with(|| a.spec_id.cmp(&b.spec_id))
    });
    assert_eq!(spark_partitions.len(), rust_partitions.len());
    for (spark_entry, rust_entry) in spark_partitions.iter().zip(rust_partitions.iter()) {
        assert_eq!(spark_entry.category, rust_entry.category);
        assert_eq!(spark_entry.record_count, rust_entry.record_count);
        assert!(spark_entry.file_count > 0);
        assert_eq!(spark_entry.position_delete_record_count, 0);
        assert_eq!(spark_entry.position_delete_file_count, 0);
        assert_eq!(spark_entry.equality_delete_record_count, 0);
        assert_eq!(spark_entry.equality_delete_file_count, 0);
    }

    let entry_batches: Vec<_> = table
        .inspect()
        .entries()
        .scan()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    let mut rust_entries = Vec::new();
    for batch in &entry_batches {
        let schema = batch.schema();
        let status_index = schema.index_of("status").expect("status column missing");
        let data_file_index = schema
            .index_of("data_file")
            .expect("data_file column missing");

        let status_col = batch
            .column(status_index)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("status should be int");
        let data_file_col = batch
            .column(data_file_index)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("data_file should be struct");
        let content_index = struct_field_index(data_file_col, "content");
        let record_count_index = struct_field_index(data_file_col, "record_count");
        let partition_index = struct_field_index(data_file_col, "partition");

        let content_col = data_file_col
            .column(content_index)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("content should be int");
        let record_count_col = data_file_col
            .column(record_count_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("record_count should be long");
        let partition_col = data_file_col
            .column(partition_index)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("partition should be struct");
        let category_index = struct_field_index(partition_col, "category");
        let category_col = partition_col
            .column(category_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("entry category should be string");

        for i in 0..batch.num_rows() {
            assert!(
                !category_col.is_null(i),
                "Entry partition category should be populated"
            );
            rust_entries.push(EntrySummary {
                category: category_col.value(i).to_string(),
                status: status_col.value(i) as i64,
                content: content_col.value(i) as i64,
                record_count: record_count_col.value(i),
            });
        }
    }
    rust_entries.sort_by(|a, b| {
        a.category
            .cmp(&b.category)
            .then_with(|| a.status.cmp(&b.status))
            .then_with(|| a.content.cmp(&b.content))
    });
    assert_eq!(rust_entries.len(), 2);
    for entry in &rust_entries {
        assert_eq!(entry.status, 1);
        assert_eq!(entry.content, 0);
        assert_eq!(entry.record_count, 2);
    }

    let entry_result = spark_entries_table_with_container(&spark_container, &table_ref, None)
        .await
        .expect("Spark entries table should succeed");
    let spark_entry_count = entry_result
        .entry_count
        .expect("Spark entry count should be present");
    assert_eq!(spark_entry_count, 2);
    let entries = entry_result
        .entries
        .expect("Spark entries should return rows");

    let mut spark_entries = Vec::new();
    for entry in entries {
        let data_file = entry.data_file.expect("data_file should be present");
        let partition_values = data_file
            .partition
            .expect("partition values should be present");
        let category = partition_values
            .get("category")
            .and_then(|value| value.as_str())
            .expect("entry category should be present")
            .to_string();
        let status = entry.status.expect("status should be present") as i64;
        let content = data_file.content.expect("content should be present") as i64;
        let record_count = data_file
            .record_count
            .expect("record_count should be present");
        spark_entries.push(EntrySummary {
            category,
            status,
            content,
            record_count,
        });
    }
    spark_entries.sort_by(|a, b| {
        a.category
            .cmp(&b.category)
            .then_with(|| a.status.cmp(&b.status))
            .then_with(|| a.content.cmp(&b.content))
    });
    assert_eq!(spark_entries, rust_entries);
}

#[tokio::test]
async fn test_crossengine_wap_branch_fast_forward() {
    use arrow_array::{ArrayRef, Int32Array, RecordBatch};
    use arrow_schema::Schema as ArrowSchema;
    use iceberg::TableCreation;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = crate::shared_tests::random_ns().await;
    let table_name = format!("wap_branch_ff_{}", Uuid::new_v4().simple());

    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "value", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .unwrap();

    let table_creation = TableCreation::builder()
        .name(table_name.clone())
        .schema(schema.clone())
        .build();

    let table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    let arrow_schema: Arc<ArrowSchema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );

    let initial_batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef,
        ],
    )
    .unwrap();

    let initial_files = write_unpartitioned_data_files(
        &table,
        &format!("wap-init-{}", Uuid::new_v4().simple()),
        vec![initial_batch],
    )
    .await;

    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(initial_files)
        .apply(tx)
        .unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();
    let main_snapshot_id = table
        .metadata()
        .current_snapshot_id()
        .expect("main snapshot should exist");

    let table = table
        .create_branch("staging")
        .commit(&rest_catalog)
        .await
        .unwrap();

    let staged_batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![3, 4])) as ArrayRef,
            Arc::new(Int32Array::from(vec![30, 40])) as ArrayRef,
        ],
    )
    .unwrap();

    let staged_files = write_unpartitioned_data_files(
        &table,
        &format!("wap-staged-{}", Uuid::new_v4().simple()),
        vec![staged_batch],
    )
    .await;

    let table = table
        .write_to_branch("staging")
        .add_data_files(staged_files)
        .commit(&rest_catalog)
        .await
        .unwrap();

    let staging_ref = table
        .metadata()
        .refs()
        .get("staging")
        .expect("staging ref should exist");
    let staging_snapshot_id = staging_ref.snapshot_id;

    assert_ne!(
        staging_snapshot_id, main_snapshot_id,
        "staging snapshot should differ from main"
    );

    let spark_container = fixture.spark_container_name();
    let table_ref = format!("{}.{}", ns.name(), table_name);

    let count_result = spark_validate_with_container(
        &spark_container,
        &table_ref,
        ValidationType::Count,
    )
    .await
    .expect("Spark count validation should succeed");
    assert_eq!(
        count_result.count,
        Some(2),
        "Spark main should not see staged data before fast-forward"
    );

    let refs_result = spark_validate_query_with_container(
        &spark_container,
        &table_ref,
        "SELECT name, snapshot_id FROM {table}.refs",
    )
    .await
    .expect("Spark refs query should succeed");
    let rows = refs_result.rows.expect("refs query should return rows");
    let staging_row = rows
        .iter()
        .find(|row| row.get("name").and_then(|value| value.as_str()) == Some("staging"))
        .expect("Spark refs should include staging");
    let spark_snapshot_id = staging_row
        .get("snapshot_id")
        .and_then(|value| value.as_i64())
        .expect("staging snapshot id should be numeric");

    assert_eq!(
        spark_snapshot_id, staging_snapshot_id,
        "Spark refs should match staging snapshot id"
    );

    let table = table
        .fast_forward("main", "staging")
        .commit(&rest_catalog)
        .await
        .unwrap();

    let after_result = spark_validate_with_container(
        &spark_container,
        &table_ref,
        ValidationType::Count,
    )
    .await
    .expect("Spark count validation should succeed");

    assert_eq!(
        after_result.count,
        Some(4),
        "Spark main should see staged data after fast-forward"
    );

    let main_snapshot_after = table
        .metadata()
        .current_snapshot_id()
        .expect("main snapshot should exist");
    assert_eq!(
        main_snapshot_after, staging_snapshot_id,
        "main should fast-forward to staging snapshot"
    );
}

#[tokio::test]
async fn test_crossengine_incremental_scan_matches_entries_table() {
    use arrow_array::{ArrayRef, Int32Array, RecordBatch};
    use arrow_schema::Schema as ArrowSchema;
    use iceberg::TableCreation;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use std::collections::HashSet;

    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = crate::shared_tests::random_ns().await;
    let table_name = format!("incremental_scan_{}", Uuid::new_v4().simple());

    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "value", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .unwrap();

    let table_creation = TableCreation::builder()
        .name(table_name.clone())
        .schema(schema.clone())
        .build();

    let table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    let arrow_schema: Arc<ArrowSchema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );

    let batch_one = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(Int32Array::from(vec![100, 200])) as ArrayRef,
        ],
    )
    .unwrap();

    let files_one = write_unpartitioned_data_files(
        &table,
        &format!("incremental-a-{}", Uuid::new_v4().simple()),
        vec![batch_one],
    )
    .await;

    let tx = Transaction::new(&table);
    let tx = tx.fast_append().add_data_files(files_one).apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();
    let snapshot_1 = table
        .metadata()
        .current_snapshot_id()
        .expect("snapshot 1 should exist");

    let batch_two = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![3, 4])) as ArrayRef,
            Arc::new(Int32Array::from(vec![300, 400])) as ArrayRef,
        ],
    )
    .unwrap();

    let files_two = write_unpartitioned_data_files(
        &table,
        &format!("incremental-b-{}", Uuid::new_v4().simple()),
        vec![batch_two],
    )
    .await;

    let tx = Transaction::new(&table);
    let tx = tx.fast_append().add_data_files(files_two).apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();
    let snapshot_2 = table
        .metadata()
        .current_snapshot_id()
        .expect("snapshot 2 should exist");

    let changes = table
        .incremental_scan(snapshot_1, snapshot_2)
        .build()
        .expect("incremental scan should build")
        .changes()
        .await
        .expect("incremental scan should succeed");

    assert!(changes.removed_data_files().is_empty(), "No files removed");
    assert!(
        !changes.added_data_files().is_empty(),
        "Expected added files"
    );

    let rust_paths: HashSet<String> = changes
        .added_data_files()
        .iter()
        .map(|file| file.file_path().to_string())
        .collect();

    let spark_container = fixture.spark_container_name();
    let table_ref = format!("{}.{}", ns.name(), table_name);
    let entries_result = spark_entries_table_with_container(&spark_container, &table_ref, Some(200))
        .await
        .expect("Spark entries table should succeed");

    let spark_paths: HashSet<String> = entries_result
        .entries
        .unwrap_or_default()
        .into_iter()
        .filter(|entry| entry.snapshot_id == Some(snapshot_2))
        .filter(|entry| entry.status == Some(ManifestStatus::Added as i32))
        .filter_map(|entry| {
            let data_file = entry.data_file?;
            if data_file.content == Some(DataContentType::Data as i32) {
                data_file.file_path
            } else {
                None
            }
        })
        .collect();

    assert_eq!(
        spark_paths, rust_paths,
        "Incremental scan added files should match Spark entries"
    );
}
