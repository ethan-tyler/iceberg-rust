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

//! Semantic Parity Validation Tests (WP1.5)
//!
//! These tests validate that Rust-written metadata matches Spark/Iceberg-Java
//! expectations EXACTLY, not just "is readable". We compare:
//!
//! 1. Snapshot summary fields (added-data-files, deleted-records, etc.)
//! 2. Manifest entry structure
//! 3. Edge case behaviors (NULL handling, empty results)
//!
//! # Approach
//!
//! For each operation type, we use paired tables:
//! - `parity_*_rust`: Table where Rust performs the operation
//! - `parity_*_spark`: Table where Spark performs the identical operation
//!
//! Both tables start with identical initial state, then we compare the
//! resulting metadata structures field-by-field.
//!
//! # Parity Comparison Strategy
//!
//! Some fields will differ legitimately:
//! - `snapshot-id`: Always unique
//! - `committed-at`: Timestamps differ
//! - `added-files-size`, `removed-files-size`: May differ slightly due to compression
//!
//! We focus on semantic parity:
//! - `added-data-files`, `deleted-data-files`: MUST match
//! - `added-records`, `deleted-records`: MUST match
//! - `operation`: MUST match
//! - `total-*` fields: MUST match (after operation)

use std::sync::Arc;

use chrono::Utc;
use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use iceberg::spec::{PrimitiveType, Type};
use iceberg::transaction::{
    ApplyTransactionAction, RewriteDataFilesOptions, RewriteStrategy, Transaction,
};
use iceberg::{Catalog, CatalogBuilder, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_datafusion::IcebergTableProvider;
use iceberg_datafusion::compaction::{CompactionOptions, compact_table};
use iceberg_integration_tests::spark_validator::{
    spark_execute_dml_with_container, spark_execute_sql_with_container,
    spark_manifest_entries_with_container, spark_snapshot_summary_with_container,
};
use crate::get_shared_containers;

use super::parity_utils::{
    assert_error_contains, assert_parity_expected_divergences, assert_rust_schema_rejection,
    assert_spark_schema_rejection, compare_manifest_entries, compare_snapshot_summaries,
    extract_rust_manifest_entries, extract_rust_snapshot_summary, summary_is_noop,
};

// =============================================================================
// Semantic Parity Tests
// =============================================================================

/// Test DELETE semantic parity: compare Rust DELETE vs Spark DELETE metadata.
///
/// Both tables start with identical data:
/// - 5 rows: (1, 'alpha', 100), (2, 'beta', 200), (3, 'gamma', 300), (4, 'delta', 400), (5, 'epsilon', 500)
///
/// Operation: DELETE WHERE value > 300 (should delete rows 4, 5)
///
/// Compare resulting snapshot summary fields.
#[tokio::test]
async fn test_semantic_parity_delete() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let ctx = SessionContext::new();
    let namespace = iceberg::NamespaceIdent::new("default".to_string());
    let spark_container = fixture.spark_container_name();

    // Step 1: Perform DELETE on Rust table via iceberg-datafusion
    let rust_provider =
        IcebergTableProvider::try_new(client.clone(), namespace.clone(), "parity_delete_rust")
            .await
            .unwrap();



    let deleted_count = rust_provider
        .delete(&ctx.state(), Some(col("value").gt(lit(300))))
        .await
        .expect("Rust DELETE should succeed");

    assert_eq!(deleted_count, 2, "Rust should delete 2 rows");

    // Reload table to get updated metadata
    let rust_table = client
        .load_table(&TableIdent::from_strs(["default", "parity_delete_rust"]).unwrap())
        .await
        .unwrap();

    let rust_summary = extract_rust_snapshot_summary(&rust_table);

    // Step 2: Perform identical DELETE on Spark table
    let spark_summary = spark_execute_dml_with_container(
        &spark_container,
        "parity_delete_spark",
        "delete",
        Some("value > 300"),
        None,
    )
    .await
    .expect("Spark DELETE should succeed");

    // Step 3: Compare snapshot summaries
    let parity_result = compare_snapshot_summaries("DELETE", &rust_summary, &spark_summary);

    println!("\n{}\n", parity_result.summary());

    // Log divergences for investigation
    let divergences = parity_result.divergences();
    if !divergences.is_empty() {
        println!("DIVERGENCES FOUND:");
        for d in &divergences {
            println!(
                "  - {}: Rust={:?}, Spark={:?}",
                d.field_name, d.rust_value, d.spark_value
            );
        }
    }

    assert_parity_expected_divergences(&parity_result);
}

/// Test snapshot summary extraction from Spark-created tables.
///
/// Validates that we can correctly extract and parse Spark snapshot summaries
/// for comparison purposes.
#[tokio::test]
async fn test_spark_snapshot_summary_extraction() {
    let fixture = get_shared_containers();
    let spark_container = fixture.spark_container_name();

    // Get snapshot summary from a Spark-created table
    let summary = spark_snapshot_summary_with_container(&spark_container, "test_compaction", None)
        .await
        .expect("Spark snapshot summary extraction should succeed");

    // Verify we got valid data
    assert!(summary.snapshot_id.is_some(), "Should have snapshot ID");
    assert!(summary.operation.is_some(), "Should have operation type");

    // The test_compaction table has 5 inserts, should have append operation
    assert_eq!(
        summary.operation.as_deref(),
        Some("append"),
        "Should be append operation"
    );

    // Should have summary fields populated
    if let Some(ref fields) = summary.summary {
        println!("Spark snapshot summary fields:");
        println!("  added-data-files: {:?}", fields.added_data_files);
        println!("  added-records: {:?}", fields.added_records);
        println!("  total-data-files: {:?}", fields.total_data_files);
        println!("  total-records: {:?}", fields.total_records);
    }
}

/// Test manifest entry structure extraction and validation.
///
/// Validates that we can extract manifest entries from Spark and that
/// required fields are populated correctly.
#[tokio::test]
async fn test_manifest_entry_structure() {
    let fixture = get_shared_containers();
    let spark_container = fixture.spark_container_name();

    // Get manifest entries from test_compaction table (has multiple data files)
    let result =
        spark_manifest_entries_with_container(&spark_container, "test_compaction", Some(10))
            .await
            .expect("Manifest entry extraction should succeed");

    assert!(result.entry_count.is_some(), "Should have entry count");
    assert!(
        result.entry_count.unwrap() >= 5,
        "Should have at least 5 entries (one per insert)"
    );

    // Validate entry structure
    if let Some(entries) = &result.entries {
        println!("\nManifest entry structure validation:");
        println!("Total entries: {}", result.entry_count.unwrap());

        for (i, entry) in entries.iter().take(3).enumerate() {
            println!("\nEntry {}:", i + 1);
            println!("  status: {:?}", entry.status);
            println!("  snapshot_id: {:?}", entry.snapshot_id);
            println!("  sequence_number: {:?}", entry.sequence_number);

            if let Some(df) = &entry.data_file {
                println!("  data_file.content: {:?}", df.content);
                println!("  data_file.file_format: {:?}", df.file_format);
                println!("  data_file.record_count: {:?}", df.record_count);
                println!(
                    "  data_file.file_size_in_bytes: {:?}",
                    df.file_size_in_bytes
                );
                println!("  data_file.partition: {:?}", df.partition);
            }

            // Required fields should be present
            assert!(entry.status.is_some(), "Entry should have status");
            assert!(entry.snapshot_id.is_some(), "Entry should have snapshot_id");

            if let Some(df) = &entry.data_file {
                assert!(df.content.is_some(), "Data file should have content type");
                assert!(df.file_path.is_some(), "Data file should have file_path");
                assert!(
                    df.file_format.is_some(),
                    "Data file should have file_format"
                );
                assert!(
                    df.record_count.is_some(),
                    "Data file should have record_count"
                );
                assert!(
                    df.file_size_in_bytes.is_some(),
                    "Data file should have file_size_in_bytes"
                );
            }
        }
    }
}

/// Test empty result behavior: DELETE that matches nothing.
///
/// Edge case: What happens when DELETE predicate matches zero rows?
/// Both Rust and Spark should handle this gracefully without creating a snapshot.
#[tokio::test]
async fn test_edge_case_empty_delete() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let ctx = SessionContext::new();
    let namespace = iceberg::NamespaceIdent::new("default".to_string());
    let spark_container = fixture.spark_container_name();

    // Get initial snapshot count
    let table_before = client
        .load_table(&TableIdent::from_strs(["default", "parity_delete_empty_rust"]).unwrap())
        .await
        .unwrap();
    let initial_snapshot_count = table_before.metadata().snapshots().count();

    // Perform DELETE that matches nothing (value > 10000, but max is 500)
    let rust_provider =
        IcebergTableProvider::try_new(client.clone(), namespace.clone(), "parity_delete_empty_rust")
            .await
            .unwrap();

    let deleted_count = rust_provider
        .delete(&ctx.state(), Some(col("value").gt(lit(10000))))
        .await
        .expect("Rust DELETE with no matches should succeed");

    assert_eq!(deleted_count, 0, "No rows should be deleted");

    // Check if a new snapshot was created
    let table_after = client
        .load_table(&TableIdent::from_strs(["default", "parity_delete_empty_rust"]).unwrap())
        .await
        .unwrap();
    let final_snapshot_count = table_after.metadata().snapshots().count();

    let spark_before =
        spark_snapshot_summary_with_container(&spark_container, "parity_delete_empty_spark", None)
            .await
            .expect("Spark snapshot summary should succeed");

    let spark_after = spark_execute_dml_with_container(
        &spark_container,
        "parity_delete_empty_spark",
        "delete",
        Some("value > 10000"),
        None,
    )
    .await
    .expect("Spark DELETE with no matches should succeed");

    let rust_created_snapshot = final_snapshot_count > initial_snapshot_count;
    let rust_noop_snapshot = if rust_created_snapshot {
        summary_is_noop(&extract_rust_snapshot_summary(&table_after))
    } else {
        false
    };

    let spark_snapshot_changed = spark_before.snapshot_id != spark_after.snapshot_id;
    let spark_noop_snapshot = if spark_snapshot_changed {
        summary_is_noop(&spark_after)
    } else {
        false
    };

    println!("\nEmpty DELETE behavior:");
    println!("  Initial snapshots: {}", initial_snapshot_count);
    println!("  Final snapshots: {}", final_snapshot_count);
    println!("  New snapshot created: {}", rust_created_snapshot);
    println!("  Rust snapshot is no-op: {}", rust_noop_snapshot);
    println!("  Spark snapshot changed: {}", spark_snapshot_changed);
    println!("  Spark snapshot is no-op: {}", spark_noop_snapshot);

    assert!(
        (!rust_created_snapshot || rust_noop_snapshot)
            && (!spark_snapshot_changed || spark_noop_snapshot),
        "Empty DELETE should be a no-op (no new snapshot or a no-op snapshot). rust_created_snapshot={}, rust_noop_snapshot={}, spark_snapshot_changed={}, spark_noop_snapshot={}",
        rust_created_snapshot,
        rust_noop_snapshot,
        spark_snapshot_changed,
        spark_noop_snapshot
    );
}

/// Test NULL handling semantic parity.
///
/// Tables have rows with NULL values. Operation: DELETE WHERE name IS NULL
/// Should delete rows 2 and 4 (both have NULL in name column).
#[tokio::test]
async fn test_semantic_parity_null_handling() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let ctx = SessionContext::new();
    let namespace = iceberg::NamespaceIdent::new("default".to_string());
    let spark_container = fixture.spark_container_name();

    // Step 1: Perform DELETE WHERE name IS NULL on Rust table
    let rust_provider =
        IcebergTableProvider::try_new(client.clone(), namespace.clone(), "parity_null_rust")
            .await
            .unwrap();

    let deleted_count = rust_provider
        .delete(&ctx.state(), Some(col("name").is_null()))
        .await
        .expect("Rust DELETE with IS NULL should succeed");

    assert_eq!(deleted_count, 2, "Rust should delete 2 rows with NULL name");

    // Reload table to get updated metadata
    let rust_table = client
        .load_table(&TableIdent::from_strs(["default", "parity_null_rust"]).unwrap())
        .await
        .unwrap();

    let rust_summary = extract_rust_snapshot_summary(&rust_table);

    // Step 2: Perform identical DELETE on Spark table
    let spark_summary = spark_execute_dml_with_container(
        &spark_container,
        "parity_null_spark",
        "delete",
        Some("name IS NULL"),
        None,
    )
    .await
    .expect("Spark DELETE with IS NULL should succeed");

    // Step 3: Compare snapshot summaries
    let parity_result =
        compare_snapshot_summaries("DELETE (NULL handling)", &rust_summary, &spark_summary);

    println!("\n{}\n", parity_result.summary());

    // Log results for documentation
    let divergences = parity_result.divergences();
    if !divergences.is_empty() {
        println!("NULL HANDLING DIVERGENCES:");
        for d in &divergences {
            println!(
                "  - {}: Rust={:?}, Spark={:?}",
                d.field_name, d.rust_value, d.spark_value
            );
        }
    }

    assert_parity_expected_divergences(&parity_result);
}

/// Test three-valued logic parity: predicates with NULL comparisons.
#[tokio::test]
async fn test_semantic_parity_three_valued_logic() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let ctx = SessionContext::new();
    let namespace = iceberg::NamespaceIdent::new("default".to_string());
    let spark_container = fixture.spark_container_name();

    let rust_provider = IcebergTableProvider::try_new(
        client.clone(),
        namespace.clone(),
        "parity_three_valued_rust",
    )
    .await
    .unwrap();

    let predicate = col("name")
        .eq(lit("alpha"))
        .or(col("name").not_eq(lit("alpha")));
    let deleted_count = rust_provider
        .delete(&ctx.state(), Some(predicate))
        .await
        .expect("Rust DELETE with three-valued logic should succeed");

    assert_eq!(deleted_count, 2, "Rust should delete 2 non-null rows");

    let rust_table = client
        .load_table(&TableIdent::from_strs(["default", "parity_three_valued_rust"]).unwrap())
        .await
        .unwrap();

    let rust_summary = extract_rust_snapshot_summary(&rust_table);

    let spark_summary = spark_execute_dml_with_container(
        &spark_container,
        "parity_three_valued_spark",
        "delete",
        Some("name = 'alpha' OR name <> 'alpha'"),
        None,
    )
    .await
    .expect("Spark DELETE with three-valued logic should succeed");

    let parity_result = compare_snapshot_summaries(
        "DELETE (three-valued logic)",
        &rust_summary,
        &spark_summary,
    );

    println!("\n{}\n", parity_result.summary());

    let divergences = parity_result.divergences();
    if !divergences.is_empty() {
        println!("THREE-VALUED LOGIC DIVERGENCES:");
        for d in &divergences {
            println!(
                "  - {}: Rust={:?}, Spark={:?}",
                d.field_name, d.rust_value, d.spark_value
            );
        }
    }

    assert_parity_expected_divergences(&parity_result);
}

/// Test UPDATE semantic parity: compare Rust UPDATE vs Spark UPDATE metadata.
///
/// Operation: UPDATE status='done' WHERE id > 2 (updates rows 3, 4)
#[tokio::test]
async fn test_semantic_parity_update() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let ctx = SessionContext::new();
    let namespace = iceberg::NamespaceIdent::new("default".to_string());
    let spark_container = fixture.spark_container_name();

    let provider =
        IcebergTableProvider::try_new(client.clone(), namespace.clone(), "parity_update_rust")
            .await
            .unwrap();

    let updated_count = provider
        .update()
        .await
        .unwrap()
        .set("status", lit("done"))
        .filter(col("id").gt(lit(2)))
        .execute(&ctx.state())
        .await
        .expect("Rust UPDATE should succeed");

    assert_eq!(updated_count, 2, "Rust should update 2 rows");

    let rust_table = client
        .load_table(&TableIdent::from_strs(["default", "parity_update_rust"]).unwrap())
        .await
        .unwrap();
    let rust_summary = extract_rust_snapshot_summary(&rust_table);

    let spark_summary = spark_execute_dml_with_container(
        &spark_container,
        "parity_update_spark",
        "update",
        Some("id > 2"),
        Some("status = 'done'"),
    )
    .await
    .expect("Spark UPDATE should succeed");

    let parity_result = compare_snapshot_summaries("UPDATE", &rust_summary, &spark_summary);

    println!("\n{}\n", parity_result.summary());
    let divergences = parity_result.divergences();
    if !divergences.is_empty() {
        println!("UPDATE DIVERGENCES:");
        for d in &divergences {
            println!(
                "  - {}: Rust={:?}, Spark={:?}",
                d.field_name, d.rust_value, d.spark_value
            );
        }
    }

    assert_parity_expected_divergences(&parity_result);
}

/// Test MERGE semantic parity: compare Rust MERGE vs Spark MERGE metadata.
#[tokio::test]
async fn test_semantic_parity_merge() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let ctx = SessionContext::new();
    let namespace = iceberg::NamespaceIdent::new("default".to_string());
    let spark_container = fixture.spark_container_name();

    let source_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let source_batch = RecordBatch::try_new(source_schema.clone(), vec![
        Arc::new(Int32Array::from(vec![1, 3, 5])),
        Arc::new(StringArray::from(vec!["alpha_upd", "gamma_upd", "epsilon"])),
        Arc::new(Int32Array::from(vec![110, 330, 500])),
    ])
    .unwrap();

    let source_table =
        Arc::new(MemTable::try_new(source_schema, vec![vec![source_batch]]).unwrap());
    ctx.register_table("merge_source", source_table).unwrap();
    let source_df = ctx.table("merge_source").await.unwrap();

    let provider =
        IcebergTableProvider::try_new(client.clone(), namespace.clone(), "parity_merge_rust")
            .await
            .unwrap();

    let stats = provider
        .merge(source_df)
        .await
        .unwrap()
        .on(col("target.id").eq(col("source.id")))
        .when_matched(None)
        .update(vec![
            ("name", col("source_name")),
            ("value", col("source_value")),
        ])
        .when_not_matched(None)
        .insert_all()
        .execute(&ctx.state())
        .await
        .expect("Rust MERGE should succeed");

    assert_eq!(stats.rows_updated, 2, "Rust should update 2 rows");
    assert_eq!(stats.rows_inserted, 1, "Rust should insert 1 row");

    let rust_table = client
        .load_table(&TableIdent::from_strs(["default", "parity_merge_rust"]).unwrap())
        .await
        .unwrap();
    let rust_summary = extract_rust_snapshot_summary(&rust_table);

    let merge_sql = r#"
 MERGE INTO rest.default.parity_merge_spark AS target
 USING (
   SELECT 1 AS id, 'alpha_upd' AS name, 110 AS value
   UNION ALL
   SELECT 3 AS id, 'gamma_upd' AS name, 330 AS value
   UNION ALL
   SELECT 5 AS id, 'epsilon' AS name, 500 AS value
 ) AS source
 ON target.id = source.id
 WHEN MATCHED THEN UPDATE SET name = source.name, value = source.value
 WHEN NOT MATCHED THEN INSERT *
 "#;

    let spark_summary =
        spark_execute_sql_with_container(&spark_container, "parity_merge_spark", merge_sql)
            .await
            .expect("Spark MERGE should succeed");

    let parity_result = compare_snapshot_summaries("MERGE", &rust_summary, &spark_summary);

    println!("\n{}\n", parity_result.summary());
    let divergences = parity_result.divergences();
    if !divergences.is_empty() {
        println!("MERGE DIVERGENCES:");
        for d in &divergences {
            println!(
                "  - {}: Rust={:?}, Spark={:?}",
                d.field_name, d.rust_value, d.spark_value
            );
        }
    }

    assert_parity_expected_divergences(&parity_result);
}

/// Test compaction semantic parity: compare Rust compaction vs Spark rewrite_data_files metadata.
#[tokio::test]
async fn test_semantic_parity_compaction() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let table_ident = TableIdent::from_strs(["default", "parity_compact_rust"]).unwrap();
    let table = client.load_table(&table_ident).await.unwrap();

    let mut rewrite_options = RewriteDataFilesOptions::default();
    rewrite_options.min_input_files = 2;

    let options = CompactionOptions::default()
        .with_rewrite_options(rewrite_options)
        .with_strategy(RewriteStrategy::BinPack);

    let result = compact_table(&table, client.clone(), Some(options))
        .await
        .expect("Rust compaction should succeed");
    assert!(result.has_changes(), "Compaction should rewrite data files");

    let table_after = client.load_table(&table_ident).await.unwrap();
    let rust_summary = extract_rust_snapshot_summary(&table_after);

    let spark_container = fixture.spark_container_name();
    let spark_summary = spark_execute_sql_with_container(
        &spark_container,
        "parity_compact_spark",
        "CALL rest.system.rewrite_data_files(table => 'default.parity_compact_spark', options => map('min-input-files','2'))",
    )
    .await
    .expect("Spark compaction should succeed");

    let parity_result = compare_snapshot_summaries("COMPACTION", &rust_summary, &spark_summary);

    println!("\n{}\n", parity_result.summary());
    let divergences = parity_result.divergences();
    if !divergences.is_empty() {
        println!("COMPACTION DIVERGENCES:");
        for d in &divergences {
            println!(
                "  - {}: Rust={:?}, Spark={:?}",
                d.field_name, d.rust_value, d.spark_value
            );
        }
    }

    assert_parity_expected_divergences(&parity_result);
}

/// Test expire snapshots semantic parity: compare Rust expire_snapshots vs Spark expire_snapshots metadata.
#[tokio::test]
async fn test_semantic_parity_expire_snapshots() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let table_ident = TableIdent::from_strs(["default", "parity_expire_rust"]).unwrap();
    let table = client.load_table(&table_ident).await.unwrap();

    let tx = Transaction::new(&table);
    let action = tx.expire_snapshots().older_than(Utc::now()).retain_last(1);
    let tx = action.apply(tx).unwrap();
    let table_after = tx
        .commit(client.as_ref())
        .await
        .expect("Rust expire snapshots should succeed");

    let rust_summary = extract_rust_snapshot_summary(&table_after);

    let spark_container = fixture.spark_container_name();
    let spark_summary = spark_execute_sql_with_container(
        &spark_container,
        "parity_expire_spark",
        "CALL rest.system.expire_snapshots('default.parity_expire_spark', TIMESTAMP '2100-01-01 00:00:00.000', 1)",
    )
    .await
    .expect("Spark expire snapshots should succeed");

    let parity_result =
        compare_snapshot_summaries("EXPIRE SNAPSHOTS", &rust_summary, &spark_summary);

    println!("\n{}\n", parity_result.summary());
    let divergences = parity_result.divergences();
    if !divergences.is_empty() {
        println!("EXPIRE SNAPSHOTS DIVERGENCES:");
        for d in &divergences {
            println!(
                "  - {}: Rust={:?}, Spark={:?}",
                d.field_name, d.rust_value, d.spark_value
            );
        }
    }

    assert_parity_expected_divergences(&parity_result);
}

/// Test boundary value parity: min/max integers and empty strings.
#[tokio::test]
async fn test_semantic_parity_boundary_values() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let ctx = SessionContext::new();
    let namespace = iceberg::NamespaceIdent::new("default".to_string());
    let spark_container = fixture.spark_container_name();

    let provider =
        IcebergTableProvider::try_new(client.clone(), namespace.clone(), "parity_boundary_rust")
            .await
            .unwrap();

    let deleted_count = provider
        .delete(
            &ctx.state(),
            Some(
                col("id")
                    .eq(lit(-2147483648i32))
                    .or(col("name").eq(lit(""))),
            ),
        )
        .await
        .expect("Rust DELETE should succeed");

    assert_eq!(deleted_count, 2, "Rust should delete 2 boundary rows");

    let rust_table = client
        .load_table(&TableIdent::from_strs(["default", "parity_boundary_rust"]).unwrap())
        .await
        .unwrap();
    let rust_summary = extract_rust_snapshot_summary(&rust_table);

    let spark_summary = spark_execute_dml_with_container(
        &spark_container,
        "parity_boundary_spark",
        "delete",
        Some("id = -2147483648 OR name = ''"),
        None,
    )
    .await
    .expect("Spark DELETE should succeed");

    let parity_result =
        compare_snapshot_summaries("DELETE (boundary values)", &rust_summary, &spark_summary);

    println!("\n{}\n", parity_result.summary());
    let divergences = parity_result.divergences();
    if !divergences.is_empty() {
        println!("BOUNDARY VALUE DIVERGENCES:");
        for d in &divergences {
            println!(
                "  - {}: Rust={:?}, Spark={:?}",
                d.field_name, d.rust_value, d.spark_value
            );
        }
    }

    assert_parity_expected_divergences(&parity_result);
}

/// Test zero-length binary semantic parity.
#[tokio::test]
async fn test_semantic_parity_zero_length_binary() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let ctx = SessionContext::new();
    let namespace = iceberg::NamespaceIdent::new("default".to_string());
    let spark_container = fixture.spark_container_name();

    let rust_provider =
        IcebergTableProvider::try_new(client.clone(), namespace.clone(), "parity_binary_rust")
            .await
            .unwrap();

    let payload_type = rust_provider
        .schema()
        .field_with_name("payload")
        .expect("payload field should exist")
        .data_type()
        .clone();

    let empty_binary = match payload_type {
        DataType::Binary => ScalarValue::Binary(Some(Vec::new())),
        DataType::LargeBinary => ScalarValue::LargeBinary(Some(Vec::new())),
        other => panic!("Unexpected payload type for parity_binary_rust: {other:?}"),
    };
    let deleted_count = rust_provider
        .delete(&ctx.state(), Some(col("payload").eq(lit(empty_binary))))
        .await
        .expect("Rust DELETE with empty binary should succeed");

    assert_eq!(deleted_count, 1, "Rust should delete 1 empty binary row");

    let rust_table = client
        .load_table(&TableIdent::from_strs(["default", "parity_binary_rust"]).unwrap())
        .await
        .unwrap();
    let rust_summary = extract_rust_snapshot_summary(&rust_table);

    let spark_summary = spark_execute_dml_with_container(
        &spark_container,
        "parity_binary_spark",
        "delete",
        Some("payload = CAST('' AS BINARY)"),
        None,
    )
    .await
    .expect("Spark DELETE with empty binary should succeed");

    let parity_result =
        compare_snapshot_summaries("DELETE (zero-length binary)", &rust_summary, &spark_summary);

    println!("\n{}\n", parity_result.summary());
    let divergences = parity_result.divergences();
    if !divergences.is_empty() {
        println!("ZERO-LENGTH BINARY DIVERGENCES:");
        for d in &divergences {
            println!(
                "  - {}: Rust={:?}, Spark={:?}",
                d.field_name, d.rust_value, d.spark_value
            );
        }
    }

    assert_parity_expected_divergences(&parity_result);
}

/// Test empty partition behavior and manifest entry parity after DELETE.
#[tokio::test]
async fn test_semantic_parity_empty_partition_delete() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let ctx = SessionContext::new();
    let namespace = iceberg::NamespaceIdent::new("default".to_string());
    let spark_container = fixture.spark_container_name();

    let provider =
        IcebergTableProvider::try_new(client.clone(), namespace.clone(), "parity_partition_rust")
            .await
            .unwrap();

    let deleted_count = provider
        .delete(&ctx.state(), Some(col("category").eq(lit("B"))))
        .await
        .expect("Rust DELETE should succeed");

    assert_eq!(deleted_count, 1, "Rust should delete 1 row");

    let rust_table = client
        .load_table(&TableIdent::from_strs(["default", "parity_partition_rust"]).unwrap())
        .await
        .unwrap();
    let rust_summary = extract_rust_snapshot_summary(&rust_table);

    let spark_summary = spark_execute_dml_with_container(
        &spark_container,
        "parity_partition_spark",
        "delete",
        Some("category = 'B'"),
        None,
    )
    .await
    .expect("Spark DELETE should succeed");

    let parity_result =
        compare_snapshot_summaries("DELETE (empty partition)", &rust_summary, &spark_summary);

    println!("\n{}\n", parity_result.summary());
    let divergences = parity_result.divergences();
    if !divergences.is_empty() {
        println!("EMPTY PARTITION DIVERGENCES:");
        for d in &divergences {
            println!(
                "  - {}: Rust={:?}, Spark={:?}",
                d.field_name, d.rust_value, d.spark_value
            );
        }
    }

    assert_parity_expected_divergences(&parity_result);

    let rust_entries = extract_rust_manifest_entries(&rust_table)
        .await
        .expect("Rust manifest entry extraction should succeed");
    let spark_entries =
        spark_manifest_entries_with_container(&spark_container, "parity_partition_spark", Some(50))
            .await
            .expect("Spark manifest entry extraction should succeed");

    let manifest_parity =
        compare_manifest_entries("DELETE (empty partition)", &rust_entries, &spark_entries);
    println!("\n{}\n", manifest_parity.summary());

    assert!(
        manifest_parity.semantic_parity,
        "Manifest parity failed for {}",
        manifest_parity.operation
    );
}

/// Error rejection parity: adding an existing column should fail.
#[tokio::test]
async fn test_error_rejection_add_existing_column() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let table = client
        .load_table(&TableIdent::from_strs(["default", "parity_schema_add_existing_rust"]).unwrap())
        .await
        .unwrap();

    let tx = Transaction::new(&table);
    let apply_result = tx
        .update_schema()
        .add_column("id", Type::Primitive(PrimitiveType::Int))
        .apply(tx);

    let rust_err = match apply_result {
        Ok(tx) => tx
            .commit(client.as_ref())
            .await
            .expect_err("Rust should reject adding existing column"),
        Err(err) => err,
    };
    assert_rust_schema_rejection(&rust_err, &["already exists"]);

    let spark_container = fixture.spark_container_name();
    let spark_result = spark_execute_sql_with_container(
        &spark_container,
        "parity_schema_add_existing_spark",
        "ALTER TABLE {table} ADD COLUMNS (id INT)",
    )
    .await;
    let spark_err = spark_result.expect_err("Spark should reject adding existing column");
    assert_spark_schema_rejection(
        &spark_err,
        &["column_already_exists", "already exists", "duplicate column"],
    );
}

/// Error rejection parity: renaming to an existing column should fail.
#[tokio::test]
async fn test_error_rejection_rename_to_existing_column() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let table = client
        .load_table(&TableIdent::from_strs(["default", "parity_schema_rename_existing_rust"]).unwrap())
        .await
        .unwrap();

    let tx = Transaction::new(&table);
    let apply_result = tx
        .update_schema()
        .rename_column("id", "name")
        .apply(tx);

    let rust_err = match apply_result {
        Ok(tx) => tx
            .commit(client.as_ref())
            .await
            .expect_err("Rust should reject renaming to an existing column"),
        Err(err) => err,
    };
    assert_rust_schema_rejection(&rust_err, &["already exists"]);

    let spark_container = fixture.spark_container_name();
    let spark_result = spark_execute_sql_with_container(
        &spark_container,
        "parity_schema_rename_existing_spark",
        "ALTER TABLE {table} RENAME COLUMN id TO name",
    )
    .await;
    let spark_err = spark_result.expect_err("Spark should reject renaming to an existing column");
    assert_spark_schema_rejection(
        &spark_err,
        &["column_already_exists", "already exists", "duplicate column"],
    );
}

#[tokio::test]
async fn test_error_rejection_drop_missing_column() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let table = client
        .load_table(&TableIdent::from_strs(["default", "parity_schema_drop_missing_rust"]).unwrap())
        .await
        .unwrap();

    let tx = Transaction::new(&table);
    let apply_result = tx.update_schema().drop_column("missing_column").apply(tx);

    let rust_err = match apply_result {
        Ok(tx) => tx
            .commit(client.as_ref())
            .await
            .expect_err("Rust should reject dropping a missing column"),
        Err(err) => err,
    };
    assert_rust_schema_rejection(&rust_err, &["does not exist", "not exist"]);

    let spark_container = fixture.spark_container_name();
    let spark_result = spark_execute_sql_with_container(
        &spark_container,
        "parity_schema_drop_missing_spark",
        "ALTER TABLE {table} DROP COLUMN missing_column",
    )
    .await;
    let spark_err = spark_result.expect_err("Spark should reject dropping a missing column");
    assert_spark_schema_rejection(
        &spark_err,
        &["missing field", "does not exist", "not found", "cannot resolve"],
    );
}

#[tokio::test]
async fn test_error_rejection_rename_missing_column() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let table = client
        .load_table(&TableIdent::from_strs(["default", "parity_schema_rename_missing_rust"]).unwrap())
        .await
        .unwrap();

    let tx = Transaction::new(&table);
    let apply_result = tx
        .update_schema()
        .rename_column("missing_column", "new_name")
        .apply(tx);

    let rust_err = match apply_result {
        Ok(tx) => tx
            .commit(client.as_ref())
            .await
            .expect_err("Rust should reject renaming a missing column"),
        Err(err) => err,
    };
    assert_rust_schema_rejection(&rust_err, &["does not exist", "not exist"]);

    let spark_container = fixture.spark_container_name();
    let spark_result = spark_execute_sql_with_container(
        &spark_container,
        "parity_schema_rename_missing_spark",
        "ALTER TABLE {table} RENAME COLUMN missing_column TO new_name",
    )
    .await;
    let spark_err = spark_result.expect_err("Spark should reject renaming a missing column");
    assert_spark_schema_rejection(
        &spark_err,
        &["missing field", "does not exist", "not found", "cannot resolve"],
    );
}

#[tokio::test]
async fn test_error_rejection_incompatible_type_promotion() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let ctx = SessionContext::new();
    let namespace = iceberg::NamespaceIdent::new("default".to_string());

    let provider =
        IcebergTableProvider::try_new(client.clone(), namespace.clone(), "parity_schema_incompatible_rust")
            .await
            .unwrap();

    let rust_result = provider
        .update()
        .await
        .unwrap()
        .set("value", lit("not-a-number"))
        .filter(col("id").eq(lit(1)))
        .execute(&ctx.state())
        .await;

    let rust_err = rust_result.expect_err("Rust should reject incompatible type update");
    let rust_message = rust_err.to_string();
    assert_error_contains(
        &rust_message,
        &[
            "column types must match schema types",
            "expected int32",
            "found utf8",
            "transformed batch",
            "schema evolution cast",
        ],
        "Rust update",
    );

    let spark_container = fixture.spark_container_name();
    let spark_result = spark_execute_sql_with_container(
        &spark_container,
        "parity_schema_incompatible_spark",
        "UPDATE {table} SET value = 'not-a-number' WHERE id = 1",
    )
    .await;
    let spark_err = spark_result.expect_err("Spark should reject incompatible type update");
    assert_error_contains(
        &spark_err.message,
        &[
            "cannot safely cast",
            "cannot cast",
            "cannot parse",
            "number format",
            "for input string",
        ],
        "Spark update",
    );
}
