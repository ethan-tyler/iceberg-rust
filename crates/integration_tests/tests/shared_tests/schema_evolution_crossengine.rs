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

//! WP6.1 Schema Evolution Cross-Engine Tests (Spark DDL -> Rust Reads)
//!
//! These tests verify that iceberg-rust correctly reads tables after Spark
//! performs schema evolution operations (ADD COLUMN, DROP COLUMN, RENAME COLUMN).
//!
//! Key tests:
//! - Field ID semantics: Iceberg uses field IDs (not names) for column resolution
//! - Historical snapshot reads: Reading prior snapshots with different schemas
//! - Type promotion: Reading promoted types correctly
//! - Nested type evolution: Struct field additions
//!
//! Tables are provisioned by Spark in `provision.py` under the WP6.1 section.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use datafusion::assert_batches_sorted_eq;
use datafusion::prelude::*;
use futures::TryStreamExt;
use iceberg::spec::{DataFileFormat, NestedField, PrimitiveType, Schema, Type};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, TableCreation, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_datafusion::IcebergTableProvider;
use iceberg_integration_tests::spark_validator::{
    ValidationType, spark_validate_query_with_container, spark_validate_with_container,
};
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use crate::get_shared_containers;
use crate::shared_tests::random_ns;

// =============================================================================
// ADD COLUMN Tests
// =============================================================================

/// Test: Spark ADD COLUMN -> Rust reads correctly
///
/// Setup (by Spark provision.py):
/// - Table `test_schema_add_column` created with (id, name)
/// - 3 rows inserted (snapshot 1)
/// - Columns `age` and `email` added
/// - 2 more rows inserted with all columns (snapshot 2)
///
/// Expected behavior:
/// - Rows 1-3: age=NULL, email=NULL
/// - Rows 4-5: all columns populated
#[tokio::test]
async fn test_spark_add_column_rust_reads() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let table = rest_catalog
        .load_table(&TableIdent::from_strs(["default", "test_schema_add_column"]).unwrap())
        .await
        .unwrap();

    // Verify schema has evolved - should have 4 columns now
    let schema = table.metadata().current_schema();
    let field_names: Vec<&str> = schema
        .as_struct()
        .fields()
        .iter()
        .map(|f| f.name.as_str())
        .collect();
    assert_eq!(
        field_names,
        vec!["id", "name", "age", "email"],
        "Schema should have all 4 columns after ADD COLUMN"
    );

    // Read and verify data
    let scan = table.scan().build().unwrap();
    let batch_stream = scan.to_arrow().await.unwrap();
    let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

    // Count total rows
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5, "Should have 5 total rows");

    // Collect all data for verification
    let mut all_ids = Vec::new();
    let mut all_names = Vec::new();
    let mut all_ages = Vec::new();
    let mut all_emails = Vec::new();

    for batch in &batches {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ages = batch
            .column_by_name("age")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let emails = batch
            .column_by_name("email")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        for i in 0..batch.num_rows() {
            all_ids.push(ids.value(i));
            all_names.push(names.value(i).to_string());
            all_ages.push(if ages.is_null(i) {
                None
            } else {
                Some(ages.value(i))
            });
            all_emails.push(if emails.is_null(i) {
                None
            } else {
                Some(emails.value(i).to_string())
            });
        }
    }

    // Sort by ID for consistent verification
    let mut data: Vec<_> = all_ids
        .iter()
        .zip(all_names.iter())
        .zip(all_ages.iter())
        .zip(all_emails.iter())
        .map(|(((id, name), age), email)| (*id, name.clone(), *age, email.clone()))
        .collect();
    data.sort_by_key(|(id, _, _, _)| *id);

    // Verify pre-evolution rows have NULL for new columns
    assert_eq!(data[0], (1, "Alice".to_string(), None, None));
    assert_eq!(data[1], (2, "Bob".to_string(), None, None));
    assert_eq!(data[2], (3, "Charlie".to_string(), None, None));

    // Verify post-evolution rows have all columns populated
    assert_eq!(
        data[3],
        (
            4,
            "Diana".to_string(),
            Some(25),
            Some("diana@example.com".to_string())
        )
    );
    assert_eq!(
        data[4],
        (
            5,
            "Eve".to_string(),
            Some(30),
            Some("eve@example.com".to_string())
        )
    );

    // Cross-engine validation with Spark
    let container = fixture.spark_container_name();
    let spark_result = spark_validate_with_container(
        &container,
        "default.test_schema_add_column",
        ValidationType::Full,
    )
    .await
    .unwrap();

    assert_eq!(
        spark_result.count.unwrap(),
        5,
        "Spark should see 5 rows too"
    );
    assert!(
        spark_result.columns.unwrap().contains(&"age".to_string()),
        "Spark should see age column"
    );
}

// =============================================================================
// DROP COLUMN Tests
// =============================================================================

/// Test: Spark DROP COLUMN -> Rust reads correctly
///
/// Setup (by Spark provision.py):
/// - Table `test_schema_drop_column` created with (id, name, to_drop, value)
/// - 3 rows inserted (snapshot 1)
/// - Column `to_drop` dropped
/// - 2 more rows inserted (snapshot 2)
///
/// Expected behavior:
/// - Schema should NOT contain `to_drop` column
/// - All 5 rows should be readable with (id, name, value)
#[tokio::test]
async fn test_spark_drop_column_rust_reads() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let table = rest_catalog
        .load_table(&TableIdent::from_strs(["default", "test_schema_drop_column"]).unwrap())
        .await
        .unwrap();

    // Verify schema has the dropped column removed
    let schema = table.metadata().current_schema();
    let field_names: Vec<&str> = schema
        .as_struct()
        .fields()
        .iter()
        .map(|f| f.name.as_str())
        .collect();
    assert_eq!(
        field_names,
        vec!["id", "name", "value"],
        "Schema should NOT contain 'to_drop' column"
    );
    assert!(
        !field_names.contains(&"to_drop"),
        "Dropped column should not be visible"
    );

    // Read and verify data
    let scan = table.scan().build().unwrap();
    let batch_stream = scan.to_arrow().await.unwrap();
    let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

    // Count total rows
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5, "Should have 5 total rows");

    // Collect and verify data
    let mut all_data = Vec::new();
    for batch in &batches {
        // Ensure to_drop column is NOT in the batch
        assert!(
            batch.column_by_name("to_drop").is_none(),
            "Dropped column should not be in record batch"
        );

        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        for i in 0..batch.num_rows() {
            all_data.push((ids.value(i), names.value(i).to_string(), values.value(i)));
        }
    }

    all_data.sort_by_key(|(id, _, _)| *id);

    // Verify all rows
    assert_eq!(all_data[0], (1, "Alice".to_string(), 100));
    assert_eq!(all_data[1], (2, "Bob".to_string(), 200));
    assert_eq!(all_data[2], (3, "Charlie".to_string(), 300));
    assert_eq!(all_data[3], (4, "Diana".to_string(), 400));
    assert_eq!(all_data[4], (5, "Eve".to_string(), 500));

    // Cross-engine validation with Spark
    let container = fixture.spark_container_name();
    let spark_result = spark_validate_with_container(
        &container,
        "default.test_schema_drop_column",
        ValidationType::Full,
    )
    .await
    .unwrap();

    assert_eq!(
        spark_result.count.unwrap(),
        5,
        "Spark should see 5 rows too"
    );
    let spark_columns = spark_result.columns.unwrap();
    assert!(
        !spark_columns.contains(&"to_drop".to_string()),
        "Spark should not see dropped column"
    );
}

// =============================================================================
// RENAME COLUMN Tests (existing coverage extension)
// =============================================================================

/// Test: Combined RENAME COLUMN + ADD COLUMN -> Rust reads correctly
///
/// Setup (by Spark provision.py):
/// - Table `test_schema_rename_then_add` created with (id, old_name)
/// - 2 rows inserted
/// - Column renamed: old_name -> new_name
/// - Column added: category
/// - 2 more rows inserted
///
/// Expected behavior:
/// - Field ID for renamed column should be preserved
/// - Old data readable via new name
/// - New column has NULL for old rows
#[tokio::test]
async fn test_spark_rename_then_add_column_rust_reads() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let table = rest_catalog
        .load_table(&TableIdent::from_strs(["default", "test_schema_rename_then_add"]).unwrap())
        .await
        .unwrap();

    // Verify schema
    let schema = table.metadata().current_schema();
    let field_names: Vec<&str> = schema
        .as_struct()
        .fields()
        .iter()
        .map(|f| f.name.as_str())
        .collect();
    assert_eq!(
        field_names,
        vec!["id", "new_name", "category"],
        "Schema should have renamed column and added column"
    );
    assert!(
        !field_names.contains(&"old_name"),
        "Old column name should not exist"
    );

    // Read and verify
    let scan = table.scan().build().unwrap();
    let batch_stream = scan.to_arrow().await.unwrap();
    let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4, "Should have 4 total rows");

    // Collect data
    let mut all_data = Vec::new();
    for batch in &batches {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = batch
            .column_by_name("new_name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let categories = batch
            .column_by_name("category")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        for i in 0..batch.num_rows() {
            all_data.push((
                ids.value(i),
                names.value(i).to_string(),
                if categories.is_null(i) {
                    None
                } else {
                    Some(categories.value(i).to_string())
                },
            ));
        }
    }

    all_data.sort_by_key(|(id, _, _)| *id);

    // Pre-rename data (accessed via new_name due to field ID)
    assert_eq!(all_data[0], (1, "Alpha".to_string(), None));
    assert_eq!(all_data[1], (2, "Beta".to_string(), None));

    // Post-evolution data
    assert_eq!(all_data[2], (3, "Gamma".to_string(), Some("A".to_string())));
    assert_eq!(all_data[3], (4, "Delta".to_string(), Some("B".to_string())));

    // Cross-engine validation
    let container = fixture.spark_container_name();
    let spark_result = spark_validate_with_container(
        &container,
        "default.test_schema_rename_then_add",
        ValidationType::Full,
    )
    .await
    .unwrap();

    assert_eq!(spark_result.count.unwrap(), 4);
}

// =============================================================================
// Type Promotion Tests
// =============================================================================

/// Test: Multiple type promotions in sequence -> Rust reads correctly
///
/// Setup (by Spark provision.py):
/// - Table `test_schema_type_promotion_chain` with (id, count) where count is int
/// - 2 rows inserted
/// - count promoted to bigint
/// - 2 more rows inserted with values > INT_MAX
///
/// Expected behavior:
/// - All values readable as bigint
/// - Values > INT_MAX preserved correctly
#[tokio::test]
async fn test_spark_type_promotion_chain_rust_reads() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let table = rest_catalog
        .load_table(
            &TableIdent::from_strs(["default", "test_schema_type_promotion_chain"]).unwrap(),
        )
        .await
        .unwrap();

    // Read and verify
    let scan = table.scan().build().unwrap();
    let batch_stream = scan.to_arrow().await.unwrap();
    let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4, "Should have 4 total rows");

    // Collect count values
    let mut all_counts = Vec::new();
    for batch in &batches {
        let counts = batch
            .column_by_name("count")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        for i in 0..batch.num_rows() {
            all_counts.push(counts.value(i));
        }
    }

    all_counts.sort();

    // Verify values including those > INT_MAX
    assert_eq!(all_counts[0], 100);
    assert_eq!(all_counts[1], 200);
    assert_eq!(all_counts[2], 3_000_000_000); // > INT_MAX
    assert_eq!(all_counts[3], 4_000_000_000); // > INT_MAX

    // Cross-engine validation
    let container = fixture.spark_container_name();
    let spark_result = spark_validate_with_container(
        &container,
        "default.test_schema_type_promotion_chain",
        ValidationType::Full,
    )
    .await
    .unwrap();

    assert_eq!(spark_result.count.unwrap(), 4);
}

// =============================================================================
// Nested Type Evolution Tests
// =============================================================================

/// Test: Spark adds field to struct -> Rust reads correctly
///
/// Setup (by Spark provision.py):
/// - Table `test_schema_nested_evolution` with struct<name, city>
/// - 2 rows inserted
/// - Field `country` added to struct
/// - 2 more rows inserted with country
///
/// Expected behavior:
/// - Old rows: info.country = NULL
/// - New rows: info.country populated
#[tokio::test]
async fn test_spark_nested_struct_evolution_rust_reads() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let table = rest_catalog
        .load_table(&TableIdent::from_strs(["default", "test_schema_nested_evolution"]).unwrap())
        .await
        .unwrap();

    // Verify schema has evolved struct
    let schema = table.metadata().current_schema();
    let info_field = schema
        .field_by_name("info")
        .expect("info field should exist");
    if let iceberg::spec::Type::Struct(struct_type) = info_field.field_type.as_ref() {
        let struct_field_names: Vec<&str> = struct_type
            .fields()
            .iter()
            .map(|f| f.name.as_str())
            .collect();
        assert!(
            struct_field_names.contains(&"country"),
            "Struct should have 'country' field after evolution"
        );
    } else {
        panic!("info field should be a struct");
    }

    // Read and verify
    let scan = table.scan().build().unwrap();
    let batch_stream = scan.to_arrow().await.unwrap();
    let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4, "Should have 4 total rows");

    // Cross-engine validation with Spark
    let container = fixture.spark_container_name();
    let spark_result = spark_validate_with_container(
        &container,
        "default.test_schema_nested_evolution",
        ValidationType::Count,
    )
    .await
    .unwrap();

    assert_eq!(spark_result.count.unwrap(), 4);

    // Query to verify struct field values
    let query_result = spark_validate_query_with_container(
        &container,
        "default.test_schema_nested_evolution",
        "SELECT id, info.name, info.city, info.country FROM {table} ORDER BY id",
    )
    .await
    .unwrap();

    let rows = query_result.rows.unwrap();
    assert_eq!(rows.len(), 4);

    // Verify old rows have NULL country
    assert_eq!(rows[0]["id"], 1);
    assert!(
        rows[0]["country"].is_null(),
        "Pre-evolution rows should have NULL country"
    );
    assert_eq!(rows[1]["id"], 2);
    assert!(
        rows[1]["country"].is_null(),
        "Pre-evolution rows should have NULL country"
    );

    // Verify new rows have country
    assert_eq!(rows[2]["id"], 3);
    assert_eq!(rows[2]["country"], "UK");
    assert_eq!(rows[3]["id"], 4);
    assert_eq!(rows[3]["country"], "France");
}

// =============================================================================
// Historical Snapshot Tests
// =============================================================================

/// Test: Read historical snapshots with prior schema
///
/// Setup (by Spark provision.py):
/// - Table `test_schema_historical` with multiple schema evolutions
/// - Snapshot 1: (id, name)
/// - Snapshot 2: (id, name, version) after ADD COLUMN
/// - Snapshot 3: (id, label, version) after RENAME COLUMN name -> label
///
/// Expected behavior:
/// - Current snapshot readable with latest schema
/// - Historical snapshots readable with their respective schemas
#[tokio::test]
async fn test_historical_snapshot_schema_reads() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let table = rest_catalog
        .load_table(&TableIdent::from_strs(["default", "test_schema_historical"]).unwrap())
        .await
        .unwrap();

    // Read current snapshot
    let scan = table.scan().build().unwrap();
    let batch_stream = scan.to_arrow().await.unwrap();
    let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "Should have 3 total rows from all snapshots");

    // Verify current schema has renamed column
    let schema = table.metadata().current_schema();
    let field_names: Vec<&str> = schema
        .as_struct()
        .fields()
        .iter()
        .map(|f| f.name.as_str())
        .collect();
    assert!(
        field_names.contains(&"label"),
        "Current schema should have 'label' (renamed from 'name')"
    );
    assert!(
        !field_names.contains(&"name"),
        "Current schema should NOT have 'name' (was renamed)"
    );
    assert!(
        field_names.contains(&"version"),
        "Current schema should have 'version' column"
    );

    // Verify data - all rows should be accessible via current schema
    // Field ID semantics: old 'name' data accessible via 'label'
    let mut all_data = Vec::new();
    for batch in &batches {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let labels = batch
            .column_by_name("label")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let versions = batch
            .column_by_name("version")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        for i in 0..batch.num_rows() {
            all_data.push((
                ids.value(i),
                labels.value(i).to_string(),
                if versions.is_null(i) {
                    None
                } else {
                    Some(versions.value(i))
                },
            ));
        }
    }

    all_data.sort_by_key(|(id, _, _)| *id);

    // Row from snapshot 1 (original schema) - version should be NULL
    assert_eq!(all_data[0], (1, "First".to_string(), None));

    // Row from snapshot 2 (after ADD COLUMN version)
    assert_eq!(all_data[1], (2, "Second".to_string(), Some(2)));

    // Row from snapshot 3 (after RENAME COLUMN)
    assert_eq!(all_data[2], (3, "Third".to_string(), Some(3)));

    // Cross-engine validation
    let container = fixture.spark_container_name();
    let spark_result = spark_validate_with_container(
        &container,
        "default.test_schema_historical",
        ValidationType::Full,
    )
    .await
    .unwrap();

    assert_eq!(spark_result.count.unwrap(), 3);
}

// =============================================================================
// ADD COLUMN with DEFAULT Tests
// =============================================================================

/// Test: ADD COLUMN with DEFAULT value -> Rust reads correctly
///
/// Setup (by Spark provision.py):
/// - Table `test_schema_add_column_default` with (id, name)
/// - 2 rows inserted
/// - Column `status` added with DEFAULT 'active'
/// - 2 rows inserted without explicit status (should get default)
/// - 1 row inserted with explicit status
///
/// Expected behavior:
/// - Pre-evolution rows: status = NULL (no default applied to existing data)
/// - New rows without explicit: status = 'active' (default)
/// - New rows with explicit: status = specified value
#[tokio::test]
async fn test_spark_add_column_with_default_rust_reads() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let table = rest_catalog
        .load_table(&TableIdent::from_strs(["default", "test_schema_add_column_default"]).unwrap())
        .await
        .unwrap();

    // Read and verify
    let scan = table.scan().build().unwrap();
    let batch_stream = scan.to_arrow().await.unwrap();
    let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5, "Should have 5 total rows");

    // Collect data
    let mut all_data = Vec::new();
    for batch in &batches {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let statuses = batch
            .column_by_name("status")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        for i in 0..batch.num_rows() {
            all_data.push((
                ids.value(i),
                names.value(i).to_string(),
                if statuses.is_null(i) {
                    None
                } else {
                    Some(statuses.value(i).to_string())
                },
            ));
        }
    }

    all_data.sort_by_key(|(id, _, _)| *id);

    // Pre-evolution rows - NULL (defaults don't apply retroactively)
    assert_eq!(all_data[0], (1, "Alpha".to_string(), None));
    assert_eq!(all_data[1], (2, "Beta".to_string(), None));

    // New rows inserted without explicit status - default applied
    assert_eq!(
        all_data[2],
        (3, "Gamma".to_string(), Some("active".to_string()))
    );
    assert_eq!(
        all_data[3],
        (4, "Delta".to_string(), Some("active".to_string()))
    );

    // Explicit status
    assert_eq!(
        all_data[4],
        (5, "Epsilon".to_string(), Some("inactive".to_string()))
    );

    // Cross-engine validation
    let container = fixture.spark_container_name();
    let spark_result = spark_validate_with_container(
        &container,
        "default.test_schema_add_column_default",
        ValidationType::Full,
    )
    .await
    .unwrap();

    assert_eq!(spark_result.count.unwrap(), 5);
}

// =============================================================================
// DataFusion Integration Tests
// =============================================================================

/// Test: Schema evolution via DataFusion SQL queries
///
/// Verifies that evolved tables are correctly queryable via DataFusion SQL.
#[tokio::test]
async fn test_schema_evolution_datafusion_sql() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let client = Arc::new(rest_catalog);
    let ctx = SessionContext::new();
    let namespace = iceberg::NamespaceIdent::new("default".to_string());

    // Register evolved table
    let provider =
        IcebergTableProvider::try_new(client.clone(), namespace.clone(), "test_schema_add_column")
            .await
            .unwrap();

    ctx.register_table("evolved_table", Arc::new(provider))
        .unwrap();

    // Query with all columns
    let df = ctx
        .sql("SELECT * FROM evolved_table ORDER BY id")
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();

    let expected = [
        "+----+---------+-----+-------------------+",
        "| id | name    | age | email             |",
        "+----+---------+-----+-------------------+",
        "| 1  | Alice   |     |                   |",
        "| 2  | Bob     |     |                   |",
        "| 3  | Charlie |     |                   |",
        "| 4  | Diana   | 25  | diana@example.com |",
        "| 5  | Eve     | 30  | eve@example.com   |",
        "+----+---------+-----+-------------------+",
    ];
    assert_batches_sorted_eq!(expected, &batches);

    // Query filtering on new column
    let df = ctx
        .sql("SELECT id, name FROM evolved_table WHERE age IS NOT NULL ORDER BY id")
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();

    let expected = [
        "+----+-------+",
        "| id | name  |",
        "+----+-------+",
        "| 4  | Diana |",
        "| 5  | Eve   |",
        "+----+-------+",
    ];
    assert_batches_sorted_eq!(expected, &batches);
}

// =============================================================================
// Rust DDL -> Spark Reads
// =============================================================================

/// Test: Rust ADD COLUMN -> Spark reads correctly
#[tokio::test]
async fn test_rust_add_column_spark_reads() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns().await;
    let table_name = format!("rust_schema_add_column_{}", Uuid::new_v4().simple());

    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .unwrap();

    let table_creation = TableCreation::builder()
        .name(table_name.clone())
        .schema(schema)
        .build();

    let mut table = rest_catalog
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
    let batch = RecordBatch::try_new(arrow_schema, vec![
        Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
        Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])) as ArrayRef,
    ])
    .unwrap();
    table = append_batch(&rest_catalog, &table, batch).await;

    let tx = Transaction::new(&table);
    let tx = tx
        .update_schema()
        .add_column("age", Type::Primitive(PrimitiveType::Int))
        .add_column("email", Type::Primitive(PrimitiveType::String))
        .apply(tx)
        .unwrap();
    table = tx.commit(&rest_catalog).await.unwrap();

    let arrow_schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );
    let batch = RecordBatch::try_new(arrow_schema, vec![
        Arc::new(Int32Array::from(vec![4, 5])) as ArrayRef,
        Arc::new(StringArray::from(vec!["Diana", "Eve"])) as ArrayRef,
        Arc::new(Int32Array::from(vec![Some(25), Some(30)])) as ArrayRef,
        Arc::new(StringArray::from(vec![
            "diana@example.com",
            "eve@example.com",
        ])) as ArrayRef,
    ])
    .unwrap();
    table = append_batch(&rest_catalog, &table, batch).await;

    let table_ref = format!("{}.{}", ns.name(), table_name);
    let container = fixture.spark_container_name();
    let spark_result = spark_validate_with_container(&container, &table_ref, ValidationType::Full)
        .await
        .unwrap();

    assert_eq!(spark_result.count.unwrap(), 5);
    let spark_columns = spark_result.columns.unwrap();
    assert!(spark_columns.contains(&"age".to_string()));
    assert!(spark_columns.contains(&"email".to_string()));
    assert!(spark_result.checksum.is_some());

    let query_result = spark_validate_query_with_container(
        &container,
        &table_ref,
        "SELECT id, name, age, email FROM {table} ORDER BY id",
    )
    .await
    .unwrap();

    let rows = query_result.rows.unwrap();
    assert_eq!(rows.len(), 5);
    assert!(rows[0]["age"].is_null());
    assert!(rows[0]["email"].is_null());
    assert!(rows[1]["age"].is_null());
    assert!(rows[2]["age"].is_null());
    assert_eq!(rows[3]["age"], 25);
    assert_eq!(rows[3]["email"], "diana@example.com");
    assert_eq!(rows[4]["age"], 30);
    assert_eq!(rows[4]["email"], "eve@example.com");
}

/// Test: Rust RENAME COLUMN -> Spark reads correctly
#[tokio::test]
async fn test_rust_rename_column_spark_reads() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns().await;
    let table_name = format!("rust_schema_rename_column_{}", Uuid::new_v4().simple());

    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "old_name", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .unwrap();

    let table_creation = TableCreation::builder()
        .name(table_name.clone())
        .schema(schema)
        .build();

    let mut table = rest_catalog
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
    let batch = RecordBatch::try_new(arrow_schema, vec![
        Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
        Arc::new(StringArray::from(vec!["Alpha", "Beta"])) as ArrayRef,
    ])
    .unwrap();
    table = append_batch(&rest_catalog, &table, batch).await;

    let tx = Transaction::new(&table);
    let tx = tx
        .update_schema()
        .rename_column("old_name", "new_name")
        .apply(tx)
        .unwrap();
    table = tx.commit(&rest_catalog).await.unwrap();

    let renamed_field = table
        .metadata()
        .current_schema()
        .field_by_name("new_name")
        .unwrap();
    assert_eq!(renamed_field.id, 2);

    let arrow_schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );
    let batch = RecordBatch::try_new(arrow_schema, vec![
        Arc::new(Int32Array::from(vec![3, 4])) as ArrayRef,
        Arc::new(StringArray::from(vec!["Gamma", "Delta"])) as ArrayRef,
    ])
    .unwrap();
    table = append_batch(&rest_catalog, &table, batch).await;

    let table_ref = format!("{}.{}", ns.name(), table_name);
    let container = fixture.spark_container_name();
    let spark_result = spark_validate_with_container(&container, &table_ref, ValidationType::Full)
        .await
        .unwrap();

    assert_eq!(spark_result.count.unwrap(), 4);
    let spark_columns = spark_result.columns.unwrap();
    assert!(spark_columns.contains(&"new_name".to_string()));
    assert!(!spark_columns.contains(&"old_name".to_string()));
    assert!(spark_result.checksum.is_some());

    let query_result = spark_validate_query_with_container(
        &container,
        &table_ref,
        "SELECT id, new_name FROM {table} ORDER BY id",
    )
    .await
    .unwrap();

    let rows = query_result.rows.unwrap();
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0]["new_name"], "Alpha");
    assert_eq!(rows[1]["new_name"], "Beta");
    assert_eq!(rows[2]["new_name"], "Gamma");
    assert_eq!(rows[3]["new_name"], "Delta");
}

/// Test: Rust DROP COLUMN -> Spark reads correctly
#[tokio::test]
async fn test_rust_drop_column_spark_reads() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns().await;
    let table_name = format!("rust_schema_drop_column_{}", Uuid::new_v4().simple());

    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "to_drop", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(4, "value", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .unwrap();

    let table_creation = TableCreation::builder()
        .name(table_name.clone())
        .schema(schema)
        .build();

    let mut table = rest_catalog
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
    let batch = RecordBatch::try_new(arrow_schema, vec![
        Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
        Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])) as ArrayRef,
        Arc::new(StringArray::from(vec!["x", "y", "z"])) as ArrayRef,
        Arc::new(Int32Array::from(vec![100, 200, 300])) as ArrayRef,
    ])
    .unwrap();
    table = append_batch(&rest_catalog, &table, batch).await;

    let tx = Transaction::new(&table);
    let tx = tx.update_schema().drop_column("to_drop").apply(tx).unwrap();
    table = tx.commit(&rest_catalog).await.unwrap();

    let arrow_schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );
    let batch = RecordBatch::try_new(arrow_schema, vec![
        Arc::new(Int32Array::from(vec![4, 5])) as ArrayRef,
        Arc::new(StringArray::from(vec!["Diana", "Eve"])) as ArrayRef,
        Arc::new(Int32Array::from(vec![400, 500])) as ArrayRef,
    ])
    .unwrap();
    table = append_batch(&rest_catalog, &table, batch).await;

    let table_ref = format!("{}.{}", ns.name(), table_name);
    let container = fixture.spark_container_name();
    let spark_result = spark_validate_with_container(&container, &table_ref, ValidationType::Full)
        .await
        .unwrap();

    assert_eq!(spark_result.count.unwrap(), 5);
    let spark_columns = spark_result.columns.unwrap();
    assert!(!spark_columns.contains(&"to_drop".to_string()));
    assert!(spark_result.checksum.is_some());

    let query_result = spark_validate_query_with_container(
        &container,
        &table_ref,
        "SELECT id, name, value FROM {table} ORDER BY id",
    )
    .await
    .unwrap();

    let rows = query_result.rows.unwrap();
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0]["value"], 100);
    assert_eq!(rows[1]["value"], 200);
    assert_eq!(rows[2]["value"], 300);
    assert_eq!(rows[3]["value"], 400);
    assert_eq!(rows[4]["value"], 500);
}

/// Test: Rust schema evolution historical snapshots -> Spark reads correctly
#[tokio::test]
async fn test_rust_historical_snapshot_schema_spark_reads() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns().await;
    let table_name = format!("rust_schema_historical_{}", Uuid::new_v4().simple());

    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .unwrap();

    let table_creation = TableCreation::builder()
        .name(table_name.clone())
        .schema(schema)
        .build();

    let mut table = rest_catalog
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
    let batch = RecordBatch::try_new(arrow_schema, vec![
        Arc::new(Int32Array::from(vec![1])) as ArrayRef,
        Arc::new(StringArray::from(vec!["First"])) as ArrayRef,
    ])
    .unwrap();
    table = append_batch(&rest_catalog, &table, batch).await;
    let snapshot_1 = table.metadata().current_snapshot().unwrap().snapshot_id();

    let tx = Transaction::new(&table);
    let tx = tx
        .update_schema()
        .add_column("version", Type::Primitive(PrimitiveType::Int))
        .apply(tx)
        .unwrap();
    table = tx.commit(&rest_catalog).await.unwrap();

    let arrow_schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );
    let batch = RecordBatch::try_new(arrow_schema, vec![
        Arc::new(Int32Array::from(vec![2])) as ArrayRef,
        Arc::new(StringArray::from(vec!["Second"])) as ArrayRef,
        Arc::new(Int32Array::from(vec![Some(2)])) as ArrayRef,
    ])
    .unwrap();
    table = append_batch(&rest_catalog, &table, batch).await;
    let snapshot_2 = table.metadata().current_snapshot().unwrap().snapshot_id();

    let tx = Transaction::new(&table);
    let tx = tx
        .update_schema()
        .rename_column("name", "label")
        .apply(tx)
        .unwrap();
    table = tx.commit(&rest_catalog).await.unwrap();

    let arrow_schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );
    let batch = RecordBatch::try_new(arrow_schema, vec![
        Arc::new(Int32Array::from(vec![3])) as ArrayRef,
        Arc::new(StringArray::from(vec!["Third"])) as ArrayRef,
        Arc::new(Int32Array::from(vec![Some(3)])) as ArrayRef,
    ])
    .unwrap();
    table = append_batch(&rest_catalog, &table, batch).await;
    let snapshot_3 = table.metadata().current_snapshot().unwrap().snapshot_id();

    let table_ref = format!("{}.{}", ns.name(), table_name);
    let container = fixture.spark_container_name();

    let query_1 = format!(
        "SELECT id, name FROM {{table}} VERSION AS OF {} ORDER BY id",
        snapshot_1
    );
    let result_1 = spark_validate_query_with_container(&container, &table_ref, &query_1)
        .await
        .unwrap();
    let rows_1 = result_1.rows.unwrap();
    assert_eq!(rows_1.len(), 1);
    assert_eq!(rows_1[0]["id"], 1);
    assert_eq!(rows_1[0]["name"], "First");

    let query_2 = format!(
        "SELECT id, name, version FROM {{table}} VERSION AS OF {} ORDER BY id",
        snapshot_2
    );
    let result_2 = spark_validate_query_with_container(&container, &table_ref, &query_2)
        .await
        .unwrap();
    let rows_2 = result_2.rows.unwrap();
    assert_eq!(rows_2.len(), 2);
    assert!(rows_2[0]["version"].is_null());
    assert_eq!(rows_2[1]["version"], 2);

    let query_3 = format!(
        "SELECT id, label, version FROM {{table}} VERSION AS OF {} ORDER BY id",
        snapshot_3
    );
    let result_3 = spark_validate_query_with_container(&container, &table_ref, &query_3)
        .await
        .unwrap();
    let rows_3 = result_3.rows.unwrap();
    assert_eq!(rows_3.len(), 3);
    assert_eq!(rows_3[0]["label"], "First");
    assert_eq!(rows_3[1]["label"], "Second");
    assert_eq!(rows_3[2]["label"], "Third");
}

/// Test: Verify checksum consistency across Rust and Spark for evolved schema
///
/// This validates the acceptance criteria: "Cross-engine read correctness validated by count + checksum"
#[tokio::test]
async fn test_schema_evolution_checksum_consistency() {
    let fixture = get_shared_containers();
    let container = fixture.spark_container_name();

    // Validate several evolved tables with checksum
    let tables = [
        "default.test_schema_add_column",
        "default.test_schema_drop_column",
        "default.test_schema_rename_then_add",
        "default.test_schema_type_promotion_chain",
    ];

    for table in tables {
        let result = spark_validate_with_container(&container, table, ValidationType::Full)
            .await
            .expect(&format!("Spark validation should succeed for {}", table));

        assert!(
            result.count.is_some(),
            "Count should be available for {}",
            table
        );
        assert!(
            result.checksum.is_some(),
            "Checksum should be available for {}",
            table
        );
        assert!(
            result.error.is_none(),
            "No errors for {}: {:?}",
            table,
            result.error
        );

        println!(
            "Table {}: count={}, checksum={}",
            table,
            result.count.unwrap(),
            result.checksum.unwrap()
        );
    }
}

async fn append_batch(catalog: &dyn Catalog, table: &Table, batch: RecordBatch) -> Table {
    let data_files = write_parquet_data_files(table, batch).await;
    let tx = Transaction::new(table);
    let tx = tx
        .fast_append()
        .add_data_files(data_files)
        .apply(tx)
        .unwrap();
    tx.commit(catalog)
        .await
        .expect("append commit should succeed")
}

async fn write_parquet_data_files(
    table: &Table,
    batch: RecordBatch,
) -> Vec<iceberg::spec::DataFile> {
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(
        format!("schema-evolution-{}", Uuid::new_v4().simple()),
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
    data_file_writer.write(batch).await.unwrap();
    data_file_writer.close().await.unwrap()
}
