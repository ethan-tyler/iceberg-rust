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

//! Performance benchmarks for Iceberg UPDATE operations.
//!
//! Run with: `cargo bench --bench update_benchmark`

use std::collections::HashMap;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use datafusion::execution::context::SessionContext;
use datafusion::prelude::{col, lit};
use iceberg::memory::{MemoryCatalog, MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};
use iceberg_datafusion::{IcebergCatalogProvider, IcebergTableProvider};
use tempfile::TempDir;

fn temp_path() -> String {
    let temp_dir = TempDir::new().unwrap();
    temp_dir.keep().to_str().unwrap().to_string()
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

async fn setup_test_table(
    namespace_name: &str,
    table_name: &str,
    row_count: usize,
) -> (Arc<MemoryCatalog>, NamespaceIdent, SessionContext) {
    let iceberg_catalog = get_iceberg_catalog().await;
    let namespace = NamespaceIdent::new(namespace_name.to_string());

    iceberg_catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();

    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "value", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .unwrap();

    let creation = TableCreation::builder()
        .location(temp_path())
        .name(table_name.to_string())
        .properties(HashMap::new())
        .schema(schema)
        .build();

    iceberg_catalog
        .create_table(&namespace, creation)
        .await
        .unwrap();

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client.clone()).await.unwrap());

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    // Insert test data
    let values: Vec<String> = (0..row_count)
        .map(|i| format!("({}, 'value_{}')", i, i))
        .collect();

    if !values.is_empty() {
        let insert_sql = format!(
            "INSERT INTO catalog.{}.{} VALUES {}",
            namespace_name,
            table_name,
            values.join(", ")
        );
        ctx.sql(&insert_sql).await.unwrap().collect().await.unwrap();
    }

    (client, namespace, ctx)
}

fn bench_update_selectivity(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("update_selectivity");
    group.sample_size(10); // Reduce sample size for faster benchmarks

    // Test different selectivities on a 1000-row table
    let row_count = 1000;

    for (name, filter_threshold) in [("1%", 10), ("10%", 100), ("50%", 500), ("100%", 1000)] {
        // Set throughput based on rows being updated
        group.throughput(Throughput::Elements(filter_threshold as u64));

        group.bench_function(BenchmarkId::new("rows", name), |b| {
            b.to_async(&rt).iter(|| async {
                let namespace_name = format!("bench_selectivity_{}", name.replace('%', "pct"));
                let (client, namespace, ctx) =
                    setup_test_table(&namespace_name, "bench_table", row_count).await;

                let provider = IcebergTableProvider::try_new(
                    client.clone(),
                    namespace.clone(),
                    "bench_table",
                )
                .await
                .unwrap();

                // UPDATE rows where id < filter_threshold
                let _count = provider
                    .update()
                    .await
                    .unwrap()
                    .set("value", lit("updated"))
                    .filter(col("id").lt(lit(filter_threshold)))
                    .execute(&ctx.state())
                    .await
                    .unwrap();
            });
        });
    }

    group.finish();
}

fn bench_update_row_count(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("update_row_count");
    group.sample_size(10);

    // Test different table sizes with full table update
    for row_count in [100, 500, 1000] {
        // Set throughput based on rows being updated
        group.throughput(Throughput::Elements(row_count as u64));

        group.bench_function(BenchmarkId::new("full_table", row_count), |b| {
            b.to_async(&rt).iter(|| async {
                let namespace_name = format!("bench_rows_{}", row_count);
                let (client, namespace, ctx) =
                    setup_test_table(&namespace_name, "bench_table", row_count).await;

                let provider = IcebergTableProvider::try_new(
                    client.clone(),
                    namespace.clone(),
                    "bench_table",
                )
                .await
                .unwrap();

                // Full table UPDATE
                let _count = provider
                    .update()
                    .await
                    .unwrap()
                    .set("value", lit("updated"))
                    .execute(&ctx.state())
                    .await
                    .unwrap();
            });
        });
    }

    group.finish();
}

/// Benchmarks DELETE operations for comparison with UPDATE.
/// This helps users understand relative performance characteristics.
fn bench_delete_operations(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("delete_operations");
    group.sample_size(10);

    // Test DELETE at different selectivities
    let row_count = 1000;

    for (name, filter_threshold) in [("10%", 100), ("50%", 500), ("100%", 1000)] {
        group.throughput(Throughput::Elements(filter_threshold as u64));

        group.bench_function(BenchmarkId::new("delete", name), |b| {
            b.to_async(&rt).iter(|| async {
                let namespace_name = format!("bench_delete_{}", name.replace('%', "pct"));
                let (client, namespace, ctx) =
                    setup_test_table(&namespace_name, "bench_table", row_count).await;

                let provider = IcebergTableProvider::try_new(
                    client.clone(),
                    namespace.clone(),
                    "bench_table",
                )
                .await
                .unwrap();

                // DELETE rows where id < filter_threshold
                let _count = provider
                    .delete(&ctx.state(), Some(col("id").lt(lit(filter_threshold))))
                    .await
                    .unwrap();
            });
        });
    }

    group.finish();
}

/// Compares UPDATE vs manual DELETE + INSERT pattern.
/// This benchmark helps users decide which approach to use.
fn bench_update_vs_delete_insert(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("update_vs_delete_insert");
    group.sample_size(10);

    // Compare patterns at 10% selectivity (100 rows out of 1000)
    let row_count = 1000;
    let rows_affected = 100;
    group.throughput(Throughput::Elements(rows_affected as u64));

    // Benchmark: Native UPDATE
    group.bench_function("native_update", |b| {
        b.to_async(&rt).iter(|| async {
            let (client, namespace, ctx) =
                setup_test_table("bench_native_update", "bench_table", row_count).await;

            let provider =
                IcebergTableProvider::try_new(client.clone(), namespace.clone(), "bench_table")
                    .await
                    .unwrap();

            let _count = provider
                .update()
                .await
                .unwrap()
                .set("value", lit("updated"))
                .filter(col("id").lt(lit(rows_affected)))
                .execute(&ctx.state())
                .await
                .unwrap();
        });
    });

    // Benchmark: DELETE + INSERT pattern (for comparison)
    group.bench_function("delete_then_insert", |b| {
        b.to_async(&rt).iter(|| async {
            let namespace_name = "bench_delete_insert";
            let (client, namespace, ctx) =
                setup_test_table(namespace_name, "bench_table", row_count).await;

            let provider =
                IcebergTableProvider::try_new(client.clone(), namespace.clone(), "bench_table")
                    .await
                    .unwrap();

            // Step 1: DELETE matching rows
            let _count = provider
                .delete(&ctx.state(), Some(col("id").lt(lit(rows_affected))))
                .await
                .unwrap();

            // Step 2: INSERT new values (simulating update)
            let values: Vec<String> = (0..rows_affected)
                .map(|i| format!("({}, 'updated_{}')", i, i))
                .collect();

            let insert_sql = format!(
                "INSERT INTO catalog.{}.bench_table VALUES {}",
                namespace_name,
                values.join(", ")
            );
            ctx.sql(&insert_sql).await.unwrap().collect().await.unwrap();
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_update_selectivity,
    bench_update_row_count,
    bench_delete_operations,
    bench_update_vs_delete_insert
);
criterion_main!(benches);
