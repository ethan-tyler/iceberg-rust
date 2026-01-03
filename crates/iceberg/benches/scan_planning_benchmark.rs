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

//! Performance benchmarks for Iceberg manifest caching operations.
//!
//! These benchmarks measure the impact of cache configurations on scan planning.
//!
//! Run with: `cargo bench --bench scan_planning_benchmark -p iceberg`
//!
//! To compare results across configurations:
//! ```shell
//! cargo bench --bench scan_planning_benchmark -p iceberg -- --baseline main
//! ```

use std::collections::HashMap;
use std::env;
use std::time::{Duration, Instant};

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use futures::TryStreamExt;
use iceberg::TableIdent;
use iceberg::io::{CacheMetrics, CacheStats, FileIO, MANIFEST_CACHE_MAX_TOTAL_BYTES};
use iceberg::scan::TableScan;
use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, Literal, ManifestListWriter,
    ManifestWriterBuilder, Struct, TableMetadata, TableMetadataRef,
};
use iceberg::table::Table;
use minijinja::value::Value;
use minijinja::{AutoEscape, Environment, context};
use tempfile::TempDir;

fn render_template(template: &str, ctx: Value) -> String {
    let mut env = Environment::new();
    env.set_auto_escape_callback(|_| AutoEscape::None);
    env.render_str(template, ctx).unwrap()
}

struct BenchProfile {
    name: &'static str,
    manifest_count: usize,
    entries_per_manifest: usize,
    iterations: usize,
}

struct PlanMeasurement {
    p50: Duration,
    p95: Duration,
    task_count: usize,
    cache_stats: CacheStats,
    cache_metrics: CacheMetrics,
    iterations: usize,
}

fn percentile(sorted: &[Duration], percentile: usize) -> Duration {
    if sorted.is_empty() {
        return Duration::from_secs(0);
    }
    let index = (sorted.len() * percentile / 100).min(sorted.len() - 1);
    sorted[index]
}

fn diff_metrics(before: CacheMetrics, after: CacheMetrics) -> CacheMetrics {
    CacheMetrics {
        hits: after.hits.saturating_sub(before.hits),
        misses: after.misses.saturating_sub(before.misses),
        loads: after.loads.saturating_sub(before.loads),
    }
}

async fn measure_plan_files(
    table: &Table,
    iterations: usize,
    invalidate_each_time: bool,
) -> PlanMeasurement {
    let mut durations = Vec::with_capacity(iterations);
    let mut expected_task_count: Option<usize> = None;
    let metrics_before = table.cache_metrics();

    for _ in 0..iterations {
        if invalidate_each_time {
            table.invalidate_cache();
        }
        let scan = table.scan().build().unwrap();
        let start = Instant::now();
        let task_count = plan_files_and_count_tasks(&scan).await;
        durations.push(start.elapsed());

        match expected_task_count {
            Some(expected) => {
                assert_eq!(
                    task_count, expected,
                    "scan task count changed between iterations"
                );
            }
            None => expected_task_count = Some(task_count),
        }
    }

    durations.sort_unstable();

    let metrics_after = table.cache_metrics();
    let cache_metrics = diff_metrics(metrics_before, metrics_after);

    PlanMeasurement {
        p50: percentile(&durations, 50),
        p95: percentile(&durations, 95),
        task_count: expected_task_count.unwrap_or(0),
        cache_stats: table.cache_stats(),
        cache_metrics,
        iterations,
    }
}

fn print_measurement(profile: &BenchProfile, scenario: &str, measurement: &PlanMeasurement) {
    println!(
        "profile={} scenario={} iterations={} tasks={} p50_us={} p95_us={} hits={} misses={} loads={} hit_rate={:.3} entries={} weighted_bytes={} max_bytes={}",
        profile.name,
        scenario,
        measurement.iterations,
        measurement.task_count,
        measurement.p50.as_micros(),
        measurement.p95.as_micros(),
        measurement.cache_metrics.hits,
        measurement.cache_metrics.misses,
        measurement.cache_metrics.loads,
        measurement.cache_metrics.hit_rate(),
        measurement.cache_stats.entry_count,
        measurement.cache_stats.weighted_size,
        measurement.cache_stats.max_capacity
    );
}

struct BenchFixture {
    _temp_dir: TempDir,
    file_io: FileIO,
    metadata_location: String,
    metadata: TableMetadataRef,
}

impl BenchFixture {
    fn new(
        runtime: &tokio::runtime::Runtime,
        manifest_count: usize,
        entries_per_manifest: usize,
    ) -> Self {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().join("bench_table");
        let manifest_list_location = table_location.join("metadata/manifests_list_bench.avro");
        let table_metadata_location = table_location.join("metadata/v1.json");

        let file_io = FileIO::from_path(table_location.as_os_str().to_str().unwrap())
            .unwrap()
            .build()
            .unwrap();

        let template_json_str = std::fs::read_to_string(format!(
            "{}/testdata/example_table_metadata_v2.json",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap();

        let metadata_json = render_template(&template_json_str, context! {
            table_location => table_location.to_str().unwrap(),
            manifest_list_1_location => manifest_list_location.to_str().unwrap(),
            manifest_list_2_location => manifest_list_location.to_str().unwrap(),
            table_metadata_1_location => table_metadata_location.to_str().unwrap(),
        });

        let mut table_metadata = serde_json::from_str::<TableMetadata>(&metadata_json).unwrap();
        if let Ok(value) = env::var("ICEBERG_MANIFEST_CACHE_MAX_BYTES")
            && let Ok(cache_bytes) = value.parse::<u64>()
        {
            let mut properties = HashMap::new();
            properties.insert(
                MANIFEST_CACHE_MAX_TOTAL_BYTES.to_string(),
                cache_bytes.to_string(),
            );
            table_metadata = table_metadata
                .into_builder(None)
                .set_properties(properties)
                .unwrap()
                .build()
                .unwrap()
                .metadata;
        }

        let metadata = std::sync::Arc::new(table_metadata);
        let table = Table::builder()
            .metadata(metadata.clone())
            .identifier(TableIdent::from_strs(["bench_db", "bench_table"]).unwrap())
            .file_io(file_io.clone())
            .metadata_location(table_metadata_location.to_str().unwrap())
            .disable_cache()
            .build()
            .unwrap();

        runtime.block_on(async {
            write_manifests_and_manifest_list(
                &table,
                &table_location,
                manifest_count,
                entries_per_manifest,
            )
            .await;
        });

        Self {
            _temp_dir: tmp_dir,
            file_io,
            metadata_location: table_metadata_location.to_str().unwrap().to_string(),
            metadata,
        }
    }

    fn build_table(
        &self,
        disable_cache: bool,
        cache_size_bytes: Option<u64>,
        cache_ttl: Option<Option<Duration>>,
    ) -> Table {
        let mut builder = Table::builder()
            .metadata(self.metadata.clone())
            .identifier(TableIdent::from_strs(["bench_db", "bench_table"]).unwrap())
            .file_io(self.file_io.clone())
            .metadata_location(self.metadata_location.clone());

        if disable_cache {
            builder = builder.disable_cache();
        }

        if let Some(size) = cache_size_bytes {
            builder = builder.cache_size_bytes(size);
        }

        if let Some(ttl) = cache_ttl {
            builder = builder.cache_ttl(ttl);
        }

        builder.build().unwrap()
    }
}

async fn write_manifests_and_manifest_list(
    table: &Table,
    table_location: &std::path::Path,
    manifest_count: usize,
    entries_per_manifest: usize,
) {
    let current_snapshot = table.metadata().current_snapshot().unwrap();
    let current_schema = current_snapshot.schema(table.metadata()).unwrap();
    let partition_spec = table.metadata().default_partition_spec();

    let mut manifests = Vec::with_capacity(manifest_count);
    for manifest_idx in 0..manifest_count {
        let output_path = table_location.join(format!("metadata/manifest_{manifest_idx}.avro"));
        let output_file = table
            .file_io()
            .new_output(output_path.to_str().unwrap())
            .unwrap();

        let mut writer = ManifestWriterBuilder::new(
            output_file,
            Some(current_snapshot.snapshot_id()),
            None,
            current_schema.clone(),
            partition_spec.as_ref().clone(),
        )
        .build_v2_data();

        for entry_idx in 0..entries_per_manifest {
            let partition_value = (manifest_idx * entries_per_manifest + entry_idx) as i64;
            let data_file = DataFileBuilder::default()
                .partition_spec_id(partition_spec.spec_id())
                .content(DataContentType::Data)
                .file_path(format!(
                    "{}/data_{}_{}.parquet",
                    table_location.display(),
                    manifest_idx,
                    entry_idx
                ))
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(100)
                .record_count(1)
                .partition(Struct::from_iter([Some(Literal::long(partition_value))]))
                .build()
                .unwrap();

            writer
                .add_file(data_file, current_snapshot.sequence_number())
                .unwrap();
        }

        manifests.push(writer.write_manifest_file().await.unwrap());
    }

    let mut manifest_list_writer = ManifestListWriter::v2(
        table
            .file_io()
            .new_output(current_snapshot.manifest_list())
            .unwrap(),
        current_snapshot.snapshot_id(),
        current_snapshot.parent_snapshot_id(),
        current_snapshot.sequence_number(),
    );
    manifest_list_writer
        .add_manifests(manifests.into_iter())
        .unwrap();
    manifest_list_writer.close().await.unwrap();
}

async fn plan_files_and_count_tasks(scan: &TableScan) -> usize {
    let mut stream = scan.plan_files().await.unwrap();
    let mut task_count = 0usize;
    while let Some(_task) = stream.try_next().await.unwrap() {
        task_count += 1;
    }
    task_count
}

fn bench_scan_planning(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let profiles = [
        BenchProfile {
            name: "small",
            manifest_count: 10,
            entries_per_manifest: 10,
            iterations: 200,
        },
        BenchProfile {
            name: "medium",
            manifest_count: 100,
            entries_per_manifest: 100,
            iterations: 100,
        },
        BenchProfile {
            name: "large",
            manifest_count: 1000,
            entries_per_manifest: 100,
            iterations: 50,
        },
    ];

    for profile in &profiles {
        let fixture = BenchFixture::new(
            &runtime,
            profile.manifest_count,
            profile.entries_per_manifest,
        );

        let warm_cache_table = fixture.build_table(false, None, None);
        let cold_cache_table = fixture.build_table(false, None, None);
        let disabled_cache_table = fixture.build_table(true, None, None);

        runtime.block_on(async {
            let scan = warm_cache_table.scan().build().unwrap();
            let _ = plan_files_and_count_tasks(&scan).await;
        });

        runtime.block_on(async {
            let disabled_metrics =
                measure_plan_files(&disabled_cache_table, profile.iterations, false).await;
            print_measurement(profile, "disabled_cache", &disabled_metrics);

            let cold_metrics =
                measure_plan_files(&cold_cache_table, profile.iterations, true).await;
            print_measurement(profile, "cold_cache_invalidate_each_time", &cold_metrics);

            let warm_metrics =
                measure_plan_files(&warm_cache_table, profile.iterations, false).await;
            print_measurement(profile, "warm_cache", &warm_metrics);
        });

        let mut group = c.benchmark_group(format!("scan_planning_plan_files_{}", profile.name));

        group.bench_function("disabled_cache", |b| {
            b.to_async(&runtime).iter(|| async {
                let scan = disabled_cache_table.scan().build().unwrap();
                black_box(plan_files_and_count_tasks(&scan).await);
            })
        });

        group.bench_function("cold_cache_invalidate_each_time", |b| {
            b.to_async(&runtime).iter(|| async {
                cold_cache_table.invalidate_cache();
                let scan = cold_cache_table.scan().build().unwrap();
                black_box(plan_files_and_count_tasks(&scan).await);
            })
        });

        group.bench_function("warm_cache", |b| {
            b.to_async(&runtime).iter(|| async {
                let scan = warm_cache_table.scan().build().unwrap();
                black_box(plan_files_and_count_tasks(&scan).await);
            })
        });

        group.finish();
    }
}

criterion_group!(benches, bench_scan_planning,);
criterion_main!(benches);
