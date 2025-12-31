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

use std::time::Duration;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use futures::TryStreamExt;
use iceberg::TableIdent;
use iceberg::io::FileIO;
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

        let table_metadata = serde_json::from_str::<TableMetadata>(&metadata_json).unwrap();

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

    // A moderately-sized manifest set so planning is large enough to measure.
    let fixture = BenchFixture::new(&runtime, 8, 250);

    let warm_cache_table = fixture.build_table(false, None, None);
    let cold_cache_table = fixture.build_table(false, None, None);
    let disabled_cache_table = fixture.build_table(true, None, None);

    runtime.block_on(async {
        // Warm cache by planning once.
        let scan = warm_cache_table.scan().build().unwrap();
        let _ = plan_files_and_count_tasks(&scan).await;
    });

    let mut group = c.benchmark_group("scan_planning_plan_files");

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

criterion_group!(benches, bench_scan_planning,);
criterion_main!(benches);
