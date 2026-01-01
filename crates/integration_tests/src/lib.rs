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

pub mod spark_validator;

use std::collections::HashMap;
use std::env;
use std::process::Command;
use std::thread::sleep;
use std::time::{Duration, Instant};

use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg::{Catalog, CatalogBuilder, TableIdent};
use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, RestCatalogBuilder};
use iceberg_test_utils::docker::{DockerCompose, skip_if_docker_unavailable};
use iceberg_test_utils::{normalize_test_name, set_up};

const DEFAULT_REST_CATALOG_PORT: u16 = 8181;
const DEFAULT_MINIO_PORT: u16 = 9000;
const REST_CATALOG_PORT_ENV: &str = "ICEBERG_TEST_REST_PORT";
const MINIO_PORT_ENV: &str = "ICEBERG_TEST_MINIO_PORT";

fn env_port(var: &str, default: u16) -> u16 {
    env::var(var)
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(default)
}

pub struct TestFixture {
    pub _docker_compose: DockerCompose,
    pub catalog_config: HashMap<String, String>,
}

impl TestFixture {
    pub fn spark_container_name(&self) -> String {
        self._docker_compose.container_name("spark-iceberg")
    }
}

pub fn set_test_fixture(func: &str) -> TestFixture {
    set_up();
    skip_if_docker_unavailable("integration tests");
    let docker_compose = DockerCompose::new(
        normalize_test_name(format!("{}_{func}", module_path!())),
        format!("{}/testdata", env!("CARGO_MANIFEST_DIR")),
    );

    // Stop any containers from previous runs and start new ones
    docker_compose.down();
    docker_compose.up();

    let rest_port = env_port(REST_CATALOG_PORT_ENV, DEFAULT_REST_CATALOG_PORT);
    let minio_port = env_port(MINIO_PORT_ENV, DEFAULT_MINIO_PORT);

    // Use localhost with port mappings for macOS compatibility.
    // Container IPs aren't accessible from host on macOS (Docker runs in VM).
    let catalog_config = HashMap::from([
        (
            REST_CATALOG_PROP_URI.to_string(),
            format!("http://localhost:{rest_port}"),
        ),
        (
            S3_ENDPOINT.to_string(),
            format!("http://localhost:{minio_port}"),
        ),
        (S3_ACCESS_KEY_ID.to_string(), "admin".to_string()),
        (S3_SECRET_ACCESS_KEY.to_string(), "password".to_string()),
        (S3_REGION.to_string(), "us-east-1".to_string()),
    ]);

    wait_for_spark_ready(&docker_compose);
    wait_for_provisioned_tables(&catalog_config);

    TestFixture {
        _docker_compose: docker_compose,
        catalog_config,
    }
}

fn wait_for_spark_ready(docker_compose: &DockerCompose) {
    let container = docker_compose.container_name("spark-iceberg");
    let deadline = Instant::now() + Duration::from_secs(180);

    loop {
        let status = Command::new("docker")
            .args(["exec", &container, "test", "-f", "/tmp/ready"])
            .status();

        if matches!(status, Ok(exit) if exit.success()) {
            return;
        }

        if Instant::now() >= deadline {
            panic!("Spark provisioning did not complete within 180s (container: {container})");
        }

        sleep(Duration::from_secs(1));
    }
}

fn wait_for_provisioned_tables(catalog_config: &HashMap<String, String>) {
    let deadline = Instant::now() + Duration::from_secs(180);
    let config = catalog_config.clone();
    let required_tables = [
        TableIdent::from_strs(["default", "test_expire_snapshots_file_deletion"]).unwrap(),
        TableIdent::from_strs(["default", "parity_expire_rust"]).unwrap(),
        TableIdent::from_strs(["default", "parity_expire_spark"]).unwrap(),
    ];

    let join = std::thread::spawn(move || {
        let runtime =
            tokio::runtime::Runtime::new().expect("Failed to create runtime for provisioning wait");
        runtime.block_on(async move {
            loop {
                if Instant::now() >= deadline {
                    panic!("Spark tables were not provisioned within 180s");
                }

                if let Ok(catalog) = RestCatalogBuilder::default()
                    .load("rest", config.clone())
                    .await
                {
                    let mut all_ready = true;
                    for table in &required_tables {
                        match catalog.table_exists(table).await {
                            Ok(true) => {}
                            _ => {
                                all_ready = false;
                                break;
                            }
                        }
                    }

                    if all_ready {
                        return;
                    }
                }

                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    });

    if join.join().is_err() {
        panic!("Provisioning wait thread panicked");
    }
}
