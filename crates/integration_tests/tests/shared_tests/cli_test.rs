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

//! Integration tests for iceberg-cli against harness tables.

use std::collections::HashMap;
use std::env;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::{Once, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value as JsonValue;
use serial_test::serial;

use crate::get_shared_containers;

const CLI_TIMEOUT: Duration = Duration::from_secs(120);
const CLI_BIN_ENV: &str = "ICEBERG_CLI_BIN";

static CLI_BUILD_ONCE: Once = Once::new();
static CLI_BIN_PATH: OnceLock<PathBuf> = OnceLock::new();

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|parent| parent.parent())
        .expect("Expected repo root two levels up from CARGO_MANIFEST_DIR")
        .to_path_buf()
}

fn target_dir(repo_root: &Path) -> PathBuf {
    match env::var_os("CARGO_TARGET_DIR") {
        Some(dir) => {
            let path = PathBuf::from(dir);
            if path.is_absolute() {
                path
            } else {
                repo_root.join(path)
            }
        }
        None => repo_root.join("target"),
    }
}

fn iceberg_cli_bin() -> PathBuf {
    if let Ok(path) = env::var(CLI_BIN_ENV) {
        let bin = PathBuf::from(path);
        if !bin.exists() {
            panic!("ICEBERG_CLI_BIN points to missing binary: {}", bin.display());
        }
        return bin;
    }

    CLI_BIN_PATH
        .get_or_init(|| {
            let repo_root = repo_root();
            let target_dir = target_dir(&repo_root);
            let bin_name = format!("iceberg-cli{}", env::consts::EXE_SUFFIX);
            let bin_path = target_dir.join("debug").join(bin_name);

            if !bin_path.exists() {
                CLI_BUILD_ONCE.call_once(|| {
                    let status = Command::new("cargo")
                        .current_dir(&repo_root)
                        .args(["build", "--package", "iceberg-cli", "--quiet"])
                        .status()
                        .expect("Failed to run cargo build for iceberg-cli");
                    assert!(status.success(), "cargo build -p iceberg-cli failed");
                });
            }

            if !bin_path.exists() {
                panic!("iceberg-cli binary not found at {}", bin_path.display());
            }

            bin_path
        })
        .clone()
}

#[derive(Debug)]
struct CliResult {
    status: ExitStatus,
    stdout: String,
    stderr: String,
}

impl CliResult {
    fn success(&self) -> bool {
        self.status.success()
    }

    fn exit_code(&self) -> i32 {
        self.status.code().unwrap_or(-1)
    }

    fn parse_json(&self) -> Result<JsonValue, serde_json::Error> {
        serde_json::from_str(&self.stdout)
    }
}

struct CliRunner {
    catalog_props: HashMap<String, String>,
}

impl CliRunner {
    fn new(catalog_props: HashMap<String, String>) -> Self {
        Self { catalog_props }
    }

    fn base_command(&self) -> Command {
        let bin_path = iceberg_cli_bin();
        let mut cmd = Command::new(bin_path);

        cmd.arg("--catalog-type").arg("rest");
        cmd.arg("--catalog-name").arg("rest");

        if let Some(uri) = self.catalog_props.get("uri") {
            cmd.arg("--uri").arg(uri);
        }
        if let Some(endpoint) = self.catalog_props.get("s3.endpoint") {
            cmd.arg("--s3-endpoint").arg(endpoint);
        }
        if let Some(access_key) = self.catalog_props.get("s3.access-key-id") {
            cmd.arg("--s3-access-key-id").arg(access_key);
        }
        if let Some(secret_key) = self.catalog_props.get("s3.secret-access-key") {
            cmd.arg("--s3-secret-access-key").arg(secret_key);
        }
        if let Some(region) = self.catalog_props.get("s3.region") {
            cmd.arg("--s3-region").arg(region);
        }

        cmd
    }

    fn run(&self, args: &[&str]) -> CliResult {
        self.run_with_timeout(args, CLI_TIMEOUT)
    }

    fn run_with_timeout(&self, args: &[&str], timeout: Duration) -> CliResult {
        let mut cmd = self.base_command();
        for arg in args {
            cmd.arg(arg);
        }

        run_command_with_timeout(cmd, timeout)
    }

    fn run_json(&self, args: &[&str]) -> CliResult {
        let mut all_args = vec!["--output", "json"];
        all_args.extend_from_slice(args);
        self.run(&all_args)
    }
}

fn read_pipe<T: Read>(pipe: &mut Option<T>) -> String {
    let mut buf = Vec::new();
    if let Some(mut handle) = pipe.take() {
        handle
            .read_to_end(&mut buf)
            .expect("Failed to read CLI output");
    }
    String::from_utf8_lossy(&buf).to_string()
}

fn run_command_with_timeout(mut cmd: Command, timeout: Duration) -> CliResult {
    let program = cmd.get_program().to_string_lossy().into_owned();
    let args: Vec<String> = cmd
        .get_args()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();

    let mut child = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn CLI process");

    let start = Instant::now();
    loop {
        if let Some(status) = child
            .try_wait()
            .expect("Failed to poll CLI process status")
        {
            let stdout = read_pipe(&mut child.stdout);
            let stderr = read_pipe(&mut child.stderr);
            return CliResult {
                status,
                stdout,
                stderr,
            };
        }

        if start.elapsed() >= timeout {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "CLI command timed out after {:?}. Command: {} {:?}",
                timeout, program, args
            );
        }

        thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn run_command_with_timeout_succeeds() {
    let mut cmd = Command::new("cargo");
    cmd.arg("--version");
    let result = run_command_with_timeout(cmd, Duration::from_secs(10));
    assert!(result.success());
}

#[cfg(unix)]
#[test]
#[should_panic(expected = "timed out")]
fn run_command_with_timeout_times_out_unix() {
    let mut cmd = Command::new("sh");
    cmd.args(["-c", "sleep 5"]);
    let _ = run_command_with_timeout(cmd, Duration::from_millis(100));
}

#[test]
#[serial]
fn test_cli_compact_human_output() {
    let fixture = get_shared_containers();
    let runner = CliRunner::new(fixture.catalog_config.clone());

    let result = runner.run(&["compact", "--table", "default.test_simple_table"]);

    assert!(
        result.success(),
        "compact failed: stdout={}, stderr={}",
        result.stdout,
        result.stderr
    );

    assert!(
        result.stdout.contains("Compacted table") || result.stdout.contains("No files to compact"),
        "Unexpected output: {}",
        result.stdout
    );
}

#[test]
#[serial]
fn test_cli_compact_json_output() {
    let fixture = get_shared_containers();
    let runner = CliRunner::new(fixture.catalog_config.clone());

    let result = runner.run_json(&["compact", "--table", "default.test_simple_table"]);

    assert!(
        result.success(),
        "compact failed: stdout={}, stderr={}",
        result.stdout,
        result.stderr
    );

    let json = result.parse_json().expect("Invalid JSON output");

    assert_eq!(json["command"], "compact");
    assert!(json["table"].is_string());
    assert!(json["result"]["has_changes"].is_boolean());
    assert!(json["result"]["duration_ms"].is_number());
}

#[test]
#[serial]
fn test_cli_rewrite_manifests_human_output() {
    let fixture = get_shared_containers();
    let runner = CliRunner::new(fixture.catalog_config.clone());

    let result = runner.run(&["rewrite-manifests", "--table", "default.test_simple_table"]);

    assert!(
        result.success(),
        "rewrite-manifests failed: stdout={}, stderr={}",
        result.stdout,
        result.stderr
    );

    assert!(
        result.stdout.contains("Rewrote manifests"),
        "Unexpected output: {}",
        result.stdout
    );
}

#[test]
#[serial]
fn test_cli_rewrite_manifests_json_output() {
    let fixture = get_shared_containers();
    let runner = CliRunner::new(fixture.catalog_config.clone());

    let result = runner.run_json(&["rewrite-manifests", "--table", "default.test_simple_table"]);

    assert!(
        result.success(),
        "rewrite-manifests failed: stdout={}, stderr={}",
        result.stdout,
        result.stderr
    );

    let json = result.parse_json().expect("Invalid JSON output");

    assert_eq!(json["command"], "rewrite-manifests");
    assert!(json["result"]["changed"].is_boolean());
    assert!(json["result"]["duration_ms"].is_number());
}

#[test]
fn test_cli_expire_snapshots_dry_run_human_output() {
    let fixture = get_shared_containers();
    let runner = CliRunner::new(fixture.catalog_config.clone());

    let result = runner.run(&[
        "expire-snapshots",
        "--table",
        "default.test_expire_snapshots_file_deletion",
        "--older-than",
        "0s",
        "--dry-run",
    ]);

    assert!(
        result.success(),
        "expire-snapshots --dry-run failed: stdout={}, stderr={}",
        result.stdout,
        result.stderr
    );

    assert!(
        result.stdout.contains("Would expire"),
        "Unexpected dry-run output: {}",
        result.stdout
    );
}

#[test]
fn test_cli_expire_snapshots_dry_run_json_output() {
    let fixture = get_shared_containers();
    let runner = CliRunner::new(fixture.catalog_config.clone());

    let result = runner.run_json(&[
        "expire-snapshots",
        "--table",
        "default.test_expire_snapshots_file_deletion",
        "--older-than",
        "0s",
        "--dry-run",
    ]);

    assert!(
        result.success(),
        "expire-snapshots --dry-run failed: stdout={}, stderr={}",
        result.stdout,
        result.stderr
    );

    let json = result.parse_json().expect("Invalid JSON output");

    assert_eq!(json["command"], "expire-snapshots");
    assert_eq!(json["dry_run"], true);
    assert!(json["planned_files"].is_number());
    assert!(json["result"]["deleted_snapshots_count"].is_number());
    assert!(
        json.get("partial_failure").is_some(),
        "JSON should include partial_failure field for orchestration"
    );
}

#[test]
fn test_cli_expire_snapshots_requires_mode_flag() {
    let fixture = get_shared_containers();
    let runner = CliRunner::new(fixture.catalog_config.clone());

    let result = runner.run(&[
        "expire-snapshots",
        "--table",
        "default.test_expire_snapshots_file_deletion",
        "--older-than",
        "7d",
    ]);

    assert!(!result.success(), "Should fail without mode flag");
    assert!(
        result.stderr.contains("--dry-run") || result.stderr.contains("--delete-files"),
        "Error should mention required flags: {}",
        result.stderr
    );
}

#[test]
fn test_cli_expire_snapshots_mutual_exclusivity() {
    let fixture = get_shared_containers();
    let runner = CliRunner::new(fixture.catalog_config.clone());

    let result = runner.run(&[
        "expire-snapshots",
        "--table",
        "default.test_expire_snapshots_file_deletion",
        "--older-than",
        "7d",
        "--dry-run",
        "--delete-files",
    ]);

    assert!(
        !result.success(),
        "Should fail with both --dry-run and --delete-files"
    );
    assert!(
        result.stderr.contains("cannot be used with")
            || result.stderr.contains("conflict")
            || result.stderr.contains("mutually exclusive")
            || result.stderr.contains("cannot be used together"),
        "Error should indicate mutual exclusivity: {}",
        result.stderr
    );
}

#[test]
fn test_cli_remove_orphans_dry_run_human_output() {
    let fixture = get_shared_containers();
    let runner = CliRunner::new(fixture.catalog_config.clone());

    let result = runner.run(&[
        "remove-orphans",
        "--table",
        "default.test_simple_table",
        "--dry-run",
    ]);

    assert!(
        result.success(),
        "remove-orphans --dry-run failed: stdout={}, stderr={}",
        result.stdout,
        result.stderr
    );

    assert!(
        result.stdout.contains("Would delete") || result.stdout.contains("orphan"),
        "Unexpected dry-run output: {}",
        result.stdout
    );
}

#[test]
fn test_cli_remove_orphans_dry_run_json_output() {
    let fixture = get_shared_containers();
    let runner = CliRunner::new(fixture.catalog_config.clone());

    let result = runner.run_json(&[
        "remove-orphans",
        "--table",
        "default.test_simple_table",
        "--dry-run",
    ]);

    assert!(
        result.success(),
        "remove-orphans --dry-run failed: stdout={}, stderr={}",
        result.stdout,
        result.stderr
    );

    let json = result.parse_json().expect("Invalid JSON output");

    assert_eq!(json["command"], "remove-orphans");
    assert_eq!(json["dry_run"], true);
    assert!(json["planned_files"].is_number());
    assert!(
        json.get("partial_failure").is_some(),
        "JSON should include partial_failure field for orchestration"
    );
}

#[test]
fn test_cli_remove_orphans_requires_mode_flag() {
    let fixture = get_shared_containers();
    let runner = CliRunner::new(fixture.catalog_config.clone());

    let result = runner.run(&["remove-orphans", "--table", "default.test_simple_table"]);

    assert!(!result.success(), "Should fail without mode flag");
    assert!(
        result.stderr.contains("--dry-run") || result.stderr.contains("--delete"),
        "Error should mention required flags: {}",
        result.stderr
    );
}

#[test]
fn test_cli_remove_orphans_mutual_exclusivity() {
    let fixture = get_shared_containers();
    let runner = CliRunner::new(fixture.catalog_config.clone());

    let result = runner.run(&[
        "remove-orphans",
        "--table",
        "default.test_simple_table",
        "--dry-run",
        "--delete",
    ]);

    assert!(
        !result.success(),
        "Should fail with both --dry-run and --delete"
    );
    assert!(
        result.stderr.contains("cannot be used with")
            || result.stderr.contains("conflict")
            || result.stderr.contains("mutually exclusive")
            || result.stderr.contains("cannot be used together"),
        "Error should indicate mutual exclusivity: {}",
        result.stderr
    );
}

#[test]
#[serial]
fn test_cli_rewrite_deletes_human_output() {
    let fixture = get_shared_containers();
    let runner = CliRunner::new(fixture.catalog_config.clone());

    let result = runner.run(&["rewrite-deletes", "--table", "default.test_simple_table"]);

    assert!(
        result.success(),
        "rewrite-deletes failed: stdout={}, stderr={}",
        result.stdout,
        result.stderr
    );

    assert!(
        result.stdout.contains("Rewrote position delete files")
            || result
                .stdout
                .contains("No position delete files to rewrite"),
        "Unexpected output: {}",
        result.stdout
    );
}

#[test]
#[serial]
fn test_cli_rewrite_deletes_json_output() {
    let fixture = get_shared_containers();
    let runner = CliRunner::new(fixture.catalog_config.clone());

    let result = runner.run_json(&["rewrite-deletes", "--table", "default.test_simple_table"]);

    assert!(
        result.success(),
        "rewrite-deletes failed: stdout={}, stderr={}",
        result.stdout,
        result.stderr
    );

    let json = result.parse_json().expect("Invalid JSON output");

    assert_eq!(json["command"], "rewrite-deletes");
    assert!(json["result"]["changed"].is_boolean());
    assert!(json["result"]["duration_ms"].is_number());
}

#[test]
fn test_cli_invalid_table_returns_error() {
    let fixture = get_shared_containers();
    let runner = CliRunner::new(fixture.catalog_config.clone());

    let result = runner.run(&[
        "compact",
        "--table",
        "nonexistent_namespace.nonexistent_table",
    ]);

    assert!(!result.success(), "Should fail with invalid table");
    assert_eq!(result.exit_code(), 1, "Expected exit code 1 for errors");
}

#[test]
fn test_cli_invalid_table_format_returns_error() {
    let fixture = get_shared_containers();
    let runner = CliRunner::new(fixture.catalog_config.clone());

    let result = runner.run(&["compact", "--table", "just_table_name"]);

    assert!(!result.success(), "Should fail with malformed table");
    assert!(
        result.stderr.contains("Invalid table identifier")
            || result.stderr.contains("namespace.table"),
        "Error should explain expected format: {}",
        result.stderr
    );
}

#[test]
fn test_cli_rejects_uri_style_table() {
    let fixture = get_shared_containers();
    let runner = CliRunner::new(fixture.catalog_config.clone());

    let result = runner.run(&["compact", "--table", "s3://bucket/table"]);

    assert!(!result.success(), "Should fail with URI-style table");
    assert!(
        result.stderr.contains("Unsupported table reference"),
        "Error should explain URI rejection: {}",
        result.stderr
    );
}

#[test]
#[serial]
fn test_cli_success_exit_code_zero() {
    let fixture = get_shared_containers();
    let runner = CliRunner::new(fixture.catalog_config.clone());

    let result = runner.run(&["rewrite-manifests", "--table", "default.test_simple_table"]);

    assert!(result.success());
    assert_eq!(
        result.exit_code(),
        0,
        "Successful command should exit with 0"
    );
}

#[test]
fn test_cli_error_exit_code_nonzero() {
    let fixture = get_shared_containers();
    let runner = CliRunner::new(fixture.catalog_config.clone());

    let result = runner.run(&["compact", "--table", "invalid"]);

    assert!(!result.success());
    assert_ne!(
        result.exit_code(),
        0,
        "Failed command should exit with non-zero code"
    );
}
