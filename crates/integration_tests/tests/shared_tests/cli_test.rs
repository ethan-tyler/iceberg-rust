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
use std::io::Read;
use std::path::PathBuf;
use std::process::{Command, ExitStatus, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use serde_json::Value as JsonValue;
use serial_test::serial;

use crate::get_shared_containers;

const CLI_TIMEOUT: Duration = Duration::from_secs(120);
const CLI_BIN_ENV: &str = "ICEBERG_CLI_BIN";
const CLI_POLL_INTERVAL: Duration = Duration::from_millis(50);

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

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("Failed to resolve repository root from CARGO_MANIFEST_DIR")
        .to_path_buf()
}

fn resolve_cli_binary() -> PathBuf {
    static CLI_BIN: OnceLock<PathBuf> = OnceLock::new();
    CLI_BIN
        .get_or_init(|| {
            if let Ok(path) = std::env::var(CLI_BIN_ENV) {
                let trimmed = path.trim();
                if !trimmed.is_empty() {
                    let override_path = PathBuf::from(trimmed);
                    if !override_path.exists() {
                        panic!(
                            "ICEBERG_CLI_BIN points to missing binary: {}",
                            override_path.display()
                        );
                    }
                    return override_path;
                }
            }

            let repo_root = repo_root();
            let mut target_dir = std::env::var("CARGO_TARGET_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| repo_root.join("target"));
            if target_dir.is_relative() {
                target_dir = repo_root.join(target_dir);
            }

            let cli_path = target_dir.join("debug").join(format!(
                "iceberg-cli{}",
                std::env::consts::EXE_SUFFIX
            ));

            if !cli_path.exists() {
                build_cli(&repo_root);
            }

            if !cli_path.exists() {
                panic!("iceberg-cli binary not found at {}", cli_path.display());
            }

            cli_path
        })
        .clone()
}

fn build_cli(repo_root: &PathBuf) {
    let status = Command::new("cargo")
        .arg("build")
        .arg("--package")
        .arg("iceberg-cli")
        .arg("--quiet")
        .current_dir(repo_root)
        .status()
        .expect("Failed to build iceberg-cli");
    if !status.success() {
        panic!("Failed to build iceberg-cli (status: {status:?})");
    }
}

fn command_description(cmd: &Command) -> String {
    let program = cmd.get_program().to_string_lossy();
    let args: Vec<String> = cmd
        .get_args()
        .map(|arg| arg.to_string_lossy().to_string())
        .collect();
    if args.is_empty() {
        program.into_owned()
    } else {
        format!("{program} {}", args.join(" "))
    }
}

fn read_pipe(reader: &mut impl Read, label: &str) -> String {
    let mut buffer = Vec::new();
    reader
        .read_to_end(&mut buffer)
        .unwrap_or_else(|err| panic!("Failed to read {label}: {err}"));
    String::from_utf8_lossy(&buffer).to_string()
}

fn run_command_with_timeout(mut cmd: Command, timeout: Duration) -> CliResult {
    let command_desc = command_description(&cmd);
    let mut child = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn CLI process");

    let mut stdout = child
        .stdout
        .take()
        .expect("Failed to capture CLI stdout");
    let mut stderr = child
        .stderr
        .take()
        .expect("Failed to capture CLI stderr");

    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("Failed to poll CLI process") {
            return CliResult {
                status,
                stdout: read_pipe(&mut stdout, "stdout"),
                stderr: read_pipe(&mut stderr, "stderr"),
            };
        }

        if start.elapsed() >= timeout {
            child
                .kill()
                .expect("Failed to kill timed-out CLI process");
            let _ = child.wait();
            panic!(
                "CLI command timed out after {:?}. Command: {}",
                timeout, command_desc
            );
        }

        std::thread::sleep(CLI_POLL_INTERVAL);
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
        let mut cmd = Command::new(resolve_cli_binary());
        cmd.current_dir(repo_root());

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

#[test]
fn run_command_with_timeout_succeeds() {
    let mut cmd = Command::new("cargo");
    cmd.arg("--version");

    let result = run_command_with_timeout(cmd, Duration::from_secs(10));

    assert!(
        result.success(),
        "Expected command to succeed: {}",
        result.stderr
    );
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
