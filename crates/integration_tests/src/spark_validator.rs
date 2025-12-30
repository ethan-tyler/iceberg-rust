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

//! Spark validation utilities for cross-engine interoperability tests.
//!
//! This module provides helpers to invoke Spark via docker exec to validate
//! that tables modified by Rust can be read correctly by Spark.
//!
//! # Example
//!
//! ```rust,ignore
//! use iceberg_integration_tests::spark_validator::{spark_validate_with_container, ValidationResult};
//!
//! // After performing a Rust DELETE operation...
//! let container_name = fixture.spark_container_name();
//! let result = spark_validate_with_container(&container_name, "test_table", ValidationType::Count)
//!     .await?;
//! assert_eq!(result.count.unwrap(), expected_count);
//! ```

use std::collections::HashMap;
use std::process::{Command as StdCommand, Output, Stdio};
use std::time::Duration;

use serde::Deserialize;
use tokio::process::Command;
use tokio::time::timeout;

/// Result of a Spark validation query.
#[derive(Debug, Deserialize)]
pub struct ValidationResult {
    /// Row count (for "count" and "full" validation types)
    pub count: Option<i64>,

    /// Checksum of all rows using hash(*) (for "full" validation type)
    pub checksum: Option<i64>,

    /// List of column names (for "full" validation type)
    pub columns: Option<Vec<String>>,

    /// Min/max bounds for numeric columns (for "full" validation type)
    pub bounds: Option<HashMap<String, ColumnBounds>>,

    /// Error message if validation failed
    pub error: Option<String>,

    /// Snapshot count (for "metadata" validation type)
    pub snapshot_count: Option<i64>,

    /// Data file count (for "metadata" validation type)
    pub file_count: Option<i64>,

    /// Manifest count (for "metadata" validation type)
    pub manifest_count: Option<i64>,

    /// Current snapshot info (for "metadata" validation type)
    pub current_snapshot: Option<SnapshotInfo>,

    /// Distinct count for a column (for "distinct" validation type)
    pub distinct_count: Option<i64>,

    /// Column name for distinct validation
    pub column: Option<String>,

    /// Query result rows (for "query" validation type)
    pub rows: Option<Vec<serde_json::Value>>,
}

/// Min/max bounds for a numeric column.
#[derive(Debug, Deserialize)]
pub struct ColumnBounds {
    pub min: Option<serde_json::Value>,
    pub max: Option<serde_json::Value>,
}

/// Current snapshot information.
#[derive(Debug, Deserialize)]
pub struct SnapshotInfo {
    pub snapshot_id: Option<i64>,
    pub operation: Option<String>,
}

/// Detailed snapshot summary fields for semantic parity comparison.
#[derive(Debug, Clone, Deserialize)]
pub struct SnapshotSummaryResult {
    /// Error message if extraction failed
    pub error: Option<String>,

    /// Snapshot ID
    pub snapshot_id: Option<i64>,

    /// Parent snapshot ID
    pub parent_id: Option<i64>,

    /// Operation type (append, overwrite, delete, etc.)
    pub operation: Option<String>,

    /// Commit timestamp
    pub committed_at: Option<String>,

    /// Summary fields
    pub summary: Option<SnapshotSummaryFields>,
}

/// Snapshot summary fields for parity comparison.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SnapshotSummaryFields {
    // Core counts
    #[serde(rename = "added-data-files")]
    pub added_data_files: Option<String>,
    #[serde(rename = "deleted-data-files")]
    pub deleted_data_files: Option<String>,
    #[serde(rename = "added-records")]
    pub added_records: Option<String>,
    #[serde(rename = "deleted-records")]
    pub deleted_records: Option<String>,
    #[serde(rename = "added-files-size")]
    pub added_files_size: Option<String>,
    #[serde(rename = "removed-files-size")]
    pub removed_files_size: Option<String>,

    // Delete file counts
    #[serde(rename = "added-delete-files")]
    pub added_delete_files: Option<String>,
    #[serde(rename = "removed-delete-files")]
    pub removed_delete_files: Option<String>,
    #[serde(rename = "added-position-deletes")]
    pub added_position_deletes: Option<String>,
    #[serde(rename = "removed-position-deletes")]
    pub removed_position_deletes: Option<String>,
    #[serde(rename = "added-equality-deletes")]
    pub added_equality_deletes: Option<String>,
    #[serde(rename = "removed-equality-deletes")]
    pub removed_equality_deletes: Option<String>,

    // Totals
    #[serde(rename = "total-data-files")]
    pub total_data_files: Option<String>,
    #[serde(rename = "total-delete-files")]
    pub total_delete_files: Option<String>,
    #[serde(rename = "total-records")]
    pub total_records: Option<String>,
    #[serde(rename = "total-files-size")]
    pub total_files_size: Option<String>,
    #[serde(rename = "total-position-deletes")]
    pub total_position_deletes: Option<String>,
    #[serde(rename = "total-equality-deletes")]
    pub total_equality_deletes: Option<String>,

    // Partition info
    #[serde(rename = "changed-partition-count")]
    pub changed_partition_count: Option<String>,

    // Raw summary for additional fields
    pub raw: Option<HashMap<String, serde_json::Value>>,
}

/// Manifest entries result for structure comparison.
#[derive(Debug, Deserialize)]
pub struct ManifestEntriesResult {
    /// Error message if extraction failed
    pub error: Option<String>,

    /// Total entry count
    pub entry_count: Option<i64>,

    /// Sample of entries
    pub entries: Option<Vec<ManifestEntryInfo>>,
}

/// Individual manifest entry info.
#[derive(Debug, Deserialize)]
pub struct ManifestEntryInfo {
    pub status: Option<i32>,
    pub snapshot_id: Option<i64>,
    pub sequence_number: Option<i64>,
    pub file_sequence_number: Option<i64>,
    pub manifest_path: Option<String>,
    pub manifest_pos: Option<i64>,
    pub partition_spec_id: Option<i32>,
    pub spec_id: Option<i32>,
    pub data_file: Option<DataFileInfo>,
}

/// Data file info from manifest entry.
#[derive(Debug, Deserialize)]
pub struct DataFileInfo {
    pub content: Option<i32>,
    pub file_path: Option<String>,
    pub file_format: Option<String>,
    pub partition: Option<serde_json::Value>,
    pub record_count: Option<i64>,
    pub file_size_in_bytes: Option<i64>,
}

/// Type of validation to perform.
#[derive(Debug, Clone, Copy)]
pub enum ValidationType {
    /// Return row count only
    Count,
    /// Return count, checksum, columns, and bounds
    Full,
    /// Return metadata table information (snapshots, files, manifests)
    Metadata,
}

impl ValidationType {
    fn as_str(&self) -> &'static str {
        match self {
            ValidationType::Count => "count",
            ValidationType::Full => "full",
            ValidationType::Metadata => "metadata",
        }
    }
}

/// Error type for Spark validation operations.
#[derive(Debug)]
pub struct SparkValidationError {
    pub message: String,
}

impl std::fmt::Display for SparkValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Spark validation error: {}", self.message)
    }
}

impl std::error::Error for SparkValidationError {}

const DEFAULT_SPARK_VALIDATE_TIMEOUT_SECS: u64 = 120;
const SPARK_VALIDATE_TIMEOUT_ENV: &str = "ICEBERG_SPARK_VALIDATE_TIMEOUT_SECS";
const SPARK_CONTAINER_ENV: &str = "ICEBERG_SPARK_CONTAINER";

fn validation_timeout() -> Duration {
    std::env::var(SPARK_VALIDATE_TIMEOUT_ENV)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(DEFAULT_SPARK_VALIDATE_TIMEOUT_SECS))
}

async fn run_spark_submit(
    container_name: &str,
    args: Vec<String>,
) -> Result<Output, SparkValidationError> {
    let timeout_duration = validation_timeout();
    let mut cmd = Command::new("docker");
    cmd.args(&args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let child = cmd.spawn().map_err(|e| SparkValidationError {
        message: format!("Failed to execute docker exec: {e}"),
    })?;

    // wait_with_output() consumes child, so if timeout expires the process may be orphaned.
    // This is acceptable for test infrastructure - the container will clean up eventually.
    match timeout(timeout_duration, child.wait_with_output()).await {
        Ok(result) => result.map_err(|e| SparkValidationError {
            message: format!("Failed to read spark-submit output: {e}"),
        }),
        Err(_) => Err(SparkValidationError {
            message: format!(
                "spark-submit timed out after {}s (container: {})",
                timeout_duration.as_secs(),
                container_name
            ),
        }),
    }
}

fn parse_validation_output(stdout: &str) -> Result<ValidationResult, SparkValidationError> {
    for line in stdout.lines().rev() {
        let trimmed = line.trim_start();
        if !trimmed.starts_with('{') {
            continue;
        }

        if let Ok(result) = serde_json::from_str::<ValidationResult>(trimmed) {
            return Ok(result);
        }
    }

    Err(SparkValidationError {
        message: format!("No JSON output found in Spark output: {stdout}"),
    })
}

fn spark_submit_args(
    container_name: &str,
    table_name: &str,
    validation_type: &str,
    extra_args: &[String],
) -> Vec<String> {
    let mut args = vec![
        "exec".to_string(),
        container_name.to_string(),
        "spark-submit".to_string(),
        "--master".to_string(),
        "local[*]".to_string(),
        "/home/validate.py".to_string(),
        table_name.to_string(),
        validation_type.to_string(),
    ];
    args.extend_from_slice(extra_args);
    args
}

fn parse_validation_result(
    output: Output,
    error_context: &str,
) -> Result<ValidationResult, SparkValidationError> {
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(SparkValidationError {
            message: format!("spark-submit failed.\nstdout: {stdout}\nstderr: {stderr}"),
        });
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let result = parse_validation_output(&stdout)?;

    if let Some(ref error) = result.error {
        return Err(SparkValidationError {
            message: format!("{error_context}: {error}"),
        });
    }

    Ok(result)
}

/// Get the container name for the spark-iceberg container.
///
/// This attempts to find the running spark container by matching the service name.
/// When running under docker compose, prefer setting `ICEBERG_SPARK_CONTAINER`
/// to avoid accidentally selecting the wrong container.
fn get_spark_container_name() -> Result<String, SparkValidationError> {
    if let Ok(container) = std::env::var(SPARK_CONTAINER_ENV) {
        let trimmed = container.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_string());
        }
    }

    // Try to find the container by looking for spark-iceberg in the name
    let output = StdCommand::new("docker")
        .args([
            "ps",
            "--filter",
            "name=spark-iceberg",
            "--format",
            "{{.Names}}",
        ])
        .output()
        .map_err(|e| SparkValidationError {
            message: format!("Failed to run docker ps: {e}"),
        })?;

    if !output.status.success() {
        return Err(SparkValidationError {
            message: format!(
                "docker ps failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ),
        });
    }

    let containers = String::from_utf8_lossy(&output.stdout);
    let container_names: Vec<&str> = containers.lines().filter(|line| !line.is_empty()).collect();
    let container_name = match container_names.as_slice() {
        [single] => *single,
        [] => {
            return Err(SparkValidationError {
                message: "No spark-iceberg container found. Is docker-compose running?".to_string(),
            });
        }
        _ => {
            return Err(SparkValidationError {
                message: format!(
                    "Multiple spark-iceberg containers found ({container_names:?}). Set {SPARK_CONTAINER_ENV} to disambiguate."
                ),
            });
        }
    };

    Ok(container_name.to_string())
}

/// Validate a table using Spark.
///
/// This function invokes the validate.py script in the Spark container to
/// perform validation queries and returns the parsed result.
///
/// # Arguments
///
/// * `table_name` - Name of the table in the default namespace (e.g., "test_table")
/// * `validation_type` - Type of validation to perform
///
/// # Returns
///
/// A `ValidationResult` containing the validation data or an error.
///
/// # Example
///
/// ```rust,ignore
/// let result = spark_validate("test_partition_evolution_delete", ValidationType::Count).await?;
/// assert_eq!(result.count.unwrap(), 3);
/// ```
pub async fn spark_validate(
    table_name: &str,
    validation_type: ValidationType,
) -> Result<ValidationResult, SparkValidationError> {
    let container_name = get_spark_container_name()?;
    spark_validate_with_container(&container_name, table_name, validation_type).await
}

/// Validate a table using Spark with an explicit container name.
pub async fn spark_validate_with_container(
    container_name: &str,
    table_name: &str,
    validation_type: ValidationType,
) -> Result<ValidationResult, SparkValidationError> {
    let args = spark_submit_args(container_name, table_name, validation_type.as_str(), &[]);
    let output = run_spark_submit(container_name, args).await?;

    parse_validation_result(output, "Spark validation failed")
}

/// Validate a table with a custom SQL query.
///
/// # Arguments
///
/// * `table_name` - Name of the table in the default namespace
/// * `query_template` - SQL query with `{table}` placeholder for the table reference
///
/// # Example
///
/// ```rust,ignore
/// let result = spark_validate_query(
///     "test_table",
///     "SELECT count(*), max(id) FROM {table} WHERE value > 100"
/// )
/// .await?;
/// ```
pub async fn spark_validate_query(
    table_name: &str,
    query_template: &str,
) -> Result<ValidationResult, SparkValidationError> {
    let container_name = get_spark_container_name()?;
    spark_validate_query_with_container(&container_name, table_name, query_template).await
}

/// Validate a table with a custom SQL query using an explicit container name.
pub async fn spark_validate_query_with_container(
    container_name: &str,
    table_name: &str,
    query_template: &str,
) -> Result<ValidationResult, SparkValidationError> {
    let extra_args = vec![query_template.to_string()];
    let args = spark_submit_args(container_name, table_name, "query", &extra_args);
    let output = run_spark_submit(container_name, args).await?;

    parse_validation_result(output, "Spark query failed")
}

/// Validate distinct count for a column.
///
/// # Arguments
///
/// * `table_name` - Name of the table in the default namespace
/// * `column` - Column name to count distinct values for
pub async fn spark_validate_distinct(
    table_name: &str,
    column: &str,
) -> Result<ValidationResult, SparkValidationError> {
    let container_name = get_spark_container_name()?;
    spark_validate_distinct_with_container(&container_name, table_name, column).await
}

/// Validate distinct count for a column using an explicit container name.
pub async fn spark_validate_distinct_with_container(
    container_name: &str,
    table_name: &str,
    column: &str,
) -> Result<ValidationResult, SparkValidationError> {
    let extra_args = vec![column.to_string()];
    let args = spark_submit_args(container_name, table_name, "distinct", &extra_args);
    let output = run_spark_submit(container_name, args).await?;

    parse_validation_result(output, "Spark distinct validation failed")
}

/// Extract detailed snapshot summary for semantic parity comparison.
///
/// # Arguments
///
/// * `table_name` - Name of the table in the default namespace
/// * `snapshot_id` - Optional specific snapshot ID to query (defaults to current)
pub async fn spark_snapshot_summary(
    table_name: &str,
    snapshot_id: Option<i64>,
) -> Result<SnapshotSummaryResult, SparkValidationError> {
    let container_name = get_spark_container_name()?;
    spark_snapshot_summary_with_container(&container_name, table_name, snapshot_id).await
}

/// Extract detailed snapshot summary using an explicit container name.
pub async fn spark_snapshot_summary_with_container(
    container_name: &str,
    table_name: &str,
    snapshot_id: Option<i64>,
) -> Result<SnapshotSummaryResult, SparkValidationError> {
    let extra_args = match snapshot_id {
        Some(id) => vec![id.to_string()],
        None => vec![],
    };
    let args = spark_submit_args(container_name, table_name, "snapshot_summary", &extra_args);
    let output = run_spark_submit(container_name, args).await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(SparkValidationError {
            message: format!("spark-submit failed.\nstdout: {stdout}\nstderr: {stderr}"),
        });
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    parse_snapshot_summary_output(&stdout)
}

fn parse_snapshot_summary_output(
    stdout: &str,
) -> Result<SnapshotSummaryResult, SparkValidationError> {
    for line in stdout.lines().rev() {
        let trimmed = line.trim_start();
        if !trimmed.starts_with('{') {
            continue;
        }

        if let Ok(result) = serde_json::from_str::<SnapshotSummaryResult>(trimmed) {
            if let Some(ref error) = result.error {
                return Err(SparkValidationError {
                    message: format!("Spark snapshot summary failed: {error}"),
                });
            }
            return Ok(result);
        }
    }

    Err(SparkValidationError {
        message: format!("No JSON output found in Spark output: {stdout}"),
    })
}

/// Extract manifest entries for structure comparison.
///
/// # Arguments
///
/// * `table_name` - Name of the table in the default namespace
/// * `limit` - Maximum number of entries to return
pub async fn spark_manifest_entries(
    table_name: &str,
    limit: Option<i64>,
) -> Result<ManifestEntriesResult, SparkValidationError> {
    let container_name = get_spark_container_name()?;
    spark_manifest_entries_with_container(&container_name, table_name, limit).await
}

/// Extract manifest entries using an explicit container name.
pub async fn spark_manifest_entries_with_container(
    container_name: &str,
    table_name: &str,
    limit: Option<i64>,
) -> Result<ManifestEntriesResult, SparkValidationError> {
    let extra_args = match limit {
        Some(l) => vec![l.to_string()],
        None => vec![],
    };
    let args = spark_submit_args(container_name, table_name, "manifest_entries", &extra_args);
    let output = run_spark_submit(container_name, args).await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(SparkValidationError {
            message: format!("spark-submit failed.\nstdout: {stdout}\nstderr: {stderr}"),
        });
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    parse_manifest_entries_output(&stdout)
}

fn parse_manifest_entries_output(
    stdout: &str,
) -> Result<ManifestEntriesResult, SparkValidationError> {
    for line in stdout.lines().rev() {
        let trimmed = line.trim_start();
        if !trimmed.starts_with('{') {
            continue;
        }

        if let Ok(result) = serde_json::from_str::<ManifestEntriesResult>(trimmed) {
            if let Some(ref error) = result.error {
                return Err(SparkValidationError {
                    message: format!("Spark manifest entries failed: {error}"),
                });
            }
            return Ok(result);
        }
    }

    Err(SparkValidationError {
        message: format!("No JSON output found in Spark output: {stdout}"),
    })
}

/// Execute a DML operation via Spark and return the resulting snapshot summary.
///
/// This enables semantic parity testing by running the same DML operation
/// via both Rust and Spark, then comparing the resulting metadata.
///
/// # Arguments
///
/// * `table_name` - Name of the table in the default namespace
/// * `dml_type` - Type of DML operation ("delete" or "update")
/// * `predicate` - Optional WHERE clause predicate
/// * `update_values` - Optional SET clause for updates (e.g., "status='done'")
pub async fn spark_execute_dml(
    table_name: &str,
    dml_type: &str,
    predicate: Option<&str>,
    update_values: Option<&str>,
) -> Result<SnapshotSummaryResult, SparkValidationError> {
    let container_name = get_spark_container_name()?;
    spark_execute_dml_with_container(
        &container_name,
        table_name,
        dml_type,
        predicate,
        update_values,
    )
    .await
}

/// Execute a DML operation via Spark using an explicit container name.
pub async fn spark_execute_dml_with_container(
    container_name: &str,
    table_name: &str,
    dml_type: &str,
    predicate: Option<&str>,
    update_values: Option<&str>,
) -> Result<SnapshotSummaryResult, SparkValidationError> {
    let mut extra_args = vec![dml_type.to_string()];
    if let Some(pred) = predicate {
        extra_args.push(pred.to_string());
    }
    if let Some(vals) = update_values {
        // Ensure predicate is present (even if empty) when update_values is provided
        if predicate.is_none() {
            extra_args.push(String::new());
        }
        extra_args.push(vals.to_string());
    }

    let args = spark_submit_args(container_name, table_name, "spark_dml", &extra_args);
    let output = run_spark_submit(container_name, args).await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(SparkValidationError {
            message: format!("spark-submit failed.\nstdout: {stdout}\nstderr: {stderr}"),
        });
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    parse_snapshot_summary_output(&stdout)
}

/// Execute one or more SQL statements via Spark and return the resulting snapshot summary.
///
/// This supports multi-statement SQL separated by ';' and allows `{table}` placeholders.
pub async fn spark_execute_sql(
    table_name: &str,
    sql_template: &str,
) -> Result<SnapshotSummaryResult, SparkValidationError> {
    let container_name = get_spark_container_name()?;
    spark_execute_sql_with_container(&container_name, table_name, sql_template).await
}

/// Execute SQL via Spark using an explicit container name.
pub async fn spark_execute_sql_with_container(
    container_name: &str,
    table_name: &str,
    sql_template: &str,
) -> Result<SnapshotSummaryResult, SparkValidationError> {
    let extra_args = vec![sql_template.to_string()];
    let args = spark_submit_args(container_name, table_name, "execute_sql", &extra_args);
    let output = run_spark_submit(container_name, args).await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(SparkValidationError {
            message: format!("spark-submit failed.\nstdout: {stdout}\nstderr: {stderr}"),
        });
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    parse_snapshot_summary_output(&stdout)
}
