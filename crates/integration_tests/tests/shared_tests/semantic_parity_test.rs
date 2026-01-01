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

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::Utc;
use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use iceberg::spec::{DataContentType, ManifestStatus, PrimitiveType, StructType, Type};
use iceberg::transaction::{
    ApplyTransactionAction, RewriteDataFilesOptions, RewriteStrategy, Transaction,
};
use iceberg::{Catalog, CatalogBuilder, ErrorKind, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_datafusion::IcebergTableProvider;
use iceberg_datafusion::compaction::{CompactionOptions, compact_table};
use iceberg_integration_tests::spark_validator::{
    ManifestEntriesResult, ManifestEntryInfo, SnapshotSummaryFields, SnapshotSummaryResult,
    SparkValidationError, spark_execute_dml_with_container, spark_execute_sql_with_container,
    spark_manifest_entries_with_container, spark_snapshot_summary_with_container,
};
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::get_shared_containers;

const EXPECTED_DIVERGENCE_FIELDS: &[&str] = &[
    "snapshot-id",
    "committed-at",
    "added-files-size",
    "removed-files-size",
    "total-files-size",
];

/// Comparison result for a single field.
#[derive(Debug)]
pub struct FieldComparison {
    pub field_name: String,
    pub rust_value: Option<String>,
    pub spark_value: Option<String>,
    pub matches: bool,
    pub divergence_reason: Option<String>,
}

/// Overall parity comparison result.
#[derive(Debug)]
pub struct ParityResult {
    pub operation: String,
    pub rust_snapshot_id: Option<i64>,
    pub spark_snapshot_id: Option<i64>,
    pub field_comparisons: Vec<FieldComparison>,
    pub semantic_parity: bool,
}

impl ParityResult {
    /// Returns all fields that diverged (excluding expected divergences).
    pub fn divergences(&self) -> Vec<&FieldComparison> {
        self.field_comparisons
            .iter()
            .filter(|f| !f.matches && f.divergence_reason.is_none())
            .collect()
    }

    /// Returns a summary suitable for documentation.
    pub fn summary(&self) -> String {
        let mut lines = vec![
            format!("## {} Operation Parity", self.operation),
            String::new(),
            "| Field | Rust | Spark | Match | Notes |".to_string(),
            "|-------|------|-------|-------|-------|".to_string(),
        ];

        for fc in &self.field_comparisons {
            let rust_val = fc.rust_value.as_deref().unwrap_or("-");
            let spark_val = fc.spark_value.as_deref().unwrap_or("-");
            let match_icon = if fc.matches { "Yes" } else { "**No**" };
            let notes = fc.divergence_reason.as_deref().unwrap_or("");
            lines.push(format!(
                "| {} | {} | {} | {} | {} |",
                fc.field_name, rust_val, spark_val, match_icon, notes
            ));
        }

        lines.push(String::new());
        lines.push(format!(
            "**Semantic Parity:** {}",
            if self.semantic_parity {
                "PASS"
            } else {
                "**FAIL**"
            }
        ));

        lines.join("\n")
    }
}

/// Manifest entry parity comparison result.
#[derive(Debug)]
pub struct ManifestParityResult {
    pub operation: String,
    pub field_comparisons: Vec<FieldComparison>,
    pub semantic_parity: bool,
}

impl ManifestParityResult {
    pub fn summary(&self) -> String {
        let mut lines = vec![
            format!("## {} Manifest Entry Parity", self.operation),
            String::new(),
            "| Field | Rust | Spark | Match | Notes |".to_string(),
            "|-------|------|-------|-------|-------|".to_string(),
        ];

        for fc in &self.field_comparisons {
            let rust_val = fc.rust_value.as_deref().unwrap_or("-");
            let spark_val = fc.spark_value.as_deref().unwrap_or("-");
            let match_icon = if fc.matches { "Yes" } else { "**No**" };
            let notes = fc.divergence_reason.as_deref().unwrap_or("");
            lines.push(format!(
                "| {} | {} | {} | {} | {} |",
                fc.field_name, rust_val, spark_val, match_icon, notes
            ));
        }

        lines.push(String::new());
        lines.push(format!(
            "**Semantic Parity:** {}",
            if self.semantic_parity {
                "PASS"
            } else {
                "**FAIL**"
            }
        ));

        lines.join("\n")
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct RustManifestEntryInfo {
    status: i32,
    snapshot_id: Option<i64>,
    sequence_number: Option<i64>,
    file_sequence_number: Option<i64>,
    data_file: RustDataFileInfo,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct RustDataFileInfo {
    content: i32,
    file_format: String,
    partition: JsonValue,
    record_count: i64,
    file_size_in_bytes: i64,
}

/// Compare snapshot summary fields between Rust and Spark.
pub fn compare_snapshot_summaries(
    operation: &str,
    rust_summary: &SnapshotSummaryResult,
    spark_summary: &SnapshotSummaryResult,
) -> ParityResult {
    let mut comparisons = Vec::new();

    // Compare operation type
    comparisons.push(compare_field(
        "operation",
        rust_summary.operation.as_ref(),
        spark_summary.operation.as_ref(),
        None,
    ));

    let rust_parent_present = Some(rust_summary.parent_id.is_some().to_string());
    let spark_parent_present = Some(spark_summary.parent_id.is_some().to_string());
    comparisons.push(compare_field(
        "parent-snapshot-present",
        rust_parent_present.as_ref(),
        spark_parent_present.as_ref(),
        None,
    ));

    let rust_snapshot_id = rust_summary.snapshot_id.map(|v| v.to_string());
    let spark_snapshot_id = spark_summary.snapshot_id.map(|v| v.to_string());
    comparisons.push(compare_field(
        "snapshot-id",
        rust_snapshot_id.as_ref(),
        spark_snapshot_id.as_ref(),
        Some("Snapshot IDs are table-specific".to_string()),
    ));

    comparisons.push(compare_field(
        "committed-at",
        rust_summary.committed_at.as_ref(),
        spark_summary.committed_at.as_ref(),
        Some("Timestamp values expected to differ (Rust=epoch ms, Spark=timestamp)".to_string()),
    ));

    // Compare core count fields (MUST match)
    let rust_fields = rust_summary.summary.as_ref();
    let spark_fields = spark_summary.summary.as_ref();

    let field_pairs = [
        (
            "added-data-files",
            get_field(rust_fields, |f| &f.added_data_files),
            get_field(spark_fields, |f| &f.added_data_files),
        ),
        (
            "deleted-data-files",
            get_field(rust_fields, |f| &f.deleted_data_files),
            get_field(spark_fields, |f| &f.deleted_data_files),
        ),
        (
            "added-records",
            get_field(rust_fields, |f| &f.added_records),
            get_field(spark_fields, |f| &f.added_records),
        ),
        (
            "deleted-records",
            get_field(rust_fields, |f| &f.deleted_records),
            get_field(spark_fields, |f| &f.deleted_records),
        ),
        (
            "added-delete-files",
            get_field(rust_fields, |f| &f.added_delete_files),
            get_field(spark_fields, |f| &f.added_delete_files),
        ),
        (
            "removed-delete-files",
            get_field(rust_fields, |f| &f.removed_delete_files),
            get_field(spark_fields, |f| &f.removed_delete_files),
        ),
        (
            "added-position-deletes",
            get_field(rust_fields, |f| &f.added_position_deletes),
            get_field(spark_fields, |f| &f.added_position_deletes),
        ),
        (
            "removed-position-deletes",
            get_field(rust_fields, |f| &f.removed_position_deletes),
            get_field(spark_fields, |f| &f.removed_position_deletes),
        ),
        (
            "total-data-files",
            get_field(rust_fields, |f| &f.total_data_files),
            get_field(spark_fields, |f| &f.total_data_files),
        ),
        (
            "total-delete-files",
            get_field(rust_fields, |f| &f.total_delete_files),
            get_field(spark_fields, |f| &f.total_delete_files),
        ),
        (
            "total-records",
            get_field(rust_fields, |f| &f.total_records),
            get_field(spark_fields, |f| &f.total_records),
        ),
        (
            "changed-partition-count",
            get_field(rust_fields, |f| &f.changed_partition_count),
            get_field(spark_fields, |f| &f.changed_partition_count),
        ),
    ];

    for (name, rust_val, spark_val) in field_pairs {
        comparisons.push(compare_field(
            name,
            rust_val.as_ref(),
            spark_val.as_ref(),
            None,
        ));
    }

    // Size fields may differ due to compression - document as expected divergence
    let size_fields = [
        (
            "added-files-size",
            get_field(rust_fields, |f| &f.added_files_size),
            get_field(spark_fields, |f| &f.added_files_size),
        ),
        (
            "removed-files-size",
            get_field(rust_fields, |f| &f.removed_files_size),
            get_field(spark_fields, |f| &f.removed_files_size),
        ),
        (
            "total-files-size",
            get_field(rust_fields, |f| &f.total_files_size),
            get_field(spark_fields, |f| &f.total_files_size),
        ),
    ];

    for (name, rust_val, spark_val) in size_fields {
        let divergence_reason = if rust_val != spark_val {
            Some("Size may differ due to compression settings".to_string())
        } else {
            None
        };
        comparisons.push(compare_field(
            name,
            rust_val.as_ref(),
            spark_val.as_ref(),
            divergence_reason,
        ));
    }

    // Determine overall semantic parity (ignoring expected divergences)
    let semantic_parity = comparisons
        .iter()
        .filter(|c| c.divergence_reason.is_none())
        .all(|c| c.matches);

    ParityResult {
        operation: operation.to_string(),
        rust_snapshot_id: rust_summary.snapshot_id,
        spark_snapshot_id: spark_summary.snapshot_id,
        field_comparisons: comparisons,
        semantic_parity,
    }
}

fn get_field<F>(fields: Option<&SnapshotSummaryFields>, getter: F) -> Option<String>
where F: Fn(&SnapshotSummaryFields) -> &Option<String> {
    fields.and_then(|f| getter(f).clone())
}

fn compare_field(
    name: &str,
    rust_val: Option<&String>,
    spark_val: Option<&String>,
    divergence_reason: Option<String>,
) -> FieldComparison {
    let matches = rust_val == spark_val;
    FieldComparison {
        field_name: name.to_string(),
        rust_value: rust_val.cloned(),
        spark_value: spark_val.cloned(),
        matches,
        divergence_reason,
    }
}

fn assert_parity_expected_divergences(result: &ParityResult) {
    let mut unexpected = Vec::new();
    let mut unexpected_divergence_reasons = Vec::new();

    for comparison in &result.field_comparisons {
        if comparison.divergence_reason.is_some()
            && !EXPECTED_DIVERGENCE_FIELDS.contains(&comparison.field_name.as_str())
        {
            unexpected_divergence_reasons.push(format!(
                "{}: {}",
                comparison.field_name,
                comparison.divergence_reason.as_deref().unwrap_or("missing reason")
            ));
        }

        if !comparison.matches && comparison.divergence_reason.is_none() {
            unexpected.push(format!(
                "{}: Rust={:?}, Spark={:?}",
                comparison.field_name, comparison.rust_value, comparison.spark_value
            ));
        }
    }

    assert!(
        unexpected_divergence_reasons.is_empty(),
        "Unexpected divergence reasons for {}:\n{}",
        result.operation,
        unexpected_divergence_reasons.join("\n")
    );

    assert!(
        unexpected.is_empty(),
        "Unexpected divergences for {}:\n{}\n\n{}",
        result.operation,
        unexpected.join("\n"),
        result.summary()
    );
}

fn assert_error_contains(message: &str, expected_fragments: &[&str], context: &str) {
    let message_lower = message.to_lowercase();
    let matches = expected_fragments.iter().any(|fragment| {
        let fragment_lower = fragment.to_lowercase();
        message_lower.contains(&fragment_lower)
    });

    assert!(
        matches,
        "{context} error did not contain expected fragment(s) {:?}. Error: {}",
        expected_fragments,
        message
    );
}

fn assert_rust_schema_rejection(err: &iceberg::Error, expected_fragments: &[&str]) {
    assert_eq!(
        err.kind(),
        ErrorKind::DataInvalid,
        "Rust schema update error kind mismatch: {err}"
    );
    assert_error_contains(err.message(), expected_fragments, "Rust schema update");
}

fn assert_spark_schema_rejection(err: &SparkValidationError, expected_fragments: &[&str]) {
    assert_error_contains(&err.message, expected_fragments, "Spark schema update");
}

fn is_zero_or_none(value: Option<&String>) -> bool {
    match value {
        None => true,
        Some(value) => {
            let trimmed = value.trim();
            trimmed == "0" || trimmed == "0.0"
        }
    }
}

fn summary_is_noop(summary: &SnapshotSummaryResult) -> bool {
    let Some(fields) = summary.summary.as_ref() else {
        return false;
    };

    let zero_fields = [
        fields.added_data_files.as_ref(),
        fields.deleted_data_files.as_ref(),
        fields.added_records.as_ref(),
        fields.deleted_records.as_ref(),
        fields.added_delete_files.as_ref(),
        fields.removed_delete_files.as_ref(),
        fields.added_position_deletes.as_ref(),
        fields.removed_position_deletes.as_ref(),
        fields.added_equality_deletes.as_ref(),
        fields.removed_equality_deletes.as_ref(),
        fields.added_files_size.as_ref(),
        fields.removed_files_size.as_ref(),
        fields.changed_partition_count.as_ref(),
    ];

    zero_fields.into_iter().all(is_zero_or_none)
}

fn manifest_status_to_i32(status: ManifestStatus) -> i32 {
    match status {
        ManifestStatus::Existing => 0,
        ManifestStatus::Added => 1,
        ManifestStatus::Deleted => 2,
    }
}

fn data_content_to_i32(content: DataContentType) -> i32 {
    match content {
        DataContentType::Data => 0,
        DataContentType::PositionDeletes => 1,
        DataContentType::EqualityDeletes => 2,
    }
}

fn partition_struct_to_json(
    partition: &iceberg::spec::Struct,
    partition_type: &StructType,
) -> iceberg::Result<JsonValue> {
    let mut map = JsonMap::new();
    for (field, value) in partition_type.fields().iter().zip(partition.iter()) {
        let json_value = match value {
            Some(literal) => literal.clone().try_into_json(&field.field_type)?,
            None => JsonValue::Null,
        };
        map.insert(field.name.clone(), json_value);
    }
    Ok(JsonValue::Object(map))
}

async fn extract_rust_manifest_entries(
    table: &iceberg::table::Table,
) -> iceberg::Result<Vec<RustManifestEntryInfo>> {
    let metadata = table.metadata();
    let Some(snapshot) = metadata.current_snapshot() else {
        return Ok(Vec::new());
    };

    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), metadata)
        .await?;

    let mut entries = Vec::new();
    for manifest_file in manifest_list.entries() {
        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await?;
        let partition_type = manifest
            .metadata()
            .partition_spec()
            .partition_type(manifest.metadata().schema())?;

        for entry_ref in manifest.entries() {
            let entry = entry_ref.as_ref();
            let data_file = entry.data_file();
            let partition_json = partition_struct_to_json(data_file.partition(), &partition_type)?;
            entries.push(RustManifestEntryInfo {
                status: manifest_status_to_i32(entry.status()),
                snapshot_id: entry.snapshot_id(),
                sequence_number: entry.sequence_number(),
                file_sequence_number: entry.file_sequence_number,
                data_file: RustDataFileInfo {
                    content: data_content_to_i32(data_file.content_type()),
                    file_format: data_file.file_format().to_string().to_ascii_lowercase(),
                    partition: partition_json,
                    record_count: data_file.record_count() as i64,
                    file_size_in_bytes: data_file.file_size_in_bytes() as i64,
                },
            });
        }
    }

    Ok(entries)
}

fn partition_multiset_from_rust(entries: &[RustManifestEntryInfo]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for entry in entries {
        let key = serde_json::to_string(&entry.data_file.partition)
            .unwrap_or_else(|_| "null".to_string());
        *counts.entry(key).or_insert(0) += 1;
    }
    counts
}

fn partition_multiset_from_spark(entries: &[ManifestEntryInfo]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for entry in entries {
        let value = entry
            .data_file
            .as_ref()
            .and_then(|df| df.partition.clone())
            .unwrap_or(JsonValue::Null);
        let key = serde_json::to_string(&value).unwrap_or_else(|_| "null".to_string());
        *counts.entry(key).or_insert(0) += 1;
    }
    counts
}

fn compare_manifest_entries(
    operation: &str,
    rust_entries: &[RustManifestEntryInfo],
    spark_entries: &ManifestEntriesResult,
) -> ManifestParityResult {
    let mut comparisons = Vec::new();
    let spark_entry_list = spark_entries.entries.as_deref().unwrap_or(&[]);

    let rust_entry_count = Some(rust_entries.len().to_string());
    let spark_entry_count = spark_entries
        .entry_count
        .map(|count| count.to_string())
        .or_else(|| Some(spark_entry_list.len().to_string()));
    comparisons.push(compare_field(
        "entry-count",
        rust_entry_count.as_ref(),
        spark_entry_count.as_ref(),
        None,
    ));

    let mut rust_sorted: Vec<&RustManifestEntryInfo> = rust_entries.iter().collect();
    rust_sorted.sort_by_key(|entry| {
        (
            entry.file_sequence_number.unwrap_or(i64::MAX),
            entry.sequence_number.unwrap_or(i64::MAX),
            entry.status,
        )
    });

    let mut spark_sorted: Vec<&ManifestEntryInfo> = spark_entry_list.iter().collect();
    spark_sorted.sort_by_key(|entry| {
        (
            entry.file_sequence_number.unwrap_or(i64::MAX),
            entry.sequence_number.unwrap_or(i64::MAX),
            entry.status.unwrap_or(-1),
        )
    });

    let rust_statuses: Vec<i32> = rust_sorted.iter().map(|entry| entry.status).collect();
    let spark_statuses: Vec<i32> = spark_sorted
        .iter()
        .map(|entry| entry.status.unwrap_or(-1))
        .collect();
    let rust_statuses_str = format!("{:?}", rust_statuses);
    let spark_statuses_str = format!("{:?}", spark_statuses);
    comparisons.push(compare_field(
        "status-sequence",
        Some(&rust_statuses_str),
        Some(&spark_statuses_str),
        None,
    ));

    let rust_contents: Vec<i32> = rust_sorted.iter().map(|entry| entry.data_file.content).collect();
    let spark_contents: Vec<i32> = spark_sorted
        .iter()
        .map(|entry| {
            entry
                .data_file
                .as_ref()
                .and_then(|df| df.content)
                .unwrap_or(-1)
        })
        .collect();
    let rust_contents_str = format!("{:?}", rust_contents);
    let spark_contents_str = format!("{:?}", spark_contents);
    comparisons.push(compare_field(
        "content-sequence",
        Some(&rust_contents_str),
        Some(&spark_contents_str),
        None,
    ));

    let rust_partition_counts = partition_multiset_from_rust(rust_entries);
    let spark_partition_counts = partition_multiset_from_spark(spark_entry_list);
    let rust_partitions_str = format!("{:?}", rust_partition_counts);
    let spark_partitions_str = format!("{:?}", spark_partition_counts);
    comparisons.push(compare_field(
        "partition-values",
        Some(&rust_partitions_str),
        Some(&spark_partitions_str),
        None,
    ));

    let rust_seq_present = rust_entries
        .iter()
        .all(|entry| entry.sequence_number.is_some());
    let spark_seq_present = spark_entry_list
        .iter()
        .all(|entry| entry.sequence_number.is_some());
    let rust_seq_present_str = rust_seq_present.to_string();
    let spark_seq_present_str = spark_seq_present.to_string();
    comparisons.push(compare_field(
        "sequence-number-present",
        Some(&rust_seq_present_str),
        Some(&spark_seq_present_str),
        None,
    ));

    let rust_file_seq_present = rust_entries
        .iter()
        .all(|entry| entry.file_sequence_number.is_some());
    let spark_file_seq_present = spark_entry_list
        .iter()
        .all(|entry| entry.file_sequence_number.is_some());
    let rust_file_seq_present_str = rust_file_seq_present.to_string();
    let spark_file_seq_present_str = spark_file_seq_present.to_string();
    comparisons.push(compare_field(
        "file-sequence-number-present",
        Some(&rust_file_seq_present_str),
        Some(&spark_file_seq_present_str),
        None,
    ));

    let semantic_parity = comparisons
        .iter()
        .filter(|c| c.divergence_reason.is_none())
        .all(|c| c.matches);

    ManifestParityResult {
        operation: operation.to_string(),
        field_comparisons: comparisons,
        semantic_parity,
    }
}

/// Extract Rust snapshot summary from table metadata.
pub fn extract_rust_snapshot_summary(table: &iceberg::table::Table) -> SnapshotSummaryResult {
    let metadata = table.metadata();
    let current = metadata.current_snapshot();

    match current {
        Some(snapshot) => {
            let summary = snapshot.summary();
            SnapshotSummaryResult {
                error: None,
                snapshot_id: Some(snapshot.snapshot_id()),
                parent_id: snapshot.parent_snapshot_id(),
                operation: Some(format!("{:?}", summary.operation).to_lowercase()),
                committed_at: Some(snapshot.timestamp_ms().to_string()),
                summary: Some(SnapshotSummaryFields {
                    added_data_files: summary
                        .additional_properties
                        .get("added-data-files")
                        .cloned(),
                    deleted_data_files: summary
                        .additional_properties
                        .get("deleted-data-files")
                        .cloned(),
                    added_records: summary.additional_properties.get("added-records").cloned(),
                    deleted_records: summary
                        .additional_properties
                        .get("deleted-records")
                        .cloned(),
                    added_files_size: summary
                        .additional_properties
                        .get("added-files-size")
                        .cloned(),
                    removed_files_size: summary
                        .additional_properties
                        .get("removed-files-size")
                        .cloned(),
                    added_delete_files: summary
                        .additional_properties
                        .get("added-delete-files")
                        .cloned(),
                    removed_delete_files: summary
                        .additional_properties
                        .get("removed-delete-files")
                        .cloned(),
                    added_position_deletes: summary
                        .additional_properties
                        .get("added-position-deletes")
                        .cloned(),
                    removed_position_deletes: summary
                        .additional_properties
                        .get("removed-position-deletes")
                        .cloned(),
                    added_equality_deletes: summary
                        .additional_properties
                        .get("added-equality-deletes")
                        .cloned(),
                    removed_equality_deletes: summary
                        .additional_properties
                        .get("removed-equality-deletes")
                        .cloned(),
                    total_data_files: summary
                        .additional_properties
                        .get("total-data-files")
                        .cloned(),
                    total_delete_files: summary
                        .additional_properties
                        .get("total-delete-files")
                        .cloned(),
                    total_records: summary.additional_properties.get("total-records").cloned(),
                    total_files_size: summary
                        .additional_properties
                        .get("total-files-size")
                        .cloned(),
                    total_position_deletes: summary
                        .additional_properties
                        .get("total-position-deletes")
                        .cloned(),
                    total_equality_deletes: summary
                        .additional_properties
                        .get("total-equality-deletes")
                        .cloned(),
                    changed_partition_count: summary
                        .additional_properties
                        .get("changed-partition-count")
                        .cloned(),
                    raw: None,
                }),
            }
        }
        None => SnapshotSummaryResult {
            error: Some("No current snapshot".to_string()),
            snapshot_id: None,
            parent_id: None,
            operation: None,
            committed_at: None,
            summary: None,
        },
    }
}

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
