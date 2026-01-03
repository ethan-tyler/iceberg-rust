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

use std::collections::BTreeMap;

use iceberg::ErrorKind;
use iceberg::spec::{DataContentType, ManifestStatus, Struct, StructType};
use iceberg_integration_tests::spark_validator::{
    ManifestEntriesResult, ManifestEntryInfo, SnapshotSummaryFields, SnapshotSummaryResult,
    SparkValidationError,
};
use serde_json::{Map as JsonMap, Value as JsonValue};

const EXPECTED_DIVERGENCE_FIELDS: &[&str] = &[
    "snapshot-id",
    "committed-at",
    "added-files-size",
    "removed-files-size",
    "total-files-size",
];

#[derive(Debug)]
pub(crate) struct FieldComparison {
    pub(crate) field_name: String,
    pub(crate) rust_value: Option<String>,
    pub(crate) spark_value: Option<String>,
    pub(crate) matches: bool,
    pub(crate) divergence_reason: Option<String>,
}

#[derive(Debug)]
pub(crate) struct ParityResult {
    pub(crate) operation: String,
    pub(crate) field_comparisons: Vec<FieldComparison>,
    pub(crate) semantic_parity: bool,
}

impl ParityResult {
    pub(crate) fn divergences(&self) -> Vec<&FieldComparison> {
        self.field_comparisons
            .iter()
            .filter(|f| !f.matches && f.divergence_reason.is_none())
            .collect()
    }

    pub(crate) fn summary(&self) -> String {
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

#[derive(Debug)]
pub(crate) struct ManifestParityResult {
    pub(crate) operation: String,
    pub(crate) field_comparisons: Vec<FieldComparison>,
    pub(crate) semantic_parity: bool,
}

impl ManifestParityResult {
    pub(crate) fn summary(&self) -> String {
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

#[derive(Debug, Clone)]
pub(crate) struct RustManifestEntryInfo {
    status: i32,
    sequence_number: Option<i64>,
    file_sequence_number: Option<i64>,
    data_file: RustDataFileInfo,
}

#[derive(Debug, Clone)]
pub(crate) struct RustDataFileInfo {
    content: i32,
    partition: JsonValue,
}

pub(crate) fn compare_snapshot_summaries(
    operation: &str,
    rust_summary: &SnapshotSummaryResult,
    spark_summary: &SnapshotSummaryResult,
) -> ParityResult {
    let mut comparisons = Vec::new();

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

    let semantic_parity = comparisons
        .iter()
        .filter(|c| c.divergence_reason.is_none())
        .all(|c| c.matches);

    ParityResult {
        operation: operation.to_string(),
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

pub(crate) fn assert_parity_expected_divergences(result: &ParityResult) {
    let mut unexpected = Vec::new();
    let mut unexpected_divergence_reasons = Vec::new();

    for comparison in &result.field_comparisons {
        if comparison.divergence_reason.is_some()
            && !EXPECTED_DIVERGENCE_FIELDS.contains(&comparison.field_name.as_str())
        {
            unexpected_divergence_reasons.push(format!(
                "{}: {}",
                comparison.field_name,
                comparison
                    .divergence_reason
                    .as_deref()
                    .unwrap_or("missing reason")
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

pub(crate) fn assert_error_contains(message: &str, expected_fragments: &[&str], context: &str) {
    let message_lower = message.to_lowercase();
    let matches = expected_fragments.iter().any(|fragment| {
        let fragment_lower = fragment.to_lowercase();
        message_lower.contains(&fragment_lower)
    });

    assert!(
        matches,
        "{context} error did not contain expected fragment(s) {expected_fragments:?}. Error: {message}"
    );
}

pub(crate) fn assert_rust_schema_rejection(err: &iceberg::Error, expected_fragments: &[&str]) {
    assert_eq!(
        err.kind(),
        ErrorKind::DataInvalid,
        "Rust schema update error kind mismatch: {err}"
    );
    assert_error_contains(err.message(), expected_fragments, "Rust schema update");
}

pub(crate) fn assert_spark_schema_rejection(
    err: &SparkValidationError,
    expected_fragments: &[&str],
) {
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

pub(crate) fn summary_is_noop(summary: &SnapshotSummaryResult) -> bool {
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
    partition: &Struct,
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

pub(crate) async fn extract_rust_manifest_entries(
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
        let manifest = manifest_file.load_manifest(table.file_io()).await?;
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
                sequence_number: entry.sequence_number(),
                file_sequence_number: entry.file_sequence_number,
                data_file: RustDataFileInfo {
                    content: data_content_to_i32(data_file.content_type()),
                    partition: partition_json,
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

pub(crate) fn compare_manifest_entries(
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
    let rust_statuses_str = format!("{rust_statuses:?}");
    let spark_statuses_str = format!("{spark_statuses:?}");
    comparisons.push(compare_field(
        "status-sequence",
        Some(&rust_statuses_str),
        Some(&spark_statuses_str),
        None,
    ));

    let rust_contents: Vec<i32> = rust_sorted
        .iter()
        .map(|entry| entry.data_file.content)
        .collect();
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
    let rust_contents_str = format!("{rust_contents:?}");
    let spark_contents_str = format!("{spark_contents:?}");
    comparisons.push(compare_field(
        "content-sequence",
        Some(&rust_contents_str),
        Some(&spark_contents_str),
        None,
    ));

    let rust_partition_counts = partition_multiset_from_rust(rust_entries);
    let spark_partition_counts = partition_multiset_from_spark(spark_entry_list);
    let rust_partitions_str = format!("{rust_partition_counts:?}");
    let spark_partitions_str = format!("{spark_partition_counts:?}");
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

pub(crate) fn extract_rust_snapshot_summary(
    table: &iceberg::table::Table,
) -> SnapshotSummaryResult {
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
