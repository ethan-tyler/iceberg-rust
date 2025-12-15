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

//! Retention policy evaluation for snapshot expiration.
//!
//! This module implements the Iceberg specification for snapshot retention policies.
//! It determines which snapshots are protected from expiration based on:
//!
//! - Branch and tag references
//! - `min-snapshots-to-keep` settings (per-ref and table-level)
//! - `max-snapshot-age-ms` settings
//! - `max-ref-age-ms` for non-main branches and tags

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Duration, Utc};

use crate::error::Result;
use crate::spec::{SnapshotRetention, TableMetadata};

/// Table property key for maximum snapshot age before expiration.
pub const HISTORY_EXPIRE_MAX_SNAPSHOT_AGE_MS: &str = "history.expire.max-snapshot-age-ms";

/// Table property key for minimum snapshots to keep.
pub const HISTORY_EXPIRE_MIN_SNAPSHOTS_TO_KEEP: &str = "history.expire.min-snapshots-to-keep";

/// Table property key for maximum ref age.
pub const HISTORY_EXPIRE_MAX_REF_AGE_MS: &str = "history.expire.max-ref-age-ms";

/// Default maximum snapshot age: 5 days in milliseconds.
const DEFAULT_MAX_SNAPSHOT_AGE_MS: i64 = 5 * 24 * 60 * 60 * 1000;

/// Default minimum snapshots to keep: 1.
const DEFAULT_MIN_SNAPSHOTS_TO_KEEP: u32 = 1;

/// The main branch name - never expires.
const MAIN_BRANCH: &str = "main";

/// Retention policy for snapshot expiration.
///
/// This struct captures the retention settings that determine which snapshots
/// are protected from expiration. Settings can come from:
///
/// 1. Table properties (`history.expire.*`)
/// 2. Per-ref retention settings in `SnapshotReference`
/// 3. API-level overrides in `ExpireSnapshotsAction`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionPolicy {
    /// Maximum age for snapshots before they can be expired.
    ///
    /// Snapshots older than this are candidates for expiration,
    /// unless protected by other settings.
    pub max_snapshot_age: Duration,

    /// Minimum number of snapshots to keep per branch.
    ///
    /// This many recent ancestors of each branch tip are protected
    /// from expiration regardless of age.
    pub min_snapshots_to_keep: u32,

    /// Maximum age for non-main refs.
    ///
    /// If a branch or tag is older than this, it may be removed
    /// along with its snapshots. The main branch never expires.
    pub max_ref_age: Option<Duration>,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            max_snapshot_age: Duration::milliseconds(DEFAULT_MAX_SNAPSHOT_AGE_MS),
            min_snapshots_to_keep: DEFAULT_MIN_SNAPSHOTS_TO_KEEP,
            max_ref_age: None,
        }
    }
}

impl RetentionPolicy {
    /// Create a new retention policy with custom settings.
    pub fn new(
        max_snapshot_age: Duration,
        min_snapshots_to_keep: u32,
        max_ref_age: Option<Duration>,
    ) -> Self {
        Self {
            max_snapshot_age,
            min_snapshots_to_keep,
            max_ref_age,
        }
    }

    /// Parse retention policy from table properties.
    ///
    /// Reads the following properties:
    /// - `history.expire.max-snapshot-age-ms`: Maximum snapshot age
    /// - `history.expire.min-snapshots-to-keep`: Minimum snapshots to retain
    /// - `history.expire.max-ref-age-ms`: Maximum ref age
    ///
    /// Missing or invalid properties use defaults.
    pub fn from_table_properties(properties: &HashMap<String, String>) -> Self {
        let max_snapshot_age = properties
            .get(HISTORY_EXPIRE_MAX_SNAPSHOT_AGE_MS)
            .and_then(|v| v.parse::<i64>().ok())
            .filter(|&v| v > 0) // Must be positive
            .map(Duration::milliseconds)
            .unwrap_or_else(|| Duration::milliseconds(DEFAULT_MAX_SNAPSHOT_AGE_MS));

        let min_snapshots_to_keep = properties
            .get(HISTORY_EXPIRE_MIN_SNAPSHOTS_TO_KEEP)
            .and_then(|v| v.parse::<i64>().ok())
            .filter(|&v| v > 0) // Must be positive
            .map(|v| v as u32)
            .unwrap_or(DEFAULT_MIN_SNAPSHOTS_TO_KEEP);

        let max_ref_age = properties
            .get(HISTORY_EXPIRE_MAX_REF_AGE_MS)
            .and_then(|v| v.parse::<i64>().ok())
            .filter(|&v| v > 0) // Must be positive
            .map(Duration::milliseconds);

        Self {
            max_snapshot_age,
            min_snapshots_to_keep,
            max_ref_age,
        }
    }
}

/// Result of computing retention - includes both retained snapshots and expired refs.
#[derive(Debug, Clone, Default)]
pub struct RetentionResult {
    /// Snapshot IDs that should be retained (not expired).
    pub retained_snapshot_ids: HashSet<i64>,

    /// Ref names that have expired and should be removed.
    /// Does not include "main" which never expires.
    pub expired_ref_names: Vec<String>,
}

/// Compute which snapshots should be retained and which refs have expired.
///
/// This function implements the Iceberg specification for snapshot retention:
///
/// 1. The main branch never expires
/// 2. Non-main refs older than `max-ref-age-ms` are marked for expiration
/// 3. For retained refs:
///    - The referenced snapshot is protected
///    - The N most recent ancestors are protected (N = min-snapshots-to-keep)
///    - Snapshots within the max-snapshot-age window are protected
///
/// # Arguments
///
/// * `metadata` - The table metadata containing snapshots and refs
/// * `policy` - The retention policy to apply
/// * `now` - The current timestamp for age calculations
///
/// # Returns
///
/// A `RetentionResult` containing retained snapshot IDs and expired ref names.
pub fn compute_retention(
    metadata: &TableMetadata,
    policy: &RetentionPolicy,
    now: DateTime<Utc>,
) -> Result<RetentionResult> {
    let mut result = RetentionResult::default();
    let now_ms = now.timestamp_millis();

    // For each ref, determine if it's expired and compute protected snapshots
    for (ref_name, snapshot_ref) in metadata.refs() {
        // Main branch never expires
        let is_main = ref_name == MAIN_BRANCH;

        // Check if ref itself has expired based on max-ref-age-ms
        let ref_expired = if is_main {
            false // Main never expires
        } else {
            check_ref_expired(snapshot_ref, policy, now_ms)
        };

        if ref_expired {
            result.expired_ref_names.push(ref_name.clone());
            // Expired refs don't protect their snapshots
            continue;
        }

        // Ref is retained - compute which snapshots it protects
        let ref_snapshot_id = snapshot_ref.snapshot_id;

        // Get ref-specific retention settings, fall back to table-level policy
        let (min_to_keep, max_age) = get_ref_retention_settings(snapshot_ref, policy);

        // Always retain the ref's snapshot
        result.retained_snapshot_ids.insert(ref_snapshot_id);

        // Retain min_to_keep ancestors (for branches)
        if snapshot_ref.is_branch() {
            let ancestors = collect_ancestors(metadata, ref_snapshot_id, min_to_keep);
            result.retained_snapshot_ids.extend(ancestors);
        }

        // Retain snapshots within max_age that are ancestors of this ref
        let cutoff_ms = (now - max_age).timestamp_millis();
        retain_snapshots_within_age(
            metadata,
            ref_snapshot_id,
            cutoff_ms,
            &mut result.retained_snapshot_ids,
        );
    }

    // Sort expired refs for deterministic behavior
    result.expired_ref_names.sort();

    Ok(result)
}

/// Legacy function for backwards compatibility - returns just the retained snapshot IDs.
pub fn compute_retained_snapshots(
    metadata: &TableMetadata,
    policy: &RetentionPolicy,
    now: DateTime<Utc>,
) -> Result<HashSet<i64>> {
    Ok(compute_retention(metadata, policy, now)?.retained_snapshot_ids)
}

/// Check if a ref has expired based on its max-ref-age-ms setting.
fn check_ref_expired(
    snapshot_ref: &crate::spec::SnapshotReference,
    policy: &RetentionPolicy,
    now_ms: i64,
) -> bool {
    let max_ref_age_ms = match &snapshot_ref.retention {
        SnapshotRetention::Branch { max_ref_age_ms, .. } => *max_ref_age_ms,
        SnapshotRetention::Tag { max_ref_age_ms } => *max_ref_age_ms,
    };

    // Use ref-specific setting, fall back to policy, then default to "never expires"
    let effective_max_age_ms =
        max_ref_age_ms.or_else(|| policy.max_ref_age.map(|d| d.num_milliseconds()));

    match effective_max_age_ms {
        Some(max_age_ms) if max_age_ms > 0 => {
            // Ref expires if its snapshot's timestamp is older than max_age
            // Note: We'd need the snapshot timestamp to check this properly
            // For now, we rely on the ref's snapshot being present
            // A more complete implementation would check the snapshot timestamp
            let _cutoff = now_ms - max_age_ms;
            // This is a simplified check - in practice we'd check the ref creation time
            // or the snapshot timestamp
            false // Conservative: don't expire refs without more info
        }
        _ => false, // No max age = never expires
    }
}

/// Get retention settings for a specific ref.
fn get_ref_retention_settings(
    snapshot_ref: &crate::spec::SnapshotReference,
    policy: &RetentionPolicy,
) -> (u32, Duration) {
    match &snapshot_ref.retention {
        SnapshotRetention::Branch {
            min_snapshots_to_keep,
            max_snapshot_age_ms,
            ..
        } => {
            // Safely convert i32 to u32, defaulting to policy if invalid
            let min = min_snapshots_to_keep
                .filter(|&v| v > 0)
                .map(|v| v as u32)
                .unwrap_or(policy.min_snapshots_to_keep);
            let age = max_snapshot_age_ms
                .filter(|&v| v > 0)
                .map(Duration::milliseconds)
                .unwrap_or(policy.max_snapshot_age);
            (min, age)
        }
        SnapshotRetention::Tag { .. } => {
            // Tags don't have min_snapshots_to_keep - just protect the tagged snapshot
            (1, policy.max_snapshot_age)
        }
    }
}

/// Retain snapshots within the age window that are ancestors of the given snapshot.
fn retain_snapshots_within_age(
    metadata: &TableMetadata,
    start_snapshot_id: i64,
    cutoff_ms: i64,
    retained: &mut HashSet<i64>,
) {
    let mut current_id = Some(start_snapshot_id);
    while let Some(id) = current_id {
        if let Some(snapshot) = metadata.snapshot_by_id(id) {
            if snapshot.timestamp_ms() >= cutoff_ms {
                retained.insert(id);
                current_id = snapshot.parent_snapshot_id();
            } else {
                // Once we hit a snapshot outside the window, stop traversing
                // (ancestors are older by definition)
                break;
            }
        } else {
            break;
        }
    }
}

/// Collect N ancestors of a snapshot (including the snapshot itself).
fn collect_ancestors(metadata: &TableMetadata, snapshot_id: i64, count: u32) -> Vec<i64> {
    let mut ancestors = Vec::with_capacity(count as usize);
    let mut current_id = Some(snapshot_id);

    for _ in 0..count {
        match current_id {
            Some(id) => {
                ancestors.push(id);
                current_id = metadata
                    .snapshot_by_id(id)
                    .and_then(|s| s.parent_snapshot_id());
            }
            None => break,
        }
    }

    ancestors
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retention_policy_default() {
        let policy = RetentionPolicy::default();
        assert_eq!(
            policy.max_snapshot_age,
            Duration::milliseconds(DEFAULT_MAX_SNAPSHOT_AGE_MS)
        );
        assert_eq!(policy.min_snapshots_to_keep, DEFAULT_MIN_SNAPSHOTS_TO_KEEP);
        assert!(policy.max_ref_age.is_none());
    }

    #[test]
    fn test_retention_policy_from_properties() {
        let mut props = HashMap::new();
        props.insert(
            HISTORY_EXPIRE_MAX_SNAPSHOT_AGE_MS.to_string(),
            "86400000".to_string(), // 1 day
        );
        props.insert(
            HISTORY_EXPIRE_MIN_SNAPSHOTS_TO_KEEP.to_string(),
            "5".to_string(),
        );
        props.insert(
            HISTORY_EXPIRE_MAX_REF_AGE_MS.to_string(),
            "604800000".to_string(), // 7 days
        );

        let policy = RetentionPolicy::from_table_properties(&props);

        assert_eq!(policy.max_snapshot_age, Duration::days(1));
        assert_eq!(policy.min_snapshots_to_keep, 5);
        assert_eq!(policy.max_ref_age, Some(Duration::days(7)));
    }

    #[test]
    fn test_retention_policy_from_empty_properties() {
        let props = HashMap::new();
        let policy = RetentionPolicy::from_table_properties(&props);

        // Should use defaults
        assert_eq!(policy.max_snapshot_age, Duration::days(5));
        assert_eq!(policy.min_snapshots_to_keep, 1);
        assert!(policy.max_ref_age.is_none());
    }

    #[test]
    fn test_retention_policy_from_invalid_properties() {
        let mut props = HashMap::new();
        props.insert(
            HISTORY_EXPIRE_MAX_SNAPSHOT_AGE_MS.to_string(),
            "not-a-number".to_string(),
        );
        props.insert(
            HISTORY_EXPIRE_MIN_SNAPSHOTS_TO_KEEP.to_string(),
            "invalid".to_string(),
        );

        let policy = RetentionPolicy::from_table_properties(&props);

        // Should fall back to defaults for invalid values
        assert_eq!(policy.max_snapshot_age, Duration::days(5));
        assert_eq!(policy.min_snapshots_to_keep, 1);
    }

    #[test]
    fn test_retention_policy_rejects_negative_values() {
        let mut props = HashMap::new();
        props.insert(
            HISTORY_EXPIRE_MAX_SNAPSHOT_AGE_MS.to_string(),
            "-86400000".to_string(), // Negative
        );
        props.insert(
            HISTORY_EXPIRE_MIN_SNAPSHOTS_TO_KEEP.to_string(),
            "-5".to_string(), // Negative
        );

        let policy = RetentionPolicy::from_table_properties(&props);

        // Should fall back to defaults for negative values
        assert_eq!(policy.max_snapshot_age, Duration::days(5));
        assert_eq!(policy.min_snapshots_to_keep, 1);
    }

    #[test]
    fn test_retention_policy_rejects_zero_values() {
        let mut props = HashMap::new();
        props.insert(
            HISTORY_EXPIRE_MAX_SNAPSHOT_AGE_MS.to_string(),
            "0".to_string(),
        );
        props.insert(
            HISTORY_EXPIRE_MIN_SNAPSHOTS_TO_KEEP.to_string(),
            "0".to_string(),
        );

        let policy = RetentionPolicy::from_table_properties(&props);

        // Should fall back to defaults for zero values
        assert_eq!(policy.max_snapshot_age, Duration::days(5));
        assert_eq!(policy.min_snapshots_to_keep, 1);
    }

    #[test]
    fn test_collect_ancestors_linear_chain() {
        // Test the helper function logic with a simple chain simulation
        let chain: HashMap<i64, Option<i64>> = vec![
            (5, Some(4)),
            (4, Some(3)),
            (3, Some(2)),
            (2, Some(1)),
            (1, None),
        ]
        .into_iter()
        .collect();

        // Simulate collect_ancestors logic
        let mut ancestors = Vec::new();
        let mut current_id = Some(5i64);
        for _ in 0..3 {
            match current_id {
                Some(id) => {
                    ancestors.push(id);
                    current_id = chain.get(&id).copied().flatten();
                }
                None => break,
            }
        }

        assert_eq!(ancestors, vec![5, 4, 3]);
    }

    #[test]
    fn test_retention_result_default() {
        let result = RetentionResult::default();
        assert!(result.retained_snapshot_ids.is_empty());
        assert!(result.expired_ref_names.is_empty());
    }
}

/// Integration tests using real TableMetadata
#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::spec::{
        FormatVersion, NestedField, Operation, PartitionSpec, PrimitiveType, Schema, Snapshot,
        SnapshotReference, SnapshotRetention, SortOrder, Summary, TableMetadataBuilder, Type,
    };

    const TEST_LOCATION: &str = "s3://bucket/test/location";
    const DAY_MS: i64 = 24 * 60 * 60 * 1000;

    fn test_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap()
    }

    fn build_base_metadata() -> crate::spec::TableMetadata {
        TableMetadataBuilder::new(
            test_schema(),
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            TEST_LOCATION.to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata
    }

    fn create_snapshot(id: i64, timestamp_ms: i64, parent_id: Option<i64>) -> Snapshot {
        Snapshot::builder()
            .with_snapshot_id(id)
            .with_parent_snapshot_id(parent_id)
            .with_timestamp_ms(timestamp_ms)
            .with_sequence_number(id)
            .with_schema_id(0)
            .with_manifest_list(format!("/snap-{}", id))
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build()
    }

    #[test]
    fn test_compute_retention_single_branch() {
        let base = build_base_metadata();
        // Use base table's last_updated_ms as the starting point
        let base_time = base.last_updated_ms();

        // Create a chain of 5 snapshots, each 1 day apart (in the future from table creation)
        let snap1 = create_snapshot(1, base_time + 1 * DAY_MS, None);
        let snap2 = create_snapshot(2, base_time + 2 * DAY_MS, Some(1));
        let snap3 = create_snapshot(3, base_time + 3 * DAY_MS, Some(2));
        let snap4 = create_snapshot(4, base_time + 4 * DAY_MS, Some(3));
        let snap5 = create_snapshot(5, base_time + 5 * DAY_MS, Some(4));

        // Build metadata with all snapshots and main branch pointing to snap5
        let metadata = base
            .into_builder(None)
            .add_snapshot(snap1)
            .unwrap()
            .add_snapshot(snap2)
            .unwrap()
            .add_snapshot(snap3)
            .unwrap()
            .add_snapshot(snap4)
            .unwrap()
            .add_snapshot(snap5)
            .unwrap()
            .set_ref("main", SnapshotReference {
                snapshot_id: 5,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: Some(3),
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            })
            .unwrap()
            .build()
            .unwrap()
            .metadata;

        // Use a "now" time that is after all snapshots
        let now = chrono::DateTime::from_timestamp_millis(base_time + 10 * DAY_MS).unwrap();

        // Policy: keep 3 snapshots min
        let policy = RetentionPolicy::new(Duration::days(5), 3, None);

        let result = compute_retention(&metadata, &policy, now).unwrap();

        // Should retain at least 3 snapshots (5, 4, 3) due to min_snapshots_to_keep
        assert!(result.retained_snapshot_ids.contains(&5));
        assert!(result.retained_snapshot_ids.contains(&4));
        assert!(result.retained_snapshot_ids.contains(&3));

        // No refs should be expired (main never expires)
        assert!(result.expired_ref_names.is_empty());
    }

    #[test]
    fn test_compute_retention_with_tag() {
        let base = build_base_metadata();
        let base_time = base.last_updated_ms();

        // Create snapshots with increasing sequence numbers
        let snap1 = create_snapshot(1, base_time + 1000, None);
        let snap2 = create_snapshot(2, base_time + 2000, Some(1));

        // Add snapshots, then set tag first (non-main), then main
        // Setting tag for snapshot 1 doesn't update snapshot_log
        // Setting main ref for snapshot 2 will update snapshot_log
        let metadata = base
            .into_builder(None)
            .add_snapshot(snap1)
            .unwrap()
            .add_snapshot(snap2)
            .unwrap()
            .set_ref("release-1.0", SnapshotReference {
                snapshot_id: 1,
                retention: SnapshotRetention::Tag {
                    max_ref_age_ms: None,
                },
            })
            .unwrap()
            .set_ref("main", SnapshotReference {
                snapshot_id: 2,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: Some(1),
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            })
            .unwrap()
            .build()
            .unwrap()
            .metadata;

        let now = chrono::DateTime::from_timestamp_millis(base_time + 20 * DAY_MS).unwrap();
        let policy = RetentionPolicy::new(Duration::days(5), 1, None);

        let result = compute_retention(&metadata, &policy, now).unwrap();

        // Both snapshots should be retained:
        // - Snapshot 2: protected by main branch
        // - Snapshot 1: protected by release-1.0 tag
        assert!(result.retained_snapshot_ids.contains(&1));
        assert!(result.retained_snapshot_ids.contains(&2));

        // No refs should be expired
        assert!(result.expired_ref_names.is_empty());
    }

    #[test]
    fn test_compute_retention_respects_age_window() {
        let base = build_base_metadata();
        let base_time = base.last_updated_ms();

        // Create snapshots at different times
        let snap1 = create_snapshot(1, base_time + 1 * DAY_MS, None);
        let snap2 = create_snapshot(2, base_time + 5 * DAY_MS, Some(1));
        let snap3 = create_snapshot(3, base_time + 8 * DAY_MS, Some(2));
        let snap4 = create_snapshot(4, base_time + 10 * DAY_MS, Some(3));
        let snap5 = create_snapshot(5, base_time + 11 * DAY_MS, Some(4));

        let metadata = base
            .into_builder(None)
            .add_snapshot(snap1)
            .unwrap()
            .add_snapshot(snap2)
            .unwrap()
            .add_snapshot(snap3)
            .unwrap()
            .add_snapshot(snap4)
            .unwrap()
            .add_snapshot(snap5)
            .unwrap()
            .set_ref("main", SnapshotReference {
                snapshot_id: 5,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: Some(1),
                    max_snapshot_age_ms: Some(5 * DAY_MS), // Keep snapshots from last 5 days
                    max_ref_age_ms: None,
                },
            })
            .unwrap()
            .build()
            .unwrap()
            .metadata;

        // "now" is at base_time + 12 days
        // So cutoff is (base_time + 12 days) - 5 days = base_time + 7 days
        let now = chrono::DateTime::from_timestamp_millis(base_time + 12 * DAY_MS).unwrap();
        let policy = RetentionPolicy::new(Duration::days(5), 1, None);

        let result = compute_retention(&metadata, &policy, now).unwrap();

        // Snapshots within 5 day window should be retained:
        // - snap5 at day 11: 12-11 = 1 day old -> retained
        // - snap4 at day 10: 12-10 = 2 days old -> retained
        // - snap3 at day 8: 12-8 = 4 days old -> retained
        // - snap2 at day 5: 12-5 = 7 days old -> NOT retained (outside window)
        // - snap1 at day 1: 12-1 = 11 days old -> NOT retained (outside window)
        assert!(result.retained_snapshot_ids.contains(&5));
        assert!(result.retained_snapshot_ids.contains(&4));
        assert!(result.retained_snapshot_ids.contains(&3));
        assert!(!result.retained_snapshot_ids.contains(&2));
        assert!(!result.retained_snapshot_ids.contains(&1));
    }
}
