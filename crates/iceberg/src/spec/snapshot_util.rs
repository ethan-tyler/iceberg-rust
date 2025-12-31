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

//! Utilities for working with snapshot history and ancestry.
//!
//! This module provides functions for navigating snapshot lineage,
//! validating ancestry relationships, and finding snapshots by timestamp.
//!
//! # Example
//!
//! ```rust,ignore
//! use iceberg::spec::SnapshotUtil;
//!
//! // Check if one snapshot is an ancestor of another
//! let is_ancestor = SnapshotUtil::is_ancestor_of(metadata, current_id, target_id);
//!
//! // Get all ancestors of a snapshot
//! let ancestors = SnapshotUtil::ancestor_ids(metadata, snapshot_id);
//!
//! // Find snapshot at a given timestamp
//! let snapshot = SnapshotUtil::snapshot_before_timestamp(metadata, timestamp_ms);
//! ```

use std::collections::HashSet;
use std::sync::Arc;

use super::{Snapshot, TableMetadata};

/// Utilities for working with snapshot history and ancestry.
///
/// `SnapshotUtil` provides static methods for navigating and querying
/// the snapshot lineage of an Iceberg table. These utilities are essential
/// for operations like rollback validation and time travel queries.
pub struct SnapshotUtil;

impl SnapshotUtil {
    /// Returns all ancestor snapshot IDs from the given snapshot back to the root.
    ///
    /// The returned Vec includes the starting snapshot_id and is ordered
    /// from newest to oldest (i.e., the starting snapshot is first, root is last).
    ///
    /// # Arguments
    ///
    /// * `metadata` - The table metadata containing snapshot information
    /// * `snapshot_id` - The snapshot ID to start from
    ///
    /// # Returns
    ///
    /// A vector of snapshot IDs representing the ancestry chain, starting with
    /// `snapshot_id` and ending with the root snapshot (the one with no parent).
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // For a chain: s1 -> s2 -> s3 (current)
    /// let ancestors = SnapshotUtil::ancestor_ids(metadata, 3);
    /// assert_eq!(ancestors, vec![3, 2, 1]);
    /// ```
    pub fn ancestor_ids(metadata: &TableMetadata, snapshot_id: i64) -> Vec<i64> {
        let mut ancestors = Vec::new();
        let mut current_id = Some(snapshot_id);

        while let Some(id) = current_id {
            ancestors.push(id);
            current_id = metadata
                .snapshot_by_id(id)
                .and_then(|s| s.parent_snapshot_id());
        }

        ancestors
    }

    /// Returns ancestor IDs between two snapshots.
    ///
    /// Returns all snapshot IDs on the path from `newest_snapshot_id` to
    /// `oldest_snapshot_id`, exclusive of `oldest_snapshot_id` but inclusive
    /// of `newest_snapshot_id`.
    ///
    /// # Arguments
    ///
    /// * `metadata` - The table metadata containing snapshot information
    /// * `oldest_snapshot_id` - The older snapshot (exclusive endpoint)
    /// * `newest_snapshot_id` - The newer snapshot (inclusive starting point)
    ///
    /// # Returns
    ///
    /// A vector of snapshot IDs between the two snapshots. Returns an empty
    /// vector if `oldest_snapshot_id` is not an ancestor of `newest_snapshot_id`.
    pub fn ancestor_ids_between(
        metadata: &TableMetadata,
        oldest_snapshot_id: i64,
        newest_snapshot_id: i64,
    ) -> Vec<i64> {
        let mut ancestors = Vec::new();
        let mut current_id = Some(newest_snapshot_id);

        while let Some(id) = current_id {
            if id == oldest_snapshot_id {
                return ancestors;
            }
            ancestors.push(id);
            current_id = metadata
                .snapshot_by_id(id)
                .and_then(|s| s.parent_snapshot_id());
        }

        // `oldest_snapshot_id` is not an ancestor of `newest_snapshot_id`.
        Vec::new()
    }

    /// Check if `potential_ancestor_id` is an ancestor of `snapshot_id`.
    ///
    /// This method walks the parent chain from `snapshot_id` to determine
    /// if `potential_ancestor_id` exists in its ancestry. A snapshot is
    /// considered an ancestor of itself.
    ///
    /// # Arguments
    ///
    /// * `metadata` - The table metadata containing snapshot information
    /// * `snapshot_id` - The snapshot to check ancestry for
    /// * `potential_ancestor_id` - The ID that might be an ancestor
    ///
    /// # Returns
    ///
    /// `true` if `potential_ancestor_id` is in the ancestry chain of `snapshot_id`,
    /// `false` otherwise.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // For a chain: s1 -> s2 -> s3 (current)
    /// assert!(SnapshotUtil::is_ancestor_of(metadata, 3, 1)); // s1 is ancestor of s3
    /// assert!(SnapshotUtil::is_ancestor_of(metadata, 3, 3)); // s3 is ancestor of itself
    /// assert!(!SnapshotUtil::is_ancestor_of(metadata, 1, 3)); // s3 is NOT ancestor of s1
    /// ```
    pub fn is_ancestor_of(
        metadata: &TableMetadata,
        snapshot_id: i64,
        potential_ancestor_id: i64,
    ) -> bool {
        Self::ancestor_ids(metadata, snapshot_id).contains(&potential_ancestor_id)
    }

    /// Get the oldest ancestor (root) of a snapshot.
    ///
    /// Returns the snapshot at the root of the ancestry chain, i.e., the
    /// snapshot with no parent.
    ///
    /// # Arguments
    ///
    /// * `metadata` - The table metadata containing snapshot information
    /// * `snapshot_id` - The snapshot to find the root ancestor for
    ///
    /// # Returns
    ///
    /// A reference to the root snapshot, or `None` if the starting snapshot
    /// does not exist in the metadata.
    pub fn oldest_ancestor(metadata: &TableMetadata, snapshot_id: i64) -> Option<&Arc<Snapshot>> {
        Self::ancestor_ids(metadata, snapshot_id)
            .last()
            .and_then(|id| metadata.snapshot_by_id(*id))
    }

    /// Find the common ancestor of two snapshots.
    ///
    /// Returns the most recent snapshot that is an ancestor of both snapshots.
    /// This is useful for operations like branch merging.
    ///
    /// # Arguments
    ///
    /// * `metadata` - The table metadata containing snapshot information
    /// * `snapshot_id_a` - First snapshot ID
    /// * `snapshot_id_b` - Second snapshot ID
    ///
    /// # Returns
    ///
    /// The snapshot ID of the common ancestor, or `None` if no common
    /// ancestor exists (which would indicate the snapshots are on
    /// completely separate lineages).
    pub fn common_ancestor(
        metadata: &TableMetadata,
        snapshot_id_a: i64,
        snapshot_id_b: i64,
    ) -> Option<i64> {
        let ancestors_a: HashSet<_> = Self::ancestor_ids(metadata, snapshot_id_a)
            .into_iter()
            .collect();

        Self::ancestor_ids(metadata, snapshot_id_b)
            .into_iter()
            .find(|ancestor_b| ancestors_a.contains(ancestor_b))
    }

    /// Find the snapshot at or before a given timestamp.
    ///
    /// Returns the most recent snapshot whose timestamp is less than or equal
    /// to the provided timestamp.
    ///
    /// # Arguments
    ///
    /// * `metadata` - The table metadata containing snapshot information
    /// * `timestamp_ms` - The timestamp in milliseconds since Unix epoch
    ///
    /// # Returns
    ///
    /// A reference to the matching snapshot, or `None` if no snapshot exists
    /// at or before the given timestamp.
    pub fn snapshot_at_timestamp(
        metadata: &TableMetadata,
        timestamp_ms: i64,
    ) -> Option<&Arc<Snapshot>> {
        metadata
            .snapshots()
            .filter(|s| s.timestamp_ms() <= timestamp_ms)
            .max_by_key(|s| s.timestamp_ms())
    }

    /// Find the snapshot strictly before a given timestamp.
    ///
    /// Returns the most recent snapshot whose timestamp is strictly less than
    /// the provided timestamp. This is useful for rollback operations where
    /// you want to go to the state before a certain point in time.
    ///
    /// # Arguments
    ///
    /// * `metadata` - The table metadata containing snapshot information
    /// * `timestamp_ms` - The timestamp in milliseconds since Unix epoch
    ///
    /// # Returns
    ///
    /// A reference to the matching snapshot, or `None` if no snapshot exists
    /// before the given timestamp.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Find the last snapshot before noon on Jan 1, 2024
    /// let timestamp = DateTime::parse_from_rfc3339("2024-01-01T12:00:00Z")
    ///     .unwrap()
    ///     .timestamp_millis();
    /// let snapshot = SnapshotUtil::snapshot_before_timestamp(metadata, timestamp);
    /// ```
    pub fn snapshot_before_timestamp(
        metadata: &TableMetadata,
        timestamp_ms: i64,
    ) -> Option<&Arc<Snapshot>> {
        metadata
            .snapshots()
            .filter(|s| s.timestamp_ms() < timestamp_ms)
            .max_by_key(|s| s.timestamp_ms())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use crate::spec::{
        FormatVersion, NestedField, Operation, PrimitiveType, Schema, Snapshot, Summary, Type,
    };

    /// Create test table metadata with a chain of snapshots.
    fn create_test_metadata_with_chain() -> TableMetadata {
        // Create snapshots: s1 -> s2 -> s3
        let s1 = Snapshot::builder()
            .with_snapshot_id(1)
            .with_sequence_number(1)
            .with_timestamp_ms(1000)
            .with_manifest_list("s3://bucket/metadata/snap-1.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build();

        let s2 = Snapshot::builder()
            .with_snapshot_id(2)
            .with_parent_snapshot_id(Some(1))
            .with_sequence_number(2)
            .with_timestamp_ms(2000)
            .with_manifest_list("s3://bucket/metadata/snap-2.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build();

        let s3 = Snapshot::builder()
            .with_snapshot_id(3)
            .with_parent_snapshot_id(Some(2))
            .with_sequence_number(3)
            .with_timestamp_ms(3000)
            .with_manifest_list("s3://bucket/metadata/snap-3.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build();

        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap();

        TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: uuid::Uuid::new_v4(),
            location: "s3://bucket/table".to_string(),
            last_sequence_number: 3,
            last_updated_ms: 3000,
            last_column_id: 1,
            current_schema_id: 0,
            schemas: HashMap::from([(0, Arc::new(schema))]),
            partition_specs: HashMap::new(),
            default_spec: Arc::new(crate::spec::PartitionSpec::unpartition_spec()),
            default_partition_type: crate::spec::StructType::new(vec![]),
            last_partition_id: -1,
            properties: HashMap::new(),
            current_snapshot_id: Some(3),
            snapshots: HashMap::from([(1, Arc::new(s1)), (2, Arc::new(s2)), (3, Arc::new(s3))]),
            snapshot_log: vec![],
            sort_orders: HashMap::new(),
            metadata_log: vec![],
            default_sort_order_id: 0,
            refs: HashMap::new(),
            statistics: HashMap::new(),
            partition_statistics: HashMap::new(),
            encryption_keys: HashMap::new(),
            next_row_id: 0,
        }
    }

    /// Create test metadata with a branch: s1 -> s2 -> s3 and s1 -> s4
    fn create_test_metadata_with_branch() -> TableMetadata {
        let mut metadata = create_test_metadata_with_chain();

        // Add s4 as a branch from s1
        let s4 = Snapshot::builder()
            .with_snapshot_id(4)
            .with_parent_snapshot_id(Some(1))
            .with_sequence_number(4)
            .with_timestamp_ms(2500)
            .with_manifest_list("s3://bucket/metadata/snap-4.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build();

        metadata.snapshots.insert(4, Arc::new(s4));
        metadata
    }

    #[test]
    fn test_ancestor_ids() {
        let metadata = create_test_metadata_with_chain();

        let ancestors = SnapshotUtil::ancestor_ids(&metadata, 3);
        assert_eq!(ancestors, vec![3, 2, 1]);

        let ancestors = SnapshotUtil::ancestor_ids(&metadata, 2);
        assert_eq!(ancestors, vec![2, 1]);

        let ancestors = SnapshotUtil::ancestor_ids(&metadata, 1);
        assert_eq!(ancestors, vec![1]);
    }

    #[test]
    fn test_ancestor_ids_nonexistent() {
        let metadata = create_test_metadata_with_chain();

        let ancestors = SnapshotUtil::ancestor_ids(&metadata, 999);
        assert_eq!(ancestors, vec![999]); // Returns the ID itself even if not found
    }

    #[test]
    fn test_ancestor_ids_between() {
        let metadata = create_test_metadata_with_chain();

        // Between s1 and s3 (exclusive of s1, inclusive of s3)
        let between = SnapshotUtil::ancestor_ids_between(&metadata, 1, 3);
        assert_eq!(between, vec![3, 2]);

        // Between s2 and s3
        let between = SnapshotUtil::ancestor_ids_between(&metadata, 2, 3);
        assert_eq!(between, vec![3]);

        // Same snapshot
        let between = SnapshotUtil::ancestor_ids_between(&metadata, 3, 3);
        assert!(between.is_empty());

        // Not an ancestor
        let between = SnapshotUtil::ancestor_ids_between(&metadata, 999, 3);
        assert!(between.is_empty());
    }

    #[test]
    fn test_is_ancestor_of() {
        let metadata = create_test_metadata_with_chain();

        // s1 is ancestor of s3
        assert!(SnapshotUtil::is_ancestor_of(&metadata, 3, 1));
        // s2 is ancestor of s3
        assert!(SnapshotUtil::is_ancestor_of(&metadata, 3, 2));
        // s3 is ancestor of itself
        assert!(SnapshotUtil::is_ancestor_of(&metadata, 3, 3));
        // s3 is NOT ancestor of s1
        assert!(!SnapshotUtil::is_ancestor_of(&metadata, 1, 3));
        // s2 is NOT ancestor of s1
        assert!(!SnapshotUtil::is_ancestor_of(&metadata, 1, 2));
    }

    #[test]
    fn test_is_ancestor_of_with_branch() {
        let metadata = create_test_metadata_with_branch();

        // s1 is ancestor of both s3 and s4
        assert!(SnapshotUtil::is_ancestor_of(&metadata, 3, 1));
        assert!(SnapshotUtil::is_ancestor_of(&metadata, 4, 1));

        // s4 is NOT ancestor of s3 (different branch)
        assert!(!SnapshotUtil::is_ancestor_of(&metadata, 3, 4));
        // s3 is NOT ancestor of s4
        assert!(!SnapshotUtil::is_ancestor_of(&metadata, 4, 3));
    }

    #[test]
    fn test_oldest_ancestor() {
        let metadata = create_test_metadata_with_chain();

        let oldest = SnapshotUtil::oldest_ancestor(&metadata, 3);
        assert!(oldest.is_some());
        assert_eq!(oldest.unwrap().snapshot_id(), 1);

        let oldest = SnapshotUtil::oldest_ancestor(&metadata, 1);
        assert!(oldest.is_some());
        assert_eq!(oldest.unwrap().snapshot_id(), 1);
    }

    #[test]
    fn test_common_ancestor() {
        let metadata = create_test_metadata_with_branch();

        // Common ancestor of s3 and s4 is s1
        let common = SnapshotUtil::common_ancestor(&metadata, 3, 4);
        assert_eq!(common, Some(1));

        // Common ancestor of s3 and s2 is s2
        let common = SnapshotUtil::common_ancestor(&metadata, 3, 2);
        assert_eq!(common, Some(2));

        // Common ancestor of a snapshot with itself
        let common = SnapshotUtil::common_ancestor(&metadata, 3, 3);
        assert_eq!(common, Some(3));
    }

    #[test]
    fn test_snapshot_at_timestamp() {
        let metadata = create_test_metadata_with_chain();

        // Exact timestamp match
        let snapshot = SnapshotUtil::snapshot_at_timestamp(&metadata, 2000);
        assert!(snapshot.is_some());
        assert_eq!(snapshot.unwrap().snapshot_id(), 2);

        // Timestamp between s2 and s3
        let snapshot = SnapshotUtil::snapshot_at_timestamp(&metadata, 2500);
        assert!(snapshot.is_some());
        assert_eq!(snapshot.unwrap().snapshot_id(), 2);

        // Timestamp after all snapshots
        let snapshot = SnapshotUtil::snapshot_at_timestamp(&metadata, 5000);
        assert!(snapshot.is_some());
        assert_eq!(snapshot.unwrap().snapshot_id(), 3);

        // Timestamp before all snapshots
        let snapshot = SnapshotUtil::snapshot_at_timestamp(&metadata, 500);
        assert!(snapshot.is_none());
    }

    #[test]
    fn test_snapshot_before_timestamp() {
        let metadata = create_test_metadata_with_chain();

        // Timestamp at s2 should return s1 (strictly before)
        let snapshot = SnapshotUtil::snapshot_before_timestamp(&metadata, 2000);
        assert!(snapshot.is_some());
        assert_eq!(snapshot.unwrap().snapshot_id(), 1);

        // Timestamp between s2 and s3
        let snapshot = SnapshotUtil::snapshot_before_timestamp(&metadata, 2500);
        assert!(snapshot.is_some());
        assert_eq!(snapshot.unwrap().snapshot_id(), 2);

        // Timestamp before all snapshots
        let snapshot = SnapshotUtil::snapshot_before_timestamp(&metadata, 1000);
        assert!(snapshot.is_none());

        // Timestamp just after s1
        let snapshot = SnapshotUtil::snapshot_before_timestamp(&metadata, 1001);
        assert!(snapshot.is_some());
        assert_eq!(snapshot.unwrap().snapshot_id(), 1);
    }
}
