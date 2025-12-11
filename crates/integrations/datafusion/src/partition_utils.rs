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

//! Partition evolution utilities for Iceberg DataFusion integration.
//!
//! This module provides utilities for handling partition evolution when
//! serializing and deserializing data files. When a table has evolved
//! partition specs, files may have different partition types that need
//! to be handled correctly during serialization/deserialization.
//!
//! # Background
//!
//! Iceberg tables support partition evolution - the ability to change
//! partitioning scheme over time. When this happens:
//! - Old data files retain their original partition spec
//! - New data files use the current partition spec
//! - Each partition spec has a unique spec_id
//!
//! When serializing files (e.g., for passing between execution plan nodes),
//! we must use the correct partition type for each file's spec_id.
//!
//! # Usage
//!
//! ```ignore
//! use crate::partition_utils::{SerializedFileWithSpec, build_partition_type_map};
//!
//! // Build partition type map once at start of operation
//! let partition_types = build_partition_type_map(&table)?;
//!
//! // Serialize a file with its spec_id
//! let partition_type = partition_types.get(&spec_id)?;
//! let file_json = serialize_data_file_to_json(data_file, partition_type, format_version)?;
//! let serialized = serde_json::to_string(&SerializedFileWithSpec {
//!     spec_id,
//!     file_json,
//! })?;
//!
//! // Deserialize using the preserved spec_id
//! let wrapper: SerializedFileWithSpec = serde_json::from_str(&serialized)?;
//! let partition_type = partition_types.get(&wrapper.spec_id)?;
//! let data_file = deserialize_data_file_from_json(&wrapper.file_json, ...)?;
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::{DataFusionError, Result as DFResult};
use futures::TryStreamExt;
use iceberg::spec::{PartitionSpec, Struct, StructType, TableMetadata};
use iceberg::table::Table;
use serde::{Deserialize, Serialize};

use crate::to_datafusion_error;

/// Wrapper for serialized data files that preserves partition spec ID.
///
/// When passing serialized data files between execution plan nodes (e.g., from
/// a write node to a commit node), we need to preserve the partition spec ID
/// alongside the serialized file JSON. This allows the deserializing side to
/// look up the correct partition type for deserialization.
///
/// # Fields
///
/// * `spec_id` - The partition spec ID under which the file was written
/// * `file_json` - The JSON-serialized DataFile (from `serialize_data_file_to_json`)
///
/// # Serialization Format
///
/// ```json
/// {
///   "spec_id": 0,
///   "file_json": "{\"content\":\"DATA\",\"file_path\":\"...\", ...}"
/// }
/// ```
#[derive(Serialize, Deserialize)]
pub(crate) struct SerializedFileWithSpec {
    /// The partition spec ID under which the file was written
    pub(crate) spec_id: i32,
    /// The JSON-serialized DataFile
    pub(crate) file_json: String,
}

/// Build a map from partition spec ID to partition struct type.
///
/// This function handles partition evolution by building a map of all partition
/// specs in the table metadata to their corresponding partition types. The
/// partition type is needed for serializing/deserializing the partition struct
/// within data files.
///
/// # Algorithm
///
/// For each partition spec in the table:
/// 1. Try to derive the partition type using the current schema
/// 2. If that fails (e.g., schema evolution removed source columns), fall back
///    to trying historical schemas
/// 3. If no schema can resolve the partition type, return an error
///
/// # Arguments
///
/// * `table` - The Iceberg table to build the partition type map for
///
/// # Returns
///
/// A HashMap mapping spec_id to StructType for all partition specs in the table.
///
/// # Errors
///
/// Returns an error if any partition spec cannot be resolved to a partition type.
/// This typically indicates data corruption or an invalid table state.
pub(crate) fn build_partition_type_map(table: &Table) -> DFResult<HashMap<i32, StructType>> {
    let metadata = table.metadata();
    let mut partition_types = HashMap::new();

    for spec_ref in metadata.partition_specs_iter() {
        let spec = spec_ref.as_ref();
        let partition_type = partition_type_for_spec(spec, metadata).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "Unable to derive partition type for partition spec {}",
                spec.spec_id()
            ))
        })?;

        partition_types.insert(spec.spec_id(), partition_type);
    }

    Ok(partition_types)
}

/// Derive the partition type for a specific partition spec.
///
/// Tries the current schema first, then falls back to historical schemas
/// if the current schema cannot resolve all partition field source columns.
///
/// # Arguments
///
/// * `spec` - The partition spec to derive the type for
/// * `metadata` - The table metadata containing schema history
///
/// # Returns
///
/// The partition StructType if it can be derived, or None if no schema can
/// resolve the partition spec's source columns.
fn partition_type_for_spec(spec: &PartitionSpec, metadata: &TableMetadata) -> Option<StructType> {
    // Try current schema first (most common case)
    spec.partition_type(metadata.current_schema())
        .ok()
        .or_else(|| {
            // Fall back to historical schemas for evolved partition specs
            metadata
                .schemas_iter()
                .find_map(|schema| spec.partition_type(schema).ok())
        })
}

/// Information about a data file's partition, used to route deletes to the correct writer.
///
/// This struct captures the partition information from a file's manifest entry,
/// which is essential for partition-evolution-aware operations. When a table has
/// evolved its partition spec, files may have different partition specs, and we
/// need to preserve the original spec_id and partition values when writing
/// position delete files.
#[derive(Clone, Debug)]
pub(crate) struct FilePartitionInfo {
    /// The partition spec ID under which the file was written
    pub(crate) spec_id: i32,
    /// The partition values (struct of partition field values)
    pub(crate) partition: Struct,
    /// The partition spec (needed to build PartitionKey for writer)
    pub(crate) partition_spec: Arc<PartitionSpec>,
}

/// Builds a mapping from file path to partition info by scanning all manifests.
///
/// This function scans the table's current snapshot to get all data files and their
/// associated partition information. This mapping is used during delete/update/merge
/// operations to route position deletes to the correct partition-specific delete file.
///
/// # Partition Evolution Support
///
/// Each file's partition spec is obtained from the `FileScanTask.partition_spec` field,
/// which carries the spec under which the file was originally written. This enables
/// correct handling of tables with partition evolution - position delete files are
/// written with the same spec_id as the data files they reference.
///
/// # Arguments
///
/// * `table` - The Iceberg table to scan
///
/// # Returns
///
/// A HashMap mapping file path to FilePartitionInfo for all data files in the
/// current snapshot.
pub(crate) async fn build_file_partition_map(
    table: &Table,
) -> DFResult<HashMap<String, FilePartitionInfo>> {
    use iceberg::scan::FileScanTask;

    let mut file_partitions = HashMap::new();

    // Build a scan to get all files in the current snapshot
    let scan = table
        .scan()
        .select_all()
        .build()
        .map_err(to_datafusion_error)?;

    // Get all file scan tasks
    let file_tasks: Vec<FileScanTask> = scan
        .plan_files()
        .await
        .map_err(to_datafusion_error)?
        .try_collect()
        .await
        .map_err(to_datafusion_error)?;

    // Build mapping from file path to partition info
    for task in file_tasks {
        if let (Some(partition), Some(partition_spec)) = (task.partition, task.partition_spec) {
            let info = FilePartitionInfo {
                spec_id: partition_spec.spec_id(),
                partition,
                partition_spec,
            };

            file_partitions.insert(task.data_file_path, info);
        }
    }

    Ok(file_partitions)
}

/// Checks if a table has partition evolution (multiple partition specs).
///
/// DML operations (UPDATE, DELETE, MERGE) do not yet fully support tables
/// with partition evolution. This function detects such tables so that
/// operations can return an appropriate error rather than producing
/// incorrect results.
///
/// # Arguments
///
/// * `table` - The Iceberg table to check
///
/// # Returns
///
/// `true` if the table has more than one partition spec (partition evolution),
/// `false` otherwise.
pub(crate) fn has_partition_evolution(table: &Table) -> bool {
    table.metadata().partition_specs_iter().len() > 1
}

/// Returns an error if the table has partition evolution.
///
/// This guard function should be called at the start of DML operations
/// (UPDATE, DELETE, MERGE) to reject tables with partition evolution
/// until full support is implemented.
///
/// # Arguments
///
/// * `table` - The Iceberg table to check
/// * `operation` - Name of the operation (for error message)
///
/// # Returns
///
/// `Ok(())` if the table has a single partition spec,
/// `Err(NotImplemented)` if the table has partition evolution.
pub(crate) fn reject_partition_evolution(table: &Table, operation: &str) -> DFResult<()> {
    if has_partition_evolution(table) {
        return Err(DataFusionError::NotImplemented(format!(
            "{} on tables with partition evolution is not yet supported. \
             This table has {} partition specs.",
            operation,
            table.metadata().partition_specs_iter().len()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialized_file_with_spec_roundtrip() {
        let original = SerializedFileWithSpec {
            spec_id: 42,
            file_json: r#"{"content":"DATA","file_path":"test.parquet"}"#.to_string(),
        };

        let serialized = serde_json::to_string(&original).unwrap();
        let deserialized: SerializedFileWithSpec = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.spec_id, 42);
        assert!(deserialized.file_json.contains("test.parquet"));
    }
}
