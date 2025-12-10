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

use std::collections::HashMap;

use crate::{Error, ErrorKind};

// Helper function to parse a property from a HashMap
// If the property is not found, use the default value
fn parse_property<T: std::str::FromStr>(
    properties: &HashMap<String, String>,
    key: &str,
    default: T,
) -> std::result::Result<T, anyhow::Error>
where
    <T as std::str::FromStr>::Err: std::fmt::Display,
{
    properties.get(key).map_or(Ok(default), |value| {
        value
            .parse::<T>()
            .map_err(|e| anyhow::anyhow!("Invalid value for {key}: {e}"))
    })
}

/// TableProperties that contains the properties of a table.
#[derive(Debug)]
pub struct TableProperties {
    /// The number of times to retry a commit.
    pub commit_num_retries: usize,
    /// The minimum wait time between retries.
    pub commit_min_retry_wait_ms: u64,
    /// The maximum wait time between retries.
    pub commit_max_retry_wait_ms: u64,
    /// The total timeout for commit retries.
    pub commit_total_retry_timeout_ms: u64,
    /// The default format for files.
    pub write_format_default: String,
    /// The target file size for files.
    pub write_target_file_size_bytes: usize,
}

impl TableProperties {
    /// Reserved table property for table format version.
    ///
    /// Iceberg will default a new table's format version to the latest stable and recommended
    /// version. This reserved property keyword allows users to override the Iceberg format version of
    /// the table metadata.
    ///
    /// If this table property exists when creating a table, the table will use the specified format
    /// version. If a table updates this property, it will try to upgrade to the specified format
    /// version.
    pub const PROPERTY_FORMAT_VERSION: &str = "format-version";
    /// Reserved table property for table UUID.
    pub const PROPERTY_UUID: &str = "uuid";
    /// Reserved table property for the total number of snapshots.
    pub const PROPERTY_SNAPSHOT_COUNT: &str = "snapshot-count";
    /// Reserved table property for current snapshot summary.
    pub const PROPERTY_CURRENT_SNAPSHOT_SUMMARY: &str = "current-snapshot-summary";
    /// Reserved table property for current snapshot id.
    pub const PROPERTY_CURRENT_SNAPSHOT_ID: &str = "current-snapshot-id";
    /// Reserved table property for current snapshot timestamp.
    pub const PROPERTY_CURRENT_SNAPSHOT_TIMESTAMP: &str = "current-snapshot-timestamp-ms";
    /// Reserved table property for the JSON representation of current schema.
    pub const PROPERTY_CURRENT_SCHEMA: &str = "current-schema";
    /// Reserved table property for the JSON representation of current(default) partition spec.
    pub const PROPERTY_DEFAULT_PARTITION_SPEC: &str = "default-partition-spec";
    /// Reserved table property for the JSON representation of current(default) sort order.
    pub const PROPERTY_DEFAULT_SORT_ORDER: &str = "default-sort-order";

    /// Property key for max number of previous versions to keep.
    pub const PROPERTY_METADATA_PREVIOUS_VERSIONS_MAX: &str =
        "write.metadata.previous-versions-max";
    /// Default value for max number of previous versions to keep.
    pub const PROPERTY_METADATA_PREVIOUS_VERSIONS_MAX_DEFAULT: usize = 100;

    /// Property key for max number of partitions to keep summary stats for.
    pub const PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT: &str = "write.summary.partition-limit";
    /// Default value for the max number of partitions to keep summary stats for.
    pub const PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT: u64 = 0;

    /// Reserved Iceberg table properties list.
    ///
    /// Reserved table properties are only used to control behaviors when creating or updating a
    /// table. The value of these properties are not persisted as a part of the table metadata.
    pub const RESERVED_PROPERTIES: [&str; 9] = [
        Self::PROPERTY_FORMAT_VERSION,
        Self::PROPERTY_UUID,
        Self::PROPERTY_SNAPSHOT_COUNT,
        Self::PROPERTY_CURRENT_SNAPSHOT_ID,
        Self::PROPERTY_CURRENT_SNAPSHOT_SUMMARY,
        Self::PROPERTY_CURRENT_SNAPSHOT_TIMESTAMP,
        Self::PROPERTY_CURRENT_SCHEMA,
        Self::PROPERTY_DEFAULT_PARTITION_SPEC,
        Self::PROPERTY_DEFAULT_SORT_ORDER,
    ];

    /// Property key for number of commit retries.
    pub const PROPERTY_COMMIT_NUM_RETRIES: &str = "commit.retry.num-retries";
    /// Default value for number of commit retries.
    pub const PROPERTY_COMMIT_NUM_RETRIES_DEFAULT: usize = 4;

    /// Property key for minimum wait time (ms) between retries.
    pub const PROPERTY_COMMIT_MIN_RETRY_WAIT_MS: &str = "commit.retry.min-wait-ms";
    /// Default value for minimum wait time (ms) between retries.
    pub const PROPERTY_COMMIT_MIN_RETRY_WAIT_MS_DEFAULT: u64 = 100;

    /// Property key for maximum wait time (ms) between retries.
    pub const PROPERTY_COMMIT_MAX_RETRY_WAIT_MS: &str = "commit.retry.max-wait-ms";
    /// Default value for maximum wait time (ms) between retries.
    pub const PROPERTY_COMMIT_MAX_RETRY_WAIT_MS_DEFAULT: u64 = 60 * 1000; // 1 minute

    /// Property key for total maximum retry time (ms).
    pub const PROPERTY_COMMIT_TOTAL_RETRY_TIME_MS: &str = "commit.retry.total-timeout-ms";
    /// Default value for total maximum retry time (ms).
    pub const PROPERTY_COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT: u64 = 30 * 60 * 1000; // 30 minutes

    /// Default file format for data files
    pub const PROPERTY_DEFAULT_FILE_FORMAT: &str = "write.format.default";
    /// Default file format for delete files
    pub const PROPERTY_DELETE_DEFAULT_FILE_FORMAT: &str = "write.delete.format.default";
    /// Default value for data file format
    pub const PROPERTY_DEFAULT_FILE_FORMAT_DEFAULT: &str = "parquet";

    /// Target file size for newly written files.
    pub const PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES: &str = "write.target-file-size-bytes";
    /// Default target file size
    pub const PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT: usize = 512 * 1024 * 1024; // 512 MB

    // =========================================================================
    // Read Split Properties
    // =========================================================================

    /// Target size when combining data for scanning.
    pub const PROPERTY_READ_SPLIT_TARGET_SIZE: &str = "read.split.target-size";
    /// Default value for read.split.target-size (128 MB).
    pub const PROPERTY_READ_SPLIT_TARGET_SIZE_DEFAULT: i64 = 134217728;

    /// Target size for metadata tasks.
    pub const PROPERTY_READ_SPLIT_METADATA_TARGET_SIZE: &str = "read.split.metadata-target-size";
    /// Default value for read.split.metadata-target-size (32 MB).
    pub const PROPERTY_READ_SPLIT_METADATA_TARGET_SIZE_DEFAULT: i64 = 33554432;

    /// Files to consider for planning lookback.
    pub const PROPERTY_READ_SPLIT_PLANNING_LOOKBACK: &str = "read.split.planning-lookback";
    /// Default value for read.split.planning-lookback.
    pub const PROPERTY_READ_SPLIT_PLANNING_LOOKBACK_DEFAULT: i32 = 10;

    /// Cost of opening a file for splitting.
    pub const PROPERTY_READ_SPLIT_OPEN_FILE_COST: &str = "read.split.open-file-cost";
    /// Default value for read.split.open-file-cost (4 MB).
    pub const PROPERTY_READ_SPLIT_OPEN_FILE_COST_DEFAULT: i64 = 4194304;

    // =========================================================================
    // Write Mode Properties
    // =========================================================================

    /// Delete operation mode (copy-on-write or merge-on-read).
    pub const PROPERTY_WRITE_DELETE_MODE: &str = "write.delete.mode";
    /// Default value for write.delete.mode.
    pub const PROPERTY_WRITE_DELETE_MODE_DEFAULT: &str = "copy-on-write";

    /// Update operation mode (copy-on-write or merge-on-read).
    pub const PROPERTY_WRITE_UPDATE_MODE: &str = "write.update.mode";
    /// Default value for write.update.mode.
    pub const PROPERTY_WRITE_UPDATE_MODE_DEFAULT: &str = "copy-on-write";

    /// Merge operation mode (copy-on-write or merge-on-read).
    pub const PROPERTY_WRITE_MERGE_MODE: &str = "write.merge.mode";
    /// Default value for write.merge.mode.
    pub const PROPERTY_WRITE_MERGE_MODE_DEFAULT: &str = "copy-on-write";

    /// Write distribution mode (none, hash, range).
    pub const PROPERTY_WRITE_DISTRIBUTION_MODE: &str = "write.distribution-mode";
    /// Default value for write.distribution-mode.
    pub const PROPERTY_WRITE_DISTRIBUTION_MODE_DEFAULT: &str = "none";

    // =========================================================================
    // Metadata Cleanup Properties
    // =========================================================================

    /// Enable automatic metadata file cleanup after commit.
    pub const PROPERTY_WRITE_METADATA_DELETE_AFTER_COMMIT_ENABLED: &str =
        "write.metadata.delete-after-commit.enabled";
    /// Default value for write.metadata.delete-after-commit.enabled.
    pub const PROPERTY_WRITE_METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT: bool = false;

    // =========================================================================
    // History/Snapshot Expiration Properties
    // =========================================================================

    /// Minimum snapshots to retain during expiration.
    pub const PROPERTY_HISTORY_EXPIRE_MIN_SNAPSHOTS_TO_KEEP: &str =
        "history.expire.min-snapshots-to-keep";
    /// Default value for history.expire.min-snapshots-to-keep.
    pub const PROPERTY_HISTORY_EXPIRE_MIN_SNAPSHOTS_TO_KEEP_DEFAULT: i32 = 1;

    /// Maximum snapshot age in milliseconds.
    pub const PROPERTY_HISTORY_EXPIRE_MAX_SNAPSHOT_AGE_MS: &str =
        "history.expire.max-snapshot-age-ms";
    /// Default value for history.expire.max-snapshot-age-ms (5 days).
    pub const PROPERTY_HISTORY_EXPIRE_MAX_SNAPSHOT_AGE_MS_DEFAULT: i64 = 432000000;
}

// =============================================================================
// Type-Safe Enums for Property Values
// =============================================================================

/// Write mode for DML operations (DELETE, UPDATE, MERGE).
///
/// Copy-on-write rewrites entire data files on modification.
/// Merge-on-read writes delete files that are merged at read time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WriteMode {
    /// Copy-on-write: Rewrite data files on modification.
    /// Better for read-heavy workloads with infrequent updates.
    #[default]
    CopyOnWrite,
    /// Merge-on-read: Write delete files merged at read time.
    /// Better for write-heavy workloads with frequent updates.
    MergeOnRead,
}

impl WriteMode {
    /// Convert to the string value used in table properties.
    pub fn as_str(&self) -> &'static str {
        match self {
            WriteMode::CopyOnWrite => "copy-on-write",
            WriteMode::MergeOnRead => "merge-on-read",
        }
    }
}

impl std::fmt::Display for WriteMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for WriteMode {
    type Err = Error;

    /// Parse from a string value (case-insensitive).
    ///
    /// Accepts: "copy-on-write", "cow", "merge-on-read", "mor"
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "copy-on-write" | "cow" => Ok(WriteMode::CopyOnWrite),
            "merge-on-read" | "mor" => Ok(WriteMode::MergeOnRead),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Invalid write mode: '{}'. Expected 'copy-on-write', 'cow', 'merge-on-read', or 'mor'",
                    s
                ),
            )),
        }
    }
}

/// Write distribution mode for controlling how data is distributed during writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WriteDistributionMode {
    /// No specific distribution - data written as-is.
    #[default]
    None,
    /// Hash distribution by partition key.
    Hash,
    /// Range distribution with sorting.
    Range,
}

impl WriteDistributionMode {
    /// Convert to the string value used in table properties.
    pub fn as_str(&self) -> &'static str {
        match self {
            WriteDistributionMode::None => "none",
            WriteDistributionMode::Hash => "hash",
            WriteDistributionMode::Range => "range",
        }
    }
}

impl std::fmt::Display for WriteDistributionMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for WriteDistributionMode {
    type Err = Error;

    /// Parse from a string value (case-insensitive).
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "none" => Ok(WriteDistributionMode::None),
            "hash" => Ok(WriteDistributionMode::Hash),
            "range" => Ok(WriteDistributionMode::Range),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Invalid write distribution mode: '{}'. Expected 'none', 'hash', or 'range'",
                    s
                ),
            )),
        }
    }
}

impl TryFrom<&HashMap<String, String>> for TableProperties {
    // parse by entry key or use default value
    type Error = anyhow::Error;

    fn try_from(props: &HashMap<String, String>) -> std::result::Result<Self, Self::Error> {
        Ok(TableProperties {
            commit_num_retries: parse_property(
                props,
                TableProperties::PROPERTY_COMMIT_NUM_RETRIES,
                TableProperties::PROPERTY_COMMIT_NUM_RETRIES_DEFAULT,
            )?,
            commit_min_retry_wait_ms: parse_property(
                props,
                TableProperties::PROPERTY_COMMIT_MIN_RETRY_WAIT_MS,
                TableProperties::PROPERTY_COMMIT_MIN_RETRY_WAIT_MS_DEFAULT,
            )?,
            commit_max_retry_wait_ms: parse_property(
                props,
                TableProperties::PROPERTY_COMMIT_MAX_RETRY_WAIT_MS,
                TableProperties::PROPERTY_COMMIT_MAX_RETRY_WAIT_MS_DEFAULT,
            )?,
            commit_total_retry_timeout_ms: parse_property(
                props,
                TableProperties::PROPERTY_COMMIT_TOTAL_RETRY_TIME_MS,
                TableProperties::PROPERTY_COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT,
            )?,
            write_format_default: parse_property(
                props,
                TableProperties::PROPERTY_DEFAULT_FILE_FORMAT,
                TableProperties::PROPERTY_DEFAULT_FILE_FORMAT_DEFAULT.to_string(),
            )?,
            write_target_file_size_bytes: parse_property(
                props,
                TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES,
                TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT,
            )?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_properties_default() {
        let props = HashMap::new();
        let table_properties = TableProperties::try_from(&props).unwrap();
        assert_eq!(
            table_properties.commit_num_retries,
            TableProperties::PROPERTY_COMMIT_NUM_RETRIES_DEFAULT
        );
        assert_eq!(
            table_properties.commit_min_retry_wait_ms,
            TableProperties::PROPERTY_COMMIT_MIN_RETRY_WAIT_MS_DEFAULT
        );
        assert_eq!(
            table_properties.commit_max_retry_wait_ms,
            TableProperties::PROPERTY_COMMIT_MAX_RETRY_WAIT_MS_DEFAULT
        );
        assert_eq!(
            table_properties.write_format_default,
            TableProperties::PROPERTY_DEFAULT_FILE_FORMAT_DEFAULT.to_string()
        );
        assert_eq!(
            table_properties.write_target_file_size_bytes,
            TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT
        );
    }

    #[test]
    fn test_table_properties_valid() {
        let props = HashMap::from([
            (
                TableProperties::PROPERTY_COMMIT_NUM_RETRIES.to_string(),
                "10".to_string(),
            ),
            (
                TableProperties::PROPERTY_COMMIT_MAX_RETRY_WAIT_MS.to_string(),
                "20".to_string(),
            ),
            (
                TableProperties::PROPERTY_DEFAULT_FILE_FORMAT.to_string(),
                "avro".to_string(),
            ),
            (
                TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES.to_string(),
                "512".to_string(),
            ),
        ]);
        let table_properties = TableProperties::try_from(&props).unwrap();
        assert_eq!(table_properties.commit_num_retries, 10);
        assert_eq!(table_properties.commit_max_retry_wait_ms, 20);
        assert_eq!(table_properties.write_format_default, "avro".to_string());
        assert_eq!(table_properties.write_target_file_size_bytes, 512);
    }

    #[test]
    fn test_table_properties_invalid() {
        let invalid_retries = HashMap::from([(
            TableProperties::PROPERTY_COMMIT_NUM_RETRIES.to_string(),
            "abc".to_string(),
        )]);

        let table_properties = TableProperties::try_from(&invalid_retries).unwrap_err();
        assert!(
            table_properties.to_string().contains(
                "Invalid value for commit.retry.num-retries: invalid digit found in string"
            )
        );

        let invalid_min_wait = HashMap::from([(
            TableProperties::PROPERTY_COMMIT_MIN_RETRY_WAIT_MS.to_string(),
            "abc".to_string(),
        )]);
        let table_properties = TableProperties::try_from(&invalid_min_wait).unwrap_err();
        assert!(
            table_properties.to_string().contains(
                "Invalid value for commit.retry.min-wait-ms: invalid digit found in string"
            )
        );

        let invalid_max_wait = HashMap::from([(
            TableProperties::PROPERTY_COMMIT_MAX_RETRY_WAIT_MS.to_string(),
            "abc".to_string(),
        )]);
        let table_properties = TableProperties::try_from(&invalid_max_wait).unwrap_err();
        assert!(
            table_properties.to_string().contains(
                "Invalid value for commit.retry.max-wait-ms: invalid digit found in string"
            )
        );

        let invalid_target_size = HashMap::from([(
            TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES.to_string(),
            "abc".to_string(),
        )]);
        let table_properties = TableProperties::try_from(&invalid_target_size).unwrap_err();
        assert!(table_properties.to_string().contains(
            "Invalid value for write.target-file-size-bytes: invalid digit found in string"
        ));
    }

    // =========================================================================
    // WriteMode Tests
    // =========================================================================

    #[test]
    fn test_write_mode_as_str() {
        assert_eq!(WriteMode::CopyOnWrite.as_str(), "copy-on-write");
        assert_eq!(WriteMode::MergeOnRead.as_str(), "merge-on-read");
    }

    #[test]
    fn test_write_mode_from_str_valid() {
        assert_eq!(
            "copy-on-write".parse::<WriteMode>().unwrap(),
            WriteMode::CopyOnWrite
        );
        assert_eq!(
            "merge-on-read".parse::<WriteMode>().unwrap(),
            WriteMode::MergeOnRead
        );
        assert_eq!("cow".parse::<WriteMode>().unwrap(), WriteMode::CopyOnWrite);
        assert_eq!("mor".parse::<WriteMode>().unwrap(), WriteMode::MergeOnRead);
        // Case insensitive
        assert_eq!(
            "Copy-On-Write".parse::<WriteMode>().unwrap(),
            WriteMode::CopyOnWrite
        );
        assert_eq!("COW".parse::<WriteMode>().unwrap(), WriteMode::CopyOnWrite);
        assert_eq!("MOR".parse::<WriteMode>().unwrap(), WriteMode::MergeOnRead);
    }

    #[test]
    fn test_write_mode_from_str_invalid() {
        let result = "invalid".parse::<WriteMode>();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message().contains("Invalid write mode"));
        assert!(err.message().contains("invalid"));

        let result = "".parse::<WriteMode>();
        assert!(result.is_err());
    }

    #[test]
    fn test_write_mode_default() {
        assert_eq!(WriteMode::default(), WriteMode::CopyOnWrite);
    }

    #[test]
    fn test_write_mode_display() {
        assert_eq!(format!("{}", WriteMode::CopyOnWrite), "copy-on-write");
        assert_eq!(format!("{}", WriteMode::MergeOnRead), "merge-on-read");
    }

    // =========================================================================
    // WriteDistributionMode Tests
    // =========================================================================

    #[test]
    fn test_write_distribution_mode_as_str() {
        assert_eq!(WriteDistributionMode::None.as_str(), "none");
        assert_eq!(WriteDistributionMode::Hash.as_str(), "hash");
        assert_eq!(WriteDistributionMode::Range.as_str(), "range");
    }

    #[test]
    fn test_write_distribution_mode_from_str_valid() {
        assert_eq!(
            "none".parse::<WriteDistributionMode>().unwrap(),
            WriteDistributionMode::None
        );
        assert_eq!(
            "hash".parse::<WriteDistributionMode>().unwrap(),
            WriteDistributionMode::Hash
        );
        assert_eq!(
            "range".parse::<WriteDistributionMode>().unwrap(),
            WriteDistributionMode::Range
        );
        // Case insensitive
        assert_eq!(
            "NONE".parse::<WriteDistributionMode>().unwrap(),
            WriteDistributionMode::None
        );
        assert_eq!(
            "Hash".parse::<WriteDistributionMode>().unwrap(),
            WriteDistributionMode::Hash
        );
    }

    #[test]
    fn test_write_distribution_mode_from_str_invalid() {
        let result = "invalid".parse::<WriteDistributionMode>();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message().contains("Invalid write distribution mode"));

        let result = "".parse::<WriteDistributionMode>();
        assert!(result.is_err());
    }

    #[test]
    fn test_write_distribution_mode_default() {
        assert_eq!(WriteDistributionMode::default(), WriteDistributionMode::None);
    }

    #[test]
    fn test_write_distribution_mode_display() {
        assert_eq!(format!("{}", WriteDistributionMode::None), "none");
        assert_eq!(format!("{}", WriteDistributionMode::Hash), "hash");
        assert_eq!(format!("{}", WriteDistributionMode::Range), "range");
    }
}
