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

//! Configuration options for the rewrite manifests action.

use std::collections::HashMap;

/// Default target manifest size (8 MB).
pub const DEFAULT_TARGET_MANIFEST_SIZE_BYTES: u64 = 8 * 1024 * 1024;
/// Default maximum manifest entries to keep in memory during rewrite.
pub const DEFAULT_MAX_ENTRIES_IN_MEMORY: u64 = 1_000_000;

/// Configuration options for the rewrite manifests operation.
///
/// These options control manifest selection, target sizes, and rewrite behavior.
/// Default values match the Apache Iceberg Java implementation for compatibility.
///
/// # Size Configuration
///
/// | Option | Default | Description |
/// |--------|---------|-------------|
/// | `target_manifest_size_bytes` | 8 MB | Target size for output manifests |
/// | `min_manifest_size_bytes` | 0 | Minimum size threshold for rewrite |
/// | `max_entries_in_memory` | 1,000,000 | Maximum entries to hold in memory (0 disables) |
///
/// # Selection Configuration
///
/// | Option | Default | Description |
/// |--------|---------|-------------|
/// | `spec_id` | None | Partition spec to rewrite (None = all) |
/// | `rewrite_delete_manifests` | true | Include delete manifests |
///
/// # Example
///
/// ```ignore
/// use iceberg::transaction::RewriteManifestsOptions;
///
/// // Default options
/// let options = RewriteManifestsOptions::default();
///
/// // Custom options
/// let options = RewriteManifestsOptions {
///     target_manifest_size_bytes: 16 * 1024 * 1024,  // 16 MB
///     min_manifest_size_bytes: 1 * 1024 * 1024,      // 1 MB minimum
///     ..Default::default()
/// };
/// ```
#[derive(Debug, Clone)]
pub struct RewriteManifestsOptions {
    /// Target size for output manifests in bytes.
    ///
    /// Output manifests will be approximately this size. The actual size may vary
    /// to avoid creating small remainder manifests or to preserve partition clustering.
    ///
    /// Default: 8 MB (matches Iceberg Java `commit.manifest.target-size-bytes`)
    pub target_manifest_size_bytes: u64,

    /// Partition spec ID to rewrite.
    ///
    /// When set, only manifests for this partition spec are rewritten.
    /// When None, all manifests regardless of spec ID are candidates.
    ///
    /// Default: None (all specs)
    pub spec_id: Option<i32>,

    /// Whether to rewrite delete manifests in addition to data manifests.
    ///
    /// When true, both data manifests and delete manifests are consolidated.
    /// When false, only data manifests are rewritten.
    ///
    /// Default: true
    pub rewrite_delete_manifests: bool,

    /// Minimum manifest size in bytes to be considered for rewrite.
    ///
    /// When set to a non-zero value, only manifests smaller than this threshold
    /// are candidates for rewrite.
    ///
    /// Default: 0 (all manifests are candidates)
    pub min_manifest_size_bytes: u64,

    /// Maximum number of manifest entries to hold in memory.
    ///
    /// This guard prevents rewrite operations from loading too many entries
    /// into memory at once. Set to `None` to disable the limit.
    ///
    /// Default: 1,000,000 entries
    pub max_entries_in_memory: Option<u64>,
}

impl Default for RewriteManifestsOptions {
    fn default() -> Self {
        Self {
            target_manifest_size_bytes: DEFAULT_TARGET_MANIFEST_SIZE_BYTES,
            spec_id: None,
            rewrite_delete_manifests: true,
            min_manifest_size_bytes: 0,
            max_entries_in_memory: Some(DEFAULT_MAX_ENTRIES_IN_MEMORY),
        }
    }
}

impl RewriteManifestsOptions {
    /// Create options from table properties.
    ///
    /// Reads the following table properties:
    /// - `commit.manifest.target-size-bytes`: Target manifest size
    /// - `commit.manifest.rewrite.max-entries`: Max entries in memory (0 disables)
    ///
    /// Unset properties fall back to defaults.
    pub fn from_table_properties(properties: &HashMap<String, String>) -> Self {
        let mut options = Self::default();

        if let Some(size) = properties
            .get("commit.manifest.target-size-bytes")
            .and_then(|size_str| size_str.parse::<u64>().ok())
        {
            options.target_manifest_size_bytes = size;
        }

        if let Some(max_entries) = properties
            .get("commit.manifest.rewrite.max-entries")
            .and_then(|entries_str| entries_str.parse::<u64>().ok())
        {
            options.max_entries_in_memory = if max_entries == 0 {
                None
            } else {
                Some(max_entries)
            };
        }

        options
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_options() {
        let options = RewriteManifestsOptions::default();
        assert_eq!(options.target_manifest_size_bytes, 8 * 1024 * 1024);
        assert!(options.spec_id.is_none());
        assert!(options.rewrite_delete_manifests);
        assert_eq!(options.min_manifest_size_bytes, 0);
        assert_eq!(
            options.max_entries_in_memory,
            Some(DEFAULT_MAX_ENTRIES_IN_MEMORY)
        );
    }

    #[test]
    fn test_from_table_properties() {
        let mut props = HashMap::new();
        props.insert(
            "commit.manifest.target-size-bytes".to_string(),
            "16777216".to_string(),
        );
        props.insert(
            "commit.manifest.rewrite.max-entries".to_string(),
            "5000".to_string(),
        );

        let options = RewriteManifestsOptions::from_table_properties(&props);
        assert_eq!(options.target_manifest_size_bytes, 16 * 1024 * 1024);
        assert_eq!(options.max_entries_in_memory, Some(5000));
    }

    #[test]
    fn test_from_table_properties_invalid() {
        let mut props = HashMap::new();
        props.insert(
            "commit.manifest.target-size-bytes".to_string(),
            "invalid".to_string(),
        );
        props.insert(
            "commit.manifest.rewrite.max-entries".to_string(),
            "invalid".to_string(),
        );

        let options = RewriteManifestsOptions::from_table_properties(&props);
        // Should fall back to default
        assert_eq!(options.target_manifest_size_bytes, 8 * 1024 * 1024);
        assert_eq!(
            options.max_entries_in_memory,
            Some(DEFAULT_MAX_ENTRIES_IN_MEMORY)
        );
    }

    #[test]
    fn test_from_table_properties_disable_entry_limit() {
        let mut props = HashMap::new();
        props.insert(
            "commit.manifest.rewrite.max-entries".to_string(),
            "0".to_string(),
        );

        let options = RewriteManifestsOptions::from_table_properties(&props);
        assert!(options.max_entries_in_memory.is_none());
    }

    // Note: candidate selection logic will be tested alongside planner/execution
    // once the rewrite_manifests action is implemented.
}
