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

//! Configuration options for the rewrite data files action.

/// Configuration options for the rewrite data files operation.
///
/// These options control file selection, grouping, execution, and commit behavior.
/// Default values match the Apache Iceberg Java implementation for compatibility.
///
/// # Size Configuration
///
/// | Option | Default | Description |
/// |--------|---------|-------------|
/// | `target_file_size_bytes` | 512 MB | Target output file size |
/// | `min_file_size_bytes` | 75% of target | Files smaller are candidates |
/// | `max_file_size_bytes` | 180% of target | Files larger are split |
/// | `max_file_group_size_bytes` | 100 GB | Max bytes per file group |
///
/// # Execution Configuration
///
/// | Option | Default | Description |
/// |--------|---------|-------------|
/// | `min_input_files` | 5 | Minimum files to trigger rewrite |
/// | `max_concurrent_file_group_rewrites` | num_cpus | Parallelism limit |
/// | `partial_progress_enabled` | false | Commit incrementally |
/// | `partial_progress_max_commits` | 10 | Max commits if partial enabled |
///
/// # Delete Handling (V2+)
///
/// | Option | Default | Description |
/// |--------|---------|-------------|
/// | `use_starting_sequence_number` | true | Critical for MoR correctness |
/// | `remove_dangling_deletes` | false | Clean up orphan deletes |
/// | `delete_file_threshold` | None | Delete count trigger |
#[derive(Debug, Clone)]
pub struct RewriteDataFilesOptions {
    // ═══════════════════════════════════════════════════════════════════════
    // Size Configuration
    // ═══════════════════════════════════════════════════════════════════════
    /// Target file size in bytes.
    ///
    /// Output files will be approximately this size. The actual size may vary
    /// slightly to avoid creating tiny remainder files.
    ///
    /// Default: 512 MB
    ///
    /// Use `from_table_properties()` to initialize from table's
    /// `write.target-file-size-bytes` property.
    pub target_file_size_bytes: u64,

    /// Minimum file size in bytes for candidate selection.
    ///
    /// Files smaller than this threshold are candidates for compaction.
    ///
    /// Default: 75% of `target_file_size_bytes`
    pub min_file_size_bytes: u64,

    /// Whether `min_file_size_bytes` was explicitly set.
    ///
    /// Used to preserve explicit values when `target_file_size_bytes` is changed.
    pub(crate) min_file_size_bytes_set: bool,

    /// Maximum file size in bytes for candidate selection.
    ///
    /// Files larger than this threshold are candidates for splitting.
    ///
    /// Default: 180% of `target_file_size_bytes`
    pub max_file_size_bytes: u64,

    /// Whether `max_file_size_bytes` was explicitly set.
    pub(crate) max_file_size_bytes_set: bool,

    /// Maximum bytes per file group.
    ///
    /// Files within a partition are grouped up to this size for processing.
    /// Larger groups mean fewer output files but higher memory usage.
    ///
    /// Default: 100 GB
    pub max_file_group_size_bytes: u64,

    // ═══════════════════════════════════════════════════════════════════════
    // Execution Configuration
    // ═══════════════════════════════════════════════════════════════════════
    /// Minimum number of input files to trigger a rewrite.
    ///
    /// File groups with fewer files than this are not rewritten unless
    /// their total size exceeds `target_file_size_bytes`.
    ///
    /// Default: 5
    pub min_input_files: u32,

    /// Maximum concurrent file group rewrites.
    ///
    /// Limits parallelism during execution. Each concurrent rewrite holds
    /// file data in memory, so this affects peak memory usage.
    ///
    /// Default: `num_cpus::get()` (number of CPU cores)
    pub max_concurrent_file_group_rewrites: Option<u32>,

    /// Enable partial progress commits.
    ///
    /// When enabled, each file group is committed independently after
    /// completion. This allows progress even if some groups fail, but
    /// creates multiple snapshots instead of one.
    ///
    /// Default: false (all-or-nothing commit)
    pub partial_progress_enabled: bool,

    /// Maximum commits for partial progress mode.
    ///
    /// Only used when `partial_progress_enabled` is true. Limits the
    /// number of intermediate commits to reduce metadata churn.
    ///
    /// Default: 10
    pub partial_progress_max_commits: u32,

    /// Maximum failed file groups before aborting.
    ///
    /// Only used when `partial_progress_enabled` is true. If more than
    /// this many groups fail, the operation aborts.
    ///
    /// Default: None (unlimited)
    pub partial_progress_max_failed_commits: Option<u32>,

    /// File group execution order.
    ///
    /// Controls which file groups are processed first. Can be used to
    /// prioritize large or small groups.
    ///
    /// Default: `None` (no specific ordering)
    pub rewrite_job_order: RewriteJobOrder,

    // ═══════════════════════════════════════════════════════════════════════
    // Delete Handling (V2+ Tables)
    // ═══════════════════════════════════════════════════════════════════════
    /// Use starting sequence number for new files.
    ///
    /// **CRITICAL for MoR (Merge-on-Read) tables**: When true, new files
    /// are written with the sequence number captured at compaction start.
    /// This ensures equality deletes added concurrently continue to apply
    /// correctly to the compacted files.
    ///
    /// From the Iceberg spec:
    /// > "This avoids commit conflicts with updates that add newer equality
    /// > deletes at a higher sequence number."
    ///
    /// Default: true
    pub use_starting_sequence_number: bool,

    /// Remove dangling delete files after compaction.
    ///
    /// When true, delete files that no longer reference any data files
    /// (because all referenced data was compacted) are removed.
    ///
    /// Default: false
    pub remove_dangling_deletes: bool,

    /// Delete file threshold for candidate selection.
    ///
    /// Files with this many or more associated delete files become
    /// candidates for rewrite regardless of their size. This helps
    /// clean up tables with many accumulated deletes.
    ///
    /// Default: None (disabled)
    pub delete_file_threshold: Option<u32>,
}

impl Default for RewriteDataFilesOptions {
    fn default() -> Self {
        // Default target size: 512 MB (matches Java implementation)
        const TARGET_SIZE: u64 = 512 * 1024 * 1024;

        // Default concurrency to number of CPUs (safe default to prevent OOM)
        // Falls back to 4 if detection fails
        let default_concurrency = std::thread::available_parallelism()
            .map(|n| n.get() as u32)
            .unwrap_or(4);

        Self {
            target_file_size_bytes: TARGET_SIZE,
            min_file_size_bytes: (TARGET_SIZE as f64 * 0.75) as u64,
            min_file_size_bytes_set: false,
            max_file_size_bytes: (TARGET_SIZE as f64 * 1.80) as u64,
            max_file_size_bytes_set: false,
            max_file_group_size_bytes: 100 * 1024 * 1024 * 1024, // 100 GB
            min_input_files: 5,
            // Default to number of CPUs for safe parallelism
            max_concurrent_file_group_rewrites: Some(default_concurrency),
            partial_progress_enabled: false,
            partial_progress_max_commits: 10,
            partial_progress_max_failed_commits: None,
            rewrite_job_order: RewriteJobOrder::default(),
            use_starting_sequence_number: true,
            remove_dangling_deletes: false,
            delete_file_threshold: None,
        }
    }
}

impl RewriteDataFilesOptions {
    /// Create options from table properties.
    ///
    /// Reads the following table properties:
    /// - `write.target-file-size-bytes`: Target output file size
    pub fn from_table_properties(
        properties: &std::collections::HashMap<String, String>,
    ) -> Self {
        let mut options = Self::default();

        // Read target file size from table property
        if let Some(size_str) = properties.get("write.target-file-size-bytes") {
            if let Ok(size) = size_str.parse::<u64>() {
                options.target_file_size_bytes = size;
                // Recompute derived defaults
                options.min_file_size_bytes = (size as f64 * 0.75) as u64;
                options.max_file_size_bytes = (size as f64 * 1.80) as u64;
            }
        }

        options
    }
}

/// Job execution ordering strategy.
///
/// Controls which file groups are processed first during compaction.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum RewriteJobOrder {
    /// No specific ordering (default).
    ///
    /// File groups are processed in the order they were created during planning.
    #[default]
    None,

    /// Process smallest groups first (by total bytes).
    ///
    /// Use this to make quick progress on small file groups.
    BytesAsc,

    /// Process largest groups first (by total bytes).
    ///
    /// Use this to prioritize high-impact compactions.
    BytesDesc,

    /// Process groups with fewest files first.
    ///
    /// Use this to quickly reduce file count.
    FilesAsc,

    /// Process groups with most files first.
    ///
    /// Use this to prioritize partitions with the most fragmentation.
    FilesDesc,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_options() {
        let options = RewriteDataFilesOptions::default();

        // Size configuration
        assert_eq!(options.target_file_size_bytes, 512 * 1024 * 1024);
        assert_eq!(
            options.min_file_size_bytes,
            (512.0 * 1024.0 * 1024.0 * 0.75) as u64
        );
        assert_eq!(
            options.max_file_size_bytes,
            (512.0 * 1024.0 * 1024.0 * 1.80) as u64
        );
        assert_eq!(options.max_file_group_size_bytes, 100 * 1024 * 1024 * 1024);

        // Execution configuration
        assert_eq!(options.min_input_files, 5);
        assert!(options.max_concurrent_file_group_rewrites.is_some());
        assert!(!options.partial_progress_enabled);
        assert_eq!(options.partial_progress_max_commits, 10);
        assert_eq!(options.rewrite_job_order, RewriteJobOrder::None);

        // Delete handling
        assert!(options.use_starting_sequence_number);
        assert!(!options.remove_dangling_deletes);
        assert!(options.delete_file_threshold.is_none());
    }

    #[test]
    fn test_from_table_properties() {
        let mut properties = std::collections::HashMap::new();
        properties.insert(
            "write.target-file-size-bytes".to_string(),
            "268435456".to_string(), // 256 MB
        );

        let options = RewriteDataFilesOptions::from_table_properties(&properties);

        assert_eq!(options.target_file_size_bytes, 268435456);
        assert_eq!(
            options.min_file_size_bytes,
            (268435456.0 * 0.75) as u64
        );
        assert_eq!(
            options.max_file_size_bytes,
            (268435456.0 * 1.80) as u64
        );
    }

    #[test]
    fn test_from_empty_properties() {
        let properties = std::collections::HashMap::new();
        let options = RewriteDataFilesOptions::from_table_properties(&properties);

        // Should use defaults
        assert_eq!(options.target_file_size_bytes, 512 * 1024 * 1024);
    }

    #[test]
    fn test_job_order_default() {
        assert_eq!(RewriteJobOrder::default(), RewriteJobOrder::None);
    }
}
