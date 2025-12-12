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

//! Result types for the rewrite data files action.

use crate::spec::Struct;

/// Result of a rewrite data files operation.
///
/// Contains metrics about the compaction operation including file counts,
/// byte counts, and per-group results.
///
/// # Field Population
///
/// The [`RewriteDataFilesCommitter`](super::RewriteDataFilesCommitter) populates
/// aggregate metrics from the plan and results:
///
/// - `rewritten_data_files_count`, `added_data_files_count` - File counts
/// - `rewritten_bytes`, `added_bytes` - Byte totals
/// - `file_groups_count` - From plan
///
/// The following fields are **placeholders** and should be populated by the
/// execution engine or caller:
///
/// - `file_group_results` - Per-group metrics (empty by default)
/// - `failed_file_groups` - Failure details (empty by default)
/// - `duration_ms` - Caller should time the operation
/// - `removed_delete_files_count` - Not yet implemented
/// - `commits_count` - Always 1 (partial progress not yet implemented)
///
/// # Computed Metrics
///
/// - `compaction_ratio()` - Input files / output files (e.g., 10.0 = 10:1)
/// - `bytes_saved()` - Positive = space savings, negative = growth
/// - `throughput_bytes_per_sec()` - Requires `duration_ms` to be set
#[derive(Debug, Clone)]
#[must_use]
pub struct RewriteDataFilesResult {
    /// Number of data files that were rewritten (removed).
    pub rewritten_data_files_count: u64,

    /// Number of new data files created (added).
    pub added_data_files_count: u64,

    /// Total bytes in the rewritten (removed) files.
    pub rewritten_bytes: u64,

    /// Total bytes in the new (added) files.
    pub added_bytes: u64,

    /// Number of file groups processed.
    pub file_groups_count: u64,

    /// Number of file groups that failed.
    pub failed_file_groups_count: u64,

    /// Per-file-group results.
    pub file_group_results: Vec<FileGroupResult>,

    /// Failed file group information (if any).
    pub failed_file_groups: Vec<FileGroupFailure>,

    /// Number of dangling delete files removed.
    ///
    /// Only populated when `remove_dangling_deletes` is enabled.
    pub removed_delete_files_count: u64,

    /// Duration of the entire operation in milliseconds.
    pub duration_ms: u64,

    /// Number of commits made.
    ///
    /// 1 for normal operation, multiple for partial progress mode.
    pub commits_count: u64,
}

impl RewriteDataFilesResult {
    /// Create an empty result (no files processed).
    ///
    /// Used when there are no files to compact.
    pub fn empty() -> Self {
        Self {
            rewritten_data_files_count: 0,
            added_data_files_count: 0,
            rewritten_bytes: 0,
            added_bytes: 0,
            file_groups_count: 0,
            failed_file_groups_count: 0,
            file_group_results: vec![],
            failed_file_groups: vec![],
            removed_delete_files_count: 0,
            duration_ms: 0,
            commits_count: 0,
        }
    }

    /// Calculate the compaction ratio.
    ///
    /// Returns how many input files were consolidated into one output file.
    /// A ratio of 10.0 means 10 files were combined into 1.
    ///
    /// Returns 0.0 if no files were processed.
    pub fn compaction_ratio(&self) -> f64 {
        if self.added_data_files_count == 0 {
            return 0.0;
        }
        self.rewritten_data_files_count as f64 / self.added_data_files_count as f64
    }

    /// Calculate bytes saved (positive) or added (negative).
    ///
    /// Positive values indicate space savings from better compression
    /// or eliminated overhead. Negative values indicate space increase
    /// (unusual but possible with certain data patterns).
    pub fn bytes_saved(&self) -> i64 {
        self.rewritten_bytes as i64 - self.added_bytes as i64
    }

    /// Calculate the compression improvement ratio.
    ///
    /// Values > 1.0 indicate better compression in output files.
    /// Values < 1.0 indicate worse compression.
    /// Returns 1.0 if no files were processed.
    pub fn compression_ratio(&self) -> f64 {
        if self.added_bytes == 0 {
            return 1.0;
        }
        self.rewritten_bytes as f64 / self.added_bytes as f64
    }

    /// Calculate throughput in bytes per second.
    ///
    /// Returns None if duration is 0.
    pub fn throughput_bytes_per_sec(&self) -> Option<f64> {
        if self.duration_ms == 0 {
            return None;
        }
        Some(self.rewritten_bytes as f64 / (self.duration_ms as f64 / 1000.0))
    }

    /// Check if the operation was successful.
    ///
    /// Returns true if no file groups failed.
    pub fn is_success(&self) -> bool {
        self.failed_file_groups_count == 0
    }

    /// Check if any work was done.
    ///
    /// Returns false if no files were rewritten.
    pub fn has_changes(&self) -> bool {
        self.rewritten_data_files_count > 0
    }
}

impl Default for RewriteDataFilesResult {
    fn default() -> Self {
        Self::empty()
    }
}

/// Result for a single file group.
///
/// Contains metrics about the processing of one group of files
/// within a partition.
#[derive(Debug, Clone)]
pub struct FileGroupResult {
    /// Index of this group in execution order.
    pub group_index: u32,

    /// Partition this group belongs to (None for unpartitioned tables).
    pub partition: Option<Struct>,

    /// Number of input files in this group.
    pub input_files_count: u32,

    /// Number of output files created.
    pub output_files_count: u32,

    /// Total bytes in input files.
    pub input_bytes: u64,

    /// Total bytes in output files.
    pub output_bytes: u64,

    /// Duration for processing this group in milliseconds.
    pub duration_ms: u64,
}

impl FileGroupResult {
    /// Calculate the compaction ratio for this group.
    pub fn compaction_ratio(&self) -> f64 {
        if self.output_files_count == 0 {
            return 0.0;
        }
        self.input_files_count as f64 / self.output_files_count as f64
    }

    /// Calculate bytes saved in this group.
    pub fn bytes_saved(&self) -> i64 {
        self.input_bytes as i64 - self.output_bytes as i64
    }
}

/// Information about a failed file group.
#[derive(Debug, Clone)]
pub struct FileGroupFailure {
    /// Index of the failed group.
    pub group_index: u32,

    /// Partition this group belongs to (None for unpartitioned tables).
    pub partition: Option<Struct>,

    /// Number of input files that were in the group.
    pub input_files_count: u32,

    /// Total bytes in input files.
    pub input_bytes: u64,

    /// Error message describing the failure.
    pub error: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_result() {
        let result = RewriteDataFilesResult::empty();

        assert_eq!(result.rewritten_data_files_count, 0);
        assert_eq!(result.added_data_files_count, 0);
        assert_eq!(result.compaction_ratio(), 0.0);
        assert_eq!(result.bytes_saved(), 0);
        assert!(result.is_success());
        assert!(!result.has_changes());
    }

    #[test]
    fn test_compaction_ratio() {
        let result = RewriteDataFilesResult {
            rewritten_data_files_count: 100,
            added_data_files_count: 10,
            ..RewriteDataFilesResult::empty()
        };

        assert_eq!(result.compaction_ratio(), 10.0);
    }

    #[test]
    fn test_bytes_saved() {
        let result = RewriteDataFilesResult {
            rewritten_bytes: 1000,
            added_bytes: 800,
            ..RewriteDataFilesResult::empty()
        };

        assert_eq!(result.bytes_saved(), 200);
    }

    #[test]
    fn test_bytes_added() {
        // Edge case: output larger than input (unusual but possible)
        let result = RewriteDataFilesResult {
            rewritten_bytes: 800,
            added_bytes: 1000,
            ..RewriteDataFilesResult::empty()
        };

        assert_eq!(result.bytes_saved(), -200);
    }

    #[test]
    fn test_compression_ratio() {
        let result = RewriteDataFilesResult {
            rewritten_bytes: 1000,
            added_bytes: 500,
            ..RewriteDataFilesResult::empty()
        };

        assert_eq!(result.compression_ratio(), 2.0);
    }

    #[test]
    fn test_throughput() {
        let result = RewriteDataFilesResult {
            rewritten_bytes: 1_000_000_000, // 1 GB
            duration_ms: 10_000,            // 10 seconds
            ..RewriteDataFilesResult::empty()
        };

        let throughput = result.throughput_bytes_per_sec().unwrap();
        assert_eq!(throughput, 100_000_000.0); // 100 MB/s
    }

    #[test]
    fn test_throughput_zero_duration() {
        let result = RewriteDataFilesResult {
            rewritten_bytes: 1000,
            duration_ms: 0,
            ..RewriteDataFilesResult::empty()
        };

        assert!(result.throughput_bytes_per_sec().is_none());
    }

    #[test]
    fn test_is_success() {
        let mut result = RewriteDataFilesResult::empty();
        assert!(result.is_success());

        result.failed_file_groups_count = 1;
        assert!(!result.is_success());
    }

    #[test]
    fn test_has_changes() {
        let mut result = RewriteDataFilesResult::empty();
        assert!(!result.has_changes());

        result.rewritten_data_files_count = 5;
        assert!(result.has_changes());
    }

    #[test]
    fn test_file_group_result() {
        let group = FileGroupResult {
            group_index: 0,
            partition: None,
            input_files_count: 20,
            output_files_count: 2,
            input_bytes: 1000,
            output_bytes: 900,
            duration_ms: 5000,
        };

        assert_eq!(group.compaction_ratio(), 10.0);
        assert_eq!(group.bytes_saved(), 100);
    }
}
