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

//! Result types for the rewrite manifests action.

/// Result of a rewrite manifests operation.
///
/// Contains metrics about the manifest consolidation operation including
/// manifest counts, file counts, and timing information.
///
/// # Computed Metrics
///
/// - `is_empty()` - Returns true if no manifests were rewritten
/// - `consolidation_ratio()` - Input manifests / output manifests
/// - `total_manifests_rewritten()` - Sum of data and delete manifests rewritten
/// - `total_manifests_added()` - Sum of data and delete manifests added
///
/// # Example
///
/// ```ignore
/// let result = table.rewrite_manifests().await?;
///
/// println!("Consolidated {} manifests into {}",
///     result.total_manifests_rewritten(),
///     result.total_manifests_added()
/// );
///
/// if result.consolidation_ratio() > 1.0 {
///     println!("Achieved {:.1}x consolidation",
///         result.consolidation_ratio());
/// }
/// ```
#[derive(Debug, Clone, Default)]
#[must_use]
pub struct RewriteManifestsResult {
    /// Number of data manifests that were rewritten (removed).
    pub rewritten_data_manifests_count: u32,

    /// Number of new data manifests created (added).
    pub added_data_manifests_count: u32,

    /// Number of delete manifests that were rewritten (removed).
    pub rewritten_delete_manifests_count: u32,

    /// Number of new delete manifests created (added).
    pub added_delete_manifests_count: u32,

    /// Total data files referenced across all rewritten manifests.
    ///
    /// This count should remain unchanged after rewrite (files are not
    /// added or removed, only reorganized).
    pub total_data_files_count: u64,

    /// Total delete files referenced across all rewritten manifests.
    ///
    /// This count should remain unchanged after rewrite.
    pub total_delete_files_count: u64,

    /// Time spent in planning phase (milliseconds).
    ///
    /// Planning includes loading manifests, filtering by predicate,
    /// and loading manifest entries.
    pub planning_duration_ms: u64,

    /// Time spent in rewrite phase (milliseconds).
    ///
    /// Rewrite includes sorting entries, batching, and writing new manifests.
    pub rewrite_duration_ms: u64,

    /// Time spent in commit phase (milliseconds).
    ///
    /// Commit includes validation and atomic metadata update.
    pub commit_duration_ms: u64,
}

impl RewriteManifestsResult {
    /// Create an empty result (no manifests processed).
    ///
    /// Used when there are no manifests to rewrite.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Returns true if no manifests were rewritten.
    ///
    /// This occurs when:
    /// - The table has no manifests
    /// - No manifests match the selection criteria
    /// - All manifests are already optimally sized
    pub fn is_empty(&self) -> bool {
        self.rewritten_data_manifests_count == 0 && self.rewritten_delete_manifests_count == 0
    }

    /// Total number of manifests rewritten (data + delete).
    pub fn total_manifests_rewritten(&self) -> u32 {
        self.rewritten_data_manifests_count + self.rewritten_delete_manifests_count
    }

    /// Total number of manifests added (data + delete).
    pub fn total_manifests_added(&self) -> u32 {
        self.added_data_manifests_count + self.added_delete_manifests_count
    }

    /// Calculate the consolidation ratio.
    ///
    /// Returns how many input manifests were consolidated into one output manifest.
    /// A ratio of 10.0 means 10 manifests were combined into 1.
    ///
    /// Returns 0.0 if no manifests were processed.
    pub fn consolidation_ratio(&self) -> f64 {
        let total_added = self.total_manifests_added();
        if total_added == 0 {
            return 0.0;
        }
        self.total_manifests_rewritten() as f64 / total_added as f64
    }

    /// Total operation duration in milliseconds.
    ///
    /// Sum of planning, rewrite, and commit phases.
    pub fn total_duration_ms(&self) -> u64 {
        self.planning_duration_ms + self.rewrite_duration_ms + self.commit_duration_ms
    }

    /// Total files processed (data + delete).
    pub fn total_files_count(&self) -> u64 {
        self.total_data_files_count + self.total_delete_files_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_result() {
        let result = RewriteManifestsResult::empty();
        assert!(result.is_empty());
        assert_eq!(result.total_manifests_rewritten(), 0);
        assert_eq!(result.total_manifests_added(), 0);
        assert_eq!(result.consolidation_ratio(), 0.0);
    }

    #[test]
    fn test_consolidation_ratio() {
        let result = RewriteManifestsResult {
            rewritten_data_manifests_count: 100,
            added_data_manifests_count: 10,
            ..Default::default()
        };

        assert!(!result.is_empty());
        assert_eq!(result.total_manifests_rewritten(), 100);
        assert_eq!(result.total_manifests_added(), 10);
        assert_eq!(result.consolidation_ratio(), 10.0);
    }

    #[test]
    fn test_total_duration() {
        let result = RewriteManifestsResult {
            planning_duration_ms: 100,
            rewrite_duration_ms: 500,
            commit_duration_ms: 50,
            ..Default::default()
        };

        assert_eq!(result.total_duration_ms(), 650);
    }

    #[test]
    fn test_with_delete_manifests() {
        let result = RewriteManifestsResult {
            rewritten_data_manifests_count: 50,
            added_data_manifests_count: 5,
            rewritten_delete_manifests_count: 20,
            added_delete_manifests_count: 2,
            total_data_files_count: 1000,
            total_delete_files_count: 100,
            ..Default::default()
        };

        assert_eq!(result.total_manifests_rewritten(), 70);
        assert_eq!(result.total_manifests_added(), 7);
        assert_eq!(result.consolidation_ratio(), 10.0);
        assert_eq!(result.total_files_count(), 1100);
    }
}
