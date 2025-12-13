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

//! Rewrite strategies for the rewrite data files action.

use crate::spec::SortOrder;

/// Strategy for rewriting data files.
///
/// Different strategies optimize for different query patterns:
///
/// - **BinPack**: Fastest - just combines files by size
/// - **Sort**: Sorts data during compaction for range scan efficiency
/// - **ZOrder**: Clusters data for multi-column range queries
#[derive(Debug, Clone)]
pub enum RewriteStrategy {
    /// Bin-pack strategy (default).
    ///
    /// Combines files based on size without changing data order.
    /// This is the fastest strategy as it avoids any sorting overhead.
    ///
    /// Use when:
    /// - Query patterns don't benefit from sorted data
    /// - Minimizing compaction time is priority
    /// - Data already has good locality
    ///
    /// # Implementation (Phase 2.1)
    ///
    /// Files are grouped using First-Fit Decreasing bin-packing algorithm:
    /// 1. Sort candidate files by size (descending)
    /// 2. Place each file into the first group that has room
    /// 3. Create new groups as needed up to max_file_group_size
    BinPack,

    /// Sort strategy.
    ///
    /// Sorts data during compaction using either the table's default sort order
    /// or a custom sort order. Produces files with sorted, non-overlapping
    /// key ranges for efficient range scans.
    ///
    /// Use when:
    /// - Queries frequently filter on sort columns
    /// - Range scans are common
    /// - Data locality on sort key improves performance
    ///
    /// # Implementation (Phase 2.2)
    ///
    /// 1. Read all data from candidate files
    /// 2. Apply sort order (DataFusion sort)
    /// 3. Write sorted data to target-sized files
    ///
    /// Sort execution is provided by external executors (e.g., `iceberg-datafusion`).
    /// The core library provides planner and committer support.
    Sort {
        /// Custom sort order, or None to use table's default.
        sort_order: Option<SortOrder>,
    },

    /// Z-order (Morton encoding) strategy.
    ///
    /// Clusters data using Z-order curves for efficient multi-dimensional
    /// range queries. Particularly effective when queries filter on multiple
    /// columns simultaneously.
    ///
    /// Use when:
    /// - Queries filter on multiple columns together
    /// - No single column dominates filter predicates
    /// - Data has multiple "hot" filter dimensions
    ///
    /// # Implementation (Phase 2.3)
    ///
    /// 1. Read all data from candidate files
    /// 2. Compute Z-order values for specified columns
    /// 3. Sort by Z-order value
    /// 4. Write to target-sized files
    ///
    /// **Note**: Phase 2.3 - Not yet implemented.
    ZOrder {
        /// Columns to include in Z-order clustering.
        columns: Vec<String>,
    },
}

impl Default for RewriteStrategy {
    fn default() -> Self {
        Self::BinPack
    }
}

impl RewriteStrategy {
    /// Check if this strategy is bin-pack.
    pub fn is_bin_pack(&self) -> bool {
        matches!(self, Self::BinPack)
    }

    /// Check if this strategy is sort.
    pub fn is_sort(&self) -> bool {
        matches!(self, Self::Sort { .. })
    }

    /// Check if this strategy is Z-order.
    pub fn is_z_order(&self) -> bool {
        matches!(self, Self::ZOrder { .. })
    }

    /// Get the name of this strategy.
    pub fn name(&self) -> &'static str {
        match self {
            Self::BinPack => "binpack",
            Self::Sort { .. } => "sort",
            Self::ZOrder { .. } => "zorder",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_strategy() {
        let strategy = RewriteStrategy::default();
        assert!(strategy.is_bin_pack());
        assert_eq!(strategy.name(), "binpack");
    }

    #[test]
    fn test_bin_pack_strategy() {
        let strategy = RewriteStrategy::BinPack;
        assert!(strategy.is_bin_pack());
        assert!(!strategy.is_sort());
        assert!(!strategy.is_z_order());
    }

    #[test]
    fn test_sort_strategy() {
        let strategy = RewriteStrategy::Sort { sort_order: None };
        assert!(!strategy.is_bin_pack());
        assert!(strategy.is_sort());
        assert!(!strategy.is_z_order());
        assert_eq!(strategy.name(), "sort");
    }

    #[test]
    fn test_z_order_strategy() {
        let strategy = RewriteStrategy::ZOrder {
            columns: vec!["col1".to_string(), "col2".to_string()],
        };
        assert!(!strategy.is_bin_pack());
        assert!(!strategy.is_sort());
        assert!(strategy.is_z_order());
        assert_eq!(strategy.name(), "zorder");
    }
}
