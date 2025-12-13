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

//! Progress reporting for compaction operations.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// Events emitted during compaction for progress tracking.
#[derive(Debug, Clone)]
pub enum CompactionProgressEvent {
    /// Compaction operation started.
    Started {
        /// Total number of file groups to process.
        total_groups: u32,
        /// Total bytes to process.
        total_bytes: u64,
        /// Total input files.
        total_files: u64,
    },
    /// A file group started processing.
    GroupStarted {
        /// The group being processed.
        group_id: u32,
        /// Number of input files in this group.
        input_files: u32,
        /// Total bytes in this group.
        input_bytes: u64,
    },
    /// A file group completed successfully.
    GroupCompleted {
        /// The group that completed.
        group_id: u32,
        /// Number of output files created.
        output_files: u32,
        /// Total bytes written.
        output_bytes: u64,
        /// Processing time in milliseconds.
        duration_ms: u64,
    },
    /// A file group failed.
    GroupFailed {
        /// The group that failed.
        group_id: u32,
        /// Error message.
        error: String,
    },
    /// Compaction operation completed.
    Completed {
        /// Total groups processed.
        groups_processed: u32,
        /// Total groups failed.
        groups_failed: u32,
        /// Total duration in milliseconds.
        duration_ms: u64,
    },
    /// Compaction was cancelled.
    Cancelled {
        /// Groups completed before cancellation.
        groups_completed: u32,
    },
}

/// Callback type for progress events.
pub type ProgressCallback = Arc<dyn Fn(CompactionProgressEvent) + Send + Sync>;

/// Thread-safe progress tracker for compaction operations.
#[derive(Debug)]
pub struct CompactionProgressTracker {
    total_groups: u32,
    total_bytes: u64,
    completed_groups: AtomicU32,
    failed_groups: AtomicU32,
    bytes_processed: AtomicU64,
}

impl CompactionProgressTracker {
    /// Create a new progress tracker.
    pub fn new(total_groups: u32, total_bytes: u64) -> Self {
        Self {
            total_groups,
            total_bytes,
            completed_groups: AtomicU32::new(0),
            failed_groups: AtomicU32::new(0),
            bytes_processed: AtomicU64::new(0),
        }
    }

    /// Get total number of groups.
    pub fn total_groups(&self) -> u32 {
        self.total_groups
    }

    /// Get total bytes to process.
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Get number of completed groups.
    pub fn completed_groups(&self) -> u32 {
        self.completed_groups.load(Ordering::Relaxed)
    }

    /// Get number of failed groups.
    pub fn failed_groups(&self) -> u32 {
        self.failed_groups.load(Ordering::Relaxed)
    }

    /// Get bytes processed so far.
    pub fn bytes_processed(&self) -> u64 {
        self.bytes_processed.load(Ordering::Relaxed)
    }

    /// Get progress as a percentage (0.0 - 1.0).
    pub fn progress_fraction(&self) -> f64 {
        if self.total_groups == 0 {
            return 1.0;
        }
        let completed = self.completed_groups() + self.failed_groups();
        completed as f64 / self.total_groups as f64
    }

    /// Check if all groups are processed.
    pub fn is_complete(&self) -> bool {
        self.completed_groups() + self.failed_groups() >= self.total_groups
    }

    /// Record a group completion.
    pub fn record_completed(&self, bytes: u64) {
        self.completed_groups.fetch_add(1, Ordering::Relaxed);
        self.bytes_processed.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record a group failure.
    pub fn record_failed(&self) {
        self.failed_groups.fetch_add(1, Ordering::Relaxed);
    }
}

impl Default for CompactionProgressTracker {
    fn default() -> Self {
        Self::new(0, 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_progress_event_creation() {
        let event = CompactionProgressEvent::GroupStarted {
            group_id: 0,
            input_files: 5,
            input_bytes: 1024 * 1024,
        };

        match event {
            CompactionProgressEvent::GroupStarted { group_id, .. } => {
                assert_eq!(group_id, 0);
            }
            _ => panic!("Wrong event type"),
        }
    }

    #[test]
    fn test_progress_tracker_updates() {
        let tracker = CompactionProgressTracker::new(10, 1024 * 1024 * 100);

        assert_eq!(tracker.total_groups(), 10);
        assert_eq!(tracker.completed_groups(), 0);
        assert!(!tracker.is_complete());
    }

    #[test]
    fn test_progress_tracker_record_completed() {
        let tracker = CompactionProgressTracker::new(3, 300);

        tracker.record_completed(100);
        assert_eq!(tracker.completed_groups(), 1);
        assert_eq!(tracker.bytes_processed(), 100);
        assert!(!tracker.is_complete());

        tracker.record_completed(100);
        tracker.record_completed(100);
        assert_eq!(tracker.completed_groups(), 3);
        assert!(tracker.is_complete());
    }

    #[test]
    fn test_progress_tracker_record_failed() {
        let tracker = CompactionProgressTracker::new(2, 200);

        tracker.record_completed(100);
        tracker.record_failed();

        assert_eq!(tracker.completed_groups(), 1);
        assert_eq!(tracker.failed_groups(), 1);
        assert!(tracker.is_complete());
    }

    #[test]
    fn test_progress_fraction() {
        let tracker = CompactionProgressTracker::new(4, 400);

        assert_eq!(tracker.progress_fraction(), 0.0);

        tracker.record_completed(100);
        assert_eq!(tracker.progress_fraction(), 0.25);

        tracker.record_failed();
        assert_eq!(tracker.progress_fraction(), 0.5);
    }

    #[test]
    fn test_progress_fraction_empty() {
        let tracker = CompactionProgressTracker::new(0, 0);
        assert_eq!(tracker.progress_fraction(), 1.0);
    }
}
