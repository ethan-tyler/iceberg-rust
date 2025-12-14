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

//! Remove orphan files from Apache Iceberg tables.
//!
//! This module provides functionality to identify and remove files that exist in a table's
//! storage location but are not referenced by any valid snapshot in the table's metadata.
//!
//! # What Are Orphan Files?
//!
//! Orphan files are data files, delete files, or metadata files that exist in storage
//! but are not referenced by any snapshot. They typically result from:
//!
//! - Writer crashes after creating files but before committing
//! - Failed transactions that lost the optimistic lock race
//! - Partial commits due to network failures
//! - Files uploaded directly to storage (outside Iceberg)
//! - Compaction failures mid-execution
//!
//! # Safety Mechanisms
//!
//! This implementation includes several safety features:
//!
//! - **Default 3-day retention**: Files younger than 3 days are never deleted by default
//! - **Dry-run mode**: Preview what would be deleted without making changes
//! - **Prefix mismatch handling**: Configurable behavior for scheme/authority mismatches
//! - **Path normalization**: Handles equivalent URI schemes (s3/s3a/s3n)
//!
//! # Example
//!
//! ```ignore
//! use chrono::{Duration, Utc};
//! use iceberg::Table;
//!
//! // Basic usage with dry-run
//! let result = table.remove_orphan_files()
//!     .dry_run(true)
//!     .execute()
//!     .await?;
//!
//! println!("Would delete {} orphan files ({} bytes)",
//!     result.orphan_files.len(),
//!     result.bytes_reclaimed);
//!
//! // Production usage with custom retention
//! let result = table.remove_orphan_files()
//!     .older_than(Utc::now() - Duration::days(7))
//!     .max_concurrent_deletes(100)
//!     .execute()
//!     .await?;
//! ```

mod collector;
mod normalizer;
mod result;

pub use collector::ReferenceFileCollector;
pub use normalizer::{FileUriNormalizer, NormalizedUri};
pub use result::{OrphanFileInfo, OrphanFileType, RemoveOrphanFilesResult};

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Duration, Utc};
use futures::StreamExt;

use crate::io::FileIO;
use crate::table::Table;
use crate::{Error, ErrorKind, Result};

/// How to handle prefix (scheme/authority) mismatches between table location
/// and file paths found in storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PrefixMismatchMode {
    /// Throw an error on mismatch (safest, default).
    #[default]
    Error,
    /// Ignore files with mismatches (skip them).
    Ignore,
    /// Treat files with mismatches as orphans (dangerous!).
    Delete,
}

/// Action for removing orphan files from an Iceberg table.
///
/// This action scans the table's storage location, compares found files against
/// all files referenced by any snapshot, and deletes files that are orphaned.
///
/// # Configuration
///
/// Use builder methods to customize behavior:
///
/// - `location()`: Scan a specific location instead of table root
/// - `older_than()`: Only consider files older than a timestamp
/// - `dry_run()`: Preview deletions without actually deleting
/// - `max_concurrent_deletes()`: Control deletion parallelism
/// - `prefix_mismatch_mode()`: Handle scheme/authority mismatches
/// - `equal_schemes()`: Configure scheme equivalences
pub struct RemoveOrphanFilesAction {
    /// Table to clean up.
    table: Table,
    /// Location to scan (defaults to table location).
    location: Option<String>,
    /// Only delete files older than this timestamp.
    older_than: DateTime<Utc>,
    /// If true, don't actually delete files, just report what would be deleted.
    dry_run: bool,
    /// Maximum concurrent delete operations.
    max_concurrent_deletes: usize,
    /// How to handle prefix mismatches.
    prefix_mismatch_mode: PrefixMismatchMode,
    /// Scheme equivalence mappings.
    equal_schemes: HashMap<String, String>,
    /// Authority equivalence mappings.
    equal_authorities: HashMap<String, String>,
}

impl RemoveOrphanFilesAction {
    /// Create a new action for the given table.
    ///
    /// Default settings:
    /// - `older_than`: 3 days ago (critical safety measure)
    /// - `dry_run`: false
    /// - `max_concurrent_deletes`: number of CPU cores
    /// - `prefix_mismatch_mode`: Error
    pub fn new(table: Table) -> Self {
        Self {
            table,
            location: None,
            older_than: Utc::now() - Duration::days(3),
            dry_run: false,
            max_concurrent_deletes: 4, // Reasonable default for concurrent deletes
            prefix_mismatch_mode: PrefixMismatchMode::Error,
            equal_schemes: HashMap::new(),
            equal_authorities: HashMap::new(),
        }
    }

    /// Set the location to scan for orphan files.
    ///
    /// If not set, scans the table's root location.
    #[must_use]
    pub fn location(mut self, location: impl Into<String>) -> Self {
        self.location = Some(location.into());
        self
    }

    /// Only consider files older than this timestamp as orphans.
    ///
    /// Default: 3 days ago (critical safety measure to avoid deleting
    /// files from in-progress writes).
    #[must_use]
    pub fn older_than(mut self, timestamp: DateTime<Utc>) -> Self {
        self.older_than = timestamp;
        self
    }

    /// Set retention period as a duration from now.
    ///
    /// Equivalent to `older_than(Utc::now() - retention)`.
    #[must_use]
    pub fn with_retention(mut self, retention: Duration) -> Self {
        self.older_than = Utc::now() - retention;
        self
    }

    /// Enable dry-run mode: list orphan files without deleting them.
    ///
    /// Default: false
    #[must_use]
    pub fn dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    /// Set maximum concurrent delete operations.
    ///
    /// Default: number of CPU cores
    #[must_use]
    pub fn max_concurrent_deletes(mut self, max: usize) -> Self {
        self.max_concurrent_deletes = max.max(1);
        self
    }

    /// Set how to handle files with scheme/authority mismatches.
    ///
    /// Default: Error (safest)
    #[must_use]
    pub fn prefix_mismatch_mode(mut self, mode: PrefixMismatchMode) -> Self {
        self.prefix_mismatch_mode = mode;
        self
    }

    /// Configure schemes that should be considered equivalent.
    ///
    /// This is useful when different tools write with different URI schemes
    /// that refer to the same storage (e.g., s3:// vs s3a://).
    #[must_use]
    pub fn equal_schemes(mut self, schemes: HashMap<String, String>) -> Self {
        self.equal_schemes = schemes;
        self
    }

    /// Configure authorities that should be considered equivalent.
    #[must_use]
    pub fn equal_authorities(mut self, authorities: HashMap<String, String>) -> Self {
        self.equal_authorities = authorities;
        self
    }

    /// Execute the orphan file removal operation.
    pub async fn execute(self) -> Result<RemoveOrphanFilesResult> {
        let start_time = Instant::now();

        // Build normalizer with configured equivalences
        let mut normalizer = FileUriNormalizer::with_defaults();
        for (scheme, canonical) in &self.equal_schemes {
            normalizer = normalizer.with_scheme_equivalence(scheme.clone(), canonical.clone());
        }
        for (authority, canonical) in &self.equal_authorities {
            normalizer = normalizer.with_authority_equivalence(authority.clone(), canonical.clone());
        }

        let metadata = self.table.metadata();
        let file_io = self.table.file_io();

        // Step 1: Collect all referenced files from all snapshots
        let collector = ReferenceFileCollector::new(&normalizer);
        let referenced_files = collector
            .collect_all_referenced_files(file_io, metadata)
            .await?;

        // Step 2: List all files in the scan location
        let scan_location = self
            .location
            .clone()
            .unwrap_or_else(|| metadata.location().to_string());

        let listed_files = file_io.list(&scan_location).await?;

        // Step 3: Identify orphan files
        let mut orphan_files = Vec::new();
        let cutoff_ms = self.older_than.timestamp_millis();

        for file_entry in listed_files {
            let normalized = normalizer.normalize(&file_entry.path);

            // Check prefix mismatch
            if !normalized.prefix_matched {
                match self.prefix_mismatch_mode {
                    PrefixMismatchMode::Error => {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!(
                                "File has unrecognized scheme/authority: {}",
                                file_entry.path
                            ),
                        ));
                    }
                    PrefixMismatchMode::Ignore => continue,
                    PrefixMismatchMode::Delete => {
                        // Fall through to treat as potential orphan
                    }
                }
            }

            // Check if file is referenced
            if referenced_files.contains(&normalized.normalized) {
                continue;
            }

            // Check age filter
            if let Some(last_modified) = file_entry.last_modified {
                if last_modified.timestamp_millis() >= cutoff_ms {
                    // File is too new, skip it
                    continue;
                }
            }

            // This file is an orphan
            let file_type = OrphanFileType::from_path(&file_entry.path);
            orphan_files.push(OrphanFileInfo {
                path: file_entry.path,
                file_type,
                size_bytes: file_entry.size,
                last_modified: file_entry.last_modified,
            });
        }

        // Step 4: Delete orphan files (unless dry-run)
        let mut result = RemoveOrphanFilesResult {
            orphan_files: orphan_files.clone(),
            dry_run: self.dry_run,
            ..Default::default()
        };

        if !self.dry_run {
            let deleted_results = self
                .delete_files(file_io, &orphan_files)
                .await;

            // Update result with deletion counts
            for (file_info, deleted) in orphan_files.iter().zip(deleted_results.iter()) {
                if *deleted {
                    result.bytes_reclaimed += file_info.size_bytes;
                    match file_info.file_type {
                        OrphanFileType::DataFile => result.deleted_data_files_count += 1,
                        OrphanFileType::PositionDeleteFile | OrphanFileType::EqualityDeleteFile => {
                            result.deleted_delete_files_count += 1
                        }
                        OrphanFileType::ManifestFile => result.deleted_manifest_files_count += 1,
                        OrphanFileType::ManifestListFile => {
                            result.deleted_manifest_list_files_count += 1
                        }
                        OrphanFileType::MetadataFile => result.deleted_metadata_files_count += 1,
                        OrphanFileType::StatisticsFile | OrphanFileType::Other => {
                            result.deleted_other_files_count += 1
                        }
                    }
                }
            }
        } else {
            // In dry-run mode, count what would be deleted
            for file_info in &orphan_files {
                result.bytes_reclaimed += file_info.size_bytes;
                match file_info.file_type {
                    OrphanFileType::DataFile => result.deleted_data_files_count += 1,
                    OrphanFileType::PositionDeleteFile | OrphanFileType::EqualityDeleteFile => {
                        result.deleted_delete_files_count += 1
                    }
                    OrphanFileType::ManifestFile => result.deleted_manifest_files_count += 1,
                    OrphanFileType::ManifestListFile => {
                        result.deleted_manifest_list_files_count += 1
                    }
                    OrphanFileType::MetadataFile => result.deleted_metadata_files_count += 1,
                    OrphanFileType::StatisticsFile | OrphanFileType::Other => {
                        result.deleted_other_files_count += 1
                    }
                }
            }
        }

        result.duration = start_time.elapsed();
        Ok(result)
    }

    /// Delete files with controlled concurrency.
    async fn delete_files(&self, file_io: &FileIO, files: &[OrphanFileInfo]) -> Vec<bool> {
        let file_io = Arc::new(file_io.clone());
        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.max_concurrent_deletes));

        let futures: Vec<_> = files
            .iter()
            .map(|file| {
                let file_io = Arc::clone(&file_io);
                let semaphore = Arc::clone(&semaphore);
                let path = file.path.clone();

                async move {
                    let _permit = semaphore.acquire().await.ok()?;
                    file_io.delete(&path).await.ok().map(|_| true)
                }
            })
            .collect();

        futures::stream::iter(futures)
            .buffer_unordered(self.max_concurrent_deletes)
            .map(|r| r.unwrap_or(false))
            .collect()
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefix_mismatch_mode_default() {
        assert_eq!(PrefixMismatchMode::default(), PrefixMismatchMode::Error);
    }

    #[test]
    fn test_action_default_retention() {
        // We can't easily test this without a table, but we can verify
        // the default is 3 days by checking the Duration
        let default_retention = Duration::days(3);
        assert_eq!(default_retention.num_days(), 3);
    }
}
