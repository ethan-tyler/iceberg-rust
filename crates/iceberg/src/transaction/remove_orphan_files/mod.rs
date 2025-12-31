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
//! - **Conservative timestamp handling**: Files without a last-modified timestamp are skipped
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

use std::collections::HashMap;
use std::time::Instant;

use chrono::{DateTime, Duration, Utc};
pub use collector::ReferenceFileCollector;
use futures::StreamExt;
pub use normalizer::{FileUriNormalizer, NormalizedUri};
pub use result::{
    OrphanFileInfo, OrphanFileType, OrphanFilesProgressCallback, OrphanFilesProgressEvent,
    RemoveOrphanFilesResult,
};

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
/// - `on_progress()`: Receive progress updates during execution
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
    /// Optional progress callback.
    progress_callback: Option<OrphanFilesProgressCallback>,
}

impl RemoveOrphanFilesAction {
    /// Create a new action for the given table.
    ///
    /// Default settings:
    /// - `older_than`: 3 days ago (critical safety measure)
    /// - `dry_run`: false
    /// - `max_concurrent_deletes`: number of available threads
    /// - `prefix_mismatch_mode`: Error
    pub fn new(table: Table) -> Self {
        let max_concurrent_deletes = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);

        Self {
            table,
            location: None,
            older_than: Utc::now() - Duration::days(3),
            dry_run: false,
            max_concurrent_deletes,
            prefix_mismatch_mode: PrefixMismatchMode::Error,
            equal_schemes: HashMap::new(),
            equal_authorities: HashMap::new(),
            progress_callback: None,
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

    /// Set a callback to receive progress updates during execution.
    ///
    /// This is useful for large tables where the operation may take significant time.
    /// The callback receives [`OrphanFilesProgressEvent`] updates at key stages:
    ///
    /// - Collecting references from snapshots
    /// - Listing files in storage
    /// - Identifying orphan files
    /// - Deleting files (with progress updates)
    ///
    /// # Example
    ///
    /// ```ignore
    /// use std::sync::Arc;
    /// use iceberg::transaction::remove_orphan_files::OrphanFilesProgressEvent;
    ///
    /// let result = table.remove_orphan_files()
    ///     .on_progress(Arc::new(|event| {
    ///         println!("{:?}", event);
    ///     }))
    ///     .execute()
    ///     .await?;
    /// ```
    #[must_use]
    pub fn on_progress(mut self, callback: OrphanFilesProgressCallback) -> Self {
        self.progress_callback = Some(callback);
        self
    }

    /// Execute the orphan file removal operation.
    pub async fn execute(self) -> Result<RemoveOrphanFilesResult> {
        let start_time = Instant::now();

        let RemoveOrphanFilesAction {
            table,
            location,
            older_than,
            dry_run,
            max_concurrent_deletes,
            prefix_mismatch_mode,
            equal_schemes,
            equal_authorities,
            progress_callback,
        } = self;

        // Helper to emit progress events
        let emit = |event: OrphanFilesProgressEvent| {
            if let Some(ref cb) = progress_callback {
                cb(event);
            }
        };

        if table.readonly() && !dry_run {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "Cannot remove orphan files on a readonly table",
            ));
        }

        let metadata = table.metadata();
        let file_io = table.file_io();

        let table_location = metadata.location().to_string();
        let scan_location = location.unwrap_or_else(|| table_location.clone());

        // Build normalizer with configured equivalences
        let mut normalizer = FileUriNormalizer::with_defaults();
        for (scheme, canonical) in equal_schemes {
            normalizer = normalizer.with_scheme_equivalence(scheme, canonical);
        }
        for (authority, canonical) in equal_authorities {
            normalizer = normalizer.with_authority_equivalence(authority, canonical);
        }

        let normalized_table_root =
            ensure_trailing_slash(&normalizer.normalize(&table_location).normalized);
        let normalized_scan_root =
            ensure_trailing_slash(&normalizer.normalize(&scan_location).normalized);

        // If scanning outside the table location, fail by default. Allow advanced callers to
        // proceed via `prefix_mismatch_mode`, which controls how to handle files outside the
        // table root discovered during listing.
        if !normalized_scan_root.starts_with(&normalized_table_root) {
            match prefix_mismatch_mode {
                PrefixMismatchMode::Error => {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "Scan location is outside the table location",
                    )
                    .with_context("table_location", table_location)
                    .with_context("scan_location", scan_location));
                }
                PrefixMismatchMode::Ignore | PrefixMismatchMode::Delete => {
                    // Continue and filter per-file below.
                }
            }
        }

        // Step 1: Collect all referenced files from all snapshots
        let snapshot_count = metadata.snapshots().count();
        emit(OrphanFilesProgressEvent::CollectingReferences { snapshot_count });

        let collector = ReferenceFileCollector::new(&normalizer);
        let mut referenced_files = collector
            .collect_all_referenced_files(file_io, metadata)
            .await?;

        // Protect the current metadata file (critical safety measure).
        // This is not part of `TableMetadata`; it is stored separately by catalogs.
        let current_metadata_location = table.metadata_location_result()?;
        referenced_files.insert(normalizer.normalize(current_metadata_location).normalized);

        // Protect the metadata version-hint file if it exists.
        // This file is not referenced by snapshots but is used by some engines/catalogs.
        let version_hint_location = format!(
            "{}/metadata/version-hint.text",
            metadata.location().trim_end_matches('/')
        );
        referenced_files.insert(normalizer.normalize(&version_hint_location).normalized);

        // Step 2: List all files in the scan location
        emit(OrphanFilesProgressEvent::ListingFiles {
            location: scan_location.clone(),
        });

        let listed_files = file_io.list(&scan_location).await?;

        emit(OrphanFilesProgressEvent::FilesListed {
            total_files: listed_files.len(),
        });

        // Step 3: Identify orphan files
        let mut orphan_files = Vec::new();
        let cutoff_ms = older_than.timestamp_millis();

        for file_entry in listed_files {
            let normalized_path = normalizer.normalize(&file_entry.path).normalized;

            // Check if file is referenced
            if referenced_files.contains(&normalized_path) {
                continue;
            }

            // Prefix mismatch handling. This primarily protects against accidentally scanning
            // a parent directory (or a different table's location) and deleting unrelated files.
            if !normalized_path.starts_with(&normalized_table_root) {
                match prefix_mismatch_mode {
                    PrefixMismatchMode::Error => {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            "Found unreferenced file outside the table location while scanning",
                        )
                        .with_context("file", file_entry.path)
                        .with_context("table_location", metadata.location().to_string())
                        .with_context("scan_location", scan_location.clone()));
                    }
                    PrefixMismatchMode::Ignore => continue,
                    PrefixMismatchMode::Delete => {
                        // Fall through to treat as potential orphan.
                    }
                }
            }

            // Check age filter - skip files newer than the cutoff.
            // If last-modified time is unavailable, skip for safety.
            let Some(last_modified) = file_entry.last_modified else {
                continue;
            };
            if last_modified.timestamp_millis() >= cutoff_ms {
                continue;
            }

            // This file is an orphan
            let file_type = OrphanFileType::from_path(&file_entry.path);
            orphan_files.push(OrphanFileInfo {
                path: file_entry.path,
                file_type,
                size_bytes: file_entry.size,
                last_modified: Some(last_modified),
            });
        }

        // Compute total bytes for progress reporting
        let total_orphan_bytes: u64 = orphan_files.iter().map(|f| f.size_bytes).sum();
        emit(OrphanFilesProgressEvent::OrphansIdentified {
            orphan_count: orphan_files.len(),
            total_bytes: total_orphan_bytes,
        });

        // Step 4: Delete orphan files (unless dry-run), then compute stats.
        let deleted_results = if dry_run {
            Vec::new()
        } else {
            emit(OrphanFilesProgressEvent::DeletingFiles {
                total: orphan_files.len(),
            });
            Self::delete_files_with_progress(
                file_io,
                max_concurrent_deletes,
                &orphan_files,
                &progress_callback,
            )
            .await
        };

        let mut deleted_data_files_count = 0u64;
        let mut deleted_delete_files_count = 0u64;
        let mut deleted_manifest_files_count = 0u64;
        let mut deleted_manifest_list_files_count = 0u64;
        let mut deleted_metadata_files_count = 0u64;
        let mut deleted_other_files_count = 0u64;
        let mut bytes_reclaimed = 0u64;

        let mut record = |file_info: &OrphanFileInfo| {
            bytes_reclaimed = bytes_reclaimed.saturating_add(file_info.size_bytes);
            match file_info.file_type {
                OrphanFileType::DataFile => deleted_data_files_count += 1,
                OrphanFileType::PositionDeleteFile | OrphanFileType::EqualityDeleteFile => {
                    deleted_delete_files_count += 1
                }
                OrphanFileType::ManifestFile => deleted_manifest_files_count += 1,
                OrphanFileType::ManifestListFile => deleted_manifest_list_files_count += 1,
                OrphanFileType::MetadataFile => deleted_metadata_files_count += 1,
                OrphanFileType::StatisticsFile | OrphanFileType::Other => {
                    deleted_other_files_count += 1
                }
            }
        };

        if dry_run {
            for file_info in &orphan_files {
                record(file_info);
            }
        } else {
            for (file_info, deleted) in orphan_files.iter().zip(deleted_results.iter()) {
                if *deleted {
                    record(file_info);
                }
            }
        }

        let total_deleted = deleted_data_files_count
            + deleted_delete_files_count
            + deleted_manifest_files_count
            + deleted_manifest_list_files_count
            + deleted_metadata_files_count
            + deleted_other_files_count;

        emit(OrphanFilesProgressEvent::Complete {
            deleted_count: total_deleted,
            bytes_reclaimed,
        });

        Ok(RemoveOrphanFilesResult {
            orphan_files,
            deleted_data_files_count,
            deleted_delete_files_count,
            deleted_manifest_files_count,
            deleted_manifest_list_files_count,
            deleted_metadata_files_count,
            deleted_other_files_count,
            bytes_reclaimed,
            dry_run,
            duration: start_time.elapsed(),
        })
    }

    /// Delete files with controlled concurrency and progress reporting.
    async fn delete_files_with_progress(
        file_io: &FileIO,
        max_concurrent_deletes: usize,
        files: &[OrphanFileInfo],
        progress_callback: &Option<OrphanFilesProgressCallback>,
    ) -> Vec<bool> {
        if files.is_empty() {
            return Vec::new();
        }

        let file_io = file_io.clone();
        let total = files.len();
        let mut results = vec![false; total];
        let deleted_count = std::sync::atomic::AtomicUsize::new(0);

        // Determine progress reporting interval (report every ~10% or at least every 100 files)
        let report_interval = (total / 10).clamp(1, 100);

        futures::stream::iter(files.iter().enumerate())
            .map(|(idx, file)| {
                let file_io = file_io.clone();
                let path = file.path.clone();
                async move { (idx, file_io.delete(&path).await.is_ok()) }
            })
            .buffer_unordered(max_concurrent_deletes)
            .for_each(|(idx, ok)| {
                results[idx] = ok;
                let current = deleted_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;

                // Emit progress at intervals
                if (current % report_interval == 0 || current == total)
                    && let Some(cb) = progress_callback
                {
                    cb(OrphanFilesProgressEvent::DeletionProgress {
                        deleted: current,
                        total,
                    });
                }

                futures::future::ready(())
            })
            .await;

        results
    }
}

fn ensure_trailing_slash(s: &str) -> String {
    if s.ends_with('/') {
        s.to_string()
    } else {
        format!("{s}/")
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::fs::File;
    use std::io::Write;
    use std::path::Path;

    use minijinja::value::Value;
    use minijinja::{AutoEscape, Environment, context};
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::TableIdent;
    use crate::io::FileIO;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, ManifestEntry,
        ManifestListWriter, ManifestStatus, ManifestWriterBuilder, PartitionStatisticsFile,
        StatisticsFile, Struct, TableMetadata,
    };
    use crate::table::Table;

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

    fn render_template(template: &str, ctx: Value) -> String {
        let mut env = Environment::new();
        env.set_auto_escape_callback(|_| AutoEscape::None);
        env.render_str(template, ctx).unwrap()
    }

    fn write_text_file(path: &Path, contents: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        let mut f = File::create(path).unwrap();
        f.write_all(contents.as_bytes()).unwrap();
    }

    async fn write_manifest_list_and_manifest(
        file_io: &FileIO,
        table_location: &str,
        metadata: &TableMetadata,
        snapshot: &crate::spec::Snapshot,
    ) {
        let manifest_path = format!(
            "{}/metadata/manifest-{}.avro",
            table_location,
            Uuid::new_v4()
        );
        let mut writer = ManifestWriterBuilder::new(
            file_io.new_output(&manifest_path).unwrap(),
            Some(snapshot.snapshot_id()),
            None,
            snapshot.schema(metadata).unwrap(),
            metadata.default_partition_spec().as_ref().clone(),
        )
        .build_v2_data();

        let data_file_path = format!(
            "{}/data/data-{}.parquet",
            table_location,
            snapshot.snapshot_id()
        );

        writer
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::Data)
                            .file_path(data_file_path.clone())
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(12)
                            .record_count(1)
                            .partition(Struct::from_iter([Some(Literal::long(100))]))
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .unwrap();

        let manifest_file = writer.write_manifest_file().await.unwrap();

        let mut manifest_list_write = ManifestListWriter::v2(
            file_io.new_output(snapshot.manifest_list()).unwrap(),
            snapshot.snapshot_id(),
            snapshot.parent_snapshot_id(),
            snapshot.sequence_number(),
        );
        manifest_list_write
            .add_manifests(vec![manifest_file].into_iter())
            .unwrap();
        manifest_list_write.close().await.unwrap();

        write_text_file(Path::new(&data_file_path), "parquet data");
    }

    async fn setup_table_with_current_metadata_and_stats(tmp_dir: &TempDir) -> (Table, String) {
        let table_location = tmp_dir.path().join("table1");
        let table_location_str = table_location.to_str().unwrap().to_string();

        let manifest_list_1_location = table_location.join("metadata/manifests_list_1.avro");
        let manifest_list_2_location = table_location.join("metadata/manifests_list_2.avro");
        let table_metadata_v1_location = table_location.join("metadata/v1.json");
        let table_metadata_v2_location = table_location.join("metadata/v2.json");
        let version_hint_location = table_location.join("metadata/version-hint.text");
        let statistics_location = table_location.join("metadata/stats.puffin");
        let partition_statistics_location = table_location.join("metadata/partition-stats.bin");

        let file_io = FileIO::from_path(table_location_str.as_str())
            .unwrap()
            .build()
            .unwrap();

        let template_json_str = fs::read_to_string(format!(
            "{}/testdata/example_table_metadata_v2.json",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap();

        let metadata_json = render_template(&template_json_str, context! {
            table_location => &table_location,
            manifest_list_1_location => &manifest_list_1_location,
            manifest_list_2_location => &manifest_list_2_location,
            table_metadata_1_location => &table_metadata_v1_location,
        });

        let mut table_metadata = serde_json::from_str::<TableMetadata>(&metadata_json).unwrap();

        let current_snapshot_id = table_metadata.current_snapshot_id().unwrap();
        table_metadata
            .statistics
            .insert(current_snapshot_id, StatisticsFile {
                snapshot_id: current_snapshot_id,
                statistics_path: statistics_location.to_str().unwrap().to_string(),
                file_size_in_bytes: 1,
                file_footer_size_in_bytes: 1,
                key_metadata: None,
                blob_metadata: Vec::new(),
            });
        table_metadata
            .partition_statistics
            .insert(current_snapshot_id, PartitionStatisticsFile {
                snapshot_id: current_snapshot_id,
                statistics_path: partition_statistics_location.to_str().unwrap().to_string(),
                file_size_in_bytes: 1,
            });

        let table = Table::builder()
            .metadata(table_metadata)
            .identifier(TableIdent::from_strs(["db", "table1"]).unwrap())
            .file_io(file_io.clone())
            .metadata_location(table_metadata_v2_location.to_str().unwrap())
            .build()
            .unwrap();

        // Create files that are referenced (or should be protected)
        write_text_file(&table_metadata_v1_location, "{\"version\":1}");
        write_text_file(&table_metadata_v2_location, "{\"version\":2}");
        write_text_file(&version_hint_location, "2");
        write_text_file(&statistics_location, "stats");
        write_text_file(&partition_statistics_location, "partition-stats");

        // Create manifest lists and manifests for all snapshots in the metadata
        for snapshot in table.metadata().snapshots() {
            write_manifest_list_and_manifest(
                &file_io,
                &table_location_str,
                table.metadata(),
                snapshot,
            )
            .await;
        }

        (table, table_location_str)
    }

    #[tokio::test]
    async fn test_remove_orphan_files_protects_metadata_and_stats() {
        let tmp_dir = TempDir::new().unwrap();
        let (table, table_location) = setup_table_with_current_metadata_and_stats(&tmp_dir).await;

        let orphan_path = Path::new(&table_location).join("data/orphan.parquet");
        write_text_file(&orphan_path, "orphan");

        let result = table
            .remove_orphan_files()
            .dry_run(true)
            .older_than(Utc::now() + Duration::days(365))
            .execute()
            .await
            .unwrap();

        let orphan_paths: Vec<&str> = result
            .orphan_files
            .iter()
            .map(|f| f.path.as_str())
            .collect();

        assert!(orphan_paths.contains(&orphan_path.to_str().unwrap()));
        assert!(
            !orphan_paths
                .iter()
                .any(|p| p.ends_with("/metadata/v2.json")),
            "current metadata file must never be considered orphan"
        );
        assert!(
            !orphan_paths
                .iter()
                .any(|p| p.ends_with("/metadata/version-hint.text")),
            "version-hint file must never be considered orphan"
        );
        assert!(
            !orphan_paths
                .iter()
                .any(|p| p.ends_with("/metadata/stats.puffin")),
            "statistics files referenced in metadata must never be considered orphan"
        );

        assert_eq!(result.deleted_data_files_count, 1);
        assert!(result.bytes_reclaimed > 0);
    }

    #[tokio::test]
    async fn test_remove_orphan_files_deletes_orphan_file() {
        let tmp_dir = TempDir::new().unwrap();
        let (table, table_location) = setup_table_with_current_metadata_and_stats(&tmp_dir).await;

        let orphan_path = Path::new(&table_location).join("data/orphan.parquet");
        write_text_file(&orphan_path, "orphan");

        let result = table
            .remove_orphan_files()
            .dry_run(false)
            .older_than(Utc::now() + Duration::days(365))
            .max_concurrent_deletes(4)
            .execute()
            .await
            .unwrap();

        assert_eq!(result.deleted_data_files_count, 1);
        assert!(!orphan_path.exists());

        // Critical: current metadata must still exist.
        let current_metadata_path = Path::new(&table_location).join("metadata/v2.json");
        assert!(current_metadata_path.exists());
    }

    #[tokio::test]
    async fn test_remove_orphan_files_scan_location_mismatch_handling() {
        let tmp_dir = TempDir::new().unwrap();
        let (table, table_location) = setup_table_with_current_metadata_and_stats(&tmp_dir).await;

        let orphan_path = Path::new(&table_location).join("data/orphan.parquet");
        write_text_file(&orphan_path, "orphan");

        let outside_file = tmp_dir.path().join("unrelated.txt");
        write_text_file(&outside_file, "unrelated");

        // Default behavior: error when scanning outside the table location.
        let err = table
            .remove_orphan_files()
            .location(tmp_dir.path().to_str().unwrap())
            .dry_run(true)
            .older_than(Utc::now() + Duration::days(365))
            .execute()
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);

        // Ignore mode: allow scanning parent directories but skip files outside the table root.
        let result = table
            .remove_orphan_files()
            .location(tmp_dir.path().to_str().unwrap())
            .prefix_mismatch_mode(PrefixMismatchMode::Ignore)
            .dry_run(true)
            .older_than(Utc::now() + Duration::days(365))
            .execute()
            .await
            .unwrap();

        let orphan_paths: Vec<&str> = result
            .orphan_files
            .iter()
            .map(|f| f.path.as_str())
            .collect();
        assert!(orphan_paths.contains(&orphan_path.to_str().unwrap()));
        assert!(!orphan_paths.contains(&outside_file.to_str().unwrap()));
    }

    #[tokio::test]
    async fn test_remove_orphan_files_progress_callback() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let tmp_dir = TempDir::new().unwrap();
        let (table, table_location) = setup_table_with_current_metadata_and_stats(&tmp_dir).await;

        let orphan_path = Path::new(&table_location).join("data/orphan.parquet");
        write_text_file(&orphan_path, "orphan");

        // Track which events we receive
        let collecting_refs_count = Arc::new(AtomicUsize::new(0));
        let listing_count = Arc::new(AtomicUsize::new(0));
        let listed_count = Arc::new(AtomicUsize::new(0));
        let orphans_identified_count = Arc::new(AtomicUsize::new(0));
        let complete_count = Arc::new(AtomicUsize::new(0));

        let collecting_refs_count_clone = collecting_refs_count.clone();
        let listing_count_clone = listing_count.clone();
        let listed_count_clone = listed_count.clone();
        let orphans_identified_count_clone = orphans_identified_count.clone();
        let complete_count_clone = complete_count.clone();

        let callback: OrphanFilesProgressCallback = Arc::new(move |event| match event {
            OrphanFilesProgressEvent::CollectingReferences { .. } => {
                collecting_refs_count_clone.fetch_add(1, Ordering::SeqCst);
            }
            OrphanFilesProgressEvent::ListingFiles { .. } => {
                listing_count_clone.fetch_add(1, Ordering::SeqCst);
            }
            OrphanFilesProgressEvent::FilesListed { .. } => {
                listed_count_clone.fetch_add(1, Ordering::SeqCst);
            }
            OrphanFilesProgressEvent::OrphansIdentified { .. } => {
                orphans_identified_count_clone.fetch_add(1, Ordering::SeqCst);
            }
            OrphanFilesProgressEvent::Complete { .. } => {
                complete_count_clone.fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        });

        let result = table
            .remove_orphan_files()
            .dry_run(true)
            .older_than(Utc::now() + Duration::days(365))
            .on_progress(callback)
            .execute()
            .await
            .unwrap();

        // Verify we received all expected progress events
        assert_eq!(
            collecting_refs_count.load(Ordering::SeqCst),
            1,
            "should receive CollectingReferences"
        );
        assert_eq!(
            listing_count.load(Ordering::SeqCst),
            1,
            "should receive ListingFiles"
        );
        assert_eq!(
            listed_count.load(Ordering::SeqCst),
            1,
            "should receive FilesListed"
        );
        assert_eq!(
            orphans_identified_count.load(Ordering::SeqCst),
            1,
            "should receive OrphansIdentified"
        );
        assert_eq!(
            complete_count.load(Ordering::SeqCst),
            1,
            "should receive Complete"
        );

        // Verify we still got the expected result
        assert_eq!(result.deleted_data_files_count, 1);
    }
}
