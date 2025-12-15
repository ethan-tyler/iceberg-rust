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

//! Reference file collector for gathering all files referenced by table snapshots.
//!
//! This module provides functionality to collect all files that are referenced by
//! any snapshot in a table. Files not in this set are candidates for orphan removal.

use std::collections::HashSet;

use super::normalizer::FileUriNormalizer;
use crate::Result;
use crate::io::FileIO;
use crate::spec::TableMetadata;

/// Collects all files referenced by a table's snapshots.
///
/// This is used to build the "live file set" - all files that should NOT be
/// deleted because they are referenced by at least one snapshot.
#[derive(Debug)]
pub struct ReferenceFileCollector<'a> {
    normalizer: &'a FileUriNormalizer,
}

impl<'a> ReferenceFileCollector<'a> {
    /// Create a new collector with the given URI normalizer.
    pub fn new(normalizer: &'a FileUriNormalizer) -> Self {
        Self { normalizer }
    }

    /// Collect all files referenced by all snapshots in the table.
    ///
    /// This includes:
    /// - Manifest list files from each snapshot
    /// - Manifest files referenced by manifest lists
    /// - Data files and delete files referenced by manifests
    /// - Metadata files from the metadata log
    /// - Statistics files
    ///
    /// All paths are normalized using the configured [`FileUriNormalizer`].
    pub async fn collect_all_referenced_files(
        &self,
        file_io: &FileIO,
        metadata: &TableMetadata,
    ) -> Result<HashSet<String>> {
        let mut referenced = HashSet::new();

        // Collect from all snapshots
        for snapshot in metadata.snapshots() {
            self.collect_snapshot_files(file_io, metadata, snapshot, &mut referenced)
                .await?;
        }

        // Also include metadata files from metadata log
        for log_entry in metadata.metadata_log() {
            let normalized = self
                .normalizer
                .normalize(&log_entry.metadata_file)
                .normalized;
            referenced.insert(normalized);
        }

        // Include statistics files
        for stats_file in metadata.statistics_iter() {
            let normalized = self
                .normalizer
                .normalize(&stats_file.statistics_path)
                .normalized;
            referenced.insert(normalized);
        }

        // Include partition statistics files
        for stats_file in metadata.partition_statistics_iter() {
            let normalized = self
                .normalizer
                .normalize(&stats_file.statistics_path)
                .normalized;
            referenced.insert(normalized);
        }

        Ok(referenced)
    }

    /// Collect files referenced by a single snapshot.
    async fn collect_snapshot_files(
        &self,
        file_io: &FileIO,
        metadata: &TableMetadata,
        snapshot: &crate::spec::Snapshot,
        referenced: &mut HashSet<String>,
    ) -> Result<()> {
        // Add manifest list file
        let manifest_list_path = snapshot.manifest_list();
        let normalized = self.normalizer.normalize(manifest_list_path).normalized;
        referenced.insert(normalized);

        // Load manifest list and process each manifest
        let manifest_list = snapshot.load_manifest_list(file_io, metadata).await?;

        for manifest_file in manifest_list.entries() {
            // Add manifest file itself
            let manifest_normalized = self
                .normalizer
                .normalize(&manifest_file.manifest_path)
                .normalized;
            referenced.insert(manifest_normalized);

            // Load manifest to get data/delete files
            let manifest = manifest_file.load_manifest(file_io).await?;

            for entry in manifest.entries() {
                // Add all data/delete files regardless of status
                // (ADDED, EXISTING, or DELETED - all are referenced by this snapshot)
                let file_normalized = self.normalizer.normalize(entry.file_path()).normalized;
                referenced.insert(file_normalized);
            }
        }

        Ok(())
    }

    /// Collect only manifest list files from all snapshots.
    ///
    /// This is a lighter-weight operation that doesn't load manifests.
    pub fn collect_manifest_list_files(&self, metadata: &TableMetadata) -> HashSet<String> {
        metadata
            .snapshots()
            .map(|s| self.normalizer.normalize(s.manifest_list()).normalized)
            .collect()
    }

    /// Collect manifest files from all snapshots.
    ///
    /// This requires loading manifest lists but not the manifests themselves.
    pub async fn collect_manifest_files(
        &self,
        file_io: &FileIO,
        metadata: &TableMetadata,
    ) -> Result<HashSet<String>> {
        let mut manifests = HashSet::new();

        for snapshot in metadata.snapshots() {
            let manifest_list = snapshot.load_manifest_list(file_io, metadata).await?;
            for manifest_file in manifest_list.entries() {
                let normalized = self
                    .normalizer
                    .normalize(&manifest_file.manifest_path)
                    .normalized;
                manifests.insert(normalized);
            }
        }

        Ok(manifests)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collector_creation() {
        let normalizer = FileUriNormalizer::with_defaults();
        let _collector = ReferenceFileCollector::new(&normalizer);
    }
}
