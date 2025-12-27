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

//! Table API for Apache Iceberg

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use crate::arrow::ArrowReaderBuilder;
use crate::inspect::MetadataTable;
use crate::io::FileIO;
use crate::io::object_cache::{CacheStats, ObjectCache, ObjectCacheConfig};
use crate::scan::{IncrementalScanBuilder, TableScanBuilder};
use crate::spec::{DataFile, SchemaRef, TableMetadata, TableMetadataRef};
use crate::transaction::{
    ApplyTransactionAction, CreateBranchAction, CreateTagAction, FastForwardAction,
    RemoveOrphanFilesAction, Transaction,
};
use crate::{Catalog, Error, ErrorKind, Result, TableIdent};

/// Builder to create table scan.
pub struct TableBuilder {
    file_io: Option<FileIO>,
    metadata_location: Option<String>,
    metadata: Option<TableMetadataRef>,
    identifier: Option<TableIdent>,
    readonly: bool,
    disable_cache: bool,
    cache_size_bytes: Option<u64>,
    /// Cache TTL configuration:
    /// - `None`: not set by user, use default TTL (5 minutes)
    /// - `Some(None)`: explicitly disable TTL (entries only expire via LRU)
    /// - `Some(Some(duration))`: use custom TTL duration
    cache_ttl: Option<Option<Duration>>,
}

impl TableBuilder {
    pub(crate) fn new() -> Self {
        Self {
            file_io: None,
            metadata_location: None,
            metadata: None,
            identifier: None,
            readonly: false,
            disable_cache: false,
            cache_size_bytes: None,
            cache_ttl: None,
        }
    }

    /// required - sets the necessary FileIO to use for the table
    pub fn file_io(mut self, file_io: FileIO) -> Self {
        self.file_io = Some(file_io);
        self
    }

    /// optional - sets the tables metadata location
    pub fn metadata_location<T: Into<String>>(mut self, metadata_location: T) -> Self {
        self.metadata_location = Some(metadata_location.into());
        self
    }

    /// required - passes in the TableMetadata to use for the Table
    pub fn metadata<T: Into<TableMetadataRef>>(mut self, metadata: T) -> Self {
        self.metadata = Some(metadata.into());
        self
    }

    /// required - passes in the TableIdent to use for the Table
    pub fn identifier(mut self, identifier: TableIdent) -> Self {
        self.identifier = Some(identifier);
        self
    }

    /// specifies if the Table is readonly or not (default not)
    pub fn readonly(mut self, readonly: bool) -> Self {
        self.readonly = readonly;
        self
    }

    /// specifies if the Table's metadata cache will be disabled,
    /// so that reads of Manifests and ManifestLists will never
    /// get cached.
    pub fn disable_cache(mut self) -> Self {
        self.disable_cache = true;
        self
    }

    /// Optionally set a non-default metadata cache size in bytes.
    ///
    /// Default is 32MB. Setting this to 0 disables caching (equivalent to `disable_cache()`).
    pub fn cache_size_bytes(mut self, cache_size_bytes: u64) -> Self {
        self.cache_size_bytes = Some(cache_size_bytes);
        self
    }

    /// Optionally set a time-to-live (TTL) for cache entries.
    ///
    /// When set, cache entries will automatically expire after this duration.
    /// This is useful for ensuring the cache doesn't hold stale data for too long.
    ///
    /// # Arguments
    ///
    /// * `ttl` - The TTL duration:
    ///   - `Some(duration)` - Cache entries expire after this duration
    ///   - `None` - Disable TTL (entries only expire via LRU eviction)
    ///
    /// Default is 5 minutes if this method is not called.
    pub fn cache_ttl(mut self, ttl: Option<Duration>) -> Self {
        self.cache_ttl = Some(ttl);
        self
    }

    /// build the Table
    pub fn build(self) -> Result<Table> {
        let Self {
            file_io,
            metadata_location,
            metadata,
            identifier,
            readonly,
            disable_cache,
            cache_size_bytes,
            cache_ttl,
        } = self;

        let Some(file_io) = file_io else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "FileIO must be provided with TableBuilder.file_io()",
            ));
        };

        let Some(metadata) = metadata else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "TableMetadataRef must be provided with TableBuilder.metadata()",
            ));
        };

        let Some(identifier) = identifier else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "TableIdent must be provided with TableBuilder.identifier()",
            ));
        };

        // Build cache config with precedence: builder overrides > table properties > defaults
        let base_config = ObjectCacheConfig::from_properties(metadata.properties());

        let object_cache_config = ObjectCacheConfig {
            // Builder's disable_cache() takes precedence; otherwise use table property/default
            enabled: if disable_cache {
                false
            } else {
                base_config.enabled
            },
            // Builder's cache_size_bytes() takes precedence if explicitly set
            max_total_bytes: cache_size_bytes.unwrap_or(base_config.max_total_bytes),
            // Builder's cache_ttl() takes precedence if explicitly set
            expiration_interval: cache_ttl.unwrap_or(base_config.expiration_interval),
        };

        let object_cache = Arc::new(ObjectCache::new_with_config(
            file_io.clone(),
            object_cache_config,
        ));

        Ok(Table {
            file_io,
            metadata_location,
            metadata,
            identifier,
            readonly,
            object_cache,
        })
    }
}

/// Table represents a table in the catalog.
#[derive(Debug, Clone)]
pub struct Table {
    file_io: FileIO,
    metadata_location: Option<String>,
    metadata: TableMetadataRef,
    identifier: TableIdent,
    readonly: bool,
    object_cache: Arc<ObjectCache>,
}

impl Table {
    /// Sets the [`Table`] metadata and returns an updated instance with the new metadata applied.
    ///
    /// This method invalidates the object cache to ensure stale manifest references
    /// are not retained. While Iceberg manifests are immutable by path (so cached
    /// data is always valid), clearing the cache on metadata update helps:
    /// - Free memory from manifests no longer referenced by any snapshot
    /// - Ensure subsequent scans start fresh with the new metadata
    pub(crate) fn with_metadata(mut self, metadata: TableMetadataRef) -> Self {
        // Invalidate cache to release references to potentially unreferenced manifests
        if !Arc::ptr_eq(&self.metadata, &metadata) {
            self.object_cache.invalidate_all();
        }
        self.metadata = metadata;
        self
    }

    /// Sets the [`Table`] metadata location and returns an updated instance.
    pub(crate) fn with_metadata_location(mut self, metadata_location: String) -> Self {
        self.metadata_location = Some(metadata_location);
        self
    }

    /// Returns a TableBuilder to build a table
    pub fn builder() -> TableBuilder {
        TableBuilder::new()
    }

    /// Returns table identifier.
    pub fn identifier(&self) -> &TableIdent {
        &self.identifier
    }
    /// Returns current metadata.
    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    /// Returns current metadata ref.
    pub fn metadata_ref(&self) -> TableMetadataRef {
        self.metadata.clone()
    }

    /// Returns current metadata location.
    pub fn metadata_location(&self) -> Option<&str> {
        self.metadata_location.as_deref()
    }

    /// Returns current metadata location in a result.
    pub fn metadata_location_result(&self) -> Result<&str> {
        self.metadata_location.as_deref().ok_or(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Metadata location does not exist for table: {}",
                self.identifier
            ),
        ))
    }

    /// Returns file io used in this table.
    pub fn file_io(&self) -> &FileIO {
        &self.file_io
    }

    /// Returns this table's object cache
    pub(crate) fn object_cache(&self) -> Arc<ObjectCache> {
        self.object_cache.clone()
    }

    /// Invalidates (clears) the manifest cache.
    ///
    /// This method clears all cached ManifestList and Manifest objects.
    /// Use this when you need to force re-reading manifests from storage,
    /// for example:
    /// - After external modifications to the table
    /// - To free memory when the table won't be scanned for a while
    /// - When debugging cache-related issues
    ///
    /// Note: This is rarely needed in normal operation. The cache automatically
    /// handles TTL-based expiration and LRU eviction.
    pub fn invalidate_cache(&self) {
        self.object_cache.invalidate_all();
    }

    /// Returns cache statistics for monitoring.
    ///
    /// This provides visibility into the manifest cache utilization and can be
    /// used for operational monitoring, debugging, and tuning cache configuration.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let stats = table.cache_stats();
    /// println!("Cache entries: {}", stats.entry_count);
    /// println!("Cache utilization: {:.1}%", stats.utilization() * 100.0);
    /// println!("Cache active: {}", stats.is_active());
    /// ```
    pub fn cache_stats(&self) -> CacheStats {
        self.object_cache.stats()
    }

    /// Creates a table scan.
    pub fn scan(&self) -> TableScanBuilder<'_> {
        TableScanBuilder::new(self)
    }

    /// Creates an incremental scan to get changes between two snapshots.
    ///
    /// This is useful for Change Data Capture (CDC) use cases where you need to
    /// identify which files were added or removed between snapshots.
    ///
    /// # Arguments
    ///
    /// * `from_snapshot_id` - The starting snapshot (exclusive)
    /// * `to_snapshot_id` - The ending snapshot (inclusive)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let changes = table.incremental_scan(from_snapshot_id, to_snapshot_id)
    ///     .build()?
    ///     .changes()
    ///     .await?;
    ///
    /// for file in changes.added_data_files() {
    ///     println!("Added: {}", file.file_path());
    /// }
    /// ```
    pub fn incremental_scan(
        &self,
        from_snapshot_id: i64,
        to_snapshot_id: i64,
    ) -> IncrementalScanBuilder<'_> {
        IncrementalScanBuilder::new(self, from_snapshot_id, to_snapshot_id)
    }

    /// Creates a metadata table which provides table-like APIs for inspecting metadata.
    /// See [`MetadataTable`] for more details.
    pub fn inspect(&self) -> MetadataTable<'_> {
        MetadataTable::new(self)
    }

    /// Returns the flag indicating whether the `Table` is readonly or not
    pub fn readonly(&self) -> bool {
        self.readonly
    }

    /// Returns the current schema as a shared reference.
    pub fn current_schema_ref(&self) -> SchemaRef {
        self.metadata.current_schema().clone()
    }

    /// Create a reader for the table.
    pub fn reader_builder(&self) -> ArrowReaderBuilder {
        ArrowReaderBuilder::new(self.file_io.clone())
    }

    /// Creates a remove orphan files action for table maintenance.
    ///
    /// This action identifies and removes files that exist in the table's storage
    /// location but are not referenced by any snapshot. These "orphan" files
    /// typically result from failed writes, crashed jobs, or incomplete transactions.
    ///
    /// # Use Cases
    ///
    /// - Clean up storage after failed writes or crashes
    /// - Reduce storage costs from accumulated orphan files
    /// - Prepare for compliance audits by removing untracked data
    ///
    /// # Example
    ///
    /// ```ignore
    /// use chrono::{Duration, Utc};
    ///
    /// // Basic usage with dry-run to preview deletions
    /// let result = table.remove_orphan_files()
    ///     .dry_run(true)
    ///     .execute()
    ///     .await?;
    ///
    /// println!("Would delete {} orphan files ({} bytes)",
    ///     result.orphan_files.len(),
    ///     result.bytes_reclaimed);
    ///
    /// // Production usage with custom retention
    /// let result = table.remove_orphan_files()
    ///     .older_than(Utc::now() - Duration::days(7))
    ///     .max_concurrent_deletes(100)
    ///     .execute()
    ///     .await?;
    /// ```
    ///
    /// # Safety
    ///
    /// The action includes several safety measures:
    /// - Default 3-day retention period (files younger than 3 days are never deleted)
    /// - Dry-run mode to preview deletions
    /// - Path normalization to handle scheme variations (s3/s3a/s3n)
    ///
    /// # Note
    ///
    /// Unlike transaction-based operations, this action operates directly on storage
    /// and does not modify table metadata. Call `execute()` directly on the returned
    /// action.
    pub fn remove_orphan_files(&self) -> RemoveOrphanFilesAction {
        RemoveOrphanFilesAction::new(self.clone())
    }

    // =========================================================================
    // WAP / Branch-Tag Helpers
    // =========================================================================

    /// Create a branch reference and commit it to the catalog.
    ///
    /// This is a convenience wrapper over [`Transaction::create_branch`]. The catalog
    /// is only required at commit time, following the same pattern as
    /// [`Table::update_properties`].
    ///
    /// # Example
    ///
    /// ```ignore
    /// let table = table.create_branch("staging").commit(&catalog).await?;
    /// ```
    pub fn create_branch(&self, name: impl Into<String>) -> TableCreateBranchBuilder {
        TableCreateBranchBuilder::new(self.clone(), name)
    }

    /// Create a tag reference and commit it to the catalog.
    ///
    /// This is a convenience wrapper over [`Transaction::create_tag`].
    pub fn create_tag(&self, name: impl Into<String>) -> TableCreateTagBuilder {
        TableCreateTagBuilder::new(self.clone(), name)
    }

    /// Fast-forward a target branch to match another reference and commit it.
    ///
    /// This is the final step in Write-Audit-Publish workflows, promoting a
    /// staged branch to main.
    pub fn fast_forward(
        &self,
        target: impl Into<String>,
        source: impl Into<String>,
    ) -> TableFastForwardBuilder {
        TableFastForwardBuilder::new(self.clone(), target, source)
    }

    /// Write new data files to a specific branch using fast append.
    ///
    /// This helper creates a branch-targeted append and commits it in a single
    /// call. Use `fast_forward()` to promote the branch after validation.
    pub fn write_to_branch(&self, branch: impl Into<String>) -> BranchWriteBuilder {
        BranchWriteBuilder::new(self.clone(), branch)
    }

    // =========================================================================
    // Property Accessor Methods
    // =========================================================================

    /// Returns all table properties as a reference to the properties map.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let props = table.properties();
    /// for (key, value) in props {
    ///     println!("{} = {}", key, value);
    /// }
    /// ```
    pub fn properties(&self) -> &HashMap<String, String> {
        self.metadata.properties()
    }

    /// Returns a specific property value.
    ///
    /// # Arguments
    ///
    /// * `key` - Property key to look up
    ///
    /// # Returns
    ///
    /// `Some(&str)` if the property exists, `None` otherwise
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// if let Some(mode) = table.property("write.delete.mode") {
    ///     println!("Delete mode: {}", mode);
    /// }
    /// ```
    pub fn property(&self, key: &str) -> Option<&str> {
        self.metadata.properties().get(key).map(|s| s.as_str())
    }

    /// Returns a property value with a default fallback.
    ///
    /// # Arguments
    ///
    /// * `key` - Property key to look up
    /// * `default` - Default value if property doesn't exist
    ///
    /// # Returns
    ///
    /// Property value if exists, otherwise the default value
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let mode = table.property_or("write.delete.mode", "copy-on-write");
    /// ```
    pub fn property_or<'a>(&'a self, key: &str, default: &'a str) -> &'a str {
        self.property(key).unwrap_or(default)
    }

    /// Returns a property value parsed as a specific type.
    ///
    /// # Arguments
    ///
    /// * `key` - Property key to look up
    ///
    /// # Returns
    ///
    /// * `Ok(Some(T))` if property exists and parses successfully
    /// * `Ok(None)` if property doesn't exist
    /// * `Err` if property exists but fails to parse
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let file_size: Option<i64> = table.property_as("write.target-file-size-bytes")?;
    /// let enabled: Option<bool> = table.property_as("write.metadata.delete-after-commit.enabled")?;
    /// ```
    pub fn property_as<T: std::str::FromStr>(&self, key: &str) -> Result<Option<T>>
    where T::Err: std::fmt::Display {
        match self.property(key) {
            Some(value) => value.parse::<T>().map(Some).map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Failed to parse property '{key}' value '{value}': {e}"),
                )
            }),
            None => Ok(None),
        }
    }

    /// Returns a property value parsed as a specific type, with a default.
    ///
    /// # Arguments
    ///
    /// * `key` - Property key to look up
    /// * `default` - Default value if property doesn't exist
    ///
    /// # Returns
    ///
    /// * `Ok(T)` - Parsed property value or default (on missing property)
    /// * `Err` - If property exists but fails to parse
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let file_size: i64 = table.property_as_or("write.target-file-size-bytes", 536870912)?;
    /// ```
    pub fn property_as_or<T: std::str::FromStr>(&self, key: &str, default: T) -> Result<T>
    where T::Err: std::fmt::Display {
        self.property_as(key).map(|opt| opt.unwrap_or(default))
    }

    // =========================================================================
    // Property Update Methods
    // =========================================================================

    /// Creates a builder for updating table properties.
    ///
    /// This is a convenience method that wraps the transaction API for simple
    /// property updates. For combining property updates with other operations,
    /// use `Transaction::update_table_properties()` directly.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let updated_table = table.update_properties()
    ///     .set("write.delete.mode", "merge-on-read")
    ///     .remove("old.property")
    ///     .commit(&catalog)
    ///     .await?;
    /// ```
    pub fn update_properties(&self) -> TableUpdatePropertiesBuilder {
        TableUpdatePropertiesBuilder::new(self.clone())
    }
}

/// Builder for creating a new branch ref and committing it.
pub struct TableCreateBranchBuilder {
    table: Table,
    action: CreateBranchAction,
}

impl TableCreateBranchBuilder {
    /// Create a new branch builder for the given table and branch name.
    pub fn new(table: Table, name: impl Into<String>) -> Self {
        Self {
            table,
            action: CreateBranchAction::new().name(name),
        }
    }

    /// Set the snapshot ID the branch should point to.
    pub fn from_snapshot(mut self, snapshot_id: i64) -> Self {
        self.action = self.action.from_snapshot(snapshot_id);
        self
    }

    /// Set the minimum number of snapshots to keep for this branch.
    pub fn min_snapshots_to_keep(mut self, count: i32) -> Self {
        self.action = self.action.min_snapshots_to_keep(count);
        self
    }

    /// Set the maximum snapshot age for this branch.
    pub fn max_snapshot_age_ms(mut self, age_ms: i64) -> Self {
        self.action = self.action.max_snapshot_age_ms(age_ms);
        self
    }

    /// Set the maximum ref age for this branch.
    pub fn max_ref_age_ms(mut self, age_ms: i64) -> Self {
        self.action = self.action.max_ref_age_ms(age_ms);
        self
    }

    /// Commit the branch creation to the catalog.
    pub async fn commit(self, catalog: &dyn Catalog) -> Result<Table> {
        let tx = Transaction::new(&self.table);
        let tx = self.action.apply(tx)?;
        tx.commit(catalog).await
    }
}

/// Builder for creating a new tag ref and committing it.
pub struct TableCreateTagBuilder {
    table: Table,
    action: CreateTagAction,
}

impl TableCreateTagBuilder {
    /// Create a new tag builder for the given table and tag name.
    pub fn new(table: Table, name: impl Into<String>) -> Self {
        Self {
            table,
            action: CreateTagAction::new().name(name),
        }
    }

    /// Set the snapshot ID the tag should point to.
    pub fn from_snapshot(mut self, snapshot_id: i64) -> Self {
        self.action = self.action.from_snapshot(snapshot_id);
        self
    }

    /// Set the maximum ref age for this tag.
    pub fn max_ref_age_ms(mut self, age_ms: i64) -> Self {
        self.action = self.action.max_ref_age_ms(age_ms);
        self
    }

    /// Commit the tag creation to the catalog.
    pub async fn commit(self, catalog: &dyn Catalog) -> Result<Table> {
        let tx = Transaction::new(&self.table);
        let tx = self.action.apply(tx)?;
        tx.commit(catalog).await
    }
}

/// Builder for fast-forwarding a target branch to a source reference.
pub struct TableFastForwardBuilder {
    table: Table,
    action: FastForwardAction,
}

impl TableFastForwardBuilder {
    /// Create a new fast-forward builder for the given table, target branch, and source ref.
    pub fn new(table: Table, target: impl Into<String>, source: impl Into<String>) -> Self {
        Self {
            table,
            action: FastForwardAction::new().target(target).source(source),
        }
    }

    /// Commit the fast-forward update to the catalog.
    pub async fn commit(self, catalog: &dyn Catalog) -> Result<Table> {
        let tx = Transaction::new(&self.table);
        let tx = self.action.apply(tx)?;
        tx.commit(catalog).await
    }
}

/// Builder for appending new data files to a specific branch.
pub struct BranchWriteBuilder {
    table: Table,
    branch: String,
    check_duplicate: bool,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
}

impl BranchWriteBuilder {
    /// Create a new branch write builder for the given table and branch name.
    pub fn new(table: Table, branch: impl Into<String>) -> Self {
        Self {
            table,
            branch: branch.into(),
            check_duplicate: true,
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::new(),
            added_data_files: Vec::new(),
        }
    }

    /// Add a data file to the branch append.
    pub fn add_data_file(mut self, file: DataFile) -> Self {
        self.added_data_files.push(file);
        self
    }

    /// Add multiple data files to the branch append.
    pub fn add_data_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(files);
        self
    }

    /// Set whether to check duplicate files.
    pub fn with_check_duplicate(mut self, check: bool) -> Self {
        self.check_duplicate = check;
        self
    }

    /// Set commit UUID for the snapshot.
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set key metadata for manifest files.
    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }

    /// Set snapshot summary properties.
    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }

    /// Commit the append to the catalog.
    pub async fn commit(self, catalog: &dyn Catalog) -> Result<Table> {
        let tx = Transaction::new(&self.table);
        let mut action = tx
            .fast_append()
            .with_check_duplicate(self.check_duplicate)
            .add_data_files(self.added_data_files)
            .to_branch(self.branch);

        if let Some(commit_uuid) = self.commit_uuid {
            action = action.set_commit_uuid(commit_uuid);
        }
        if let Some(key_metadata) = self.key_metadata {
            action = action.set_key_metadata(key_metadata);
        }
        if !self.snapshot_properties.is_empty() {
            action = action.set_snapshot_properties(self.snapshot_properties);
        }

        let tx = action.apply(tx)?;
        tx.commit(catalog).await
    }
}

/// Builder for standalone table property updates.
///
/// This builder provides a convenient fluent API for updating table properties
/// without manually creating a transaction. The catalog is only required at
/// commit time, following the same pattern as `Transaction::commit()`.
///
/// # Example
///
/// ```rust,ignore
/// let updated = table.update_properties()
///     .set("write.delete.mode", "merge-on-read")
///     .set("write.update.mode", "merge-on-read")
///     .remove("deprecated.property")
///     .commit(&catalog)
///     .await?;
/// ```
pub struct TableUpdatePropertiesBuilder {
    table: Table,
    updates: HashMap<String, String>,
    removals: HashSet<String>,
}

impl TableUpdatePropertiesBuilder {
    /// Creates a new builder for the given table.
    pub fn new(table: Table) -> Self {
        Self {
            table,
            updates: HashMap::new(),
            removals: HashSet::new(),
        }
    }

    /// Sets a property key-value pair.
    ///
    /// If the key was previously marked for removal, it will be moved
    /// to the update set instead.
    ///
    /// # Arguments
    ///
    /// * `key` - Property key to set
    /// * `value` - Property value to set
    pub fn set(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let key = key.into();
        self.removals.remove(&key);
        self.updates.insert(key, value.into());
        self
    }

    /// Marks a property key for removal.
    ///
    /// If the key was previously set in this builder, it will be moved
    /// to the removal set instead.
    ///
    /// # Arguments
    ///
    /// * `key` - Property key to remove
    pub fn remove(mut self, key: impl Into<String>) -> Self {
        let key = key.into();
        self.updates.remove(&key);
        self.removals.insert(key);
        self
    }

    /// Commits the property updates to the catalog.
    ///
    /// If no updates or removals have been specified, returns the
    /// original table without making any catalog calls.
    ///
    /// # Arguments
    ///
    /// * `catalog` - The catalog to commit the changes to
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A reserved property is being modified
    /// - The same key is in both updates and removals (shouldn't happen with this API)
    /// - The catalog commit fails
    pub async fn commit(self, catalog: &dyn Catalog) -> Result<Table> {
        if self.updates.is_empty() && self.removals.is_empty() {
            // No changes, return current table
            return Ok(self.table);
        }

        let tx = Transaction::new(&self.table);
        let mut update_action = tx.update_table_properties();

        for (key, value) in self.updates {
            update_action = update_action.set(key, value);
        }

        for key in self.removals {
            update_action = update_action.remove(key);
        }

        let tx = update_action.apply(tx)?;
        tx.commit(catalog).await
    }
}

/// `StaticTable` is a read-only table struct that can be created from a metadata file or from `TableMetaData` without a catalog.
/// It can only be used to read metadata and for table scan.
/// # Examples
///
/// ```rust, no_run
/// # use iceberg::io::FileIO;
/// # use iceberg::table::StaticTable;
/// # use iceberg::TableIdent;
/// # async fn example() {
/// let metadata_file_location = "s3://bucket_name/path/to/metadata.json";
/// let file_io = FileIO::from_path(&metadata_file_location)
///     .unwrap()
///     .build()
///     .unwrap();
/// let static_identifier = TableIdent::from_strs(["static_ns", "static_table"]).unwrap();
/// let static_table =
///     StaticTable::from_metadata_file(&metadata_file_location, static_identifier, file_io)
///         .await
///         .unwrap();
/// let snapshot_id = static_table
///     .metadata()
///     .current_snapshot()
///     .unwrap()
///     .snapshot_id();
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct StaticTable(Table);

impl StaticTable {
    /// Creates a static table from a given `TableMetadata` and `FileIO`
    pub async fn from_metadata(
        metadata: TableMetadata,
        table_ident: TableIdent,
        file_io: FileIO,
    ) -> Result<Self> {
        let table = Table::builder()
            .metadata(metadata)
            .identifier(table_ident)
            .file_io(file_io.clone())
            .readonly(true)
            .build();

        Ok(Self(table?))
    }
    /// Creates a static table directly from metadata file and `FileIO`
    pub async fn from_metadata_file(
        metadata_location: &str,
        table_ident: TableIdent,
        file_io: FileIO,
    ) -> Result<Self> {
        let metadata = TableMetadata::read_from(&file_io, metadata_location).await?;

        let table = Table::builder()
            .metadata(metadata)
            .metadata_location(metadata_location)
            .identifier(table_ident)
            .file_io(file_io.clone())
            .readonly(true)
            .build();

        Ok(Self(table?))
    }

    /// Create a TableScanBuilder for the static table.
    pub fn scan(&self) -> TableScanBuilder<'_> {
        self.0.scan()
    }

    /// Get TableMetadataRef for the static table
    pub fn metadata(&self) -> TableMetadataRef {
        self.0.metadata_ref()
    }

    /// Consumes the `StaticTable` and return it as a `Table`
    /// Please use this method carefully as the Table it returns remains detached from a catalog
    /// and can't be used to perform modifications on the table.
    pub fn into_table(self) -> Table {
        self.0
    }

    /// Create a reader for the table.
    pub fn reader_builder(&self) -> ArrowReaderBuilder {
        ArrowReaderBuilder::new(self.0.file_io.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_static_table_from_file() {
        let metadata_file_name = "TableMetadataV2Valid.json";
        let metadata_file_path = format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            metadata_file_name
        );
        let file_io = FileIO::from_path(&metadata_file_path)
            .unwrap()
            .build()
            .unwrap();
        let static_identifier = TableIdent::from_strs(["static_ns", "static_table"]).unwrap();
        let static_table =
            StaticTable::from_metadata_file(&metadata_file_path, static_identifier, file_io)
                .await
                .unwrap();
        let snapshot_id = static_table
            .metadata()
            .current_snapshot()
            .unwrap()
            .snapshot_id();
        assert_eq!(
            snapshot_id, 3055729675574597004,
            "snapshot id from metadata don't match"
        );
    }

    #[tokio::test]
    async fn test_static_into_table() {
        let metadata_file_name = "TableMetadataV2Valid.json";
        let metadata_file_path = format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            metadata_file_name
        );
        let file_io = FileIO::from_path(&metadata_file_path)
            .unwrap()
            .build()
            .unwrap();
        let static_identifier = TableIdent::from_strs(["static_ns", "static_table"]).unwrap();
        let static_table =
            StaticTable::from_metadata_file(&metadata_file_path, static_identifier, file_io)
                .await
                .unwrap();
        let table = static_table.into_table();
        assert!(table.readonly());
        assert_eq!(table.identifier.name(), "static_table");
        assert_eq!(
            table.metadata_location(),
            Some(metadata_file_path).as_deref()
        );
    }

    #[tokio::test]
    async fn test_table_readonly_flag() {
        let metadata_file_name = "TableMetadataV2Valid.json";
        let metadata_file_path = format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            metadata_file_name
        );
        let file_io = FileIO::from_path(&metadata_file_path)
            .unwrap()
            .build()
            .unwrap();
        let metadata_file = file_io.new_input(metadata_file_path).unwrap();
        let metadata_file_content = metadata_file.read().await.unwrap();
        let table_metadata =
            serde_json::from_slice::<TableMetadata>(&metadata_file_content).unwrap();
        let static_identifier = TableIdent::from_strs(["ns", "table"]).unwrap();
        let table = Table::builder()
            .metadata(table_metadata)
            .identifier(static_identifier)
            .file_io(file_io.clone())
            .build()
            .unwrap();
        assert!(!table.readonly());
        assert_eq!(table.identifier.name(), "table");
    }

    /// Helper to create a table with properties for testing
    async fn create_table_with_properties() -> Table {
        // Use TableMetadataV1Compat.json which has properties set
        let metadata_file_path = format!(
            "{}/testdata/table_metadata/TableMetadataV1Compat.json",
            env!("CARGO_MANIFEST_DIR"),
        );
        let file_io = FileIO::from_path(&metadata_file_path)
            .unwrap()
            .build()
            .unwrap();
        let metadata_file = file_io.new_input(&metadata_file_path).unwrap();
        let metadata_file_content = metadata_file.read().await.unwrap();
        let table_metadata =
            serde_json::from_slice::<TableMetadata>(&metadata_file_content).unwrap();
        let identifier = TableIdent::from_strs(["ns", "table_with_props"]).unwrap();
        Table::builder()
            .metadata(table_metadata)
            .metadata_location(metadata_file_path)
            .identifier(identifier)
            .file_io(file_io)
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_properties_returns_all_properties() {
        let table = create_table_with_properties().await;
        let props = table.properties();
        assert!(!props.is_empty());
        assert!(props.contains_key("owner"));
    }

    #[tokio::test]
    async fn test_property_returns_existing_value() {
        let table = create_table_with_properties().await;
        assert_eq!(table.property("owner"), Some("spark"));
    }

    #[tokio::test]
    async fn test_property_returns_none_for_missing() {
        let table = create_table_with_properties().await;
        assert_eq!(table.property("nonexistent.property.key"), None);
    }

    #[tokio::test]
    async fn test_property_or_returns_value_when_exists() {
        let table = create_table_with_properties().await;
        assert_eq!(table.property_or("owner", "default"), "spark");
    }

    #[tokio::test]
    async fn test_property_or_returns_default_when_missing() {
        let table = create_table_with_properties().await;
        assert_eq!(
            table.property_or("nonexistent.key", "default-value"),
            "default-value"
        );
    }

    #[tokio::test]
    async fn test_property_as_parses_i64() {
        let table = create_table_with_properties().await;
        // history.expire.max-snapshot-age-ms is "18000000" in the test file
        let value: Option<i64> = table
            .property_as("history.expire.max-snapshot-age-ms")
            .unwrap();
        assert_eq!(value, Some(18000000));
    }

    #[tokio::test]
    async fn test_property_as_parses_i32() {
        let table = create_table_with_properties().await;
        // write.metadata.previous-versions-max is "20" in the test file
        let value: Option<i32> = table
            .property_as("write.metadata.previous-versions-max")
            .unwrap();
        assert_eq!(value, Some(20));
    }

    #[tokio::test]
    async fn test_property_as_returns_none_for_missing() {
        let table = create_table_with_properties().await;
        let value: Option<i64> = table.property_as("nonexistent.key").unwrap();
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn test_property_as_errors_on_invalid_parse() {
        let table = create_table_with_properties().await;
        // "owner" is "spark" which can't be parsed as i64
        let result: Result<Option<i64>> = table.property_as("owner");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message().contains("Failed to parse property"));
        assert!(err.message().contains("owner"));
    }

    #[tokio::test]
    async fn test_property_as_or_returns_parsed_value() {
        let table = create_table_with_properties().await;
        let value: i64 = table
            .property_as_or("history.expire.max-snapshot-age-ms", 0)
            .unwrap();
        assert_eq!(value, 18000000);
    }

    #[tokio::test]
    async fn test_property_as_or_returns_default_when_missing() {
        let table = create_table_with_properties().await;
        let value: i64 = table.property_as_or("nonexistent.key", 999).unwrap();
        assert_eq!(value, 999);
    }

    #[tokio::test]
    async fn test_property_as_or_errors_on_invalid_parse() {
        let table = create_table_with_properties().await;
        // "owner" is "spark" which can't be parsed as i64
        let result: Result<i64> = table.property_as_or("owner", 0);
        assert!(result.is_err());
    }

    // =========================================================================
    // TableUpdatePropertiesBuilder Tests
    // =========================================================================

    #[tokio::test]
    async fn test_update_properties_builder_set() {
        let table = create_table_with_properties().await;
        let builder = table.update_properties();

        // Set some properties
        let builder = builder.set("key1", "value1").set("key2", "value2");

        // Verify the builder has the updates tracked
        assert_eq!(builder.updates.len(), 2);
        assert_eq!(builder.updates.get("key1"), Some(&"value1".to_string()));
        assert_eq!(builder.updates.get("key2"), Some(&"value2".to_string()));
        assert!(builder.removals.is_empty());
    }

    #[tokio::test]
    async fn test_update_properties_builder_remove() {
        let table = create_table_with_properties().await;
        let builder = table.update_properties();

        // Remove some properties
        let builder = builder.remove("key1").remove("key2");

        // Verify the builder has the removals tracked
        assert_eq!(builder.removals.len(), 2);
        assert!(builder.removals.contains("key1"));
        assert!(builder.removals.contains("key2"));
        assert!(builder.updates.is_empty());
    }

    #[tokio::test]
    async fn test_update_properties_builder_set_then_remove() {
        let table = create_table_with_properties().await;
        let builder = table.update_properties();

        // Set a property then remove it
        let builder = builder.set("key1", "value1").remove("key1");

        // Should be in removals, not updates
        assert!(builder.updates.is_empty());
        assert_eq!(builder.removals.len(), 1);
        assert!(builder.removals.contains("key1"));
    }

    #[tokio::test]
    async fn test_update_properties_builder_remove_then_set() {
        let table = create_table_with_properties().await;
        let builder = table.update_properties();

        // Remove a property then set it
        let builder = builder.remove("key1").set("key1", "value1");

        // Should be in updates, not removals
        assert!(builder.removals.is_empty());
        assert_eq!(builder.updates.len(), 1);
        assert_eq!(builder.updates.get("key1"), Some(&"value1".to_string()));
    }

    #[tokio::test]
    async fn test_update_properties_builder_mixed_operations() {
        let table = create_table_with_properties().await;
        let builder = table.update_properties();

        // Mix of set and remove operations
        let builder = builder
            .set("set1", "value1")
            .set("set2", "value2")
            .remove("remove1")
            .remove("remove2")
            .set("set3", "value3");

        // Verify final state
        assert_eq!(builder.updates.len(), 3);
        assert_eq!(builder.removals.len(), 2);
        assert_eq!(builder.updates.get("set1"), Some(&"value1".to_string()));
        assert_eq!(builder.updates.get("set2"), Some(&"value2".to_string()));
        assert_eq!(builder.updates.get("set3"), Some(&"value3".to_string()));
        assert!(builder.removals.contains("remove1"));
        assert!(builder.removals.contains("remove2"));
    }

    #[tokio::test]
    async fn test_update_properties_builder_accepts_string_refs() {
        let table = create_table_with_properties().await;

        // Test that the API accepts &str, String, and other Into<String> types
        let key: &str = "key";
        let value: String = "value".to_string();

        let builder = table.update_properties().set(key, &value).remove("other");

        assert_eq!(builder.updates.len(), 1);
        assert_eq!(builder.removals.len(), 1);
    }

    #[tokio::test]
    async fn test_update_properties_builder_empty_returns_same_table() {
        use crate::catalog::MockCatalog;

        let table = create_table_with_properties().await;
        let mock_catalog = MockCatalog::new();

        // Empty builder should return same table without calling catalog
        let result = table.update_properties().commit(&mock_catalog).await;
        assert!(result.is_ok());
        let returned_table = result.unwrap();
        assert_eq!(returned_table.identifier(), table.identifier());
    }

    #[tokio::test]
    async fn test_create_branch_builder_commits() {
        use crate::catalog::MockCatalog;
        use crate::transaction::tests::make_v2_table;

        let table = make_v2_table();

        let mut mock_catalog = MockCatalog::new();
        let load_table = table.clone();
        mock_catalog.expect_load_table().returning_st(move |_| {
            let t = load_table.clone();
            Box::pin(async move { Ok(t) })
        });

        let update_table = table.clone();
        mock_catalog
            .expect_update_table()
            .times(1)
            .returning_st(move |commit| {
                let t = update_table.clone();
                Box::pin(async move { commit.apply(t) })
            });

        let updated = table
            .create_branch("audit")
            .commit(&mock_catalog)
            .await
            .unwrap();

        let branch_ref = updated.metadata().refs().get("audit").unwrap();
        assert_eq!(
            Some(branch_ref.snapshot_id),
            updated.metadata().current_snapshot_id()
        );
        assert!(branch_ref.is_branch());
    }

    #[tokio::test]
    async fn test_write_to_branch_builder_commits() {
        use crate::catalog::MockCatalog;
        use crate::spec::{DataContentType, DataFileBuilder, DataFileFormat, Literal, Struct};
        use crate::transaction::tests::make_v2_minimal_table;

        let table = make_v2_minimal_table();

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("s3://bucket/table/data/part-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(1))]))
            .build()
            .unwrap();

        let mut mock_catalog = MockCatalog::new();
        let load_table = table.clone();
        mock_catalog.expect_load_table().returning_st(move |_| {
            let t = load_table.clone();
            Box::pin(async move { Ok(t) })
        });

        let update_table = table.clone();
        mock_catalog
            .expect_update_table()
            .times(1)
            .returning_st(move |commit| {
                let t = update_table.clone();
                Box::pin(async move { commit.apply(t) })
            });

        let updated = table
            .write_to_branch("staging")
            .add_data_file(data_file)
            .commit(&mock_catalog)
            .await
            .unwrap();

        let branch_ref = updated.metadata().refs().get("staging").unwrap();
        assert!(
            updated
                .metadata()
                .snapshot_by_id(branch_ref.snapshot_id)
                .is_some()
        );
        assert!(updated.metadata().current_snapshot_id().is_none());
    }

    // =========================================================================
    // Cache Property Precedence Tests
    // =========================================================================

    use crate::io::object_cache::{
        DEFAULT_CACHE_SIZE_BYTES, DEFAULT_CACHE_TTL, MANIFEST_CACHE_ENABLED,
        MANIFEST_CACHE_EXPIRATION_INTERVAL_MS, MANIFEST_CACHE_MAX_TOTAL_BYTES,
    };

    /// Helper to create table metadata with cache properties set
    fn create_metadata_with_cache_properties(
        enabled: &str,
        max_bytes: &str,
        ttl_ms: &str,
    ) -> TableMetadata {
        let json = format!(
            r#"{{
                "format-version": 2,
                "table-uuid": "fb072c92-a02b-11e9-ae9c-1bb7bc9eca94",
                "location": "s3://bucket/test/location",
                "last-sequence-number": 0,
                "last-updated-ms": 1515100955770,
                "last-column-id": 1,
                "schemas": [
                    {{
                        "schema-id": 0,
                        "type": "struct",
                        "fields": [
                            {{"id": 1, "name": "x", "required": true, "type": "long"}}
                        ]
                    }}
                ],
                "current-schema-id": 0,
                "partition-specs": [{{"spec-id": 0, "fields": []}}],
                "default-spec-id": 0,
                "last-partition-id": 999,
                "properties": {{
                    "{MANIFEST_CACHE_ENABLED}": "{enabled}",
                    "{MANIFEST_CACHE_MAX_TOTAL_BYTES}": "{max_bytes}",
                    "{MANIFEST_CACHE_EXPIRATION_INTERVAL_MS}": "{ttl_ms}"
                }},
                "sort-orders": [{{"order-id": 0, "fields": []}}],
                "default-sort-order-id": 0
            }}"#
        );
        serde_json::from_str(&json).unwrap()
    }

    #[tokio::test]
    async fn test_cache_config_from_table_properties() {
        // Table metadata has: cache disabled, 100MB size, 10 minute TTL
        let metadata = create_metadata_with_cache_properties("false", "104857600", "600000");
        let file_io = FileIO::from_path("memory:///").unwrap().build().unwrap();
        let identifier = TableIdent::from_strs(["ns", "table"]).unwrap();

        let table = Table::builder()
            .metadata(metadata)
            .identifier(identifier)
            .file_io(file_io)
            .build()
            .unwrap();

        let stats = table.cache_stats();
        // Cache should be disabled per table property
        assert!(!stats.enabled);
        assert_eq!(stats.max_capacity, 0);
        assert_eq!(table.object_cache().time_to_live(), None);
    }

    #[tokio::test]
    async fn test_cache_config_table_properties_enabled() {
        // Table metadata has: cache enabled, 64MB size, 2 minute TTL
        let metadata = create_metadata_with_cache_properties("true", "67108864", "120000");
        let file_io = FileIO::from_path("memory:///").unwrap().build().unwrap();
        let identifier = TableIdent::from_strs(["ns", "table"]).unwrap();

        let table = Table::builder()
            .metadata(metadata)
            .identifier(identifier)
            .file_io(file_io)
            .build()
            .unwrap();

        let stats = table.cache_stats();
        assert!(stats.enabled);
        // Size should be from table property (64MB)
        assert_eq!(stats.max_capacity, 67108864);
        assert_eq!(
            table.object_cache().time_to_live(),
            Some(Duration::from_millis(120000))
        );
    }

    #[tokio::test]
    async fn test_cache_builder_disable_overrides_table_property() {
        // Table property enables cache, but builder disables it
        let metadata = create_metadata_with_cache_properties("true", "104857600", "600000");
        let file_io = FileIO::from_path("memory:///").unwrap().build().unwrap();
        let identifier = TableIdent::from_strs(["ns", "table"]).unwrap();

        let table = Table::builder()
            .metadata(metadata)
            .identifier(identifier)
            .file_io(file_io)
            .disable_cache() // Builder override
            .build()
            .unwrap();

        let stats = table.cache_stats();
        // Builder's disable_cache() should take precedence
        assert!(!stats.enabled);
        assert_eq!(stats.max_capacity, 0);
        assert_eq!(table.object_cache().time_to_live(), None);
    }

    #[tokio::test]
    async fn test_cache_builder_size_overrides_table_property() {
        // Table property says 64MB, but builder sets 128MB
        let metadata = create_metadata_with_cache_properties("true", "67108864", "600000");
        let file_io = FileIO::from_path("memory:///").unwrap().build().unwrap();
        let identifier = TableIdent::from_strs(["ns", "table"]).unwrap();

        let table = Table::builder()
            .metadata(metadata)
            .identifier(identifier)
            .file_io(file_io)
            .cache_size_bytes(128 * 1024 * 1024) // Builder override: 128MB
            .build()
            .unwrap();

        let stats = table.cache_stats();
        assert!(stats.enabled);
        // Builder's cache_size_bytes() should take precedence
        assert_eq!(stats.max_capacity, 128 * 1024 * 1024);
        assert_eq!(
            table.object_cache().time_to_live(),
            Some(Duration::from_millis(600000))
        );
    }

    #[tokio::test]
    async fn test_cache_defaults_when_no_properties() {
        // Table metadata without cache properties should use defaults
        let json = r#"{
            "format-version": 2,
            "table-uuid": "fb072c92-a02b-11e9-ae9c-1bb7bc9eca94",
            "location": "s3://bucket/test/location",
            "last-sequence-number": 0,
            "last-updated-ms": 1515100955770,
            "last-column-id": 1,
            "schemas": [
                {
                    "schema-id": 0,
                    "type": "struct",
                    "fields": [
                        {"id": 1, "name": "x", "required": true, "type": "long"}
                    ]
                }
            ],
            "current-schema-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "default-spec-id": 0,
            "last-partition-id": 999,
            "properties": {},
            "sort-orders": [{"order-id": 0, "fields": []}],
            "default-sort-order-id": 0
        }"#;
        let metadata: TableMetadata = serde_json::from_str(json).unwrap();
        let file_io = FileIO::from_path("memory:///").unwrap().build().unwrap();
        let identifier = TableIdent::from_strs(["ns", "table"]).unwrap();

        let table = Table::builder()
            .metadata(metadata)
            .identifier(identifier)
            .file_io(file_io)
            .build()
            .unwrap();

        let stats = table.cache_stats();
        // Should use defaults
        assert!(stats.enabled);
        assert_eq!(stats.max_capacity, DEFAULT_CACHE_SIZE_BYTES);
        assert_eq!(table.object_cache().time_to_live(), Some(DEFAULT_CACHE_TTL));
    }

    #[tokio::test]
    async fn test_cache_mixed_precedence() {
        // Table property: enabled=false, size=64MB, ttl=2min
        // Builder: size=128MB (but not disabling cache explicitly)
        // Expected: enabled=false (from table); cache remains disabled regardless of size override
        let metadata = create_metadata_with_cache_properties("false", "67108864", "120000");
        let file_io = FileIO::from_path("memory:///").unwrap().build().unwrap();
        let identifier = TableIdent::from_strs(["ns", "table"]).unwrap();

        let table = Table::builder()
            .metadata(metadata)
            .identifier(identifier)
            .file_io(file_io)
            .cache_size_bytes(128 * 1024 * 1024) // Only override size
            .build()
            .unwrap();

        let stats = table.cache_stats();
        // Table property disabled cache, builder didn't enable it
        assert!(!stats.enabled);
        assert_eq!(stats.max_capacity, 0);
        assert_eq!(table.object_cache().time_to_live(), None);
    }

    #[tokio::test]
    async fn test_cache_ttl_from_table_property() {
        // Table metadata has: cache enabled, default size, 10 minute TTL
        let metadata = create_metadata_with_cache_properties("true", "33554432", "600000");
        let file_io = FileIO::from_path("memory:///").unwrap().build().unwrap();
        let identifier = TableIdent::from_strs(["ns", "table"]).unwrap();

        // No builder TTL override - should use table property
        let table = Table::builder()
            .metadata(metadata)
            .identifier(identifier)
            .file_io(file_io)
            .build()
            .unwrap();

        let stats = table.cache_stats();
        assert!(stats.enabled);
        // Size from table property
        assert_eq!(stats.max_capacity, DEFAULT_CACHE_SIZE_BYTES);
        assert_eq!(
            table.object_cache().time_to_live(),
            Some(Duration::from_millis(600000))
        );
    }

    #[tokio::test]
    async fn test_cache_builder_ttl_overrides_table_property() {
        // Table property: 10 minute TTL
        // Builder: 1 minute TTL
        let metadata = create_metadata_with_cache_properties("true", "33554432", "600000");
        let file_io = FileIO::from_path("memory:///").unwrap().build().unwrap();
        let identifier = TableIdent::from_strs(["ns", "table"]).unwrap();

        let table = Table::builder()
            .metadata(metadata)
            .identifier(identifier)
            .file_io(file_io)
            .cache_ttl(Some(Duration::from_secs(60))) // Override to 1 minute
            .build()
            .unwrap();

        let stats = table.cache_stats();
        assert!(stats.enabled);
        assert_eq!(stats.max_capacity, DEFAULT_CACHE_SIZE_BYTES);
        assert_eq!(
            table.object_cache().time_to_live(),
            Some(Duration::from_secs(60))
        );
    }
}
