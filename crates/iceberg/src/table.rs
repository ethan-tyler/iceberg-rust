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

use crate::arrow::ArrowReaderBuilder;
use crate::inspect::MetadataTable;
use crate::io::FileIO;
use crate::io::object_cache::ObjectCache;
use crate::scan::TableScanBuilder;
use crate::spec::{SchemaRef, TableMetadata, TableMetadataRef};
use crate::transaction::{ApplyTransactionAction, Transaction};
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

    /// optionally set a non-default metadata cache size
    pub fn cache_size_bytes(mut self, cache_size_bytes: u64) -> Self {
        self.cache_size_bytes = Some(cache_size_bytes);
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

        let object_cache = if disable_cache {
            Arc::new(ObjectCache::with_disabled_cache(file_io.clone()))
        } else if let Some(cache_size_bytes) = cache_size_bytes {
            Arc::new(ObjectCache::new_with_capacity(
                file_io.clone(),
                cache_size_bytes,
            ))
        } else {
            Arc::new(ObjectCache::new(file_io.clone()))
        };

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
    pub(crate) fn with_metadata(mut self, metadata: TableMetadataRef) -> Self {
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

    /// Creates a table scan.
    pub fn scan(&self) -> TableScanBuilder<'_> {
        TableScanBuilder::new(self)
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
}
