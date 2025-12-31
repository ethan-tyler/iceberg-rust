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

//! Object cache for Iceberg metadata.
//!
//! This module provides caching for manifest and manifest list objects to avoid
//! repeated reads from remote storage during query planning. Caching parsed objects
//! (rather than raw bytes) saves both network I/O and parsing overhead.
//!
//! # Performance
//!
//! Manifest caching can provide significant performance improvements for query planning,
//! especially for tables with many snapshots or when using cloud storage with higher latency.
//! Performance gains vary by workload; see `benches/scan_planning_benchmark.rs` for measurement.
//!
//! # Configuration
//!
//! Cache behavior can be configured via properties (matching Iceberg Java spec):
//!
//! | Property | Default | Description |
//! |----------|---------|-------------|
//! | `iceberg.io.manifest.cache-enabled` | `true` | Enable/disable caching |
//! | `iceberg.io.manifest.cache.max-total-bytes` | `33554432` (32MB) | Maximum cache size |
//! | `iceberg.io.manifest.cache.expiration-interval-ms` | `300000` (5min) | TTL for entries |
//!
//! # Property Precedence
//!
//! When building tables, cache configuration follows this precedence order (highest first):
//!
//! 1. **Explicit builder methods** - Direct calls to `TableBuilder::disable_cache()`,
//!    `TableBuilder::cache_size_bytes()`, or `TableBuilder::cache_ttl()`
//! 2. **Table metadata properties** - Properties stored in the table's metadata JSON
//! 3. **Default values** - `DEFAULT_CACHE_SIZE_BYTES` (32MB) and `DEFAULT_CACHE_TTL` (5min)
//!
//! This allows fine-grained control: set table-level defaults via metadata properties,
//! while still allowing runtime overrides via the builder API. For example, disable
//! caching for rarely-accessed tables via table properties, or increase cache size
//! for frequently-scanned large tables.
//!
//! # Example: Using Properties
//!
//! Table metadata properties are automatically read when building a table via
//! `Table::builder()`. This example shows manual parsing for custom integrations.
//!
//! ```rust
//! use std::collections::HashMap;
//!
//! use iceberg::io::object_cache::{
//!     MANIFEST_CACHE_ENABLED, MANIFEST_CACHE_MAX_TOTAL_BYTES, ObjectCacheConfig,
//! };
//!
//! // Parse config from a properties map
//! let mut props = HashMap::new();
//! props.insert(MANIFEST_CACHE_ENABLED.to_string(), "true".to_string());
//! props.insert(
//!     MANIFEST_CACHE_MAX_TOTAL_BYTES.to_string(),
//!     "104857600".to_string(),
//! ); // 100MB
//!
//! let config = ObjectCacheConfig::from_properties(&props);
//! assert!(config.enabled);
//! assert_eq!(config.max_total_bytes, 104857600);
//! ```
//!
//! # Example: Using Builder
//!
//! ```rust
//! use std::time::Duration;
//!
//! use iceberg::io::object_cache::ObjectCacheConfig;
//!
//! // Build config programmatically
//! let config = ObjectCacheConfig::builder()
//!     .max_total_bytes(100 * 1024 * 1024) // 100MB
//!     .expiration_interval(Duration::from_secs(600)) // 10 minutes
//!     .build();
//!
//! // Or disable caching entirely
//! let disabled = ObjectCacheConfig::builder().enabled(false).build();
//! ```
//!
//! # Example: Monitoring Cache Usage
//!
//! ```rust,ignore
//! // Get cache statistics from a table
//! let stats = table.cache_stats();
//! println!("Cache entries: {}", stats.entry_count);
//! println!("Cache size: {} bytes", stats.weighted_size);
//! println!("Utilization: {:.1}%", stats.utilization() * 100.0);
//!
//! // Check if cache might need tuning
//! if stats.utilization() > 0.9 {
//!     println!("Cache is nearly full; consider increasing max_total_bytes");
//! }
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use moka::policy::EvictionPolicy;

use crate::io::FileIO;
use crate::spec::{
    FormatVersion, Manifest, ManifestFile, ManifestList, SchemaId, SnapshotRef, TableMetadataRef,
};
use crate::{Error, ErrorKind, Result};

// ============================================================================
// Configuration Property Names (matching Iceberg Java spec)
// ============================================================================

/// Property to enable/disable manifest caching.
///
/// When set to "false", manifest files will always be read from storage.
/// Default: "true"
pub const MANIFEST_CACHE_ENABLED: &str = "iceberg.io.manifest.cache-enabled";

/// Property to set the maximum total cache size in bytes.
///
/// When the cache exceeds this size, least-recently-used entries are evicted.
/// Default: 33554432 (32MB)
pub const MANIFEST_CACHE_MAX_TOTAL_BYTES: &str = "iceberg.io.manifest.cache.max-total-bytes";

/// Property to set the cache entry expiration interval in milliseconds.
///
/// Entries older than this duration are automatically evicted.
/// Set to "0" to disable TTL-based expiration (entries only expire via LRU).
/// Default: 300000 (5 minutes)
pub const MANIFEST_CACHE_EXPIRATION_INTERVAL_MS: &str =
    "iceberg.io.manifest.cache.expiration-interval-ms";

// ============================================================================
// Default Values
// ============================================================================

/// Default cache size: 32MB
pub const DEFAULT_CACHE_SIZE_BYTES: u64 = 32 * 1024 * 1024;

/// Default TTL for cache entries: 5 minutes
pub const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(300);

/// Default cache enabled state
pub const DEFAULT_CACHE_ENABLED: bool = true;

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for the object cache.
///
/// This struct holds all cache configuration parameters and can be constructed
/// from a properties map (for Iceberg Java compatibility) or using the builder
/// pattern.
///
/// # Example
///
/// ```rust
/// use std::time::Duration;
///
/// use iceberg::io::object_cache::ObjectCacheConfig;
///
/// // Using defaults
/// let config = ObjectCacheConfig::default();
///
/// // Using builder
/// let config = ObjectCacheConfig::builder()
///     .max_total_bytes(100 * 1024 * 1024) // 100MB
///     .expiration_interval(Duration::from_secs(600)) // 10 minutes
///     .build();
/// ```
#[derive(Debug, Clone)]
pub struct ObjectCacheConfig {
    /// Whether caching is enabled.
    pub enabled: bool,
    /// Maximum total bytes to cache.
    pub max_total_bytes: u64,
    /// Time-to-live for cache entries. `None` means no TTL (LRU only).
    pub expiration_interval: Option<Duration>,
}

impl Default for ObjectCacheConfig {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_CACHE_ENABLED,
            max_total_bytes: DEFAULT_CACHE_SIZE_BYTES,
            expiration_interval: Some(DEFAULT_CACHE_TTL),
        }
    }
}

impl ObjectCacheConfig {
    /// Creates a new configuration builder.
    pub fn builder() -> ObjectCacheConfigBuilder {
        ObjectCacheConfigBuilder::default()
    }

    /// Creates configuration from a properties map.
    ///
    /// This parses Iceberg-standard property names to configure the cache.
    /// Unknown or invalid property values are ignored and defaults are used.
    ///
    /// # Arguments
    ///
    /// * `props` - A map of property key-value pairs
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::collections::HashMap;
    ///
    /// use iceberg::io::object_cache::ObjectCacheConfig;
    ///
    /// let mut props = HashMap::new();
    /// props.insert(
    ///     "iceberg.io.manifest.cache-enabled".to_string(),
    ///     "true".to_string(),
    /// );
    /// props.insert(
    ///     "iceberg.io.manifest.cache.max-total-bytes".to_string(),
    ///     "104857600".to_string(), // 100MB
    /// );
    ///
    /// let config = ObjectCacheConfig::from_properties(&props);
    /// assert!(config.enabled);
    /// assert_eq!(config.max_total_bytes, 104857600);
    /// ```
    pub fn from_properties(props: &HashMap<String, String>) -> Self {
        let enabled = props
            .get(MANIFEST_CACHE_ENABLED)
            .and_then(|v| parse_bool(v))
            .unwrap_or(DEFAULT_CACHE_ENABLED);

        let max_total_bytes = props
            .get(MANIFEST_CACHE_MAX_TOTAL_BYTES)
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_CACHE_SIZE_BYTES);

        let expiration_interval = props
            .get(MANIFEST_CACHE_EXPIRATION_INTERVAL_MS)
            .and_then(|v| v.parse::<u64>().ok())
            .map(|ms| {
                if ms == 0 {
                    None
                } else {
                    Some(Duration::from_millis(ms))
                }
            })
            .unwrap_or(Some(DEFAULT_CACHE_TTL));

        Self {
            enabled,
            max_total_bytes,
            expiration_interval,
        }
    }
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "t" | "1" | "on" => Some(true),
        "false" | "f" | "0" | "off" => Some(false),
        _ => None,
    }
}

/// Builder for [`ObjectCacheConfig`].
#[derive(Debug, Clone)]
pub struct ObjectCacheConfigBuilder {
    enabled: bool,
    max_total_bytes: u64,
    expiration_interval: Option<Duration>,
}

impl Default for ObjectCacheConfigBuilder {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_CACHE_ENABLED,
            max_total_bytes: DEFAULT_CACHE_SIZE_BYTES,
            expiration_interval: Some(DEFAULT_CACHE_TTL),
        }
    }
}

impl ObjectCacheConfigBuilder {
    /// Sets whether caching is enabled.
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Sets the maximum total bytes to cache.
    pub fn max_total_bytes(mut self, bytes: u64) -> Self {
        self.max_total_bytes = bytes;
        self
    }

    /// Sets the expiration interval for cache entries.
    ///
    /// Use [`ObjectCacheConfigBuilder::no_expiration`] to disable TTL-based expiration.
    pub fn expiration_interval(mut self, interval: Duration) -> Self {
        self.expiration_interval = Some(interval);
        self
    }

    /// Disables TTL-based expiration (entries only expire via LRU).
    pub fn no_expiration(mut self) -> Self {
        self.expiration_interval = None;
        self
    }

    /// Builds the configuration.
    pub fn build(self) -> ObjectCacheConfig {
        ObjectCacheConfig {
            enabled: self.enabled,
            max_total_bytes: self.max_total_bytes,
            expiration_interval: self.expiration_interval,
        }
    }
}

// ============================================================================
// Cache Statistics
// ============================================================================

/// Statistics about cache usage for monitoring.
///
/// These statistics can be used for operational monitoring, debugging,
/// and tuning cache configuration.
///
/// # Example
///
/// ```rust,ignore
/// let stats = table.cache_stats();
/// println!("Cache entries: {}", stats.entry_count);
/// println!("Cache utilization: {:.1}%", stats.utilization() * 100.0);
/// ```
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    /// Number of entries currently in the cache.
    pub entry_count: u64,
    /// Total weighted size of all entries in bytes.
    pub weighted_size: u64,
    /// Maximum capacity of the cache in bytes.
    pub max_capacity: u64,
    /// Whether the cache is enabled.
    pub enabled: bool,
}

impl CacheStats {
    /// Returns the cache utilization as a ratio (0.0 to 1.0).
    ///
    /// Returns 0.0 if the cache is disabled or has zero capacity.
    pub fn utilization(&self) -> f64 {
        if !self.enabled || self.max_capacity == 0 {
            0.0
        } else {
            self.weighted_size as f64 / self.max_capacity as f64
        }
    }

    /// Returns true if the cache is enabled and has capacity.
    pub fn is_active(&self) -> bool {
        self.enabled && self.max_capacity > 0
    }
}

/// Cache hit/miss/load counters for manifest cache operations.
#[derive(Debug, Clone, Default)]
pub struct CacheMetrics {
    /// Number of cache hits.
    pub hits: u64,
    /// Number of cache misses.
    pub misses: u64,
    /// Number of successful loads after a miss.
    pub loads: u64,
}

impl CacheMetrics {
    /// Returns the hit rate as a ratio (0.0 to 1.0).
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

#[derive(Debug, Default)]
struct CacheMetricsInner {
    hits: AtomicU64,
    misses: AtomicU64,
    loads: AtomicU64,
}

impl CacheMetricsInner {
    fn snapshot(&self) -> CacheMetrics {
        CacheMetrics {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            loads: self.loads.load(Ordering::Relaxed),
        }
    }

    fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    fn record_load(&self) {
        self.loads.fetch_add(1, Ordering::Relaxed);
    }
}

/// Estimates the memory size of a ManifestList.
///
/// This provides a reasonable approximation of memory usage based on:
/// - Each ManifestFile entry contains strings (paths, partition summaries) and numeric fields
/// - Typical ManifestFile is ~500-1000 bytes when accounting for all fields
fn estimate_manifest_list_size(manifest_list: &ManifestList) -> u32 {
    const BASE_SIZE: u32 = 64; // Base overhead for ManifestList struct
    const PER_ENTRY_SIZE: u32 = 800; // Estimated size per ManifestFile entry

    BASE_SIZE.saturating_add((manifest_list.entries().len() as u32).saturating_mul(PER_ENTRY_SIZE))
}

/// Estimates the memory size of a Manifest.
///
/// This provides a reasonable approximation of memory usage based on:
/// - ManifestMetadata contains schema, partition spec, and other metadata
/// - Each ManifestEntry contains file paths, partition values, column stats
/// - Typical ManifestEntry is ~1-2KB when accounting for statistics
fn estimate_manifest_size(manifest: &Manifest) -> u32 {
    const BASE_SIZE: u32 = 1024; // Base overhead for Manifest + ManifestMetadata
    const PER_ENTRY_SIZE: u32 = 1500; // Estimated size per ManifestEntry

    BASE_SIZE.saturating_add((manifest.entries().len() as u32).saturating_mul(PER_ENTRY_SIZE))
}

#[derive(Clone, Debug)]
pub(crate) enum CachedItem {
    ManifestList(Arc<ManifestList>),
    Manifest(Arc<Manifest>),
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub(crate) enum CachedObjectKey {
    ManifestList((String, FormatVersion, SchemaId)),
    Manifest(String),
}

/// Caches metadata objects deserialized from immutable files.
///
/// The cache uses moka's async cache with:
/// - Weight-based eviction using estimated memory sizes
/// - Optional TTL (time-to-live) for automatic entry expiration
/// - Thread-safe concurrent access
#[derive(Clone, Debug)]
pub struct ObjectCache {
    cache: moka::future::Cache<CachedObjectKey, CachedItem>,
    file_io: FileIO,
    cache_disabled: bool,
    metrics: Arc<CacheMetricsInner>,
}

impl ObjectCache {
    /// Creates a new [`ObjectCache`] with specific cache size and TTL.
    ///
    /// # Arguments
    ///
    /// * `file_io` - The FileIO instance for loading files
    /// * `cache_size_bytes` - Maximum cache size in bytes (0 disables caching)
    /// * `ttl` - Optional time-to-live for cache entries. `None` means no expiration.
    pub(crate) fn new_with_options(
        file_io: FileIO,
        cache_size_bytes: u64,
        ttl: Option<Duration>,
    ) -> Self {
        if cache_size_bytes == 0 {
            Self::with_disabled_cache(file_io)
        } else {
            let mut builder = moka::future::Cache::builder()
                .eviction_policy(EvictionPolicy::lru())
                .weigher(|_, val: &CachedItem| match val {
                    CachedItem::ManifestList(item) => estimate_manifest_list_size(item),
                    CachedItem::Manifest(item) => estimate_manifest_size(item),
                })
                .max_capacity(cache_size_bytes);

            if let Some(duration) = ttl {
                builder = builder.time_to_live(duration);
            }

            Self {
                cache: builder.build(),
                file_io,
                cache_disabled: false,
                metrics: Arc::new(CacheMetricsInner::default()),
            }
        }
    }

    /// Creates a new [`ObjectCache`] from a configuration.
    ///
    /// This is the recommended way to create an ObjectCache when using
    /// property-based configuration.
    ///
    /// # Arguments
    ///
    /// * `file_io` - The FileIO instance for loading files
    /// * `config` - The cache configuration
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use iceberg::io::object_cache::{ObjectCache, ObjectCacheConfig};
    ///
    /// let config = ObjectCacheConfig::from_properties(&props);
    /// let cache = ObjectCache::new_with_config(file_io, config);
    /// ```
    pub(crate) fn new_with_config(file_io: FileIO, config: ObjectCacheConfig) -> Self {
        if !config.enabled {
            Self::with_disabled_cache(file_io)
        } else {
            Self::new_with_options(file_io, config.max_total_bytes, config.expiration_interval)
        }
    }

    /// Creates a new [`ObjectCache`] with caching disabled.
    pub(crate) fn with_disabled_cache(file_io: FileIO) -> Self {
        Self {
            cache: moka::future::Cache::new(0),
            file_io,
            cache_disabled: true,
            metrics: Arc::new(CacheMetricsInner::default()),
        }
    }

    /// Invalidates (clears) all entries in the cache.
    ///
    /// This is useful when table metadata changes, as cached manifests
    /// may no longer be relevant to the new snapshot.
    pub(crate) fn invalidate_all(&self) {
        self.cache.invalidate_all();
    }

    /// Returns the current number of entries in the cache.
    #[cfg(test)]
    pub(crate) fn entry_count(&self) -> u64 {
        self.cache.entry_count()
    }

    /// Returns cache statistics for monitoring.
    ///
    /// This provides visibility into cache utilization and can be used for
    /// operational monitoring, debugging, and tuning cache configuration.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let stats = cache.stats();
    /// println!("Cache utilization: {:.1}%", stats.utilization() * 100.0);
    /// if stats.utilization() > 0.9 {
    ///     println!("Consider increasing cache size");
    /// }
    /// ```
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            entry_count: self.cache.entry_count(),
            weighted_size: self.cache.weighted_size(),
            max_capacity: self.cache.policy().max_capacity().unwrap_or(0),
            enabled: !self.cache_disabled,
        }
    }

    /// Returns cache hit/miss/load metrics for monitoring.
    pub fn metrics(&self) -> CacheMetrics {
        self.metrics.snapshot()
    }

    #[cfg(test)]
    pub(crate) fn time_to_live(&self) -> Option<Duration> {
        self.cache.policy().time_to_live()
    }

    /// Returns true if caching is enabled.
    pub fn is_enabled(&self) -> bool {
        !self.cache_disabled
    }

    /// Retrieves an Arc [`Manifest`] from the cache
    /// or retrieves one from FileIO and parses it if not present
    pub(crate) async fn get_manifest(&self, manifest_file: &ManifestFile) -> Result<Arc<Manifest>> {
        if self.cache_disabled {
            self.metrics.record_miss();
            let manifest = manifest_file.load_manifest(&self.file_io).await?;
            self.metrics.record_load();
            return Ok(Arc::new(manifest));
        }

        let key = CachedObjectKey::Manifest(manifest_file.manifest_path.clone());

        let cache_entry = self
            .cache
            .entry_by_ref(&key)
            .or_try_insert_with(self.fetch_and_parse_manifest(manifest_file))
            .await
            .map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Failed to load manifest {}", manifest_file.manifest_path),
                )
                .with_source(err)
            })?;

        if cache_entry.is_fresh() {
            self.metrics.record_miss();
        } else {
            self.metrics.record_hit();
        }

        let cache_entry = cache_entry.into_value();

        match cache_entry {
            CachedItem::Manifest(arc_manifest) => Ok(arc_manifest),
            _ => Err(Error::new(
                ErrorKind::Unexpected,
                format!("cached object for key '{key:?}' is not a Manifest"),
            )),
        }
    }

    /// Retrieves an Arc [`ManifestList`] from the cache
    /// or retrieves one from FileIO and parses it if not present
    pub(crate) async fn get_manifest_list(
        &self,
        snapshot: &SnapshotRef,
        table_metadata: &TableMetadataRef,
    ) -> Result<Arc<ManifestList>> {
        if self.cache_disabled {
            self.metrics.record_miss();
            let manifest_list = snapshot
                .load_manifest_list(&self.file_io, table_metadata)
                .await?;
            self.metrics.record_load();
            return Ok(Arc::new(manifest_list));
        }

        let key = CachedObjectKey::ManifestList((
            snapshot.manifest_list().to_string(),
            table_metadata.format_version,
            snapshot.schema_id().unwrap(),
        ));
        let cache_entry = self
            .cache
            .entry_by_ref(&key)
            .or_try_insert_with(self.fetch_and_parse_manifest_list(snapshot, table_metadata))
            .await
            .map_err(|err| {
                Arc::try_unwrap(err).unwrap_or_else(|err| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "Failed to load manifest list in cache",
                    )
                    .with_source(err)
                })
            })?;

        if cache_entry.is_fresh() {
            self.metrics.record_miss();
        } else {
            self.metrics.record_hit();
        }

        let cache_entry = cache_entry.into_value();

        match cache_entry {
            CachedItem::ManifestList(arc_manifest_list) => Ok(arc_manifest_list),
            _ => Err(Error::new(
                ErrorKind::Unexpected,
                format!("cached object for path '{key:?}' is not a manifest list"),
            )),
        }
    }

    async fn fetch_and_parse_manifest(&self, manifest_file: &ManifestFile) -> Result<CachedItem> {
        let manifest = manifest_file.load_manifest(&self.file_io).await?;
        self.metrics.record_load();

        Ok(CachedItem::Manifest(Arc::new(manifest)))
    }

    async fn fetch_and_parse_manifest_list(
        &self,
        snapshot: &SnapshotRef,
        table_metadata: &TableMetadataRef,
    ) -> Result<CachedItem> {
        let manifest_list = snapshot
            .load_manifest_list(&self.file_io, table_metadata)
            .await?;
        self.metrics.record_load();

        Ok(CachedItem::ManifestList(Arc::new(manifest_list)))
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use futures::TryStreamExt;
    use minijinja::value::Value;
    use minijinja::{AutoEscape, Environment, context};
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::TableIdent;
    use crate::io::{FileIO, OutputFile};
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, ManifestListWriter,
        ManifestWriterBuilder, Struct, TableMetadata,
    };
    use crate::table::Table;

    fn render_template(template: &str, ctx: Value) -> String {
        let mut env = Environment::new();
        env.set_auto_escape_callback(|_| AutoEscape::None);
        env.render_str(template, ctx).unwrap()
    }

    struct TableTestFixture {
        _temp_dir: TempDir,
        table_location: String,
        table: Table,
    }

    impl TableTestFixture {
        fn new() -> Self {
            let tmp_dir = TempDir::new().unwrap();
            let table_location = tmp_dir.path().join("table1");
            let manifest_list1_location = table_location.join("metadata/manifests_list_1.avro");
            let manifest_list2_location = table_location.join("metadata/manifests_list_2.avro");
            let table_metadata1_location = table_location.join("metadata/v1.json");

            let file_io = FileIO::from_path(table_location.as_os_str().to_str().unwrap())
                .unwrap()
                .build()
                .unwrap();

            let table_metadata = {
                let template_json_str = fs::read_to_string(format!(
                    "{}/testdata/example_table_metadata_v2.json",
                    env!("CARGO_MANIFEST_DIR")
                ))
                .unwrap();
                let metadata_json = render_template(&template_json_str, context! {
                    table_location => &table_location,
                    manifest_list_1_location => &manifest_list1_location,
                    manifest_list_2_location => &manifest_list2_location,
                    table_metadata_1_location => &table_metadata1_location,
                });
                serde_json::from_str::<TableMetadata>(&metadata_json).unwrap()
            };

            let table = Table::builder()
                .metadata(table_metadata)
                .identifier(TableIdent::from_strs(["db", "table1"]).unwrap())
                .file_io(file_io.clone())
                .metadata_location(table_metadata1_location.as_os_str().to_str().unwrap())
                .build()
                .unwrap();

            Self {
                _temp_dir: tmp_dir,
                table_location: table_location.to_str().unwrap().to_string(),
                table,
            }
        }

        fn next_manifest_file(&self) -> OutputFile {
            self.table
                .file_io()
                .new_output(format!(
                    "{}/metadata/manifest_{}.avro",
                    self.table_location,
                    Uuid::new_v4()
                ))
                .unwrap()
        }

        async fn setup_manifest_files(&mut self) {
            self.write_manifest_list_with_entries(1, 1).await;
        }

        async fn write_manifest_list_with_entries(
            &self,
            manifest_count: usize,
            entries_per_manifest: usize,
        ) {
            let current_snapshot = self.table.metadata().current_snapshot().unwrap();
            let current_schema = current_snapshot.schema(self.table.metadata()).unwrap();
            let current_partition_spec = self.table.metadata().default_partition_spec();

            let mut manifests = Vec::with_capacity(manifest_count);
            for manifest_idx in 0..manifest_count {
                let mut writer = ManifestWriterBuilder::new(
                    self.next_manifest_file(),
                    Some(current_snapshot.snapshot_id()),
                    None,
                    current_schema.clone(),
                    current_partition_spec.as_ref().clone(),
                )
                .build_v2_data();

                for entry_idx in 0..entries_per_manifest {
                    let partition_value = (manifest_idx * entries_per_manifest + entry_idx) as i64;
                    let data_file = DataFileBuilder::default()
                        .partition_spec_id(current_partition_spec.spec_id())
                        .content(DataContentType::Data)
                        .file_path(format!(
                            "{}/{}_{}.parquet",
                            &self.table_location,
                            manifest_idx,
                            entry_idx
                        ))
                        .file_format(DataFileFormat::Parquet)
                        .file_size_in_bytes(100)
                        .record_count(1)
                        .partition(Struct::from_iter([Some(Literal::long(partition_value))]))
                        .build()
                        .unwrap();

                    writer
                        .add_file(data_file, current_snapshot.sequence_number())
                        .unwrap();
                }

                manifests.push(writer.write_manifest_file().await.unwrap());
            }

            let mut manifest_list_write = ManifestListWriter::v2(
                self.table
                    .file_io()
                    .new_output(current_snapshot.manifest_list())
                    .unwrap(),
                current_snapshot.snapshot_id(),
                current_snapshot.parent_snapshot_id(),
                current_snapshot.sequence_number(),
            );
            manifest_list_write
                .add_manifests(manifests.into_iter())
                .unwrap();
            manifest_list_write.close().await.unwrap();
        }
    }

    async fn plan_files_task_count(table: &Table) -> usize {
        let scan = table.scan().build().unwrap();
        let mut stream = scan.plan_files().await.unwrap();
        let mut task_count = 0usize;
        while let Some(_task) = stream.try_next().await.unwrap() {
            task_count += 1;
        }
        task_count
    }

    #[tokio::test]
    async fn test_get_manifest_list_and_manifest_from_disabled_cache() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        let object_cache = ObjectCache::with_disabled_cache(fixture.table.file_io().clone());
        assert_eq!(object_cache.entry_count(), 0);

        let result_manifest_list = object_cache
            .get_manifest_list(
                fixture.table.metadata().current_snapshot().unwrap(),
                &fixture.table.metadata_ref(),
            )
            .await
            .unwrap();

        assert_eq!(result_manifest_list.entries().len(), 1);

        let manifest_file = result_manifest_list.entries().first().unwrap();
        let result_manifest = object_cache.get_manifest(manifest_file).await.unwrap();

        assert_eq!(object_cache.entry_count(), 0);

        assert_eq!(
            result_manifest
                .entries()
                .first()
                .unwrap()
                .file_path()
                .split("/")
                .last()
                .unwrap(),
            "0_0.parquet"
        );
    }

    #[tokio::test]
    async fn test_get_manifest_list_and_manifest_from_default_cache() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        let object_cache = ObjectCache::new_with_config(
            fixture.table.file_io().clone(),
            ObjectCacheConfig::default(),
        );
        assert_eq!(
            object_cache.cache.policy().time_to_live(),
            Some(DEFAULT_CACHE_TTL)
        );
        assert_eq!(
            object_cache.cache.policy().max_capacity(),
            Some(DEFAULT_CACHE_SIZE_BYTES)
        );
        assert_eq!(object_cache.entry_count(), 0);

        // not in cache
        let result_manifest_list = object_cache
            .get_manifest_list(
                fixture.table.metadata().current_snapshot().unwrap(),
                &fixture.table.metadata_ref(),
            )
            .await
            .unwrap();

        object_cache.cache.run_pending_tasks().await;
        assert_eq!(object_cache.entry_count(), 1);

        assert_eq!(result_manifest_list.entries().len(), 1);

        // retrieve cached version
        let result_manifest_list = object_cache
            .get_manifest_list(
                fixture.table.metadata().current_snapshot().unwrap(),
                &fixture.table.metadata_ref(),
            )
            .await
            .unwrap();

        object_cache.cache.run_pending_tasks().await;
        assert_eq!(object_cache.entry_count(), 1);

        assert_eq!(result_manifest_list.entries().len(), 1);

        let manifest_file = result_manifest_list.entries().first().unwrap();

        // not in cache
        let result_manifest = object_cache.get_manifest(manifest_file).await.unwrap();

        object_cache.cache.run_pending_tasks().await;
        assert_eq!(object_cache.entry_count(), 2);

        assert_eq!(
            result_manifest
                .entries()
                .first()
                .unwrap()
                .file_path()
                .split("/")
                .last()
                .unwrap(),
            "0_0.parquet"
        );

        // retrieve cached version
        let result_manifest = object_cache.get_manifest(manifest_file).await.unwrap();

        object_cache.cache.run_pending_tasks().await;
        assert_eq!(object_cache.entry_count(), 2);

        assert_eq!(
            result_manifest
                .entries()
                .first()
                .unwrap()
                .file_path()
                .split("/")
                .last()
                .unwrap(),
            "0_0.parquet"
        );
    }

    #[tokio::test]
    async fn test_table_with_metadata_invalidates_cache() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        let object_cache = fixture.table.object_cache();

        let manifest_list = object_cache
            .get_manifest_list(
                fixture.table.metadata().current_snapshot().unwrap(),
                &fixture.table.metadata_ref(),
            )
            .await
            .unwrap();
        object_cache
            .get_manifest(manifest_list.entries().first().unwrap())
            .await
            .unwrap();
        object_cache.cache.run_pending_tasks().await;
        assert_eq!(object_cache.entry_count(), 2);

        // No-op metadata set (same Arc): should not invalidate.
        let table_same_metadata = fixture
            .table
            .clone()
            .with_metadata(fixture.table.metadata_ref());
        assert_eq!(table_same_metadata.object_cache().entry_count(), 2);

        // New metadata Arc: should invalidate.
        let updated_metadata = Arc::new(fixture.table.metadata().clone());
        let updated_table = table_same_metadata.with_metadata(updated_metadata);

        updated_table.object_cache().cache.run_pending_tasks().await;
        assert_eq!(updated_table.object_cache().entry_count(), 0);
    }

    #[tokio::test]
    async fn test_plan_files_reflects_metadata_update() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        let initial_tasks = plan_files_task_count(&fixture.table).await;
        assert_eq!(initial_tasks, 1);

        fixture.write_manifest_list_with_entries(2, 1).await;

        let updated_table = fixture
            .table
            .clone()
            .with_metadata(Arc::new(fixture.table.metadata().clone()));
        let updated_tasks = plan_files_task_count(&updated_table).await;

        assert_eq!(updated_tasks, 2);
    }

    #[tokio::test]
    async fn test_lru_eviction_when_cache_full() {
        let fixture = TableTestFixture::new();
        let current_snapshot = fixture.table.metadata().current_snapshot().unwrap();
        let current_schema = current_snapshot.schema(fixture.table.metadata()).unwrap();
        let partition_spec = fixture.table.metadata().default_partition_spec();

        let mut writer1 = ManifestWriterBuilder::new(
            fixture.next_manifest_file(),
            Some(current_snapshot.snapshot_id()),
            None,
            current_schema.clone(),
            partition_spec.as_ref().clone(),
        )
        .build_v2_data();
        writer1
            .add_file(
                DataFileBuilder::default()
                    .partition_spec_id(partition_spec.spec_id())
                    .content(DataContentType::Data)
                    .file_path(format!("{}/1.parquet", &fixture.table_location))
                    .file_format(DataFileFormat::Parquet)
                    .file_size_in_bytes(100)
                    .record_count(1)
                    .partition(Struct::from_iter([Some(Literal::long(1))]))
                    .build()
                    .unwrap(),
                current_snapshot.sequence_number(),
            )
            .unwrap();
        let manifest_file1 = writer1.write_manifest_file().await.unwrap();

        let mut writer2 = ManifestWriterBuilder::new(
            fixture.next_manifest_file(),
            Some(current_snapshot.snapshot_id()),
            None,
            current_schema,
            partition_spec.as_ref().clone(),
        )
        .build_v2_data();
        writer2
            .add_file(
                DataFileBuilder::default()
                    .partition_spec_id(partition_spec.spec_id())
                    .content(DataContentType::Data)
                    .file_path(format!("{}/2.parquet", &fixture.table_location))
                    .file_format(DataFileFormat::Parquet)
                    .file_size_in_bytes(100)
                    .record_count(1)
                    .partition(Struct::from_iter([Some(Literal::long(2))]))
                    .build()
                    .unwrap(),
                current_snapshot.sequence_number(),
            )
            .unwrap();
        let manifest_file2 = writer2.write_manifest_file().await.unwrap();

        // Cache sized to fit only one small manifest (weight ~ 2.5KB).
        let object_cache =
            ObjectCache::new_with_options(fixture.table.file_io().clone(), 2_600, None);

        let key1 = CachedObjectKey::Manifest(manifest_file1.manifest_path.clone());
        let key2 = CachedObjectKey::Manifest(manifest_file2.manifest_path.clone());

        object_cache.get_manifest(&manifest_file1).await.unwrap();
        object_cache.cache.run_pending_tasks().await;
        assert!(object_cache.cache.contains_key(&key1));

        object_cache.get_manifest(&manifest_file2).await.unwrap();
        object_cache.cache.run_pending_tasks().await;

        // LRU: the first inserted entry should be evicted.
        assert!(!object_cache.cache.contains_key(&key1));
        assert!(object_cache.cache.contains_key(&key2));

        // Accessing the first manifest again should evict the second.
        object_cache.get_manifest(&manifest_file1).await.unwrap();
        object_cache.cache.run_pending_tasks().await;
        assert!(object_cache.cache.contains_key(&key1));
        assert!(!object_cache.cache.contains_key(&key2));
    }

    // =========================================================================
    // ObjectCacheConfig Tests
    // =========================================================================

    #[test]
    fn test_config_default() {
        let config = ObjectCacheConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_total_bytes, DEFAULT_CACHE_SIZE_BYTES);
        assert_eq!(config.expiration_interval, Some(DEFAULT_CACHE_TTL));
    }

    #[test]
    fn test_config_builder() {
        let config = ObjectCacheConfig::builder()
            .enabled(false)
            .max_total_bytes(100 * 1024 * 1024)
            .expiration_interval(Duration::from_secs(600))
            .build();

        assert!(!config.enabled);
        assert_eq!(config.max_total_bytes, 100 * 1024 * 1024);
        assert_eq!(config.expiration_interval, Some(Duration::from_secs(600)));
    }

    #[test]
    fn test_config_builder_no_expiration() {
        let config = ObjectCacheConfig::builder().no_expiration().build();

        assert!(config.enabled);
        assert_eq!(config.expiration_interval, None);
    }

    #[test]
    fn test_config_from_properties_defaults() {
        let props = HashMap::new();
        let config = ObjectCacheConfig::from_properties(&props);

        assert!(config.enabled);
        assert_eq!(config.max_total_bytes, DEFAULT_CACHE_SIZE_BYTES);
        assert_eq!(config.expiration_interval, Some(DEFAULT_CACHE_TTL));
    }

    #[test]
    fn test_config_from_properties_custom_values() {
        let mut props = HashMap::new();
        props.insert(MANIFEST_CACHE_ENABLED.to_string(), "true".to_string());
        props.insert(
            MANIFEST_CACHE_MAX_TOTAL_BYTES.to_string(),
            "104857600".to_string(),
        ); // 100MB
        props.insert(
            MANIFEST_CACHE_EXPIRATION_INTERVAL_MS.to_string(),
            "600000".to_string(),
        ); // 10 min

        let config = ObjectCacheConfig::from_properties(&props);

        assert!(config.enabled);
        assert_eq!(config.max_total_bytes, 104857600);
        assert_eq!(
            config.expiration_interval,
            Some(Duration::from_millis(600000))
        );
    }

    #[test]
    fn test_config_from_properties_disabled() {
        let mut props = HashMap::new();
        props.insert(MANIFEST_CACHE_ENABLED.to_string(), "false".to_string());

        let config = ObjectCacheConfig::from_properties(&props);

        assert!(!config.enabled);
    }

    #[test]
    fn test_config_from_properties_zero_ttl_disables_expiration() {
        let mut props = HashMap::new();
        props.insert(
            MANIFEST_CACHE_EXPIRATION_INTERVAL_MS.to_string(),
            "0".to_string(),
        );

        let config = ObjectCacheConfig::from_properties(&props);

        assert_eq!(config.expiration_interval, None);
    }

    #[test]
    fn test_config_from_properties_invalid_values_use_defaults() {
        let mut props = HashMap::new();
        props.insert(MANIFEST_CACHE_ENABLED.to_string(), "not_a_bool".to_string());
        props.insert(
            MANIFEST_CACHE_MAX_TOTAL_BYTES.to_string(),
            "not_a_number".to_string(),
        );
        props.insert(
            MANIFEST_CACHE_EXPIRATION_INTERVAL_MS.to_string(),
            "invalid".to_string(),
        );

        let config = ObjectCacheConfig::from_properties(&props);

        // Should fall back to defaults
        assert!(config.enabled);
        assert_eq!(config.max_total_bytes, DEFAULT_CACHE_SIZE_BYTES);
        assert_eq!(config.expiration_interval, Some(DEFAULT_CACHE_TTL));
    }

    // =========================================================================
    // CacheStats Tests
    // =========================================================================

    #[test]
    fn test_cache_stats_default() {
        let stats = CacheStats::default();
        assert_eq!(stats.entry_count, 0);
        assert_eq!(stats.weighted_size, 0);
        assert_eq!(stats.max_capacity, 0);
        assert!(!stats.enabled);
        assert_eq!(stats.utilization(), 0.0);
        assert!(!stats.is_active());
    }

    #[test]
    fn test_cache_stats_utilization() {
        let stats = CacheStats {
            entry_count: 10,
            weighted_size: 50,
            max_capacity: 100,
            enabled: true,
        };
        assert_eq!(stats.utilization(), 0.5);
        assert!(stats.is_active());
    }

    #[test]
    fn test_cache_stats_utilization_disabled() {
        let stats = CacheStats {
            entry_count: 10,
            weighted_size: 50,
            max_capacity: 100,
            enabled: false,
        };
        assert_eq!(stats.utilization(), 0.0);
        assert!(!stats.is_active());
    }

    #[test]
    fn test_cache_stats_utilization_zero_capacity() {
        let stats = CacheStats {
            entry_count: 0,
            weighted_size: 0,
            max_capacity: 0,
            enabled: true,
        };
        assert_eq!(stats.utilization(), 0.0);
        assert!(!stats.is_active());
    }

    #[test]
    fn test_cache_metrics_default() {
        let metrics = CacheMetrics::default();
        assert_eq!(metrics.hits, 0);
        assert_eq!(metrics.misses, 0);
        assert_eq!(metrics.loads, 0);
        assert_eq!(metrics.hit_rate(), 0.0);
    }

    // =========================================================================
    // ObjectCache Stats Integration Tests
    // =========================================================================

    #[tokio::test]
    async fn test_object_cache_stats_enabled() {
        let fixture = TableTestFixture::new();
        let cache = ObjectCache::new_with_config(
            fixture.table.file_io().clone(),
            ObjectCacheConfig::default(),
        );

        let stats = cache.stats();
        assert!(stats.enabled);
        assert!(stats.is_active());
        assert_eq!(stats.entry_count, 0);
        assert_eq!(stats.max_capacity, DEFAULT_CACHE_SIZE_BYTES);
    }

    #[tokio::test]
    async fn test_object_cache_stats_disabled() {
        let fixture = TableTestFixture::new();
        let cache = ObjectCache::with_disabled_cache(fixture.table.file_io().clone());

        let stats = cache.stats();
        assert!(!stats.enabled);
        assert!(!stats.is_active());
    }

    #[tokio::test]
    async fn test_object_cache_new_with_config() {
        let fixture = TableTestFixture::new();

        // Test with enabled config
        let config = ObjectCacheConfig::builder()
            .max_total_bytes(50 * 1024 * 1024)
            .build();
        let cache = ObjectCache::new_with_config(fixture.table.file_io().clone(), config);
        assert!(cache.is_enabled());
        assert_eq!(cache.stats().max_capacity, 50 * 1024 * 1024);

        // Test with disabled config
        let config = ObjectCacheConfig::builder().enabled(false).build();
        let cache = ObjectCache::new_with_config(fixture.table.file_io().clone(), config);
        assert!(!cache.is_enabled());
    }

    #[tokio::test]
    async fn test_object_cache_stats_after_operations() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        let cache = ObjectCache::new_with_config(
            fixture.table.file_io().clone(),
            ObjectCacheConfig::default(),
        );

        // Initially empty
        let stats = cache.stats();
        assert_eq!(stats.entry_count, 0);
        assert_eq!(stats.weighted_size, 0);

        // Load manifest list
        cache
            .get_manifest_list(
                fixture.table.metadata().current_snapshot().unwrap(),
                &fixture.table.metadata_ref(),
            )
            .await
            .unwrap();

        cache.cache.run_pending_tasks().await;

        // Should have one entry now
        let stats = cache.stats();
        assert_eq!(stats.entry_count, 1);
        assert!(stats.weighted_size > 0);
        assert!(stats.utilization() > 0.0);
    }

    #[tokio::test]
    async fn test_object_cache_metrics_after_operations() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;

        let cache = ObjectCache::new_with_config(
            fixture.table.file_io().clone(),
            ObjectCacheConfig::default(),
        );

        let manifest_list = cache
            .get_manifest_list(
                fixture.table.metadata().current_snapshot().unwrap(),
                &fixture.table.metadata_ref(),
            )
            .await
            .unwrap();
        let manifest_file = manifest_list.entries().first().unwrap();

        cache
            .get_manifest_list(
                fixture.table.metadata().current_snapshot().unwrap(),
                &fixture.table.metadata_ref(),
            )
            .await
            .unwrap();
        cache.get_manifest(manifest_file).await.unwrap();
        cache.get_manifest(manifest_file).await.unwrap();

        let metrics = cache.metrics();
        assert_eq!(metrics.hits, 2);
        assert_eq!(metrics.misses, 2);
        assert_eq!(metrics.loads, 2);
        assert!(metrics.hit_rate() > 0.0);
    }
}
