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

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DFResult};
use futures::future::try_join_all;
use iceberg::inspect::MetadataTableType;
use iceberg::{Catalog, NamespaceIdent, Result, TableIdent};

use crate::table::{IcebergStaticTableProvider, IcebergTableProvider};
use crate::to_datafusion_error;

/// Time travel specification for table queries.
///
/// Supports two forms:
/// - `@v<snapshot_id>` - Query a specific snapshot by ID (e.g., `mytable@v12345`)
/// - `@ts<timestamp_ms>` - Query the snapshot at a specific timestamp in milliseconds (e.g., `mytable@ts1703123456789`)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimeTravelSpec {
    /// Query a specific snapshot by ID
    SnapshotId(i64),
    /// Query the snapshot that was current at the given timestamp (milliseconds since epoch)
    Timestamp(i64),
}

/// Parse a table name that may include time travel syntax.
///
/// Returns the base table name and optional time travel specification.
///
/// # Supported syntax
/// - `table` - current snapshot (no time travel)
/// - `table@v12345` - snapshot ID 12345
/// - `table@ts1703123456789` - timestamp in milliseconds since epoch
///
/// # Examples
/// ```ignore
/// parse_time_travel("mytable") => ("mytable", None)
/// parse_time_travel("mytable@v12345") => ("mytable", Some(TimeTravelSpec::SnapshotId(12345)))
/// parse_time_travel("mytable@ts1703123456789") => ("mytable", Some(TimeTravelSpec::Timestamp(1703123456789)))
/// ```
fn parse_time_travel(name: &str) -> (&str, Option<TimeTravelSpec>) {
    if let Some((table_name, suffix)) = name.split_once('@') {
        if let Some(version_str) = suffix.strip_prefix('v') {
            if let Ok(snapshot_id) = version_str.parse::<i64>() {
                return (table_name, Some(TimeTravelSpec::SnapshotId(snapshot_id)));
            }
        } else if let Some(ts_str) = suffix.strip_prefix("ts")
            && let Ok(timestamp_ms) = ts_str.parse::<i64>()
        {
            return (table_name, Some(TimeTravelSpec::Timestamp(timestamp_ms)));
        }
    }
    (name, None)
}

/// Represents a [`SchemaProvider`] for the Iceberg [`Catalog`], managing
/// access to table providers within a specific namespace.
///
/// # Time Travel Support
///
/// This provider supports time travel queries using table name suffixes:
/// - `table@v<snapshot_id>` - Query a specific snapshot by ID
/// - `table@ts<timestamp_ms>` - Query the snapshot at a specific timestamp
///
/// Time travel queries return read-only static table providers that point to
/// the specified historical snapshot.
#[derive(Debug)]
pub(crate) struct IcebergSchemaProvider {
    /// Reference to the catalog for loading tables on demand (e.g., for time travel)
    catalog: Arc<dyn Catalog>,
    /// The namespace this schema provider serves
    namespace: NamespaceIdent,
    /// A `HashMap` where keys are table names
    /// and values are dynamic references to objects implementing the
    /// [`TableProvider`] trait.
    tables: HashMap<String, Arc<IcebergTableProvider>>,
}

impl IcebergSchemaProvider {
    /// Asynchronously tries to construct a new [`IcebergSchemaProvider`]
    /// using the given client to fetch and initialize table providers for
    /// the provided namespace in the Iceberg [`Catalog`].
    ///
    /// This method retrieves a list of table names
    /// attempts to create a table provider for each table name, and
    /// collects these providers into a `HashMap`.
    pub(crate) async fn try_new(
        client: Arc<dyn Catalog>,
        namespace: NamespaceIdent,
    ) -> Result<Self> {
        // TODO:
        // Tables and providers should be cached based on table_name
        // if we have a cache miss; we update our internal cache & check again
        // As of right now; tables might become stale.
        let table_names: Vec<_> = client
            .list_tables(&namespace)
            .await?
            .iter()
            .map(|tbl| tbl.name().to_string())
            .collect();

        let providers = try_join_all(
            table_names
                .iter()
                .map(|name| IcebergTableProvider::try_new(client.clone(), namespace.clone(), name))
                .collect::<Vec<_>>(),
        )
        .await?;

        let tables: HashMap<String, Arc<IcebergTableProvider>> = table_names
            .into_iter()
            .zip(providers.into_iter())
            .map(|(name, provider)| (name, Arc::new(provider)))
            .collect();

        Ok(IcebergSchemaProvider {
            catalog: client,
            namespace,
            tables,
        })
    }

    /// Create a static table provider for time travel queries.
    ///
    /// Loads the table from the catalog and creates a provider for the specified
    /// snapshot (either by ID or by finding the snapshot at the given timestamp).
    async fn create_time_travel_provider(
        &self,
        table_name: &str,
        time_travel: TimeTravelSpec,
    ) -> DFResult<Arc<dyn TableProvider>> {
        let table_ident = TableIdent::new(self.namespace.clone(), table_name.to_string());
        let table = self
            .catalog
            .load_table(&table_ident)
            .await
            .map_err(to_datafusion_error)?;

        match time_travel {
            TimeTravelSpec::SnapshotId(snapshot_id) => {
                // Validate snapshot exists and create provider
                if table.metadata().snapshot_by_id(snapshot_id).is_none() {
                    let available_ids: Vec<_> = table
                        .metadata()
                        .snapshots()
                        .map(|s| s.snapshot_id())
                        .collect();
                    return Err(DataFusionError::Plan(format!(
                        "Snapshot {snapshot_id} not found in table '{table_name}'. \
                         Available snapshots: {available_ids:?}"
                    )));
                }
                let provider =
                    IcebergStaticTableProvider::try_new_from_table_snapshot(table, snapshot_id)
                        .await
                        .map_err(to_datafusion_error)?;
                Ok(Arc::new(provider))
            }
            TimeTravelSpec::Timestamp(timestamp_ms) => {
                // Find the snapshot at the given timestamp
                let snapshot = table
                    .metadata()
                    .snapshot_at_timestamp(timestamp_ms)
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "No snapshot found at or before timestamp {timestamp_ms} \
                             for table '{table_name}'"
                        ))
                    })?;
                let snapshot_id = snapshot.snapshot_id();
                let provider =
                    IcebergStaticTableProvider::try_new_from_table_snapshot(table, snapshot_id)
                        .await
                        .map_err(to_datafusion_error)?;
                Ok(Arc::new(provider))
            }
        }
    }
}

#[async_trait]
impl SchemaProvider for IcebergSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables
            .keys()
            .flat_map(|table_name| {
                [table_name.clone()]
                    .into_iter()
                    .chain(MetadataTableType::all_types().map(|metadata_table_name| {
                        format!("{}${}", table_name.clone(), metadata_table_name.as_str())
                    }))
            })
            .collect()
    }

    fn table_exist(&self, name: &str) -> bool {
        // First check for time travel syntax (@v or @ts suffix)
        let (base_name, time_travel) = parse_time_travel(name);
        if time_travel.is_some() {
            // For time travel queries, just check if the base table exists
            // (actual snapshot validation happens in table())
            return self.tables.contains_key(base_name);
        }

        // Then check for metadata table syntax ($suffix)
        if let Some((table_name, metadata_table_name)) = name.split_once('$') {
            self.tables.contains_key(table_name)
                && MetadataTableType::try_from(metadata_table_name).is_ok()
        } else {
            self.tables.contains_key(name)
        }
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        // First check for time travel syntax (@v or @ts suffix)
        let (base_name, time_travel) = parse_time_travel(name);
        if let Some(tt_spec) = time_travel {
            if !self.tables.contains_key(base_name) {
                return Ok(None);
            }
            return Ok(Some(
                self.create_time_travel_provider(base_name, tt_spec).await?,
            ));
        }

        // Then check for metadata table syntax ($suffix)
        if let Some((table_name, metadata_table_name)) = name.split_once('$') {
            let metadata_table_type =
                MetadataTableType::try_from(metadata_table_name).map_err(DataFusionError::Plan)?;
            if let Some(table) = self.tables.get(table_name) {
                let metadata_table = table
                    .metadata_table(metadata_table_type)
                    .await
                    .map_err(to_datafusion_error)?;
                return Ok(Some(Arc::new(metadata_table)));
            } else {
                return Ok(None);
            }
        }

        Ok(self
            .tables
            .get(name)
            .cloned()
            .map(|t| t as Arc<dyn TableProvider>))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_time_travel_no_suffix() {
        let (name, tt) = parse_time_travel("mytable");
        assert_eq!(name, "mytable");
        assert!(tt.is_none());
    }

    #[test]
    fn test_parse_time_travel_snapshot_id() {
        let (name, tt) = parse_time_travel("mytable@v12345678901234");
        assert_eq!(name, "mytable");
        assert_eq!(tt, Some(TimeTravelSpec::SnapshotId(12345678901234)));
    }

    #[test]
    fn test_parse_time_travel_timestamp() {
        let (name, tt) = parse_time_travel("mytable@ts1703123456789");
        assert_eq!(name, "mytable");
        assert_eq!(tt, Some(TimeTravelSpec::Timestamp(1703123456789)));
    }

    #[test]
    fn test_parse_time_travel_invalid_suffix() {
        // Invalid version prefix - whole string treated as table name
        let (name, tt) = parse_time_travel("mytable@x12345");
        assert_eq!(name, "mytable@x12345");
        assert!(tt.is_none());

        // Non-numeric version - whole string treated as table name
        let (name, tt) = parse_time_travel("mytable@vabc");
        assert_eq!(name, "mytable@vabc");
        assert!(tt.is_none());

        // Invalid timestamp prefix - whole string treated as table name
        let (name, tt) = parse_time_travel("mytable@t12345");
        assert_eq!(name, "mytable@t12345");
        assert!(tt.is_none());

        // Non-numeric timestamp - whole string treated as table name
        let (name, tt) = parse_time_travel("mytable@tsabc");
        assert_eq!(name, "mytable@tsabc");
        assert!(tt.is_none());
    }

    #[test]
    fn test_parse_time_travel_metadata_table_not_confused() {
        // $ is for metadata tables, should not be parsed as time travel
        let (name, tt) = parse_time_travel("mytable$snapshots");
        assert_eq!(name, "mytable$snapshots");
        assert!(tt.is_none());
    }

    #[test]
    fn test_parse_time_travel_negative_values() {
        // Negative snapshot IDs should work (they're valid i64)
        let (name, tt) = parse_time_travel("mytable@v-123");
        assert_eq!(name, "mytable");
        assert_eq!(tt, Some(TimeTravelSpec::SnapshotId(-123)));
    }
}
