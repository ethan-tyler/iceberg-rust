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

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use chrono::Utc;
use clap::{Args, Parser, Subcommand, ValueEnum};
use iceberg::Catalog;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg_catalog_loader::CatalogLoader;
use serde_json::json;

use crate::table_ref::{self, LoadedTable};

const REST_CATALOG_PROP_URI_KEY: &str = "uri";
const S3_ENDPOINT_PROP_KEY: &str = "s3.endpoint";
const S3_ACCESS_KEY_ID_PROP_KEY: &str = "s3.access-key-id";
const S3_SECRET_ACCESS_KEY_PROP_KEY: &str = "s3.secret-access-key";
const S3_REGION_PROP_KEY: &str = "s3.region";
const S3_SESSION_TOKEN_PROP_KEY: &str = "s3.session-token";

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    /// Human-readable output (default).
    Human,
    /// Machine-readable JSON output.
    Json,
}

#[derive(Debug, Clone, Args, Default)]
pub struct CatalogArgs {
    /// Catalog type (e.g. "rest", "glue", "hms", "sql").
    #[arg(long, global = true)]
    pub catalog_type: Option<String>,

    /// Optional catalog name (used by catalog loader).
    #[arg(long, global = true)]
    pub catalog_name: Option<String>,

    /// Catalog URI (e.g., rest://localhost:8181 or rest+http://localhost:8181).
    #[arg(long, global = true)]
    pub catalog: Option<String>,

    /// Catalog config file path (YAML).
    #[arg(long, global = true)]
    pub catalog_config: Option<PathBuf>,

    /// REST catalog base URI (e.g., http://localhost:8181).
    #[arg(long, global = true)]
    pub uri: Option<String>,

    /// Optional S3 endpoint for object storage.
    #[arg(long, global = true, env = "AWS_ENDPOINT_URL")]
    pub s3_endpoint: Option<String>,

    /// Optional S3 access key id.
    #[arg(long, global = true, env = "AWS_ACCESS_KEY_ID")]
    pub s3_access_key_id: Option<String>,

    /// Optional S3 secret access key.
    #[arg(long, global = true, env = "AWS_SECRET_ACCESS_KEY")]
    pub s3_secret_access_key: Option<String>,

    /// Optional S3 region.
    #[arg(long, global = true, env = "AWS_REGION")]
    pub s3_region: Option<String>,

    #[arg(long, global = true, env = "AWS_SESSION_TOKEN")]
    pub s3_session_token: Option<String>,
}

impl CatalogArgs {
    pub async fn load_catalog(&self) -> anyhow::Result<Arc<dyn Catalog>> {
        let mut props: HashMap<String, String> = HashMap::new();

        let mut catalog_type: Option<String> = None;
        let mut catalog_name: Option<String> = None;

        if let Some(config_path) = self.catalog_config.as_deref() {
            let cfg = crate::config::load_catalog_config_file(config_path)?;
            catalog_type = Some(cfg.catalog_type);
            catalog_name = cfg.name;
            props.extend(cfg.props);
        }

        if let Some(catalog_uri) = self.catalog.as_deref() {
            let parsed = crate::config::parse_catalog_uri(catalog_uri)?;
            if let Some(existing) = catalog_type.as_deref() {
                anyhow::ensure!(
                    existing == parsed.catalog_type,
                    "--catalog scheme '{}' does not match catalog config type '{}'",
                    parsed.catalog_type,
                    existing
                );
            } else {
                catalog_type = Some(parsed.catalog_type);
            }
            props.extend(parsed.props);
        }

        if let Some(flag_type) = self.catalog_type.as_deref() {
            if let Some(existing) = catalog_type.as_deref() {
                anyhow::ensure!(
                    existing == flag_type,
                    "--catalog-type '{flag_type}' does not match catalog config type '{existing}'"
                );
            }
            catalog_type = Some(flag_type.to_string());
        }

        if let Some(flag_name) = self.catalog_name.as_deref() {
            catalog_name = Some(flag_name.to_string());
        }

        if let Some(uri) = self.uri.as_deref() {
            props.insert(REST_CATALOG_PROP_URI_KEY.to_string(), uri.to_string());
        }

        self.apply_storage_props(&mut props);

        let catalog_type = catalog_type.context(
            "catalog configuration missing: provide --catalog-type, --catalog, or --catalog-config",
        )?;

        if catalog_type == "rest" {
            anyhow::ensure!(
                props.contains_key(REST_CATALOG_PROP_URI_KEY),
                "rest catalog requires --uri, --catalog (rest://...), or catalog config with '{REST_CATALOG_PROP_URI_KEY}'"
            );
        }

        let catalog_name = catalog_name.unwrap_or_else(|| catalog_type.clone());
        let loader = CatalogLoader::from(catalog_type.as_str());
        loader
            .load(catalog_name, props)
            .await
            .map_err(anyhow::Error::from)
    }

    pub fn apply_storage_props(&self, props: &mut HashMap<String, String>) {
        if let Some(endpoint) = self.s3_endpoint.as_deref() {
            props.insert(S3_ENDPOINT_PROP_KEY.to_string(), endpoint.to_string());
        }
        if let Some(access_key_id) = self.s3_access_key_id.as_deref() {
            props.insert(
                S3_ACCESS_KEY_ID_PROP_KEY.to_string(),
                access_key_id.to_string(),
            );
        }
        if let Some(secret_access_key) = self.s3_secret_access_key.as_deref() {
            props.insert(
                S3_SECRET_ACCESS_KEY_PROP_KEY.to_string(),
                secret_access_key.to_string(),
            );
        }
        if let Some(region) = self.s3_region.as_deref() {
            props.insert(S3_REGION_PROP_KEY.to_string(), region.to_string());
        }
        if let Some(session_token) = self.s3_session_token.as_deref() {
            props.insert(
                S3_SESSION_TOKEN_PROP_KEY.to_string(),
                session_token.to_string(),
            );
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "iceberg-cli",
    about = "Operational maintenance CLI for Apache Iceberg",
    arg_required_else_help = true
)]
pub struct Cli {
    /// Output format.
    #[arg(long, value_enum, default_value_t = OutputFormat::Human, global = true)]
    pub output: OutputFormat,

    #[command(flatten)]
    pub catalog: CatalogArgs,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Compact a table's data files.
    #[command(name = "compact", aliases = ["rewrite-data-files"])]
    Compact(CompactArgs),

    /// Rewrite small manifests into larger consolidated manifests.
    #[command(name = "rewrite-manifests")]
    RewriteManifests(RewriteManifestsArgs),

    /// Expire snapshots older than a retention window.
    #[command(name = "expire-snapshots")]
    ExpireSnapshots(ExpireSnapshotsArgs),

    /// Remove orphan files from table storage.
    #[command(name = "remove-orphans")]
    RemoveOrphans(RemoveOrphansArgs),

    /// Rewrite position delete files.
    #[command(name = "rewrite-deletes", aliases = ["rewrite-position-delete-files"])]
    RewriteDeletes(RewriteDeletesArgs),
}

#[derive(Debug, Clone, Args)]
#[command(
    group(clap::ArgGroup::new("table_target").required(true).multiple(false).args([
        "table",
        "location",
    ]))
)]
pub struct TableRefArgs {
    /// Catalog-qualified table identifier (e.g., default.my_table).
    #[arg(long, conflicts_with = "location")]
    pub table: Option<String>,

    /// Direct table location (read-only operations only).
    #[arg(long, conflicts_with = "table")]
    pub location: Option<String>,
}

impl TableRefArgs {
    pub fn is_location(&self) -> bool {
        self.location.is_some()
    }

    pub fn is_table(&self) -> bool {
        self.table.is_some()
    }

    pub fn table_ident_str(&self) -> anyhow::Result<&str> {
        self.table
            .as_deref()
            .context("--table is required for this command")
    }

    pub fn location_str(&self) -> anyhow::Result<&str> {
        self.location
            .as_deref()
            .context("--location is required for this command")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CompactionStrategy {
    /// Binpack compaction.
    Binpack,
}

#[derive(Debug, Args)]
pub struct CompactArgs {
    #[command(flatten)]
    pub table: TableRefArgs,

    #[arg(long, value_enum, default_value_t = CompactionStrategy::Binpack)]
    pub strategy: CompactionStrategy,
}

#[derive(Debug, Args)]
pub struct RewriteManifestsArgs {
    #[command(flatten)]
    pub table: TableRefArgs,
}

#[derive(Debug, Args)]
#[command(
    group(clap::ArgGroup::new("expire_mode").required(true).multiple(false).args([
        "dry_run",
        "delete_files",
    ]))
)]
pub struct ExpireSnapshotsArgs {
    #[command(flatten)]
    pub table: TableRefArgs,

    /// Expire snapshots older than this duration (e.g., 7d, 24h, 30d).
    #[arg(long)]
    pub older_than: humantime::Duration,

    /// Preview what would be expired without making changes.
    #[arg(long)]
    pub dry_run: bool,

    /// Delete files made unreferenced by expiration (requires explicit opt-in).
    #[arg(long)]
    pub delete_files: bool,
}

#[derive(Debug, Args)]
#[command(
    group(clap::ArgGroup::new("remove_mode").required(true).multiple(false).args([
        "dry_run",
        "delete",
    ]))
)]
pub struct RemoveOrphansArgs {
    #[command(flatten)]
    pub table: TableRefArgs,

    /// Only consider files older than this duration (e.g., 7d, 24h, 30d).
    #[arg(long)]
    pub older_than: Option<humantime::Duration>,

    /// Preview what would be deleted without making changes.
    #[arg(long)]
    pub dry_run: bool,

    /// Actually delete orphan files (requires explicit opt-in).
    #[arg(long)]
    pub delete: bool,
}

#[derive(Debug, Args)]
pub struct RewriteDeletesArgs {
    #[command(flatten)]
    pub table: TableRefArgs,
}

impl Cli {
    pub async fn run(self) -> std::process::ExitCode {
        match self.run_inner().await {
            Ok(()) => std::process::ExitCode::SUCCESS,
            Err(err) => {
                eprintln!("{err:#}");
                std::process::ExitCode::FAILURE
            }
        }
    }

    async fn run_inner(self) -> anyhow::Result<()> {
        match self.command {
            Commands::Compact(args) => {
                anyhow::ensure!(
                    !args.table.is_location(),
                    "--location is not supported for compact"
                );

                let LoadedTable::Catalog(table) =
                    table_ref::load_table(&args.table, &self.catalog).await?
                else {
                    anyhow::bail!("--location is not supported for compact");
                };

                let start = Instant::now();
                let result = iceberg_datafusion::compaction::compact_table(
                    &table.table,
                    table.catalog.clone(),
                    None,
                )
                .await
                .map_err(anyhow::Error::from)?;
                let duration_ms = start.elapsed().as_millis() as u64;

                if self.output == OutputFormat::Json {
                    let payload = json!({
                        "command": "compact",
                        "table": table.ident.to_string(),
                        "result": {
                            "has_changes": result.has_changes(),
                            "duration_ms": duration_ms,
                        }
                    });
                    println!("{}", serde_json::to_string(&payload)?);
                } else if result.has_changes() {
                    println!("Compacted table {}", table.ident);
                } else {
                    println!("No files to compact");
                }

                Ok(())
            }
            Commands::RewriteManifests(args) => {
                anyhow::ensure!(
                    !args.table.is_location(),
                    "--location is not supported for rewrite-manifests"
                );

                let LoadedTable::Catalog(table) =
                    table_ref::load_table(&args.table, &self.catalog).await?
                else {
                    anyhow::bail!("--location is not supported for rewrite-manifests");
                };

                let before_snapshot = table.table.metadata().current_snapshot_id();
                let start = Instant::now();

                let tx = Transaction::new(&table.table);
                let tx = tx
                    .rewrite_manifests()
                    .apply(tx)
                    .map_err(anyhow::Error::from)?;
                let table_after = tx
                    .commit(table.catalog.as_ref())
                    .await
                    .map_err(anyhow::Error::from)?;

                let duration_ms = start.elapsed().as_millis() as u64;
                let after_snapshot = table_after.metadata().current_snapshot_id();
                let changed = after_snapshot != before_snapshot;

                if self.output == OutputFormat::Json {
                    let payload = json!({
                        "command": "rewrite-manifests",
                        "table": table.ident.to_string(),
                        "result": {
                            "changed": changed,
                            "duration_ms": duration_ms,
                        }
                    });
                    println!("{}", serde_json::to_string(&payload)?);
                } else {
                    println!("Rewrote manifests");
                }

                Ok(())
            }
            Commands::ExpireSnapshots(args) => {
                anyhow::ensure!(
                    !args.table.is_location(),
                    "--location is not supported for expire-snapshots"
                );

                let LoadedTable::Catalog(table) =
                    table_ref::load_table(&args.table, &self.catalog).await?
                else {
                    anyhow::bail!("--location is not supported for expire-snapshots");
                };

                let retention = chrono::Duration::from_std(args.older_than.into())
                    .context("--older-than is out of range")?;
                let cutoff = Utc::now() - retention;

                let tx = Transaction::new(&table.table);
                let mut action = tx.expire_snapshots().older_than(cutoff);

                if args.dry_run {
                    action = action.delete_files(true).dry_run(true);
                } else if args.delete_files {
                    action = action.delete_files(true);
                }

                let plan = action
                    .plan(&table.table)
                    .await
                    .map_err(anyhow::Error::from)?;
                let options = action.options();
                let planned_files = if options.delete_files {
                    plan.cleanup_plan().total_files() as u64
                } else {
                    0
                };

                let result = action
                    .execute_with_plan(&table.table, table.catalog.as_ref(), plan, options)
                    .await
                    .map_err(anyhow::Error::from)?;

                let deleted_files = if options.dry_run {
                    0
                } else {
                    result.total_files_deleted()
                };
                let partial_failure =
                    options.delete_files && !options.dry_run && deleted_files < planned_files;

                if self.output == OutputFormat::Json {
                    let payload = json!({
                        "command": "expire-snapshots",
                        "dry_run": args.dry_run,
                        "planned_files": planned_files,
                        "deleted_files": deleted_files,
                        "partial_failure": partial_failure,
                        "result": {
                            "deleted_snapshots_count": result.deleted_snapshots_count,
                            "duration_ms": result.duration_ms,
                        }
                    });
                    println!("{}", serde_json::to_string(&payload)?);
                } else if args.dry_run {
                    println!(
                        "Would expire {} snapshots ({} planned files)",
                        result.deleted_snapshots_count, planned_files
                    );
                } else {
                    println!(
                        "Expired {} snapshots ({} deleted files)",
                        result.deleted_snapshots_count, deleted_files
                    );
                }

                Ok(())
            }
            Commands::RemoveOrphans(args) => {
                anyhow::ensure!(
                    !args.table.is_location(),
                    "--location is not supported for remove-orphans"
                );

                let LoadedTable::Catalog(table) =
                    table_ref::load_table(&args.table, &self.catalog).await?
                else {
                    anyhow::bail!("--location is not supported for remove-orphans");
                };

                let mut action = table.table.remove_orphan_files();

                if let Some(retention) = args.older_than {
                    let retention = chrono::Duration::from_std(retention.into())
                        .context("--older-than is out of range")?;
                    action = action.with_retention(retention);
                }

                action = action.dry_run(args.dry_run);

                let result = action.execute().await.map_err(anyhow::Error::from)?;

                let planned_files = result.orphan_files.len() as u64;
                let deleted_files = if result.dry_run {
                    0
                } else {
                    result.total_deleted_files()
                };
                let partial_failure = !result.dry_run && deleted_files < planned_files;
                let duration_ms = result.duration.as_millis() as u64;

                if self.output == OutputFormat::Json {
                    let payload = json!({
                        "command": "remove-orphans",
                        "dry_run": args.dry_run,
                        "planned_files": planned_files,
                        "deleted_files": deleted_files,
                        "partial_failure": partial_failure,
                        "result": {
                            "deleted_files": deleted_files,
                            "duration_ms": duration_ms,
                        }
                    });
                    println!("{}", serde_json::to_string(&payload)?);
                } else if args.dry_run {
                    println!("Would delete {planned_files} orphan files");
                } else {
                    println!("Deleted {deleted_files} orphan files");
                }

                Ok(())
            }
            Commands::RewriteDeletes(args) => {
                anyhow::ensure!(
                    !args.table.is_location(),
                    "--location is not supported for rewrite-deletes"
                );

                let LoadedTable::Catalog(table) =
                    table_ref::load_table(&args.table, &self.catalog).await?
                else {
                    anyhow::bail!("--location is not supported for rewrite-deletes");
                };

                let before_snapshot = table.table.metadata().current_snapshot_id();
                let start = Instant::now();

                let tx = Transaction::new(&table.table);
                let tx = tx
                    .rewrite_position_delete_files()
                    .apply(tx)
                    .map_err(anyhow::Error::from)?;
                let table_after = tx.commit(table.catalog.as_ref()).await.map_err(anyhow::Error::from)?;

                let duration_ms = start.elapsed().as_millis() as u64;
                let after_snapshot = table_after.metadata().current_snapshot_id();
                let changed = after_snapshot != before_snapshot;

                if self.output == OutputFormat::Json {
                    let payload = json!({
                        "command": "rewrite-deletes",
                        "table": table.ident.to_string(),
                        "result": {
                            "changed": changed,
                            "duration_ms": duration_ms,
                        }
                    });
                    println!("{}", serde_json::to_string(&payload)?);
                } else if changed {
                    println!("Rewrote position delete files");
                } else {
                    println!("No position delete files to rewrite");
                }

                Ok(())
            }
        }
    }
}
