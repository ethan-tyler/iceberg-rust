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

use std::path::PathBuf;

use anyhow::Context;
use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    /// Human-readable output (default).
    Human,
    /// Machine-readable JSON output.
    Json,
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

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Compact a table's data files.
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

    /// Rewrite position delete files (not yet implemented).
    #[command(name = "rewrite-deletes")]
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

    /// Catalog URI (e.g., rest://localhost:8181).
    #[arg(long)]
    pub catalog: Option<String>,

    /// Catalog config file path (YAML).
    #[arg(long)]
    pub catalog_config: Option<PathBuf>,
}

impl TableRefArgs {
    pub fn is_location(&self) -> bool {
        self.location.is_some()
    }

    pub fn is_table(&self) -> bool {
        self.table.is_some()
    }

    pub fn require_catalog(&self) -> anyhow::Result<()> {
        if self.is_location() {
            return Ok(());
        }

        let has_catalog = self.catalog.is_some() || self.catalog_config.is_some();
        anyhow::ensure!(
            has_catalog,
            "--catalog or --catalog-config is required when using --table"
        );
        Ok(())
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

    #[arg(long, value_enum)]
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
    pub older_than: humantime::Duration,

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
                args.table.require_catalog()?;
                anyhow::bail!("compact not implemented yet")
            }
            Commands::RewriteManifests(args) => {
                anyhow::ensure!(
                    !args.table.is_location(),
                    "--location is not supported for rewrite-manifests"
                );
                args.table.require_catalog()?;
                anyhow::bail!("rewrite-manifests not implemented yet")
            }
            Commands::ExpireSnapshots(args) => {
                if args.table.is_table() {
                    args.table.require_catalog()?;
                }

                if args.table.is_location() && !args.dry_run {
                    anyhow::bail!("--location mode only supports --dry-run")
                }

                anyhow::bail!("expire-snapshots not implemented yet")
            }
            Commands::RemoveOrphans(args) => {
                if args.table.is_table() {
                    args.table.require_catalog()?;
                }

                if args.table.is_location() && !args.dry_run {
                    anyhow::bail!("--location mode only supports --dry-run")
                }

                anyhow::bail!("remove-orphans not implemented yet")
            }
            Commands::RewriteDeletes(args) => {
                anyhow::ensure!(
                    !args.table.is_location(),
                    "--location is not supported for rewrite-deletes"
                );
                args.table.require_catalog()?;
                anyhow::bail!("rewrite-deletes not implemented yet")
            }
        }
    }
}
