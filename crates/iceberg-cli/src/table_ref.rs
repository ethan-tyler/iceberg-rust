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

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Context;
use iceberg::io::FileIO;
use iceberg::table::{StaticTable, Table};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use url::Url;

use crate::cli::{CatalogArgs, TableRefArgs};
use crate::config;

#[derive(Debug, Clone)]
pub struct CatalogTable {
    pub catalog: Arc<dyn Catalog>,
    pub ident: TableIdent,
    pub table: Table,
}

#[derive(Debug, Clone)]
pub struct LocationTable {
    pub location: String,
    pub metadata_location: String,
    pub table: Table,
}

#[derive(Debug, Clone)]
pub enum LoadedTable {
    Catalog(CatalogTable),
    Location(LocationTable),
}

pub async fn load_table(
    table_ref: &TableRefArgs,
    catalog_args: &CatalogArgs,
) -> anyhow::Result<LoadedTable> {
    if let Some(table_ident) = table_ref.table.as_deref() {
        let catalog = catalog_args.load_catalog().await?;

        let ident = parse_table_ident(table_ident)?;
        let table = catalog
            .load_table(&ident)
            .await
            .map_err(anyhow::Error::from)?;

        return Ok(LoadedTable::Catalog(CatalogTable {
            catalog,
            ident,
            table,
        }));
    }

    let location = table_ref.location_str()?.to_string();
    let mut props: HashMap<String, String> = HashMap::new();
    if let Some(config_path) = catalog_args.catalog_config.as_deref() {
        let cfg = config::load_catalog_config_file(config_path)?;
        props = cfg.props;
    }
    catalog_args.apply_storage_props(&mut props);

    let file_io = FileIO::from_path(&location)
        .map_err(anyhow::Error::from)?
        .with_props(props)
        .build()
        .map_err(anyhow::Error::from)?;

    let metadata_location = resolve_metadata_location(&file_io, &location).await?;
    let ident = derive_ident_from_location(&location)?;

    let table = StaticTable::from_metadata_file(&metadata_location, ident, file_io)
        .await
        .map_err(anyhow::Error::from)?
        .into_table();

    Ok(LoadedTable::Location(LocationTable {
        location,
        metadata_location,
        table,
    }))
}

pub fn parse_table_ident(table: &str) -> anyhow::Result<TableIdent> {
    if table.contains("://") {
        anyhow::bail!("Unsupported table reference: {table}");
    }

    let parts: Vec<&str> = table.split('.').filter(|s| !s.is_empty()).collect();

    anyhow::ensure!(
        parts.len() >= 2,
        "Invalid table identifier '{table}' (expected 'namespace.table')"
    );

    let (namespace_parts, name_part) = parts.split_at(parts.len() - 1);
    let namespace = NamespaceIdent::from_strs(namespace_parts)
        .map_err(anyhow::Error::from)
        .context("invalid namespace in --table")?;

    Ok(TableIdent::new(namespace, name_part[0].to_string()))
}

pub fn derive_ident_from_location(location: &str) -> anyhow::Result<TableIdent> {
    let parsed = Url::parse(location).with_context(|| "invalid --location URI")?;
    let name = parsed
        .path_segments()
        .and_then(|mut segs| segs.next_back())
        .filter(|s| !s.is_empty())
        .unwrap_or("table")
        .to_string();

    let namespace = NamespaceIdent::new("location".to_string());
    Ok(TableIdent::new(namespace, name))
}

pub async fn resolve_metadata_location(
    file_io: &FileIO,
    table_location: &str,
) -> anyhow::Result<String> {
    let table_location = table_location.trim_end_matches('/');
    let metadata_dir = format!("{table_location}/metadata");
    let version_hint_location = format!("{metadata_dir}/version-hint.text");

    let hinted_version = match file_io.exists(&version_hint_location).await {
        Ok(true) => {
            let input = file_io
                .new_input(&version_hint_location)
                .map_err(anyhow::Error::from)?;
            let bytes = input.read().await.map_err(anyhow::Error::from)?;
            let content = std::str::from_utf8(&bytes)
                .context("version-hint.text is not valid UTF-8")?
                .trim();
            if content.is_empty() {
                None
            } else {
                Some(
                    content
                        .parse::<u32>()
                        .context("invalid version-hint.text contents")?,
                )
            }
        }
        _ => None,
    };

    let mut candidates: Vec<(u32, String)> = Vec::new();
    let entries = file_io
        .list(&metadata_dir)
        .await
        .map_err(anyhow::Error::from)?;
    for entry in entries {
        if !entry.path.ends_with(".metadata.json") {
            continue;
        }
        let Some(version) = parse_metadata_version(&entry.path) else {
            continue;
        };
        candidates.push((version, entry.path));
    }

    anyhow::ensure!(
        !candidates.is_empty(),
        "no Iceberg metadata files found under: {metadata_dir}"
    );

    if let Some(target) = hinted_version
        && let Some((_, path)) = candidates.iter().find(|(v, _)| *v == target)
    {
        return Ok(path.clone());
    }

    candidates.sort_by_key(|(v, _)| *v);
    Ok(candidates
        .last()
        .expect("candidates is non-empty")
        .1
        .clone())
}

fn parse_metadata_version(path: &str) -> Option<u32> {
    let file_name = path.rsplit_once('/').map(|(_, f)| f).unwrap_or(path);
    let file_name = file_name.strip_suffix(".metadata.json")?;
    let (version_str, _uuid_str) = file_name.split_once('-')?;
    version_str.parse::<u32>().ok()
}
