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
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use iceberg::Catalog;
use iceberg_catalog_loader::CatalogLoader;
use url::Url;

const REST_CATALOG_PROP_URI_KEY: &str = "uri";

#[derive(Debug, Clone, serde::Deserialize)]
pub struct CatalogConfigFile {
    /// Optional catalog name (used only for logging/debugging).
    pub name: Option<String>,

    /// Catalog type (e.g. "rest", "glue", "hms", "sql").
    #[serde(rename = "type")]
    pub catalog_type: String,

    /// Catalog properties.
    #[serde(default)]
    pub props: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct ParsedCatalogUri {
    pub catalog_type: String,
    pub props: HashMap<String, String>,
}

pub fn parse_catalog_uri(uri: &str) -> anyhow::Result<ParsedCatalogUri> {
    let parsed = Url::parse(uri).with_context(|| "invalid --catalog URI")?;

    let (catalog_type, inner_scheme) = match parsed.scheme().split_once('+') {
        Some((t, inner)) => (t.to_string(), inner.to_string()),
        None => (parsed.scheme().to_string(), "http".to_string()),
    };

    if catalog_type == "rest" {
        let mut http_url = parsed;
        http_url.set_scheme(&inner_scheme).map_err(|()| {
            anyhow::anyhow!("invalid inner scheme for rest catalog: {inner_scheme}")
        })?;

        let mut props = HashMap::new();
        props.insert(REST_CATALOG_PROP_URI_KEY.to_string(), http_url.to_string());
        return Ok(ParsedCatalogUri {
            catalog_type,
            props,
        });
    }

    anyhow::bail!(
        "unsupported --catalog URI scheme: {catalog_type} (supported: rest://, rest+http://, rest+https://)"
    )
}

pub fn expand_tilde(path: &Path) -> PathBuf {
    let Some(path_str) = path.to_str() else {
        return path.to_path_buf();
    };

    if path_str == "~" {
        return dirs::home_dir().unwrap_or_else(|| path.to_path_buf());
    }

    let Some(rest) = path_str.strip_prefix("~/") else {
        return path.to_path_buf();
    };

    let Some(home) = dirs::home_dir() else {
        return path.to_path_buf();
    };

    home.join(rest)
}

pub fn load_catalog_config_file(path: &Path) -> anyhow::Result<CatalogConfigFile> {
    let path = expand_tilde(path);
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read catalog config: {}", path.display()))?;
    serde_yaml::from_str(&raw)
        .with_context(|| format!("failed to parse catalog config: {}", path.display()))
}

pub async fn load_catalog(
    catalog_uri: Option<&str>,
    catalog_config_path: Option<&Path>,
) -> anyhow::Result<Arc<dyn Catalog>> {
    let name: String;
    let catalog_type: String;
    let mut props: HashMap<String, String>;

    if let Some(config_path) = catalog_config_path {
        let cfg = load_catalog_config_file(config_path)?;

        catalog_type = cfg.catalog_type;
        name = cfg.name.unwrap_or_else(|| catalog_type.clone());
        props = cfg.props;
    } else {
        let uri = catalog_uri.context("--catalog is required when --catalog-config is not set")?;
        let parsed = parse_catalog_uri(uri)?;
        catalog_type = parsed.catalog_type;
        name = catalog_type.clone();
        props = parsed.props;
    }

    if let Some(uri) = catalog_uri {
        let parsed = parse_catalog_uri(uri)?;
        anyhow::ensure!(
            parsed.catalog_type == catalog_type,
            "--catalog scheme '{}' does not match catalog config type '{}'",
            parsed.catalog_type,
            catalog_type
        );
        props.extend(parsed.props);
    }

    let loader = CatalogLoader::from(catalog_type.as_str());
    loader.load(name, props).await.map_err(anyhow::Error::from)
}
