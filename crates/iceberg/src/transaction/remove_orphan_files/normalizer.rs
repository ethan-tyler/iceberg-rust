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

//! File URI normalization for comparing paths across different cloud storage schemes.
//!
//! Different tools and systems may refer to the same physical storage location
//! using different URI schemes (e.g., `s3://`, `s3a://`, `s3n://` all refer to S3).
//! This module provides utilities to normalize these URIs for consistent comparison.

use std::collections::HashMap;

use url::Url;

/// Normalizes file URIs for comparison across different cloud storage schemes.
///
/// # Example
///
/// ```ignore
/// let normalizer = FileUriNormalizer::with_defaults();
/// let uri1 = normalizer.normalize("s3://bucket/path/file.parquet")?;
/// let uri2 = normalizer.normalize("s3a://bucket/path/file.parquet")?;
/// assert_eq!(uri1.normalized, uri2.normalized);
/// ```
#[derive(Debug, Clone)]
pub struct FileUriNormalizer {
    /// Mapping of schemes to their canonical form.
    /// Key can be a single scheme or comma-separated list.
    equal_schemes: HashMap<String, String>,
    /// Mapping of authorities to their canonical form.
    equal_authorities: HashMap<String, String>,
}

impl Default for FileUriNormalizer {
    fn default() -> Self {
        Self::with_defaults()
    }
}

impl FileUriNormalizer {
    /// Create a normalizer with custom mappings.
    pub fn new(
        equal_schemes: HashMap<String, String>,
        equal_authorities: HashMap<String, String>,
    ) -> Self {
        Self {
            equal_schemes,
            equal_authorities,
        }
    }

    /// Create with default cloud storage equivalences.
    ///
    /// Default scheme mappings:
    /// - `s3`, `s3a`, `s3n` -> `s3`
    /// - `abfs`, `abfss` -> `abfs`
    /// - `wasb`, `wasbs` -> `wasb`
    /// - `gs` -> `gs`
    pub fn with_defaults() -> Self {
        let mut equal_schemes = HashMap::new();

        // S3 scheme variations all refer to the same storage
        equal_schemes.insert("s3".into(), "s3".into());
        equal_schemes.insert("s3a".into(), "s3".into());
        equal_schemes.insert("s3n".into(), "s3".into());

        // Azure scheme variations
        equal_schemes.insert("abfs".into(), "abfs".into());
        equal_schemes.insert("abfss".into(), "abfs".into());
        equal_schemes.insert("wasb".into(), "wasb".into());
        equal_schemes.insert("wasbs".into(), "wasb".into());

        // GCS - typically consistent
        equal_schemes.insert("gs".into(), "gs".into());
        equal_schemes.insert("gcs".into(), "gs".into());

        // Local file scheme
        equal_schemes.insert("file".into(), "file".into());

        Self {
            equal_schemes,
            equal_authorities: HashMap::new(),
        }
    }

    /// Add additional scheme equivalence.
    pub fn with_scheme_equivalence(mut self, scheme: impl Into<String>, canonical: impl Into<String>) -> Self {
        self.equal_schemes.insert(scheme.into(), canonical.into());
        self
    }

    /// Add additional authority equivalence.
    pub fn with_authority_equivalence(mut self, authority: impl Into<String>, canonical: impl Into<String>) -> Self {
        self.equal_authorities.insert(authority.into(), canonical.into());
        self
    }

    /// Normalize a file path for comparison.
    ///
    /// Returns a [`NormalizedUri`] containing both the original and normalized paths.
    pub fn normalize(&self, path: &str) -> NormalizedUri {
        match Url::parse(path) {
            Ok(url) => {
                let original_scheme = url.scheme();
                let scheme = self.normalize_scheme(original_scheme);
                let authority = url
                    .host_str()
                    .map(|h| self.normalize_authority(h))
                    .unwrap_or("");
                let path_part = url.path();

                // Check if the scheme was found in our mappings
                let scheme_matched = self.equal_schemes.contains_key(original_scheme);
                let authority_matched = url
                    .host_str()
                    .map(|h| self.equal_authorities.contains_key(h))
                    .unwrap_or(false);

                let normalized = format!("{}://{}{}", scheme, authority, path_part);

                NormalizedUri {
                    original: path.to_string(),
                    normalized,
                    scheme: scheme.to_string(),
                    authority: authority.to_string(),
                    path: path_part.to_string(),
                    prefix_matched: scheme_matched || authority_matched,
                }
            }
            Err(_) => {
                // Handle local file paths that don't parse as URLs
                NormalizedUri {
                    original: path.to_string(),
                    normalized: path.to_string(),
                    scheme: "file".to_string(),
                    authority: String::new(),
                    path: path.to_string(),
                    prefix_matched: true,
                }
            }
        }
    }

    /// Normalize a scheme to its canonical form.
    fn normalize_scheme<'a>(&'a self, scheme: &'a str) -> &'a str {
        // Check direct mapping first
        if let Some(canonical) = self.equal_schemes.get(scheme) {
            return canonical;
        }

        // Check for comma-separated key mappings
        for (key, canonical) in &self.equal_schemes {
            if key.contains(',') {
                for s in key.split(',') {
                    if s.trim() == scheme {
                        return canonical;
                    }
                }
            }
        }

        scheme
    }

    /// Normalize an authority to its canonical form.
    fn normalize_authority<'a>(&'a self, authority: &'a str) -> &'a str {
        if let Some(canonical) = self.equal_authorities.get(authority) {
            return canonical;
        }

        // Check for comma-separated key mappings
        for (key, canonical) in &self.equal_authorities {
            if key.contains(',') {
                for a in key.split(',') {
                    if a.trim() == authority {
                        return canonical;
                    }
                }
            }
        }

        authority
    }
}

/// Result of normalizing a URI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedUri {
    /// Original path as provided.
    pub original: String,
    /// Normalized path for comparison.
    pub normalized: String,
    /// Normalized scheme.
    pub scheme: String,
    /// Normalized authority (host).
    pub authority: String,
    /// Path component (without scheme and authority).
    pub path: String,
    /// Whether the prefix (scheme/authority) matched a known mapping.
    pub prefix_matched: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_s3_schemes() {
        let normalizer = FileUriNormalizer::with_defaults();

        let uri1 = normalizer.normalize("s3://bucket/path/file.parquet");
        let uri2 = normalizer.normalize("s3a://bucket/path/file.parquet");
        let uri3 = normalizer.normalize("s3n://bucket/path/file.parquet");

        assert_eq!(uri1.normalized, "s3://bucket/path/file.parquet");
        assert_eq!(uri2.normalized, "s3://bucket/path/file.parquet");
        assert_eq!(uri3.normalized, "s3://bucket/path/file.parquet");

        assert_eq!(uri1.normalized, uri2.normalized);
        assert_eq!(uri2.normalized, uri3.normalized);
    }

    #[test]
    fn test_normalize_azure_schemes() {
        let normalizer = FileUriNormalizer::with_defaults();

        let uri1 = normalizer.normalize("abfs://container@account.dfs.core.windows.net/path/file.parquet");
        let uri2 = normalizer.normalize("abfss://container@account.dfs.core.windows.net/path/file.parquet");

        assert_eq!(uri1.scheme, "abfs");
        assert_eq!(uri2.scheme, "abfs");
    }

    #[test]
    fn test_normalize_gcs_schemes() {
        let normalizer = FileUriNormalizer::with_defaults();

        let uri1 = normalizer.normalize("gs://bucket/path/file.parquet");
        let uri2 = normalizer.normalize("gcs://bucket/path/file.parquet");

        assert_eq!(uri1.scheme, "gs");
        assert_eq!(uri2.scheme, "gs");
    }

    #[test]
    fn test_normalize_preserves_path() {
        let normalizer = FileUriNormalizer::with_defaults();

        let uri = normalizer.normalize("s3://bucket/warehouse/db/table/data/file.parquet");

        assert_eq!(uri.authority, "bucket");
        assert_eq!(uri.path, "/warehouse/db/table/data/file.parquet");
    }

    #[test]
    fn test_normalize_local_path() {
        let normalizer = FileUriNormalizer::with_defaults();

        let uri = normalizer.normalize("/local/path/to/file.parquet");

        assert_eq!(uri.scheme, "file");
        assert_eq!(uri.normalized, "/local/path/to/file.parquet");
        assert!(uri.prefix_matched);
    }

    #[test]
    fn test_normalize_file_scheme() {
        let normalizer = FileUriNormalizer::with_defaults();

        let uri = normalizer.normalize("file:///local/path/to/file.parquet");

        assert_eq!(uri.scheme, "file");
        assert_eq!(uri.path, "/local/path/to/file.parquet");
    }

    #[test]
    fn test_custom_scheme_equivalence() {
        let normalizer = FileUriNormalizer::new(HashMap::new(), HashMap::new())
            .with_scheme_equivalence("custom", "canonical");

        let uri = normalizer.normalize("custom://bucket/path");

        assert_eq!(uri.scheme, "canonical");
    }

    #[test]
    fn test_custom_authority_equivalence() {
        let normalizer = FileUriNormalizer::with_defaults()
            .with_authority_equivalence("old-bucket", "new-bucket");

        let uri = normalizer.normalize("s3://old-bucket/path/file.parquet");

        assert_eq!(uri.authority, "new-bucket");
        assert_eq!(uri.normalized, "s3://new-bucket/path/file.parquet");
    }

    #[test]
    fn test_unknown_scheme_passthrough() {
        let normalizer = FileUriNormalizer::with_defaults();

        let uri = normalizer.normalize("hdfs://namenode/path/file.parquet");

        assert_eq!(uri.scheme, "hdfs");
        assert_eq!(uri.normalized, "hdfs://namenode/path/file.parquet");
    }

    #[test]
    fn test_prefix_matched_known_scheme() {
        let normalizer = FileUriNormalizer::with_defaults();

        let uri = normalizer.normalize("s3://bucket/path");
        assert!(uri.prefix_matched);
    }

    #[test]
    fn test_prefix_matched_unknown_scheme() {
        let normalizer = FileUriNormalizer::with_defaults();

        let uri = normalizer.normalize("hdfs://namenode/path");
        // hdfs is not in default mappings but scheme equals itself
        assert!(!uri.prefix_matched);
    }
}
