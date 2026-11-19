//! Wire types for the Iceberg REST catalog spec (specs/lakehouse/03-catalog.md §2).
//!
//! M1 slice 1: `GET /v1/config`. Field names follow the Iceberg REST OpenAPI
//! spec exactly (serde rename where rust idioms differ).

use serde::Deserialize;
use std::collections::BTreeMap;

/// Response of `GET /v1/config` (Iceberg REST spec `CatalogConfig`).
///
/// Tolerant of newer servers: unknown keys are ignored (the spec evolves) and
/// missing maps default to empty.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CatalogConfig {
    /// Server-provided defaults for client configuration.
    #[serde(default)]
    pub defaults: BTreeMap<String, String>,
    /// Server overrides — highest precedence in the merge order
    /// `overrides > table properties > defaults > client` (doc 03 §2).
    #[serde(default)]
    pub overrides: BTreeMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// TDD anchor (M1 slice 1): a spec-shaped `GET /v1/config` response parses
    /// into CatalogConfig with defaults/overrides intact.
    #[test]
    fn catalog_config_from_json_parses_defaults_and_overrides() {
        let json = r#"{
            "defaults": {
                "s3.endpoint": "http://s3:9000",
                "s3.path-style-access": "true"
            },
            "overrides": {
                "write.format.default": "parquet"
            }
        }"#;
        let cfg: CatalogConfig = serde_json::from_str(json).expect("parse CatalogConfig");
        assert_eq!(
            cfg.defaults.get("s3.endpoint").map(String::as_str),
            Some("http://s3:9000")
        );
        assert_eq!(
            cfg.overrides
                .get("write.format.default")
                .map(String::as_str),
            Some("parquet")
        );
    }

    /// Empty config objects are valid (minimal servers may omit keys entirely).
    #[test]
    fn catalog_config_from_empty_json() {
        let cfg: CatalogConfig = serde_json::from_str("{}").expect("parse empty CatalogConfig");
        assert!(cfg.defaults.is_empty());
        assert!(cfg.overrides.is_empty());
    }
}

/// Response of loadTable (Iceberg REST spec `LoadTableResult`).
/// `metadata` is the deprecated in-band metadata field; when a server
/// includes it we parse it eagerly (v3-first model) — otherwise the caller
/// fetches `metadata_location` over file IO.
#[derive(Debug, Clone, Deserialize)]
pub struct LoadTableResult {
    #[serde(rename = "metadata-location")]
    pub metadata_location: String,
    #[serde(default)]
    pub config: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    pub metadata: Option<crate::catalog::metadata::TableMetadata>,
}
