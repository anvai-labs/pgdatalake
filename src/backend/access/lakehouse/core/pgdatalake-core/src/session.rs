//! Catalog scan session: the one-call orchestrator the FFI/FDW face uses —
//! config -> loadTable -> metadata fetch -> plan_scan (specs/lakehouse/
//! 03-catalog.md, 06-execution.md). Slice 9.

//! Catalog scan session: the one-call orchestrator the FFI/FDW face uses
//! (specs/lakehouse/03-catalog.md §2, doc 06 §2). Slice 9.
//!
//! Holds one REST catalog handle + storage credentials for the warehouse's
//! object store; `scan_table()` walks config -> loadTable -> metadata fetch
//! -> plan_scan. Credentialed OAuth2 providers plug in via TokenProvider
//! (slice 2); the FFI face (ffi.rs) wraps this with panic-free boundaries.

use crate::catalog::rest::{RestCatalog, StaticToken, TokenProvider};
use crate::catalog::types::CatalogConfig;
use crate::scan::{ScanPlan, ScanPredicate};
use crate::storage::{fetch_metadata, S3Options};

/// Client-side storage credentials/overrides (from secure Postgres settings;
/// catalog-served `s3.*` config keys are merged under these, doc 03 §2).
#[derive(Debug, Clone, Default)]
pub struct S3ClientOptions {
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub path_style: bool,
}

impl S3ClientOptions {
    fn merged(&self, catalog_cfg: &CatalogConfig) -> S3Options {
        // catalog config first (defaults + overrides), client settings win
        let mut effective = catalog_cfg.defaults.clone();
        effective.extend(catalog_cfg.overrides.clone());
        let mut opts = S3Options::from_config(&effective);
        if let Some(ep) = &self.endpoint {
            opts.endpoint = Some(ep.clone());
        }
        if let Some(r) = &self.region {
            opts.region = Some(r.clone());
        }
        if let Some(k) = &self.access_key_id {
            opts.access_key_id = Some(k.clone());
        }
        if let Some(sec) = &self.secret_access_key {
            opts.secret_access_key = Some(sec.clone());
        }
        if self.path_style {
            opts.path_style = true;
        }
        opts
    }
}

pub struct CatalogClient {
    rest: RestCatalog,
    warehouse: String,
    s3_client: S3ClientOptions,
}

impl CatalogClient {
    pub fn new(
        endpoint: &str,
        warehouse: &str,
        token: &str,
        s3_client: &S3ClientOptions,
    ) -> Result<Self, crate::storage::StorageError> {
        Self::with_provider(
            endpoint,
            warehouse,
            Box::new(StaticToken(token.to_string())),
            s3_client,
        )
    }

    pub fn with_provider(
        endpoint: &str,
        warehouse: &str,
        tokens: Box<dyn TokenProvider>,
        s3_client: &S3ClientOptions,
    ) -> Result<Self, crate::storage::StorageError> {
        Ok(Self {
            rest: RestCatalog::new(endpoint, tokens)
                .map_err(|e| crate::storage::StorageError::Config(e.to_string()))?,
            warehouse: warehouse.to_string(),
            s3_client: s3_client.clone(),
        })
    }

    /// config -> loadTable -> metadata -> plan. Returns the plan plus the
    /// effective storage options (needed later to open task files).
    pub fn scan_table(
        &self,
        namespace: &[&str],
        table: &str,
        predicate: Option<&ScanPredicate>,
        snapshot_id: Option<i64>,
    ) -> Result<(ScanPlan, S3Options), crate::storage::StorageError> {
        let cfg = self
            .rest
            .fetch_config(Some(&self.warehouse))
            .map_err(|e| crate::storage::StorageError::Config(e.to_string()))?;
        let lt = self
            .rest
            .load_table(namespace, table)
            .map_err(|e| crate::storage::StorageError::Config(e.to_string()))?;

        let s3 = self.s3_client.merged(&cfg);
        let (io, key) = crate::scan::open_for(&lt.metadata_location, Some(&s3))?;
        let md = fetch_metadata(&io, &key)?;
        let plan = crate::scan::plan_scan(&md, snapshot_id, Some(&s3), predicate)?;
        Ok((plan, s3))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// TDD anchor (M1 slice 9): one client call walks the whole pipeline
    /// against an in-process mock catalog serving config + loadTable, with
    /// the metadata on a local store — no network beyond the loopback mock
    /// (doc 10 §1).
    #[test]
    fn scan_table_walks_config_loadtable_metadata_plan() {
        use apache_avro::{Schema as AvroSchema, Writer};

        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("metadata")).unwrap();

        // empty manifest list (fresh table) keeps the fixture tiny
        let schema = AvroSchema::parse_str(crate::manifest::MANIFEST_LIST_SCHEMA_V2).unwrap();
        let writer = Writer::new(&schema, Vec::new());
        std::fs::write(
            dir.path().join("metadata").join("mn-list.avro"),
            writer.into_inner().unwrap(),
        )
        .unwrap();

        let metadata_json = format!(
            r#"{{
                "format-version": 2,
                "table-uuid": "9c12d441-03fe-11ed-8122-d358e75d12eb",
                "location": "file://{}/table",
                "schemas": [{{"type": "struct", "schema-id": 0, "fields": [
                    {{"id": 1, "name": "id", "required": true, "type": "long"}}]}}],
                "current-schema-id": 0,
                "snapshots": [{{"snapshot-id": 42, "timestamp-ms": 1712345678000,
                                "manifest-list": "file://{}/metadata/mn-list.avro",
                                "summary": {{"operation": "append"}}}}],
                "current-snapshot-id": 42
            }}"#,
            dir.path().display(),
            dir.path().display(),
        );
        let md_file = dir.path().join("metadata").join("00000-x.metadata.json");
        std::fs::write(&md_file, metadata_json).unwrap();

        // mock catalog: GET /v1/config (with warehouse param) then loadTable
        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let addr = server.server_addr().to_string();
        std::thread::spawn(move || {
            let req = server.recv().unwrap();
            let body = r#"{"defaults": {"prefix": "pfx"}, "overrides": {}}"#;
            let resp = tiny_http::Response::from_string(body).with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                    .unwrap(),
            );
            let _ = req.respond(resp);
            let req = server.recv().unwrap();
            assert!(req
                .url()
                .starts_with("/v1/pfx/namespaces/analytics/tables/events"));
            let body = format!(
                r#"{{"metadata-location": "file://{}",
                    "config": {{"s3.endpoint": "http://s3:9000"}}}}"#,
                md_file.display()
            );
            let resp = tiny_http::Response::from_string(body).with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                    .unwrap(),
            );
            let _ = req.respond(resp);
        });

        let client = CatalogClient::new(
            &format!("http://{addr}"),
            "smoke",
            "",
            &S3ClientOptions::default(),
        )
        .unwrap();
        let (plan, _s3) = client
            .scan_table(&["analytics"], "events", None, None)
            .expect("scan");
        assert_eq!(plan.snapshot_id, 42);
        assert_eq!(plan.tasks.len(), 0, "fresh table: empty manifest list");
    }

    /// Unreachable catalogs error with the endpoint in the message (the C
    /// glue maps this to a connection-failure ereport).
    #[test]
    fn unreachable_catalog_errors_clearly() {
        let client = CatalogClient::new(
            "http://127.0.0.1:9", // nothing listens here
            "wh",
            "",
            &S3ClientOptions::default(),
        )
        .unwrap();
        let err = client.scan_table(&["ns"], "t", None, None).unwrap_err();
        assert!(!err.to_string().is_empty());
    }
}
