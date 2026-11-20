//! REST catalog client (specs/lakehouse/03-catalog.md §2, tier 1).
//!
//! M1 slice 1: `GET /v1/config`. Auth model: bearer token from a
//! `TokenProvider` (OAuth2 client-credentials flow with proactive refresh
//! lands in the next slice; SigV4 profiles after that).

use std::time::Duration;

use serde::Deserialize;

use super::types::CatalogConfig;

/// Source of the `Authorization: Bearer …` value for each request.
pub trait TokenProvider: Send + Sync {
    fn token(&self) -> String;
}

/// Static bearer token (env-provided or pre-fetched); the trivial provider.
pub struct StaticToken(pub String);

impl TokenProvider for StaticToken {
    fn token(&self) -> String {
        self.0.clone()
    }
}

/// HTTP client for one REST catalog endpoint.
pub struct RestCatalog {
    pub(crate) endpoint: String,
    pub(crate) tokens: Box<dyn TokenProvider>,
    pub(crate) inner: reqwest::blocking::Client,
    /// Table-route prefix from the config response (`overrides.prefix` in
    /// the Iceberg REST spec); set via with_prefix() or fetch_config().
    pub(crate) prefix: std::sync::Mutex<Option<String>>,
}

/// Wire type for the config response; alias to the domain type.
#[derive(Deserialize)]
struct ConfigWire {
    #[serde(default)]
    defaults: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    overrides: std::collections::BTreeMap<String, String>,
}

#[derive(Debug)]
pub enum RestError {
    Http(String),
    /// 404 from loadTable/dropTable etc. — distinct because the caller can
    /// react (doc 03 §5: remote table deleted under us → rel invalid).
    NotFound(String),
    /// commitTable returned 409 CommitFailedException — a requirement we
    /// sent failed server-side (concurrent commit won). The ONLY retryable
    /// error class (doc 03 §5): commit_with_retry re-plans from a fresh
    /// loadTable; everything else aborts the transaction.
    Conflict(String),
    Decode(String),
}

impl std::fmt::Display for RestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RestError::Http(m) => write!(f, "REST catalog HTTP error: {m}"),
            RestError::NotFound(m) => write!(f, "REST catalog object not found: {m}"),
            RestError::Conflict(m) => {
                write!(f, "REST catalog commit conflict (stale base): {m}")
            }
            RestError::Decode(m) => write!(f, "REST catalog decode error: {m}"),
        }
    }
}

impl std::error::Error for RestError {}

impl RestCatalog {
    pub fn new(endpoint: &str, tokens: Box<dyn TokenProvider>) -> Result<Self, RestError> {
        Ok(Self {
            endpoint: endpoint.trim_end_matches('/').to_string(),
            tokens,
            inner: reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .map_err(|e| RestError::Http(e.to_string()))?,
            prefix: std::sync::Mutex::new(None),
        })
    }

    /// Set the table-route prefix explicitly (normally stored automatically
    /// by fetch_config from the response's `overrides.prefix`).
    pub fn with_prefix(self, prefix: &str) -> Self {
        *self.prefix.lock().expect("prefix mutex poisoned") = Some(prefix.to_string());
        self
    }

    /// `GET /v1/{prefix}/namespaces/{ns}/tables/{table}` — loadTable
    /// (doc 03 §2). Namespace levels are URL-encoded and joined by %1F per
    /// the Iceberg REST spec; the table name is URL-encoded too. Uses the
    /// stored prefix when present.
    pub fn load_table(
        &self,
        namespace: &[&str],
        table: &str,
    ) -> Result<super::types::LoadTableResult, RestError> {
        let prefix = self.prefix.lock().expect("prefix mutex poisoned").clone();
        let ns = namespace
            .iter()
            .map(|c| urlencoding::encode(c))
            .collect::<Vec<_>>()
            .join("%1F");
        // branch/snapshot pinning via query params lands with time travel (doc 02 §4)
        let url = format!(
            "{}/v1{}/namespaces/{ns}/tables/{}",
            self.endpoint,
            prefix.as_ref().map(|p| format!("/{p}")).unwrap_or_default(),
            urlencoding::encode(table),
        );
        let resp = self
            .inner
            .get(&url)
            .bearer_auth(self.tokens.token())
            .send()
            .map_err(|e| RestError::Http(e.to_string()))?;
        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(RestError::NotFound(format!("GET {url} -> {status}")));
        }
        if !status.is_success() {
            return Err(RestError::Http(format!("GET {url} -> {status}")));
        }
        let wire: super::types::LoadTableResult =
            resp.json().map_err(|e| RestError::Decode(e.to_string()))?;
        Ok(wire)
    }

    /// `POST /v1/{prefix}/namespaces` — createNamespace; 409 (already
    /// exists) surfaces as Conflict so callers can upsert.
    pub fn create_namespace(&self, namespace: &[&str]) -> Result<(), RestError> {
        let prefix = self.prefix.lock().expect("prefix mutex poisoned").clone();
        let ns_json = namespace
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(",");
        let url = format!(
            "{}/v1{}/namespaces",
            self.endpoint,
            prefix.as_ref().map(|p| format!("/{p}")).unwrap_or_default()
        );
        let body = format!("{{\"namespace\": [{ns_json}]}}");
        let resp = self
            .inner
            .post(&url)
            .bearer_auth(self.tokens.token())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .map_err(|e| RestError::Http(e.to_string()))?;
        let status = resp.status();
        if status == reqwest::StatusCode::CONFLICT {
            return Err(RestError::Conflict(format!("POST {url} -> {status}")));
        }
        if !status.is_success() {
            return Err(RestError::Http(format!("POST {url} -> {status}")));
        }
        Ok(())
    }

    /// `POST /v1/{prefix}/namespaces/{ns}/tables` — createTable with the
    /// schema (server assigns metadata location + uuid).
    pub fn create_table(
        &self,
        namespace: &[&str],
        table: &str,
        schema: &super::metadata::Schema,
    ) -> Result<super::types::LoadTableResult, RestError> {
        let prefix = self.prefix.lock().expect("prefix mutex poisoned").clone();
        let ns = namespace
            .iter()
            .map(|c| urlencoding::encode(c))
            .collect::<Vec<_>>()
            .join("%1F");
        let url = format!(
            "{}/v1{}/namespaces/{ns}/tables",
            self.endpoint,
            prefix.as_ref().map(|p| format!("/{p}")).unwrap_or_default()
        );
        let payload = serde_json::json!({
            "name": table,
            "schema": schema,
            "properties": {},
        });
        let resp = self
            .inner
            .post(&url)
            .bearer_auth(self.tokens.token())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(serde_json::to_vec(&payload).map_err(|e| RestError::Http(e.to_string()))?)
            .send()
            .map_err(|e| RestError::Http(e.to_string()))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(RestError::Http(format!("POST {url} -> {status}")));
        }
        let text = resp.text().unwrap_or_default();
        serde_json::from_str(&text).map_err(|e| {
            RestError::Decode(format!(
                "createTable response: {e}; body starts: {:?}",
                text.chars().take(200).collect::<String>()
            ))
        })
    }

    /// `GET /v1/config` — server capabilities and config defaults/overrides.
    ///
    /// `warehouse` is optional per the REST spec but some profiles (e.g.
    /// Lakekeeper) require it — doc 03 §2: profile quirks are data, and the
    /// caller supplies the right value per profile.
    pub fn fetch_config(&self, warehouse: Option<&str>) -> Result<CatalogConfig, RestError> {
        let mut url = format!("{}/v1/config", self.endpoint);
        if let Some(w) = warehouse {
            url.push_str(&format!("?warehouse={}", urlencoding::encode(w)));
        }
        let resp = self
            .inner
            .get(&url)
            .bearer_auth(self.tokens.token())
            .send()
            .map_err(|e| RestError::Http(e.to_string()))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(RestError::Http(format!("GET {url} -> {status}")));
        }
        let wire: ConfigWire = resp.json().map_err(|e| RestError::Decode(e.to_string()))?;
        let cfg = CatalogConfig {
            defaults: wire.defaults,
            overrides: wire.overrides,
        };
        // Effective config = defaults overlaid by overrides (doc 03 §2 merge
        // order: overrides > table properties > defaults > client). Table
        // properties and client settings are applied by later layers; the
        // table-route prefix resolves from the merged server view.
        let mut effective = cfg.defaults.clone();
        effective.extend(cfg.overrides.clone());
        if let Some(prefix) = effective.get("prefix") {
            *self.prefix.lock().expect("prefix mutex poisoned") = Some(prefix.clone());
        }
        Ok(cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mock of `GET /v1/config` on an in-process server (doc 10 §1: no
    /// network at unit-test time). Asserts the client hits the spec path and
    /// sends the bearer token, then returns a canned config response.
    #[test]
    fn fetch_config_sends_auth_and_parses_response() {
        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let addr = server.server_addr().to_string();
        let path_seen = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
        let auth_seen = std::sync::Arc::new(std::sync::Mutex::new(String::new()));

        let server_thread = {
            let path_seen = std::sync::Arc::clone(&path_seen);
            let auth_seen = std::sync::Arc::clone(&auth_seen);
            std::thread::spawn(move || {
                let req = server.recv().unwrap();
                *path_seen.lock().unwrap() = req.url().to_string();
                for h in req.headers() {
                    if h.field
                        .as_str()
                        .to_string()
                        .eq_ignore_ascii_case("Authorization")
                    {
                        *auth_seen.lock().unwrap() = h.value.as_str().to_string();
                    }
                }
                let body = r#"{"defaults": {"s3.endpoint": "http://s3:9000"},
                                "overrides": {}}"#;
                let resp = tiny_http::Response::from_string(body).with_header(
                    tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                        .unwrap(),
                );
                let _ = req.respond(resp);
            })
        };

        let catalog = RestCatalog::new(
            &format!("http://{addr}"),
            Box::new(StaticToken("tok-123".into())),
        )
        .unwrap();
        let cfg = catalog.fetch_config(None).expect("fetch_config");

        server_thread.join().unwrap();
        assert_eq!(*path_seen.lock().unwrap(), "/v1/config", "spec path");
        assert_eq!(*auth_seen.lock().unwrap(), "Bearer tok-123", "auth header");
        assert_eq!(
            cfg.defaults.get("s3.endpoint").map(String::as_str),
            Some("http://s3:9000")
        );
    }

    /// Warehouse parameter is appended and URL-encoded (Lakekeeper requires
    /// it; doc 03 §2 profile quirk handled by the caller).
    #[test]
    fn fetch_config_warehouse_param_appended_and_encoded() {
        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let addr = server.server_addr().to_string();
        let url_seen = std::sync::Arc::new(std::sync::Mutex::new(String::new()));

        let server_thread = {
            let url_seen = std::sync::Arc::clone(&url_seen);
            std::thread::spawn(move || {
                let req = server.recv().unwrap();
                *url_seen.lock().unwrap() = req.url().to_string();
                let resp = tiny_http::Response::from_string("{}");
                let _ = req.respond(resp);
            })
        };

        let catalog =
            RestCatalog::new(&format!("http://{addr}"), Box::new(StaticToken("t".into()))).unwrap();
        let cfg = catalog
            .fetch_config(Some("analytics/eu table"))
            .expect("fetch_config");
        server_thread.join().unwrap();
        assert_eq!(
            *url_seen.lock().unwrap(),
            "/v1/config?warehouse=analytics%2Feu%20table"
        );
        assert!(cfg.defaults.is_empty());
    }

    /// loadTable (doc 03 §2): GET /v1/{prefix}/namespaces/{ns}/tables/{tbl}
    /// with namespace levels joined by %1F and components URL-encoded; the
    /// catalog prefix comes from the config response (fetch_config stores it).
    #[test]
    fn load_table_builds_spec_path_and_parses_result() {
        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let addr = server.server_addr().to_string();
        let url_seen = std::sync::Arc::new(std::sync::Mutex::new(String::new()));

        let server_thread = {
            let url_seen = std::sync::Arc::clone(&url_seen);
            std::thread::spawn(move || {
                let req = server.recv().unwrap();
                *url_seen.lock().unwrap() = req.url().to_string();
                let body = r#"{
                    "metadata-location": "s3://warehouse/analytics/events/metadata/00001-abc.metadata.json",
                    "config": {"s3.endpoint": "http://s3:9000"}
                }"#;
                let resp = tiny_http::Response::from_string(body).with_header(
                    tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                        .unwrap(),
                );
                let _ = req.respond(resp);
            })
        };

        let catalog =
            RestCatalog::new(&format!("http://{addr}"), Box::new(StaticToken("t".into())))
                .unwrap()
                .with_prefix("01a1f4b1-b095-7361");
        let lt = catalog
            .load_table(&["analytics", "eu"], "events 2026")
            .expect("load_table");

        server_thread.join().unwrap();
        assert_eq!(
            *url_seen.lock().unwrap(),
            "/v1/01a1f4b1-b095-7361/namespaces/analytics%1Feu/tables/events%202026"
        );
        assert_eq!(
            lt.metadata_location,
            "s3://warehouse/analytics/events/metadata/00001-abc.metadata.json"
        );
        assert_eq!(
            lt.config.get("s3.endpoint").map(String::as_str),
            Some("http://s3:9000")
        );
    }

    /// A remote-dropped table (404, doc 03 §5) is a distinct error so the
    /// caller can mark the rel invalid instead of surfacing a generic error.
    #[test]
    fn load_table_404_is_not_found() {
        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let addr = server.server_addr().to_string();
        let server_thread = std::thread::spawn(move || {
            let req = server.recv().unwrap();
            let resp = tiny_http::Response::from_string(
                r#"{"error": {"message": "table not found", "type": "NoSuchTableException"}}"#,
            )
            .with_status_code(404);
            let _ = req.respond(resp);
        });

        let catalog =
            RestCatalog::new(&format!("http://{addr}"), Box::new(StaticToken("t".into()))).unwrap();
        let err = catalog.load_table(&["ns"], "gone").unwrap_err();
        server_thread.join().unwrap();
        assert!(matches!(err, RestError::NotFound(_)), "{err}");
    }

    /// Non-2xx responses surface as Http errors with the status code.
    #[test]
    fn fetch_config_surfaces_http_status() {
        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let addr = server.server_addr().to_string();
        let server_thread = std::thread::spawn(move || {
            let req = server.recv().unwrap();
            let resp = tiny_http::Response::from_string("not found").with_status_code(404);
            let _ = req.respond(resp);
        });

        let catalog =
            RestCatalog::new(&format!("http://{addr}"), Box::new(StaticToken("t".into()))).unwrap();
        let err = catalog.fetch_config(None).unwrap_err();
        server_thread.join().unwrap();
        assert!(
            matches!(err, RestError::Http(ref m) if m.contains("404")),
            "{err}"
        );
    }

    /// Trailing slashes on the endpoint don't produce `//v1/config`.
    #[test]
    fn endpoint_trailing_slash_normalized() {
        let catalog =
            RestCatalog::new("http://example.com/", Box::new(StaticToken("t".into()))).unwrap();
        assert_eq!(catalog.endpoint, "http://example.com");
        let url = format!("{}/v1/config", catalog.endpoint);
        assert!(!url.contains("com//v1"), "no doubled slash: {url}");
    }
}
