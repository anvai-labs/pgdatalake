//! REST catalog client (specs/lakehouse/03-catalog.md §2, tier 1).
//!
//! M1 slice 1: `GET /v1/config`. Auth model: bearer token from a
//! `TokenProvider` (`OAuth2` client-credentials flow with proactive refresh
//! lands in the next slice; `SigV4` profiles after that).

use std::io::Read;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use serde::Deserialize;

use super::types::CatalogConfig;

/// Source of the `Authorization: Bearer …` value for each request.
pub trait TokenProvider: Send + Sync {
    /// Resolve the bearer token for one request. Loud failure: a provider
    /// that cannot produce a usable token errors the request instead of
    /// degrading to an empty (or stale-expired) bearer (13-review #6).
    fn token(&self) -> Result<String, RestError>;
}

/// Static bearer token (env-provided or pre-fetched); the trivial provider.
pub struct StaticToken(pub String);

impl TokenProvider for StaticToken {
    fn token(&self) -> Result<String, RestError> {
        Ok(self.0.clone())
    }
}

/// Mask anything credential-shaped in a URL before it lands in an error
/// message (13-review #6): userinfo (`user:pass@`) and query VALUES are
/// redacted; scheme/host/path stay for diagnosis.
#[must_use]
pub fn redact_url(url: &str) -> String {
    let (base, query) = match url.split_once('?') {
        Some((b, q)) => (b, Some(q)),
        None => (url, None),
    };
    // userinfo: everything between "://" and the next "/"
    let base = match base.split_once("://") {
        Some((scheme, rest)) => match rest.split_once('/') {
            Some((authority, path)) => {
                let authority = match authority.split_once('@') {
                    Some((_, host)) => host,
                    None => authority,
                };
                format!("{scheme}://{authority}/{path}")
            }
            None => base.to_string(),
        },
        None => base.to_string(),
    };
    match query {
        Some(q) => {
            let redacted = q
                .split('&')
                .map(|kv| match kv.split_once('=') {
                    Some((k, _)) => format!("{k}=***"),
                    None => kv.to_string(),
                })
                .collect::<Vec<_>>()
                .join("&");
            format!("{base}?{redacted}")
        }
        None => base,
    }
}

/// Same-origin redirect policy (13-review #6): each hop must keep the
/// previous hop's scheme/host/port. A catalog that redirects off-origin
/// fails the request instead of silently sending credentials elsewhere.
fn same_origin_redirect_policy() -> reqwest::redirect::Policy {
    reqwest::redirect::Policy::custom(|attempt| {
        let prev = attempt.previous().last().cloned();
        let target = attempt.url().clone();
        let same_origin = match &prev {
            Some(p) => {
                let (a, b) = (p.origin(), target.origin());
                a == b && !a.ascii_serialization().is_empty()
            }
            None => true,
        };
        if same_origin {
            attempt.follow()
        } else {
            attempt.error(format!(
                "catalog redirect crosses origins: {} -> {} (refused)",
                redact_url(prev.as_ref().map_or_default(reqwest::Url::as_str)),
                redact_url(target.as_str())
            ))
        }
    })
}

/// HTTP client for one REST catalog endpoint.
#[derive(Clone)]
pub struct RestCatalog {
    pub(crate) endpoint: String,
    pub(crate) tokens: Arc<dyn TokenProvider>,
    pub(crate) inner: reqwest::blocking::Client,
    // Reads follow approved redirects. Commit POSTs may be sent only once.
    scoped: bool,
    commit_client: Arc<OnceLock<Result<reqwest::blocking::Client, String>>>,
    /// Table-route prefix from the config response (`overrides.prefix` in
    /// the Iceberg REST spec); set via `with_prefix()` or `fetch_config()`.
    pub(crate) prefix: Arc<Mutex<Option<String>>>,
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
    /// commitTable returned 409 `CommitFailedException` — a requirement we
    /// sent failed server-side (concurrent commit won). The ONLY retryable
    /// error class (doc 03 §5): `commit_with_retry` re-plans from a fresh
    /// loadTable; everything else aborts the transaction.
    Conflict(String),
    CommitStateUnknown(String),
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
            RestError::CommitStateUnknown(m) => write!(f, "catalog commit outcome unknown: {m}"),
            RestError::Decode(m) => write!(f, "REST catalog decode error: {m}"),
        }
    }
}

impl std::error::Error for RestError {}

impl RestCatalog {
    pub fn new(endpoint: &str, tokens: Box<dyn TokenProvider>) -> Result<Self, RestError> {
        Self::with_read_client(
            endpoint,
            tokens,
            reqwest::blocking::Client::builder().redirect(same_origin_redirect_policy()),
            false,
        )
    }

    pub(crate) fn new_scoped(
        endpoint: &str,
        tokens: Box<dyn TokenProvider>,
    ) -> Result<Self, RestError> {
        Self::with_read_client(
            endpoint,
            tokens,
            reqwest::blocking::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .no_proxy(),
            true,
        )
    }

    fn with_read_client(
        endpoint: &str,
        tokens: Box<dyn TokenProvider>,
        client: reqwest::blocking::ClientBuilder,
        scoped: bool,
    ) -> Result<Self, RestError> {
        Ok(Self {
            endpoint: endpoint.trim_end_matches('/').to_string(),
            tokens: tokens.into(),
            scoped,
            commit_client: Arc::default(),
            inner: client
                .timeout(Duration::from_secs(30))
                .build()
                .map_err(|e| RestError::Http(e.to_string()))?,
            prefix: Arc::default(),
        })
    }

    pub(super) fn commit_client(&self) -> Result<&reqwest::blocking::Client, RestError> {
        self.commit_client
            .get_or_init(|| {
                let client = reqwest::blocking::Client::builder();
                let client = if self.scoped {
                    client.no_proxy()
                } else {
                    client
                };
                client
                    .timeout(Duration::from_secs(30))
                    .retry(reqwest::retry::never())
                    .redirect(reqwest::redirect::Policy::none())
                    .build()
                    .map_err(|e| e.to_string())
            })
            .as_ref()
            .map_err(|e| RestError::Http(e.clone()))
    }

    /// Set the table-route prefix explicitly (normally stored automatically
    /// by `fetch_config` from the response's `overrides.prefix`).
    #[must_use]
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
        self.load_table_at_prefix(namespace, table, prefix.as_deref())
    }

    pub(crate) fn load_table_at_prefix(
        &self,
        namespace: &[&str],
        table: &str,
        prefix: Option<&str>,
    ) -> Result<super::types::LoadTableResult, RestError> {
        self.get_table_response(namespace, table, prefix, None)?
            .json()
            .map_err(|e| RestError::Decode(e.to_string()))
    }

    /// Resolve only the object pointer. Delegated credentials and inline
    /// metadata in the REST envelope cannot change a scoped read's authority.
    pub(crate) fn load_scoped_metadata_location(
        &self,
        namespace: &[&str],
        table: &str,
        prefix: Option<&str>,
        max_bytes: usize,
        deadline: Duration,
    ) -> Result<String, RestError> {
        #[derive(Deserialize)]
        struct MetadataPointer {
            #[serde(rename = "metadata-location")]
            location: String,
        }

        if !self.scoped || deadline.is_zero() || max_bytes == 0 {
            return Err(RestError::Http("invalid scoped catalog load budget".into()));
        }
        let limit = u64::try_from(max_bytes)
            .ok()
            .and_then(|bytes| bytes.checked_add(1))
            .ok_or_else(|| RestError::Http("invalid scoped catalog load budget".into()))?;
        let too_large =
            || RestError::Decode("scoped catalog response exceeds its size limit".into());
        let response = self.get_table_response(namespace, table, prefix, Some(deadline))?;
        if response
            .content_length()
            .is_some_and(|length| length >= limit)
        {
            return Err(too_large());
        }
        let mut bytes = Vec::new();
        response.take(limit).read_to_end(&mut bytes).map_err(|_| {
            RestError::Http("scoped catalog body read failed or exceeded its deadline".into())
        })?;
        if bytes.len() > max_bytes {
            return Err(too_large());
        }
        let pointer: MetadataPointer = serde_json::from_slice(&bytes)
            .map_err(|_| RestError::Decode("invalid scoped catalog metadata pointer".into()))?;
        Ok(pointer.location)
    }

    fn get_table_response(
        &self,
        namespace: &[&str],
        table: &str,
        prefix: Option<&str>,
        timeout: Option<Duration>,
    ) -> Result<reqwest::blocking::Response, RestError> {
        let ns = namespace
            .iter()
            .map(|c| urlencoding::encode(c))
            .collect::<Vec<_>>()
            .join("%1F");
        // branch/snapshot pinning via query params lands with time travel (doc 02 §4)
        let url = format!(
            "{}/v1{}/namespaces/{ns}/tables/{}",
            self.endpoint,
            prefix.map_or_default(|p| format!("/{p}")),
            urlencoding::encode(table),
        );
        let mut request = self.inner.get(&url).bearer_auth(
            self.tokens
                .token()
                .map_err(|e| RestError::Http(format!("token provider: {e}")))?,
        );
        if let Some(timeout) = timeout {
            request = request.timeout(timeout);
        }
        let resp = request.send().map_err(|e| RestError::Http(e.to_string()))?;
        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(RestError::NotFound(format!(
                "GET {} -> {status}",
                redact_url(&url)
            )));
        }
        if !status.is_success() {
            return Err(RestError::Http(format!(
                "GET {} -> {status}",
                redact_url(&url)
            )));
        }
        Ok(resp)
    }

    /// `POST /v1/{prefix}/namespaces` — createNamespace; 409 (already
    /// exists) surfaces as Conflict so callers can upsert.
    pub fn create_namespace(&self, namespace: &[&str]) -> Result<(), RestError> {
        let prefix = self.prefix.lock().expect("prefix mutex poisoned").clone();
        let ns_json = namespace
            .iter()
            .map(|c| format!("\"{c}\""))
            .collect::<Vec<_>>()
            .join(",");
        let url = format!(
            "{}/v1{}/namespaces",
            self.endpoint,
            prefix.as_ref().map_or_default(|p| format!("/{p}"))
        );
        let body = format!("{{\"namespace\": [{ns_json}]}}");
        let resp = self
            .inner
            .post(&url)
            .bearer_auth(
                self.tokens
                    .token()
                    .map_err(|e| RestError::Http(format!("token provider: {e}")))?,
            )
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .map_err(|e| RestError::Http(e.to_string()))?;
        let status = resp.status();
        if status == reqwest::StatusCode::CONFLICT {
            return Err(RestError::Conflict(format!(
                "POST {} -> {status}",
                redact_url(&url)
            )));
        }
        if !status.is_success() {
            return Err(RestError::Http(format!(
                "POST {} -> {status}",
                redact_url(&url)
            )));
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
            prefix.as_ref().map_or_default(|p| format!("/{p}"))
        );
        let payload = serde_json::json!({
            "name": table,
            "schema": schema,
            "properties": {},
        });
        let resp = self
            .inner
            .post(&url)
            .bearer_auth(
                self.tokens
                    .token()
                    .map_err(|e| RestError::Http(format!("token provider: {e}")))?,
            )
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(serde_json::to_vec(&payload).map_err(|e| RestError::Http(e.to_string()))?)
            .send()
            .map_err(|e| RestError::Http(e.to_string()))?;
        let status = resp.status();
        // 409 (table already exists) surfaces as Conflict — typed, so the
        // create_remote caller reacts by error KIND, never by searching
        // messages for "409" (13-review #6)
        if status == reqwest::StatusCode::CONFLICT {
            return Err(RestError::Conflict(format!(
                "POST {} -> {status}",
                redact_url(&url)
            )));
        }
        if !status.is_success() {
            return Err(RestError::Http(format!(
                "POST {} -> {status}",
                redact_url(&url)
            )));
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
            use std::fmt::Write as _;
            let _ = write!(url, "?warehouse={}", urlencoding::encode(w));
        }
        let resp = self
            .inner
            .get(&url)
            .bearer_auth(
                self.tokens
                    .token()
                    .map_err(|e| RestError::Http(format!("token provider: {e}")))?,
            )
            .send()
            .map_err(|e| RestError::Http(e.to_string()))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(RestError::Http(format!(
                "GET {} -> {status}",
                redact_url(&url)
            )));
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

    /// 13-review #6: credential-shaped URL parts never reach error
    /// messages — userinfo is stripped, query VALUES masked, scheme/host/
    /// path kept for diagnosis.
    #[test]
    fn redact_url_masks_userinfo_and_query_values() {
        assert_eq!(
            redact_url("https://user:secret@cat.example.com/v1/config"),
            "https://cat.example.com/v1/config"
        );
        assert_eq!(
            redact_url("http://cat.example.com/v1/config?warehouse=smoke&token=abc123"),
            "http://cat.example.com/v1/config?warehouse=***&token=***"
        );
        // bare query keys and plain URLs pass through
        assert_eq!(
            redact_url("http://cat.example.com/v1/config?flag"),
            "http://cat.example.com/v1/config?flag"
        );
        assert_eq!(
            redact_url("https://cat.example.com/v1/namespaces/ns/tables/t"),
            "https://cat.example.com/v1/namespaces/ns/tables/t"
        );
    }

    /// 13-review #6: a catalog redirect that crosses origins fails the
    /// request loudly instead of silently following (and sending the
    /// bearer token) elsewhere; same-origin hops still follow.
    #[test]
    fn cross_origin_redirects_are_refused() {
        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let addr = server.server_addr().to_string();
        let handle = std::thread::spawn(move || {
            // one request: 302 to a DIFFERENT origin (closed port)
            let rq = server.recv().unwrap();
            let resp = tiny_http::Response::empty(302);
            rq.respond(
                resp.with_header(
                    tiny_http::Header::from_bytes(
                        &b"Location"[..],
                        &b"http://127.0.0.1:9/v1/config"[..],
                    )
                    .unwrap(),
                ),
            )
            .unwrap();
        });
        let client =
            RestCatalog::new(&format!("http://{addr}"), Box::new(StaticToken("t".into()))).unwrap();
        let err = client.fetch_config(None).unwrap_err();
        handle.join().unwrap();
        let msg = err.to_string();
        assert!(
            msg.contains("crosses origins") || msg.contains("redirect"),
            "cross-origin redirect must be refused loudly: {msg}"
        );
    }

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
    /// catalog prefix comes from the config response (`fetch_config` stores it).
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

    #[test]
    fn scoped_load_distinguishes_missing_table_from_unavailable_publication() {
        for status in [404, 500] {
            let fixture = tiny_http::Server::http("127.0.0.1:0").unwrap();
            let endpoint = format!("http://{}", fixture.server_addr().to_ip().unwrap());
            let worker = std::thread::spawn(move || {
                let request = fixture
                    .recv_timeout(Duration::from_secs(2))
                    .unwrap()
                    .unwrap();
                assert_eq!(request.url(), "/v1/namespaces/ns/tables/events");
                let response = tiny_http::Response::from_string(
                    r#"{"metadata-location":"s3://warehouse/events/metadata/md.json"}"#,
                )
                .with_status_code(status);
                request.respond(response).unwrap();
            });

            let catalog =
                RestCatalog::new_scoped(&endpoint, Box::new(StaticToken("test".into()))).unwrap();
            let error = catalog
                .load_scoped_metadata_location(&["ns"], "events", None, 256, Duration::from_secs(2))
                .unwrap_err();
            worker.join().unwrap();
            match (status, error) {
                (404, RestError::NotFound(_)) | (500, RestError::Http(_)) => {}
                (status, other) => panic!("unexpected error for {status}: {other}"),
            }
        }
    }

    #[test]
    fn scoped_metadata_pointer_caps_declared_and_streamed_responses() {
        use std::io::Cursor;

        for known_length in [true, false] {
            let fixture = tiny_http::Server::http("127.0.0.1:0").unwrap();
            let endpoint = format!("http://{}", fixture.server_addr().to_ip().unwrap());
            let worker = std::thread::spawn(move || {
                let request = fixture.recv().unwrap();
                assert_eq!(request.url(), "/v1/fixed/namespaces/ns/tables/events");
                let body = format!(
                    r#"{{"metadata-location":"s3://warehouse/events/metadata/md.json","metadata":"{}"}}"#,
                    "x".repeat(2048)
                );
                let len = known_length.then_some(body.len());
                let _ = request.respond(tiny_http::Response::new(
                    200.into(),
                    vec![],
                    Cursor::new(body),
                    len,
                    None,
                ));
            });

            let catalog =
                RestCatalog::new_scoped(&endpoint, Box::new(StaticToken("test".into()))).unwrap();
            let error = catalog
                .load_scoped_metadata_location(
                    &["ns"],
                    "events",
                    Some("fixed"),
                    256,
                    Duration::from_secs(2),
                )
                .unwrap_err();
            assert!(
                matches!(error, RestError::Decode(ref reason) if reason.contains("size limit"))
            );
            worker.join().unwrap();
        }
    }

    #[test]
    fn scoped_metadata_pointer_deadline_covers_the_body() {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (finished, completed) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut headers = [0u8; 2048];
            assert!(stream.read(&mut headers).unwrap() > 0);
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 60\r\n\r\n{\"metadata-location\":")
                .unwrap();
            assert!(completed.recv_timeout(Duration::from_secs(3)).is_ok());
        });

        let catalog =
            RestCatalog::new_scoped(&endpoint, Box::new(StaticToken("test".into()))).unwrap();
        let error = catalog
            .load_scoped_metadata_location(&["ns"], "events", None, 256, Duration::from_millis(250))
            .unwrap_err();
        finished.send(()).unwrap();
        assert!(matches!(error, RestError::Http(ref reason) if reason.contains("deadline")));
        worker.join().unwrap();
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
