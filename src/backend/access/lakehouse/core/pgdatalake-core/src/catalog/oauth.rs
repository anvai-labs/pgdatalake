//! OAuth2 client-credentials token provider (specs/lakehouse/03-catalog.md §2).
//!
//! Iceberg REST spec token endpoint: `POST /v1/oauth/tokens` with
//! form-encoded `grant_type=client_credentials`, `client_id`,
//! `client_secret`, optional `scope`. Tokens are cached and proactively
//! refreshed at 80% of their TTL (configurable) so no request ever races
//! expiry. Static bearer tokens use `rest::StaticToken` instead.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::Deserialize;

use super::rest::{RestError, TokenProvider};

/// Default proactive-refresh point: 80% of the token TTL.
const DEFAULT_REFRESH_FRACTION: f64 = 0.8;

/// Pure refresh decision: refresh once we are at or past `fraction` of TTL.
fn should_refresh(elapsed: Duration, ttl: Duration, fraction: f64) -> bool {
    elapsed >= Duration::from_secs_f64(ttl.as_secs_f64() * fraction)
}

#[derive(Deserialize)]
struct TokenWire {
    access_token: String,
    #[serde(default)]
    expires_in: Option<u64>,
}

struct CachedToken {
    token: String,
    #[allow(dead_code)] // surfaced via token(); kept for future debug/metrics
    fetched_at: Instant,
    ttl: Duration,
}

/// OAuth2 client-credentials provider; implements [`TokenProvider`].
pub struct OAuth2ClientCredentials {
    token_endpoint: String,
    client_id: String,
    client_secret: String,
    scope: Option<String>,
    refresh_fraction: f64,
    http: reqwest::blocking::Client,
    state: Mutex<Option<CachedToken>>,
}

/// Builder for [`OAuth2ClientCredentials`].
pub struct OAuth2Builder {
    token_endpoint: String,
    client_id: String,
    client_secret: String,
    scope: Option<String>,
    refresh_fraction: f64,
}

impl OAuth2ClientCredentials {
    pub fn builder() -> OAuth2Builder {
        OAuth2Builder {
            token_endpoint: String::new(),
            client_id: String::new(),
            client_secret: String::new(),
            scope: None,
            refresh_fraction: DEFAULT_REFRESH_FRACTION,
        }
    }
}

impl OAuth2Builder {
    /// Full URL of the token endpoint, e.g. `{endpoint}/v1/oauth/tokens`.
    pub fn token_endpoint(mut self, url: String) -> Self {
        self.token_endpoint = url;
        self
    }

    /// Client id and secret (sent form-encoded per the Iceberg REST spec;
    /// Basic-auth profiles are added as profile data in a later slice).
    pub fn client_id(mut self, id: &str, secret: &str) -> Self {
        self.client_id = id.to_string();
        self.client_secret = secret.to_string();
        self
    }

    /// Optional OAuth scope.
    pub fn scope(mut self, scope: &str) -> Self {
        self.scope = Some(scope.to_string());
        self
    }

    /// Fraction of TTL after which a refresh happens (default 0.8).
    pub fn refresh_after_fraction(mut self, fraction: f64) -> Self {
        self.refresh_fraction = fraction.clamp(0.0, 1.0);
        self
    }

    pub fn build(self) -> Result<OAuth2ClientCredentials, RestError> {
        let http = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| RestError::Http(e.to_string()))?;
        Ok(OAuth2ClientCredentials {
            token_endpoint: self.token_endpoint,
            client_id: self.client_id,
            client_secret: self.client_secret,
            scope: self.scope,
            refresh_fraction: self.refresh_fraction,
            http,
            state: Mutex::new(None),
        })
    }
}

impl OAuth2ClientCredentials {
    fn fetch_token_blocking(&self) -> Result<(String, Duration), RestError> {
        let mut form = vec![
            ("grant_type", "client_credentials".to_string()),
            ("client_id", self.client_id.clone()),
            ("client_secret", self.client_secret.clone()),
        ];
        if let Some(scope) = &self.scope {
            form.push(("scope", scope.clone()));
        }
        let wire: TokenWire = self
            .http
            .post(&self.token_endpoint)
            .form(&form)
            .send()
            .map_err(|e| RestError::Http(e.to_string()))?
            .error_for_status()
            .map_err(|e| RestError::Http(e.to_string()))?
            .json()
            .map_err(|e| RestError::Decode(e.to_string()))?;
        let ttl = Duration::from_secs(wire.expires_in.unwrap_or(3600));
        Ok((wire.access_token, ttl))
    }
}

impl TokenProvider for OAuth2ClientCredentials {
    fn token(&self) -> String {
        let mut state = self.state.lock().expect("token state mutex poisoned");
        let needs_refresh = match &*state {
            Some(cached) => should_refresh(
                cached.fetched_at.elapsed(),
                cached.ttl,
                self.refresh_fraction,
            ),
            None => true,
        };
        if needs_refresh {
            match self.fetch_token_blocking() {
                Ok((token, ttl)) => {
                    *state = Some(CachedToken {
                        token: token.clone(),
                        fetched_at: Instant::now(),
                        ttl,
                    });
                    return token;
                }
                // Serve the stale token rather than fail the request outright
                // when a refresh fails mid-window; a hard failure at 100% TTL
                // is the caller's retry problem (doc 03 §5 failure table).
                Err(_) if state.is_some() => {}
                Err(e) => {
                    // No cached token at all: nothing usable to serve.
                    // Degrade to an empty bearer; the API call will 401 and
                    // surface the real status to the caller.
                    let _ = e;
                    return String::new();
                }
            }
        }
        state.as_ref().map(|c| c.token.clone()).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::rest::RestCatalog;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc as StdArc;
    use std::time::Duration;

    /// Mock token endpoint + config endpoint. Counts token requests.
    fn start_mock(expires_in: u64, token_counter: StdArc<AtomicUsize>) -> String {
        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let addr = server.server_addr().to_string();
        std::thread::spawn(move || {
            for _ in 0..8 {
                let req = match server.recv() {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let url = req.url().to_string();
                let resp = if url.starts_with("/v1/oauth/tokens") {
                    token_counter.fetch_add(1, Ordering::SeqCst);
                    tiny_http::Response::from_string(
                        r#"{"access_token":"tok-abc","token_type":"bearer","expires_in":EXPIRES}"#,
                    )
                } else {
                    tiny_http::Response::from_string("{}")
                };
                let resp = match url.starts_with("/v1/oauth/tokens") {
                    true => tiny_http::Response::from_string(format!(
                        r#"{{"access_token":"tok-abc","token_type":"bearer","expires_in":{expires_in}}}"#
                    )),
                    false => resp,
                };
                let _ = req.respond(
                    resp.with_header(
                        tiny_http::Header::from_bytes(
                            &b"Content-Type"[..],
                            &b"application/json"[..],
                        )
                        .unwrap(),
                    ),
                );
            }
        });
        addr
    }

    /// Token fetch hits the spec path with form-encoded client credentials.
    #[test]
    fn oauth_token_request_is_spec_shaped() {
        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let addr = server.server_addr().to_string();
        let seen = StdArc::new(std::sync::Mutex::new((String::new(), String::new())));
        let seen_t = std::sync::Arc::clone(&seen);
        std::thread::spawn(move || {
            let mut req = server.recv().unwrap();
            let mut body = String::new();
            let _ = req.as_reader().read_to_string(&mut body);
            *seen_t.lock().unwrap() = (req.url().to_string(), body);
            let resp = tiny_http::Response::from_string(
                r#"{"access_token":"tok-abc","token_type":"bearer","expires_in":3600}"#,
            );
            let _ = req.respond(resp);
        });

        let provider = OAuth2ClientCredentials::builder()
            .token_endpoint(format!("http://{addr}/v1/oauth/tokens"))
            .client_id("cid", "secret")
            .build()
            .unwrap();
        let (tok, ttl) = provider.fetch_token_blocking().unwrap();
        assert_eq!(tok, "tok-abc");
        assert_eq!(ttl, Duration::from_secs(3600));

        let (url, body) = &*seen.lock().unwrap();
        assert_eq!(url, "/v1/oauth/tokens");
        assert!(
            body.contains("grant_type=client_credentials"),
            "body: {body}"
        );
        assert!(body.contains("client_id=cid"), "body: {body}");
        assert!(body.contains("client_secret=secret"), "body: {body}");
    }

    /// The cached token is reused: N API calls trigger exactly one token fetch.
    #[test]
    fn token_is_cached_across_calls() {
        let counter = StdArc::new(AtomicUsize::new(0));
        let addr = start_mock(3600, std::sync::Arc::clone(&counter));

        let provider = OAuth2ClientCredentials::builder()
            .token_endpoint(format!("http://{addr}/v1/oauth/tokens"))
            .client_id("cid", "secret")
            .build()
            .unwrap();
        let catalog = RestCatalog::new(&format!("http://{addr}"), Box::new(provider)).unwrap();
        for _ in 0..3 {
            catalog.fetch_config(None).unwrap();
        }
        assert_eq!(counter.load(Ordering::SeqCst), 1, "one fetch");
    }

    /// Proactive refresh at 80% TTL: a second call after the refresh window
    /// opens triggers exactly one more token request (doc 03 §2).
    #[test]
    fn token_refreshes_at_eighty_percent_ttl() {
        let counter = StdArc::new(AtomicUsize::new(0));
        let addr = start_mock(1, std::sync::Arc::clone(&counter)); // expires_in = 1s

        let provider = OAuth2ClientCredentials::builder()
            .token_endpoint(format!("http://{addr}/v1/oauth/tokens"))
            .client_id("cid", "secret")
            .refresh_after_fraction(0.8)
            .build()
            .unwrap();
        let catalog = RestCatalog::new(&format!("http://{addr}"), Box::new(provider)).unwrap();

        catalog.fetch_config(None).unwrap(); // token fetched (1)
        std::thread::sleep(Duration::from_millis(900)); // > 0.8 * 1s
        catalog.fetch_config(None).unwrap(); // refresh (2)
        catalog.fetch_config(None).unwrap(); // still fresh (2)

        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    /// Pure decision function: exactly at/after 80% of TTL -> refresh.
    #[test]
    fn should_refresh_decision_boundary() {
        let ttl = Duration::from_secs(100);
        let fraction = 0.8;
        assert!(!should_refresh(Duration::from_secs(0), ttl, fraction));
        assert!(!should_refresh(Duration::from_secs(79), ttl, fraction));
        assert!(should_refresh(Duration::from_secs(80), ttl, fraction));
        assert!(should_refresh(Duration::from_secs(101), ttl, fraction));
    }
}
