//! `OAuth2` client-credentials token provider (specs/lakehouse/03-catalog.md §2).
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

/// `OAuth2` client-credentials provider; implements [`TokenProvider`].
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
    #[must_use]
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
    #[must_use]
    pub fn token_endpoint(mut self, url: String) -> Self {
        self.token_endpoint = url;
        self
    }

    /// Client id and secret (sent form-encoded per the Iceberg REST spec;
    /// Basic-auth profiles are added as profile data in a later slice).
    #[must_use]
    pub fn client_id(mut self, id: &str, secret: &str) -> Self {
        self.client_id = id.to_string();
        self.client_secret = secret.to_string();
        self
    }

    /// Optional OAuth scope.
    #[must_use]
    pub fn scope(mut self, scope: &str) -> Self {
        self.scope = Some(scope.to_string());
        self
    }

    /// Fraction of TTL after which a refresh happens (default 0.8).
    #[must_use]
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
    fn token(&self) -> Result<String, RestError> {
        let mut state = self.state.lock().expect("token state mutex poisoned");
        let (needs_refresh, within_hard_ttl) = match &*state {
            Some(cached) => {
                let elapsed = cached.fetched_at.elapsed();
                (
                    should_refresh(elapsed, cached.ttl, self.refresh_fraction),
                    elapsed < cached.ttl,
                )
            }
            None => (true, false),
        };
        if needs_refresh {
            match self.fetch_token_blocking() {
                Ok((token, ttl)) => {
                    *state = Some(CachedToken {
                        token: token.clone(),
                        fetched_at: Instant::now(),
                        ttl,
                    });
                    return Ok(token);
                }
                // Serve the stale token rather than fail the request outright
                // when a refresh fails MID-WINDOW (before 100% TTL); past the
                // hard TTL the token is expired — serving it would send a
                // dead bearer and mask the refresh failure (13-review #6).
                Err(_) if state.is_some() && within_hard_ttl => {}
                Err(e) => {
                    // No usable token (nothing cached, or cached token past
                    // its TTL): fail the request loudly — never an empty
                    // bearer, never a dead one.
                    return Err(RestError::Http(format!("token refresh failed: {e}")));
                }
            }
        }
        Ok(state.as_ref().map_or_default(|c| c.token.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// 13-review #6: a token provider that cannot fetch (endpoint down,
    /// 500) fails LOUDLY — never an empty bearer that would 401 opaquely.
    #[test]
    fn fetch_failure_with_no_cache_is_loud() {
        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let addr = server.server_addr().to_string();
        std::thread::spawn(move || {
            // one 500, then hang forever (thread never joins)
            if let Ok(rq) = server.recv() {
                let _ = rq.respond(tiny_http::Response::empty(500));
            }
            loop {
                std::thread::sleep(std::time::Duration::from_secs(3600));
            }
        });
        let provider = OAuth2ClientCredentials::builder()
            .token_endpoint(format!("http://{addr}/v1/oauth/tokens"))
            .client_id("id", "secret")
            .build()
            .unwrap();
        let err = provider.token().unwrap_err();
        assert!(
            err.to_string().contains("token refresh failed"),
            "failed fetch with no cache must be a loud error: {err}"
        );
    }

    /// 13-review #6: an EXPIRED cached token whose refresh fails is a
    /// loud error — serving a dead bearer only masks the failure.
    #[test]
    fn expired_token_with_failed_refresh_is_loud() {
        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let addr = server.server_addr().to_string();
        std::thread::spawn(move || {
            // first request: a 1-second token; afterwards: always 500
            let body = r#"{"access_token":"tok","expires_in":1}"#;
            if let Ok(rq) = server.recv() {
                let r = tiny_http::Response::from_string(body).with_header(
                    tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                        .unwrap(),
                );
                let _ = rq.respond(r);
            }
            loop {
                if let Ok(rq) = server.recv() {
                    let _ = rq.respond(tiny_http::Response::empty(500));
                }
            }
        });
        let provider = OAuth2ClientCredentials::builder()
            .token_endpoint(format!("http://{addr}/v1/oauth/tokens"))
            .client_id("id", "secret")
            .refresh_after_fraction(0.01) // refresh almost immediately
            .build()
            .unwrap();
        assert_eq!(provider.token().unwrap(), "tok");
        std::thread::sleep(Duration::from_millis(1100)); // token expires (1s TTL)
        let err = provider.token().unwrap_err();
        assert!(
            err.to_string().contains("token refresh failed"),
            "expired token + failed refresh must be loud: {err}"
        );
    }
}
