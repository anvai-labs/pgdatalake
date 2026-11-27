//! Object-store file IO (specs/lakehouse/01-architecture.md layer 1,
//! doc 04-storage.md). Slice 4: location parsing + metadata.json fetch over
//! any `object_store` backend (local for tests, S3/SeaweedFS in fixtures;
//! ADLS/GCS land in M3). Blocking surface: the C glue is synchronous, so a
//! shared single-thread tokio runtime lives behind these calls.

use std::sync::OnceLock;

use object_store::{ObjectStore, PutPayload};

use crate::catalog::metadata::TableMetadata;

/// Errors surfaced to the C glue; `NotFound` is distinct (doc 03 §5).
#[derive(Debug)]
pub enum StorageError {
    NotFound(String),
    ReadDenied(String),
    Http(String),
    Decode(String),
    Config(String),
    /// REST catalog 409 CommitFailedException-equivalent: a concurrent
    /// writer won, or the object already exists. Distinct so callers react
    /// by TYPE, never by searching error strings for "409" (13-review #6).
    Conflict(String),
    CommitStateUnknown(String),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::NotFound(m) => write!(f, "object not found: {m}"),
            StorageError::ReadDenied(m) => write!(f, "storage read denied: {m}"),
            StorageError::Http(m) => write!(f, "storage IO error: {m}"),
            StorageError::Decode(m) => write!(f, "storage decode error: {m}"),
            StorageError::Config(m) => write!(f, "storage config error: {m}"),
            StorageError::Conflict(m) => write!(f, "storage conflict: {m}"),
            StorageError::CommitStateUnknown(m) => write!(f, "catalog commit outcome unknown: {m}"),
        }
    }
}

impl std::error::Error for StorageError {}

/// A parsed table/file location.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Location {
    S3 {
        bucket: String,
        key: String,
    },
    Local {
        root: String,
    },
    #[allow(dead_code)] // https(s) metadata locations arrive with remote profiles
    Http {
        url: String,
    },
}

/// Parsed location: scheme + bucket/key. For S3 the key is the FULL path
/// from the bucket root (Iceberg locations are absolute within the bucket);
/// for Local the root is the directory prefix and `get_bytes()` keys are
/// relative to it.
pub fn parse_location(loc: &str) -> Result<Location, StorageError> {
    if let Some(rest) = loc.strip_prefix("s3://") {
        let (bucket, root) = rest
            .split_once('/')
            .ok_or_else(|| StorageError::Config(format!("s3 location without key: {loc}")))?;
        return Ok(Location::S3 {
            bucket: bucket.to_string(),
            key: root.trim_end_matches('/').to_string(),
        });
    }
    if let Some(rest) = loc.strip_prefix("file://") {
        return Ok(Location::Local {
            root: rest.trim_end_matches('/').to_string(),
        });
    }
    if loc.starts_with("http://") || loc.starts_with("https://") {
        return Ok(Location::Http {
            url: loc.trim_end_matches('/').to_string(),
        });
    }
    // Hadoop-style bare absolute paths (the Iceberg Java writer's local
    // convention) are Local locations — same default-deny + table-root
    // scope gates as file:// URIs apply downstream.
    if loc.starts_with('/') {
        return Ok(Location::Local {
            root: loc.trim_end_matches('/').to_string(),
        });
    }
    Err(StorageError::Config(format!("unsupported location: {loc}")))
}

/// S3 connection options, assembled from the effective catalog config
/// (defaults overlaid by overrides; doc 03 §2 merge order) plus client-side
/// credentials that the Postgres layer supplies from secure settings.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct S3Options {
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub path_style: bool,
}

impl S3Options {
    /// Keys per the Iceberg REST config conventions (`s3.*`).
    #[must_use]
    pub fn from_config(cfg: &std::collections::BTreeMap<String, String>) -> Self {
        Self {
            endpoint: cfg.get("s3.endpoint").cloned(),
            region: cfg.get("s3.region").cloned(),
            access_key_id: cfg.get("s3.access-key-id").cloned(),
            secret_access_key: cfg.get("s3.secret-access-key").cloned(),
            path_style: cfg
                .get("s3.path-style-access")
                .is_none_or(|v| v == "true" || v == "1"),
        }
    }
}

/// Explicit opt-in for file:// object stores (13-review #6): catalogs and
/// manifests supply absolute paths, so a compromised catalog must not be
/// able to direct reads at arbitrary local paths. The flag is process-
/// global, monotonic (false -> true, never reset) and test-only — the
/// shipped binary has NO API to enable local stores (explicit local
/// deployments arrive with the M3 profile work).
static ALLOW_LOCAL_STORES: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

fn local_stores_allowed() -> bool {
    ALLOW_LOCAL_STORES.load(std::sync::atomic::Ordering::Relaxed)
}

/// Construct the local object store with an explicit opt-in decision —
/// the only place file:// stores are ever created.
fn open_local_store(root: &str, allowed: bool) -> Result<Box<dyn ObjectStore>, StorageError> {
    if !allowed {
        return Err(StorageError::Config(format!(
            "file:// object store refused: local paths from catalogs/manifests \
             require explicit opt-in (path: {root})"
        )));
    }
    Ok(Box::new(
        object_store::local::LocalFileSystem::new_with_prefix(root)
            .map_err(|e| StorageError::Config(e.to_string()))?,
    ))
}

/// Fixture opt-in (13-review #6): test code ONLY — cfg(test), monotonic
/// (once opted in the process stays opted in; parallel tests never
/// un-opt-in). The production binary structurally cannot open file://
/// stores.
#[cfg(test)]
pub fn test_allow_local_stores() {
    ALLOW_LOCAL_STORES.store(true, std::sync::atomic::Ordering::Relaxed);
}

/// Plain HTTP for the S3 client is allowed ONLY for explicitly configured
/// http:// endpoints (fixture stores). TLS-only otherwise — no endpoint
/// configured means AWS, never plaintext.
fn s3_allow_http(endpoint: Option<&str>) -> bool {
    endpoint.is_some_and(|e| e.starts_with("http://"))
}

fn runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime for object_store")
    })
}

// A newly opened network client must re-establish access with the provider.
static NEXT_REMOTE_CACHE_SCOPE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// An open handle to one object-store location.
///
/// Cheap to CLONE (the store is behind an Arc): P0b's `RangedReader` owns
/// a clone because parquet's `ChunkReader` consumers require 'static.
#[derive(Clone)]
pub struct StorageIo {
    /// Storage cache identity. Clones share an open handle's identity.
    pub(crate) scope_id: std::sync::Arc<str>,
    store: std::sync::Arc<dyn ObjectStore>,
}

// object_store's default connector follows redirects. An explicit endpoint
// is an authority boundary for scoped reads and writes.
#[derive(Debug)]
struct ScopedS3Connector;

impl object_store::client::HttpConnector for ScopedS3Connector {
    fn connect(
        &self,
        _: &object_store::ClientOptions,
    ) -> object_store::Result<object_store::client::HttpClient> {
        let client = reqwest_s3::Client::builder()
            .redirect(reqwest_s3::redirect::Policy::none())
            .no_proxy()
            .no_gzip()
            .no_brotli()
            .no_zstd()
            .no_deflate()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|source| object_store::Error::Generic {
                store: "scoped S3",
                source: Box::new(source),
            })?;
        Ok(object_store::client::HttpClient::new(client))
    }
}

impl StorageIo {
    pub fn open(loc: &Location, s3: Option<&S3Options>) -> Result<Self, StorageError> {
        Self::open_inner(loc, s3, false, None)
    }

    pub(crate) fn open_bound_s3(loc: &Location, s3: &S3Options) -> Result<Self, StorageError> {
        Self::open_bound_s3_with_credentials(loc, s3, None)
    }

    pub(crate) fn open_bound_s3_with_credentials(
        loc: &Location,
        s3: &S3Options,
        credentials: Option<object_store::aws::AwsCredentialProvider>,
    ) -> Result<Self, StorageError> {
        if !matches!(loc, Location::S3 { .. }) || s3.endpoint.is_none() || s3.region.is_none() {
            return Err(StorageError::Config(
                "bound S3 access requires explicit transport".into(),
            ));
        }
        Self::open_inner(loc, Some(s3), true, credentials)
    }

    fn open_inner(
        loc: &Location,
        s3: Option<&S3Options>,
        scoped: bool,
        credentials: Option<object_store::aws::AwsCredentialProvider>,
    ) -> Result<Self, StorageError> {
        let store: Box<dyn ObjectStore> = match loc {
            Location::Local { root } => open_local_store(root, local_stores_allowed())?,
            Location::S3 { bucket, .. } => {
                let opts = s3.ok_or_else(|| {
                    StorageError::Config("s3 location requires S3Options".to_string())
                })?;
                let mut b = object_store::aws::AmazonS3Builder::new()
                    .with_bucket_name(bucket)
                    .with_allow_http(s3_allow_http(opts.endpoint.as_deref()));
                if let Some(credentials) = credentials {
                    b = b.with_credentials(credentials);
                } else if let (Some(ak), Some(sk)) = (&opts.access_key_id, &opts.secret_access_key)
                {
                    b = b.with_access_key_id(ak).with_secret_access_key(sk);
                } else {
                    return Err(StorageError::Config(
                        "static S3 credentials missing: set s3_access_key/s3_secret_key \
                         on the user mapping (ambient cloud-credential fallback is disabled)"
                            .into(),
                    ));
                }
                if let Some(ep) = &opts.endpoint {
                    b = b.with_endpoint(ep);
                }
                if let Some(r) = &opts.region {
                    b = b.with_region(r);
                }
                if scoped {
                    b = b
                        .with_virtual_hosted_style_request(false)
                        .with_http_connector(ScopedS3Connector);
                }
                Box::new(b.build().map_err(|e| StorageError::Config(e.to_string()))?)
            }
            // https(s) metadata locations are rare (signed URLs); wire-up
            // lands with remote-profile slices
            Location::Http { url } => Box::new(
                object_store::http::HttpBuilder::new()
                    .with_url(url.clone())
                    .build()
                    .map_err(|e| StorageError::Config(e.to_string()))?,
            ),
        };
        // Keys passed to get_bytes()/put_bytes() are FULL paths: from the
        // bucket root for S3 (Iceberg locations are absolute within the
        // bucket), or relative to the prefix for Local (which is why the
        // local store is opened rooted). No re-prefixing happens here.
        //
        let scope_id = match loc {
            Location::Local { root } => format!("local:{root}"),
            Location::S3 { .. } | Location::Http { .. } => {
                use std::sync::atomic::Ordering::Relaxed;

                let scope = NEXT_REMOTE_CACHE_SCOPE
                    .try_update(Relaxed, Relaxed, |value| value.checked_add(1))
                    .map_err(|_| StorageError::Config("remote cache scope exhausted".into()))?;
                format!("remote:{scope}")
            }
        };
        Ok(Self {
            scope_id: scope_id.into(),
            store: std::sync::Arc::from(store),
        })
    }

    fn full_path(key: &str) -> object_store::path::Path {
        object_store::path::Path::from(key)
    }

    /// GET one object as bytes; 404s map to `StorageError::NotFound`.
    pub fn get_bytes(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        use object_store::ObjectStoreExt;
        let path = Self::full_path(key);
        match runtime().block_on(self.store.get(&path)) {
            Ok(res) => {
                let bytes = runtime()
                    .block_on(res.bytes())
                    .map_err(|e| StorageError::Http(e.to_string()))?;
                Ok(bytes.to_vec())
            }
            Err(object_store::Error::NotFound { path, source }) => {
                Err(StorageError::NotFound(format!("{path}: {source}")))
            }
            Err(e) => Err(StorageError::Http(e.to_string())),
        }
    }

    /// Object length via HEAD (P0b): the parquet `ChunkReader` needs the
    /// file size to seek the footer. One stat per open — cheap, and NOT
    /// cached (a stat is metadata, not the immutable-bytes the §6.2 law
    /// is about).
    pub fn stat_len(&self, key: &str) -> Result<u64, StorageError> {
        use object_store::ObjectStoreExt;
        let path = Self::full_path(key);
        let meta = runtime()
            .block_on(self.store.head(&path))
            .map_err(|e| match e {
                object_store::Error::NotFound { path, source } => {
                    StorageError::NotFound(format!("{path}: {source}"))
                }
                e => StorageError::Http(e.to_string()),
            })?;
        Ok(meta.size)
    }

    /// Ranged GET of `[offset, offset+len)` bytes (slice P0, spec 15
    /// §6.2): the read-side prerequisite for footer-driven reads and the
    /// block cache. Byte-identity contract: the returned bytes MUST
    /// equal `&get_bytes(key)[offset..offset+len]` (tested). Out-of-range
    /// is an Http error from `object_store`, surfaced as-is.
    pub fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Vec<u8>, StorageError> {
        use object_store::{GetOptions, GetRange};
        let path = Self::full_path(key);
        let opts = GetOptions {
            range: Some(GetRange::Bounded(offset..offset.saturating_add(len))),
            ..Default::default()
        };
        match runtime().block_on(self.store.get_opts(&path, opts)) {
            Ok(res) => {
                let bytes = runtime()
                    .block_on(res.bytes())
                    .map_err(|e| StorageError::Http(e.to_string()))?;
                Ok(bytes.to_vec())
            }
            Err(object_store::Error::NotFound { path, source }) => {
                Err(StorageError::NotFound(format!("{path}: {source}")))
            }
            Err(e) => Err(StorageError::Http(e.to_string())),
        }
    }

    /// Hash a complete object without retaining its contents or using the scan cache.
    pub(crate) fn sha256_bounded(
        &self,
        key: &str,
        max_bytes: u64,
        deadline: std::time::Duration,
    ) -> Result<[u8; 32], StorageError> {
        let mut hasher = ring::digest::Context::new(&ring::digest::SHA256);
        self.stream_bounded(key, max_bytes, deadline, |chunk| hasher.update(chunk))?;
        let mut digest = [0; 32];
        digest.copy_from_slice(hasher.finish().as_ref());
        Ok(digest)
    }

    /// Read the complete body for the manifest decoder, using the same range
    /// and deadline as reconciliation.
    pub(crate) fn get_bytes_bounded(
        &self,
        key: &str,
        max_bytes: u64,
        deadline: std::time::Duration,
    ) -> Result<Vec<u8>, StorageError> {
        let mut bytes = Vec::new();
        self.stream_bounded(key, max_bytes, deadline, |chunk| {
            bytes.extend_from_slice(chunk);
        })?;
        Ok(bytes)
    }

    fn stream_bounded(
        &self,
        key: &str,
        max_bytes: u64,
        deadline: std::time::Duration,
        mut consume: impl FnMut(&[u8]),
    ) -> Result<(), StorageError> {
        use object_store::{GetOptions, GetRange};
        let Some(probe_end) = max_bytes.checked_add(1).filter(|_| max_bytes > 0) else {
            return Err(StorageError::Config(
                "invalid object read byte limit".into(),
            ));
        };
        if deadline.is_zero() {
            return Err(StorageError::Config("invalid object read deadline".into()));
        }
        let path = Self::full_path(key);
        let options = GetOptions {
            range: Some(GetRange::Bounded(0..probe_end)),
            ..Default::default()
        };
        runtime().block_on(async {
            tokio::time::timeout(deadline, async {
                let result =
                    self.store
                        .get_opts(&path, options)
                        .await
                        .map_err(|error| match error {
                            object_store::Error::NotFound { .. } => {
                                StorageError::NotFound("scoped object is unavailable".into())
                            }
                            _ => StorageError::Http("could not read scoped object".into()),
                        })?;
                let expected = result.meta.size;
                if expected > max_bytes {
                    return Err(StorageError::Config(
                        "scoped object exceeds the byte limit".into(),
                    ));
                }
                if result.range != (0..expected) {
                    return Err(StorageError::Decode(
                        "scoped object response is incomplete".into(),
                    ));
                }
                let mut stream = result.into_stream();
                let mut remaining = expected;
                while let Some(chunk) =
                    std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await
                {
                    let chunk = chunk
                        .map_err(|_| StorageError::Http("could not stream scoped object".into()))?;
                    remaining = remaining.checked_sub(chunk.len() as u64).ok_or_else(|| {
                        StorageError::Decode("scoped object exceeds its reported size".into())
                    })?;
                    consume(&chunk);
                }
                if remaining != 0 {
                    return Err(StorageError::Decode(
                        "scoped object response is truncated".into(),
                    ));
                }
                Ok(())
            })
            .await
            .map_err(|_| StorageError::Http("scoped object read deadline exceeded".into()))?
        })
    }

    /// Atomically create an object. A key collision never replaces existing
    /// bytes, including when storage retries a write whose reply was lost.
    pub(crate) fn put_bytes_new(&self, key: &str, data: Vec<u8>) -> Result<(), StorageError> {
        use object_store::{PutMode, PutOptions};
        let path = Self::full_path(key);
        let options = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        runtime()
            .block_on(
                self.store
                    .put_opts(&path, PutPayload::from_bytes(data.into()), options),
            )
            .map_err(|error| match error {
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. } => {
                    StorageError::Conflict("immutable object key already exists".into())
                }
                other => StorageError::Http(other.to_string()),
            })?;
        Ok(())
    }

    /// PUT one object (used by later write-path slices; exposed now so the
    /// fixture example can verify the roundtrip).
    #[allow(dead_code)]
    pub fn put_bytes(&self, key: &str, data: Vec<u8>) -> Result<(), StorageError> {
        let path = Self::full_path(key);
        {
            use object_store::ObjectStoreExt;
            runtime()
                .block_on(self.store.put(&path, PutPayload::from_bytes(data.into())))
                .map_err(|e| StorageError::Http(e.to_string()))?;
        }
        Ok(())
    }
}

/// Convenience: fetch + parse a metadata.json in one call. Gzip is decoded
/// transparently (Lakekeeper writes `.gz.metadata.json`).
pub fn fetch_metadata(
    io: &StorageIo,
    metadata_location_key: &str,
) -> Result<TableMetadata, StorageError> {
    // DELIBERATELY NOT cached (spec 15 §2a correction): metadata.json is
    // the catalog's pointer TARGET — its path may be served with new
    // content (fixtures, HadoopTables reuse names; a new commit keeps
    // its own name but the catalog can serve any metadata-location).
    // Manifests/manifest-lists/DV puffins are content-unique and cached.
    let bytes = io.get_bytes(metadata_location_key)?;
    let bytes = maybe_gunzip(bytes)?;
    serde_json::from_slice(&bytes).map_err(|e| StorageError::Decode(e.to_string()))
}

/// Read one complete metadata object, including an expansion cap for gzip.
pub(crate) fn fetch_metadata_bounded(
    io: &StorageIo,
    key: &str,
    max_bytes: usize,
    deadline: std::time::Duration,
) -> Result<TableMetadata, StorageError> {
    use std::io::Read;

    let probe_limit = u64::try_from(max_bytes)
        .ok()
        .and_then(|limit| limit.checked_add(1))
        .ok_or_else(|| StorageError::Config("invalid scoped metadata byte limit".into()))?;
    let bytes = io.get_bytes_bounded(key, probe_limit - 1, deadline)?;
    let bytes = if bytes.starts_with(&[0x1f, 0x8b]) {
        let mut expanded = Vec::new();
        flate2::read::MultiGzDecoder::new(&bytes[..])
            .take(probe_limit)
            .read_to_end(&mut expanded)
            .map_err(|_| StorageError::Decode("invalid gzip scoped metadata".into()))?;
        if expanded.len() > max_bytes {
            return Err(StorageError::Config(
                "scoped metadata expansion exceeds its size limit".into(),
            ));
        }
        expanded
    } else {
        bytes
    };
    serde_json::from_slice(&bytes)
        .map_err(|_| StorageError::Decode("invalid scoped metadata JSON".into()))
}

/// Gzip magic 0x1f 0x8b; plain JSON passes through untouched.
/// public wrapper for examples/tests
pub fn maybe_gunzip_pub(bytes: Vec<u8>) -> Result<Vec<u8>, StorageError> {
    maybe_gunzip(bytes)
}

fn maybe_gunzip(bytes: Vec<u8>) -> Result<Vec<u8>, StorageError> {
    if bytes.len() >= 2 && bytes[0] == 0x1f && bytes[1] == 0x8b {
        let mut out = Vec::new();
        let mut dec = flate2::read::MultiGzDecoder::new(&bytes[..]);
        std::io::Read::read_to_end(&mut dec, &mut out)
            .map_err(|e| StorageError::Decode(format!("gzip: {e}")))?;
        return Ok(out);
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    #[test]
    fn cache_scope_encoding_is_injective() {
        // round-5 M1: '|' in a component must not collapse distinct
        // (endpoint, bucket) pairs onto one scope. Backslash-first
        // escaping keeps the transform injective and reversible.
        let esc = |c: &str| -> String { c.replace('\\', "\\\\").replace('|', "\\|") };
        let a = format!("remote:{}|{}", esc("aws|x"), esc("y"));
        let b = format!("remote:{}|{}", esc("aws"), esc("x|y"));
        assert_ne!(a, b);
        let round = esc("aws|x");
        assert_eq!(round, "aws\\|x");
        assert_eq!(round.replace("\\|", "|"), "aws|x");
    }

    use super::*;

    /// 13-review #6: file:// object stores are refused unless explicitly
    /// opted in — and the opt-in API is cfg(test) only, so the shipped
    /// binary structurally cannot open catalog/manifest-supplied local
    /// paths. (The gate is tested directly on `open_local_store` so the
    /// process-global monotonic flag never races parallel tests.)
    #[test]
    fn file_store_requires_explicit_opt_in() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_string_lossy().into_owned();
        let Err(err) = open_local_store(&root, false) else {
            panic!("local store must be refused without opt-in")
        };
        assert!(
            matches!(err, StorageError::Config(ref m) if m.contains("explicit opt-in")),
            "local store must be refused by default: {err}"
        );
        // explicit opt-in admits it (fixtures, doc 10 §1)
        open_local_store(&root, true).expect("opted-in local store opens");
    }

    /// 13-review #6: ambient cloud credential fallback is removed —
    /// static keys are REQUIRED for S3; absent keys are a loud config
    /// error pointing at the user mapping, never an IMDS probe.
    #[test]
    fn s3_static_keys_required() {
        let loc = Location::S3 {
            bucket: "warehouse".into(),
            key: "t/metadata/x.json".into(),
        };
        let Err(err) = StorageIo::open(&loc, Some(&S3Options::default())) else {
            panic!("missing static keys must fail")
        };
        assert!(
            matches!(err, StorageError::Config(ref m) if m.contains("static S3 credentials")),
            "missing static keys must be a loud config error: {err}"
        );
        let with_keys = S3Options {
            endpoint: Some("https://s3.example.com".into()),
            region: Some("us-east-1".into()),
            access_key_id: Some("k".into()),
            secret_access_key: Some("s".into()),
            path_style: true,
        };
        StorageIo::open(&loc, Some(&with_keys)).expect("static keys open");
    }

    /// 13-review #6: plain HTTP is allowed ONLY for explicitly configured
    /// http:// endpoints (fixture stores); https and AWS defaults stay
    /// TLS-only.
    #[test]
    fn s3_allow_http_tracks_endpoint_scheme() {
        assert!(!s3_allow_http(None));
        assert!(!s3_allow_http(Some("https://s3.example.com")));
        assert!(!s3_allow_http(Some("s3.amazonaws.com")));
        assert!(s3_allow_http(Some("http://localhost:9000")));
    }

    /// TDD anchor (M1 slice 4): an s3:// location parses into
    /// (bucket, key) — the `metadata_location` from slice 3's loadTable.
    #[test]
    fn parses_s3_location() {
        let loc =
            parse_location("s3://warehouse/01a1f4c8/metadata/00000-x.gz.metadata.json").unwrap();
        match loc {
            Location::S3 { bucket, key } => {
                assert_eq!(bucket, "warehouse");
                assert_eq!(key, "01a1f4c8/metadata/00000-x.gz.metadata.json");
            }
            other => panic!("expected S3, got {other:?}"),
        }
    }

    /// file:// locations roundtrip through `object_store`'s local backend
    /// (this is how unit tests exercise the full fetch+parse path with no
    /// network, doc 10 §1).
    #[test]
    fn fetches_and_parses_metadata_from_local_store() {
        let dir = tempfile::tempdir().unwrap();
        let md_path = dir.path().join("metadata");
        std::fs::create_dir_all(&md_path).unwrap();
        let metadata_json = r#"{
            "format-version": 2,
            "table-uuid": "9c12d441-03fe-11ed-8122-d358e75d12eb",
            "location": "s3://warehouse/analytics/events",
            "schemas": [{"type": "struct", "schema-id": 0, "fields": [
                {"id": 1, "name": "id", "required": true, "type": "long"}
            ]}],
            "current-schema-id": 0
        }"#;
        std::fs::write(md_path.join("00000-x.metadata.json"), metadata_json).unwrap();

        let loc = parse_location(&format!("file://{}", dir.path().display()))
            .expect("parse file location");
        let io = StorageIo::open(&loc, None).expect("open store");
        let fetched = io
            .get_bytes("metadata/00000-x.metadata.json")
            .expect("fetch");
        let md: crate::catalog::metadata::TableMetadata =
            serde_json::from_slice(&fetched).expect("parse fetched metadata");
        assert_eq!(
            md.format_version,
            crate::catalog::metadata::FormatVersion::V2
        );
    }

    /// Gzipped metadata (Lakekeeper writes .gz.metadata.json) decodes
    /// transparently in `fetch_metadata`.
    #[test]
    fn fetch_metadata_decodes_gzip() {
        use flate2::write::GzEncoder;
        use std::io::Write as _;

        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path()).unwrap();
        let io = StorageIo::open(
            &parse_location(&format!("file://{}", dir.path().display())).unwrap(),
            None,
        )
        .unwrap();

        let metadata_json = br#"{"format-version": 3,
            "table-uuid": "9c12d441-03fe-11ed-8122-d358e75d12eb",
            "location": "s3://warehouse/t",
            "schemas": [{"type": "struct", "schema-id": 0, "fields": [
                {"id": 1, "name": "id", "required": true, "type": "long"}]}],
            "current-schema-id": 0}"#;
        let mut enc = GzEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(metadata_json).unwrap();
        io.put_bytes("metadata/00000.gz.metadata.json", enc.finish().unwrap())
            .unwrap();

        let md = fetch_metadata(&io, "metadata/00000.gz.metadata.json").unwrap();
        assert_eq!(
            md.format_version,
            crate::catalog::metadata::FormatVersion::V3
        );
    }

    #[test]
    fn scoped_metadata_caps_both_object_bytes_and_gzip_expansion() {
        use std::io::Write;

        test_allow_local_stores();
        let dir = tempfile::tempdir().unwrap();
        let location = parse_location(&format!("file://{}", dir.path().display())).unwrap();
        let io = StorageIo::open(&location, None).unwrap();
        let metadata = format!(
            r#"{{"format-version":2,"table-uuid":"9c12d441-03fe-11ed-8122-d358e75d12eb",
                 "location":"s3://warehouse/events","padding":"{}"}}"#,
            "x".repeat(4096)
        );
        let mut writer = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        writer.write_all(metadata.as_bytes()).unwrap();
        let compressed = writer.finish().unwrap();
        assert!(compressed.len() < 256);
        io.put_bytes("expanded.json", compressed).unwrap();
        io.put_bytes("oversized.json", metadata.into_bytes())
            .unwrap();

        let deadline = std::time::Duration::from_secs(2);
        for (key, diagnostic) in [
            ("expanded.json", "expansion exceeds"),
            ("oversized.json", "byte limit"),
        ] {
            let error = fetch_metadata_bounded(&io, key, 256, deadline).unwrap_err();
            assert!(matches!(error, StorageError::Config(reason) if reason.contains(diagnostic)));
        }
    }

    #[test]
    fn create_only_put_keeps_original_object() {
        test_allow_local_stores();
        let dir = tempfile::tempdir().unwrap();
        let location = Location::Local {
            root: dir.path().display().to_string(),
        };
        let io = StorageIo::open(&location, None).unwrap();
        io.put_bytes_new("metadata/manifest.avro", b"original".to_vec())
            .unwrap();
        assert!(matches!(
            io.put_bytes_new("metadata/manifest.avro", b"replacement".to_vec()),
            Err(StorageError::Conflict(_))
        ));
        assert_eq!(io.get_bytes("metadata/manifest.avro").unwrap(), b"original");
    }

    #[test]
    fn bounded_digest_reads_the_exact_object() {
        test_allow_local_stores();
        let dir = tempfile::tempdir().unwrap();
        let location = Location::Local {
            root: dir.path().display().to_string(),
        };
        let io = StorageIo::open(&location, None).unwrap();
        let bytes = vec![0x7a; 32 * 1024];
        io.put_bytes("manifest.avro", bytes.clone()).unwrap();
        let budget = std::time::Duration::from_secs(2);
        assert_eq!(
            io.sha256_bounded("manifest.avro", 32768, budget).unwrap(),
            crate::catalog::publication::sha256(&bytes)
        );
        assert!(matches!(
            io.sha256_bounded("manifest.avro", 32767, budget),
            Err(StorageError::Config(_))
        ));
        assert!(matches!(
            io.sha256_bounded("missing.avro", 32768, budget),
            Err(StorageError::NotFound(_))
        ));
    }

    #[test]
    fn bounded_digest_reads_one_s3_range_and_enforces_its_deadline() {
        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", server.server_addr());
        let (release, held) = std::sync::mpsc::channel();
        let responder = std::thread::spawn(move || {
            let receive = || {
                let request = server
                    .recv_timeout(std::time::Duration::from_secs(4))
                    .unwrap()
                    .unwrap();
                assert_eq!(request.method(), &tiny_http::Method::Get);
                let range = request
                    .headers()
                    .iter()
                    .find(|h| h.field.equiv("Range"))
                    .unwrap();
                assert_eq!(range.value.as_str(), "bytes=0-64");
                request
            };
            let mut response =
                tiny_http::Response::from_string("full object").with_status_code(206);
            for (name, value) in [
                ("Content-Range", "bytes 0-10/11"),
                ("ETag", "\"abc\""),
                ("Last-Modified", "Sat, 21 Nov 2026 14:00:00 GMT"),
            ] {
                response.add_header(tiny_http::Header::from_bytes(name, value).unwrap());
            }
            receive().respond(response).unwrap();
            let request = receive();
            held.recv_timeout(std::time::Duration::from_secs(4))
                .unwrap();
            drop(request);
        });
        let io = StorageIo::open(
            &Location::S3 {
                bucket: "fixture".into(),
                key: "t".into(),
            },
            Some(&S3Options {
                endpoint: Some(endpoint),
                region: Some("us-east-1".into()),
                access_key_id: Some("test".into()),
                secret_access_key: Some("test".into()),
                path_style: true,
            }),
        )
        .unwrap();
        assert_eq!(
            io.sha256_bounded("manifest.avro", 64, std::time::Duration::from_secs(3))
                .unwrap(),
            crate::catalog::publication::sha256(b"full object")
        );
        let started = std::time::Instant::now();
        let result = io.sha256_bounded("manifest.avro", 64, std::time::Duration::from_millis(500));
        let elapsed = started.elapsed();
        release.send(()).unwrap();
        responder.join().unwrap();
        assert!(matches!(result, Err(StorageError::Http(message)) if message.contains("deadline")));
        eprintln!("manifest-list S3 fixture: 1 ranged GET per lookup, stalled read {elapsed:?}, budget 500 ms");
    }

    /// A missing object is a `NotFound`, distinct from transport errors
    /// (doc 03 §5 failure semantics; callers surface per-kind errors).
    #[test]
    fn missing_object_is_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let loc = parse_location(&format!("file://{}", dir.path().display())).unwrap();
        let io = StorageIo::open(&loc, None).expect("open store");
        let err = io.get_bytes("nope/missing.json").unwrap_err();
        assert!(matches!(err, StorageError::NotFound(_)), "{err}");
    }
}
