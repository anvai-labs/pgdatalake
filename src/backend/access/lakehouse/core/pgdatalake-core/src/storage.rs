//! Object-store file IO (specs/lakehouse/01-architecture.md layer 1,
//! doc 04-storage.md). Slice 4: location parsing + metadata.json fetch over
//! any object_store backend (local for tests, S3/SeaweedFS in fixtures;
//! ADLS/GCS land in M3). Blocking surface: the C glue is synchronous, so a
//! shared single-thread tokio runtime lives behind these calls.

use std::sync::OnceLock;

use object_store::{ObjectStore, PutPayload};

use crate::catalog::metadata::TableMetadata;

/// Errors surfaced to the C glue; NotFound is distinct (doc 03 §5).
#[derive(Debug)]
pub enum StorageError {
    NotFound(String),
    Http(String),
    Decode(String),
    Config(String),
    /// REST catalog 409 CommitFailedException-equivalent: a concurrent
    /// writer won, or the object already exists. Distinct so callers react
    /// by TYPE, never by searching error strings for "409" (13-review #6).
    Conflict(String),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::NotFound(m) => write!(f, "object not found: {m}"),
            StorageError::Http(m) => write!(f, "storage IO error: {m}"),
            StorageError::Decode(m) => write!(f, "storage decode error: {m}"),
            StorageError::Config(m) => write!(f, "storage config error: {m}"),
            StorageError::Conflict(m) => write!(f, "storage conflict: {m}"),
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
/// for Local the root is the directory prefix and get_bytes() keys are
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
    pub fn from_config(cfg: &std::collections::BTreeMap<String, String>) -> Self {
        Self {
            endpoint: cfg.get("s3.endpoint").cloned(),
            region: cfg.get("s3.region").cloned(),
            access_key_id: cfg.get("s3.access-key-id").cloned(),
            secret_access_key: cfg.get("s3.secret-access-key").cloned(),
            path_style: cfg
                .get("s3.path-style-access")
                .map(|v| v == "true" || v == "1")
                .unwrap_or(true),
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
    endpoint.map(|e| e.starts_with("http://")).unwrap_or(false)
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

/// An open handle to one object-store location.
pub struct StorageIo {
    /// Cache namespace (spec 15 §6.2): endpoint+bucket for S3-family
    /// stores, local-root for file stores — paths are only unique within
    /// a warehouse, so every cached key is scoped by this.
    pub scope_id: String,
    store: Box<dyn ObjectStore>,
}

impl StorageIo {
    pub fn open(loc: &Location, s3: Option<&S3Options>) -> Result<Self, StorageError> {
        let store: Box<dyn ObjectStore> = match loc {
            Location::Local { root } => open_local_store(root, local_stores_allowed())?,
            Location::S3 { bucket, .. } => {
                let opts = s3.ok_or_else(|| {
                    StorageError::Config("s3 location requires S3Options".to_string())
                })?;
                // 13-review #6: ambient cloud credential fallback is
                // removed — no static keys means a loud config error
                // (pointing at the user mapping), never an IMDS probe.
                let (ak, sk) = match (&opts.access_key_id, &opts.secret_access_key) {
                    (Some(k), Some(sec)) => (k, sec),
                    _ => {
                        return Err(StorageError::Config(
                            "static S3 credentials missing: set s3_access_key/s3_secret_key \
                             on the user mapping (ambient cloud-credential fallback is disabled)"
                                .into(),
                        ));
                    }
                };
                let mut b = object_store::aws::AmazonS3Builder::new()
                    .with_bucket_name(bucket)
                    .with_allow_http(s3_allow_http(opts.endpoint.as_deref()))
                    .with_access_key_id(ak)
                    .with_secret_access_key(sk);
                if let Some(ep) = &opts.endpoint {
                    b = b.with_endpoint(ep);
                }
                if let Some(r) = &opts.region {
                    b = b.with_region(r);
                }
                if let (Some(k), Some(sec)) = (&opts.access_key_id, &opts.secret_access_key) {
                    b = b.with_access_key_id(k).with_secret_access_key(sec);
                }
                Box::new(b.build().map_err(|e| StorageError::Config(e.to_string()))?)
            }
            // https(s) metadata locations are rare (signed URLs); wire-up
            // lands with remote-profile slices
            Location::Http { url } => Box::new(
                object_store::http::HttpBuilder::new()
                    .with_url(url.to_string())
                    .build()
                    .map_err(|e| StorageError::Config(e.to_string()))?,
            ),
        };
        // Keys passed to get_bytes()/put_bytes() are FULL paths: from the
        // bucket root for S3 (Iceberg locations are absolute within the
        // bucket), or relative to the prefix for Local (which is why the
        // local store is opened rooted). No re-prefixing happens here.
        //
        // cache namespace: endpoint+bucket (S3), the local root, or the
        // http URL — the only globally-unique identity of a store
        let scope_id = match loc {
            Location::Local { root } => format!("local:{root}"),
            Location::S3 { bucket, .. } => format!(
                "s3:{}:{bucket}",
                s3.and_then(|o| o.endpoint.as_deref()).unwrap_or("aws")
            ),
            Location::Http { url } => format!("http:{url}"),
        };
        Ok(Self { store, scope_id })
    }

    fn full_path(&self, key: &str) -> object_store::path::Path {
        object_store::path::Path::from(key)
    }

    /// GET one object as bytes; 404s map to StorageError::NotFound.
    pub fn get_bytes(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        let path = self.full_path(key);
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

    /// Ranged GET of `[offset, offset+len)` bytes (slice P0, spec 15
    /// §6.2): the read-side prerequisite for footer-driven reads and the
    /// block cache. Byte-identity contract: the returned bytes MUST
    /// equal `&get_bytes(key)[offset..offset+len]` (tested). Out-of-range
    /// is an Http error from object_store, surfaced as-is.
    pub fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Vec<u8>, StorageError> {
        use object_store::{GetOptions, GetRange};
        let path = self.full_path(key);
        let opts = GetOptions {
            range: Some(GetRange::Bounded({
                let o = offset as usize;
                let l = len as usize;
                o..o.saturating_add(l)
            })),
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

    /// PUT one object (used by later write-path slices; exposed now so the
    /// fixture example can verify the roundtrip).
    #[allow(dead_code)]
    pub fn put_bytes(&self, key: &str, data: Vec<u8>) -> Result<(), StorageError> {
        let path = self.full_path(key);
        runtime()
            .block_on(self.store.put(&path, PutPayload::from_bytes(data.into())))
            .map_err(|e| StorageError::Http(e.to_string()))?;
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
    use super::*;

    /// 13-review #6: file:// object stores are refused unless explicitly
    /// opted in — and the opt-in API is cfg(test) only, so the shipped
    /// binary structurally cannot open catalog/manifest-supplied local
    /// paths. (The gate is tested directly on open_local_store so the
    /// process-global monotonic flag never races parallel tests.)
    #[test]
    fn file_store_requires_explicit_opt_in() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_string_lossy().into_owned();
        let err = match open_local_store(&root, false) {
            Err(e) => e,
            Ok(_) => panic!("local store must be refused without opt-in"),
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
        let err = match StorageIo::open(&loc, Some(&S3Options::default())) {
            Err(e) => e,
            Ok(_) => panic!("missing static keys must fail"),
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
    /// (bucket, key) — the metadata_location from slice 3's loadTable.
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

    /// file:// locations roundtrip through object_store's local backend
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
    /// transparently in fetch_metadata.
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

    /// A missing object is a NotFound, distinct from transport errors
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
