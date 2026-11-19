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
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::NotFound(m) => write!(f, "object not found: {m}"),
            StorageError::Http(m) => write!(f, "storage IO error: {m}"),
            StorageError::Decode(m) => write!(f, "storage decode error: {m}"),
            StorageError::Config(m) => write!(f, "storage config error: {m}"),
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
    store: Box<dyn ObjectStore>,
}

impl StorageIo {
    pub fn open(loc: &Location, s3: Option<&S3Options>) -> Result<Self, StorageError> {
        let store: Box<dyn ObjectStore> = match loc {
            Location::Local { root } => Box::new(
                object_store::local::LocalFileSystem::new_with_prefix(root)
                    .map_err(|e| StorageError::Config(e.to_string()))?,
            ),
            Location::S3 { bucket, .. } => {
                let opts = s3.ok_or_else(|| {
                    StorageError::Config("s3 location requires S3Options".to_string())
                })?;
                let mut b = object_store::aws::AmazonS3Builder::new()
                    .with_bucket_name(bucket)
                    .with_allow_http(true);
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
        Ok(Self { store })
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
    let bytes = io.get_bytes(metadata_location_key)?;
    let bytes = maybe_gunzip(bytes)?;
    serde_json::from_slice(&bytes).map_err(|e| StorageError::Decode(e.to_string()))
}

/// Gzip magic 0x1f 0x8b; plain JSON passes through untouched.
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
