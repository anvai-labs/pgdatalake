//! One explicitly enrolled, exact-pin Iceberg v2 read over REST and S3.

use std::time::Duration;

use crate::catalog::metadata::{FormatVersion, Schema, SnapshotSelector, TableMetadata};
use crate::catalog::rest::{RestCatalog, StaticToken};
use crate::scan::{ScanPlan, ScanPredicate};
use crate::storage::{fetch_metadata_bounded, Location, S3Options, StorageError, StorageIo};

const SCOPED_METADATA_BYTES: usize = 8 * 1024 * 1024;
const SCOPED_METADATA_DEADLINE: Duration = Duration::from_secs(4);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
pub enum PgDatalakeReadStatus {
    ReadOk,
    ReadDenied,
    ReadChangedBinding,
    ReadPinUnavailable,
    ReadUnsupported,
    ReadUnavailable,
}

#[derive(Debug)]
pub(crate) struct ReadError(pub PgDatalakeReadStatus, pub String);

impl ReadError {
    pub(crate) fn unsupported(message: impl Into<String>) -> Self {
        Self(PgDatalakeReadStatus::ReadUnsupported, message.into())
    }
}

impl From<StorageError> for ReadError {
    fn from(error: StorageError) -> Self {
        let status = match error {
            StorageError::ReadDenied(_) => PgDatalakeReadStatus::ReadDenied,
            _ => PgDatalakeReadStatus::ReadUnavailable,
        };
        Self(status, error.to_string())
    }
}

pub(crate) fn valid_component(component: &str) -> bool {
    !matches!(component, "" | "." | "..")
        && component
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
}

pub(crate) fn valid_path(path: &str) -> bool {
    path.split('/').all(valid_component)
}

/// The approved directory is independent of the catalog response. Only
/// canonical S3 paths are accepted so the check and the transport see the
/// same bucket and key.
#[derive(Clone)]
pub(crate) struct S3ReadRoot {
    uri: String,
    bucket: String,
    prefix: String,
}

impl S3ReadRoot {
    pub(crate) fn parse(uri: String) -> Result<Self, ReadError> {
        let Some((bucket, prefix)) = uri.strip_prefix("s3://").and_then(|s| s.split_once('/'))
        else {
            return Err(ReadError::unsupported(
                "expected a canonical s3:// table root",
            ));
        };
        let valid_bucket = (3..=63).contains(&bucket.len())
            && bucket.as_bytes()[0].is_ascii_alphanumeric()
            && bucket.as_bytes()[bucket.len() - 1].is_ascii_alphanumeric()
            && !bucket.contains("..")
            && bucket.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'.')
            });
        if !valid_bucket || !valid_path(prefix) {
            return Err(ReadError::unsupported(
                "expected a canonical s3:// table root",
            ));
        }
        Ok(Self {
            bucket: bucket.to_owned(),
            prefix: format!("{prefix}/"),
            uri,
        })
    }

    fn relative_path<'a>(&self, uri: &'a str) -> Result<&'a str, StorageError> {
        uri.strip_prefix(&self.uri)
            .and_then(|tail| tail.strip_prefix('/'))
            .filter(|tail| valid_path(tail))
            .ok_or_else(|| {
                StorageError::ReadDenied("object is outside the approved table root".into())
            })
    }

    pub(crate) fn check_object(&self, uri: &str) -> Result<(), StorageError> {
        self.relative_path(uri).map(|_| ())
    }

    pub(crate) fn open(
        &self,
        uri: &str,
        s3: &S3Options,
    ) -> Result<(StorageIo, String), StorageError> {
        let location = self.object_location(uri)?;
        let Location::S3 { key, .. } = &location else {
            unreachable!("validated S3 root");
        };
        Ok((StorageIo::open_bound_s3(&location, s3)?, key.clone()))
    }

    pub(crate) fn object_location(&self, uri: &str) -> Result<Location, StorageError> {
        Ok(Location::S3 {
            bucket: self.bucket.clone(),
            key: format!("{}{}", self.prefix, self.relative_path(uri)?),
        })
    }
}

/// Both routes come from local operator configuration. Restrict cleartext
/// to the same-machine fixture profile. Storage endpoints are origins.
pub(crate) fn checked_endpoint(value: &str, storage: bool) -> Result<String, ReadError> {
    let value = value.trim_end_matches('/');
    let parse_error = || ReadError::unsupported("invalid endpoint for the bound read profile");
    let url = reqwest::Url::parse(value).map_err(|_| parse_error())?;
    let loopback = match url.host_str() {
        Some(host) => host
            .trim_matches(['[', ']'])
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback()),
        None => false,
    };
    if !(url.scheme() == "https" || (url.scheme() == "http" && loopback))
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || (storage && url.path() != "/")
    {
        return Err(parse_error());
    }
    Ok(value.to_owned())
}

pub(crate) struct ReadBinding {
    root: S3ReadRoot,
    table_uuid: uuid::Uuid,
    catalog_prefix: String,
    region: String,
    catalog: RestCatalog,
}

/// One metadata observation fetched from the enrolled route and storage root.
/// Selection can inspect metadata before consuming the observation to plan an
/// exact scan. Raw metadata alone cannot open a scoped reader.
pub(crate) struct BoundReadObservation {
    root: S3ReadRoot,
    metadata: TableMetadata,
    s3: S3Options,
}

pub(crate) struct BoundSnapshotPlan {
    pub(crate) plan: ScanPlan,
    pub(crate) s3: S3Options,
    pub(crate) schema: Schema,
    pub(crate) root: S3ReadRoot,
}

impl BoundReadObservation {
    pub(crate) fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    pub(crate) fn plan_snapshot(
        self,
        snapshot_id: i64,
        predicate: Option<&ScanPredicate>,
    ) -> Result<BoundSnapshotPlan, ReadError> {
        let md = self.metadata();
        if md.snapshot(snapshot_id).is_none() {
            return Err(ReadError(
                PgDatalakeReadStatus::ReadPinUnavailable,
                "requested snapshot is absent from the enrolled table".into(),
            ));
        }
        let selector = SnapshotSelector::Id { value: snapshot_id };
        let plan = crate::scan::plan_scan_with_root(
            md,
            Some(&selector),
            Some(&self.s3),
            predicate,
            Some(&self.root),
        )?;
        let schema = md.current_schema().cloned().ok_or_else(|| {
            ReadError(
                PgDatalakeReadStatus::ReadUnavailable,
                "enrolled table has no current schema".into(),
            )
        })?;
        Ok(BoundSnapshotPlan {
            plan,
            s3: self.s3,
            schema,
            root: self.root,
        })
    }
}

impl ReadBinding {
    pub(crate) fn new(
        table_uuid: &str,
        root: String,
        catalog_prefix: String,
        region: String,
        endpoint: &str,
        token: String,
    ) -> Result<Self, ReadError> {
        let uuid = uuid::Uuid::parse_str(table_uuid)
            .map_err(|_| ReadError::unsupported("expected a canonical table UUID"))?;
        if uuid.to_string() != table_uuid
            || (!catalog_prefix.is_empty() && !valid_path(&catalog_prefix))
            || region.is_empty()
            || !region
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return Err(ReadError::unsupported(
                "invalid table UUID, catalog prefix or S3 signing region",
            ));
        }
        Ok(Self {
            root: S3ReadRoot::parse(root)?,
            table_uuid: uuid,
            catalog_prefix,
            region,
            catalog: RestCatalog::new_scoped(
                &checked_endpoint(endpoint, false)?,
                Box::new(StaticToken(token)),
            )
            .map_err(|e| ReadError(PgDatalakeReadStatus::ReadUnavailable, e.to_string()))?,
        })
    }

    pub(crate) fn scan(
        &self,
        namespace: &[&str],
        table: &str,
        client: crate::session::S3ClientOptions,
        predicate: Option<&ScanPredicate>,
        selector: Option<&SnapshotSelector>,
    ) -> Result<BoundSnapshotPlan, ReadError> {
        let Some(SnapshotSelector::Id { value }) = selector else {
            return Err(ReadError::unsupported(
                "bound reads require an explicit snapshot ID",
            ));
        };
        self.observe(namespace, table, client)?
            .plan_snapshot(*value, predicate)
    }

    pub(crate) fn observe(
        &self,
        namespace: &[&str],
        table: &str,
        client: crate::session::S3ClientOptions,
    ) -> Result<BoundReadObservation, ReadError> {
        // Names form URL path segments. Refuse dot segments before URL handling.
        if namespace.is_empty()
            || namespace
                .iter()
                .any(|part| matches!(*part, "" | "." | ".."))
            || matches!(table, "" | "." | "..")
        {
            return Err(ReadError::unsupported(
                "bound reads require a table namespace and name",
            ));
        }
        let (Some(s3_endpoint), Some(access_key_id), Some(secret_access_key)) = (
            client.endpoint,
            client.access_key_id,
            client.secret_access_key,
        ) else {
            return Err(ReadError::unsupported(
                "bound reads require a configured S3 endpoint and mapped static S3 credentials",
            ));
        };
        let s3 = S3Options {
            endpoint: Some(checked_endpoint(&s3_endpoint, true)?),
            region: Some(self.region.clone()),
            access_key_id: Some(access_key_id),
            secret_access_key: Some(secret_access_key),
            path_style: true,
        };

        // The operator provides the exact route. Fetching configuration could
        // accept delegated storage credentials or change the route or profile.
        let prefix = (!self.catalog_prefix.is_empty()).then_some(self.catalog_prefix.as_str());
        let metadata_location = self
            .catalog
            .load_scoped_metadata_location(
                namespace,
                table,
                prefix,
                SCOPED_METADATA_BYTES,
                SCOPED_METADATA_DEADLINE,
            )
            .map_err(|e| ReadError(PgDatalakeReadStatus::ReadUnavailable, e.to_string()))?;

        let (io, key) = self.root.open(&metadata_location, &s3)?;
        let metadata =
            fetch_metadata_bounded(&io, &key, SCOPED_METADATA_BYTES, SCOPED_METADATA_DEADLINE)?;
        if metadata.table_uuid != self.table_uuid || metadata.location != self.root.uri {
            return Err(ReadError(
                PgDatalakeReadStatus::ReadChangedBinding,
                "catalog table UUID or location differs from the enrolled binding".into(),
            ));
        }
        if metadata.format_version != FormatVersion::V2 {
            return Err(ReadError::unsupported(
                "bound reads currently require Iceberg v2",
            ));
        }
        Ok(BoundReadObservation {
            root: self.root.clone(),
            metadata,
            s3,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_approved_observation_plans_its_exact_retained_pin() {
        use serde_json::json;
        use tiny_http::{Header, Response, Server};

        const UUID: &str = "2a830328-da37-4b0b-a867-78912e62c620";
        const ROOT: &str = "s3://warehouse/ns/events";
        const PIN: i64 = 9_007_199_254_740_993;
        let server = Server::http("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", server.server_addr().to_ip().unwrap());
        let worker = std::thread::spawn(move || {
            let recv = || {
                server
                    .recv_timeout(Duration::from_secs(3))
                    .unwrap()
                    .unwrap()
            };
            let header = |name: &str, value: &str| Header::from_bytes(name, value).unwrap();
            let catalog = recv();
            assert_eq!(catalog.url(), "/v1/site/namespaces/ns/tables/events");
            catalog
                .respond(Response::from_string(
                    json!({"metadata-location": format!("{ROOT}/metadata/current.json"),
                           "metadata": {"location": "s3://foreign/unsafe"}})
                    .to_string(),
                ))
                .unwrap();
            let object = recv();
            assert_eq!(object.url(), "/warehouse/ns/events/metadata/current.json");
            assert!(object
                .headers()
                .iter()
                .any(|field| field.field.equiv("Range")));
            let metadata = json!({
                "format-version": 2,
                "table-uuid": UUID,
                "location": ROOT,
                "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
                "current-schema-id": 0,
                "current-snapshot-id": PIN+1,
                "snapshots": [
                    {"snapshot-id": PIN, "timestamp-ms": 1000, "manifest-list": format!("{ROOT}/metadata/retained.avro")},
                    {"snapshot-id": PIN+1, "timestamp-ms": 1001, "manifest-list": format!("{ROOT}/metadata/current.avro")}
                ]
            })
            .to_string();
            let content_range = format!("bytes 0-{}/{}", metadata.len() - 1, metadata.len());
            let etag = header("ETag", r#""fixture""#);
            let modified = header("Last-Modified", "Mon, 01 Jan 2024 00:00:00 GMT");
            object
                .respond(
                    Response::from_string(metadata)
                        .with_status_code(206)
                        .with_header(header("Content-Range", &content_range))
                        .with_header(etag.clone())
                        .with_header(modified.clone()),
                )
                .unwrap();
            let manifest = recv();
            assert_eq!(
                manifest.url(),
                "/warehouse/ns/events/metadata/retained.avro"
            );
            manifest
                .respond(
                    Response::from_data(
                        crate::snapshot_write::build_manifest_list_bytes(&[]).unwrap(),
                    )
                    .with_header(etag)
                    .with_header(modified),
                )
                .unwrap();
        });

        let binding = ReadBinding::new(
            UUID,
            ROOT.into(),
            "site".into(),
            "us-east-1".into(),
            &endpoint,
            "fixture".into(),
        )
        .unwrap();
        let observation = binding
            .observe(
                &["ns"],
                "events",
                crate::session::S3ClientOptions {
                    endpoint: Some(endpoint),
                    access_key_id: Some("test-key".into()),
                    secret_access_key: Some("test-secret".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(observation.metadata().current_snapshot_id, Some(PIN + 1));
        let plan = observation.plan_snapshot(PIN, None).unwrap().plan;
        assert_eq!(plan.snapshot_id, PIN);
        assert!(plan.tasks.is_empty() && plan.delete_tasks.is_empty());
        worker.join().unwrap();
    }

    #[test]
    fn bound_storage_paths_have_one_canonical_identity() {
        let root = S3ReadRoot::parse("s3://bucket/catalog/orders".to_owned()).unwrap();
        assert_eq!(
            root.relative_path("s3://bucket/catalog/orders/metadata/abc.avro")
                .unwrap(),
            "metadata/abc.avro"
        );
        for uri in [
            "s3://other/catalog/orders/metadata/abc.avro",
            "s3://bucket/catalog/orders-old/abc.avro",
            "s3://bucket/catalog/orders",
            "s3://bucket/catalog/orders/../abc.avro",
            "s3://bucket/catalog/orders//abc.avro",
            "s3://bucket/catalog/orders/metadata%2fabc.avro",
            "s3://bucket/catalog/orders/metadata/abc.avro?x=1",
        ] {
            assert!(matches!(
                root.check_object(uri),
                Err(StorageError::ReadDenied(_))
            ));
        }
    }
}
