//! One explicitly enrolled, exact-pin Iceberg v2 read over REST and S3.

use crate::catalog::metadata::{FormatVersion, SnapshotSelector};
use crate::catalog::rest::{RestCatalog, StaticToken};
use crate::scan::{ScanPlan, ScanPredicate};
use crate::storage::{fetch_metadata, Location, S3Options, StorageError, StorageIo};

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
        let key = format!("{}{}", self.prefix, self.relative_path(uri)?);
        let location = Location::S3 {
            bucket: self.bucket.clone(),
            key: key.clone(),
        };
        Ok((StorageIo::open_bound_s3(&location, s3)?, key))
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
    pub(crate) root: S3ReadRoot,
    table_uuid: uuid::Uuid,
    catalog_prefix: String,
    region: String,
    catalog: RestCatalog,
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
    ) -> Result<(ScanPlan, S3Options, crate::catalog::metadata::Schema), ReadError> {
        let Some(SnapshotSelector::Id { value }) = selector else {
            return Err(ReadError::unsupported(
                "bound reads require an explicit snapshot ID",
            ));
        };
        // Names form URL path segments. Refuse dot segments before URL handling.
        if namespace
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
        let table_result = self
            .catalog
            .load_table_at_prefix(namespace, table, prefix)
            .map_err(|e| ReadError(PgDatalakeReadStatus::ReadUnavailable, e.to_string()))?;

        let (io, key) = self.root.open(&table_result.metadata_location, &s3)?;
        let md = fetch_metadata(&io, &key)?;
        if md.table_uuid != self.table_uuid || md.location != self.root.uri {
            return Err(ReadError(
                PgDatalakeReadStatus::ReadChangedBinding,
                "catalog table UUID or location differs from the enrolled binding".into(),
            ));
        }
        if md.format_version != FormatVersion::V2 {
            return Err(ReadError::unsupported(
                "bound reads currently require Iceberg v2",
            ));
        }
        if md.snapshot(*value).is_none() {
            return Err(ReadError(
                PgDatalakeReadStatus::ReadPinUnavailable,
                "requested snapshot is absent from the enrolled table".into(),
            ));
        }
        let plan = crate::scan::plan_scan_with_root(
            &md,
            selector,
            Some(&s3),
            predicate,
            Some(&self.root),
        )?;
        let schema = md.current_schema().cloned().ok_or_else(|| {
            ReadError(
                PgDatalakeReadStatus::ReadUnavailable,
                "enrolled table has no current schema".into(),
            )
        })?;
        Ok((plan, s3, schema))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
