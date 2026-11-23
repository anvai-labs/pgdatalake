//! Scoped REST and S3 transport for an independently admitted v2 append.

use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{collections::HashSet, sync::Arc};

use object_store::aws::{AwsCredential, AwsCredentialProvider};
use uuid::Uuid;

use crate::catalog::commit::{validate_staged_base, PreparedPublication};
use crate::catalog::metadata::{FormatVersion, IcebergType, TableMetadata};
use crate::catalog::publication::{
    scoped_manifest_location, AuthorizedPublication, PublicationLookup,
};
use crate::catalog::rest::{RestCatalog, RestError, TokenProvider};
use crate::catalog::types::LoadTableResult;
use crate::publication_worker::{
    CapturedMember, PublicationBindingIdentity, PublicationCatalog, PublicationClaim,
};
use crate::read_binding::{checked_endpoint, valid_component, valid_path, S3ReadRoot};
use crate::snapshot_write::{write_bounded_snapshot, NewDataFile, SnapshotParams};
use crate::storage::{parse_location, Location, S3Options, StorageError, StorageIo};
use crate::writer::{parse_row_json, BatchWriter, WriteColumn};

/// Immutable target facts provided by trusted enrollment, independently of
/// the catalog response. Each worker gets the original descriptor on replay.
#[derive(Clone, PartialEq, Eq, serde::Deserialize)]
pub struct PublicationTarget {
    pub binding: PublicationBindingIdentity,
    pub table_uuid: Uuid,
    pub table_root: String,
    pub schema_id: i64,
    pub spec_id: i32,
    pub ref_name: String,
    pub catalog_endpoint: String,
    pub catalog_prefix: String,
    pub namespace: Vec<String>,
    pub table_name: String,
    pub object_endpoint: String,
    pub s3_region: String,
}

/// The operator resolves separate authority for an admitted send and for
/// read-only verification of the binding that was retained with an intent.
#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub enum PublicationPurpose {
    Send,
    Lookup,
}

/// An independently resolved, in-memory credential bundle for one target.
/// The resolver sets the earliest expiry of the two credentials and grant.
pub struct PublicationCredentials {
    pub target: PublicationTarget,
    pub purpose: PublicationPurpose,
    pub expires_at: SystemTime,
    pub catalog_bearer: String,
    pub objects: AwsCredential,
}

struct PublicationBearer {
    value: String,
    expires_at: SystemTime,
}

impl TokenProvider for PublicationBearer {
    fn token(&self) -> Result<String, RestError> {
        if SystemTime::now() >= self.expires_at {
            return Err(RestError::Http(
                "publication catalog credential expired".into(),
            ));
        }
        Ok(self.value.clone())
    }
}

struct PublicationS3Credentials {
    credential: Arc<AwsCredential>,
    expires_at: SystemTime,
}

impl std::fmt::Debug for PublicationS3Credentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("PublicationS3Credentials")
    }
}

#[async_trait::async_trait]
impl object_store::CredentialProvider for PublicationS3Credentials {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
        if SystemTime::now() >= self.expires_at {
            return Err(object_store::Error::Generic {
                store: "scoped S3",
                source: "publication storage credential expired".into(),
            });
        }
        Ok(self.credential.clone())
    }
}

/// Resource limits for one admitted append. The SQL scheduler must also
/// bound the members it claims and reads from the ledger.
pub struct PublicationLimits {
    pub captured_bytes: usize,
    pub captured_rows: usize,
    pub manifest_bytes: u64,
    pub manifest_timeout: Duration,
}

pub struct ScopedPublisher {
    target: PublicationTarget,
    root: S3ReadRoot,
    catalog: RestCatalog,
    objects: S3Options,
    s3_credentials: AwsCredentialProvider,
    purpose: PublicationPurpose,
    columns: Vec<(String, IcebergType)>,
    limits: PublicationLimits,
}

fn canonical_endpoint(endpoint: &str, storage: bool) -> Result<String, StorageError> {
    let invalid = || StorageError::Config("invalid endpoint for the publication binding".into());
    let checked = checked_endpoint(endpoint, storage).map_err(|_| invalid())?;
    let url = reqwest::Url::parse(&checked).map_err(|_| invalid())?;
    let path = url.path().strip_prefix('/').unwrap_or(url.path());
    if url.as_str().trim_end_matches('/') != checked || (!path.is_empty() && !valid_path(path)) {
        return Err(invalid());
    }
    Ok(checked)
}

impl ScopedPublisher {
    /// Credentials are resolved independently for the immutable descriptor.
    /// The caller supplies the source PostgreSQL column order and types.
    pub fn new(
        target: PublicationTarget,
        purpose: PublicationPurpose,
        credentials: PublicationCredentials,
        columns: Vec<(String, IcebergType)>,
        limits: PublicationLimits,
    ) -> Result<Self, StorageError> {
        if target != credentials.target
            || purpose != credentials.purpose
            || SystemTime::now() >= credentials.expires_at
        {
            return Err(StorageError::ReadDenied(
                "publication credentials differ from the approved target or purpose, or have expired".into(),
            ));
        }
        if target.binding.binding_generation <= 0
            || target.table_uuid.is_nil()
            || target.binding.authority_id.is_nil()
            || target.binding.binding_id.is_nil()
            || target.binding.incarnation_id.is_nil()
            || target.schema_id < 0
            || target.spec_id < 0
            || target.ref_name != "main"
            || (!target.catalog_prefix.is_empty() && !valid_path(&target.catalog_prefix))
            || target.namespace.is_empty()
            || target.namespace.iter().any(|part| !valid_component(part))
            || !valid_component(&target.table_name)
            || target.s3_region.is_empty()
            || !target
                .s3_region
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
            || credentials.catalog_bearer.is_empty()
            || !credentials
                .catalog_bearer
                .bytes()
                .all(|byte| byte.is_ascii_graphic())
            || credentials.objects.key_id.is_empty()
            || credentials.objects.secret_key.is_empty()
            || credentials.objects.token.as_ref().is_some_and(|token| {
                token.is_empty() || !token.bytes().all(|byte| byte.is_ascii_graphic())
            })
            || (purpose == PublicationPurpose::Send
                && (columns.is_empty() || limits.captured_bytes == 0 || limits.captured_rows == 0))
            || limits.manifest_bytes == 0
            || limits.manifest_bytes == u64::MAX
            || limits.manifest_timeout.is_zero()
        {
            return Err(StorageError::Config(
                "invalid target, credentials or limits for scoped publication".into(),
            ));
        }
        let root = S3ReadRoot::parse(target.table_root.clone())
            .map_err(|_| StorageError::Config("publication requires a canonical S3 root".into()))?;
        let mut catalog = RestCatalog::new_scoped(
            &canonical_endpoint(&target.catalog_endpoint, false)?,
            Box::new(PublicationBearer {
                value: credentials.catalog_bearer,
                expires_at: credentials.expires_at,
            }),
        )
        .map_err(|error| StorageError::Config(error.to_string()))?;
        if !target.catalog_prefix.is_empty() {
            catalog = catalog.with_prefix(&target.catalog_prefix);
        }
        let objects = S3Options {
            endpoint: Some(canonical_endpoint(&target.object_endpoint, true)?),
            region: Some(target.s3_region.clone()),
            ..S3Options::default()
        };
        Ok(Self {
            target,
            root,
            catalog,
            objects,
            s3_credentials: Arc::new(PublicationS3Credentials {
                credential: Arc::new(credentials.objects),
                expires_at: credentials.expires_at,
            }),
            purpose,
            columns,
            limits,
        })
    }

    fn open_object(&self, location: &Location) -> Result<StorageIo, StorageError> {
        StorageIo::open_bound_s3_with_credentials(
            location,
            &self.objects,
            Some(self.s3_credentials.clone()),
        )
    }

    fn open_scoped_object(&self, uri: &str) -> Result<(StorageIo, String), StorageError> {
        let location = self.root.object_location(uri)?;
        let Location::S3 { key, .. } = &location else {
            unreachable!("validated S3 root");
        };
        Ok((self.open_object(&location)?, key.clone()))
    }

    fn check_claim(&self, claim: &PublicationClaim) -> Result<(), RestError> {
        if claim.binding != Some(self.target.binding)
            || claim.identity.table_uuid != self.target.table_uuid
        {
            return Err(RestError::Conflict(
                "claim differs from the enrolled publication binding".into(),
            ));
        }
        Ok(())
    }

    fn namespace(&self) -> Vec<&str> {
        self.target.namespace.iter().map(String::as_str).collect()
    }

    fn load(&self) -> Result<LoadTableResult, RestError> {
        self.catalog
            .load_table(&self.namespace(), &self.target.table_name)
    }

    fn target_metadata(&self, loaded: LoadTableResult) -> Result<TableMetadata, RestError> {
        if !scoped_manifest_location(&self.target.table_root, &loaded.metadata_location) {
            return Err(RestError::Decode(
                "catalog metadata location is outside the enrolled metadata root".into(),
            ));
        }
        let metadata = loaded.metadata.ok_or_else(|| {
            RestError::Decode("scoped publication requires in-band table metadata".into())
        })?;
        if metadata.table_uuid != self.target.table_uuid
            || metadata.location != self.target.table_root
            || metadata.format_version != FormatVersion::V2
        {
            return Err(RestError::Conflict(
                "catalog table identity differs from the enrolled publication binding".into(),
            ));
        }
        Ok(metadata)
    }

    fn writer(
        &self,
        base: &TableMetadata,
        members: &[CapturedMember],
    ) -> Result<BatchWriter, StorageError> {
        let schema = base.current_schema().expect("validated staged base");
        let mismatch = || {
            StorageError::Config(
                "captured column layout differs from the enrolled Iceberg schema".into(),
            )
        };
        if schema.fields.len() != self.columns.len() {
            return Err(mismatch());
        }
        let mut field_ids = HashSet::new();
        let mut columns = Vec::with_capacity(self.columns.len());
        let mut required = Vec::with_capacity(self.columns.len());
        for (name, ty) in &self.columns {
            let field = schema
                .fields
                .iter()
                .find(|field| &field.name == name)
                .filter(|field| field.r#type == *ty && field.id > 0 && field_ids.insert(field.id))
                .ok_or_else(mismatch)?;
            required.push(field.required);
            columns.push(WriteColumn {
                name: name.clone(),
                ty: ty.clone(),
                field_id: Some(field.id),
            });
        }

        let mut bytes = 0usize;
        let mut row_count = 0usize;
        for member in members {
            bytes = bytes.saturating_add(member.payload.len());
            if member.row_count <= 0 {
                return Err(StorageError::Decode(
                    "invalid captured segment row count".into(),
                ));
            }
            row_count = row_count.saturating_add(member.row_count as usize);
        }
        if row_count == 0
            || row_count > self.limits.captured_rows
            || bytes > self.limits.captured_bytes
        {
            return Err(StorageError::Config(
                "captured publication exceeds the configured batch limits".into(),
            ));
        }

        let mut writer = BatchWriter::new(columns.clone())?;
        for member in members {
            let rows: Vec<serde_json::Value> = serde_json::from_slice(&member.payload)
                .map_err(|_| StorageError::Decode("invalid captured segment JSON".into()))?;
            if rows.len() != member.row_count as usize {
                return Err(StorageError::Decode(
                    "captured segment row count differs from the payload".into(),
                ));
            }
            for row in rows {
                let cells = parse_row_json(&columns, &row)?;
                if cells.iter().zip(&required).any(|(cell, required)| {
                    *required && matches!(cell, crate::parquet::CellValue::Null)
                }) {
                    return Err(StorageError::Decode(
                        "captured row contains a null for a required Iceberg field".into(),
                    ));
                }
                writer.push(&cells)?;
            }
        }
        Ok(writer)
    }
}

fn storage_error(error: RestError) -> StorageError {
    match error {
        RestError::Conflict(message) => StorageError::Conflict(message),
        other => StorageError::Config(other.to_string()),
    }
}

impl PublicationCatalog for ScopedPublisher {
    fn prepare(
        &self,
        claim: &PublicationClaim,
        members: &[CapturedMember],
    ) -> Result<PreparedPublication<'_>, StorageError> {
        if self.purpose != PublicationPurpose::Send {
            return Err(StorageError::ReadDenied(
                "lookup credentials cannot prepare a publication".into(),
            ));
        }
        self.check_claim(claim).map_err(storage_error)?;
        let base = self
            .target_metadata(self.load().map_err(storage_error)?)
            .map_err(storage_error)?;
        let spec_id = validate_staged_base(&base, self.target.table_uuid).map_err(storage_error)?;
        if base.current_schema_id != self.target.schema_id || spec_id != self.target.spec_id {
            return Err(StorageError::Conflict(
                "publication schema or spec changed".into(),
            ));
        }

        let mut writer = self.writer(&base, members)?;
        let carried_manifests = if let Some(snapshot) = base.current_snapshot() {
            let path = &snapshot.manifest_list;
            if !scoped_manifest_location(&self.target.table_root, path) {
                return Err(StorageError::ReadDenied(
                    "base manifest list is outside the enrolled table".into(),
                ));
            }
            let (io, key) = self.open_scoped_object(path)?;
            let manifests = crate::manifest::parse_manifest_list(&io.get_bytes_bounded(
                &key,
                self.limits.manifest_bytes,
                self.limits.manifest_timeout,
            )?)?;
            if manifests.iter().any(|manifest| {
                !scoped_manifest_location(&self.target.table_root, &manifest.manifest_path)
            }) {
                return Err(StorageError::ReadDenied(
                    "carried manifest is outside the enrolled table".into(),
                ));
            }
            manifests
        } else {
            Vec::new()
        };

        let location = parse_location(&self.target.table_root)?;
        let Location::S3 { bucket, key } = &location else {
            unreachable!("validated S3 root");
        };
        let io = self.open_object(&location)?;
        let file = writer.write_file(&io, &format!("{key}/data"), 0)?;
        let snapshot_id = ((Uuid::new_v4().as_u128() >> 64) as i64) & i64::MAX;
        let written = write_bounded_snapshot(
            &io,
            &self.target.table_root,
            &[NewDataFile {
                file_path: format!("s3://{bucket}/{}", file.key),
                record_count: file.record_count,
                file_size_in_bytes: file.file_size_in_bytes,
                column_sizes: file.column_sizes,
                value_counts: file.value_counts,
                null_value_counts: file.null_value_counts,
                lower_bounds: file.lower_bounds,
                upper_bounds: file.upper_bounds,
            }],
            SnapshotParams {
                snapshot_id,
                timestamp_ms: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_or(0, |elapsed| elapsed.as_millis() as i64),
                parent_snapshot_id: base.current_snapshot_id,
                last_sequence_number: base.last_sequence_number,
                schema_id: Some(base.current_schema_id),
                partition_spec_id: spec_id,
                carried_manifests,
                deleted_files: Vec::new(),
            },
            self.limits.manifest_bytes,
        )?;
        self.catalog
            .prepare_staged_append(
                &self.namespace(),
                &self.target.table_name,
                &base,
                written.snapshot,
                claim.identity,
                written.manifest_list_digest,
            )
            .map_err(storage_error)
    }

    fn lookup(
        &self,
        claim: &PublicationClaim,
        intent: AuthorizedPublication<'_>,
    ) -> Result<PublicationLookup, RestError> {
        if self.purpose != PublicationPurpose::Lookup {
            return Err(RestError::Http(
                "publication lookup requires separate read credentials".into(),
            ));
        }
        self.check_claim(claim)?;
        if intent.identity.operation_id != claim.identity.operation_id
            || intent.identity.input_digest != claim.identity.input_digest
            || intent.identity.table_uuid != self.target.table_uuid
        {
            return Ok(PublicationLookup::Unresolved);
        }
        let Some(retained) = intent.decode() else {
            return Ok(PublicationLookup::Unresolved);
        };
        if !retained.matches_schema_and_spec(self.target.schema_id, self.target.spec_id) {
            return Ok(PublicationLookup::Unresolved);
        }
        let loaded = match self.load() {
            Err(RestError::NotFound(_)) => return Ok(PublicationLookup::Unresolved),
            result => result?,
        };
        let base = self.target_metadata(loaded)?;
        match retained.verify(&base, |path| {
            let (io, key) = self
                .open_scoped_object(path)
                .map_err(storage_error_to_rest)?;
            io.sha256_bounded(
                &key,
                self.limits.manifest_bytes,
                self.limits.manifest_timeout,
            )
            .map_err(storage_error_to_rest)
        }) {
            Err(RestError::NotFound(_)) => Ok(PublicationLookup::Unresolved),
            result => result,
        }
    }
}

fn storage_error_to_rest(error: StorageError) -> RestError {
    match error {
        StorageError::NotFound(_) => {
            RestError::NotFound("retained manifest list is unavailable".into())
        }
        StorageError::ReadDenied(message) | StorageError::Decode(message) => {
            RestError::Decode(message)
        }
        _ => RestError::Http("scoped manifest-list verification failed".into()),
    }
}

#[cfg(test)]
mod tests;
