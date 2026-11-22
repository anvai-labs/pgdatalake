//! Read-only reconciliation of a durably authorized staged append.

use std::collections::HashMap;

use super::commit::{RefKind, TableRequirement, TableUpdate};
use super::metadata::{Snapshot, TableMetadata};
use super::rest::{RestCatalog, RestError};

pub(super) const OPERATION_ID_KEY: &str = "pgdatalake.operation-id";
pub(super) const INPUT_DIGEST_KEY: &str = "pgdatalake.input-sha256";
pub(super) const MANIFEST_LIST_DIGEST_KEY: &str = "pgdatalake.manifest-list-sha256";

// lakehouse.authorize_publication_send persists the exact request.
pub(super) const MAX_STAGED_REQUEST_BYTES: usize = 64 * 1024;

pub(super) fn hex_digest(digest: [u8; 32]) -> String {
    const HEX: &[u8] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 15)]));
    }
    encoded
}

pub(crate) fn sha256(bytes: &[u8]) -> [u8; 32] {
    let digest = ring::digest::digest(&ring::digest::SHA256, bytes);
    let mut result = [0; 32];
    result.copy_from_slice(digest.as_ref());
    result
}

pub(crate) fn scoped_manifest_location(root: &str, manifest: &str) -> bool {
    scoped_object_location(root, manifest, "/metadata/")
}

pub(crate) fn scoped_object_location(root: &str, object: &str, directory: &str) -> bool {
    let Ok(root_url) = reqwest::Url::parse(root) else {
        return false;
    };
    if !root_url.username().is_empty()
        || root_url.password().is_some()
        || root_url.query().is_some()
        || root_url.fragment().is_some()
    {
        return false;
    }
    object
        .strip_prefix(root.trim_end_matches('/'))
        .and_then(|path| path.strip_prefix(directory))
        .is_some_and(|key| {
            key.split('/').all(|component| {
                !matches!(component, "" | "." | "..")
                    && component.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_')
                    })
            })
        })
}

/// Stable identity from the durable PostgreSQL claim.
#[derive(Clone, Copy)]
pub struct PublicationIdentity {
    pub operation_id: uuid::Uuid,
    pub table_uuid: uuid::Uuid,
    pub input_digest: [u8; 32],
}

/// The attempt's persisted bytes and independent ledger identity. The
/// worker must resolve the catalog authority and locator from the binding.
#[derive(Clone, Copy)]
pub struct AuthorizedPublication<'a> {
    pub identity: PublicationIdentity,
    pub expected_parent: Option<i64>,
    pub snapshot_id: i64,
    pub request_digest: [u8; 32],
    pub request_body: &'a [u8],
}

/// Storage scope resolved independently of returned catalog metadata.
pub struct PublicationStorage<'a> {
    pub table_root: &'a str,
    pub s3: &'a crate::storage::S3Options,
    pub max_bytes: u64,
    pub read_timeout: std::time::Duration,
}

impl PublicationStorage<'_> {
    fn digest(&self, location: &str) -> Result<[u8; 32], RestError> {
        use crate::storage::StorageError;
        if !scoped_manifest_location(self.table_root, location) {
            return Err(RestError::Decode(
                "manifest list falls outside the bound table root".into(),
            ));
        }
        let read = || {
            let (io, key) = crate::scan::open_for(location, Some(self.s3))?;
            io.sha256_bounded(&key, self.max_bytes, self.read_timeout)
        };
        read().map_err(|err| match err {
            StorageError::NotFound(_) => RestError::NotFound("manifest list is unavailable".into()),
            StorageError::Decode(message) => RestError::Decode(message),
            StorageError::Config(message) | StorageError::Http(message) => RestError::Http(message),
            _ => RestError::Http("manifest-list verification failed".into()),
        })
    }
}

/// A lookup can only prove acceptance. Unresolved state permits lookup
/// again and never authorizes another POST.
#[derive(Debug, PartialEq, Eq)]
pub enum PublicationLookup {
    Accepted { snapshot_id: i64 },
    Unresolved,
}

#[derive(serde::Deserialize)]
struct RetainedRequest {
    requirements: Vec<TableRequirement>,
    updates: Vec<TableUpdate>,
}

pub(super) struct RetainedPublication {
    table_uuid: uuid::Uuid,
    proposed: Snapshot,
}

impl AuthorizedPublication<'_> {
    pub(super) fn decode(self) -> Option<RetainedPublication> {
        if self.request_body.len() > MAX_STAGED_REQUEST_BYTES
            || sha256(self.request_body) != self.request_digest
        {
            return None;
        }
        let RetainedRequest {
            requirements,
            updates,
        } = serde_json::from_slice(self.request_body).ok()?;
        let [TableRequirement::AssertTableUUID { uuid }, TableRequirement::AssertRefSnapshotId {
            r#ref,
            snapshot_id: parent,
        }, TableRequirement::AssertCurrentSchemaId { current_schema_id }, TableRequirement::AssertDefaultSpecId { .. }] =
            requirements.as_slice()
        else {
            return None;
        };
        let [TableUpdate::AddSnapshot { snapshot }, TableUpdate::SetSnapshotRef {
            ref_name,
            kind: RefKind::Branch,
            snapshot_id,
            ..
        }] = updates.as_slice()
        else {
            return None;
        };
        let digest = snapshot.summary.get(MANIFEST_LIST_DIGEST_KEY)?;
        if *uuid != self.identity.table_uuid
            || r#ref != "main"
            || ref_name != "main"
            || *parent != self.expected_parent
            || snapshot.parent_snapshot_id != *parent
            || *snapshot_id != self.snapshot_id
            || snapshot.snapshot_id != self.snapshot_id
            || snapshot.schema_id != Some(*current_schema_id)
            || snapshot.summary.get("operation").map(String::as_str) != Some("append")
            || snapshot.summary.get(OPERATION_ID_KEY)
                != Some(&self.identity.operation_id.to_string())
            || snapshot.summary.get(INPUT_DIGEST_KEY)
                != Some(&hex_digest(self.identity.input_digest))
            || digest.len() != 64
            || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return None;
        }
        Some(RetainedPublication {
            table_uuid: *uuid,
            proposed: snapshot.clone(),
        })
    }
}

impl RetainedPublication {
    pub(super) fn verify<F>(
        &self,
        md: &TableMetadata,
        read_manifest_digest: F,
    ) -> Result<PublicationLookup, RestError>
    where
        F: FnOnce(&str) -> Result<[u8; 32], RestError>,
    {
        let Some(main) = md.refs.get("main").filter(|r| r.kind == "branch") else {
            return Ok(PublicationLookup::Unresolved);
        };
        if md.table_uuid != self.table_uuid || md.current_snapshot_id != Some(main.snapshot_id) {
            return Ok(PublicationLookup::Unresolved);
        }
        let proposed = &self.proposed;
        let mut by_id = HashMap::with_capacity(md.snapshots.len());
        for snapshot in &md.snapshots {
            if by_id.insert(snapshot.snapshot_id, snapshot).is_some()
                || (snapshot.snapshot_id != proposed.snapshot_id
                    && snapshot.summary.get(OPERATION_ID_KEY)
                        == proposed.summary.get(OPERATION_ID_KEY))
            {
                return Ok(PublicationLookup::Unresolved);
            }
        }
        let mut next_id = Some(main.snapshot_id);
        let mut child_sequence = None;
        while let Some(snapshot) = next_id.and_then(|id| by_id.remove(&id)) {
            let Some(sequence) = snapshot.sequence_number else {
                break;
            };
            if child_sequence.is_some_and(|child| sequence >= child) {
                break;
            }
            if snapshot.snapshot_id == proposed.snapshot_id {
                if snapshot.parent_snapshot_id == proposed.parent_snapshot_id
                    && snapshot.sequence_number == proposed.sequence_number
                    && snapshot.schema_id == proposed.schema_id
                    && snapshot.timestamp_ms == proposed.timestamp_ms
                    && snapshot.first_row_id == proposed.first_row_id
                    && snapshot.manifest_list == proposed.manifest_list
                    && scoped_manifest_location(&md.location, &snapshot.manifest_list)
                    && proposed
                        .summary
                        .iter()
                        .all(|(key, expected)| snapshot.summary.get(key) == Some(expected))
                    && Some(&hex_digest(read_manifest_digest(&snapshot.manifest_list)?))
                        == proposed.summary.get(MANIFEST_LIST_DIGEST_KEY)
                {
                    return Ok(PublicationLookup::Accepted {
                        snapshot_id: snapshot.snapshot_id,
                    });
                }
                break;
            }
            child_sequence = Some(sequence);
            next_id = snapshot.parent_snapshot_id;
        }
        Ok(PublicationLookup::Unresolved)
    }
}

impl RestCatalog {
    /// Check catalog acceptance using a fresh, scoped, bounded manifest read.
    pub fn lookup_staged_append_on_storage(
        &self,
        namespace: &[&str],
        table: &str,
        authorization: AuthorizedPublication<'_>,
        storage: &PublicationStorage<'_>,
    ) -> Result<PublicationLookup, RestError> {
        match self.lookup_staged_append(namespace, table, authorization, |location| {
            storage.digest(location)
        }) {
            Err(RestError::NotFound(_)) => Ok(PublicationLookup::Unresolved),
            result => result,
        }
    }

    /// Resolve from a fresh catalog read and a fresh, scoped object digest.
    /// The object callback must apply the catalog authority's I/O bounds
    /// and verify the expected object against that authority.
    pub(crate) fn lookup_staged_append<F>(
        &self,
        namespace: &[&str],
        table: &str,
        authorization: AuthorizedPublication<'_>,
        read_manifest_digest: F,
    ) -> Result<PublicationLookup, RestError>
    where
        F: FnOnce(&str) -> Result<[u8; 32], RestError>,
    {
        let Some(retained) = authorization.decode() else {
            return Ok(PublicationLookup::Unresolved);
        };
        let loaded = match self.load_table(namespace, table) {
            Err(RestError::NotFound(_)) => return Ok(PublicationLookup::Unresolved),
            result => result?,
        };
        let Some(md) = loaded.metadata else {
            return Ok(PublicationLookup::Unresolved);
        };
        retained.verify(&md, read_manifest_digest)
    }
}
