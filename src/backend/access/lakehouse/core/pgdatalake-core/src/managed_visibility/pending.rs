//! Read-only membership proof for one WAL-authorized append at the certified head.

use std::collections::BTreeSet;
use std::time::Duration;

use crate::{
    catalog::{
        publication::{AuthorizedPublication, PublicationLookup},
        rest::RestError,
    },
    publication_worker::MAX_PUBLICATION_MEMBERS,
    read_binding::{checked_endpoint, S3ReadRoot},
    storage::S3Options,
};

use super::{
    ManagedAncestry, ManagedPinCandidate, ManagedReadError, ManagedReadScope, PinMembership,
};

const MAX_INCLUDED_MEMBERS: usize = 16_384;
const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;
const MANIFEST_DEADLINE: Duration = Duration::from_secs(4);

/// Identity of a sealed member from the same committed ledger observation as
/// the exact retained attempt. Row payloads remain with the original SQL snapshot.
#[derive(Debug)]
pub struct SealedPendingMember {
    pub segment_id: i64,
    pub row_count: i32,
    pub payload_digest: [u8; 32],
}

/// The caller reads exactly one immutable pending attempt and its full member
/// list from the active managed lineage after observing the catalog head.
pub struct PendingHeadAttempt<'a> {
    pub scope: &'a ManagedReadScope,
    pub schema_id: i64,
    pub spec_id: i32,
    pub authorization: AuthorizedPublication<'a>,
    pub members: &'a [SealedPendingMember],
}

impl ManagedAncestry<'_> {
    /// Upgrade one unknown head using the same metadata used for ancestry.
    /// The parent must already have complete cumulative membership. Object
    /// credentials and endpoint come from the independently scoped reader.
    /// A missing or unproved attempt leaves the head unknown.
    pub fn prove_pending_head(
        &self,
        parent: &ManagedPinCandidate<'_>,
        pending: &PendingHeadAttempt<'_>,
        s3: &S3Options,
    ) -> Result<Option<Vec<i64>>, ManagedReadError> {
        if parent.scope != self.scope || pending.scope != self.scope {
            return Err(ManagedReadError::ScopeMismatch);
        }
        let authorization = pending.authorization;
        let Some([head, parent_pin]) = self.pins().get(..2) else {
            return Ok(None);
        };
        if authorization.snapshot_id != *head
            || authorization.expected_parent != Some(*parent_pin)
            || parent.pin != *parent_pin
            || authorization.identity.table_uuid != self.scope.table_uuid
            || authorization.identity.operation_id.is_nil()
        {
            return Ok(None);
        }
        let PinMembership::Complete(parent_members) = parent.membership else {
            return Ok(None);
        };
        let Some(included) = checked_members(parent_members, pending) else {
            return Ok(None);
        };
        let Some(retained) = authorization.decode() else {
            return Ok(None);
        };
        if !retained.matches_schema_and_spec(pending.schema_id, pending.spec_id) {
            return Ok(None);
        }
        let Some(endpoint) = s3
            .endpoint
            .as_deref()
            .and_then(|endpoint| checked_endpoint(endpoint, true).ok())
        else {
            return Ok(None);
        };
        let Ok(root) = S3ReadRoot::parse(self.scope.storage_root.clone()) else {
            return Ok(None);
        };
        let s3 = S3Options {
            endpoint: Some(endpoint),
            ..s3.clone()
        };
        let accepted = retained.verify(self.metadata, |path| {
            let digest = || {
                let (io, key) = root.open(path, &s3)?;
                io.sha256_bounded(&key, MAX_MANIFEST_BYTES, MANIFEST_DEADLINE)
            };
            digest().map_err(|_| RestError::Http("managed read manifest proof unavailable".into()))
        });
        Ok(match accepted {
            Ok(PublicationLookup::Accepted { snapshot_id }) if snapshot_id == *head => {
                Some(included)
            }
            _ => None,
        })
    }
}

fn checked_members(parent: &[i64], pending: &PendingHeadAttempt<'_>) -> Option<Vec<i64>> {
    if !(1..=MAX_PUBLICATION_MEMBERS).contains(&pending.members.len())
        || parent.len().checked_add(pending.members.len())? > MAX_INCLUDED_MEMBERS
    {
        return None;
    }
    let mut included = BTreeSet::new();
    for &id in parent {
        if id <= 0 || !included.insert(id) {
            return None;
        }
    }
    let mut digest = ring::digest::Context::new(&ring::digest::SHA256);
    let mut previous_id = 0;
    for member in pending.members {
        if member.segment_id <= previous_id
            || member.row_count <= 0
            || !included.insert(member.segment_id)
        {
            return None;
        }
        previous_id = member.segment_id;
        digest.update(&member.segment_id.to_be_bytes());
        digest.update(&member.row_count.to_be_bytes());
        digest.update(&member.payload_digest);
    }
    (digest.finish().as_ref() == pending.authorization.identity.input_digest)
        .then(|| included.into_iter().collect())
}
