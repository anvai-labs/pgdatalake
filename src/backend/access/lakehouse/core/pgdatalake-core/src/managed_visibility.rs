//! Select the published snapshot and local overlay for one managed append read.

use std::collections::BTreeSet;

use serde::Deserialize;
use uuid::Uuid;

mod ancestry;
pub(crate) mod pending;

pub use ancestry::{ManagedAncestry, ManagedCatalogObservation};
pub use pending::{PendingHeadAttempt, SealedPendingMember};

/// The enrolled remote ref and local capture lineage. A lineage spans
/// capture binding generations while its incarnation and remote scope agree.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ManagedReadScope {
    pub authority_id: Uuid,
    pub table_uuid: Uuid,
    pub incarnation_id: Uuid,
    pub ref_name: String,
    pub storage_root: String,
}

impl ManagedReadScope {
    fn is_valid(&self) -> bool {
        !self.authority_id.is_nil()
            && !self.table_uuid.is_nil()
            && !self.incarnation_id.is_nil()
            && !self.ref_name.is_empty()
            && !self.storage_root.is_empty()
    }
}

/// Catalog and ledger evidence for one candidate on the certified ancestry.
/// A complete set includes members from all retained capture generations,
/// including PostgreSQL transactions invisible to the reader.
#[derive(Debug, Clone, Copy)]
pub enum PinMembership<'a> {
    Complete(&'a [i64]),
    Unknown,
    Expired,
}

#[derive(Debug, Clone, Copy)]
pub struct ManagedPinCandidate<'a> {
    pub scope: &'a ManagedReadScope,
    pub pin: i64,
    pub membership: PinMembership<'a>,
}

/// A segment visible to the statement's PostgreSQL snapshot. Keep visible
/// IDs even when their payload was reclaimed, so an older pin cannot omit them.
#[derive(Debug, Clone, Copy)]
pub struct VisibleSegment {
    pub segment_id: i64,
    pub payload_retained: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ManagedReadCut {
    pub pin: i64,
    pub overlay_segment_ids: Vec<i64>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ManagedReadError {
    ScopeMismatch,
    InvalidProof,
    AncestryUnavailable,
    AncestryLimitExceeded,
    NoCompatiblePin,
}

/// Check exact segment identities and monotone append membership in a chain
/// already certified by `ManagedAncestry`. The caller holds the chosen pin
/// and local payloads through execution and accounts for own commands.
pub(crate) fn select_compatible_pin(
    scope: &ManagedReadScope,
    candidates: &[ManagedPinCandidate<'_>],
    visible: &[VisibleSegment],
) -> Result<ManagedReadCut, ManagedReadError> {
    if !scope.is_valid() {
        return Err(ManagedReadError::InvalidProof);
    }

    let mut visible_ids = BTreeSet::new();
    let mut missing_payloads = BTreeSet::new();
    for segment in visible {
        if segment.segment_id <= 0 || !visible_ids.insert(segment.segment_id) {
            return Err(ManagedReadError::InvalidProof);
        }
        if !segment.payload_retained {
            missing_payloads.insert(segment.segment_id);
        }
    }

    let mut seen_pins = BTreeSet::new();
    let mut newer_membership: Option<BTreeSet<i64>> = None;
    let mut selected = None;
    for candidate in candidates {
        if candidate.scope != scope {
            return Err(ManagedReadError::ScopeMismatch);
        }
        if candidate.pin <= 0 || !seen_pins.insert(candidate.pin) {
            return Err(ManagedReadError::InvalidProof);
        }
        let PinMembership::Complete(members) = candidate.membership else {
            continue;
        };
        let mut included = BTreeSet::new();
        for &id in members {
            if id <= 0 || !included.insert(id) {
                return Err(ManagedReadError::InvalidProof);
            }
        }
        if newer_membership
            .as_ref()
            .is_some_and(|newer| !included.is_subset(newer))
        {
            return Err(ManagedReadError::InvalidProof);
        }
        if selected.is_none()
            && included.is_subset(&visible_ids)
            && missing_payloads.is_subset(&included)
        {
            selected = Some(ManagedReadCut {
                pin: candidate.pin,
                overlay_segment_ids: visible_ids.difference(&included).copied().collect(),
            });
        }
        newer_membership = Some(included);
    }

    selected.ok_or(ManagedReadError::NoCompatiblePin)
}

#[cfg(test)]
mod tests;
