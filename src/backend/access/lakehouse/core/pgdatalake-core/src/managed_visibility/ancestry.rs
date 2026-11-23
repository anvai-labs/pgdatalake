//! Certify append ancestry from one scoped Iceberg metadata observation.

use std::collections::HashMap;

use crate::catalog::metadata::{FormatVersion, TableMetadata};
use crate::catalog::publication::scoped_manifest_location;
use uuid::Uuid;

use super::{
    select_compatible_pin, ManagedPinCandidate, ManagedReadCut, ManagedReadError, ManagedReadScope,
    VisibleSegment,
};

pub(super) const MAX_MANAGED_SNAPSHOTS: usize = 4096;
pub(super) const MAX_MANAGED_ANCESTRY_PINS: usize = 512;

/// The caller gets the route identity from the independently approved
/// enrollment used to load this metadata. Catalog-supplied config and
/// snapshot summaries cannot supply authority or local incarnation.
#[derive(Debug, Clone, Copy)]
pub struct ManagedCatalogObservation<'a> {
    pub authority_id: Uuid,
    pub incarnation_id: Uuid,
    pub ref_name: &'a str,
    pub metadata: &'a TableMetadata,
}

/// The current branch head and its retained parents, through the enrolled pin.
/// This validates metadata ancestry. Ledger membership and payload lifetimes
/// require independent evidence before a scan can produce rows.
#[derive(Debug)]
pub struct ManagedAncestry<'a> {
    scope: &'a ManagedReadScope,
    pins: Vec<i64>,
}

impl<'a> ManagedAncestry<'a> {
    /// Certify one fresh response from the approved Iceberg v2 `main` route.
    /// The positive anchor is an operator-enrolled, retained snapshot.
    ///
    /// Callers must bound the catalog response before decoding it and
    /// protect the retained pins for the duration of a subsequent scan.
    pub fn certify(
        scope: &'a ManagedReadScope,
        anchor: i64,
        observation: ManagedCatalogObservation<'_>,
    ) -> Result<Self, ManagedReadError> {
        let metadata = observation.metadata;
        if !scope.is_valid() || scope.ref_name != "main" || anchor <= 0 {
            return Err(ManagedReadError::InvalidProof);
        }
        if observation.authority_id != scope.authority_id
            || observation.incarnation_id != scope.incarnation_id
            || observation.ref_name != scope.ref_name
            || metadata.table_uuid != scope.table_uuid
            || metadata.location.trim_end_matches('/') != scope.storage_root.trim_end_matches('/')
        {
            return Err(ManagedReadError::ScopeMismatch);
        }
        if metadata.format_version != FormatVersion::V2 {
            return Err(ManagedReadError::InvalidProof);
        }
        let main = metadata
            .refs
            .get(&scope.ref_name)
            .ok_or(ManagedReadError::AncestryUnavailable)?;
        if main.kind != "branch"
            || main.snapshot_id <= 0
            || metadata.current_snapshot_id != Some(main.snapshot_id)
        {
            return Err(ManagedReadError::InvalidProof);
        }

        if metadata.snapshots.len() > MAX_MANAGED_SNAPSHOTS {
            return Err(ManagedReadError::AncestryLimitExceeded);
        }
        let mut by_id = HashMap::with_capacity(metadata.snapshots.len());
        for snapshot in &metadata.snapshots {
            if snapshot.snapshot_id <= 0 || by_id.insert(snapshot.snapshot_id, snapshot).is_some() {
                return Err(ManagedReadError::InvalidProof);
            }
        }

        let mut pins = Vec::new();
        let mut pin = main.snapshot_id;
        let mut child_sequence = None;
        loop {
            if pins.len() == MAX_MANAGED_ANCESTRY_PINS {
                return Err(ManagedReadError::AncestryLimitExceeded);
            }
            let snapshot = by_id
                .get(&pin)
                .ok_or(ManagedReadError::AncestryUnavailable)?;
            let sequence = snapshot
                .sequence_number
                .filter(|&sequence| sequence >= 0)
                .ok_or(ManagedReadError::InvalidProof)?;
            if child_sequence.is_some_and(|child| sequence >= child)
                || snapshot
                    .parent_snapshot_id
                    .is_some_and(|id| id <= 0 || id == pin)
                || !scoped_manifest_location(&scope.storage_root, &snapshot.manifest_list)
                || (pin != anchor
                    && snapshot.summary.get("operation").map(String::as_str) != Some("append"))
            {
                return Err(ManagedReadError::InvalidProof);
            }

            pins.push(pin);
            if pin == anchor {
                return Ok(Self { scope, pins });
            }
            pin = snapshot
                .parent_snapshot_id
                .ok_or(ManagedReadError::AncestryUnavailable)?;
            child_sequence = Some(sequence);
        }
    }

    /// Exact pins in parent-link order, newest first.
    #[must_use]
    pub fn pins(&self) -> &[i64] {
        &self.pins
    }

    /// Apply independently qualified membership to an ordered subset of the
    /// certified chain. Unlisted pins carry no usable membership evidence.
    pub fn select(
        &self,
        candidates: &[ManagedPinCandidate<'_>],
        visible: &[VisibleSegment],
    ) -> Result<ManagedReadCut, ManagedReadError> {
        let mut pins = self.pins.iter();
        for candidate in candidates {
            if candidate.scope != self.scope {
                return Err(ManagedReadError::ScopeMismatch);
            }
            if !pins.any(|&pin| pin == candidate.pin) {
                return Err(ManagedReadError::InvalidProof);
            }
        }
        select_compatible_pin(self.scope, candidates, visible)
    }
}
