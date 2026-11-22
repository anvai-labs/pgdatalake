//! One operation step for explicitly admitted staged append workers.

use std::{error::Error, fmt};

use crate::catalog::{
    commit::PreparedPublication,
    publication::{AuthorizedPublication, PublicationIdentity, PublicationLookup},
    rest::RestError,
};
use crate::storage::StorageError;
use uuid::Uuid;

pub type LedgerResult<T> = Result<T, Box<dyn Error>>;

pub struct CapturedMember {
    pub segment_id: i64,
    pub row_count: i32,
    pub payload: Vec<u8>,
}

#[derive(PartialEq, Eq)]
pub struct RetainedAttempt {
    pub expected_parent: Option<i64>,
    pub snapshot_id: i64,
    pub request_digest: [u8; 32],
    // A pre-1.6 attempt only retained its digest and is still lookup-only.
    pub request_body: Option<Vec<u8>>,
}

pub enum ClaimState {
    Unsent,
    Reserved(RetainedAttempt),
    Complete { snapshot_id: i64 },
}

impl ClaimState {
    // A first intent can appear during transfer. An intent already observed
    // for an operation must survive the transfer byte-for-byte.
    fn follows_acquisition_of(&self, previous: &Self) -> bool {
        match (previous, self) {
            (Self::Unsent, Self::Unsent | Self::Reserved(_)) => true,
            (Self::Reserved(old), Self::Reserved(current)) => old == current,
            _ => false,
        }
    }
}

/// Immutable provenance from the captured members and durable claim.
/// This is distinct from the mutable publisher owner generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PublicationBindingIdentity {
    pub authority_id: Uuid,
    pub binding_id: Uuid,
    pub incarnation_id: Uuid,
    pub binding_generation: i64,
}

pub struct PublicationClaim {
    pub identity: PublicationIdentity,
    pub binding: Option<PublicationBindingIdentity>,
    pub generation: i64,
    pub state: ClaimState,
}

/// Adapter scoped to one independently admitted immutable target binding.
/// A load observes the claim and attempt in the same database snapshot.
/// Acquire, authorize and finalize use the DB's generation and state fences.
pub trait PublicationLedger {
    fn load(&self, operation: Uuid) -> LedgerResult<PublicationClaim>;
    fn acquire(&self, claim: &PublicationClaim) -> LedgerResult<i64>;
    fn members(&self, claim: &PublicationClaim) -> LedgerResult<Vec<CapturedMember>>;

    /// Success means the standalone send authorization committed and the
    /// database confirmed that the exact request body is locally durable.
    fn authorize(
        &self,
        claim: &PublicationClaim,
        intent: AuthorizedPublication<'_>,
    ) -> LedgerResult<()>;

    fn finalize(
        &self,
        claim: &PublicationClaim,
        intent: AuthorizedPublication<'_>,
    ) -> LedgerResult<usize>;
}

/// The operator supplies this adapter with the claim's approved route and
/// scope. No preparation may submit a catalog request that changes the ref.
pub trait PublicationCatalog {
    fn prepare(
        &self,
        claim: &PublicationClaim,
        members: &[CapturedMember],
    ) -> Result<PreparedPublication<'_>, StorageError>;

    fn lookup(
        &self,
        claim: &PublicationClaim,
        intent: AuthorizedPublication<'_>,
    ) -> Result<PublicationLookup, RestError>;
}

#[derive(Debug)]
pub enum PendingReason {
    RetainedBodyMissing,
    AcceptanceUnproved,
    LookupUnavailable(RestError),
    DispatchUnknown(RestError),
    DispatchConflict(RestError),
}

#[derive(Debug)]
pub enum WorkerOutcome {
    Complete { snapshot_id: i64 },
    Verified { snapshot_id: i64, members: usize },
    Pending(PendingReason),
}

#[derive(Debug)]
pub struct WorkerFailure {
    pub stage: &'static str,
    pub source: Box<dyn Error>,
}

impl fmt::Display for WorkerFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.stage, self.source)
    }
}

impl Error for WorkerFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

fn stage<T, E: Into<Box<dyn Error>>>(
    result: Result<T, E>,
    stage: &'static str,
) -> Result<T, WorkerFailure> {
    result.map_err(|source| WorkerFailure {
        stage,
        source: source.into(),
    })
}

fn members_match(claim: &PublicationClaim, members: &[CapturedMember]) -> bool {
    if members.is_empty()
        || members.len() > 1024
        || members.iter().any(|member| member.row_count <= 0)
        || members
            .windows(2)
            .any(|pair| pair[0].segment_id >= pair[1].segment_id)
    {
        return false;
    }
    let mut digest = ring::digest::Context::new(&ring::digest::SHA256);
    for member in members {
        digest.update(&member.segment_id.to_be_bytes());
        digest.update(&member.row_count.to_be_bytes());
        digest.update(ring::digest::digest(&ring::digest::SHA256, &member.payload).as_ref());
    }
    digest.finish().as_ref() == claim.identity.input_digest
}

fn verify(
    ledger: &impl PublicationLedger,
    claim: &PublicationClaim,
    intent: AuthorizedPublication<'_>,
    evidence: Result<PublicationLookup, RestError>,
) -> Result<WorkerOutcome, WorkerFailure> {
    match evidence {
        Ok(PublicationLookup::Accepted { snapshot_id }) if snapshot_id == intent.snapshot_id => {
            let members = stage(ledger.finalize(claim, intent), "finalization_unconfirmed")?;
            Ok(WorkerOutcome::Verified {
                snapshot_id,
                members,
            })
        }
        Ok(_) => Ok(WorkerOutcome::Pending(PendingReason::AcceptanceUnproved)),
        Err(error) => Ok(WorkerOutcome::Pending(PendingReason::LookupUnavailable(
            error,
        ))),
    }
}

fn load_operation(
    ledger: &impl PublicationLedger,
    operation: Uuid,
) -> Result<PublicationClaim, WorkerFailure> {
    let claim = stage(ledger.load(operation), "load_claim")?;
    if claim.identity.operation_id != operation {
        return stage(
            Err::<_, &str>("ledger supplied another operation"),
            "load_claim",
        );
    }
    Ok(claim)
}

fn acquire_claim(
    ledger: &impl PublicationLedger,
    observed: &PublicationClaim,
) -> Result<PublicationClaim, WorkerFailure> {
    let generation = stage(ledger.acquire(observed), "acquire_claim")?;
    let claim = stage(
        ledger.load(observed.identity.operation_id),
        "load_acquired_claim",
    )?;
    if !claim.state.follows_acquisition_of(&observed.state)
        || generation <= observed.generation
        || claim.generation != generation
        || claim.binding != observed.binding
        || claim.identity.operation_id != observed.identity.operation_id
        || claim.identity.table_uuid != observed.identity.table_uuid
        || claim.identity.input_digest != observed.identity.input_digest
    {
        return stage(
            Err::<_, &str>("claim changed during ownership transfer"),
            "load_acquired_claim",
        );
    }
    Ok(claim)
}

fn verify_reserved(
    ledger: &impl PublicationLedger,
    claim: &PublicationClaim,
    attempt: &RetainedAttempt,
    lookup: impl FnOnce(
        &PublicationClaim,
        AuthorizedPublication<'_>,
    ) -> Result<PublicationLookup, RestError>,
) -> Result<WorkerOutcome, WorkerFailure> {
    let Some(request_body) = &attempt.request_body else {
        return Ok(WorkerOutcome::Pending(PendingReason::RetainedBodyMissing));
    };
    let intent = AuthorizedPublication {
        identity: claim.identity,
        expected_parent: attempt.expected_parent,
        snapshot_id: attempt.snapshot_id,
        request_digest: attempt.request_digest,
        request_body,
    };
    if !intent.matches_retained_request() {
        return Ok(WorkerOutcome::Pending(PendingReason::AcceptanceUnproved));
    }
    verify(ledger, claim, intent, lookup(claim, intent))
}

/// Reconcile a wake scheduled only for verification on the original scope.
/// An unsent claim returns None without acquiring ownership or contacting the
/// catalog. The caller supplies only a lookup operation; an accepted attempt
/// still needs a generation-fenced local finalization.
pub fn reconcile_publication(
    ledger: &impl PublicationLedger,
    operation: Uuid,
    lookup: impl FnOnce(
        &PublicationClaim,
        AuthorizedPublication<'_>,
    ) -> Result<PublicationLookup, RestError>,
) -> Result<Option<WorkerOutcome>, WorkerFailure> {
    let observed = load_operation(ledger, operation)?;
    match observed.state {
        ClaimState::Unsent => return Ok(None),
        ClaimState::Complete { snapshot_id } => {
            return Ok(Some(WorkerOutcome::Complete { snapshot_id }))
        }
        ClaimState::Reserved(_) => {}
    }
    let claim = acquire_claim(ledger, &observed)?;
    let ClaimState::Reserved(ref attempt) = claim.state else {
        return stage(
            Err::<_, &str>("retained attempt unavailable"),
            "load_acquired_claim",
        );
    };
    verify_reserved(ledger, &claim, attempt, lookup).map(Some)
}

/// Drive one existing claim through the durable one-POST boundary.
/// The caller owns scheduling, active-grant admission and the binding
/// supplied to the adapters. Restart by loading the same operation again.
pub fn drive_publication(
    ledger: &impl PublicationLedger,
    catalog: &impl PublicationCatalog,
    operation: Uuid,
) -> Result<WorkerOutcome, WorkerFailure> {
    let observed = load_operation(ledger, operation)?;
    if let ClaimState::Complete { snapshot_id } = observed.state {
        return Ok(WorkerOutcome::Complete { snapshot_id });
    }
    let claim = acquire_claim(ledger, &observed)?;
    if let ClaimState::Reserved(ref attempt) = claim.state {
        return verify_reserved(ledger, &claim, attempt, |claim, intent| {
            catalog.lookup(claim, intent)
        });
    }

    let members = stage(ledger.members(&claim), "load_members")?;
    if !members_match(&claim, &members) {
        return stage(
            Err::<_, &str>("captured members do not match the claimed input digest"),
            "load_members",
        );
    }
    let prepared = stage(catalog.prepare(&claim, &members), "prepare_unsent")?;
    let body = prepared.request_body().to_vec();
    let intent = AuthorizedPublication {
        identity: claim.identity,
        expected_parent: prepared.expected_parent(),
        snapshot_id: prepared.snapshot_id(),
        request_digest: prepared.request_digest(),
        request_body: &body,
    };
    if !intent.matches_retained_request() {
        return stage(
            Err::<_, &str>("prepared request does not match the claim"),
            "prepare_unsent",
        );
    }
    stage(
        ledger.authorize(&claim, intent),
        "authorization_unconfirmed",
    )?;
    match prepared.submit() {
        Ok(_) => verify(ledger, &claim, intent, catalog.lookup(&claim, intent)),
        Err(error @ RestError::Conflict(_)) => Ok(WorkerOutcome::Pending(
            PendingReason::DispatchConflict(error),
        )),
        Err(error) => Ok(WorkerOutcome::Pending(PendingReason::DispatchUnknown(
            error,
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attempt(bytes: &[u8]) -> ClaimState {
        ClaimState::Reserved(RetainedAttempt {
            expected_parent: Some(100),
            snapshot_id: 101,
            request_digest: crate::catalog::publication::sha256(bytes),
            request_body: Some(bytes.to_vec()),
        })
    }

    #[test]
    fn takeover_preserves_an_observed_attempt() {
        let observed = attempt(b"first");
        assert!(attempt(b"first").follows_acquisition_of(&observed));
        assert!(observed.follows_acquisition_of(&ClaimState::Unsent));
        assert!(!ClaimState::Unsent.follows_acquisition_of(&observed));
        assert!(!attempt(b"replacement").follows_acquisition_of(&observed));
    }
}
