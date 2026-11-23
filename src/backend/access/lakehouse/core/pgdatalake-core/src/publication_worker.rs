//! One operation step for explicitly admitted staged append workers.

use std::{
    error::Error,
    fmt,
    sync::atomic::{AtomicBool, Ordering},
};

use crate::catalog::{
    commit::PreparedPublication,
    publication::{AuthorizedPublication, PublicationIdentity, PublicationLookup},
    rest::RestError,
};
use crate::publication_transport::ScopedPublisher;
use crate::storage::StorageError;
use uuid::Uuid;

pub type LedgerResult<T> = Result<T, Box<dyn Error>>;
pub(crate) const MAX_PUBLICATION_MEMBERS: usize = 1024;

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
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize)]
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

/// The mode reported by the ledger's bounded, read-only discovery query.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationDispatchMode {
    Send,
    Lookup,
    Hold,
}

/// One discovery row. A hint may be stale and supplies no send authorization.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize)]
pub struct PublicationDispatchHint {
    pub operation_id: Uuid,
    pub binding_id: Uuid,
    pub owner_generation: i64,
    pub mode: PublicationDispatchMode,
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
    ) -> Result<PreparedPublication, StorageError>;

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
        || members.len() > MAX_PUBLICATION_MEMBERS
        || members
            .iter()
            .any(|member| member.segment_id <= 0 || member.row_count <= 0)
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

fn load_dispatch_hint(
    ledger: &impl PublicationLedger,
    hint: PublicationDispatchHint,
    mode: PublicationDispatchMode,
) -> Result<Option<PublicationClaim>, WorkerFailure> {
    if hint.mode != mode {
        return Ok(None);
    }
    let claim = load_operation(ledger, hint.operation_id)?;
    if !hint_matches_claim(hint, &claim) {
        return Ok(None);
    }
    Ok(Some(claim))
}

fn hint_matches_claim(hint: PublicationDispatchHint, claim: &PublicationClaim) -> bool {
    claim.identity.operation_id == hint.operation_id
        && claim.generation == hint.owner_generation
        && claim
            .binding
            .is_some_and(|binding| binding.binding_id == hint.binding_id)
}

/// An owned process-local proposal. A shared view exposes the exact bytes for
/// WAL authorization. The proposal exposes no submit operation at this boundary.
pub struct PreparedManagedSend {
    identity: PublicationIdentity,
    owner_generation: i64,
    binding: Option<PublicationBindingIdentity>,
    request: PreparedPublication,
}

impl PreparedManagedSend {
    #[must_use]
    pub fn identity(&self) -> PublicationIdentity {
        self.identity
    }

    #[must_use]
    pub fn owner_generation(&self) -> i64 {
        self.owner_generation
    }

    #[must_use]
    pub fn binding(&self) -> Option<PublicationBindingIdentity> {
        self.binding
    }

    #[must_use]
    pub fn request(&self) -> &PreparedPublication {
        &self.request
    }

    fn intent(&self) -> AuthorizedPublication<'_> {
        AuthorizedPublication {
            identity: self.identity,
            expected_parent: self.request.expected_parent(),
            snapshot_id: self.request.snapshot_id(),
            request_digest: self.request.request_digest(),
            request_body: self.request.request_body(),
        }
    }
}

/// Prepare one acquired generation using its exact job and member observations.
/// The caller releases PostgreSQL locks before this call and later revalidates
/// the unchanged claim in the standalone WAL authorization transaction. This
/// call may read the catalog and create immutable objects. Cancellation before
/// or after transport preparation discards any local proposal. No commit POST
/// occurs and a cancellation never promotes prepared objects into a snapshot.
pub fn prepare_publication_send(
    catalog: &ScopedPublisher,
    admitted: PublicationDispatchHint,
    claim: &PublicationClaim,
    members: &[CapturedMember],
    cancelled: &AtomicBool,
) -> Result<PreparedManagedSend, WorkerFailure> {
    if admitted.mode != PublicationDispatchMode::Send || !hint_matches_claim(admitted, claim) {
        return stage(
            Err::<_, &str>("publication prepare observation does not match the acquired owner"),
            "prepare_unsent",
        );
    }
    stage(catalog.check_member_budget(members), "load_members")?;
    prepare_unsent(catalog, claim, members, || {
        cancelled.load(Ordering::Relaxed)
    })
}

/// Try an admitted send wake. If its hint is stale or an intent is retained
/// before or during acquisition, return to discovery for a fresh lookup wake.
/// As with direct dispatch, the ledger and catalog enforce current admission.
pub fn dispatch_publication_send(
    ledger: &impl PublicationLedger,
    catalog: &impl PublicationCatalog,
    hint: PublicationDispatchHint,
) -> Result<Option<WorkerOutcome>, WorkerFailure> {
    let Some(observed) = load_dispatch_hint(ledger, hint, PublicationDispatchMode::Send)? else {
        return Ok(None);
    };
    if !matches!(observed.state, ClaimState::Unsent) {
        return Ok(None);
    }
    let claim = acquire_claim(ledger, &observed)?;
    if !matches!(claim.state, ClaimState::Unsent) {
        return Ok(None);
    }
    drive_unsent(ledger, catalog, &claim).map(Some)
}

/// Try a discovered lookup wake using only its lookup capability. If the
/// operation has moved since discovery or is unsent, return to discovery.
pub fn dispatch_publication_lookup(
    ledger: &impl PublicationLedger,
    hint: PublicationDispatchHint,
    lookup: impl FnOnce(
        &PublicationClaim,
        AuthorizedPublication<'_>,
    ) -> Result<PublicationLookup, RestError>,
) -> Result<Option<WorkerOutcome>, WorkerFailure> {
    let Some(observed) = load_dispatch_hint(ledger, hint, PublicationDispatchMode::Lookup)? else {
        return Ok(None);
    };
    if !matches!(observed.state, ClaimState::Reserved(_)) {
        return Ok(None);
    }
    reconcile_reserved(ledger, &observed, lookup).map(Some)
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
    reconcile_reserved(ledger, &observed, lookup).map(Some)
}

fn reconcile_reserved(
    ledger: &impl PublicationLedger,
    observed: &PublicationClaim,
    lookup: impl FnOnce(
        &PublicationClaim,
        AuthorizedPublication<'_>,
    ) -> Result<PublicationLookup, RestError>,
) -> Result<WorkerOutcome, WorkerFailure> {
    let claim = acquire_claim(ledger, observed)?;
    let ClaimState::Reserved(ref attempt) = claim.state else {
        return stage(
            Err::<_, &str>("retained attempt unavailable"),
            "load_acquired_claim",
        );
    };
    verify_reserved(ledger, &claim, attempt, lookup)
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

    drive_unsent(ledger, catalog, &claim)
}

fn drive_unsent(
    ledger: &impl PublicationLedger,
    catalog: &impl PublicationCatalog,
    claim: &PublicationClaim,
) -> Result<WorkerOutcome, WorkerFailure> {
    let members = stage(ledger.members(claim), "load_members")?;
    let prepared = prepare_unsent(catalog, claim, &members, || false)?;
    let intent = prepared.intent();
    stage(ledger.authorize(claim, intent), "authorization_unconfirmed")?;
    let body = intent.request_body.to_vec();
    let intent = AuthorizedPublication {
        request_body: &body,
        ..intent
    };
    match prepared.request.submit() {
        Ok(_) => verify(ledger, claim, intent, catalog.lookup(claim, intent)),
        Err(error @ RestError::Conflict(_)) => Ok(WorkerOutcome::Pending(
            PendingReason::DispatchConflict(error),
        )),
        Err(error) => Ok(WorkerOutcome::Pending(PendingReason::DispatchUnknown(
            error,
        ))),
    }
}

fn prepare_unsent(
    catalog: &impl PublicationCatalog,
    claim: &PublicationClaim,
    members: &[CapturedMember],
    cancelled: impl Fn() -> bool,
) -> Result<PreparedManagedSend, WorkerFailure> {
    if claim.generation <= 0
        || claim.identity.operation_id.is_nil()
        || !matches!(claim.state, ClaimState::Unsent)
    {
        return stage(
            Err::<_, &str>("publication preparation requires an unsent claim with a valid owner"),
            "prepare_unsent",
        );
    }
    if cancelled() {
        return stage(
            Err::<_, &str>("publication preparation cancelled"),
            "prepare_unsent",
        );
    }
    if !members_match(claim, members) {
        return stage(
            Err::<_, &str>("captured members do not match the claimed input digest"),
            "load_members",
        );
    }
    let request = stage(catalog.prepare(claim, members), "prepare_unsent")?;
    if cancelled() {
        return stage(
            Err::<_, &str>("publication preparation cancelled"),
            "prepare_unsent",
        );
    }
    let prepared = PreparedManagedSend {
        identity: claim.identity,
        owner_generation: claim.generation,
        binding: claim.binding,
        request,
    };
    if !prepared.intent().matches_retained_request() {
        return stage(
            Err::<_, &str>("prepared request does not match the claim"),
            "prepare_unsent",
        );
    }
    Ok(prepared)
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, collections::VecDeque};

    use super::*;

    fn attempt(bytes: &[u8]) -> ClaimState {
        ClaimState::Reserved(RetainedAttempt {
            expected_parent: Some(100),
            snapshot_id: 101,
            request_digest: crate::catalog::publication::sha256(bytes),
            request_body: Some(bytes.to_vec()),
        })
    }

    struct DispatchLedger {
        loads: RefCell<VecDeque<PublicationClaim>>,
        calls: RefCell<Vec<&'static str>>,
    }

    impl DispatchLedger {
        fn new(claims: impl IntoIterator<Item = PublicationClaim>) -> Self {
            Self {
                loads: RefCell::new(claims.into_iter().collect()),
                calls: RefCell::new(Vec::new()),
            }
        }
    }

    impl PublicationLedger for DispatchLedger {
        fn load(&self, _operation: Uuid) -> LedgerResult<PublicationClaim> {
            self.calls.borrow_mut().push("load");
            Ok(self
                .loads
                .borrow_mut()
                .pop_front()
                .expect("unexpected load"))
        }

        fn acquire(&self, claim: &PublicationClaim) -> LedgerResult<i64> {
            self.calls.borrow_mut().push("acquire");
            Ok(claim.generation + 1)
        }

        fn members(&self, _claim: &PublicationClaim) -> LedgerResult<Vec<CapturedMember>> {
            panic!("dispatch must not load source members")
        }

        fn authorize(
            &self,
            _claim: &PublicationClaim,
            _intent: AuthorizedPublication<'_>,
        ) -> LedgerResult<()> {
            panic!("dispatch must not authorize")
        }

        fn finalize(
            &self,
            _claim: &PublicationClaim,
            _intent: AuthorizedPublication<'_>,
        ) -> LedgerResult<usize> {
            panic!("dispatch must not finalize without verification")
        }
    }

    struct NoCatalog;

    impl PublicationCatalog for NoCatalog {
        fn prepare(
            &self,
            _claim: &PublicationClaim,
            _members: &[CapturedMember],
        ) -> Result<PreparedPublication, StorageError> {
            panic!("dispatch must not prepare")
        }

        fn lookup(
            &self,
            _claim: &PublicationClaim,
            _intent: AuthorizedPublication<'_>,
        ) -> Result<PublicationLookup, RestError> {
            panic!("dispatch must not issue a lookup on a send wake for a newly retained intent")
        }
    }

    fn discovered(mode: PublicationDispatchMode) -> PublicationDispatchHint {
        PublicationDispatchHint {
            operation_id: Uuid::from_u128(1),
            binding_id: Uuid::from_u128(2),
            owner_generation: 3,
            mode,
        }
    }

    fn hinted_claim(hint: PublicationDispatchHint, state: ClaimState) -> PublicationClaim {
        PublicationClaim {
            identity: PublicationIdentity {
                operation_id: hint.operation_id,
                table_uuid: Uuid::from_u128(3),
                input_digest: [0; 32],
            },
            binding: Some(PublicationBindingIdentity {
                authority_id: Uuid::from_u128(4),
                binding_id: hint.binding_id,
                incarnation_id: Uuid::from_u128(5),
                binding_generation: 1,
            }),
            generation: hint.owner_generation,
            state,
        }
    }

    #[test]
    fn held_or_wrong_duty_never_opens_ledger() {
        let ledger = DispatchLedger::new([]);
        let hold = discovered(PublicationDispatchMode::Hold);
        assert!(dispatch_publication_send(&ledger, &NoCatalog, hold)
            .unwrap()
            .is_none());
        assert!(
            dispatch_publication_lookup(&ledger, hold, |_, _| unreachable!())
                .unwrap()
                .is_none()
        );
        let lookup = discovered(PublicationDispatchMode::Lookup);
        assert!(dispatch_publication_send(&ledger, &NoCatalog, lookup)
            .unwrap()
            .is_none());
        let send = discovered(PublicationDispatchMode::Send);
        assert!(
            dispatch_publication_lookup(&ledger, send, |_, _| unreachable!())
                .unwrap()
                .is_none()
        );
        assert!(ledger.calls.borrow().is_empty());
    }

    #[test]
    fn stale_hints_and_changed_modes_never_acquire() {
        let send = discovered(PublicationDispatchMode::Send);
        let mut wrong_generation = hinted_claim(send, ClaimState::Unsent);
        wrong_generation.generation += 1;
        let mut wrong_binding = hinted_claim(send, ClaimState::Unsent);
        wrong_binding.binding.as_mut().unwrap().binding_id = Uuid::from_u128(6);
        let already_reserved = hinted_claim(send, attempt(b"already reserved"));
        let ledger = DispatchLedger::new([wrong_generation, wrong_binding, already_reserved]);
        for _ in 0..3 {
            assert!(dispatch_publication_send(&ledger, &NoCatalog, send)
                .unwrap()
                .is_none());
        }
        let lookup = discovered(PublicationDispatchMode::Lookup);
        let lookup_ledger = DispatchLedger::new([hinted_claim(lookup, ClaimState::Unsent)]);
        assert!(
            dispatch_publication_lookup(&lookup_ledger, lookup, |_, _| unreachable!())
                .unwrap()
                .is_none()
        );
        assert_eq!(*lookup_ledger.calls.borrow(), ["load"]);
        assert_eq!(*ledger.calls.borrow(), ["load", "load", "load"]);
    }

    #[test]
    fn send_wake_yields_if_attempt_appears_during_takeover() {
        let wake = discovered(PublicationDispatchMode::Send);
        let mut acquired = hinted_claim(wake, attempt(b"reserved during takeover"));
        acquired.generation += 1;
        let ledger = DispatchLedger::new([hinted_claim(wake, ClaimState::Unsent), acquired]);
        assert!(dispatch_publication_send(&ledger, &NoCatalog, wake)
            .unwrap()
            .is_none());
        assert_eq!(*ledger.calls.borrow(), ["load", "acquire", "load"]);
    }

    #[test]
    fn lookup_wake_does_not_accept_a_replaced_attempt() {
        let wake = discovered(PublicationDispatchMode::Lookup);
        let mut acquired = hinted_claim(wake, attempt(b"replacement"));
        acquired.generation += 1;
        let ledger = DispatchLedger::new([hinted_claim(wake, attempt(b"original")), acquired]);
        let error = dispatch_publication_lookup(&ledger, wake, |_, _| unreachable!()).unwrap_err();
        assert_eq!(error.stage, "load_acquired_claim");
        assert_eq!(*ledger.calls.borrow(), ["load", "acquire", "load"]);
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
