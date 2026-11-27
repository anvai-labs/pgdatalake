//! A single opaque scoped observation for the native managed-reader join.

use std::ffi::c_char;
use std::ptr;

use uuid::Uuid;

use super::{
    borrow_string, return_read_handle, PgDatalakeReadBinding, PgDatalakeReadStatus, ScanSession,
};
use crate::managed_visibility::{
    ManagedAncestry, ManagedCatalogObservation, ManagedPinCandidate, ManagedReadError,
    ManagedReadScope, PinMembership,
};
use crate::read_binding::{BoundReadObservation, ReadBinding, ReadError};
use crate::session::S3ClientOptions;

/// One operator-approved route, table incarnation and enrollment anchor.
/// The native caller obtains these fields after PostgreSQL permission checks.
#[repr(C)]
pub struct PgDatalakeManagedReadRequest {
    pub binding: PgDatalakeReadBinding,
    pub catalog_endpoint: *const c_char,
    pub catalog_token: *const c_char,
    pub namespace: *const c_char,
    pub table: *const c_char,
    pub s3_endpoint: *const c_char,
    pub s3_access_key: *const c_char,
    pub s3_secret_key: *const c_char,
    pub authority_id: [u8; 16],
    pub incarnation_id: [u8; 16],
    pub anchor_snapshot_id: i64,
    pub enrolled_schema_id: i64,
}

/// Owns one bounded observation and its enrollment for the current statement.
pub struct ManagedReadObservation {
    observed: BoundReadObservation,
    scope: ManagedReadScope,
    anchor_snapshot_id: i64,
    enrolled_schema_id: i64,
    pins: Vec<i64>,
}

impl ManagedReadObservation {
    fn certify_ancestry(&self) -> Result<ManagedAncestry<'_>, ReadError> {
        ManagedAncestry::certify(
            &self.scope,
            self.anchor_snapshot_id,
            ManagedCatalogObservation {
                authority_id: self.scope.authority_id,
                incarnation_id: self.scope.incarnation_id,
                ref_name: &self.scope.ref_name,
                metadata: self.observed.metadata(),
            },
        )
        .map_err(|error| {
            let status = match error {
                ManagedReadError::ScopeMismatch => PgDatalakeReadStatus::ReadChangedBinding,
                ManagedReadError::AncestryUnavailable => PgDatalakeReadStatus::ReadPinUnavailable,
                _ => PgDatalakeReadStatus::ReadUnsupported,
            };
            ReadError(status, format!("managed read ancestry refused: {error:?}"))
        })
    }

    fn plan_exact(self, pin: i64) -> Result<ScanSession, ReadError> {
        if !self.pins.contains(&pin) {
            return Err(ReadError(
                PgDatalakeReadStatus::ReadPinUnavailable,
                "requested managed pin is outside the certified ancestry".into(),
            ));
        }

        // The current planner interprets files using the current schema.
        // A certified managed cut needs explicit proof that this equals both
        // the selected snapshot schema and the enrolled Iceberg schema.
        let metadata = self.observed.metadata();
        let expected = self.enrolled_schema_id;
        if metadata.current_schema_id != expected
            || metadata
                .snapshot(pin)
                .and_then(|snapshot| snapshot.schema_id)
                != Some(expected)
            || metadata
                .schemas
                .iter()
                .filter(|schema| schema.schema_id == expected)
                .count()
                != 1
        {
            return Err(ReadError::unsupported(
                "managed read cannot prove the selected pin schema",
            ));
        }

        let bound = self.observed.plan_snapshot(pin, None)?;
        ScanSession::from_read_plan(bound.plan, None, bound.s3, &bound.schema, Some(bound.root))
    }
}

unsafe fn observe(
    request: *const PgDatalakeManagedReadRequest,
) -> Result<ManagedReadObservation, ReadError> {
    let request = request
        .as_ref()
        .ok_or_else(|| ReadError::unsupported("managed read requires an enrolled route"))?;
    let authority_id = Uuid::from_bytes(request.authority_id);
    let incarnation_id = Uuid::from_bytes(request.incarnation_id);
    if authority_id.is_nil()
        || incarnation_id.is_nil()
        || request.anchor_snapshot_id <= 0
        || request.enrolled_schema_id < 0
        || request.binding.catalog_prefix.is_null()
    {
        return Err(ReadError::unsupported(
            "managed read enrollment is incomplete",
        ));
    }

    let binding = ReadBinding::new(
        &borrow_string(request.binding.expected_table_uuid),
        borrow_string(request.binding.storage_root),
        borrow_string(request.binding.catalog_prefix),
        borrow_string(request.binding.s3_region),
        &borrow_string(request.catalog_endpoint),
        borrow_string(request.catalog_token),
    )?;
    let namespace = borrow_string(request.namespace);
    let namespace: Vec<_> = namespace.split('.').collect();
    let observed = binding.observe(
        &namespace,
        &borrow_string(request.table),
        S3ClientOptions {
            endpoint: Some(borrow_string(request.s3_endpoint)).filter(|value| !value.is_empty()),
            access_key_id: Some(borrow_string(request.s3_access_key))
                .filter(|value| !value.is_empty()),
            secret_access_key: Some(borrow_string(request.s3_secret_key))
                .filter(|value| !value.is_empty()),
            ..Default::default()
        },
    )?;
    let scope = ManagedReadScope {
        authority_id,
        table_uuid: observed.metadata().table_uuid,
        incarnation_id,
        ref_name: "main".into(),
        storage_root: observed.metadata().location.clone(),
    };
    let mut owner = ManagedReadObservation {
        observed,
        scope,
        anchor_snapshot_id: request.anchor_snapshot_id,
        enrolled_schema_id: request.enrolled_schema_id,
        pins: Vec::new(),
    };
    owner.pins = owner.certify_ancestry()?.pins().to_vec();
    Ok(owner)
}

fn runtime_error() -> ReadError {
    ReadError(
        PgDatalakeReadStatus::ReadUnavailable,
        "managed read observation failed inside the data-plane runtime".into(),
    )
}

/// Open one approved, bounded metadata observation and certify its main ancestry.
/// The handle is process-local and bound to this statement's enrollment.
///
/// # Safety
/// Non-null request and its non-null string pointers must remain readable for
/// the call. Nullable output pointers require their declared writable storage.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_managed_read_observe(
    request: *const PgDatalakeManagedReadRequest,
    status_out: *mut PgDatalakeReadStatus,
    err_buf: *mut c_char,
    err_len: usize,
) -> *mut ManagedReadObservation {
    let result =
        std::panic::catch_unwind(|| observe(request)).unwrap_or_else(|_| Err(runtime_error()));
    return_read_handle(result, status_out, err_buf, err_len)
}

/// Count certified pins from newest to the enrolled anchor.
///
/// # Safety
/// A non-null observation must be live in this process for the call.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_managed_read_pin_count(
    observation: *const ManagedReadObservation,
) -> usize {
    observation.as_ref().map_or(0, |value| value.pins.len())
}

/// Return one exact positive snapshot ID, or zero when the index is unavailable.
///
/// # Safety
/// A non-null observation must be live in this process for the call.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_managed_read_pin_at(
    observation: *const ManagedReadObservation,
    index: usize,
) -> i64 {
    observation
        .as_ref()
        .and_then(|value| value.pins.get(index).copied())
        .unwrap_or(0)
}

/// Consume the observation and plan its exact, previously selected pin.
/// C supplies the pin after proving membership and payload visibility under
/// the original statement snapshot. Planning reuses the observed metadata.
///
/// # Safety
/// A non-null owner is a writable slot with the sole live process-local
/// observation or NULL. The function clears the slot even on failure.
/// Nullable output pointers require their declared writable storage.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_managed_read_plan_exact(
    owner: *mut *mut ManagedReadObservation,
    pin: i64,
    status_out: *mut PgDatalakeReadStatus,
    err_buf: *mut c_char,
    err_len: usize,
) -> *mut ScanSession {
    let result = std::panic::catch_unwind(|| {
        let observation = take_owner(owner)
            .ok_or_else(|| ReadError::unsupported("no managed read observation is available"))?;
        observation.plan_exact(pin)
    })
    .unwrap_or_else(|_| Err(runtime_error()));
    return_read_handle(result, status_out, err_buf, err_len)
}

unsafe fn take_owner(
    owner: *mut *mut ManagedReadObservation,
) -> Option<Box<ManagedReadObservation>> {
    let owner = owner.as_mut()?;
    let value = std::mem::replace(owner, ptr::null_mut());
    if value.is_null() {
        return None;
    }
    Some(Box::from_raw(value))
}

/// Release the observation and clear its slot without planning.
///
/// # Safety
/// A non-null owner is a writable slot with the sole live process-local
/// observation or NULL.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_managed_read_observation_drop(
    owner: *mut *mut ManagedReadObservation,
) {
    drop(take_owner(owner));
}

/// One sealed pending member read from the fresh committed ledger evidence.
#[repr(C)]
pub struct PgDatalakeManagedPendingMember {
    pub segment_id: i64,
    pub row_count: i32,
    pub payload_digest: [u8; 32],
}

/// Borrowed pending-head proof inputs. All pointers are caller-owned and
/// readable for the call; buffers never contain credentials.
#[repr(C)]
pub struct PgDatalakeManagedPendingProof {
    pub parent_pin: i64,
    pub parent_members: *const i64,
    pub parent_member_count: usize,
    pub schema_id: i64,
    pub spec_id: i32,
    pub operation_id: [u8; 16],
    pub input_digest: [u8; 32],
    pub expected_parent: i64,
    pub snapshot_id: i64,
    pub request_digest: [u8; 32],
    pub request_body: *const u8,
    pub request_body_len: usize,
    pub members: *const PgDatalakeManagedPendingMember,
    pub member_count: usize,
    pub s3_endpoint: *const c_char,
    pub s3_access_key: *const c_char,
    pub s3_secret_key: *const c_char,
    pub s3_region: *const c_char,
}

/// One certified candidate on the ancestry: membership is incremental at
/// this pin (0 unknown, 1 complete, 2 expired).
#[repr(C)]
pub struct PgDatalakeManagedCutCandidate {
    pub pin: i64,
    pub membership: u32,
    pub members: *const i64,
    pub member_count: usize,
}

impl ManagedReadObservation {
    /// Prove one pending head using THIS observation's own certified
    /// ancestry and metadata (the later same-handle proof the ABI allows).
    /// Returns the cumulative proven member list, empty when the lookup is
    /// unresolved — never an error for an unproved head.
    fn prove_pending(&self, proof: &PgDatalakeManagedPendingProof) -> Result<Vec<i64>, ReadError> {
        use crate::catalog::publication::AuthorizedPublication;
        use crate::catalog::publication::PublicationIdentity;
        use crate::managed_visibility::pending::{PendingHeadAttempt, SealedPendingMember};
        use crate::managed_visibility::{ManagedPinCandidate, PinMembership};

        let ancestry = self.certify_ancestry()?;
        let parent_members =
            unsafe { std::slice::from_raw_parts(proof.parent_members, proof.parent_member_count) };
        let members = unsafe { std::slice::from_raw_parts(proof.members, proof.member_count) }
            .iter()
            .map(|member| SealedPendingMember {
                segment_id: member.segment_id,
                row_count: member.row_count,
                payload_digest: member.payload_digest,
            })
            .collect::<Vec<_>>();
        let parent = ManagedPinCandidate {
            scope: &self.scope,
            pin: proof.parent_pin,
            membership: PinMembership::Complete(parent_members),
        };
        let pending = PendingHeadAttempt {
            scope: &self.scope,
            schema_id: proof.schema_id,
            spec_id: proof.spec_id,
            authorization: AuthorizedPublication {
                identity: PublicationIdentity {
                    operation_id: uuid::Uuid::from_bytes(proof.operation_id),
                    table_uuid: self.scope.table_uuid,
                    input_digest: proof.input_digest,
                },
                expected_parent: (proof.expected_parent != 0).then_some(proof.expected_parent),
                snapshot_id: proof.snapshot_id,
                request_digest: proof.request_digest,
                request_body: unsafe {
                    std::slice::from_raw_parts(proof.request_body, proof.request_body_len)
                },
            },
            members: &members,
        };
        let s3 = crate::storage::S3Options {
            endpoint: Some(unsafe { borrow_string(proof.s3_endpoint) })
                .filter(|value| !value.is_empty()),
            access_key_id: Some(unsafe { borrow_string(proof.s3_access_key) })
                .filter(|value| !value.is_empty()),
            secret_access_key: Some(unsafe { borrow_string(proof.s3_secret_key) })
                .filter(|value| !value.is_empty()),
            region: Some(unsafe { borrow_string(proof.s3_region) }),
            ..Default::default()
        };
        ancestry
            .prove_pending_head(&parent, &pending, &s3)
            .map(Option::unwrap_or_default)
            .map_err(|error| {
                ReadError(
                    PgDatalakeReadStatus::ReadUnsupported,
                    format!("managed pending-head proof failed: {error:?}"),
                )
            })
    }
}

/// Prove one pending managed head against this observation's certified
/// ancestry. Writes the proven cumulative member list into `included_out`
/// (capped at `included_cap`) and returns the count. Zero means the head
/// is not proven at this pin — NOT an error.
///
/// # Safety
/// `observation` must be a live unplanned observation; proof pointers and
/// their buffers must remain readable for the call; `included_out` requires
/// `included_cap` writable i64 slots; nullable `status_out`/`err_buf` follow
/// the module convention.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_managed_read_prove_pending(
    observation: *const ManagedReadObservation,
    proof: *const PgDatalakeManagedPendingProof,
    included_out: *mut i64,
    included_cap: usize,
    status_out: *mut PgDatalakeReadStatus,
    err_buf: *mut c_char,
    err_len: usize,
) -> usize {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let observed = observation
            .as_ref()
            .ok_or_else(|| ReadError::unsupported("managed read requires an observation handle"))?;
        let proof = proof
            .as_ref()
            .ok_or_else(|| ReadError::unsupported("managed pending proof requires inputs"))?;
        observed.prove_pending(proof)
    }));
    match result {
        Ok(Ok(included)) => {
            if !status_out.is_null() {
                status_out.write(PgDatalakeReadStatus::ReadOk);
            }
            let written = included.len().min(included_cap);
            for (i, id) in included.iter().take(written).enumerate() {
                included_out.add(i).write(*id);
            }
            written
        }
        Ok(Err(error)) => {
            return_read_handle(Err::<(), _>(error), status_out, err_buf, err_len);
            0
        }
        Err(payload) => {
            return_read_handle::<()>(
                Err(runtime_error_with_payload(&payload)),
                status_out,
                err_buf,
                err_len,
            );
            0
        }
    }
}

fn runtime_error_with_payload(payload: &Box<dyn std::any::Any + Send>) -> ReadError {
    let message = payload
        .downcast_ref::<&'static str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("panic with an opaque payload");
    ReadError(
        PgDatalakeReadStatus::ReadUnavailable,
        format!("pgdatalake-core panic: {message}"),
    )
}

/// Select the managed cut from the certified candidates and the statement's
/// visible segments. Pure computation: no catalog or object contact.
/// Writes the selected pin into `pin_out` and the overlay segment IDs into
/// `overlay_out` (capped at `overlay_cap`); returns the overlay count.
/// `NoCompatiblePin` is reported as an error — the caller must fail with a
/// consistency error before emitting any row.
///
/// # Safety
/// Observation and candidate/visible pointers must remain readable for the
/// call; `pin_out` requires one writable slot and `overlay_out` requires
/// `overlay_cap` writable slots; nullable `status_out`/`err_buf` follow the
/// module convention.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_managed_read_select_cut(
    observation: *const ManagedReadObservation,
    candidates: *const PgDatalakeManagedCutCandidate,
    candidate_count: usize,
    visible_segment_ids: *const i64,
    visible_payload_retained: *const u8,
    visible_count: usize,
    pin_out: *mut i64,
    overlay_out: *mut i64,
    overlay_cap: usize,
    status_out: *mut PgDatalakeReadStatus,
    err_buf: *mut c_char,
    err_len: usize,
) -> usize {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let observed = observation
            .as_ref()
            .ok_or_else(|| ReadError::unsupported("managed read requires an observation handle"))?;
        let candidates = unsafe { std::slice::from_raw_parts(candidates, candidate_count) }
            .iter()
            .map(|candidate| ManagedPinCandidate {
                scope: &observed.scope,
                pin: candidate.pin,
                membership: match candidate.membership {
                    1 => PinMembership::Complete(unsafe {
                        std::slice::from_raw_parts(candidate.members, candidate.member_count)
                    }),
                    2 => PinMembership::Expired,
                    _ => PinMembership::Unknown,
                },
            })
            .collect::<Vec<_>>();
        let visible = unsafe { std::slice::from_raw_parts(visible_segment_ids, visible_count) }
            .iter()
            .zip(unsafe { std::slice::from_raw_parts(visible_payload_retained, visible_count) })
            .map(
                |(segment_id, retained)| crate::managed_visibility::VisibleSegment {
                    segment_id: *segment_id,
                    payload_retained: *retained != 0,
                },
            )
            .collect::<Vec<_>>();
        crate::managed_visibility::select_compatible_pin(&observed.scope, &candidates, &visible)
            .map_err(|error| {
                ReadError(
                    PgDatalakeReadStatus::ReadUnsupported,
                    format!("managed read cut selection refused: {error:?}"),
                )
            })
    }));
    match result {
        Ok(Ok(cut)) => {
            if !status_out.is_null() {
                status_out.write(PgDatalakeReadStatus::ReadOk);
            }
            unsafe { pin_out.write(cut.pin) };
            let written = cut.overlay_segment_ids.len().min(overlay_cap);
            for (i, id) in cut.overlay_segment_ids.iter().take(written).enumerate() {
                unsafe { overlay_out.add(i).write(*id) };
            }
            written
        }
        Ok(Err(error)) => {
            return_read_handle(Err::<(), _>(error), status_out, err_buf, err_len);
            0
        }
        Err(payload) => {
            return_read_handle::<()>(
                Err(runtime_error_with_payload(&payload)),
                status_out,
                err_buf,
                err_len,
            );
            0
        }
    }
}
