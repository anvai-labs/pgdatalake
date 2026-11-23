//! A single opaque scoped observation for the native managed-reader join.

use std::ffi::c_char;
use std::ptr;

use uuid::Uuid;

use super::{
    borrow_string, return_read_handle, PgDatalakeReadBinding, PgDatalakeReadStatus, ScanSession,
};
use crate::managed_visibility::{
    ManagedAncestry, ManagedCatalogObservation, ManagedReadError, ManagedReadScope,
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
        ScanSession::from_read_plan(bound.plan, bound.s3, &bound.schema, Some(bound.root))
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
