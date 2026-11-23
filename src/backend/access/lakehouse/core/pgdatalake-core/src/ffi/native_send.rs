//! Process-local preparation for a native managed publisher.

use std::ptr;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, UNIX_EPOCH};

use object_store::aws::AwsCredential;
use serde::Deserialize;
use uuid::Uuid;

use super::parse_digest_hex;
use crate::catalog::{metadata::IcebergType, publication::PublicationIdentity};
use crate::publication_transport::{
    PublicationCredentials, PublicationLimits, PublicationPurpose, PublicationTarget,
    ScopedPublisher,
};
use crate::publication_worker::{
    prepare_publication_send, CapturedMember, ClaimState, NativeSendAuthorization,
    PreparedManagedSend, PublicationClaim, PublicationDispatchHint, MAX_PUBLICATION_MEMBERS,
};

pub const PG_DATALAKE_NATIVE_SEND_MAX_BYTES: usize = 8 * 1024 * 1024;
pub const PG_DATALAKE_NATIVE_SEND_MAX_ROWS: usize = 65_536;
const MAX_DESCRIPTOR_BYTES: usize = 16 * 1024;
const MAX_SCHEMA_BYTES: usize = 64 * 1024;
const MAX_CREDENTIAL_BYTES: usize = 4 * 1024;

/// Purpose-specific material copied from an independently approved send grant.
/// The target is that grant's bounded JSON descriptor, separate from the job.
#[repr(C)]
pub struct PgDatalakeNativeSendCredentials {
    pub target: *const u8,
    pub target_len: usize,
    pub catalog_token: *const u8,
    pub catalog_token_len: usize,
    pub access_key: *const u8,
    pub access_key_len: usize,
    pub secret_key: *const u8,
    pub secret_key_len: usize,
    pub session_token: *const u8,
    pub session_token_len: usize,
    pub expires_unix_seconds: u64,
}

#[repr(C)]
pub struct PgDatalakeNativeSendMember {
    pub segment_id: i64,
    pub row_count: i32,
    pub payload: *const u8,
    pub payload_len: usize,
}

#[repr(C)]
pub struct PgDatalakeNativeSendInputs {
    pub request: *const u8,
    pub request_len: usize,
    pub schema: *const u8,
    pub schema_len: usize,
    pub members: *const PgDatalakeNativeSendMember,
    pub member_count: usize,
    pub credentials: *const PgDatalakeNativeSendCredentials,
}

/// The exact immutable request bytes. Copy before dropping the owner.
#[repr(C)]
pub struct PgDatalakeNativeSendProposal {
    pub parent_is_null: bool,
    pub parent: i64,
    pub snapshot: i64,
    pub digest: [u8; 32],
    pub body: *const u8,
    pub body_len: usize,
}

/// Opaque Rust-allocated owner. Keep in the preparing process.
pub struct PgDatalakePreparedSend(PreparedManagedSend);

#[derive(Deserialize)]
struct NativeSendRequest {
    target: PublicationTarget,
    operation_id: Uuid,
    owner_generation: i64,
    input_digest: String,
    observation: PublicationDispatchHint,
}

#[derive(Deserialize)]
struct NativeSendColumn {
    name: String,
    r#type: IcebergType,
}

unsafe fn bounded_json<T: serde::de::DeserializeOwned>(
    bytes: *const u8,
    len: usize,
    maximum: usize,
) -> Option<T> {
    if bytes.is_null() || !(1..=maximum).contains(&len) {
        return None;
    }
    serde_json::from_slice(std::slice::from_raw_parts(bytes, len)).ok()
}

unsafe fn credential_string(value: *const u8, len: usize) -> Option<String> {
    if value.is_null() || !(1..=MAX_CREDENTIAL_BYTES).contains(&len) {
        return None;
    }
    let bytes = std::slice::from_raw_parts(value, len);
    if bytes.contains(&0) {
        return None;
    }
    Some(std::str::from_utf8(bytes).ok()?.to_owned())
}

unsafe fn prepare_native_send(
    input: &PgDatalakeNativeSendInputs,
) -> Option<PgDatalakePreparedSend> {
    let request: NativeSendRequest =
        bounded_json(input.request, input.request_len, MAX_DESCRIPTOR_BYTES)?;
    let columns: Vec<NativeSendColumn> =
        bounded_json(input.schema, input.schema_len, MAX_SCHEMA_BYTES)?;
    let credentials = input.credentials.as_ref()?;
    let target = bounded_json(
        credentials.target,
        credentials.target_len,
        MAX_DESCRIPTOR_BYTES,
    )?;
    let expires_at =
        UNIX_EPOCH.checked_add(Duration::from_secs(credentials.expires_unix_seconds))?;

    if input.members.is_null() || !(1..=MAX_PUBLICATION_MEMBERS).contains(&input.member_count) {
        return None;
    }

    let claim = PublicationClaim {
        identity: PublicationIdentity {
            operation_id: request.operation_id,
            table_uuid: request.target.table_uuid,
            input_digest: parse_digest_hex(&request.input_digest)?,
        },
        binding: Some(request.target.binding),
        generation: request.owner_generation,
        state: ClaimState::Unsent,
    };
    let purpose = PublicationPurpose::Send;
    let publisher = ScopedPublisher::new(
        request.target,
        purpose,
        PublicationCredentials {
            target,
            purpose,
            expires_at,
            catalog_bearer: credential_string(
                credentials.catalog_token,
                credentials.catalog_token_len,
            )?,
            objects: AwsCredential {
                key_id: credential_string(credentials.access_key, credentials.access_key_len)?,
                secret_key: credential_string(credentials.secret_key, credentials.secret_key_len)?,
                token: if credentials.session_token.is_null() && credentials.session_token_len == 0
                {
                    None
                } else {
                    Some(credential_string(
                        credentials.session_token,
                        credentials.session_token_len,
                    )?)
                },
            },
        },
        columns
            .into_iter()
            .map(|column| (column.name, column.r#type))
            .collect(),
        PublicationLimits {
            captured_bytes: PG_DATALAKE_NATIVE_SEND_MAX_BYTES,
            captured_rows: PG_DATALAKE_NATIVE_SEND_MAX_ROWS,
            manifest_bytes: 1024 * 1024,
            manifest_timeout: Duration::from_secs(4),
        },
    )
    .ok()?;

    let mut members = Vec::with_capacity(input.member_count);
    let mut rows = 0usize;
    let mut bytes = 0usize;
    for member in std::slice::from_raw_parts(input.members, input.member_count) {
        rows = rows.checked_add(usize::try_from(member.row_count).ok()?)?;
        bytes = bytes.checked_add(member.payload_len)?;
        if member.payload.is_null()
            || member.payload_len == 0
            || member.row_count == 0
            || rows > PG_DATALAKE_NATIVE_SEND_MAX_ROWS
            || bytes > PG_DATALAKE_NATIVE_SEND_MAX_BYTES
        {
            return None;
        }
        members.push(CapturedMember {
            segment_id: member.segment_id,
            row_count: member.row_count,
            payload: std::slice::from_raw_parts(member.payload, member.payload_len).to_vec(),
        });
    }

    // A PostgreSQL worker checks cancellation before and after this FFI call.
    // No PostgreSQL callbacks run across Rust frames.
    let prepared = prepare_publication_send(
        &publisher,
        request.observation,
        &claim,
        &members,
        &AtomicBool::new(false),
    )
    .ok()?;
    Some(PgDatalakePreparedSend(prepared))
}

/// Prepare outside a PostgreSQL transaction, write an exact borrowed view,
/// and return its unique owner. Failure returns NULL with an empty view.
/// An unconfirmed owner can be dropped without I/O. Preparation may finish
/// immutable PUTs while the caller's cancellation is pending.
///
/// # Safety
/// Non-null input pointers must be aligned and readable for the supplied
/// lengths throughout this call, including UTF-8 credential bytes. Non-null
/// `view` must be aligned, writable, and must not
/// alias the inputs. Keep its borrowed body only while the owner lives.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_prepare_managed_send(
    input: *const PgDatalakeNativeSendInputs,
    view: *mut PgDatalakeNativeSendProposal,
) -> *mut PgDatalakePreparedSend {
    if view.is_null() {
        return ptr::null_mut();
    }
    view.write(PgDatalakeNativeSendProposal {
        parent_is_null: true,
        parent: 0,
        snapshot: 0,
        digest: [0; 32],
        body: ptr::null(),
        body_len: 0,
    });

    std::panic::catch_unwind(|| {
        let prepared = Box::new(prepare_native_send(input.as_ref()?)?);
        let proposal = prepared.0.request();
        view.write(PgDatalakeNativeSendProposal {
            parent_is_null: proposal.expected_parent().is_none(),
            parent: proposal.expected_parent().unwrap_or_default(),
            snapshot: proposal.snapshot_id(),
            digest: proposal.request_digest(),
            body: proposal.request_body().as_ptr(),
            body_len: proposal.request_body().len(),
        });
        Some(Box::into_raw(prepared))
    })
    .ok()
    .flatten()
    .unwrap_or(ptr::null_mut())
}

/// Consume the caller's owner and clear its slot. This does no provider I/O.
///
/// # Safety
/// A non-null owner must be a writable initialized slot. If occupied, it
/// must hold the sole owner from a successful preparation in this process.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_drop_managed_send(owner: *mut *mut PgDatalakePreparedSend) {
    let Some(owner) = owner.as_mut() else {
        return;
    };
    let handle = std::mem::replace(owner, ptr::null_mut());
    if !handle.is_null() {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            drop(Box::from_raw(handle));
        }));
    }
}

/// Exact worker observation after PostgreSQL confirmed the locally flushed COMMIT.
#[repr(C)]
pub struct PgDatalakeNativeSendAuthorization {
    pub operation_id: [u8; 16],
    pub owner_generation: i64,
    pub input_digest: [u8; 32],
    pub intent_xid: u32,
    pub parent_is_null: bool,
    pub parent: i64,
    pub snapshot: i64,
    pub proposal_digest: [u8; 32],
}

#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
pub enum PgDatalakeNativeSendStatus {
    ManagedSendRefused,
    ManagedSendAcknowledged,
    ManagedSendUnknown,
}

/// Consume the original owner after the same worker committed exact authorization.
/// A provider error leaves a reserved attempt for read-only verification.
///
/// # Safety
/// The owner slot must be writable and initialized by this process. If occupied,
/// it holds the sole process-local owner from successful preparation. A non-null
/// authorization pointer must be aligned and readable for the call. The trusted
/// caller supplies it only after PostgreSQL confirmed the synchronous-local
/// authorization transaction committed. The XID and matching fields cannot
/// prove that transaction outside the PostgreSQL worker.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_submit_managed_send(
    owner: *mut *mut PgDatalakePreparedSend,
    authorization: *const PgDatalakeNativeSendAuthorization,
) -> PgDatalakeNativeSendStatus {
    use PgDatalakeNativeSendStatus::{
        ManagedSendAcknowledged, ManagedSendRefused, ManagedSendUnknown,
    };

    let Some(owner) = owner.as_mut() else {
        return ManagedSendRefused;
    };
    let handle = std::mem::replace(owner, ptr::null_mut());
    if handle.is_null() {
        return ManagedSendRefused;
    }
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let prepared = Box::from_raw(handle).0;
        let Some(authorization) = authorization.as_ref() else {
            return ManagedSendRefused;
        };
        // PostgreSQL's first normal transaction ID is 3.
        if authorization.intent_xid < 3
            || (authorization.parent_is_null && authorization.parent != 0)
        {
            return ManagedSendRefused;
        }
        let Some(request) = prepared.into_authorized_request(&NativeSendAuthorization {
            operation_id: Uuid::from_bytes(authorization.operation_id),
            owner_generation: authorization.owner_generation,
            input_digest: authorization.input_digest,
            parent: (!authorization.parent_is_null).then_some(authorization.parent),
            snapshot: authorization.snapshot,
            proposal_digest: authorization.proposal_digest,
        }) else {
            return ManagedSendRefused;
        };
        if request.submit().is_ok() {
            ManagedSendAcknowledged
        } else {
            ManagedSendUnknown
        }
    }))
    .unwrap_or(ManagedSendUnknown)
}
