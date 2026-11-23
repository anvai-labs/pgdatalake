use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

use serde_json::{json, Value};
use tiny_http::{Header, Method, Response, Server};

use super::*;
use crate::catalog::publication::{sha256, PublicationIdentity};
use crate::publication_worker::{
    prepare_publication_send, ClaimState, PublicationDispatchHint, PublicationDispatchMode,
    RetainedAttempt,
};

const ROUTE: &str = "/catalog/v1/enrolled/namespaces/nested%1Forders/tables/events";
const ROOT: &str = "s3://fixture/warehouse/events";
const OBJECT_PATH: &str = "/fixture/warehouse/events/";
const PARENT_PATH: &str = "/fixture/warehouse/events/metadata/parent.avro";

fn target(endpoint: &str) -> PublicationTarget {
    PublicationTarget {
        binding: PublicationBindingIdentity {
            authority_id: Uuid::from_u128(1),
            binding_id: Uuid::from_u128(2),
            incarnation_id: Uuid::from_u128(3),
            binding_generation: 4,
        },
        table_uuid: Uuid::from_u128(5),
        table_root: ROOT.into(),
        schema_id: 7,
        spec_id: 0,
        ref_name: "main".into(),
        catalog_endpoint: format!("{endpoint}/catalog"),
        catalog_prefix: "enrolled".into(),
        namespace: vec!["nested".into(), "orders".into()],
        table_name: "events".into(),
        object_endpoint: endpoint.into(),
        s3_region: "us-east-1".into(),
    }
}

fn publisher(target: PublicationTarget) -> Result<ScopedPublisher, StorageError> {
    publisher_for(target, PublicationPurpose::Send)
}

fn publisher_for(
    target: PublicationTarget,
    purpose: PublicationPurpose,
) -> Result<ScopedPublisher, StorageError> {
    let (bearer, key, token) = match purpose {
        PublicationPurpose::Send => ("catalog-token", "enrolled-key", "send-session"),
        PublicationPurpose::Lookup => ("lookup-token", "lookup-key", "lookup-session"),
    };
    let credentials = PublicationCredentials {
        target: target.clone(),
        purpose,
        expires_at: SystemTime::now() + Duration::from_secs(60),
        catalog_bearer: bearer.into(),
        objects: AwsCredential {
            key_id: key.into(),
            secret_key: "fixture-secret".into(),
            token: Some(token.into()),
        },
    };
    publisher_with_credentials(target, purpose, credentials)
}

fn publisher_with_credentials(
    target: PublicationTarget,
    purpose: PublicationPurpose,
    credentials: PublicationCredentials,
) -> Result<ScopedPublisher, StorageError> {
    ScopedPublisher::new(
        target,
        purpose,
        credentials,
        vec![("id".into(), IcebergType::Long)],
        PublicationLimits {
            captured_bytes: 1024,
            captured_rows: 8,
            manifest_bytes: 64 * 1024,
            manifest_timeout: Duration::from_secs(3),
        },
    )
}

#[test]
fn credential_admission_blocks_wrong_scope_purpose_and_expiry_without_io() {
    let remote = FakeProviders::start();
    let expected = target(&remote.endpoint());
    let credentials = |scope, purpose, expires_at| PublicationCredentials {
        target: scope,
        purpose,
        expires_at,
        catalog_bearer: "fixture-bearer".into(),
        objects: object_store::aws::AwsCredential {
            key_id: "fixture-key".into(),
            secret_key: "fixture-secret".into(),
            token: Some("fixture-session".into()),
        },
    };
    let now = SystemTime::now();
    let future = now + Duration::from_secs(30);
    let mut sibling = target(&remote.endpoint());
    sibling.table_root.push_str("-old");
    for supplied in [
        credentials(sibling, PublicationPurpose::Send, future),
        credentials(
            target(&remote.endpoint()),
            PublicationPurpose::Lookup,
            future,
        ),
        credentials(target(&remote.endpoint()), PublicationPurpose::Send, now),
    ] {
        let result =
            publisher_with_credentials(expected.clone(), PublicationPurpose::Send, supplied);
        assert!(matches!(result, Err(StorageError::ReadDenied(_))));
    }
    let lookup = publisher_with_credentials(
        expected,
        PublicationPurpose::Lookup,
        credentials(
            target(&remote.endpoint()),
            PublicationPurpose::Lookup,
            future,
        ),
    )
    .unwrap();
    assert!(matches!(
        lookup.prepare(&claim(), &[member()]),
        Err(StorageError::ReadDenied(_))
    ));
    let state = remote.state.lock().unwrap();
    assert!(
        state.reads.is_empty()
            && state.posts.is_empty()
            && state.object_reads.is_empty()
            && state.object_writes.is_empty()
    );
}

#[test]
fn expired_credentials_are_refused_at_request_signing() {
    let catalog = PublicationBearer {
        value: "fixture-bearer".into(),
        expires_at: UNIX_EPOCH,
    };
    assert!(catalog.token().is_err());

    let storage = PublicationS3Credentials {
        credential: Arc::new(AwsCredential {
            key_id: "fixture-key".into(),
            secret_key: "fixture-secret".into(),
            token: Some("fixture-token".into()),
        }),
        expires_at: UNIX_EPOCH,
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    assert!(runtime
        .block_on(object_store::CredentialProvider::get_credential(&storage))
        .is_err());
    assert_eq!(format!("{storage:?}"), "PublicationS3Credentials");
}

fn claim() -> PublicationClaim {
    let target = target("http://127.0.0.1:1");
    PublicationClaim {
        identity: PublicationIdentity {
            operation_id: Uuid::from_u128(6),
            table_uuid: target.table_uuid,
            input_digest: [3; 32],
        },
        binding: Some(target.binding),
        generation: 9,
        state: ClaimState::Unsent,
    }
}

fn member() -> CapturedMember {
    CapturedMember {
        segment_id: 11,
        row_count: 1,
        payload: b"[[20]]".to_vec(),
    }
}

fn acquired_send() -> (PublicationDispatchHint, PublicationClaim) {
    let mut claim = claim();
    // PostgreSQL member digest for segment 11, one row, and payload [[20]].
    claim.identity.input_digest = [
        0xc2, 0x80, 0x8a, 0xc4, 0xad, 0x23, 0x61, 0x15, 0xd6, 0x2c, 0x85, 0xff, 0xd4, 0x83, 0x32,
        0x63, 0xd2, 0x02, 0x0e, 0x97, 0xcf, 0xec, 0x41, 0x77, 0x5b, 0xe6, 0x1b, 0x3e, 0xa6, 0x85,
        0x67, 0xcc,
    ];
    (
        PublicationDispatchHint {
            operation_id: claim.identity.operation_id,
            binding_id: claim.binding.unwrap().binding_id,
            owner_generation: claim.generation,
            mode: PublicationDispatchMode::Send,
        },
        claim,
    )
}

fn loaded() -> Value {
    json!({
        "metadata-location": format!("{ROOT}/metadata/v1.json"),
        "config": {
            "prefix": "untrusted-route",
            "s3.endpoint": "http://169.254.169.254",
            "s3.access-key-id": "untrusted-key",
            "s3.secret-access-key": "untrusted-secret",
        },
        "metadata": {
            "format-version": 2,
            "table-uuid": Uuid::from_u128(5),
            "location": ROOT,
            "last-sequence-number": 1,
            "schemas": [{"type": "struct", "schema-id": 7,
                         "fields": [{"id": 11, "name": "id", "required": true, "type": "long"}]}],
            "current-schema-id": 7,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "default-spec-id": 0,
            "current-snapshot-id": 100,
            "refs": {"main": {"type": "branch", "snapshot-id": 100}},
            "snapshots": [{
                "snapshot-id": 100, "sequence-number": 1, "timestamp-ms": 1,
                "manifest-list": format!("{ROOT}/metadata/parent.avro"),
                "schema-id": 7
            }]
        }
    })
}

#[derive(Default)]
struct RemoteState {
    catalog: Value,
    reads: Vec<String>,
    posts: Vec<Value>,
    objects: BTreeMap<String, Vec<u8>>,
    object_reads: Vec<String>,
    object_writes: Vec<String>,
    catalog_redirect: bool,
    object_redirect: bool,
    post_redirect: bool,
    cancel_after_three_puts: Option<Arc<AtomicBool>>,
}

struct FakeProviders {
    server: Arc<Server>,
    state: Arc<Mutex<RemoteState>>,
    worker: Option<std::thread::JoinHandle<()>>,
}

fn header(name: &str, value: &str) -> Header {
    Header::from_bytes(name, value).unwrap()
}

impl FakeProviders {
    fn start() -> Self {
        let server = Arc::new(Server::http("127.0.0.1:0").unwrap());
        let state = Arc::new(Mutex::new(RemoteState {
            catalog: loaded(),
            objects: BTreeMap::from([(
                PARENT_PATH.into(),
                crate::snapshot_write::build_manifest_list_bytes(&[]).unwrap(),
            )]),
            ..RemoteState::default()
        }));

        let listener = server.clone();
        let remote = state.clone();
        let worker = std::thread::spawn(move || {
            for mut req in listener.incoming_requests() {
                let mut state = remote.lock().unwrap();
                let path = req.url().to_string();
                let auth = req
                    .headers()
                    .iter()
                    .find(|h| h.field.equiv("Authorization"))
                    .map_or_default(|h| h.value.as_str());
                let redirect = || {
                    Response::from_data(Vec::new())
                        .with_status_code(307)
                        .with_header(header("Location", "/unexpected-route"))
                };
                let empty = || Response::from_data(Vec::new());
                let response = match (req.method(), path.as_str()) {
                    (&Method::Get, ROUTE) => {
                        assert!(matches!(
                            auth,
                            "Bearer catalog-token" | "Bearer lookup-token"
                        ));
                        state.reads.push(path);
                        if state.catalog_redirect {
                            redirect()
                        } else {
                            Response::from_data(serde_json::to_vec(&state.catalog).unwrap())
                        }
                    }
                    (&Method::Post, ROUTE) => {
                        assert_eq!(auth, "Bearer catalog-token");
                        let body: Value = serde_json::from_reader(req.as_reader()).unwrap();
                        if state.post_redirect {
                            state.posts.push(body);
                            redirect()
                        } else {
                            let proposed = body["updates"][0]["snapshot"].clone();
                            let snapshot = proposed["snapshot-id"].clone();
                            let md = &mut state.catalog["metadata"];
                            md["snapshots"].as_array_mut().unwrap().push(proposed);
                            md["current-snapshot-id"] = snapshot.clone();
                            md["refs"]["main"]["snapshot-id"] = snapshot;
                            state.posts.push(body);
                            empty().with_status_code(504)
                        }
                    }
                    (&Method::Put, _) if path.starts_with(OBJECT_PATH) => {
                        assert!(auth.contains("Credential=enrolled-key/"));
                        assert!(req
                            .headers()
                            .iter()
                            .any(|h| h.field.equiv("x-amz-security-token")
                                && h.value.as_str() == "send-session"));
                        assert!(req
                            .headers()
                            .iter()
                            .any(|h| h.field.equiv("If-None-Match") && h.value.as_str() == "*"));
                        let mut body = Vec::new();
                        req.as_reader().read_to_end(&mut body).unwrap();
                        assert!(state.objects.insert(path.clone(), body).is_none());
                        state.object_writes.push(path);
                        if let Some(cancelled) = &state.cancel_after_three_puts {
                            if state.object_writes.len() == 3 {
                                cancelled.store(true, Ordering::Relaxed);
                            }
                        }
                        empty().with_header(header("ETag", "\"created\""))
                    }
                    (&Method::Get, _) if path.starts_with(OBJECT_PATH) => {
                        let (key, token) = if path == PARENT_PATH {
                            ("Credential=enrolled-key/", "send-session")
                        } else {
                            ("Credential=lookup-key/", "lookup-session")
                        };
                        assert!(auth.contains(key));
                        assert!(req
                            .headers()
                            .iter()
                            .any(|h| h.field.equiv("x-amz-security-token")
                                && h.value.as_str() == token));
                        state.object_reads.push(path.clone());
                        if state.object_redirect {
                            redirect()
                        } else {
                            assert!(req.headers().iter().any(|h| h.field.equiv("Range")));
                            let body = state.objects[&path].clone();
                            let len = body.len();
                            Response::from_data(body)
                                .with_status_code(206)
                                .with_header(header(
                                    "Content-Range",
                                    &format!("bytes 0-{}/{len}", len - 1),
                                ))
                                .with_header(header("ETag", "\"stored\""))
                                .with_header(header(
                                    "Last-Modified",
                                    "Sat, 21 Nov 2026 14:00:00 GMT",
                                ))
                        }
                    }
                    _ => {
                        state.reads.push(path);
                        empty().with_status_code(400)
                    }
                };
                req.respond(response).unwrap();
            }
        });
        Self {
            server,
            state,
            worker: Some(worker),
        }
    }

    fn endpoint(&self) -> String {
        format!("http://{}", self.server.server_addr())
    }
}

impl Drop for FakeProviders {
    fn drop(&mut self) {
        self.server.unblock();
        self.worker.take().unwrap().join().unwrap();
    }
}

#[test]
fn refuses_wrong_claim_and_provider_identity_before_object_io() {
    let remote = FakeProviders::start();
    let publisher = publisher(target(&remote.endpoint())).unwrap();
    let mut stale = claim();
    stale.binding.as_mut().unwrap().binding_generation -= 1;
    assert!(matches!(
        publisher.prepare(&stale, &[member()]),
        Err(StorageError::Conflict(_))
    ));
    let mut stale_authority = claim();
    stale_authority.binding.as_mut().unwrap().authority_id = Uuid::from_u128(99);
    assert!(matches!(
        publisher.prepare(&stale_authority, &[member()]),
        Err(StorageError::Conflict(_))
    ));
    assert_eq!(remote.state.lock().unwrap().reads.len(), 0);

    let refused = [
        (
            "/metadata-location",
            json!("s3://fixture/warehouse/events-old/metadata/a.json"),
        ),
        (
            "/metadata/location",
            json!("s3://fixture/warehouse/events-old"),
        ),
        ("/metadata/table-uuid", json!(Uuid::from_u128(99))),
        ("/metadata/current-schema-id", json!(99)),
        ("/metadata/default-spec-id", json!(9)),
        ("/metadata/refs/main/snapshot-id", json!(101)),
        (
            "/metadata/snapshots/0/manifest-list",
            json!("s3://fixture/warehouse/events/metadata/../secret"),
        ),
    ];
    for (field, value) in refused {
        {
            let mut state = remote.state.lock().unwrap();
            state.catalog = loaded();
            *state.catalog.pointer_mut(field).unwrap() = value;
        }
        assert!(publisher.prepare(&claim(), &[member()]).is_err(), "{field}");
    }
    let state = remote.state.lock().unwrap();
    assert_eq!(state.reads, vec![ROUTE; 7]);
    assert!(
        state.posts.is_empty() && state.object_reads.is_empty() && state.object_writes.is_empty()
    );
}

#[test]
fn refuses_reserved_and_invalid_owner_at_prepare() {
    let remote = FakeProviders::start();
    let publisher = publisher(target(&remote.endpoint())).unwrap();
    let mut invalid = claim();
    invalid.generation = 0;
    assert!(matches!(
        publisher.prepare(&invalid, &[member()]),
        Err(StorageError::Conflict(_))
    ));

    let mut reserved = claim();
    reserved.state = ClaimState::Reserved(RetainedAttempt {
        expected_parent: Some(100),
        snapshot_id: 101,
        request_digest: [0; 32],
        request_body: None,
    });
    assert!(matches!(
        publisher.prepare(&reserved, &[member()]),
        Err(StorageError::Conflict(_))
    ));
    let state = remote.state.lock().unwrap();
    assert_eq!(state.reads, [] as [String; 0]);
    assert_eq!(state.object_reads, [] as [String; 0]);
    assert_eq!(state.object_writes, [] as [String; 0]);
    assert_eq!(state.posts, [] as [Value; 0]);
}

#[test]
fn preparation_refuses_stale_owner_wrong_scope_oversize_expiry_and_cancellation() {
    let remote = FakeProviders::start();
    let approved_target = target(&remote.endpoint());
    let mut publisher = publisher(approved_target.clone()).unwrap();
    let lookup = publisher_for(approved_target, PublicationPurpose::Lookup).unwrap();
    let cancelled = AtomicBool::new(false);
    let (hint, mut claim) = acquired_send();
    let stale = PublicationDispatchHint {
        owner_generation: hint.owner_generation - 1,
        ..hint
    };
    assert!(prepare_publication_send(&publisher, stale, &claim, &[member()], &cancelled).is_err());
    assert!(prepare_publication_send(&lookup, hint, &claim, &[member()], &cancelled).is_err());

    claim.binding.as_mut().unwrap().binding_generation += 1;
    assert!(prepare_publication_send(&publisher, hint, &claim, &[member()], &cancelled).is_err());
    claim.binding.as_mut().unwrap().binding_generation -= 1;

    let mut changed_member = member();
    changed_member.payload = b"[[99]]".to_vec();
    assert!(matches!(
        prepare_publication_send(&publisher, hint, &claim, &[changed_member], &cancelled),
        Err(error) if error.stage == "load_members"
    ));
    let mut over_budget = member();
    over_budget.payload.resize(1025, b' ');
    assert!(matches!(
        prepare_publication_send(&publisher, hint, &claim, &[over_budget], &cancelled),
        Err(error) if error.stage == "load_members" && error.to_string().contains("batch limits")
    ));
    let too_many_members: Vec<_> = (0..=MAX_PUBLICATION_MEMBERS)
        .map(|_| {
            let mut invalid_member = member();
            invalid_member.row_count = 0;
            invalid_member
        })
        .collect();
    assert!(matches!(
        prepare_publication_send(&publisher, hint, &claim, &too_many_members, &cancelled),
        Err(error) if error.stage == "load_members" && error.to_string().contains("batch limits")
    ));
    let member = member();
    cancelled.store(true, Ordering::Relaxed);
    assert!(matches!(
        prepare_publication_send(&publisher, hint, &claim, std::slice::from_ref(&member), &cancelled),
        Err(error) if error.to_string().contains("cancelled")
    ));
    cancelled.store(false, Ordering::Relaxed);
    publisher.expires_at = UNIX_EPOCH;
    assert!(matches!(
        prepare_publication_send(&publisher, hint, &claim, &[member], &cancelled),
        Err(error) if error.to_string().contains("current send credentials")
    ));

    let state = remote.state.lock().unwrap();
    assert_eq!(state.reads, [] as [String; 0]);
    assert_eq!(state.object_reads, [] as [String; 0]);
    assert_eq!(state.object_writes, [] as [String; 0]);
    assert_eq!(state.posts, [] as [Value; 0]);
}

#[test]
fn dropping_owned_preparation_keeps_only_create_only_objects() {
    let remote = FakeProviders::start();
    let (hint, claim) = acquired_send();
    let prepared = {
        let publisher = publisher(target(&remote.endpoint())).unwrap();
        prepare_publication_send(
            &publisher,
            hint,
            &claim,
            &[member()],
            &AtomicBool::new(false),
        )
        .unwrap()
    };
    // The proposal outlives the transport, and its view is read-only.
    let request = prepared.request();
    let exact = AuthorizedPublication {
        identity: prepared.identity(),
        expected_parent: request.expected_parent(),
        snapshot_id: request.snapshot_id(),
        request_digest: request.request_digest(),
        request_body: request.request_body(),
    };
    assert_eq!(prepared.owner_generation(), hint.owner_generation);
    assert_eq!(prepared.binding(), claim.binding);
    assert_eq!(exact.expected_parent, Some(100));
    assert!(exact.snapshot_id > 0 && exact.snapshot_id != 100);
    assert!((1..=64 * 1024).contains(&exact.request_body.len()));
    assert!(exact.matches_retained_request());
    drop(prepared);

    let state = remote.state.lock().unwrap();
    assert_eq!(state.reads, [ROUTE]);
    assert_eq!(state.object_reads, [PARENT_PATH]);
    assert_eq!(state.object_writes.len(), 3);
    assert_eq!(state.posts, [] as [Value; 0]);
    assert_eq!(state.catalog["metadata"]["current-snapshot-id"], 100);
}

#[test]
#[allow(unsafe_code)]
fn native_prepare_owner_requires_exact_authorization_and_submits_once() {
    use crate::ffi::{
        pg_datalake_drop_managed_send, pg_datalake_prepare_managed_send,
        pg_datalake_submit_managed_send, PgDatalakeNativeSendAuthorization,
        PgDatalakeNativeSendCredentials, PgDatalakeNativeSendInputs, PgDatalakeNativeSendMember,
        PgDatalakeNativeSendProposal, PgDatalakeNativeSendStatus,
    };
    use std::{fmt::Write, mem::MaybeUninit};

    let remote = FakeProviders::start();
    let target = target(&remote.endpoint());
    let credential_target = json!({
        "binding": {
            "authority_id": target.binding.authority_id,
            "binding_id": target.binding.binding_id,
            "incarnation_id": target.binding.incarnation_id,
            "binding_generation": target.binding.binding_generation,
        },
        "table_uuid": target.table_uuid,
        "table_root": target.table_root,
        "schema_id": target.schema_id,
        "spec_id": target.spec_id,
        "ref_name": target.ref_name,
        "catalog_endpoint": target.catalog_endpoint,
        "catalog_prefix": target.catalog_prefix,
        "namespace": target.namespace,
        "table_name": target.table_name,
        "object_endpoint": target.object_endpoint,
        "s3_region": target.s3_region,
    });
    let (hint, claim) = acquired_send();
    let mut input_digest = String::with_capacity(64);
    for byte in claim.identity.input_digest {
        write!(input_digest, "{byte:02x}").unwrap();
    }
    let request = json!({
        "target": &credential_target,
        "operation_id": claim.identity.operation_id,
        "owner_generation": claim.generation,
        "input_digest": input_digest,
        "observation": {
            "mode": "send", "operation_id": hint.operation_id, "binding_id": hint.binding_id,
            "owner_generation": hint.owner_generation,
        },
    });
    let request_bytes = serde_json::to_vec(&request).unwrap();
    let schema = br#"[{"name":"id","type":"long"}]"#;
    let member = member();
    let ffi_members = [PgDatalakeNativeSendMember {
        segment_id: member.segment_id,
        row_count: member.row_count,
        payload: member.payload.as_ptr(),
        payload_len: member.payload.len(),
    }];
    let target_bytes = serde_json::to_vec(&credential_target).unwrap();
    let expiry = (SystemTime::now() + Duration::from_secs(60))
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut credentials = PgDatalakeNativeSendCredentials {
        target: target_bytes.as_ptr(),
        target_len: target_bytes.len(),
        catalog_token: b"catalog-token".as_ptr(),
        catalog_token_len: b"catalog-token".len(),
        access_key: b"enrolled-key".as_ptr(),
        access_key_len: b"enrolled-key".len(),
        secret_key: b"fixture-secret".as_ptr(),
        secret_key_len: b"fixture-secret".len(),
        session_token: b"send-session".as_ptr(),
        session_token_len: b"send-session".len(),
        expires_unix_seconds: expiry,
    };
    let mut input = PgDatalakeNativeSendInputs {
        request: request_bytes.as_ptr(),
        request_len: request_bytes.len(),
        schema: schema.as_ptr(),
        schema_len: schema.len(),
        members: ffi_members.as_ptr(),
        member_count: ffi_members.len(),
        credentials: std::ptr::null(),
    };
    let prepare = |input: &PgDatalakeNativeSendInputs,
                   credentials: &PgDatalakeNativeSendCredentials| {
        let input = PgDatalakeNativeSendInputs {
            credentials,
            ..*input
        };
        let mut view = MaybeUninit::<PgDatalakeNativeSendProposal>::uninit();
        let handle =
            unsafe { pg_datalake_prepare_managed_send(&raw const input, view.as_mut_ptr()) };
        (handle, unsafe { view.assume_init() })
    };

    let mut wrong_scope = credential_target;
    wrong_scope["table_root"] = json!(format!("{ROOT}-other"));
    let wrong_target = serde_json::to_vec(&wrong_scope).unwrap();
    credentials.target = wrong_target.as_ptr();
    credentials.target_len = wrong_target.len();
    let (handle, view) = prepare(&input, &credentials);
    assert!(handle.is_null() && view.body.is_null() && view.body_len == 0);
    credentials.target = target_bytes.as_ptr();
    credentials.target_len = target_bytes.len();

    let mut stale = request;
    stale["observation"]["owner_generation"] = json!(hint.owner_generation - 1);
    let stale = serde_json::to_vec(&stale).unwrap();
    input.request = stale.as_ptr();
    input.request_len = stale.len();
    assert!(prepare(&input, &credentials).0.is_null());
    input.request = request_bytes.as_ptr();
    input.request_len = request_bytes.len();

    credentials.expires_unix_seconds = 0;
    assert!(prepare(&input, &credentials).0.is_null());
    credentials.expires_unix_seconds = expiry;
    input.member_count = MAX_PUBLICATION_MEMBERS + 1;
    assert!(prepare(&input, &credentials).0.is_null());
    input.member_count = ffi_members.len();
    let oversized_token = [b'x'; 4097];
    let approved_token = credentials.catalog_token;
    let approved_len = credentials.catalog_token_len;
    credentials.catalog_token = oversized_token.as_ptr();
    credentials.catalog_token_len = oversized_token.len();
    assert!(prepare(&input, &credentials).0.is_null());
    credentials.catalog_token = approved_token;
    credentials.catalog_token_len = approved_len;
    {
        let state = remote.state.lock().unwrap();
        assert!(state.reads.is_empty() && state.object_reads.is_empty());
        assert!(state.object_writes.is_empty() && state.posts.is_empty());
    }

    let (mut handle, view) = prepare(&input, &credentials);
    assert!(!handle.is_null());
    assert!(!view.parent_is_null && view.parent == 100);
    assert!((1..=64 * 1024).contains(&view.body_len));
    let exact_body = unsafe { std::slice::from_raw_parts(view.body, view.body_len) }.to_vec();
    let exact = AuthorizedPublication {
        identity: claim.identity,
        expected_parent: Some(view.parent),
        snapshot_id: view.snapshot,
        request_digest: view.digest,
        request_body: &exact_body,
    };
    assert!(exact.matches_retained_request());
    unsafe {
        pg_datalake_drop_managed_send(&raw mut handle);
        pg_datalake_drop_managed_send(&raw mut handle);
    }
    assert!(handle.is_null());

    {
        let state = remote.state.lock().unwrap();
        assert_eq!(state.reads, [ROUTE]);
        assert_eq!(state.object_reads, [PARENT_PATH]);
        assert_eq!(state.object_writes.len(), 3);
        assert_eq!(state.posts, [] as [Value; 0]);
        assert_eq!(state.catalog["metadata"]["current-snapshot-id"], 100);
    }

    let (mut mismatched, proposal) = prepare(&input, &credentials);
    assert!(!mismatched.is_null());
    let mut authorization = PgDatalakeNativeSendAuthorization {
        operation_id: *claim.identity.operation_id.as_bytes(),
        owner_generation: claim.generation,
        input_digest: claim.identity.input_digest,
        intent_xid: 42,
        parent_is_null: false,
        parent: 100,
        snapshot: proposal.snapshot,
        proposal_digest: [0; 32],
    };
    let refused =
        unsafe { pg_datalake_submit_managed_send(&raw mut mismatched, &raw const authorization) };
    assert_eq!(refused, PgDatalakeNativeSendStatus::ManagedSendRefused);
    assert!(mismatched.is_null());
    assert_eq!(remote.state.lock().unwrap().posts, [] as [Value; 0]);

    let (mut owner, exact) = prepare(&input, &credentials);
    assert!(!owner.is_null());
    authorization.snapshot = exact.snapshot;
    authorization.proposal_digest = exact.digest;
    let (sent, replay) = unsafe {
        (
            pg_datalake_submit_managed_send(&raw mut owner, &raw const authorization),
            pg_datalake_submit_managed_send(&raw mut owner, &raw const authorization),
        )
    };
    assert!(owner.is_null());
    assert_eq!(sent, PgDatalakeNativeSendStatus::ManagedSendUnknown);
    assert_eq!(replay, PgDatalakeNativeSendStatus::ManagedSendRefused);
    let state = remote.state.lock().unwrap();
    assert_eq!(state.posts.len(), 1);
    assert_eq!(
        state.catalog["metadata"]["current-snapshot-id"],
        exact.snapshot
    );
}

#[test]
fn cancellation_during_last_prepare_put_yields_no_proposal_or_post() {
    let remote = FakeProviders::start();
    let cancelled = Arc::new(AtomicBool::new(false));
    remote.state.lock().unwrap().cancel_after_three_puts = Some(cancelled.clone());
    let publisher = publisher(target(&remote.endpoint())).unwrap();
    let (hint, claim) = acquired_send();
    assert!(matches!(
        prepare_publication_send(&publisher, hint, &claim, &[member()], &cancelled),
        Err(error) if error.to_string().contains("cancelled")
    ));
    let state = remote.state.lock().unwrap();
    assert_eq!(state.reads, [ROUTE]);
    assert_eq!(state.object_reads, [PARENT_PATH]);
    assert_eq!(state.object_writes.len(), 3);
    assert_eq!(state.posts, [] as [Value; 0]);
    assert_eq!(state.catalog["metadata"]["current-snapshot-id"], 100);
}

#[test]
fn refuses_route_changes_and_redirects() {
    let remote = FakeProviders::start();
    let mut unsafe_target = target(&remote.endpoint());
    unsafe_target.catalog_prefix = "../different-route".into();
    assert!(publisher(unsafe_target).is_err());
    let mut unsafe_target = target(&remote.endpoint());
    unsafe_target.catalog_endpoint = format!("{}/test/../different-route", remote.endpoint());
    assert!(publisher(unsafe_target).is_err());
    let publisher = publisher(target(&remote.endpoint())).unwrap();

    remote.state.lock().unwrap().catalog_redirect = true;
    assert!(publisher.prepare(&claim(), &[member()]).is_err());
    {
        let mut state = remote.state.lock().unwrap();
        state.catalog_redirect = false;
        state.object_redirect = true;
    }
    assert!(publisher.prepare(&claim(), &[member()]).is_err());
    {
        let mut state = remote.state.lock().unwrap();
        state.object_redirect = false;
        state.post_redirect = true;
        assert_eq!(state.object_writes.len(), 0);
    }
    let prepared = publisher.prepare(&claim(), &[member()]).unwrap();
    assert!(matches!(
        prepared.submit(),
        Err(RestError::CommitStateUnknown(_))
    ));
    let state = remote.state.lock().unwrap();
    assert_eq!(state.reads, vec![ROUTE; 3]);
    assert_eq!(state.object_reads, vec![PARENT_PATH; 2]);
    assert_eq!(state.posts.len(), 1);
}

#[test]
fn one_pinned_post_and_read_only_exact_recovery() {
    let remote = FakeProviders::start();
    let publisher = publisher(target(&remote.endpoint())).unwrap();
    let verifier = publisher_for(target(&remote.endpoint()), PublicationPurpose::Lookup).unwrap();
    let mut claim = claim();
    let prepared = publisher.prepare(&claim, &[member()]).unwrap();
    let body = prepared.request_body().to_vec();
    let snapshot = prepared.snapshot_id();
    assert_eq!(prepared.expected_parent(), Some(100));
    let intent = AuthorizedPublication {
        identity: claim.identity,
        expected_parent: Some(100),
        snapshot_id: snapshot,
        request_digest: sha256(&body),
        request_body: &body,
    };
    claim.state = ClaimState::Reserved(RetainedAttempt {
        expected_parent: intent.expected_parent,
        snapshot_id: intent.snapshot_id,
        request_digest: intent.request_digest,
        request_body: Some(body.clone()),
    });
    assert!(publisher.lookup(&claim, intent).is_err());
    assert_eq!(
        verifier.lookup(&claim, intent).unwrap(),
        PublicationLookup::Unresolved
    );
    assert!(matches!(
        prepared.submit(),
        Err(RestError::CommitStateUnknown(_))
    ));
    {
        let mut state = remote.state.lock().unwrap();
        let md = &mut state.catalog["metadata"];
        let later_snapshot = if snapshot == 102 { 103 } else { 102 };
        md["snapshots"].as_array_mut().unwrap().push(json!({
            "snapshot-id": later_snapshot, "parent-snapshot-id": snapshot,
            "sequence-number": 3, "timestamp-ms": 3, "schema-id": 8,
            "manifest-list": format!("{ROOT}/metadata/later.avro"),
        }));
        let mut later_schema = md["schemas"][0].clone();
        later_schema["schema-id"] = json!(8);
        later_schema["fields"].as_array_mut().unwrap().push(json!({
            "id": 12, "name": "note", "required": false, "type": "string"
        }));
        md["schemas"].as_array_mut().unwrap().push(later_schema);
        md["current-schema-id"] = json!(8);
        md["current-snapshot-id"] = json!(later_snapshot);
        md["refs"]["main"]["snapshot-id"] = json!(later_snapshot);
    }
    assert_eq!(
        verifier.lookup(&claim, intent).unwrap(),
        PublicationLookup::Accepted {
            snapshot_id: snapshot
        }
    );

    let mut stale = claim;
    stale.binding.as_mut().unwrap().binding_generation += 1;
    assert!(verifier.lookup(&stale, intent).is_err());

    let state = remote.state.lock().unwrap();
    assert_eq!(state.reads, vec![ROUTE; 3]);
    assert_eq!(state.posts.len(), 1);
    assert_eq!(
        state.posts[0],
        serde_json::from_slice::<Value>(&body).unwrap()
    );
    assert_eq!(state.posts[0]["requirements"][1]["snapshot-id"], 100);
    assert_eq!(state.posts[0]["requirements"][2]["current-schema-id"], 7);
    assert_eq!(state.posts[0]["requirements"][3]["default-spec-id"], 0);
    assert_eq!(state.object_writes.len(), 3);
    let parquet = &state.objects[&state.object_writes[0]];
    assert_eq!(&parquet[..4], b"PAR1");
    assert_eq!(state.object_reads.len(), 2);
    assert_eq!(state.object_reads[0], PARENT_PATH);
    let manifest = state.posts[0]["updates"][0]["snapshot"]["manifest-list"]
        .as_str()
        .unwrap();
    assert_eq!(
        state.object_reads[1],
        format!("/{}", manifest.strip_prefix("s3://").unwrap())
    );
}
