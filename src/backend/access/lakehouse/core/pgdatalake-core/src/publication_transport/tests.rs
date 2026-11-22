use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use tiny_http::{Header, Method, Response, Server};

use super::*;
use crate::catalog::publication::{sha256, PublicationIdentity};
use crate::catalog::rest::StaticToken;
use crate::publication_worker::{ClaimState, RetainedAttempt};

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
    ScopedPublisher::new(
        target,
        Box::new(StaticToken("catalog-token".into())),
        "enrolled-key".into(),
        "enrolled-secret".into(),
        vec![("id".into(), IcebergType::Long)],
        PublicationLimits {
            captured_bytes: 1024,
            captured_rows: 8,
            manifest_bytes: 64 * 1024,
            manifest_timeout: Duration::from_secs(3),
        },
    )
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
                        assert_eq!(auth, "Bearer catalog-token");
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
                        assert!(auth.contains("Credential=enrolled-key/"), "{auth}");
                        assert!(req
                            .headers()
                            .iter()
                            .any(|h| h.field.equiv("If-None-Match") && h.value.as_str() == "*"));
                        let mut body = Vec::new();
                        req.as_reader().read_to_end(&mut body).unwrap();
                        assert!(state.objects.insert(path.clone(), body).is_none());
                        state.object_writes.push(path);
                        empty().with_header(header("ETag", "\"created\""))
                    }
                    (&Method::Get, _) if path.starts_with(OBJECT_PATH) => {
                        assert!(auth.contains("Credential=enrolled-key/"), "{auth}");
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
    assert_eq!(
        publisher.lookup(&claim, intent).unwrap(),
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
        publisher.lookup(&claim, intent).unwrap(),
        PublicationLookup::Accepted {
            snapshot_id: snapshot
        }
    );

    let mut stale = claim;
    stale.binding.as_mut().unwrap().binding_generation += 1;
    assert!(publisher.lookup(&stale, intent).is_err());

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
