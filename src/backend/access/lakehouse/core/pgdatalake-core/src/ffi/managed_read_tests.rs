use super::*;
use serde_json::json;
use std::time::Duration;
use tiny_http::{Header, Response, Server};

const UUID: &CStr = c"2a830328-da37-4b0b-a867-78912e62c620";
const ROOT: &CStr = c"s3://warehouse/ns/events";
const ANCHOR: i64 = 9_007_199_254_740_993;
const HEAD: i64 = ANCHOR + 1;
const SIBLING: i64 = ANCHOR + 2;

#[derive(Clone, Copy)]
enum Case {
    Good,
    MissingSchema,
    ChangedSchema,
    ChangedCurrentSchema,
    MissingSchemaDefinition,
    DuplicateSchema,
    SiblingPin,
    Drop,
}

fn provider(case: Case) -> (CString, std::thread::JoinHandle<()>) {
    let server = Server::http("127.0.0.1:0").unwrap();
    let endpoint =
        CString::new(format!("http://{}", server.server_addr().to_ip().unwrap())).unwrap();
    let worker = std::thread::spawn(move || {
        let recv = || {
            server
                .recv_timeout(Duration::from_secs(3))
                .unwrap()
                .unwrap()
        };
        let header = |name: &str, value: &str| Header::from_bytes(name, value).unwrap();
        let catalog = recv();
        assert_eq!(catalog.url(), "/v1/enrolled/namespaces/ns/tables/events");
        let root = ROOT.to_str().unwrap();
        catalog
            .respond(Response::from_string(
                json!({"metadata-location": format!("{root}/metadata/pin.json")}).to_string(),
            ))
            .unwrap();

        let object = recv();
        assert_eq!(object.url(), "/warehouse/ns/events/metadata/pin.json");
        assert!(object
            .headers()
            .iter()
            .any(|field| field.field.equiv("Range")));
        let selected_schema = match case {
            Case::MissingSchema => None,
            Case::ChangedSchema => Some(2),
            _ => Some(3),
        };
        let mut schemas = vec![
            json!({"type": "struct", "schema-id": 3, "fields": []}),
            json!({"type": "struct", "schema-id": 2, "fields": []}),
        ];
        if matches!(case, Case::DuplicateSchema) {
            schemas.push(json!({"type": "struct", "schema-id": 3, "fields": []}));
        }
        if matches!(case, Case::MissingSchemaDefinition) {
            schemas.remove(0);
        }
        let current_schema = if matches!(case, Case::ChangedCurrentSchema) {
            2
        } else {
            3
        };
        let metadata = json!({
            "format-version": 2,
            "table-uuid": UUID.to_str().unwrap(),
            "location": root,
            "schemas": schemas,
            "current-schema-id": current_schema,
            "current-snapshot-id": HEAD,
            "refs": {"main": {"type": "branch", "snapshot-id": HEAD}},
            "snapshots": [
                {"snapshot-id": ANCHOR, "schema-id": selected_schema, "timestamp-ms": 1,
                 "sequence-number": 1, "manifest-list": format!("{root}/metadata/retained.avro")},
                {"snapshot-id": HEAD, "schema-id": 3, "timestamp-ms": 2,
                 "sequence-number": 2, "manifest-list": format!("{root}/metadata/current.avro"),
                 "parent-snapshot-id": ANCHOR, "summary": {"operation":"append"}},
                {"snapshot-id": SIBLING, "schema-id": 3, "timestamp-ms": 3,
                 "sequence-number": 2, "manifest-list": format!("{root}/metadata/sibling.avro")}
            ]
        })
        .to_string();
        let range = format!("bytes 0-{}/{}", metadata.len() - 1, metadata.len());
        let etag = header("ETag", r#""managed-fixture""#);
        let modified = header("Last-Modified", "Mon, 01 Jan 2024 00:00:00 GMT");
        object
            .respond(
                Response::from_string(metadata)
                    .with_status_code(206)
                    .with_header(header("Content-Range", &range))
                    .with_header(etag.clone())
                    .with_header(modified.clone()),
            )
            .unwrap();

        if matches!(case, Case::Good) {
            let manifest = recv();
            assert_eq!(
                manifest.url(),
                "/warehouse/ns/events/metadata/retained.avro"
            );
            manifest
                .respond(
                    Response::from_data(
                        crate::snapshot_write::build_manifest_list_bytes(&[]).unwrap(),
                    )
                    .with_header(etag)
                    .with_header(modified),
                )
                .unwrap();
        }
        assert!(server
            .recv_timeout(Duration::from_millis(100))
            .unwrap()
            .is_none());
    });
    (endpoint, worker)
}

#[test]
fn native_observation_abi_uses_one_load_and_refuses_unproved_pins_before_manifests() {
    for case in [
        Case::Good,
        Case::MissingSchema,
        Case::ChangedSchema,
        Case::ChangedCurrentSchema,
        Case::MissingSchemaDefinition,
        Case::DuplicateSchema,
        Case::SiblingPin,
        Case::Drop,
    ] {
        let (endpoint, worker) = provider(case);
        let request = PgDatalakeManagedReadRequest {
            binding: PgDatalakeReadBinding {
                expected_table_uuid: UUID.as_ptr(),
                storage_root: ROOT.as_ptr(),
                catalog_prefix: c"enrolled".as_ptr(),
                s3_region: c"us-east-1".as_ptr(),
            },
            catalog_endpoint: endpoint.as_ptr(),
            catalog_token: c"fixture".as_ptr(),
            namespace: c"ns".as_ptr(),
            table: c"events".as_ptr(),
            s3_endpoint: endpoint.as_ptr(),
            s3_access_key: c"test-access".as_ptr(),
            s3_secret_key: c"test-secret".as_ptr(),
            authority_id: [1; 16],
            incarnation_id: [2; 16],
            anchor_snapshot_id: ANCHOR,
            enrolled_schema_id: 3,
        };
        let mut status = PgDatalakeReadStatus::ReadUnavailable;
        let mut owner = unsafe {
            pg_datalake_managed_read_observe(
                &raw const request,
                &raw mut status,
                std::ptr::null_mut(),
                0,
            )
        };
        assert!(!owner.is_null());
        assert_eq!(status, PgDatalakeReadStatus::ReadOk);
        assert_eq!(unsafe { pg_datalake_managed_read_pin_count(owner) }, 2);
        assert_eq!(unsafe { pg_datalake_managed_read_pin_at(owner, 0) }, HEAD);
        assert_eq!(unsafe { pg_datalake_managed_read_pin_at(owner, 1) }, ANCHOR);
        assert_eq!(unsafe { pg_datalake_managed_read_pin_at(owner, 2) }, 0);

        if !matches!(case, Case::Drop) {
            let pin = if matches!(case, Case::SiblingPin) {
                SIBLING
            } else {
                ANCHOR
            };
            let session = unsafe {
                pg_datalake_managed_read_plan_exact(
                    &raw mut owner,
                    pin,
                    &raw mut status,
                    std::ptr::null_mut(),
                    0,
                )
            };
            assert!(owner.is_null());
            match case {
                Case::Good => {
                    assert_eq!(status, PgDatalakeReadStatus::ReadOk);
                    assert_eq!(
                        unsafe { session.as_ref() }.unwrap().plan.snapshot_id,
                        ANCHOR
                    );
                    unsafe { pg_datalake_scan_free(session) };
                }
                Case::SiblingPin => {
                    assert!(session.is_null());
                    assert_eq!(status, PgDatalakeReadStatus::ReadPinUnavailable);
                }
                _ => {
                    assert!(session.is_null());
                    assert_eq!(status, PgDatalakeReadStatus::ReadUnsupported);
                }
            }
            let retry = unsafe {
                pg_datalake_managed_read_plan_exact(
                    &raw mut owner,
                    ANCHOR,
                    &raw mut status,
                    std::ptr::null_mut(),
                    0,
                )
            };
            assert!(retry.is_null());
        }
        unsafe {
            pg_datalake_managed_read_observation_drop(&raw mut owner);
            pg_datalake_managed_read_observation_drop(&raw mut owner);
        }
        assert!(owner.is_null());
        worker.join().unwrap();
    }
}
