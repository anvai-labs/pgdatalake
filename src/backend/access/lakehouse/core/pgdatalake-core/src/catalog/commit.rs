//! REST commit protocol (specs/lakehouse/03-catalog.md §2 Commit, doc 07
//! §2) — M2 slice 25. `commitTable` is our optimistic-concurrency
//! instrument: every managed write carries requirements
//! (`AssertRefSnapshotId(main, base)`, `AssertTableUUID`, …) so a stale
//! plan fails loudly at the server (409 `CommitFailedException`) instead
//! of silently losing updates. `commit_with_retry` re-plans the delta from
//! a fresh `loadTable` on conflict, bounded by the caller's retry budget
//! (SQL GUC `lakehouse.commit_retries` maps onto it one level up).
//!
//! Wire shapes follow the Iceberg REST spec as pyiceberg 0.12 writes them
//! (verified against the installed pyiceberg source — the interop oracle
//! for M2's "Trino reads a table we wrote" gate): requirements
//! discriminate on `"type"`, updates on `"action"`, and
//! `assert-ref-snapshot-id` ALWAYS carries `snapshot-id` (null = the ref
//! must not exist yet).

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::metadata::Snapshot;
use super::rest::{RestCatalog, RestError};
use super::types::LoadTableResult;

/// One entry of the commitTable `requirements` list (REST spec
/// `TableRequirement`). All server-side requirement types are implemented
/// because this is the conflict instrument for every managed write (doc 03
/// §2: "every requirement type implemented").
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum TableRequirement {
    /// Table must not exist (createTable path, slice 26/27).
    #[serde(rename_all = "kebab-case")]
    AssertCreate {},
    /// Table UUID must match the one we loaded (we never commit against a
    /// dropped-and-recreated table).
    #[serde(rename = "assert-table-uuid", rename_all = "kebab-case")]
    AssertTableUUID { uuid: uuid::Uuid },
    /// The ref (branch/tag) must still point at `snapshot_id`; None means
    /// the ref must not exist. Serialized as an explicit null (pyiceberg
    /// contract — some servers require the key present).
    #[serde(rename_all = "kebab-case")]
    AssertRefSnapshotId {
        r#ref: String,
        #[serde(rename = "snapshot-id")]
        snapshot_id: Option<i64>,
    },
    #[serde(rename_all = "kebab-case")]
    AssertLastAssignedFieldId {
        #[serde(rename = "last-assigned-field-id")]
        last_assigned_field_id: i64,
    },
    #[serde(rename_all = "kebab-case")]
    AssertCurrentSchemaId {
        #[serde(rename = "current-schema-id")]
        current_schema_id: i64,
    },
    #[serde(rename_all = "kebab-case")]
    AssertLastAssignedPartitionId {
        #[serde(rename = "last-assigned-partition-id")]
        last_assigned_partition_id: Option<i64>,
    },
    #[serde(rename_all = "kebab-case")]
    AssertDefaultSpecId {
        #[serde(rename = "default-spec-id")]
        default_spec_id: i64,
    },
    #[serde(rename_all = "kebab-case")]
    AssertDefaultSortOrderId {
        #[serde(rename = "default-sort-order-id")]
        default_sort_order_id: i64,
    },
}

/// One entry of the commitTable `updates` list (REST spec `TableUpdate`).
/// The catalog applies these to its stored metadata — the client ships
/// intent, not a rewritten metadata.json. This set covers the M2 append /
/// truncate / ref-management commit shapes; schema-evolution updates land
/// with the managed-DDL slices.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum TableUpdate {
    #[serde(rename_all = "kebab-case")]
    AddSnapshot { snapshot: Snapshot },
    #[serde(rename_all = "kebab-case")]
    SetSnapshotRef {
        #[serde(rename = "ref-name")]
        ref_name: String,
        /// "branch" or "tag".
        #[serde(rename = "type")]
        kind: RefKind,
        #[serde(rename = "snapshot-id")]
        snapshot_id: i64,
        #[serde(
            rename = "min-snapshots-to-keep",
            skip_serializing_if = "Option::is_none"
        )]
        min_snapshots_to_keep: Option<i64>,
        #[serde(
            rename = "max-snapshot-age-ms",
            skip_serializing_if = "Option::is_none"
        )]
        max_snapshot_age_ms: Option<i64>,
    },
    #[serde(rename_all = "kebab-case")]
    SetProperties { updates: BTreeMap<String, String> },
    #[serde(rename_all = "kebab-case")]
    RemoveProperties { removals: Vec<String> },
    #[serde(rename_all = "kebab-case")]
    SetLocation { location: String },
    #[serde(rename_all = "kebab-case")]
    RemoveSnapshots {
        #[serde(rename = "snapshot-ids")]
        snapshot_ids: Vec<i64>,
    },
    #[serde(rename_all = "kebab-case")]
    RemoveSnapshotRef {
        #[serde(rename = "ref-name")]
        ref_name: String,
    },
}

/// Branch/tag discriminator of `SetSnapshotRef` (REST spec `RefType`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RefKind {
    Branch,
    Tag,
}

/// Body of `POST /v1/{prefix}/namespaces/{ns}/tables/{table}`
/// (commitTable; the REST spec overload of loadTable's route).
#[derive(Debug, Serialize)]
struct CommitTableRequest<'a> {
    requirements: &'a [TableRequirement],
    updates: &'a [TableUpdate],
}

/// Tolerant map decoder: Lakekeeper sends explicit `"config": null` /
/// `"metadata": null` in commitTable responses (v3-first in-band metadata);
/// serde's `default` alone rejects explicit nulls.
fn null_map_as_empty<'de, D>(deserializer: D) -> Result<BTreeMap<String, String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let v: Option<BTreeMap<String, String>> = serde::Deserialize::deserialize(deserializer)?;
    Ok(v.unwrap_or_default())
}

/// Response of a successful commitTable. The in-band `metadata` field of
/// the v3-first model is tolerated and ignored (we fetch fresh metadata on
/// the next open anyway).
#[derive(Debug, Clone, Deserialize)]
pub struct CommitTableResponse {
    #[serde(rename = "metadata-location")]
    pub metadata_location: String,
    #[serde(default, deserialize_with = "null_map_as_empty")]
    pub config: BTreeMap<String, String>,
    /// Present (possibly null) from v3-first servers; ignored.
    #[serde(default)]
    pub metadata: Option<serde::de::IgnoredAny>,
}

/// Wire shape of the REST spec `ErrorResponse` (error bodies).
#[derive(Deserialize)]
struct ErrorWire {
    #[serde(default)]
    error: Option<ErrorDetail>,
}

#[derive(Deserialize)]
struct ErrorDetail {
    message: String,
}

impl RestCatalog {
    /// commitTable (doc 03 §2 Commit): POST requirements + updates; a 409
    /// (`CommitFailedException` — some requirement failed server-side)
    /// classifies as `RestError::Conflict` so `commit_with_retry` can react
    /// while other 4xx/5xx stay non-retryable.
    pub fn commit_table(
        &self,
        namespace: &[&str],
        table: &str,
        requirements: Vec<TableRequirement>,
        updates: Vec<TableUpdate>,
    ) -> Result<CommitTableResponse, RestError> {
        let prefix = self.prefix.lock().expect("prefix mutex poisoned").clone();
        let ns = namespace
            .iter()
            .map(|c| urlencoding::encode(c))
            .collect::<Vec<_>>()
            .join("%1F");
        // Same route as loadTable; the method (POST) selects commitTable
        // per the REST spec.
        let url = format!(
            "{}/v1{}/namespaces/{ns}/tables/{}",
            self.endpoint,
            prefix.as_ref().map(|p| format!("/{p}")).unwrap_or_default(),
            urlencoding::encode(table),
        );
        let body = CommitTableRequest {
            requirements: &requirements,
            updates: &updates,
        };
        let payload = serde_json::to_vec(&body)
            .map_err(|e| RestError::Http(format!("commitTable serialize: {e}")))?;
        let resp = self
            .inner
            .post(&url)
            .bearer_auth(self.tokens.token())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(payload)
            .send()
            .map_err(|e| RestError::Http(e.to_string()))?;
        let status = resp.status();
        if status == reqwest::StatusCode::CONFLICT {
            // Extract the server's requirement-failure message (REST spec
            // ErrorResponse shape; fall back to the raw body).
            let text = resp.text().unwrap_or_default();
            let msg = serde_json::from_str::<ErrorWire>(&text)
                .ok()
                .and_then(|e| e.error.map(|x| x.message))
                .unwrap_or(text);
            return Err(RestError::Conflict(msg));
        }
        if !status.is_success() {
            return Err(RestError::Http(format!("POST {url} -> {status}")));
        }
        // read as text first: decode failures surface the body prefix in
        // the error instead of an opaque reqwest message
        let text = resp.text().unwrap_or_default();
        serde_json::from_str(&text).map_err(|e| {
            RestError::Decode(format!(
                "commitTable response: {e}; body starts: {:?}",
                text.chars().take(200).collect::<String>()
            ))
        })
    }

    /// Commit with bounded re-plan retry (doc 07 §2 step 3). On each
    /// conflict the table is RELOADED and `plan` is called again with the
    /// fresh base — "409 -> re-plan delta" is a fresh commit, not a blind
    /// replay of stale requirements. `max_retries` bounds total attempts
    /// AFTER the first (SQL: `lakehouse.commit_retries`, default 8);
    /// `backoff` sleeps between attempts (tests pass ZERO).
    pub fn commit_with_retry<F>(
        &self,
        namespace: &[&str],
        table: &str,
        mut plan: F,
        max_retries: u32,
        backoff: Duration,
    ) -> Result<CommitTableResponse, RestError>
    where
        F: FnMut(&LoadTableResult) -> Result<(Vec<TableRequirement>, Vec<TableUpdate>), RestError>,
    {
        let mut attempt = 0u32;
        loop {
            let loaded = self.load_table(namespace, table)?;
            let (requirements, updates) = plan(&loaded)?;
            match self.commit_table(namespace, table, requirements, updates) {
                Ok(resp) => return Ok(resp),
                Err(e @ RestError::Conflict(_)) if attempt < max_retries => {
                    let _ = e;
                    attempt += 1;
                    if !backoff.is_zero() {
                        std::thread::sleep(backoff);
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::metadata::{
        FormatVersion, IcebergTypeMarker, Schema, SnapshotRef, TableMetadata,
    };
    use crate::catalog::rest::StaticToken;
    use std::collections::VecDeque;

    fn snap(id: i64, parent: Option<i64>) -> Snapshot {
        Snapshot {
            snapshot_id: id,
            sequence_number: Some(1),
            first_row_id: None,
            parent_snapshot_id: parent,
            timestamp_ms: 1_700_000_000_000,
            manifest_list: format!("s3://w/t/metadata/manifests/snap-{id}-m0.avro"),
            summary: BTreeMap::from([
                ("operation".to_string(), "append".to_string()),
                ("added-data-files".to_string(), "1".to_string()),
            ]),
            schema_id: Some(0),
        }
    }

    /// Minimal loadTable result with one snapshot and a main ref pointing
    /// at it — the base a plan closure asserts against.
    fn load_result(cur_snapshot: i64) -> LoadTableResult {
        LoadTableResult {
            metadata_location: format!("s3://w/t/metadata/000{cur_snapshot}-abc.metadata.json"),
            config: BTreeMap::new(),
            metadata: Some(TableMetadata {
                format_version: FormatVersion::V2,
                table_uuid: uuid::Uuid::parse_str("1ca92afa-70b9-4d02-9ba6-7ee1a3ac0f5e").unwrap(),
                location: "s3://w/t".into(),
                last_sequence_number: Some(1),
                schemas: vec![Schema {
                    struct_type: IcebergTypeMarker::Struct,
                    schema_id: 0,
                    fields: vec![],
                }],
                current_schema_id: 0,
                snapshots: vec![snap(cur_snapshot, None)],
                current_snapshot_id: Some(cur_snapshot),
                properties: BTreeMap::new(),
                partition_specs: vec![],
                default_partition_spec_id: Some(0),
                refs: BTreeMap::from([(
                    "main".to_string(),
                    SnapshotRef {
                        snapshot_id: cur_snapshot,
                        kind: "branch".into(),
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                    },
                )]),
            }),
        }
    }

    /// The plan closure doc 07 §2 prescribes: AssertTableUUID +
    /// AssertRefSnapshotId(main, loaded base) + add-snapshot + set main ref.
    fn append_plan(loaded: &LoadTableResult) -> (Vec<TableRequirement>, Vec<TableUpdate>) {
        let md = loaded.metadata.as_ref().unwrap();
        (
            vec![
                TableRequirement::AssertTableUUID {
                    uuid: md.table_uuid,
                },
                TableRequirement::AssertRefSnapshotId {
                    r#ref: "main".into(),
                    snapshot_id: md.current_snapshot_id,
                },
            ],
            vec![
                TableUpdate::AddSnapshot {
                    snapshot: snap(md.current_snapshot_id.unwrap() + 1, md.current_snapshot_id),
                },
                TableUpdate::SetSnapshotRef {
                    ref_name: "main".into(),
                    kind: RefKind::Branch,
                    snapshot_id: md.current_snapshot_id.unwrap() + 1,
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                },
            ],
        )
    }

    /// A scripted mock catalog: sequential (status, body) responses, with
    /// every request recorded (method, url, body) for assertions.
    struct Mock {
        addr: String,
        requests: std::sync::Arc<std::sync::Mutex<Vec<(String, String, String)>>>,
        handle: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
    }

    impl Mock {
        fn new(script: Vec<(u16, &str)>) -> Self {
            let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
            let addr = server.server_addr().to_string();
            let script: VecDeque<(u16, String)> = script
                .into_iter()
                .map(|(s, b)| (s, b.to_string()))
                .collect();
            let script = std::sync::Arc::new(std::sync::Mutex::new(script));
            let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let requests_t = std::sync::Arc::clone(&requests);
            let handle = std::thread::spawn(move || loop {
                let next = {
                    let mut q = script.lock().unwrap();
                    q.pop_front()
                };
                let Some((status, body)) = next else { break };
                let mut req = server.recv().unwrap();
                let mut req_body = String::new();
                let _ = req.as_reader().read_to_string(&mut req_body);
                requests_t.lock().unwrap().push((
                    req.method().to_string(),
                    req.url().to_string(),
                    req_body,
                ));
                let resp = tiny_http::Response::from_string(body)
                    .with_status_code(status)
                    .with_header(
                        tiny_http::Header::from_bytes(
                            &b"Content-Type"[..],
                            &b"application/json"[..],
                        )
                        .unwrap(),
                    );
                let _ = req.respond(resp);
            });
            Self {
                addr,
                requests,
                handle: std::sync::Mutex::new(Some(handle)),
            }
        }

        fn join(&self) {
            if let Some(h) = self.handle.lock().unwrap().take() {
                h.join().unwrap();
            }
        }

        fn catalog(&self) -> RestCatalog {
            RestCatalog::new(
                &format!("http://{}", self.addr),
                Box::new(StaticToken("t".into())),
            )
            .unwrap()
            .with_prefix("01a1f6a0-7ee1-70cb-a62d-93b40593d633")
        }

        fn requests(&self) -> Vec<(String, String, String)> {
            self.requests.lock().unwrap().clone()
        }
    }

    fn commit_ok_body() -> String {
        r#"{"metadata-location": "s3://w/t/metadata/00002-xyz.metadata.json", "config": {}}"#.into()
    }

    fn conflict_body() -> String {
        r#"{"error": {"message": "Requirement failed: branch main has changed: expected id 100, found 200", "type": "CommitFailedException", "code": 409}}"#.into()
    }

    /// TDD anchor (M2 slice 25): commitTable hits the loadTable route with
    /// POST, sends requirements (uuid + main-ref pin) and updates per the
    /// pyiceberg-verified wire shapes, and parses the metadata-location.
    #[test]
    fn commit_table_posts_requirements_and_updates() {
        let mock = Mock::new(vec![(200, commit_ok_body().leak())]);

        let (requirements, updates) = append_plan(&load_result(100));
        let resp = mock
            .catalog()
            .commit_table(&["analytics"], "events", requirements, updates)
            .expect("commit_table");

        mock.join();
        let reqs = mock.requests();
        assert_eq!(reqs.len(), 1, "one POST");
        let (method, url, body) = &reqs[0];
        assert_eq!(method, "POST", "commitTable is POST");
        assert_eq!(
            url,
            "/v1/01a1f6a0-7ee1-70cb-a62d-93b40593d633/namespaces/analytics/tables/events"
        );
        assert!(
            body.contains(r#""requirements":["#) && body.contains(r#""updates":["#),
            "body has requirements+updates: {body}"
        );
        assert!(
            body.contains(r#""type":"assert-table-uuid""#)
                && body.contains(r#""type":"assert-ref-snapshot-id""#),
            "requirement discriminants are spec types: {body}"
        );
        assert!(
            body.contains(r#""ref":"main""#) && body.contains(r#""snapshot-id":100"#),
            "main ref pinned to base: {body}"
        );
        assert!(
            body.contains(r#""action":"add-snapshot""#)
                && body.contains(r#""action":"set-snapshot-ref""#),
            "update discriminants are spec actions: {body}"
        );
        assert_eq!(
            resp.metadata_location,
            "s3://w/t/metadata/00002-xyz.metadata.json"
        );
    }

    /// 409 + CommitFailedException classifies as Conflict (retryable),
    /// carrying the server's requirement message for the ERROR report.
    #[test]
    fn commit_table_409_is_conflict() {
        let mock = Mock::new(vec![(409, conflict_body().leak())]);
        let (requirements, updates) = append_plan(&load_result(100));
        let err = mock
            .catalog()
            .commit_table(&["analytics"], "events", requirements, updates)
            .unwrap_err();
        mock.join();
        match err {
            RestError::Conflict(m) => assert!(
                m.contains("branch main has changed"),
                "server message preserved: {m}"
            ),
            other => panic!("expected Conflict, got {other}"),
        }
    }

    /// A 500 (catalog down / internal) is NOT a conflict: no retry, Http.
    #[test]
    fn commit_table_500_is_http_not_conflict() {
        let mock = Mock::new(vec![(
            500,
            r#"{"error": {"message": "boom", "type": "RESTException", "code": 500}}"#,
        )]);
        let (requirements, updates) = append_plan(&load_result(100));
        let err = mock
            .catalog()
            .commit_table(&["analytics"], "events", requirements, updates)
            .unwrap_err();
        mock.join();
        assert!(matches!(err, RestError::Http(_)), "{err}");
    }

    /// doc 07 §2 step 3: on conflict the commit RE-PLANS — reload gives a
    /// newer base (100 -> 200), the plan closure asserts against it, and
    /// the second POST carries snapshot-id 200. Bounded, then succeeds.
    #[test]
    fn commit_with_retry_replans_after_conflict() {
        let mock = Mock::new(vec![
            (
                200,
                serde_json::to_string(&load_result(100)).unwrap().leak(),
            ),
            (409, conflict_body().leak()),
            (
                200,
                serde_json::to_string(&load_result(200)).unwrap().leak(),
            ),
            (200, commit_ok_body().leak()),
        ]);

        let mut bases = Vec::new();
        let resp = mock
            .catalog()
            .commit_with_retry(
                &["analytics"],
                "events",
                |loaded| {
                    bases.push(loaded.metadata.as_ref().unwrap().current_snapshot_id);
                    Ok(append_plan(loaded))
                },
                3,
                Duration::ZERO,
            )
            .expect("commit_with_retry");

        mock.join();
        assert_eq!(bases, vec![Some(100), Some(200)], "plan saw both bases");
        let reqs = mock.requests();
        assert_eq!(reqs.len(), 4, "GET,POST,GET,POST");
        assert_eq!(reqs[0].0, "GET");
        assert_eq!(reqs[1].0, "POST");
        assert!(
            reqs[1].2.contains(r#""snapshot-id":100"#),
            "first POST pins base 100: {}",
            reqs[1].2
        );
        assert!(
            reqs[3].2.contains(r#""snapshot-id":200"#),
            "retry POST pins replanned base 200: {}",
            reqs[3].2
        );
        assert_eq!(
            resp.metadata_location,
            "s3://w/t/metadata/00002-xyz.metadata.json"
        );
    }

    /// Retries are bounded: a permanently-conflicting commit exhausts the
    /// budget and surfaces the LAST conflict (doc 03 §5: on exhaustion
    /// ERROR, staged files orphaned, orphan GC cleans up).
    #[test]
    fn commit_with_retry_exhausts_budget() {
        // max_retries=2 -> 3 attempts = 3x(GET loadTable + POST 409)
        let mut script = Vec::new();
        for _ in 0..3 {
            script.push((200, serde_json::to_string(&load_result(100)).unwrap()));
            script.push((409, conflict_body()));
        }
        let script = script
            .into_iter()
            .map(|(s, b)| (s, Box::leak(b.into_boxed_str()) as &str))
            .collect();
        let mock = Mock::new(script);

        let err = mock
            .catalog()
            .commit_with_retry(
                &["analytics"],
                "events",
                |l| Ok(append_plan(l)),
                2,
                Duration::ZERO,
            )
            .unwrap_err();

        mock.join();
        match err {
            RestError::Conflict(m) => assert!(m.contains("has changed"), "{m}"),
            other => panic!("expected Conflict after exhaustion, got {other}"),
        }
        assert_eq!(mock.requests().len(), 6, "3 reload+commit attempts");
    }

    /// `AssertRefSnapshotId` with snapshot_id=None serializes an explicit
    /// null (pyiceberg contract: some servers require the key present) —
    /// the "ref must not exist" create-time assertion.
    #[test]
    fn assert_ref_snapshot_id_none_is_explicit_null() {
        let json = serde_json::to_string(&TableRequirement::AssertRefSnapshotId {
            r#ref: "main".into(),
            snapshot_id: None,
        })
        .unwrap();
        assert!(json.contains(r#""snapshot-id":null"#), "{json}");
        let back: TableRequirement = serde_json::from_str(&json).unwrap();
        assert_eq!(
            back,
            TableRequirement::AssertRefSnapshotId {
                r#ref: "main".into(),
                snapshot_id: None
            }
        );
    }

    /// Round-trip: the add-snapshot update serializes the full snapshot
    /// (kebab-case fields, summary map) and parses back identical.
    #[test]
    fn add_snapshot_update_roundtrips() {
        let upd = TableUpdate::AddSnapshot {
            snapshot: snap(300, Some(299)),
        };
        let json = serde_json::to_string(&upd).unwrap();
        assert!(json.contains(r#""action":"add-snapshot""#), "{json}");
        assert!(json.contains(r#""snapshot-id":300"#), "{json}");
        assert!(json.contains(r#""parent-snapshot-id":299"#), "{json}");
        assert!(json.contains(r#""manifest-list":"#), "{json}");
        assert!(json.contains(r#""operation":"append""#), "{json}");
        let back: TableUpdate = serde_json::from_str(&json).unwrap();
        assert_eq!(back, upd);
    }
}
