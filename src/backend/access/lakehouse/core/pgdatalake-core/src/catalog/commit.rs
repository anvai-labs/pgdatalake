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

use super::metadata::{FormatVersion, Snapshot, TableMetadata};
pub use super::publication::PublicationIdentity;
use super::publication::{
    hex_digest, scoped_manifest_location, sha256, INPUT_DIGEST_KEY, MANIFEST_LIST_DIGEST_KEY,
    MAX_STAGED_REQUEST_BYTES, OPERATION_ID_KEY,
};
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

/// An immutable append request associated with its original catalog client
/// and route. Store its request digest in the ledger before consuming it.
pub struct PreparedPublication {
    catalog: RestCatalog,
    prefix: Option<String>,
    url: String,
    payload: Vec<u8>,
    expected_parent: Option<i64>,
    snapshot_id: i64,
}

impl PreparedPublication {
    #[must_use]
    pub fn expected_parent(&self) -> Option<i64> {
        self.expected_parent
    }

    #[must_use]
    pub fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    #[must_use]
    pub fn request_body(&self) -> &[u8] {
        &self.payload
    }

    #[must_use]
    pub fn request_digest(&self) -> [u8; 32] {
        sha256(&self.payload)
    }

    pub(crate) fn route_prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    /// Submit after the durable send fence authorizes this exact digest.
    /// Consumes the proposal and sends without retries or redirect handling.
    pub fn submit(self) -> Result<CommitTableResponse, RestError> {
        if self
            .catalog
            .prefix
            .lock()
            .expect("prefix mutex poisoned")
            .as_ref()
            != self.prefix.as_ref()
        {
            return Err(RestError::Http(
                "catalog route changed after append preparation".into(),
            ));
        }
        self.catalog.post_commit_payload(&self.url, self.payload)
    }
}

fn serialize_commit(
    requirements: &[TableRequirement],
    updates: &[TableUpdate],
) -> Result<Vec<u8>, RestError> {
    serde_json::to_vec(&CommitTableRequest {
        requirements,
        updates,
    })
    .map_err(|e| RestError::Http(format!("commitTable serialize: {e}")))
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
    #[serde(rename = "type")]
    kind: String,
}

pub(crate) fn validate_staged_base(
    base: &TableMetadata,
    table_uuid: uuid::Uuid,
) -> Result<i32, RestError> {
    if base.table_uuid != table_uuid {
        return Err(RestError::Conflict(
            "staged append table UUID changed".into(),
        ));
    }
    let main = base.refs.get("main");
    let expected_parent = base.current_snapshot_id;
    if main.map(|reference| reference.snapshot_id) != expected_parent
        || main.is_some_and(|reference| reference.kind != "branch")
        || expected_parent.is_some_and(|parent| parent <= 0 || base.snapshot(parent).is_none())
    {
        return Err(RestError::Conflict(
            "staged append has no verified main pin".into(),
        ));
    }
    let spec = base
        .default_partition_spec_id
        .and_then(|spec_id| base.partition_spec(spec_id));
    let Some(spec) = spec.filter(|spec| spec.fields.is_empty()) else {
        return Err(RestError::Decode(
            "staged append requires a known unpartitioned spec".into(),
        ));
    };
    if base.format_version != FormatVersion::V2 || base.current_schema().is_none() {
        return Err(RestError::Decode(
            "staged append requires a known v2 schema".into(),
        ));
    }
    Ok(spec.spec_id)
}

impl RestCatalog {
    /// Package the writer's immutable append snapshot for a bound v2 table
    /// on main. Preparation performs no network calls or catalog mutations.
    pub fn prepare_staged_append(
        &self,
        namespace: &[&str],
        table: &str,
        base: &TableMetadata,
        mut snapshot: Snapshot,
        identity: PublicationIdentity,
        manifest_list_digest: [u8; 32],
    ) -> Result<PreparedPublication, RestError> {
        let spec_id = validate_staged_base(base, identity.table_uuid)?;
        let main = base.refs.get("main");
        let expected_parent = base.current_snapshot_id;
        if !scoped_manifest_location(&base.location, &snapshot.manifest_list) {
            return Err(RestError::Decode(
                "staged append manifest list is outside a valid table metadata location".into(),
            ));
        }
        let expected_sequence = base.last_sequence_number.unwrap_or(0).checked_add(1);
        if snapshot.parent_snapshot_id != expected_parent
            || snapshot.schema_id != Some(base.current_schema_id)
            || snapshot.snapshot_id <= 0
            || base.snapshot(snapshot.snapshot_id).is_some()
            || expected_sequence != snapshot.sequence_number
            || snapshot.sequence_number.is_none()
            || snapshot.first_row_id.is_some()
            || snapshot.summary.get("operation").map(String::as_str) != Some("append")
        {
            return Err(RestError::Conflict(
                "prepared snapshot does not match the staged append base".into(),
            ));
        }

        snapshot
            .summary
            .insert(OPERATION_ID_KEY.into(), identity.operation_id.to_string());
        snapshot
            .summary
            .insert(INPUT_DIGEST_KEY.into(), hex_digest(identity.input_digest));
        snapshot.summary.insert(
            MANIFEST_LIST_DIGEST_KEY.into(),
            hex_digest(manifest_list_digest),
        );
        let snapshot_id = snapshot.snapshot_id;
        let requirements = [
            TableRequirement::AssertTableUUID {
                uuid: identity.table_uuid,
            },
            TableRequirement::AssertRefSnapshotId {
                r#ref: "main".into(),
                snapshot_id: expected_parent,
            },
            TableRequirement::AssertCurrentSchemaId {
                current_schema_id: base.current_schema_id,
            },
            TableRequirement::AssertDefaultSpecId {
                default_spec_id: i64::from(spec_id),
            },
        ];
        let updates = [
            TableUpdate::AddSnapshot { snapshot },
            TableUpdate::SetSnapshotRef {
                ref_name: "main".into(),
                kind: RefKind::Branch,
                snapshot_id,
                min_snapshots_to_keep: main
                    .and_then(|reference| reference.min_snapshots_to_keep)
                    .map(i64::from),
                max_snapshot_age_ms: main.and_then(|reference| reference.max_snapshot_age_ms),
            },
        ];
        let payload = serialize_commit(&requirements, &updates)?;
        if payload.len() > MAX_STAGED_REQUEST_BYTES {
            return Err(RestError::Decode(
                "staged append request exceeds 64 KiB".into(),
            ));
        }
        let prefix = self.prefix.lock().expect("prefix mutex poisoned").clone();
        Ok(PreparedPublication {
            catalog: self.clone(),
            url: self.commit_url(namespace, table, prefix.as_deref()),
            prefix,
            expected_parent,
            snapshot_id,
            payload,
        })
    }

    /// Send one commit proposal. Only a 409 carrying a catalog requirement
    /// failure permits replanning. Ambiguous outcomes require reconciliation.
    pub fn commit_table(
        &self,
        namespace: &[&str],
        table: &str,
        requirements: &[TableRequirement],
        updates: &[TableUpdate],
    ) -> Result<CommitTableResponse, RestError> {
        let prefix = self.prefix.lock().expect("prefix mutex poisoned").clone();
        let url = self.commit_url(namespace, table, prefix.as_deref());
        self.post_commit_payload(&url, serialize_commit(requirements, updates)?)
    }

    fn commit_url(&self, namespace: &[&str], table: &str, prefix: Option<&str>) -> String {
        let ns = namespace
            .iter()
            .map(|c| urlencoding::encode(c))
            .collect::<Vec<_>>()
            .join("%1F");
        format!(
            "{}/v1{}/namespaces/{ns}/tables/{}",
            self.endpoint,
            prefix.map_or_default(|p| format!("/{p}")),
            urlencoding::encode(table),
        )
    }

    fn post_commit_payload(
        &self,
        url: &str,
        payload: Vec<u8>,
    ) -> Result<CommitTableResponse, RestError> {
        let token = self
            .tokens
            .token()
            .map_err(|e| RestError::Http(format!("token provider: {e}")))?;
        let resp = self
            .commit_client()?
            .post(url)
            .bearer_auth(token)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(payload)
            .send()
            .map_err(|e| RestError::CommitStateUnknown(e.without_url().to_string()))?;
        let status = resp.status();
        if status == reqwest::StatusCode::CONFLICT {
            if let Ok(ErrorWire {
                error: Some(detail),
            }) = resp.json::<ErrorWire>()
            {
                if detail.kind == "CommitFailedException" {
                    return Err(RestError::Conflict(detail.message));
                }
            }
            return Err(RestError::CommitStateUnknown(
                "unrecognized HTTP 409 response".into(),
            ));
        }
        if status.is_server_error()
            || status.is_redirection()
            || status == reqwest::StatusCode::REQUEST_TIMEOUT
        {
            return Err(RestError::CommitStateUnknown(format!(
                "commitTable returned {status}"
            )));
        }
        if !status.is_success() {
            return Err(RestError::Http(format!(
                "POST {} -> {status}",
                super::rest::redact_url(url)
            )));
        }
        resp.json()
            .map_err(|e| RestError::CommitStateUnknown(e.without_url().to_string()))
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
            match self.commit_table(namespace, table, &requirements, &updates) {
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

    /// The plan closure doc 07 §2 prescribes: `AssertTableUUID` +
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
            .commit_table(&["analytics"], "events", &requirements, &updates)
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

    /// 409 + `CommitFailedException` classifies as Conflict (retryable),
    /// carrying the server's requirement message for the ERROR report.
    #[test]
    fn commit_table_409_is_conflict() {
        let mock = Mock::new(vec![(409, conflict_body().leak())]);
        let (requirements, updates) = append_plan(&load_result(100));
        let err = mock
            .catalog()
            .commit_table(&["analytics"], "events", &requirements, &updates)
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

    #[test]
    fn accepted_commit_with_lost_reply_is_unknown() {
        let server = std::sync::Arc::new(tiny_http::Server::http("127.0.0.1:0").unwrap());
        let endpoint = format!("http://{}", server.server_addr());
        let listener = server.clone();
        let worker = std::thread::spawn(move || {
            let mut head = 100;
            let mut posts = 0;
            for mut req in listener.incoming_requests() {
                let response = if req.method() == &tiny_http::Method::Post {
                    let body: serde_json::Value = serde_json::from_reader(req.as_reader()).unwrap();
                    assert_eq!(body["requirements"][1]["snapshot-id"], head);
                    head = body["updates"][0]["snapshot"]["snapshot-id"]
                        .as_i64()
                        .unwrap();
                    posts += 1;
                    // The catalog has installed the snapshot. Its gateway times out
                    // before the caller receives the commit response.
                    tiny_http::Response::from_string("").with_status_code(504)
                } else {
                    tiny_http::Response::from_string(
                        serde_json::to_string(&load_result(head)).unwrap(),
                    )
                };
                req.respond(response).unwrap();
            }
            posts
        });
        let catalog = RestCatalog::new(&endpoint, Box::new(StaticToken(String::new()))).unwrap();
        let result =
            catalog.commit_with_retry(&["ns"], "t", |t| Ok(append_plan(t)), 3, Duration::ZERO);
        let observed = catalog.load_table(&["ns"], "t").unwrap();
        server.unblock();
        assert_eq!(worker.join().unwrap(), 1);
        assert_eq!(observed.metadata.unwrap().current_snapshot_id, Some(101));
        assert!(matches!(result, Err(RestError::CommitStateUnknown(_))));
    }

    #[test]
    fn prepared_append_sends_exact_bytes_once() {
        let mock = Mock::new(vec![(504, "")]);
        let catalog = mock.catalog();
        let mut loaded = load_result(100).metadata.unwrap();
        loaded
            .partition_specs
            .push(crate::catalog::metadata::PartitionSpec {
                spec_id: 0,
                fields: vec![],
            });
        let operation = uuid::Uuid::from_u128(39);
        let identity = PublicationIdentity {
            operation_id: operation,
            table_uuid: loaded.table_uuid,
            input_digest: [0xab; 32],
        };
        let mut proposed = snap(101, Some(100));
        proposed.sequence_number = Some(2);
        let prepared = catalog
            .prepare_staged_append(
                &["my ns", "nested"],
                "my table",
                &loaded,
                proposed,
                identity,
                [0; 32],
            )
            .unwrap();
        assert_eq!(mock.requests().len(), 0);
        assert_eq!(prepared.expected_parent(), Some(100));
        assert_eq!(prepared.snapshot_id(), 101);
        let body = prepared.request_body().to_vec();
        assert_eq!(
            prepared.request_digest().as_slice(),
            ring::digest::digest(&ring::digest::SHA256, &body).as_ref()
        );
        let result = prepared.submit();
        mock.join();
        assert!(matches!(result, Err(RestError::CommitStateUnknown(_))));
        let received = mock.requests();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].0, "POST");
        assert!(received[0]
            .1
            .ends_with("/namespaces/my%20ns%1Fnested/tables/my%20table"));
        assert_eq!(received[0].2.as_bytes(), body);
        let wire: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            wire["requirements"][0]["uuid"],
            identity.table_uuid.to_string()
        );
        assert_eq!(wire["requirements"][1]["snapshot-id"], 100);
        assert_eq!(wire["requirements"][2]["type"], "assert-current-schema-id");
        assert_eq!(wire["requirements"][3]["type"], "assert-default-spec-id");
        assert_eq!(
            wire["updates"][0]["snapshot"]["summary"]["pgdatalake.operation-id"],
            operation.to_string()
        );
        assert_eq!(
            wire["updates"][0]["snapshot"]["summary"]["pgdatalake.input-sha256"],
            "ab".repeat(32)
        );
        assert_eq!(wire["updates"][1]["snapshot-id"], 101);
    }

    #[test]
    fn staged_lookup_recovers_one_accepted_append_after_lost_reply() {
        use crate::catalog::publication::{
            sha256, AuthorizedPublication, PublicationLookup, PublicationStorage,
        };

        let server = std::sync::Arc::new(tiny_http::Server::http("127.0.0.1:0").unwrap());
        let endpoint = format!("http://{}", server.server_addr());
        let mut loaded = load_result(100);
        let base = loaded.metadata.as_mut().unwrap();
        base.partition_specs
            .push(crate::catalog::metadata::PartitionSpec {
                spec_id: 0,
                fields: vec![],
            });
        crate::storage::test_allow_local_stores();
        let local = tempfile::tempdir().unwrap();
        let table_root = format!("file://{}", local.path().display());
        base.location.clone_from(&table_root);
        let identity = PublicationIdentity {
            operation_id: uuid::Uuid::from_u128(42),
            table_uuid: base.table_uuid,
            input_digest: [0x31; 32],
        };
        let mut proposed = snap(101, Some(100));
        proposed.sequence_number = Some(2);
        let manifest = b"sealed manifest list";
        let relative_manifest = "metadata/manifests/snap-101-m0.avro";
        proposed.manifest_list = format!("{table_root}/{relative_manifest}");
        std::fs::create_dir_all(local.path().join("metadata/manifests")).unwrap();
        std::fs::write(local.path().join(relative_manifest), manifest).unwrap();
        let catalog = RestCatalog::new(&endpoint, Box::new(StaticToken(String::new()))).unwrap();
        let prepared = catalog
            .prepare_staged_append(&["ns"], "t", base, proposed, identity, sha256(manifest))
            .unwrap();
        let body = prepared.request_body().to_vec();
        let authorized = AuthorizedPublication {
            identity,
            expected_parent: Some(100),
            snapshot_id: 101,
            request_digest: prepared.request_digest(),
            request_body: &body,
        };

        let listener = server.clone();
        let worker = std::thread::spawn(move || {
            let mut posts = 0;
            let mut reads = 0;
            for mut req in listener.incoming_requests() {
                let response = if req.method() == &tiny_http::Method::Post {
                    let body: serde_json::Value = serde_json::from_reader(req.as_reader()).unwrap();
                    let snapshot =
                        serde_json::from_value(body["updates"][0]["snapshot"].clone()).unwrap();
                    let md = loaded.metadata.as_mut().unwrap();
                    let mut later = snap(102, Some(101));
                    later.sequence_number = Some(3);
                    md.snapshots.extend([snapshot, later]);
                    md.refs.get_mut("main").unwrap().snapshot_id = 102;
                    md.current_snapshot_id = Some(102);
                    posts += 1;
                    tiny_http::Response::from_string("").with_status_code(504)
                } else {
                    reads += 1;
                    tiny_http::Response::from_string(serde_json::to_string(&loaded).unwrap())
                };
                req.respond(response).unwrap();
            }
            (posts, reads)
        });

        // The original request can still arrive after an inconclusive lookup.
        assert_eq!(
            catalog
                .lookup_staged_append(&["ns"], "t", authorized, |_| panic!(
                    "unexpected manifest read"
                ))
                .unwrap(),
            PublicationLookup::Unresolved
        );
        assert!(matches!(
            prepared.submit(),
            Err(RestError::CommitStateUnknown(_))
        ));
        let credentials = crate::storage::S3Options::default();
        let bound = PublicationStorage {
            table_root: &table_root,
            s3: &credentials,
            max_bytes: 64,
            read_timeout: Duration::from_secs(2),
        };
        let wrong = PublicationStorage {
            table_root: "file:///other/table",
            ..bound
        };
        assert!(matches!(
            catalog.lookup_staged_append_on_storage(&["ns"], "t", authorized, &wrong),
            Err(RestError::Decode(_))
        ));
        let result = catalog.lookup_staged_append_on_storage(&["ns"], "t", authorized, &bound);
        std::fs::write(
            local.path().join(relative_manifest),
            b"changed manifest list",
        )
        .unwrap();
        assert_eq!(
            catalog
                .lookup_staged_append_on_storage(&["ns"], "t", authorized, &bound)
                .unwrap(),
            PublicationLookup::Unresolved
        );
        server.unblock();
        assert_eq!(worker.join().unwrap(), (1, 4));
        assert_eq!(
            result.unwrap(),
            PublicationLookup::Accepted { snapshot_id: 101 }
        );
    }

    #[test]
    fn staged_lookup_keeps_nonmatching_evidence_unresolved() {
        use crate::catalog::publication::{sha256, AuthorizedPublication, PublicationLookup};

        let mock = Mock::new(vec![]);
        let catalog = mock.catalog();
        let mut base = load_result(100).metadata.unwrap();
        base.partition_specs
            .push(crate::catalog::metadata::PartitionSpec {
                spec_id: 0,
                fields: vec![],
            });
        let identity = PublicationIdentity {
            operation_id: uuid::Uuid::from_u128(43),
            table_uuid: base.table_uuid,
            input_digest: [0; 32],
        };
        let manifest = b"sealed manifest list";
        let mut snapshot = snap(101, Some(100));
        snapshot.sequence_number = Some(2);
        let prepared = catalog
            .prepare_staged_append(&["ns"], "t", &base, snapshot, identity, sha256(manifest))
            .unwrap();
        let authorized = AuthorizedPublication {
            identity,
            expected_parent: Some(100),
            snapshot_id: 101,
            request_digest: prepared.request_digest(),
            request_body: prepared.request_body(),
        };
        let wire: serde_json::Value = serde_json::from_slice(prepared.request_body()).unwrap();
        let matching: Snapshot =
            serde_json::from_value(wire["updates"][0]["snapshot"].clone()).unwrap();
        let retained = authorized.decode().unwrap();
        base.snapshots.push(matching.clone()); // Present in metadata, unreachable from main.
        assert_eq!(
            retained
                .verify(&base, |_| panic!("unexpected manifest read"))
                .unwrap(),
            PublicationLookup::Unresolved
        );
        base.current_snapshot_id = Some(102);
        base.refs.get_mut("main").unwrap().snapshot_id = 102;
        let mut later = snap(102, Some(101));
        later.sequence_number = Some(3);
        base.snapshots.push(later);
        assert_eq!(
            retained
                .verify(&base, |_| Ok(sha256(b"different contents")))
                .unwrap(),
            PublicationLookup::Unresolved
        );
        let verified_table = base.table_uuid;
        base.table_uuid = uuid::Uuid::from_u128(99);
        assert_eq!(
            retained
                .verify(&base, |_| panic!("unexpected manifest read"))
                .unwrap(),
            PublicationLookup::Unresolved
        );
        base.table_uuid = verified_table;
        let mut duplicate = snap(103, Some(102));
        duplicate.summary = matching.summary;
        base.snapshots.push(duplicate);
        assert_eq!(
            retained
                .verify(&base, |_| panic!("unexpected manifest read"))
                .unwrap(),
            PublicationLookup::Unresolved
        );
        base.snapshots.pop();
        let md_snapshot = base
            .snapshots
            .iter_mut()
            .find(|s| s.snapshot_id == 101)
            .unwrap();
        md_snapshot
            .summary
            .insert("pgdatalake.input-sha256".into(), "bad".into());
        assert_eq!(
            retained
                .verify(&base, |_| panic!("unexpected manifest read"))
                .unwrap(),
            PublicationLookup::Unresolved
        );
        // A mismatched retained digest is rejected before the catalog lookup.
        assert_eq!(
            catalog
                .lookup_staged_append(
                    &["ns"],
                    "t",
                    AuthorizedPublication {
                        request_digest: [1; 32],
                        ..authorized
                    },
                    |_| panic!("unexpected manifest read")
                )
                .unwrap(),
            PublicationLookup::Unresolved
        );
        mock.join();
        assert_eq!(mock.requests().len(), 0);
    }

    #[test]
    fn prepared_append_rejects_unretainable_requests() {
        let mock = Mock::new(vec![]);
        let catalog = mock.catalog();
        let mut loaded = load_result(100).metadata.unwrap();
        loaded
            .partition_specs
            .push(crate::catalog::metadata::PartitionSpec {
                spec_id: 0,
                fields: vec![],
            });
        let identity = PublicationIdentity {
            operation_id: uuid::Uuid::from_u128(42),
            table_uuid: loaded.table_uuid,
            input_digest: [0; 32],
        };
        let mut snapshot = snap(101, Some(100));
        snapshot.sequence_number = Some(2);
        let mut oversized = snapshot.clone();
        oversized.summary.insert("extra".into(), " ".repeat(65536));
        assert!(matches!(
            catalog.prepare_staged_append(&["ns"], "t", &loaded, oversized, identity, [0; 32]),
            Err(RestError::Decode(_))
        ));

        loaded.location = "s3://user:password@w/t".into();
        snapshot.manifest_list = format!("{}/metadata/snap.avro", loaded.location);
        assert!(matches!(
            catalog.prepare_staged_append(&["ns"], "t", &loaded, snapshot, identity, [0; 32]),
            Err(RestError::Decode(_))
        ));
        mock.join();
        assert_eq!(mock.requests().len(), 0);
    }

    #[test]
    fn prepared_append_rejects_wrong_binding_and_keeps_an_empty_main_pin() {
        let mock = Mock::new(vec![]);
        let catalog = mock.catalog();
        let mut loaded = load_result(100).metadata.unwrap();
        loaded
            .partition_specs
            .push(crate::catalog::metadata::PartitionSpec {
                spec_id: 0,
                fields: vec![],
            });
        let mut identity = PublicationIdentity {
            operation_id: uuid::Uuid::from_u128(40),
            table_uuid: uuid::Uuid::from_u128(1),
            input_digest: [0; 32],
        };
        assert!(matches!(
            catalog.prepare_staged_append(
                &["ns"],
                "t",
                &loaded,
                snap(101, Some(100)),
                identity,
                [0; 32]
            ),
            Err(RestError::Conflict(_))
        ));
        identity.table_uuid = loaded.table_uuid;
        let mut proposed = snap(101, Some(100));
        proposed.sequence_number = Some(2);
        for malformed in [
            Snapshot {
                parent_snapshot_id: Some(99),
                ..proposed.clone()
            },
            Snapshot {
                schema_id: Some(2),
                ..proposed.clone()
            },
            Snapshot {
                manifest_list: "s3://other/table/metadata/foreign.avro".into(),
                ..proposed.clone()
            },
            Snapshot {
                manifest_list: "s3://w/t/metadata/../other.avro".into(),
                ..proposed
            },
        ] {
            assert!(catalog
                .prepare_staged_append(&["ns"], "t", &loaded, malformed, identity, [0; 32])
                .is_err());
        }
        loaded.refs.get_mut("main").unwrap().snapshot_id = 99;
        assert!(catalog
            .prepare_staged_append(
                &["ns"],
                "t",
                &loaded,
                snap(101, Some(100)),
                identity,
                [0; 32]
            )
            .is_err());
        loaded.snapshots.clear();
        loaded.refs.clear();
        loaded.current_snapshot_id = None;
        loaded.last_sequence_number = Some(0);
        let prepared = catalog
            .prepare_staged_append(&["ns"], "t", &loaded, snap(101, None), identity, [0; 32])
            .unwrap();
        assert_eq!(prepared.expected_parent(), None);
        let body: serde_json::Value = serde_json::from_slice(prepared.request_body()).unwrap();
        assert_eq!(
            body["requirements"][1]["snapshot-id"],
            serde_json::Value::Null
        );
        assert!(body["requirements"][1].get("snapshot-id").is_some());
        // A route change invalidates the original scope before network dispatch.
        *catalog.prefix.lock().unwrap() = Some("another-warehouse".into());
        assert!(matches!(prepared.submit(), Err(RestError::Http(_))));
        mock.join();
        assert_eq!(mock.requests().len(), 0);
    }

    /// A server failure after dispatch leaves the result unresolved.
    #[test]
    fn commit_table_500_is_unknown() {
        let mock = Mock::new(vec![(
            500,
            r#"{"error": {"message": "boom", "type": "RESTException", "code": 500}}"#,
        )]);
        let (requirements, updates) = append_plan(&load_result(100));
        let err = mock
            .catalog()
            .commit_table(&["analytics"], "events", &requirements, &updates)
            .unwrap_err();
        mock.join();
        assert!(matches!(err, RestError::CommitStateUnknown(_)), "{err}");
    }

    #[test]
    fn unrecognized_conflict_response_is_unknown() {
        let mock = Mock::new(vec![(409, "{}")]);
        let (requirements, updates) = append_plan(&load_result(100));
        let result = mock
            .catalog()
            .commit_table(&["ns"], "t", &requirements, &updates);
        mock.join();
        assert!(matches!(result, Err(RestError::CommitStateUnknown(_))));
    }

    #[test]
    fn malformed_successful_commit_response_is_unknown() {
        let mock = Mock::new(vec![(200, "{")]);
        let (requirements, updates) = append_plan(&load_result(100));
        let result = mock
            .catalog()
            .commit_table(&["ns"], "t", &requirements, &updates);
        mock.join();
        assert!(matches!(result, Err(RestError::CommitStateUnknown(_))));
    }

    #[test]
    fn commit_does_not_follow_a_same_origin_redirect() {
        let server = std::sync::Arc::new(tiny_http::Server::http("127.0.0.1:0").unwrap());
        let endpoint = format!("http://{}", server.server_addr());
        let listener = server.clone();
        let worker = std::thread::spawn(move || {
            let mut requests = 0;
            for req in listener.incoming_requests() {
                requests += 1;
                let response = if requests == 1 {
                    tiny_http::Response::from_string("")
                        .with_status_code(307)
                        .with_header(
                            tiny_http::Header::from_bytes("Location", "/another-route").unwrap(),
                        )
                } else {
                    tiny_http::Response::from_string(commit_ok_body())
                };
                req.respond(response).unwrap();
            }
            requests
        });
        let catalog = RestCatalog::new(&endpoint, Box::new(StaticToken(String::new()))).unwrap();
        let (requirements, updates) = append_plan(&load_result(100));
        let result = catalog.commit_table(&["ns"], "t", &requirements, &updates);
        server.unblock();
        assert_eq!(worker.join().unwrap(), 1);
        assert!(matches!(result, Err(RestError::CommitStateUnknown(_))));
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

    /// `AssertRefSnapshotId` with `snapshot_id=None` serializes an explicit
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
