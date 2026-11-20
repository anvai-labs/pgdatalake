//! Catalog scan session: the one-call orchestrator the FFI/FDW face uses —
//! config -> loadTable -> metadata fetch -> plan_scan (specs/lakehouse/
//! 03-catalog.md, 06-execution.md). Slice 9.

//! Catalog scan session: the one-call orchestrator the FFI/FDW face uses
//! (specs/lakehouse/03-catalog.md §2, doc 06 §2). Slice 9.
//!
//! Holds one REST catalog handle + storage credentials for the warehouse's
//! object store; `scan_table()` walks config -> loadTable -> metadata fetch
//! -> plan_scan. Credentialed OAuth2 providers plug in via TokenProvider
//! (slice 2); the FFI face (ffi.rs) wraps this with panic-free boundaries.

use crate::catalog::rest::{RestCatalog, StaticToken, TokenProvider};
use crate::catalog::types::CatalogConfig;
use crate::scan::{ScanPlan, ScanPredicate};
use crate::storage::{fetch_metadata, S3Options};

/// Client-side storage credentials/overrides (from secure Postgres settings;
/// catalog-served `s3.*` config keys are merged under these, doc 03 §2).
#[derive(Debug, Clone, Default)]
pub struct S3ClientOptions {
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub path_style: bool,
}

impl S3ClientOptions {
    fn merged(&self, catalog_cfg: &CatalogConfig) -> S3Options {
        // catalog config first (defaults + overrides), client settings win
        let mut effective = catalog_cfg.defaults.clone();
        effective.extend(catalog_cfg.overrides.clone());
        let mut opts = S3Options::from_config(&effective);
        if let Some(ep) = &self.endpoint {
            opts.endpoint = Some(ep.clone());
        }
        if let Some(r) = &self.region {
            opts.region = Some(r.clone());
        }
        if let Some(k) = &self.access_key_id {
            opts.access_key_id = Some(k.clone());
        }
        if let Some(sec) = &self.secret_access_key {
            opts.secret_access_key = Some(sec.clone());
        }
        if self.path_style {
            opts.path_style = true;
        }
        opts
    }
}

/// Everything an append commit needs about the target table, resolved at
/// open time (doc 07 §2: requirements come from THIS base; conflict =>
/// retry re-pins).
pub struct TableWriteState {
    pub table_uuid: uuid::Uuid,
    pub base_snapshot_id: Option<i64>,
    pub last_sequence_number: Option<i64>,
    /// manifest-list key of the BASE snapshot (blocker #1: appends carry
    /// its live manifests forward)
    pub base_manifest_list: Option<String>,
    pub location: String,
    pub schema_id: Option<i64>,
    pub partition_spec_id: i32,
    pub s3: S3Options,
    pub field_ids: std::collections::BTreeMap<String, i64>,
}

pub struct CatalogClient {
    rest: RestCatalog,
    warehouse: String,
    s3_client: S3ClientOptions,
}

impl CatalogClient {
    pub fn new(
        endpoint: &str,
        warehouse: &str,
        token: &str,
        s3_client: &S3ClientOptions,
    ) -> Result<Self, crate::storage::StorageError> {
        Self::with_provider(
            endpoint,
            warehouse,
            Box::new(StaticToken(token.to_string())),
            s3_client,
        )
    }

    pub fn with_provider(
        endpoint: &str,
        warehouse: &str,
        tokens: Box<dyn TokenProvider>,
        s3_client: &S3ClientOptions,
    ) -> Result<Self, crate::storage::StorageError> {
        Ok(Self {
            rest: RestCatalog::new(endpoint, tokens)
                .map_err(|e| crate::storage::StorageError::Config(e.to_string()))?,
            warehouse: warehouse.to_string(),
            s3_client: s3_client.clone(),
        })
    }

    /// Open write state for a managed append (M2 slice 27b): config ->
    /// loadTable -> metadata.json, resolving the table's identity (uuid,
    /// base snapshot, location, partition spec) and the current schema's
    /// field-ids by column name (the manifest metric maps are keyed by
    /// field id, so the write schema must map onto the table's schema).
    pub fn write_table_open(
        &self,
        namespace: &[&str],
        table: &str,
        columns: &[(String, crate::catalog::metadata::IcebergType)],
    ) -> Result<TableWriteState, crate::storage::StorageError> {
        let cfg = self
            .rest
            .fetch_config(Some(&self.warehouse))
            .map_err(|e| crate::storage::StorageError::Config(e.to_string()))?;
        let lt = self
            .rest
            .load_table(namespace, table)
            .map_err(|e| crate::storage::StorageError::Config(e.to_string()))?;
        let s3 = self.s3_client.merged(&cfg);
        let (io, key) = crate::scan::open_for(&lt.metadata_location, Some(&s3))?;
        let md = crate::storage::fetch_metadata(&io, &key)?;
        // writer capability gates (13-implementation-review blocker #2:
        // "Initially allow only write formats and unpartitioned schemas
        // the writer can actually emit")
        match md.format_version {
            crate::catalog::metadata::FormatVersion::V2 => {}
            other => {
                return Err(crate::storage::StorageError::Config(format!(
                    "write path: table {table} is format-version {:?}; this writer emits v2",
                    other
                )))
            }
        }
        if let Some(spec) = md
            .default_partition_spec_id
            .and_then(|id| md.partition_spec(id))
        {
            if !spec.fields.is_empty() {
                return Err(crate::storage::StorageError::Config(format!(
                    "write path: table {table} is partitioned; this writer emits unpartitioned tables only"
                )));
            }
        }
        let schema = md.current_schema().ok_or_else(|| {
            crate::storage::StorageError::Config(format!(
                "write path: table {table} has no current schema"
            ))
        })?;
        let mut field_ids = std::collections::BTreeMap::new();
        for (name, _) in columns {
            let field = schema
                .fields
                .iter()
                .find(|f| &f.name == name)
                .ok_or_else(|| {
                    crate::storage::StorageError::Config(format!(
                        "write path: column {name} is not in table {table}'s schema"
                    ))
                })?;
            field_ids.insert(name.clone(), field.id);
        }
        let base_manifest_list = md.current_snapshot().map(|snap| snap.manifest_list.clone());
        Ok(TableWriteState {
            table_uuid: md.table_uuid,
            base_snapshot_id: md.current_snapshot_id,
            last_sequence_number: md.last_sequence_number,
            base_manifest_list,
            location: md.location.clone(),
            schema_id: Some(md.current_schema_id),
            partition_spec_id: md.default_partition_spec_id.unwrap_or(0),
            s3,
            field_ids,
        })
    }

    /// Flush + assemble + COMMIT an append snapshot (doc 07 §2 steps 1-3,
    /// rebuilt per 13-implementation-review blocker #2): EVERY attempt
    /// reloads metadata, binds the requirement to the ORIGINAL table
    /// uuid (a replacement table aborts, never gets adopted), re-derives
    /// parent/sequence/carried manifests from the fresh base, assembles
    /// a fresh snapshot with a collision-resistant id, and never replays
    /// stale requirements. Fatal assembly errors abort without retry.
    /// `deleted` (M2 slice 28 COW): files this snapshot removes, emitted
    /// as DELETED manifest entries (operation=overwrite). Every retry
    /// attempt RE-VERIFIES each deleted path is still live in the fresh
    /// base — a concurrent writer that rewrote the same file aborts the
    /// commit (loud lost-update protection), never silently re-deletes
    /// against a rewritten file.
    pub fn commit_append(
        &self,
        st: &TableWriteState,
        namespace: &[&str],
        table: &str,
        new_files: &[crate::snapshot_write::NewDataFile],
        deleted: &[crate::snapshot_write::DeletedDataFile],
        retries: u32,
    ) -> Result<String, crate::storage::StorageError> {
        use crate::catalog::commit::{TableRequirement, TableUpdate};
        let loc = crate::storage::parse_location(&st.location)?;
        let io = crate::storage::StorageIo::open(&loc, Some(&st.s3))?;

        let resp = self
            .rest
            .commit_with_retry(
                namespace,
                table,
                |lt| {
                    // the FRESH base is authoritative per attempt; a
                    // table replaced under us has a different uuid and
                    // the requirement fails server-side (never adopted)
                    // verified current metadata per attempt: in-band when
                    // the catalog serves it, else fetch the referenced
                    // metadata file (13-review blocker #2)
                    let md_owned;
                    let md = match lt.metadata.as_ref() {
                        Some(m) => m,
                        None => {
                            let (mio, mkey) =
                                crate::scan::open_for(&lt.metadata_location, Some(&st.s3))
                                    .map_err(|e| {
                                        crate::catalog::rest::RestError::Http(e.to_string())
                                    })?;
                            md_owned =
                                crate::storage::fetch_metadata(&mio, &mkey).map_err(|e| {
                                    crate::catalog::rest::RestError::Http(e.to_string())
                                })?;
                            &md_owned
                        }
                    };
                    let base_seq = md.last_sequence_number;
                    let parent = md.current_snapshot_id;
                    let base_list = md.current_snapshot().map(|snap| snap.manifest_list.clone());

                    // blocker #1: carry the fresh base's live manifests
                    let carried = match &base_list {
                        Some(path) => {
                            let (cio, ckey) =
                                crate::scan::open_for(path, Some(&st.s3)).map_err(|e| {
                                    crate::catalog::rest::RestError::Http(e.to_string())
                                })?;
                            crate::manifest::parse_manifest_list(&cio.get_bytes(&ckey).map_err(
                                |e| crate::catalog::rest::RestError::Http(e.to_string()),
                            )?)
                            .map_err(|e| crate::catalog::rest::RestError::Http(e.to_string()))?
                        }
                        None => Vec::new(),
                    };

                    // slice 28: lost-update protection — every deleted
                    // path must still be LIVE in the fresh base (plan_scan
                    // applies the cross-manifest DELETED exclusion, so a
                    // path another writer already removed is absent here)
                    if !deleted.is_empty() {
                        let plan = crate::scan::plan_scan(md, None, Some(&st.s3), None)
                            .map_err(|e| {
                                crate::catalog::rest::RestError::Http(e.to_string())
                            })?;
                        let live: std::collections::HashSet<&str> = plan
                            .tasks
                            .iter()
                            .map(|t| t.entry.data_file.file_path.as_str())
                            .collect();
                        for d in deleted {
                            if !live.contains(d.file_path.as_str()) {
                                return Err(crate::catalog::rest::RestError::Http(format!(
                                    "overwrite conflict: file {} is no longer live in the table's current snapshot (a concurrent writer removed or rewrote it); aborting to avoid a lost update",
                                    d.file_path
                                )));
                            }
                        }
                    }

                    // collision-resistant positive id (entropy + time),
                    // NOT sequence-derived (two writers can share a seq)
                    let snapshot_id = {
                        let u = uuid::Uuid::new_v4().as_u128();
                        let ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as i64)
                            .unwrap_or(1);
                        ((ms << 20) | ((u >> 44) as i64 & 0xF_FFFF)) & i64::MAX
                    };

                    let written = crate::snapshot_write::write_snapshot(
                        &io,
                        &st.location,
                        new_files,
                        crate::snapshot_write::SnapshotParams {
                            snapshot_id,
                            timestamp_ms: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_millis() as i64)
                                .unwrap_or(0),
                            parent_snapshot_id: parent,
                            last_sequence_number: base_seq,
                            schema_id: st.schema_id,
                            partition_spec_id: st.partition_spec_id,
                            carried_manifests: carried,
                            deleted_files: deleted.to_vec(),
                        },
                    )
                    .map_err(|e| crate::catalog::rest::RestError::Http(e.to_string()))?;

                    let updates = vec![
                        TableUpdate::AddSnapshot {
                            snapshot: written.snapshot.clone(),
                        },
                        TableUpdate::SetSnapshotRef {
                            ref_name: "main".into(),
                            kind: crate::catalog::commit::RefKind::Branch,
                            snapshot_id,
                            min_snapshots_to_keep: None,
                            max_snapshot_age_ms: None,
                        },
                    ];
                    Ok((
                        vec![
                            TableRequirement::AssertTableUUID {
                                uuid: st.table_uuid,
                            },
                            TableRequirement::AssertRefSnapshotId {
                                r#ref: "main".into(),
                                snapshot_id: parent,
                            },
                        ],
                        updates,
                    ))
                },
                retries,
                std::time::Duration::from_millis(50),
            )
            .map_err(|e| crate::storage::StorageError::Config(e.to_string()))?;
        Ok(resp.metadata_location)
    }

    /// Create the remote Iceberg table (managed CREATE, slice 27c):
    /// namespace created if missing, then createTable with a schema built
    /// from the column list (field ids 1..n, all optional columns —
    /// Postgres-side constraints are enforced locally). Returns the
    /// metadata-location the server generated.
    pub fn create_table(
        &self,
        namespace: &[&str],
        table: &str,
        columns: &[(String, crate::catalog::metadata::IcebergType)],
    ) -> Result<String, crate::storage::StorageError> {
        use crate::catalog::metadata::{Field, IcebergTypeMarker, Schema};
        let cfg = self
            .rest
            .fetch_config(Some(&self.warehouse))
            .map_err(|e| crate::storage::StorageError::Config(e.to_string()))?;
        let _ = cfg;
        // namespace upsert (REST: create + tolerate already-exists 409)
        if let Err(e) = self.rest.create_namespace(namespace) {
            match e {
                crate::catalog::rest::RestError::Conflict(_) => {}
                other => return Err(crate::storage::StorageError::Config(other.to_string())),
            }
        }
        let fields = columns
            .iter()
            .enumerate()
            .map(|(i, (name, ty))| Field {
                id: (i + 1) as i64,
                name: name.clone(),
                required: false,
                r#type: ty.clone(),
                doc: None,
            })
            .collect();
        let schema = Schema {
            struct_type: IcebergTypeMarker::Struct,
            schema_id: 0,
            fields,
        };
        let resp: super::catalog::types::LoadTableResult = self
            .rest
            .create_table(namespace, table, &schema)
            .map_err(|e| match e {
                crate::catalog::rest::RestError::Conflict(m) => {
                    crate::storage::StorageError::Conflict(m)
                }
                other => crate::storage::StorageError::Config(other.to_string()),
            })?;
        Ok(resp.metadata_location)
    }

    /// config -> loadTable -> metadata -> plan. Returns the plan plus the
    /// config -> loadTable -> metadata -> plan. Returns the plan plus the
    /// effective storage options (needed later to open task files).
    pub fn scan_table(
        &self,
        namespace: &[&str],
        table: &str,
        predicate: Option<&ScanPredicate>,
        selector: Option<&crate::catalog::metadata::SnapshotSelector>,
    ) -> Result<(ScanPlan, S3Options, crate::catalog::metadata::Schema), crate::storage::StorageError>
    {
        let cfg = self
            .rest
            .fetch_config(Some(&self.warehouse))
            .map_err(|e| crate::storage::StorageError::Config(e.to_string()))?;
        let lt = self
            .rest
            .load_table(namespace, table)
            .map_err(|e| crate::storage::StorageError::Config(e.to_string()))?;

        let s3 = self.s3_client.merged(&cfg);
        let (io, key) = crate::scan::open_for(&lt.metadata_location, Some(&s3))?;
        let md = fetch_metadata(&io, &key)?;
        // v3 conformance (doc 14): the V0 loud gate is LIFTED (slice V1) —
        // deletion vectors are decoded and applied (puffin.rs), so v3
        // tables read correctly. Row lineage / schema defaults remain
        // deferred-but-inert (they affect writers, not read correctness).
        let plan = crate::scan::plan_scan(&md, selector, Some(&s3), predicate)?;
        let schema = md.current_schema().cloned().ok_or_else(|| {
            crate::storage::StorageError::Config("scan: table has no current schema".to_string())
        })?;
        Ok((plan, s3, schema))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// TDD anchor (M1 slice 9): one client call walks the whole pipeline
    /// against an in-process mock catalog serving config + loadTable, with
    /// the metadata on a local store — no network beyond the loopback mock
    /// (doc 10 §1).
    #[test]
    fn scan_table_walks_config_loadtable_metadata_plan() {
        use apache_avro::{Schema as AvroSchema, Writer};

        let dir = tempfile::tempdir().unwrap();
        crate::storage::test_allow_local_stores();
        std::fs::create_dir_all(dir.path().join("metadata")).unwrap();

        // empty manifest list (fresh table) keeps the fixture tiny
        let schema = AvroSchema::parse_str(crate::manifest::MANIFEST_LIST_SCHEMA_V2).unwrap();
        let writer = Writer::new(&schema, Vec::new());
        std::fs::write(
            dir.path().join("metadata").join("mn-list.avro"),
            writer.into_inner().unwrap(),
        )
        .unwrap();

        let metadata_json = format!(
            r#"{{
                "format-version": 2,
                "table-uuid": "9c12d441-03fe-11ed-8122-d358e75d12eb",
                "location": "file://{}",
                "schemas": [{{"type": "struct", "schema-id": 0, "fields": [
                    {{"id": 1, "name": "id", "required": true, "type": "long"}}]}}],
                "current-schema-id": 0,
                "snapshots": [{{"snapshot-id": 42, "timestamp-ms": 1712345678000,
                                "manifest-list": "file://{}/metadata/mn-list.avro",
                                "summary": {{"operation": "append"}}}}],
                "current-snapshot-id": 42
            }}"#,
            dir.path().display(),
            dir.path().display(),
        );
        let md_file = dir.path().join("metadata").join("00000-x.metadata.json");
        std::fs::write(&md_file, metadata_json).unwrap();

        // mock catalog: GET /v1/config (with warehouse param) then loadTable
        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let addr = server.server_addr().to_string();
        std::thread::spawn(move || {
            let req = server.recv().unwrap();
            let body = r#"{"defaults": {"prefix": "pfx"}, "overrides": {}}"#;
            let resp = tiny_http::Response::from_string(body).with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                    .unwrap(),
            );
            let _ = req.respond(resp);
            let req = server.recv().unwrap();
            assert!(req
                .url()
                .starts_with("/v1/pfx/namespaces/analytics/tables/events"));
            let body = format!(
                r#"{{"metadata-location": "file://{}",
                    "config": {{"s3.endpoint": "http://s3:9000"}}}}"#,
                md_file.display()
            );
            let resp = tiny_http::Response::from_string(body).with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                    .unwrap(),
            );
            let _ = req.respond(resp);
        });

        let client = CatalogClient::new(
            &format!("http://{addr}"),
            "smoke",
            "",
            &S3ClientOptions::default(),
        )
        .unwrap();
        let (plan, _s3, _schema) = client
            .scan_table(&["analytics"], "events", None, None)
            .expect("scan");
        assert_eq!(plan.snapshot_id, 42);
        assert_eq!(plan.tasks.len(), 0, "fresh table: empty manifest list");
    }

    /// v3 conformance (doc 14): the V0 read gate is LIFTED in slice V1 —
    /// deletion vectors are decoded and applied, so format-version 3
    /// tables scan (this fixture: an empty v3 table; the DV-apply matrix
    /// is the Java-written fixture test in scan.rs).
    #[test]
    fn v3_tables_scan_after_gate_lift() {
        let dir = tempfile::tempdir().unwrap();
        crate::storage::test_allow_local_stores();
        std::fs::create_dir_all(dir.path().join("metadata")).unwrap();
        let location = format!("file://{}", dir.path().display());
        let metadata_json = format!(
            r#"{{
                "format-version": 3,
                "table-uuid": "1ca92afa-70b9-4d02-9ba6-7ee1a3ac0f5e",
                "location": "{location}",
                "last-sequence-number": 1,
                "schemas": [{{"type": "struct", "schema-id": 0, "fields": [
                    {{"id": 1, "name": "id", "required": true, "type": "long"}}]}}],
                "current-schema-id": 0,
                "snapshots": []
            }}"#
        );
        let md_file = dir.path().join("metadata").join("00001.metadata.json");
        std::fs::write(&md_file, metadata_json).unwrap();

        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let addr = server.server_addr().to_string();
        let md_path = md_file.clone();
        std::thread::spawn(move || {
            let req = server.recv().unwrap(); // config
            let _ = req.respond(tiny_http::Response::from_string(
                r#"{"defaults": {}, "overrides": {}}"#,
            ));
            let req = server.recv().unwrap(); // loadTable
            let _ = req.respond(tiny_http::Response::from_string(format!(
                r#"{{"metadata-location": "file://{}", "config": {{}}}}"#,
                md_path.display()
            )));
        });

        let client = CatalogClient::new(
            &format!("http://{addr}"),
            "wh",
            "",
            &S3ClientOptions::default(),
        )
        .unwrap();
        let (plan, _s3, _schema) = client
            .scan_table(&["analytics"], "t", None, None)
            .expect("v3 table scans after the gate lift");
        assert_eq!(plan.tasks.len(), 0, "empty v3 table: no tasks");
    }

    /// Unreachable catalogs error with the endpoint in the message (the C
    /// glue maps this to a connection-failure ereport).
    #[test]
    fn unreachable_catalog_errors_clearly() {
        let client = CatalogClient::new(
            "http://127.0.0.1:9", // nothing listens here
            "wh",
            "",
            &S3ClientOptions::default(),
        )
        .unwrap();
        let err = client.scan_table(&["ns"], "t", None, None).unwrap_err();
        assert!(!err.to_string().is_empty());
    }

    // ---- slice 28: overwrite commits (COW UPDATE/DELETE) ----

    /// Local-store fixture: one manifest (optionally holding one ADDED
    /// data file of 3 rows) + a manifest-list, under `dir`. Returns the
    /// table location and the data file's URI.
    fn overwrite_fixture(dir: &std::path::Path, with_data_file: bool) -> (String, String) {
        use crate::snapshot_write::{build_manifest_bytes, build_manifest_list_bytes, NewDataFile};
        crate::storage::test_allow_local_stores();
        let location = format!("file://{}", dir.display());
        let data_path = format!("{location}/data/000-0.parquet");
        let added: Vec<NewDataFile> = if with_data_file {
            vec![NewDataFile {
                file_path: data_path.clone(),
                record_count: 3,
                file_size_in_bytes: 4096,
                ..Default::default()
            }]
        } else {
            Vec::new()
        };
        let manifest_bytes = build_manifest_bytes(&added, &[], 42, 1).unwrap();
        std::fs::create_dir_all(dir.join("metadata")).unwrap();
        std::fs::write(dir.join("metadata").join("m0.avro"), &manifest_bytes).unwrap();
        let list_bytes = build_manifest_list_bytes(&[crate::manifest::ManifestListEntry {
            manifest_path: format!("{location}/metadata/m0.avro"),
            manifest_length: manifest_bytes.len() as i64,
            partition_spec_id: 0,
            content: Some(0),
            added_snapshot_id: 42,
            added_files_count: Some(1),
            existing_files_count: None,
            deleted_files_count: None,
            added_rows_count: Some(3),
            existing_rows_count: None,
            deleted_rows_count: None,
            sequence_number: Some(1),
            min_sequence_number: Some(1),
        }])
        .unwrap();
        std::fs::write(dir.join("metadata").join("snap.avro"), list_bytes).unwrap();
        (location, data_path)
    }

    fn overwrite_metadata_file(dir: &std::path::Path, location: &str) -> String {
        let metadata_json = format!(
            r#"{{
                "format-version": 2,
                "table-uuid": "1ca92afa-70b9-4d02-9ba6-7ee1a3ac0f5e",
                "location": "{location}",
                "last-sequence-number": 1,
                "schemas": [{{"type": "struct", "schema-id": 0, "fields": [
                    {{"id": 1, "name": "id", "required": true, "type": "long"}}]}}],
                "current-schema-id": 0,
                "snapshots": [{{"snapshot-id": 42, "timestamp-ms": 1712345678000,
                                "manifest-list": "{location}/metadata/snap.avro",
                                "summary": {{"operation": "append"}}}}],
                "current-snapshot-id": 42
            }}"#
        );
        let md_file = dir.join("metadata").join("00001-abc.metadata.json");
        std::fs::write(&md_file, metadata_json).unwrap();
        format!("file://{}", md_file.display())
    }

    /// commit_append with a deleted file emits an overwrite snapshot:
    /// the commit request carries operation=overwrite and the deleted
    /// path as a DELETED manifest entry, and the commit succeeds.
    #[test]
    fn commit_append_overwrite_emits_deleted_entries() {
        let dir = tempfile::tempdir().unwrap();
        let (location, data_path) = overwrite_fixture(dir.path(), true);
        let md_loc = overwrite_metadata_file(dir.path(), &location);

        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let addr = server.server_addr().to_string();
        let handle = std::thread::spawn(move || {
            let req = server.recv().unwrap(); // config
            let _ = req.respond(tiny_http::Response::from_string(
                r#"{"defaults": {}, "overrides": {}}"#,
            ));
            let resp_body = format!(r#"{{"metadata-location": "{}", "config": {{}}}}"#, md_loc);
            let req = server.recv().unwrap(); // loadTable (write_table_open)
            let _ = req.respond(tiny_http::Response::from_string(resp_body.clone()));
            let req = server.recv().unwrap(); // loadTable (commit_append)
            let _ = req.respond(tiny_http::Response::from_string(resp_body));
            let mut req = server.recv().unwrap(); // commit — CAPTURE the body
            let mut body = String::new();
            req.as_reader().read_to_string(&mut body).unwrap();
            // the response parser opens metadata-location: keep it local+real
            let next_loc = md_loc.replace("00001-abc", "00002-x");
            let _ = req.respond(tiny_http::Response::from_string(format!(
                r#"{{"metadata-location": "{}", "config": {{}}}}"#,
                next_loc
            )));
            body
        });

        let client = CatalogClient::new(
            &format!("http://{addr}"),
            "wh",
            "",
            &S3ClientOptions::default(),
        )
        .unwrap();
        let st = client
            .write_table_open(
                &["analytics"],
                "events",
                &[("id".into(), crate::catalog::metadata::IcebergType::Long)],
            )
            .unwrap();
        let deleted = vec![crate::snapshot_write::DeletedDataFile {
            file_path: data_path.clone(),
            record_count: 3,
            file_size_in_bytes: 4096,
        }];
        let new_loc = client
            .commit_append(&st, &["analytics"], "events", &[], &deleted, 3)
            .expect("overwrite commit");
        assert!(new_loc.contains("00002-x.metadata.json"));

        let body = handle.join().unwrap();
        assert!(
            body.contains(r#""operation":"overwrite""#)
                || body.contains(r#""operation": "overwrite""#),
            "commit body must say overwrite: {body}"
        );
        // the DELETED entry itself lives in the manifest avro the commit
        // wrote; verify it parses back with the deleted path + counts
        let snap_id: i64 = {
            let v: serde_json::Value = serde_json::from_str(&body).unwrap();
            v["updates"][0]["snapshot"]["snapshot-id"].as_i64().unwrap()
        };
        let list_file = std::fs::read_dir(dir.path().join("metadata"))
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with(&format!("snap-{snap_id}-")))
                    .unwrap_or(false)
            })
            .expect("commit's manifest-list written to the store");
        let list =
            crate::manifest::parse_manifest_list(&std::fs::read(&list_file).unwrap()).unwrap();
        // the carried manifest is REWRITTEN in place (deleted entry flips
        // there); a deletion-only commit writes no new data manifest
        assert_eq!(list.len(), 1, "rewritten carried manifest only: {list:?}");
        assert_eq!(list[0].deleted_files_count, Some(1));
        let manifest_rel = list[0]
            .manifest_path
            .trim_start_matches(&format!("{location}/"));
        let entries =
            crate::manifest::parse_manifest(&std::fs::read(dir.path().join(manifest_rel)).unwrap())
                .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].status,
            crate::manifest::ManifestEntryStatus::Deleted
        );
        assert_eq!(entries[0].data_file.file_path, data_path);
        assert_eq!(entries[0].data_file.record_count, 3);
        assert_eq!(entries[0].data_file.file_size_in_bytes, 4096);
    }

    /// Lost-update protection: if the deleted file is NOT live in the
    /// fresh base (a concurrent writer removed/rewrote it), the commit
    /// aborts BEFORE any commit request is sent.
    #[test]
    fn overwrite_aborts_when_deleted_file_not_live() {
        let dir = tempfile::tempdir().unwrap();
        // fixture WITHOUT the data file: the base's live set is empty
        let (location, _data_path) = overwrite_fixture(dir.path(), false);
        let md_loc = overwrite_metadata_file(dir.path(), &location);

        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let addr = server.server_addr().to_string();
        let handle = std::thread::spawn(move || {
            let req = server.recv().unwrap(); // config
            let _ = req.respond(tiny_http::Response::from_string(
                r#"{"defaults": {}, "overrides": {}}"#,
            ));
            let resp_body = format!(r#"{{"metadata-location": "{}", "config": {{}}}}"#, md_loc);
            let req = server.recv().unwrap(); // loadTable (write_table_open)
            let _ = req.respond(tiny_http::Response::from_string(resp_body.clone()));
            let req = server.recv().unwrap(); // loadTable (commit_append)
            let _ = req.respond(tiny_http::Response::from_string(resp_body));
            // NO commit request may arrive
            if let Ok(Some(extra)) = server.recv_timeout(std::time::Duration::from_secs(1)) {
                panic!(
                    "commit must abort before any commit request; got {}",
                    extra.url()
                );
            }
        });

        let client = CatalogClient::new(
            &format!("http://{addr}"),
            "wh",
            "",
            &S3ClientOptions::default(),
        )
        .unwrap();
        let st = client
            .write_table_open(
                &["analytics"],
                "events",
                &[("id".into(), crate::catalog::metadata::IcebergType::Long)],
            )
            .unwrap();
        let deleted = vec![crate::snapshot_write::DeletedDataFile {
            file_path: format!("{location}/data/000-0.parquet"),
            record_count: 3,
            file_size_in_bytes: 4096,
        }];
        let err = client
            .commit_append(&st, &["analytics"], "events", &[], &deleted, 3)
            .expect_err("must abort");
        let msg = err.to_string();
        assert!(
            msg.contains("no longer live") || msg.contains("conflict"),
            "abort must name the conflict: {msg}"
        );
        handle.join().unwrap();
    }
}
