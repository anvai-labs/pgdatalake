//! Catalog scan session: the one-call orchestrator the FFI/FDW face uses —
//! config -> loadTable -> metadata fetch -> `plan_scan` (specs/lakehouse/
//! 03-catalog.md, 06-execution.md). Slice 9.

//! Catalog scan session: the one-call orchestrator the FFI/FDW face uses
//! (specs/lakehouse/03-catalog.md §2, doc 06 §2). Slice 9.
//!
//! Holds one REST catalog handle + storage credentials for the warehouse's
//! object store; `scan_table()` walks config -> loadTable -> metadata fetch
//! -> `plan_scan`. Credentialed `OAuth2` providers plug in via `TokenProvider`
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
            crate::catalog::metadata::FormatVersion::V3 => {
                return Err(crate::storage::StorageError::Config(format!(
                    "write path: table {table} is format-version V3; this writer emits v2"
                )));
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

    /// Build one staged append from a verified writer pin and create-only
    /// data files. The worker authorizes the returned bytes separately.
    pub fn prepare_staged_append(
        &self,
        st: &TableWriteState,
        namespace: &[&str],
        table: &str,
        new_files: &[crate::snapshot_write::NewDataFile],
        identity: crate::catalog::publication::PublicationIdentity,
        manifest_budget: (u64, std::time::Duration),
    ) -> Result<crate::catalog::commit::PreparedPublication, crate::storage::StorageError> {
        use crate::catalog::commit::validate_staged_base;
        use crate::catalog::publication::{scoped_manifest_location, scoped_object_location};
        use crate::catalog::rest::RestError;
        use crate::snapshot_write::{write_bounded_snapshot, SnapshotParams};
        use crate::storage::{StorageError, StorageIo};

        let (max_bytes, timeout) = manifest_budget;
        if new_files.is_empty() || max_bytes == 0 || max_bytes == u64::MAX || timeout.is_zero() {
            return Err(StorageError::Config(
                "staged append requires files and a manifest budget".into(),
            ));
        }
        let mut file_paths = std::collections::HashSet::with_capacity(new_files.len());
        for file in new_files {
            if !scoped_object_location(&st.location, &file.file_path, "/data/")
                || !file_paths.insert(&file.file_path)
                || file.record_count <= 0
                || file.file_size_in_bytes <= 0
            {
                return Err(StorageError::Config(
                    "invalid staged append data file".into(),
                ));
            }
        }

        let map_error = |error| match error {
            RestError::Conflict(message) => StorageError::Conflict(message),
            other => StorageError::Config(other.to_string()),
        };
        let prefix = self
            .rest
            .prefix
            .lock()
            .expect("prefix mutex poisoned")
            .clone();
        let loaded = self
            .rest
            .load_table_at_prefix(namespace, table, prefix.as_deref())
            .map_err(map_error)?;

        let base = loaded.metadata.ok_or_else(|| {
            StorageError::Config("staged append requires in-band table metadata".into())
        })?;
        let spec_id = validate_staged_base(&base, identity.table_uuid).map_err(map_error)?;
        let base_list = base
            .current_snapshot()
            .map(|snapshot| snapshot.manifest_list.as_str());
        if st.table_uuid != base.table_uuid
            || st.location.trim_end_matches('/') != base.location.trim_end_matches('/')
            || st.base_snapshot_id != base.current_snapshot_id
            || st.last_sequence_number != base.last_sequence_number
            || st.base_manifest_list.as_deref() != base_list
            || st.schema_id != Some(base.current_schema_id)
            || st.partition_spec_id != spec_id
        {
            return Err(StorageError::Conflict(
                "staged append writer pin changed".into(),
            ));
        }
        let carried_manifests = if let Some(path) = base_list {
            if !scoped_manifest_location(&st.location, path) {
                return Err(StorageError::Decode(
                    "base manifest list is outside the bound table".into(),
                ));
            }
            let (io, key) = crate::scan::open_for(path, Some(&st.s3))?;
            let manifests = crate::manifest::parse_manifest_list(
                &io.get_bytes_bounded(&key, max_bytes, timeout)?,
            )?;
            if manifests
                .iter()
                .any(|manifest| !scoped_manifest_location(&st.location, &manifest.manifest_path))
            {
                return Err(StorageError::Decode(
                    "carried manifest is outside the bound table".into(),
                ));
            }
            manifests
        } else {
            Vec::new()
        };
        let location = crate::storage::parse_location(&st.location)?;
        let io = StorageIo::open(&location, Some(&st.s3))?;
        let snapshot_id = ((uuid::Uuid::new_v4().as_u128() >> 64) as i64) & i64::MAX;
        let written = write_bounded_snapshot(
            &io,
            &st.location,
            new_files,
            SnapshotParams {
                snapshot_id,
                timestamp_ms: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |elapsed| elapsed.as_millis() as i64),
                parent_snapshot_id: st.base_snapshot_id,
                last_sequence_number: st.last_sequence_number,
                schema_id: st.schema_id,
                partition_spec_id: spec_id,
                carried_manifests,
                deleted_files: Vec::new(),
            },
            max_bytes,
        )?;
        let prepared = self
            .rest
            .prepare_staged_append(
                namespace,
                table,
                &base,
                written.snapshot,
                identity,
                written.manifest_list_digest,
            )
            .map_err(map_error)?;
        if prepared.route_prefix() != prefix.as_deref() {
            return Err(StorageError::Conflict(
                "catalog route changed during staged preparation".into(),
            ));
        }
        Ok(prepared)
    }

    /// Flush + assemble + COMMIT an append snapshot (doc 07 §2 steps 1-3,
    /// rebuilt per 13-implementation-review blocker #2): EVERY attempt
    /// reloads metadata, binds the requirement to the ORIGINAL table
    /// uuid (a replacement table aborts, never gets adopted), re-derives
    /// parent/sequence/carried manifests from the fresh base, assembles
    /// a fresh snapshot with a collision-resistant id, and never replays
    /// stale requirements. Fatal assembly errors abort without retry.
    /// `deleted` (M2 slice 28 COW): files this snapshot removes, emitted
    /// as DELETED manifest entries. An overwrite requires the original
    /// delete-free base and checks target file liveness. A ref conflict
    /// returns to PostgreSQL so the caller can restart the transaction.
    pub fn commit_append(
        &self,
        st: &TableWriteState,
        namespace: &[&str],
        table: &str,
        new_files: &[crate::snapshot_write::NewDataFile],
        deleted: &[crate::snapshot_write::DeletedDataFile],
        retries: u32,
    ) -> Result<(String, i64), crate::storage::StorageError> {
        use crate::catalog::commit::{TableRequirement, TableUpdate};
        use crate::catalog::rest::RestError;
        use crate::storage::StorageError;

        let loc = crate::storage::parse_location(&st.location)?;
        let io = crate::storage::StorageIo::open(&loc, Some(&st.s3))?;

        let mut last_generated_snapshot_id = 0i64;
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
                    let md = if let Some(m) = lt.metadata.as_ref() { m } else {
                        let (mio, mkey) =
                            crate::scan::open_for(&lt.metadata_location, Some(&st.s3))
                                .map_err(|e| {
                                    RestError::Http(e.to_string())
                                })?;
                        md_owned =
                            crate::storage::fetch_metadata(&mio, &mkey).map_err(|e| {
                                RestError::Http(e.to_string())
                            })?;
                        &md_owned
                    };
                    let base_seq = md.last_sequence_number;
                    let parent = md.current_snapshot_id;
                    if !deleted.is_empty()
                        && (parent != st.base_snapshot_id
                            || md.table_uuid != st.table_uuid
                            || md.format_version != crate::catalog::metadata::FormatVersion::V2
                            || Some(md.current_schema_id) != st.schema_id
                            || md.default_partition_spec_id.unwrap_or(0) != st.partition_spec_id)
                    {
                        return Err(RestError::Conflict(
                            "copy-on-write source snapshot or table definition changed".into(),
                        ));
                    }
                    let base_list = md.current_snapshot().map(|snap| snap.manifest_list.clone());

                    // blocker #1: carry the fresh base's live manifests
                    let carried = match &base_list {
                        Some(path) => {
                            let (cio, ckey) =
                                crate::scan::open_for(path, Some(&st.s3)).map_err(|e| {
                                    RestError::Http(e.to_string())
                                })?;
                            crate::manifest::parse_manifest_list(&cio.get_bytes(&ckey).map_err(
                                |e| RestError::Http(e.to_string()),
                            )?)
                            .map_err(|e| RestError::Http(e.to_string()))?
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
                                RestError::Http(e.to_string())
                            })?;
                        if !plan.delete_tasks.is_empty() {
                            return Err(RestError::Conflict(
                                "copy-on-write snapshot contains Iceberg deletes".into()
                            ));
                        }
                        let live: std::collections::HashSet<&str> = plan
                            .tasks
                            .iter()
                            .map(|t| t.entry.data_file.file_path.as_str())
                            .collect();
                        for d in deleted {
                            if !live.contains(d.file_path.as_str()) {
                                return Err(RestError::Conflict(format!(
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
                            .map_or(1, |d| d.as_millis() as i64);
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
                                .map_or(0, |d| d.as_millis() as i64),
                            parent_snapshot_id: parent,
                            last_sequence_number: base_seq,
                            schema_id: st.schema_id,
                            partition_spec_id: st.partition_spec_id,
                            carried_manifests: carried,
                            deleted_files: deleted.to_vec(),
                        },
                    )
                    .map_err(|e| RestError::Http(e.to_string()))?;
                    last_generated_snapshot_id = snapshot_id;

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
                    let mut requirements = vec![
                        TableRequirement::AssertTableUUID { uuid: st.table_uuid },
                        TableRequirement::AssertRefSnapshotId {
                            r#ref: "main".into(),
                            snapshot_id: parent,
                        },
                    ];
                    if !deleted.is_empty() {
                        requirements.push(TableRequirement::AssertCurrentSchemaId {
                            current_schema_id: md.current_schema_id,
                        });
                        requirements.push(TableRequirement::AssertDefaultSpecId {
                            default_spec_id: i64::from(st.partition_spec_id),
                        });
                    }
                    Ok((requirements, updates))
                },
                if deleted.is_empty() { retries } else { 0 },
                std::time::Duration::from_millis(50),
            )
            .map_err(|e| match e {
                RestError::Conflict(m) => StorageError::Conflict(m),
                RestError::CommitStateUnknown(m) => StorageError::CommitStateUnknown(m),
                other => StorageError::Config(other.to_string()),
            })?;
        Ok((resp.metadata_location, last_generated_snapshot_id))
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
    fn prepares_real_manifests_without_publishing_or_rebasing() {
        use crate::catalog::metadata::{PartitionSpec, SnapshotRef, TableMetadata};
        use crate::catalog::publication::PublicationIdentity;
        use crate::snapshot_write::{write_snapshot, NewDataFile, SnapshotParams};

        crate::storage::test_allow_local_stores();
        let dir = tempfile::tempdir().unwrap();
        let location = format!("file://{}", dir.path().display());
        let loc = crate::storage::parse_location(&location).unwrap();
        let io = crate::storage::StorageIo::open(&loc, None).unwrap();
        let mut batch = crate::writer::BatchWriter::new(vec![crate::writer::WriteColumn {
            name: "id".into(),
            ty: crate::catalog::metadata::IcebergType::Long,
            field_id: Some(1),
        }])
        .unwrap();
        let mut files = Vec::new();
        for id in [10, 20] {
            batch.push(&[crate::parquet::CellValue::Int64(id)]).unwrap();
            let written = batch.write_file(&io, "data", files.len()).unwrap();
            files.push(NewDataFile {
                file_path: format!("{location}/{}", written.key),
                record_count: written.record_count,
                file_size_in_bytes: written.file_size_in_bytes,
                ..Default::default()
            });
        }
        let base = write_snapshot(
            &io,
            &location,
            &files[..1],
            SnapshotParams {
                snapshot_id: 100,
                timestamp_ms: 1,
                parent_snapshot_id: None,
                last_sequence_number: None,
                schema_id: Some(0),
                partition_spec_id: 0,
                carried_manifests: vec![],
                deleted_files: vec![],
            },
        )
        .unwrap()
        .snapshot;
        let mut metadata: TableMetadata = serde_json::from_value(serde_json::json!({
            "format-version": 2, "table-uuid": uuid::Uuid::from_u128(1), "location": location,
            "current-schema-id": 0, "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
            "current-snapshot-id": 100, "last-sequence-number": 1,
        }))
        .unwrap();
        metadata.partition_specs.push(PartitionSpec {
            spec_id: 0,
            fields: vec![],
        });
        metadata.default_partition_spec_id = Some(0);
        metadata.refs.insert(
            "main".into(),
            SnapshotRef {
                snapshot_id: 100,
                kind: "branch".into(),
                min_snapshots_to_keep: None,
                max_snapshot_age_ms: None,
            },
        );
        metadata.snapshots.push(base.clone());
        let mut st = TableWriteState {
            table_uuid: metadata.table_uuid,
            base_snapshot_id: Some(100),
            last_sequence_number: Some(1),
            base_manifest_list: Some(base.manifest_list.clone()),
            location: location.clone(),
            schema_id: Some(0),
            partition_spec_id: 0,
            s3: S3Options::default(),
            field_ids: std::collections::BTreeMap::new(),
        };
        let identity = PublicationIdentity {
            operation_id: uuid::Uuid::from_u128(2),
            table_uuid: metadata.table_uuid,
            input_digest: [7; 32],
        };
        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", server.server_addr());
        let worker = std::thread::spawn(move || {
            let body =
                serde_json::json!({"metadata-location": "", "metadata": metadata}).to_string();
            for _ in 0..3 {
                let req = server
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap()
                    .unwrap();
                assert_eq!(req.method(), &tiny_http::Method::Get);
                req.respond(tiny_http::Response::from_string(body.clone()))
                    .unwrap();
            }
        });
        let client = CatalogClient::new(&endpoint, "", "", &S3ClientOptions::default()).unwrap();
        let budget = (1 << 20, std::time::Duration::from_secs(3));
        let prepared = client
            .prepare_staged_append(&st, &["ns"], "t", &files[1..], identity, budget)
            .unwrap();
        let wire: serde_json::Value = serde_json::from_slice(prepared.request_body()).unwrap();
        let manifest = wire["updates"][0]["snapshot"]["manifest-list"]
            .as_str()
            .unwrap();
        let persisted = io
            .get_bytes(manifest.strip_prefix(&format!("{location}/")).unwrap())
            .unwrap();
        let carried = crate::manifest::parse_manifest_list(&persisted).unwrap();
        assert_eq!(carried.len(), 2);
        assert_eq!(carried[0].added_snapshot_id, 100);
        assert_eq!(carried[1].added_snapshot_id, prepared.snapshot_id());
        assert_eq!(prepared.expected_parent(), Some(100));

        let metadata_dir = dir.path().join("metadata");
        let before = std::fs::read_dir(&metadata_dir).unwrap().count();
        let base_bytes = io
            .get_bytes(
                base.manifest_list
                    .strip_prefix(&format!("{location}/"))
                    .unwrap(),
            )
            .unwrap()
            .len() as u64;
        assert!(matches!(
            client.prepare_staged_append(&st, &["ns"], "t", &files[1..], identity,
                (base_bytes, budget.1)),
            Err(crate::storage::StorageError::Config(message))
                if message.contains("prepared manifest list exceeds")
        ));
        assert_eq!(std::fs::read_dir(&metadata_dir).unwrap().count(), before);

        st.base_snapshot_id = Some(99);
        assert!(matches!(
            client.prepare_staged_append(&st, &["ns"], "t", &files[1..], identity, budget),
            Err(crate::storage::StorageError::Conflict(_))
        ));
        assert_eq!(std::fs::read_dir(metadata_dir).unwrap().count(), before);
        worker.join().unwrap();
    }

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
        assert_ne!(err.to_string(), "");
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

    /// `commit_append` with a deleted file emits an overwrite snapshot:
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
            let resp_body = format!(r#"{{"metadata-location": "{md_loc}", "config": {{}}}}"#);
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
                r#"{{"metadata-location": "{next_loc}", "config": {{}}}}"#
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
        let (new_loc, _snap) = client
            .commit_append(&st, &["analytics"], "events", &[], &deleted, 3)
            .expect("overwrite commit");
        assert!(new_loc.contains("00002-x.metadata.json"));

        let body = handle.join().unwrap();
        let request: serde_json::Value = serde_json::from_str(&body).unwrap();
        let requirements = request["requirements"].as_array().unwrap();
        assert!(requirements.contains(
            &serde_json::json!({"type":"assert-current-schema-id", "current-schema-id":0})
        ));
        assert!(requirements
            .contains(&serde_json::json!({"type":"assert-default-spec-id", "default-spec-id":0})));
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
            .filter_map(std::result::Result::ok)
            .map(|e| e.path())
            .find(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with(&format!("snap-{snap_id}-")))
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
            let resp_body = format!(r#"{{"metadata-location": "{md_loc}", "config": {{}}}}"#);
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

    fn check_rejected_overwrite(source_snapshot: i64, expected_posts: usize) {
        let dir = tempfile::tempdir().unwrap();
        let (location, data_path) = overwrite_fixture(dir.path(), true);
        let metadata = overwrite_metadata_file(dir.path(), &location);
        let server = std::sync::Arc::new(tiny_http::Server::http("127.0.0.1:0").unwrap());
        let endpoint = format!("http://{}", server.server_addr());
        let listener = server.clone();
        let worker = std::thread::spawn(move || {
            let mut posts = 0;
            for req in listener.incoming_requests() {
                let response = if req.method() == &tiny_http::Method::Post {
                    posts += 1;
                    tiny_http::Response::from_string(
                        r#"{"error":{"type":"CommitFailedException","message":"stale ref"}}"#,
                    )
                    .with_status_code(409)
                } else if req.url().contains("/v1/config") {
                    tiny_http::Response::from_string(r#"{"defaults":{},"overrides":{}}"#)
                } else {
                    tiny_http::Response::from_string(
                        serde_json::json!({"metadata-location": metadata}).to_string(),
                    )
                };
                req.respond(response).unwrap();
            }
            posts
        });
        let client = CatalogClient::new(&endpoint, "wh", "", &S3ClientOptions::default()).unwrap();
        let mut state = client.write_table_open(&["ns"], "t", &[]).unwrap();
        state.base_snapshot_id = Some(source_snapshot);
        let deleted = [crate::snapshot_write::DeletedDataFile {
            file_path: data_path,
            record_count: 3,
            file_size_in_bytes: 4096,
        }];
        let result = client.commit_append(&state, &["ns"], "t", &[], &deleted, 3);
        server.unblock();
        assert_eq!(worker.join().unwrap(), expected_posts);
        assert!(matches!(
            result,
            Err(crate::storage::StorageError::Conflict(_))
        ));
    }

    #[test]
    fn overwrite_requires_its_original_base_even_when_file_survives() {
        // Snapshot 42 still contains the source file selected at snapshot 41.
        check_rejected_overwrite(41, 0);
    }

    #[test]
    fn overwrite_does_not_resubmit_on_catalog_cas_conflict() {
        // The base still matches during planning. The conflict occurs at POST.
        check_rejected_overwrite(42, 1);
    }
}
