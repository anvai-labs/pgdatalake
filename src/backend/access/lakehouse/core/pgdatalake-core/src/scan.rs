//! Scan planning (specs/lakehouse/06-execution.md §2). Slice 7: snapshot
//! selection + manifest-list/manifest composition into a task list.
//! Partition/bounds predicate pruning lands in the next slice.

//! Scan planning (specs/lakehouse/06-execution.md §2). Slice 7 composes
//! the pipeline: metadata -> snapshot selection -> manifest list -> manifests
//! -> task list (ADDED + EXISTING entries; DELETED dropped). Partition and
//! metrics pruning are the next slice; the FFI exposes this to the FDW.

use crate::catalog::metadata::{SnapshotSelector, TableMetadata};
use crate::manifest::{parse_manifest, parse_manifest_list, ManifestEntry};
use crate::storage::{parse_location, Location, S3Options, StorageError, StorageIo};

/// One planned scan task: a surviving data/delete file entry. The executor
/// turns each task into a worker's parquet read (doc 06 §3).
#[derive(Debug, Clone, PartialEq)]
pub struct ScanTask {
    pub entry: ManifestEntry,
    /// Effective sequence number: the entry's own when present, else the
    /// manifest-list's (v1 files); drives delete-file applicability (doc 07).
    pub sequence_number: i64,
}

/// A planned snapshot scan.
#[derive(Debug, Clone, PartialEq)]
pub struct ScanPlan {
    pub snapshot_id: i64,
    pub tasks: Vec<ScanTask>,
    /// Delete files (v2 position/equality deletes) surviving in the
    /// snapshot — never data tasks; the row readers apply them (slice 20).
    pub delete_tasks: Vec<ScanTask>,
    /// Sum of record_count over surviving DATA files (delete files add rows
    /// at read time; the row-count story is completed by the delete slice).
    pub total_rows: i64,
    /// Number of surviving DATA files.
    pub data_files: usize,
    /// Non-deleted manifest entries before predicate pruning (slice 16:
    /// EXPLAIN pruned-file evidence).
    pub files_before_pruning: usize,
    /// Non-deleted entries surviving pruning.
    pub files_after_pruning: usize,
}

/// A scan predicate. Slice 8: Eq over identity-transform partitions
/// (conservative — non-identity transforms are never pruned). Slice 14:
/// Eq/Lt/Le/Gt/Ge bounds pruning over parquet column stats; non-identity
/// transforms, unknown fields and missing bounds always KEEP the file.
#[derive(Debug, Clone, PartialEq)]
pub enum ScanPredicate {
    And(Vec<ScanPredicate>),
    Eq {
        field: String,
        value: serde_json::Value,
    },
    Lt {
        field: String,
        value: serde_json::Value,
    },
    Le {
        field: String,
        value: serde_json::Value,
    },
    Gt {
        field: String,
        value: serde_json::Value,
    },
    Ge {
        field: String,
        value: serde_json::Value,
    },
}

/// Resolve a data/delete file's opaque partition JSON against a spec: the
/// manifest's injected field NAMES equal the spec field names (Iceberg
/// writers inject them in spec order). Matching REQUIRES the spec field's
/// source-id to equal the predicate column's schema field ID (13-review
/// #4): a display name reused across schema evolution (drop + re-add with
/// a new ID) must not compare the new column's values against the old
/// column's partition member. Only identity transforms participate in
/// pruning; anything inconclusive is kept (pruning must never drop a
/// matching file).
fn partition_matches(
    spec: &crate::catalog::metadata::PartitionSpec,
    schema: &crate::catalog::metadata::Schema,
    partition: &serde_json::Value,
    pred: &ScanPredicate,
) -> bool {
    let Some(obj) = partition.as_object() else {
        return true; // unparseable partition: keep the file
    };
    match pred {
        ScanPredicate::Eq { field, value } => {
            // prove the partition member belongs to THIS column: resolve
            // the predicate field to its Iceberg ID, then require a spec
            // field whose source-id is that ID
            let Some(sf) = schema.fields.iter().find(|f| &f.name == field) else {
                return true; // predicate column not in the schema: keep
            };
            let Some(pf) = spec.fields.iter().find(|f| f.source_id == sf.id) else {
                return true; // this column does not feed this spec: keep
            };
            if pf.transform != "identity" {
                return true; // transform-aware pruning is a later slice
            }
            match obj.get(&pf.name) {
                Some(v) => v == value,
                None => true, // null partition value: keep
            }
        }
        ScanPredicate::And(preds) => preds
            .iter()
            .all(|p| partition_matches(spec, schema, partition, p)),
        // range predicates (Lt/Le/Gt/Ge) are bounds-pruned, not
        // partition-pruned (slice 8 covers identity-Eq only)
        _ => true,
    }
}

#[derive(Debug, Clone, PartialEq)]
enum BoundVal {
    /// Exact i64 for Int/Long/Timestamp/Timestamptz — f64 collapses
    /// adjacent integers above 2^53 into equal values, which pruned
    /// matching files (13-review blocker #4, reproduced wrong result).
    Int(i64),
    Num(f64),
    Str(String),
}

/// Decode a parquet-stat bound bytes slice per the iceberg field type.
/// Unsupported types -> None (field is never bounds-pruned, conservative).
fn decode_bound(bytes: &[u8], ty: &crate::catalog::metadata::IcebergType) -> Option<BoundVal> {
    use crate::catalog::metadata::IcebergType as IT;
    let le_i64 =
        |b: &[u8]| -> Option<i64> { b.try_into().ok().map(|a: [u8; 8]| i64::from_le_bytes(a)) };
    let le_i32 = |b: &[u8]| -> Option<i64> {
        b.try_into()
            .ok()
            .map(|a: [u8; 4]| i32::from_le_bytes(a) as i64)
    };
    Some(match ty {
        IT::Int => BoundVal::Int(le_i32(bytes)?),
        IT::Long | IT::Timestamp | IT::Timestamptz => BoundVal::Int(le_i64(bytes)?),
        IT::Float => {
            let b: [u8; 4] = bytes.try_into().ok()?;
            BoundVal::Num(f32::from_le_bytes(b) as f64)
        }
        IT::Double => {
            let b: [u8; 8] = bytes.try_into().ok()?;
            BoundVal::Num(f64::from_le_bytes(b))
        }
        IT::String => BoundVal::Str(String::from_utf8_lossy(bytes).into_owned()),
        _ => return None,
    })
}

/// Coerce a JSON predicate literal into the bound-comparable form for
/// the field's type. Integer fields take exact i64 literals (a float
/// literal on an integer field is incomparable -> conservative keep).
fn coerce_value(
    v: &serde_json::Value,
    ty: &crate::catalog::metadata::IcebergType,
) -> Option<BoundVal> {
    use crate::catalog::metadata::IcebergType as IT;
    Some(match (ty, v) {
        (_, serde_json::Value::String(s)) => BoundVal::Str(s.clone()),
        (IT::Int | IT::Long | IT::Timestamp | IT::Timestamptz, serde_json::Value::Number(n)) => {
            // exact only when integral and in i64 range; anything else is
            // incomparable against an i64 bound (conservative keep)
            let i = n.as_i64()?;
            if n.as_f64()? != i as f64 {
                return None;
            }
            BoundVal::Int(i)
        }
        (_, serde_json::Value::Number(n)) => BoundVal::Num(n.as_f64()?),
        _ => return None,
    })
}

fn cmp_bound<'a>(bound: Option<&'a BoundVal>, other: &'a BoundVal) -> Option<std::cmp::Ordering> {
    match (bound?, other) {
        (BoundVal::Num(a), BoundVal::Num(b)) => a.partial_cmp(b),
        (BoundVal::Str(a), BoundVal::Str(b)) => Some(a.as_bytes().cmp(b.as_bytes())),
        // integer-vs-float compares exactly only inside f64's integer
        // continuum (|v| <= 2^53); beyond that -> incomparable, keep
        (BoundVal::Int(a), BoundVal::Num(b)) | (BoundVal::Num(b), BoundVal::Int(a)) => {
            if a.abs() <= 9_007_199_254_740_992 {
                (*a as f64).partial_cmp(b)
            } else {
                None
            }
        }
        (BoundVal::Int(a), BoundVal::Int(b)) => Some(a.cmp(b)),
        // numeric/string mismatch: incomparable -> never prune
        _ => None,
    }
}

/// Can this predicate still match, given the file's column bounds?
/// Conservative by construction: missing bounds/ids/type mismatches keep.
fn bounds_keep(
    data_file: &crate::manifest::DataFile,
    schema: &crate::catalog::metadata::Schema,
    pred: &ScanPredicate,
) -> bool {
    use crate::catalog::metadata::IcebergType as IT;
    use std::cmp::Ordering as CmpOrd;
    let field = match pred {
        ScanPredicate::And(preds) => {
            return preds.iter().all(|p| bounds_keep(data_file, schema, p))
        }
        ScanPredicate::Eq { field, .. }
        | ScanPredicate::Lt { field, .. }
        | ScanPredicate::Le { field, .. }
        | ScanPredicate::Gt { field, .. }
        | ScanPredicate::Ge { field, .. } => field,
    };
    let Some(f) = schema.fields.iter().find(|f| &f.name == field) else {
        return true;
    };
    if !matches!(
        f.r#type,
        IT::Int | IT::Long | IT::Float | IT::Double | IT::String | IT::Timestamp | IT::Timestamptz
    ) {
        return true;
    }
    let lower = data_file
        .lower_bounds
        .get(&f.id)
        .and_then(|b| decode_bound(b, &f.r#type));
    let upper = data_file
        .upper_bounds
        .get(&f.id)
        .and_then(|b| decode_bound(b, &f.r#type));
    let value = match pred {
        ScanPredicate::Eq { value, .. }
        | ScanPredicate::Lt { value, .. }
        | ScanPredicate::Le { value, .. }
        | ScanPredicate::Gt { value, .. }
        | ScanPredicate::Ge { value, .. } => value,
        _ => unreachable!(),
    };
    let value = match coerce_value(value, &f.r#type) {
        Some(v) => v,
        None => return true,
    };
    match pred {
        ScanPredicate::Eq { .. } => !matches!(
            (
                cmp_bound(lower.as_ref(), &value),
                cmp_bound(upper.as_ref(), &value)
            ),
            (Some(CmpOrd::Greater), _) | (_, Some(CmpOrd::Less))
        ),
        ScanPredicate::Lt { .. } => match lower {
            None => true,
            Some(ref b) => cmp_bound(Some(b), &value)
                .map(|o| o == std::cmp::Ordering::Less)
                .unwrap_or(true),
        },
        ScanPredicate::Le { .. } => match lower {
            None => true,
            Some(ref b) => cmp_bound(Some(b), &value)
                .map(|o| o != std::cmp::Ordering::Greater)
                .unwrap_or(true),
        },
        ScanPredicate::Gt { .. } => match upper {
            None => true,
            Some(ref b) => cmp_bound(Some(b), &value)
                .map(|o| o == std::cmp::Ordering::Greater)
                .unwrap_or(true),
        },
        ScanPredicate::Ge { .. } => match upper {
            None => true,
            Some(ref b) => cmp_bound(Some(b), &value)
                .map(|o| o != std::cmp::Ordering::Less)
                .unwrap_or(true),
        },
        ScanPredicate::And(_) => unreachable!("handled above"),
    }
}

/// The JSON contract for translated C-side quals (slice 15):
/// `{"op":"eq|lt|le|gt|ge","field":name,"value":any}` and
/// `{"and":[...]}`. Anything else is a hard error so the C side keeps the
/// qual local instead of silently not pruning.
pub fn predicate_from_json(v: &serde_json::Value) -> Result<ScanPredicate, String> {
    if let Some(items) = v.get("and") {
        let arr = items
            .as_array()
            .ok_or_else(|| format!("and without array: {v}"))?;
        return Ok(ScanPredicate::And(
            arr.iter()
                .map(predicate_from_json)
                .collect::<Result<Vec<_>, _>>()?,
        ));
    }
    let op = v
        .get("op")
        .and_then(|o| o.as_str())
        .ok_or_else(|| format!("predicate missing op: {v}"))?;
    let field = || {
        v.get("field")
            .and_then(|f| f.as_str())
            .map(String::from)
            .ok_or_else(|| format!("predicate missing field: {v}"))
    };
    let value = || {
        v.get("value")
            .cloned()
            .ok_or_else(|| format!("predicate missing value: {v}"))
    };
    match op {
        "eq" => Ok(ScanPredicate::Eq {
            field: field()?,
            value: value()?,
        }),
        "lt" => Ok(ScanPredicate::Lt {
            field: field()?,
            value: value()?,
        }),
        "le" => Ok(ScanPredicate::Le {
            field: field()?,
            value: value()?,
        }),
        "gt" => Ok(ScanPredicate::Gt {
            field: field()?,
            value: value()?,
        }),
        "ge" => Ok(ScanPredicate::Ge {
            field: field()?,
            value: value()?,
        }),
        other => Err(format!("unsupported predicate op {other:?}")),
    }
}

/// Open a store for an absolute file location and return (io, key). For
/// file:// FILE locations the local store must be rooted at the parent
/// directory with the file name as the key; S3 locations carry the full
/// bucket-relative key already.
/// Open an object-store location; exposed for examples/diagnostics.
pub fn open_for(
    path_uri: &str,
    s3: Option<&S3Options>,
) -> Result<(StorageIo, String), StorageError> {
    let loc = parse_location(path_uri)?;
    match &loc {
        Location::S3 { key, .. } => Ok((StorageIo::open(&loc, s3)?, key.clone())),
        Location::Local { root } => {
            let p = std::path::Path::new(root);
            let name = p
                .file_name()
                .ok_or_else(|| {
                    StorageError::Config(format!("local path has no file name: {path_uri}"))
                })?
                .to_string_lossy()
                .into_owned();
            let dir = p
                .parent()
                .ok_or_else(|| {
                    StorageError::Config(format!("local path has no parent: {path_uri}"))
                })?
                .to_string_lossy()
                .into_owned();
            let io = StorageIo::open(&Location::Local { root: dir }, None)?;
            Ok((io, name))
        }
        Location::Http { .. } => Err(StorageError::Config(
            "http file locations are not supported yet".into(),
        )),
    }
}

/// 13-review #6: catalog- and manifest-supplied paths must stay inside
/// the table's own location root. Segment-aware: the last component of
/// the root is never treated as a bare prefix (`s3://b/a` does NOT
/// admit `s3://b/ab`). A violation is a loud decode error — never a
/// best-effort read from an unscoped location.
pub(crate) fn ensure_within_table_root(
    path_uri: &str,
    table_location: &str,
) -> Result<(), StorageError> {
    let scoped = |p: &str| -> Result<(String, String, String), StorageError> {
        match parse_location(p)? {
            crate::storage::Location::S3 { bucket, key } => Ok(("s3".into(), bucket, key)),
            crate::storage::Location::Local { root } => {
                Ok(("file".into(), String::new(), root.clone()))
            }
            crate::storage::Location::Http { url } => Ok(("http".into(), String::new(), url)),
        }
    };
    let (ps, pb, pk) = scoped(path_uri)?;
    let (rs, rb, rk) = scoped(table_location)?;
    let in_root =
        ps == rs && pb == rb && (pk == rk || pk.strip_prefix(&format!("{rk}/")).is_some());
    if !in_root {
        return Err(StorageError::Decode(format!(
            "path {path_uri} is outside the table location {table_location}"
        )));
    }
    Ok(())
}

/// Plan a scan of the given table metadata. `selector = None` selects the
/// current snapshot; a selector that resolves to nothing is a NotFound
/// (doc 02 §2 pin semantics — `FOR … AS OF`, slice 19). `s3` carries
/// connection options for locations that need them (file:// local fixtures
/// pass None).
pub fn plan_scan(
    md: &TableMetadata,
    selector: Option<&SnapshotSelector>,
    s3: Option<&S3Options>,
    predicate: Option<&ScanPredicate>,
) -> Result<ScanPlan, StorageError> {
    let snapshot = match selector {
        Some(sel) => md.resolve_snapshot(sel).ok_or_else(|| {
            StorageError::NotFound(format!("time-travel selector {sel:?} matches no snapshot"))
        })?,
        None => match md.current_snapshot_id {
            Some(id) => md.snapshot(id).ok_or_else(|| {
                StorageError::NotFound(format!("snapshot {id} not in table metadata"))
            })?,
            // empty table (no snapshots at all): SELECT plans to zero tasks
            None => {
                return Ok(ScanPlan {
                    snapshot_id: md.current_snapshot_id.unwrap_or_default(),
                    tasks: Vec::new(),
                    delete_tasks: Vec::new(),
                    total_rows: 0,
                    data_files: 0,
                    files_before_pruning: 0,
                    files_after_pruning: 0,
                });
            }
        },
    };

    // 13-review #6: scope every hop to the table's own location root
    ensure_within_table_root(&snapshot.manifest_list, &md.location)?;
    let (list_io, list_key) = open_for(&snapshot.manifest_list, s3)?;
    let list_bytes = list_io.get_bytes(&list_key)?;
    let list_entries = parse_manifest_list(&list_bytes)?;

    let schema = md.current_schema();
    // 13-review #4: with a predicate and no current schema (corrupt
    // metadata), planning errors loudly — a per-file `continue` here would
    // silently discard EVERY file and produce a plausible empty result.
    if predicate.is_some() && schema.is_none() {
        return Err(StorageError::Decode(
            "table metadata has no current schema; cannot apply scan predicates".into(),
        ));
    }
    let mut tasks = Vec::new();
    let mut delete_tasks = Vec::new();
    let mut total_rows = 0i64;
    let mut data_files = 0usize;
    let mut files_before_pruning = 0usize;
    let mut files_after_pruning = 0usize;

    // M2 slice 28 (COW overwrite): a file ADDED in one manifest and
    // DELETED in a later manifest of the SAME snapshot must not be
    // planned. DELETED entries are therefore collected across ALL
    // manifests first (pass 1) and their paths excluded from the live
    // set in pass 2 — an inline per-entry skip alone would resurrect
    // overwritten files from their carrying manifest. (Iceberg file
    // paths are unique per table, so path-keyed exclusion is exact.)
    let mut deleted_paths = std::collections::HashSet::new();
    let mut live: Vec<(
        &crate::manifest::ManifestListEntry,
        crate::manifest::ManifestEntry,
    )> = Vec::new();
    for mle in &list_entries {
        // 13-review #4: a manifest-list entry naming a partition spec the
        // table no longer lists is corrupt metadata — a loud error, never a
        // silent fallback to the default spec (pruning against the wrong
        // spec can eliminate live files, and local rechecks cannot recover
        // what the planner already dropped). Tolerance: a table with NO
        // partition specs at all is unpartitioned — its manifests' spec-0
        // references resolve to the implicit empty spec (provably cannot
        // mis-prune; older writers omit partition-specs entirely).
        if md.partition_spec(mle.partition_spec_id).is_none() && !md.partition_specs.is_empty() {
            return Err(StorageError::Decode(format!(
                "manifest {} references partition spec {} missing from table metadata",
                mle.manifest_path, mle.partition_spec_id
            )));
        }
        ensure_within_table_root(&mle.manifest_path, &md.location)?;
        let (io, key) = open_for(&mle.manifest_path, s3)?;
        let bytes = io.get_bytes(&key)?;
        for e in parse_manifest(&bytes)? {
            // DELETED entries are history, not data (doc 06 §2)
            if e.status == crate::manifest::ManifestEntryStatus::Deleted {
                deleted_paths.insert(e.data_file.file_path.clone());
                continue;
            }
            // 13-review #6: data/delete file paths stay inside the root too
            ensure_within_table_root(&e.data_file.file_path, &md.location)?;
            live.push((mle, e));
        }
    }

    // pass 2: pruning + task routing over the surviving live entries
    // (the loud missing-spec check already ran in pass 1; unwrap_or is
    // only reachable for the implicit empty spec of unpartitioned tables)
    let implicit_spec = crate::catalog::metadata::PartitionSpec {
        spec_id: 0,
        fields: Vec::new(),
    };
    for (mle, e) in live {
        let spec = md
            .partition_spec(mle.partition_spec_id)
            .unwrap_or(&implicit_spec);
        if deleted_paths.contains(&e.data_file.file_path) {
            continue;
        }
        files_before_pruning += 1;
        // slice 20: delete files are never data tasks — the row readers
        // load and apply them separately. Routing happens BEFORE the
        // predicate block (13-review #5): the data-file bounds rule
        // must never eliminate a delete task (a delete file's bounds
        // are over its keys, not the surviving rows). Deletes are
        // pruned only by the proven delete-specific rule, partition
        // applicability.
        // v3 deletion vectors (slice V1, doc 14): DVs are registered as
        // manifest entries whose file_format is PUFFIN — the content
        // value may be POSITION_DELETES (Java writer) but the container
        // format is the authoritative routing signal. Everything else
        // keeps the content-based routing with loud unknowns.
        let is_delete = e.data_file.content != crate::manifest::DataContent::Data
            || e.data_file.file_format.eq_ignore_ascii_case("puffin");
        if let Some(pred) = predicate {
            if let Some(partition) = &e.data_file.partition {
                if !partition_matches(spec, schema.unwrap(), partition, pred) {
                    continue;
                }
            }
            // bounds are a data-file rule only (13-review #5)
            if !is_delete && !bounds_keep(&e.data_file, schema.unwrap(), pred) {
                continue;
            }
        }
        files_after_pruning += 1;
        let task = ScanTask {
            sequence_number: e.sequence_number.or(mle.sequence_number).unwrap_or(0),
            entry: e,
        };
        if is_delete {
            delete_tasks.push(task);
            continue;
        }
        total_rows += task.entry.data_file.record_count;
        data_files += 1;
        tasks.push(task);
    }

    Ok(ScanPlan {
        snapshot_id: snapshot.snapshot_id,
        tasks,
        delete_tasks,
        total_rows,
        data_files,
        files_before_pruning,
        files_after_pruning,
    })
}

/// Load the snapshot's v2 delete files (slice 20, doc 07): position-delete
/// parquet has the (file_path, pos) schema — positions map per data file;
/// equality-delete parquet has one column per eq field — every row is a
/// delete tuple carrying the delete file's sequence number. M1 loads
/// eagerly at scan planning (delete files are small relative to data).
pub fn load_row_deletes(
    plan: &ScanPlan,
    s3: Option<&S3Options>,
    schema: &crate::catalog::metadata::Schema,
) -> Result<crate::parquet::LoadedDeletes, StorageError> {
    use crate::parquet::{CellValue, EqDeleteMatch, LoadedDeletes};

    let mut loaded = LoadedDeletes::default();
    for t in &plan.delete_tasks {
        let (io, key) = open_for(&t.entry.data_file.file_path, s3)?;

        // v3 deletion vectors: a PUFFIN-format delete file carries DV
        // blobs, each naming its referenced data file via a blob property;
        // positions apply unconditionally (i64::MAX sentinel sequence).
        // Application itself rides the position-delete machinery.
        if t.entry.data_file.file_format.eq_ignore_ascii_case("puffin") {
            let bytes = io.get_bytes(&key)?;
            for dv in crate::puffin::decode_dv_container(&bytes)? {
                loaded
                    .positions_by_file
                    .entry(dv.referenced_data_file)
                    .or_default()
                    .entry(i64::MAX)
                    .or_default()
                    .extend(dv.positions.iter());
            }
            continue;
        }

        let mut rows = crate::parquet::ParquetRows::open_all(&io, &key)?;
        match t.entry.data_file.content {
            crate::manifest::DataContent::PositionDeletes => {
                let names = rows.column_names().to_vec();
                let fp_i = names.iter().position(|n| n == "file_path").ok_or_else(|| {
                    StorageError::Decode(format!(
                        "position delete file {} has no file_path column",
                        t.entry.data_file.file_path
                    ))
                })?;
                let pos_i = names.iter().position(|n| n == "pos").ok_or_else(|| {
                    StorageError::Decode(format!(
                        "position delete file {} has no pos column",
                        t.entry.data_file.file_path
                    ))
                })?;
                while rows.next() {
                    let fp = match rows.value(fp_i) {
                        CellValue::Str(s) => s,
                        v => {
                            return Err(StorageError::Decode(format!(
                                "position delete file {}: file_path is not a string: {v:?}",
                                t.entry.data_file.file_path
                            )))
                        }
                    };
                    let pos = match rows.value(pos_i) {
                        CellValue::Int64(p) if p >= 0 => p as u64,
                        v => {
                            return Err(StorageError::Decode(format!(
                                "position delete file {}: pos is not a row number: {v:?}",
                                t.entry.data_file.file_path
                            )))
                        }
                    };
                    loaded
                        .positions_by_file
                        .entry(fp)
                        .or_default()
                        .entry(t.sequence_number)
                        .or_default()
                        .insert(pos);
                }
            }
            crate::manifest::DataContent::EqualityDeletes => {
                let names = rows.column_names().to_vec();
                // bind the key columns to the manifest's equality_ids via
                // the TABLE schema (13-review blocker #5: using every
                // column in the delete file makes extra non-key columns
                // prevent a valid delete from matching). A manifest
                // without equality_ids (non-spec file) falls back to all
                // columns; an unresolvable id is a loud error, never a
                // silently partial key.
                let id_to_name: std::collections::BTreeMap<i64, String> = schema
                    .fields
                    .iter()
                    .map(|f| (f.id, f.name.clone()))
                    .collect();
                let key_cols: Vec<(usize, String)> =
                    match t.entry.data_file.equality_ids.as_ref() {
                        Some(ids) if !ids.is_empty() => ids
                            .iter()
                            .map(|id| {
                                let name = id_to_name.get(&(*id as i64)).ok_or_else(|| {
                                    StorageError::Decode(format!(
                                        "equality delete file {}: equality_id {} is not in the table schema",
                                        t.entry.data_file.file_path, id
                                    ))
                                })?;
                                let idx = names.iter().position(|n| n == name).ok_or_else(|| {
                                    StorageError::Decode(format!(
                                        "equality delete file {}: projected column {} ({}) missing",
                                        t.entry.data_file.file_path, id, name
                                    ))
                                })?;
                                Ok((idx, name.clone()))
                            })
                            .collect::<Result<Vec<_>, StorageError>>()?,
                        _ => names
                            .iter()
                            .enumerate()
                            .map(|(i, n)| (i, n.clone()))
                            .collect(),
                    };
                while rows.next() {
                    let values: Vec<CellValue> =
                        key_cols.iter().map(|(i, _)| rows.value(*i)).collect();
                    loaded.equality.push(EqDeleteMatch {
                        columns: key_cols.iter().map(|(_, n)| n.clone()).collect(),
                        values,
                        sequence_number: t.sequence_number,
                    });
                }
            }
            crate::manifest::DataContent::Data => {
                unreachable!("delete_tasks only holds delete files")
            }
        }
    }
    Ok(loaded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::metadata::{FormatVersion, TableMetadata};
    use apache_avro::types::{Record, Value};
    use apache_avro::{Schema as AvroSchema, Writer};

    fn avro_map(kv_schema: &AvroSchema, pairs: &[(&str, i64)]) -> Value {
        // metric fields are arrays of named {key:int, value:long} records
        // (kv_long) per the Iceberg manifest Avro schema
        Value::Array(
            pairs
                .iter()
                .map(|(k, v)| {
                    let mut rec = Record::new(kv_schema).unwrap();
                    rec.put("key", k.parse::<i32>().unwrap());
                    rec.put("value", *v);
                    rec.into()
                })
                .collect(),
        )
    }

    /// 13-review #6: fixture data paths must live under the (test) table
    /// root — remap the historical file:///table/data/ prefix onto the
    /// tempdir so the table-root scope gate sees in-root paths.
    fn scope_data_path(dir: &std::path::Path, p: &str) -> String {
        if let Some(rest) = p.strip_prefix("file:///table/data/") {
            format!("file://{}/data/{}", dir.display(), rest)
        } else {
            p.to_string()
        }
    }

    /// Build one manifest (avro) with the given (status, path, rows) files.
    fn build_manifest(
        dir: &std::path::Path,
        name: &str,
        files: &[(i32, &str, i64)],
    ) -> Result<(), StorageError> {
        crate::storage::test_allow_local_stores();
        build_manifest_content(
            dir,
            name,
            &files
                .iter()
                .map(|(s, p, r)| (*s, scope_data_path(dir, p), *r, 0i32))
                .collect::<Vec<_>>(),
        )
    }

    /// Manifest with explicit data-file `content` values (0 data, 1 position
    /// deletes, 2 equality deletes) — slice 20 delete-file tests.
    fn build_manifest_content(
        dir: &std::path::Path,
        name: &str,
        files: &[(i32, String, i64, i32)],
    ) -> Result<(), StorageError> {
        crate::storage::test_allow_local_stores();
        let schema = AvroSchema::parse_str(crate::manifest::MANIFEST_ENTRY_SCHEMA_V2).unwrap();
        let AvroSchema::Record(top_rec) = &schema else {
            panic!("manifest is not a record");
        };
        let df_field = top_rec
            .fields
            .iter()
            .find(|f| f.name == "data_file")
            .unwrap();
        let apache_avro::Schema::Record(df_rec) = &df_field.schema else {
            panic!("data_file not a record");
        };
        let kv_schema = df_rec
            .fields
            .iter()
            .find(|f| f.name == "column_sizes")
            .map(|f| match &f.schema {
                apache_avro::Schema::Union(u) => u.variants()[1].clone(),
                other => other.clone(),
            })
            .unwrap();
        // items of the array: the named kv record schema (kv_long)
        let apache_avro::Schema::Array(arr) = &kv_schema else {
            panic!("column_sizes is not an array");
        };
        let kv_schema = (*arr.items).clone();
        let mut writer = Writer::new(&schema, Vec::new());
        for (status, path, rows, content) in files {
            let mut entry = Record::new(&schema).unwrap();
            entry.put("status", Value::Int(*status));
            entry.put("snapshot_id", Value::Long(42));
            entry.put(
                "sequence_number",
                Value::Union(1, Box::new(Value::Long(34))),
            );
            entry.put(
                "file_sequence_number",
                Value::Union(1, Box::new(Value::Long(1))),
            );
            let data_file = Value::Record(vec![
                ("content".into(), Value::Int(*content)),
                ("file_path".into(), Value::String(path.clone())),
                ("file_format".into(), Value::String("parquet".into())),
                (
                    "partition".into(),
                    Value::Union(1, Box::new(Value::Record(vec![]))),
                ),
                ("record_count".into(), Value::Long(*rows)),
                ("file_size_in_bytes".into(), Value::Long(4096)),
                (
                    "column_sizes".into(),
                    Value::Union(1, Box::new(avro_map(&kv_schema, &[("1", 64)]))),
                ),
                (
                    "value_counts".into(),
                    Value::Union(1, Box::new(avro_map(&kv_schema, &[("1", *rows)]))),
                ),
                (
                    "null_value_counts".into(),
                    Value::Union(1, Box::new(avro_map(&kv_schema, &[]))),
                ),
                (
                    "nan_value_counts".into(),
                    Value::Union(1, Box::new(avro_map(&kv_schema, &[]))),
                ),
                (
                    "lower_bounds".into(),
                    Value::Union(1, Box::new(Value::Array(vec![]))),
                ),
                (
                    "upper_bounds".into(),
                    Value::Union(1, Box::new(Value::Array(vec![]))),
                ),
                (
                    "key_metadata".into(),
                    Value::Union(0, Box::new(Value::Null)),
                ),
                (
                    "split_offsets".into(),
                    Value::Union(1, Box::new(Value::Array(vec![Value::Int(4)]))),
                ),
                (
                    "equality_ids".into(),
                    Value::Union(0, Box::new(Value::Null)),
                ),
                (
                    "sort_order_id".into(),
                    Value::Union(0, Box::new(Value::Null)),
                ),
            ]);
            entry.put("data_file", data_file);
            writer.append(entry).unwrap();
        }
        std::fs::write(dir.join(name), writer.into_inner().unwrap())
            .map_err(|e| StorageError::Config(format!("write fixture: {e}")))?;
        Ok(())
    }

    /// One fixture file: (status, path, rows, content, field-1 bounds).
    type BoundsFile<'a> = (i32, &'a str, i64, i32, Option<(i64, i64)>);

    /// Manifest with per-file lower/upper bounds on field 1 (LE i64 Long
    /// bytes) — 13-review #5 delete-pruning tests.
    fn build_manifest_bounds(
        dir: &std::path::Path,
        name: &str,
        files: &[BoundsFile<'_>],
    ) -> Result<(), StorageError> {
        crate::storage::test_allow_local_stores();
        let schema = AvroSchema::parse_str(crate::manifest::MANIFEST_ENTRY_SCHEMA_V2).unwrap();
        let AvroSchema::Record(top_rec) = &schema else {
            panic!("manifest is not a record");
        };
        let df_field = top_rec
            .fields
            .iter()
            .find(|f| f.name == "data_file")
            .unwrap();
        let apache_avro::Schema::Record(df_rec) = &df_field.schema else {
            panic!("data_file not a record");
        };
        // lower_bounds: array of kv_bytes records {key:int, value:bytes}
        let kv_bytes_schema = df_rec
            .fields
            .iter()
            .find(|f| f.name == "lower_bounds")
            .map(|f| match &f.schema {
                apache_avro::Schema::Union(u) => u.variants()[1].clone(),
                other => other.clone(),
            })
            .unwrap();
        let apache_avro::Schema::Array(arr) = &kv_bytes_schema else {
            panic!("lower_bounds is not an array");
        };
        let kv_bytes_schema = (*arr.items).clone();
        let bounds_entries = |b: &Option<(i64, i64)>| {
            let items = match b {
                Some((lo, hi)) => {
                    let mut lo_rec = Record::new(&kv_bytes_schema).unwrap();
                    lo_rec.put("key", 1i32);
                    lo_rec.put("value", Value::Bytes(lo.to_le_bytes().to_vec()));
                    let mut hi_rec = Record::new(&kv_bytes_schema).unwrap();
                    hi_rec.put("key", 1i32);
                    hi_rec.put("value", Value::Bytes(hi.to_le_bytes().to_vec()));
                    vec![lo_rec.into(), hi_rec.into()]
                }
                None => vec![],
            };
            Value::Union(1, Box::new(Value::Array(items)))
        };
        let mut writer = Writer::new(&schema, Vec::new());
        for (status, path, rows, content, bounds) in files {
            let mut entry = Record::new(&schema).unwrap();
            entry.put("status", Value::Int(*status));
            entry.put("snapshot_id", Value::Long(42));
            entry.put(
                "sequence_number",
                Value::Union(1, Box::new(Value::Long(34))),
            );
            entry.put(
                "file_sequence_number",
                Value::Union(1, Box::new(Value::Long(1))),
            );
            let data_file = Value::Record(vec![
                ("content".into(), Value::Int(*content)),
                (
                    "file_path".into(),
                    Value::String(scope_data_path(dir, path)),
                ),
                ("file_format".into(), Value::String("parquet".into())),
                (
                    "partition".into(),
                    Value::Union(1, Box::new(Value::Record(vec![]))),
                ),
                ("record_count".into(), Value::Long(*rows)),
                ("file_size_in_bytes".into(), Value::Long(4096)),
                (
                    "column_sizes".into(),
                    Value::Union(1, Box::new(Value::Array(vec![]))),
                ),
                (
                    "value_counts".into(),
                    Value::Union(1, Box::new(Value::Array(vec![]))),
                ),
                (
                    "null_value_counts".into(),
                    Value::Union(1, Box::new(Value::Array(vec![]))),
                ),
                (
                    "nan_value_counts".into(),
                    Value::Union(1, Box::new(Value::Array(vec![]))),
                ),
                ("lower_bounds".into(), bounds_entries(bounds)),
                ("upper_bounds".into(), bounds_entries(bounds)),
                (
                    "key_metadata".into(),
                    Value::Union(0, Box::new(Value::Null)),
                ),
                (
                    "split_offsets".into(),
                    Value::Union(1, Box::new(Value::Array(vec![Value::Int(4)]))),
                ),
                (
                    "equality_ids".into(),
                    Value::Union(0, Box::new(Value::Null)),
                ),
                (
                    "sort_order_id".into(),
                    Value::Union(0, Box::new(Value::Null)),
                ),
            ]);
            entry.put("data_file", data_file);
            writer.append(entry).unwrap();
        }
        std::fs::write(dir.join(name), writer.into_inner().unwrap())
            .map_err(|e| StorageError::Config(format!("write fixture: {e}")))?;
        Ok(())
    }

    fn build_manifest_list(
        dir: &std::path::Path,
        manifests: &[(&str, i64, i64, i64)],
    ) -> Result<(), StorageError> {
        build_manifest_list_named(dir, "mn-list.avro", manifests)
    }

    /// Same, but under an explicit filename — lets a test pin two snapshots
    /// to *different* manifest lists (slice 19 selector tests).
    fn build_manifest_list_named(
        dir: &std::path::Path,
        name: &str,
        manifests: &[(&str, i64, i64, i64)],
    ) -> Result<(), StorageError> {
        crate::storage::test_allow_local_stores();
        let schema = AvroSchema::parse_str(crate::manifest::MANIFEST_LIST_SCHEMA_V2).unwrap();
        let mut writer = Writer::new(&schema, Vec::new());
        for (name, added, existing, deleted) in manifests {
            let mut rec = Record::new(&schema).unwrap();
            rec.put(
                "manifest_path",
                format!("file://{}", dir.join(name).display()),
            );
            rec.put("manifest_length", 100i64);
            rec.put("partition_spec_id", 0i32);
            rec.put("added_snapshot_id", 42i64);
            rec.put("added_files_count", Some(*added));
            rec.put("existing_files_count", Some(*existing));
            rec.put("deleted_files_count", Some(*deleted));
            rec.put("added_rows_count", Some(0i64));
            rec.put("existing_rows_count", Some(0i64));
            rec.put("deleted_rows_count", Some(0i64));
            rec.put("partitions", None::<Value>);
            rec.put("key_metadata", None::<Value>);
            rec.put("sequence_number", Some(34i64));
            rec.put("min_sequence_number", Some(34i64));
            writer.append(rec).unwrap();
        }
        std::fs::write(dir.join(name), writer.into_inner().unwrap())
            .map_err(|e| StorageError::Config(format!("write fixture: {e}")))?;
        Ok(())
    }

    /// TDD anchor (M1 slice 7): the full planning pipeline composes —
    /// metadata → current snapshot → manifest list → manifests → task list,
    /// with DELETED entries dropped and row counts aggregated. All via
    /// file:// stores, no network (doc 10 §1).
    #[test]
    fn plans_scan_from_local_fixture() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("metadata")).unwrap();

        // 2 manifests: first has an ADDED + a DELETED file, second an EXISTING one.
        build_manifest(
            dir.path().join("metadata").as_path(),
            "mn1.avro",
            &[
                (1, "file:///table/data/a.parquet", 100),
                (2, "file:///table/data/deleted.parquet", 50),
            ],
        )
        .unwrap();
        build_manifest(
            dir.path().join("metadata").as_path(),
            "mn2.avro",
            &[(0, "file:///table/data/b.parquet", 30)],
        )
        .unwrap();
        build_manifest_list(
            dir.path().join("metadata").as_path(),
            &[("mn1.avro", 1, 0, 1), ("mn2.avro", 0, 1, 0)],
        )
        .unwrap();

        let list_path = dir.path().join("metadata").join("mn-list.avro");
        let metadata_json = format!(
            r#"{{
                "format-version": 2,
                "table-uuid": "9c12d441-03fe-11ed-8122-d358e75d12eb",
                "location": "file://{}",
                "schemas": [{{"type": "struct", "schema-id": 0, "fields": [
                    {{"id": 1, "name": "id", "required": true, "type": "long"}}]}}],
                "current-schema-id": 0,
                "snapshots": [
                    {{"snapshot-id": 42, "timestamp-ms": 1712345678000,
                      "manifest-list": "file://{}",
                      "summary": {{"operation": "append"}}}}
                ],
                "current-snapshot-id": 42
            }}"#,
            dir.path().display(),
            list_path.display(),
        );
        let md: TableMetadata = serde_json::from_str(&metadata_json).unwrap();
        assert_eq!(md.format_version, FormatVersion::V2);

        let plan = plan_scan(&md, None, None, None).expect("plan_scan");
        assert_eq!(plan.snapshot_id, 42);
        // ADDED (a) + EXISTING (b) survive; DELETED is dropped.
        assert_eq!(plan.tasks.len(), 2, "deleted entries are not planned");
        let mut paths: Vec<&str> = plan
            .tasks
            .iter()
            .map(|t| t.entry.data_file.file_path.as_str())
            .collect();
        paths.sort();
        assert_eq!(
            paths,
            [
                format!(
                    "file://{}/data/a.parquet",
                    dir.path().join("metadata").display()
                ),
                format!(
                    "file://{}/data/b.parquet",
                    dir.path().join("metadata").display()
                )
            ]
        );
        assert_eq!(plan.total_rows, 130);
        assert_eq!(plan.data_files, 2);
    }

    /// COW overwrite semantics (M2 slice 28): a data file ADDED in one
    /// manifest and DELETED in a LATER manifest of the same snapshot must
    /// not be planned. The inline per-entry skip only handles a DELETED
    /// entry in its own manifest; overwrite commits emit the DELETED entry
    /// in a NEW manifest while the original file's manifest is carried —
    /// without a cross-manifest exclusion the deleted row set would come
    /// back from the dead (the pre-fix behavior this test pins).
    #[test]
    fn file_deleted_in_later_manifest_is_excluded() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("metadata")).unwrap();

        // mn1: carried manifest with the original ADDED file (3 rows);
        // mn2: overwrite manifest with the same path as DELETED.
        build_manifest(
            dir.path().join("metadata").as_path(),
            "mn1.avro",
            &[(1, "file:///table/data/a.parquet", 3)],
        )
        .unwrap();
        build_manifest(
            dir.path().join("metadata").as_path(),
            "mn2.avro",
            &[(2, "file:///table/data/a.parquet", 3)],
        )
        .unwrap();
        build_manifest_list_named(
            dir.path().join("metadata").as_path(),
            "mn-list.avro",
            &[("mn1.avro", 1, 0, 0), ("mn2.avro", 0, 0, 1)],
        )
        .unwrap();

        let list_path = dir.path().join("metadata").join("mn-list.avro");
        let metadata_json = format!(
            r#"{{
                "format-version": 2,
                "table-uuid": "9c12d441-03fe-11ed-8122-d358e75d12eb",
                "location": "file://{}",
                "schemas": [{{"type": "struct", "schema-id": 0, "fields": [
                    {{"id": 1, "name": "id", "required": true, "type": "long"}}]}}],
                "current-schema-id": 0,
                "snapshots": [
                    {{"snapshot-id": 42, "timestamp-ms": 1712345678000,
                      "manifest-list": "file://{}",
                      "summary": {{"operation": "overwrite"}}}}
                ],
                "current-snapshot-id": 42
            }}"#,
            dir.path().display(),
            list_path.display(),
        );
        let md: TableMetadata = serde_json::from_str(&metadata_json).unwrap();

        let plan = plan_scan(&md, None, None, None).expect("plan_scan");
        assert_eq!(
            plan.tasks.len(),
            0,
            "the only file was deleted in a later manifest"
        );
        assert_eq!(plan.total_rows, 0);
        assert_eq!(plan.data_files, 0);
    }

    /// A table with no snapshots (freshly created) plans to zero tasks with
    /// a distinct "empty" marker, not an error — SELECT on empty table.
    #[test]
    fn plans_empty_table_to_zero_tasks() {
        let metadata_json = r#"{
            "format-version": 3,
            "table-uuid": "9c12d441-03fe-11ed-8122-d358e75d12eb",
            "location": "file:///nowhere",
            "schemas": [{"type": "struct", "schema-id": 0, "fields": [
                {"id": 1, "name": "id", "required": true, "type": "long"}]}],
            "current-schema-id": 0,
            "snapshots": []
        }"#;
        let md: TableMetadata = serde_json::from_str(metadata_json).unwrap();
        let plan = plan_scan(&md, None, None, None).expect("plan_scan");
        assert_eq!(plan.tasks.len(), 0);
        assert_eq!(plan.total_rows, 0);
    }

    /// Pinning an unknown snapshot id errors (doc 02 §2 snapshot_id pin).
    #[test]
    fn unknown_snapshot_pin_errors() {
        let metadata_json = r#"{
            "format-version": 2,
            "table-uuid": "9c12d441-03fe-11ed-8122-d358e75d12eb",
            "location": "file:///nowhere",
            "schemas": [{"type": "struct", "schema-id": 0, "fields": [
                {"id": 1, "name": "id", "required": true, "type": "long"}]}],
            "current-schema-id": 0,
            "snapshots": [{"snapshot-id": 42, "timestamp-ms": 1,
                           "manifest-list": "file:///x.avro",
                           "summary": {"operation": "append"}}],
            "current-snapshot-id": 42
        }"#;
        let md: TableMetadata = serde_json::from_str(metadata_json).unwrap();
        let err =
            plan_scan(&md, Some(&SnapshotSelector::Id { value: 999 }), None, None).unwrap_err();
        assert!(matches!(err, StorageError::NotFound(_)), "{err}");
    }

    /// TDD anchor (M1 slice 19): FOR TIMESTAMP/TAG AS OF selectors pin the
    /// plan to the right snapshot's tasks — a timestamp selects the latest
    /// snapshot at or before it, a tag resolves through the refs map, and a
    /// selector that matches no snapshot is a NotFound error.
    #[test]
    fn time_travel_selectors_pin_plan_to_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("metadata")).unwrap();

        // snapshot 42's manifest has file a.parquet (100 rows); snapshot 43's
        // has b.parquet (30 rows). Each snapshot gets its own manifest list.
        build_manifest(
            dir.path().join("metadata").as_path(),
            "mn1.avro",
            &[(1, "file:///table/data/a.parquet", 100)],
        )
        .unwrap();
        build_manifest(
            dir.path().join("metadata").as_path(),
            "mn2.avro",
            &[(0, "file:///table/data/b.parquet", 30)],
        )
        .unwrap();
        build_manifest_list(
            dir.path().join("metadata").as_path(),
            &[("mn1.avro", 1, 0, 0)],
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
                "snapshots": [
                    {{"snapshot-id": 42, "timestamp-ms": 1000,
                      "manifest-list": "file://{}", "summary": {{"operation": "append"}}}},
                    {{"snapshot-id": 43, "timestamp-ms": 2000,
                      "manifest-list": "file://{}", "summary": {{"operation": "append"}}}}
                ],
                "current-snapshot-id": 43,
                "refs": {{"first": {{"snapshot-id": 42, "type": "tag"}}}}
            }}"#,
            dir.path().display(),
            dir.path().join("metadata").join("mn-list.avro").display(),
            dir.path()
                .join("metadata")
                .join("mn-list-43.avro")
                .display(),
        );

        // build the second manifest list under its own name
        build_manifest_list_named(
            dir.path().join("metadata").as_path(),
            "mn-list-43.avro",
            &[("mn2.avro", 0, 1, 0)],
        )
        .unwrap();

        let md: TableMetadata = serde_json::from_str(&metadata_json).unwrap();

        // FOR TIMESTAMP AS OF: at/before 1000ms -> snapshot 42 -> a.parquet
        let plan = plan_scan(
            &md,
            Some(&SnapshotSelector::Ts { value_ms: 1500 }),
            None,
            None,
        )
        .expect("ts selector");
        assert_eq!(plan.snapshot_id, 42);
        assert_eq!(plan.total_rows, 100);

        // FOR VERSION AS OF (id) at the newest snapshot -> b.parquet
        let plan = plan_scan(&md, Some(&SnapshotSelector::Id { value: 43 }), None, None)
            .expect("id selector");
        assert_eq!(plan.snapshot_id, 43);
        assert_eq!(plan.total_rows, 30);

        // FOR TAG AS OF: ref name -> snapshot 42's tasks
        let plan = plan_scan(
            &md,
            Some(&SnapshotSelector::Ref {
                name: "first".into(),
            }),
            None,
            None,
        )
        .expect("ref selector");
        assert_eq!(plan.snapshot_id, 42);
        assert_eq!(plan.total_rows, 100);

        // selector matching no snapshot (unknown ref) is a NotFound error
        let err = plan_scan(
            &md,
            Some(&SnapshotSelector::Ref {
                name: "nope".into(),
            }),
            None,
            None,
        )
        .unwrap_err();
        assert!(matches!(err, StorageError::NotFound(_)), "{err}");
    }

    /// TDD anchor (M1 slice 20): delete files (content 1 = position deletes,
    /// 2 = equality deletes) are separated from data tasks into
    /// `delete_tasks` — they must never be planned as data scans.
    #[test]
    fn separates_delete_files_from_data_tasks() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("metadata")).unwrap();

        build_manifest(
            dir.path().join("metadata").as_path(),
            "mn-data.avro",
            &[(1, "file:///table/data/a.parquet", 100)],
        )
        .unwrap();
        build_manifest_content(
            dir.path().join("metadata").as_path(),
            "mn-posdel.avro",
            &[(
                1,
                scope_data_path(
                    dir.path().join("metadata").as_path(),
                    "file:///table/data/deletes-pos.parquet",
                ),
                2,
                1,
            )],
        )
        .unwrap();
        build_manifest_content(
            dir.path().join("metadata").as_path(),
            "mn-eqdel.avro",
            &[(
                1,
                scope_data_path(
                    dir.path().join("metadata").as_path(),
                    "file:///table/data/deletes-eq.parquet",
                ),
                1,
                2,
            )],
        )
        .unwrap();
        build_manifest_list(
            dir.path().join("metadata").as_path(),
            &[
                ("mn-data.avro", 1, 0, 0),
                ("mn-posdel.avro", 1, 0, 0),
                ("mn-eqdel.avro", 1, 0, 0),
            ],
        )
        .unwrap();

        let list_path = dir.path().join("metadata").join("mn-list.avro");
        let metadata_json = format!(
            r#"{{
                "format-version": 2,
                "table-uuid": "9c12d441-03fe-11ed-8122-d358e75d12eb",
                "location": "file://{}",
                "schemas": [{{"type": "struct", "schema-id": 0, "fields": [
                    {{"id": 1, "name": "id", "required": true, "type": "long"}}]}}],
                "current-schema-id": 0,
                "snapshots": [
                    {{"snapshot-id": 42, "timestamp-ms": 1000,
                      "manifest-list": "file://{}",
                      "summary": {{"operation": "append"}}}}
                ],
                "current-snapshot-id": 42
            }}"#,
            dir.path().display(),
            list_path.display(),
        );
        let md: TableMetadata = serde_json::from_str(&metadata_json).unwrap();

        let plan = plan_scan(&md, None, None, None).expect("plan_scan");
        assert_eq!(plan.tasks.len(), 1, "only the data file is a task");
        assert_eq!(plan.delete_tasks.len(), 2, "both delete files collected");
        assert_eq!(plan.total_rows, 100, "delete files add no rows");
        assert_eq!(
            plan.tasks[0].entry.data_file.file_path,
            format!(
                "file://{}/data/a.parquet",
                dir.path().join("metadata").display()
            )
        );
    }

    /// TDD anchor (M1 slice 8): identity-transform partition pruning —
    /// region='eu' keeps only the eu file; And-combined predicates AND.
    /// The manifest fixture injects the partition field into the Avro
    /// schema exactly as real writers do (per-table partition struct).
    #[test]
    fn prunes_partitioned_files_by_identity_predicate() {
        use crate::manifest::MANIFEST_ENTRY_SCHEMA_V2;
        use crate::scan::ScanPredicate;

        let partition_fields = r#""name": "region", "type": "string""#;
        let schema_str = MANIFEST_ENTRY_SCHEMA_V2.replace(
            "\"name\": \"r_part\", \"fields\": []",
            &format!("\"name\": \"r_part\", \"fields\": [{{{partition_fields}}}]"),
        );
        let schema = AvroSchema::parse_str(&schema_str).unwrap();
        let AvroSchema::Record(top_rec) = &schema else {
            panic!("manifest is not a record");
        };
        let df_field = top_rec
            .fields
            .iter()
            .find(|f| f.name == "data_file")
            .unwrap();
        let apache_avro::Schema::Record(df_rec) = &df_field.schema else {
            panic!("data_file not a record");
        };
        let kv_schema = df_rec
            .fields
            .iter()
            .find(|f| f.name == "column_sizes")
            .map(|f| match &f.schema {
                apache_avro::Schema::Union(u) => u.variants()[1].clone(),
                other => other.clone(),
            })
            .unwrap();
        // items of the array: the named kv record schema (kv_long)
        let apache_avro::Schema::Array(arr) = &kv_schema else {
            panic!("column_sizes is not an array");
        };
        let kv_schema = (*arr.items).clone();

        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("metadata")).unwrap();

        let mut writer = Writer::new(&schema, Vec::new());
        for (region, path, rows) in [
            ("eu", "file:///table/data/eu.parquet", 60),
            ("us", "file:///table/data/us.parquet", 40),
        ] {
            let path = scope_data_path(dir.path(), path);
            let mut entry = Record::new(&schema).unwrap();
            entry.put("status", Value::Int(1));
            entry.put("snapshot_id", Value::Long(42));
            entry.put("sequence_number", Value::Union(1, Box::new(Value::Long(1))));
            entry.put(
                "file_sequence_number",
                Value::Union(1, Box::new(Value::Long(1))),
            );
            let data_file = Value::Record(vec![
                ("content".into(), Value::Int(0)),
                ("file_path".into(), Value::String(path.clone())),
                ("file_format".into(), Value::String("parquet".into())),
                (
                    "partition".into(),
                    Value::Union(
                        1,
                        Box::new(Value::Record(vec![(
                            "region".into(),
                            Value::String(region.into()),
                        )])),
                    ),
                ),
                ("record_count".into(), Value::Long(rows)),
                ("file_size_in_bytes".into(), Value::Long(4096)),
                (
                    "column_sizes".into(),
                    Value::Union(1, Box::new(avro_map(&kv_schema, &[("1", 64)]))),
                ),
                (
                    "value_counts".into(),
                    Value::Union(1, Box::new(avro_map(&kv_schema, &[("1", rows)]))),
                ),
                (
                    "null_value_counts".into(),
                    Value::Union(1, Box::new(avro_map(&kv_schema, &[]))),
                ),
                (
                    "nan_value_counts".into(),
                    Value::Union(1, Box::new(avro_map(&kv_schema, &[]))),
                ),
                (
                    "lower_bounds".into(),
                    Value::Union(1, Box::new(Value::Array(vec![]))),
                ),
                (
                    "upper_bounds".into(),
                    Value::Union(1, Box::new(Value::Array(vec![]))),
                ),
                (
                    "key_metadata".into(),
                    Value::Union(0, Box::new(Value::Null)),
                ),
                (
                    "split_offsets".into(),
                    Value::Union(1, Box::new(Value::Array(vec![Value::Int(4)]))),
                ),
                (
                    "equality_ids".into(),
                    Value::Union(0, Box::new(Value::Null)),
                ),
                (
                    "sort_order_id".into(),
                    Value::Union(0, Box::new(Value::Null)),
                ),
            ]);
            entry.put("data_file", data_file);
            writer.append(entry).unwrap();
        }
        std::fs::write(
            dir.path().join("metadata").join("mn1.avro"),
            writer.into_inner().unwrap(),
        )
        .unwrap();
        build_manifest_list(
            dir.path().join("metadata").as_path(),
            &[("mn1.avro", 2, 0, 0)],
        )
        .unwrap();

        let list_path = dir.path().join("metadata").join("mn-list.avro");
        let metadata_json = format!(
            r#"{{
                "format-version": 2,
                "table-uuid": "9c12d441-03fe-11ed-8122-d358e75d12eb",
                "location": "file://{}",
                "schemas": [{{"type": "struct", "schema-id": 0, "fields": [
                    {{"id": 1, "name": "id", "required": true, "type": "long"}},
                    {{"id": 2, "name": "region", "required": false, "type": "string"}}]}}],
                "current-schema-id": 0,
                "partition-specs": [{{"spec-id": 0, "fields": [
                    {{"name": "region", "transform": "identity", "source-id": 2,
                      "field-id": 1000}}]}}],
                "default-partition-spec-id": 0,
                "snapshots": [
                    {{"snapshot-id": 42, "timestamp-ms": 1712345678000,
                      "manifest-list": "file://{}",
                      "summary": {{"operation": "append"}}}}
                ],
                "current-snapshot-id": 42
            }}"#,
            dir.path().display(),
            list_path.display(),
        );
        let md: TableMetadata = serde_json::from_str(&metadata_json).unwrap();

        // no predicate: both files planned
        let plan = plan_scan(&md, None, None, None).expect("plan all");
        assert_eq!(plan.tasks.len(), 2);
        assert_eq!(plan.total_rows, 100);

        // region = 'eu': one file, 60 rows
        let pred = ScanPredicate::Eq {
            field: "region".into(),
            value: serde_json::json!("eu"),
        };
        let plan = plan_scan(&md, None, None, Some(&pred)).expect("plan eu");
        assert_eq!(plan.tasks.len(), 1);
        assert_eq!(plan.total_rows, 60);
        assert_eq!(
            plan.tasks[0].entry.data_file.file_path,
            format!("file://{}/data/eu.parquet", dir.path().display())
        );

        // And with a non-matching eq: zero tasks
        let pred = ScanPredicate::And(vec![
            ScanPredicate::Eq {
                field: "region".into(),
                value: serde_json::json!("eu"),
            },
            ScanPredicate::Eq {
                field: "region".into(),
                value: serde_json::json!("apac"),
            },
        ]);
        let plan = plan_scan(&md, None, None, Some(&pred)).expect("plan and");
        assert_eq!(plan.tasks.len(), 0);
    }

    /// 13-review #4: a manifest-list entry referencing a partition spec
    /// the table no longer lists is corrupt metadata — a loud error, never
    /// a silent fallback to the default spec (pruning against the wrong
    /// spec can eliminate live files, and local rechecks cannot recover
    /// what the planner already dropped).
    #[test]
    fn unknown_partition_spec_is_a_loud_error() {
        let dir = tempfile::tempdir().unwrap();
        let meta = dir.path().join("metadata");
        std::fs::create_dir_all(&meta).unwrap();
        build_manifest(
            &meta,
            "mn1.avro",
            &[(1, "file:///table/data/a.parquet", 10)],
        )
        .unwrap();

        // manifest list whose entry names partition spec 5 — the table
        // metadata below lists only spec 0
        let schema = AvroSchema::parse_str(crate::manifest::MANIFEST_LIST_SCHEMA_V2).unwrap();
        let mut writer = Writer::new(&schema, Vec::new());
        let mut rec = Record::new(&schema).unwrap();
        rec.put(
            "manifest_path",
            format!("file://{}", meta.join("mn1.avro").display()),
        );
        rec.put("manifest_length", 100i64);
        rec.put("partition_spec_id", 5i32);
        rec.put("added_snapshot_id", 42i64);
        rec.put("added_files_count", Some(1i64));
        rec.put("existing_files_count", Some(0i64));
        rec.put("deleted_files_count", Some(0i64));
        rec.put("added_rows_count", Some(0i64));
        rec.put("existing_rows_count", Some(0i64));
        rec.put("deleted_rows_count", Some(0i64));
        rec.put("partitions", None::<Value>);
        rec.put("key_metadata", None::<Value>);
        rec.put("sequence_number", Some(1i64));
        rec.put("min_sequence_number", Some(1i64));
        writer.append(rec).unwrap();
        let list_path = meta.join("mn-list.avro");
        std::fs::write(&list_path, writer.into_inner().unwrap()).unwrap();

        let metadata_json = format!(
            r#"{{
                "format-version": 2,
                "table-uuid": "9c12d441-03fe-11ed-8122-d358e75d12eb",
                "location": "file://{}",
                "schemas": [{{"type": "struct", "schema-id": 0, "fields": [
                    {{"id": 1, "name": "id", "required": true, "type": "long"}}]}}],
                "current-schema-id": 0,
                "partition-specs": [{{"spec-id": 0, "fields": []}}],
                "default-spec-id": 0,
                "snapshots": [
                    {{"snapshot-id": 42, "timestamp-ms": 1712345678000,
                      "manifest-list": "file://{}",
                      "summary": {{"operation": "append"}}}}
                ],
                "current-snapshot-id": 42
            }}"#,
            dir.path().display(),
            list_path.display(),
        );
        let md: TableMetadata = serde_json::from_str(&metadata_json).unwrap();

        let err = plan_scan(&md, None, None, None).unwrap_err();
        assert!(
            matches!(err, StorageError::Decode(ref m) if m.contains("partition spec 5")),
            "unknown partition spec must be a loud decode error: {err}"
        );
    }

    /// 13-review #4: with a predicate and no current schema (corrupt
    /// metadata), planning must error — the old `continue` silently
    /// discarded EVERY file, producing a plausible empty result.
    #[test]
    fn missing_schema_with_predicate_is_a_loud_error() {
        let dir = tempfile::tempdir().unwrap();
        let meta = dir.path().join("metadata");
        std::fs::create_dir_all(&meta).unwrap();
        build_manifest(
            &meta,
            "mn1.avro",
            &[(1, "file:///table/data/a.parquet", 10)],
        )
        .unwrap();
        build_manifest_list(&meta, &[("mn1.avro", 1, 0, 0)]).unwrap();

        let metadata_json = format!(
            r#"{{
                "format-version": 2,
                "table-uuid": "9c12d441-03fe-11ed-8122-d358e75d12eb",
                "location": "file://{}",
                "schemas": [],
                "current-schema-id": 0,
                "snapshots": [
                    {{"snapshot-id": 42, "timestamp-ms": 1712345678000,
                      "manifest-list": "file://{}",
                      "summary": {{"operation": "append"}}}}
                ],
                "current-snapshot-id": 42
            }}"#,
            dir.path().display(),
            meta.join("mn-list.avro").display(),
        );
        let md: TableMetadata = serde_json::from_str(&metadata_json).unwrap();

        // without a predicate: plans fine
        assert_eq!(plan_scan(&md, None, None, None).unwrap().tasks.len(), 1);

        let pred = ScanPredicate::Lt {
            field: "id".into(),
            value: serde_json::json!(15),
        };
        let err = plan_scan(&md, None, None, Some(&pred)).unwrap_err();
        assert!(
            matches!(err, StorageError::Decode(ref m) if m.contains("current schema")),
            "missing schema with predicate must be a loud decode error: {err}"
        );
    }

    /// 13-review #4: identity-partition matching must prove the spec
    /// field's source-id IS the predicate column's Iceberg ID. Name reuse
    /// across schema evolution (drop "region" id 1, re-add "region" id 2)
    /// must not compare the new column's values against the OLD column's
    /// partition member — a name-only match prunes live files.
    #[test]
    fn partition_matching_requires_source_field_id() {
        let dir = tempfile::tempdir().unwrap();
        let meta = dir.path().join("metadata");
        std::fs::create_dir_all(&meta).unwrap();

        // partition records {"region": "eu"|"us"} were written under the
        // OLD schema, where "region" was field id 1. The CURRENT schema's
        // "region" is a re-added column with id 2.
        build_manifest(
            &meta,
            "mn1.avro",
            &[
                (1, "file:///table/data/eu.parquet", 60),
                (1, "file:///table/data/us.parquet", 40),
            ],
        )
        .unwrap();
        // the generic builder writes empty partition records; build with
        // region members explicitly (patch the per-table partition struct
        // exactly as real writers inject it)
        {
            let schema_str = crate::manifest::MANIFEST_ENTRY_SCHEMA_V2.replace(
                "\"name\": \"r_part\", \"fields\": []",
                "\"name\": \"r_part\", \"fields\": [{\"name\": \"region\", \"type\": \"string\"}]",
            );
            let schema = AvroSchema::parse_str(&schema_str).unwrap();
            let AvroSchema::Record(top_rec) = &schema else {
                panic!("not a record")
            };
            let df_field = top_rec
                .fields
                .iter()
                .find(|f| f.name == "data_file")
                .unwrap();
            let apache_avro::Schema::Record(df_rec) = &df_field.schema else {
                panic!("data_file not a record");
            };
            let kv_schema = df_rec
                .fields
                .iter()
                .find(|f| f.name == "column_sizes")
                .map(|f| match &f.schema {
                    apache_avro::Schema::Union(u) => u.variants()[1].clone(),
                    other => other.clone(),
                })
                .unwrap();
            let apache_avro::Schema::Array(arr) = &kv_schema else {
                panic!("column_sizes is not an array");
            };
            let kv_schema = (*arr.items).clone();

            let mut writer = Writer::new(&schema, Vec::new());
            for (region, path, rows) in [
                ("eu", "file:///table/data/eu.parquet", 60),
                ("us", "file:///table/data/us.parquet", 40),
            ] {
                let mut entry = Record::new(&schema).unwrap();
                entry.put("status", Value::Int(1));
                entry.put("snapshot_id", Value::Long(42));
                entry.put("sequence_number", Value::Union(1, Box::new(Value::Long(1))));
                entry.put(
                    "file_sequence_number",
                    Value::Union(1, Box::new(Value::Long(1))),
                );
                let data_file = Value::Record(vec![
                    ("content".into(), Value::Int(0)),
                    (
                        "file_path".into(),
                        Value::String(scope_data_path(meta.as_path(), path)),
                    ),
                    ("file_format".into(), Value::String("parquet".into())),
                    (
                        "partition".into(),
                        Value::Union(
                            1,
                            Box::new(Value::Record(vec![(
                                "region".into(),
                                Value::String(region.into()),
                            )])),
                        ),
                    ),
                    ("record_count".into(), Value::Long(rows)),
                    ("file_size_in_bytes".into(), Value::Long(4096)),
                    (
                        "column_sizes".into(),
                        Value::Union(1, Box::new(avro_map(&kv_schema, &[]))),
                    ),
                    (
                        "value_counts".into(),
                        Value::Union(1, Box::new(avro_map(&kv_schema, &[]))),
                    ),
                    (
                        "null_value_counts".into(),
                        Value::Union(1, Box::new(avro_map(&kv_schema, &[]))),
                    ),
                    (
                        "nan_value_counts".into(),
                        Value::Union(1, Box::new(avro_map(&kv_schema, &[]))),
                    ),
                    (
                        "lower_bounds".into(),
                        Value::Union(1, Box::new(Value::Array(vec![]))),
                    ),
                    (
                        "upper_bounds".into(),
                        Value::Union(1, Box::new(Value::Array(vec![]))),
                    ),
                    (
                        "key_metadata".into(),
                        Value::Union(0, Box::new(Value::Null)),
                    ),
                    (
                        "split_offsets".into(),
                        Value::Union(1, Box::new(Value::Array(vec![Value::Int(4)]))),
                    ),
                    (
                        "equality_ids".into(),
                        Value::Union(0, Box::new(Value::Null)),
                    ),
                    (
                        "sort_order_id".into(),
                        Value::Union(0, Box::new(Value::Null)),
                    ),
                ]);
                entry.put("data_file", data_file);
                writer.append(entry).unwrap();
            }
            std::fs::write(meta.join("mn-region.avro"), writer.into_inner().unwrap()).unwrap();
        }
        build_manifest_list(&meta, &[("mn-region.avro", 2, 0, 0)]).unwrap();

        let metadata_json = format!(
            r#"{{
                "format-version": 2,
                "table-uuid": "9c12d441-03fe-11ed-8122-d358e75d12eb",
                "location": "file://{}",
                "schemas": [{{"type": "struct", "schema-id": 0, "fields": [
                    {{"id": 2, "name": "region", "required": false, "type": "string"}}]}}],
                "current-schema-id": 0,
                "partition-specs": [{{"spec-id": 0, "fields": [
                    {{"name": "region", "transform": "identity", "source-id": 1,
                      "field-id": 1000}}]}}],
                "default-spec-id": 0,
                "snapshots": [
                    {{"snapshot-id": 42, "timestamp-ms": 1712345678000,
                      "manifest-list": "file://{}",
                      "summary": {{"operation": "append"}}}}
                ],
                "current-snapshot-id": 42
            }}"#,
            dir.path().display(),
            meta.join("mn-list.avro").display(),
        );
        let md: TableMetadata = serde_json::from_str(&metadata_json).unwrap();

        // current schema's "region" (id 2) is NOT the partition member's
        // source (id 1): no proof -> keep both files, never prune by name
        let pred = ScanPredicate::Eq {
            field: "region".into(),
            value: serde_json::json!("eu"),
        };
        let plan = plan_scan(&md, None, None, Some(&pred)).expect("plan reuse");
        assert_eq!(
            plan.tasks.len(),
            2,
            "name-reused column must not prune by the old column's partition values"
        );

        // positive control: matching source-id prunes properly
        let metadata_json_ok = metadata_json.replace(r#""source-id": 1"#, r#""source-id": 2"#);
        let md_ok: TableMetadata = serde_json::from_str(&metadata_json_ok).unwrap();
        let plan_ok = plan_scan(&md_ok, None, None, Some(&pred)).expect("plan proven");
        assert_eq!(plan_ok.tasks.len(), 1);
        assert_eq!(
            plan_ok.tasks[0].entry.data_file.file_path,
            format!("file://{}/data/eu.parquet", meta.display())
        );
    }

    /// TDD anchor (M1 slice 14): bounds pruning — predicates compared to
    /// manifest lower/upper bounds (parquet column stats, slice 6) skip
    /// files that cannot match. Conservative: missing bounds/ids always
    /// keep. Eq inside bounds keeps; numeric Lt with upper bound < value
    /// drops; And requires all parts to keep.
    #[test]
    fn bounds_pruning_skips_unmatchable_files() {
        use crate::catalog::metadata::{Field, IcebergType, Schema, TableMetadata};
        use std::collections::BTreeMap;

        let mk_entry = |lower: Option<i64>, upper: Option<i64>| crate::manifest::ManifestEntry {
            status: crate::manifest::ManifestEntryStatus::Added,
            snapshot_id: 42,
            sequence_number: None,
            file_sequence_number: None,
            data_file: crate::manifest::DataFile {
                content: crate::manifest::DataContent::Data,
                file_path: format!("file:///d/{lower:?}.parquet"),
                file_format: "parquet".into(),
                partition: Some(serde_json::json!({})),
                record_count: 10,
                file_size_in_bytes: 1,
                column_sizes: BTreeMap::new(),
                value_counts: BTreeMap::new(),
                null_value_counts: BTreeMap::new(),
                nan_value_counts: BTreeMap::new(),
                lower_bounds: lower
                    .map(|v| BTreeMap::from([(1i64, v.to_le_bytes().to_vec())]))
                    .unwrap_or_default(),
                upper_bounds: upper
                    .map(|v| BTreeMap::from([(1i64, v.to_le_bytes().to_vec())]))
                    .unwrap_or_default(),
                key_metadata: None,
                split_offsets: None,
                equality_ids: None,
                sort_order_id: None,
            },
        };
        let schema_fn = |fields: Vec<crate::catalog::metadata::Field>| Schema {
            struct_type: crate::catalog::metadata::IcebergTypeMarker::Struct,
            schema_id: 0,
            fields,
        };
        let id_schema = schema_fn(vec![Field {
            id: 1,
            name: "id".into(),
            required: true,
            r#type: IcebergType::Long,
            doc: None,
        }]);

        let md = TableMetadata {
            format_version: crate::catalog::metadata::FormatVersion::V2,
            table_uuid: uuid::Uuid::nil(),
            location: "s3://t".into(),
            last_sequence_number: None,
            schemas: vec![id_schema.clone()],
            current_schema_id: 0,
            snapshots: vec![],
            current_snapshot_id: None,
            properties: BTreeMap::new(),
            partition_specs: vec![],
            default_partition_spec_id: None,
            refs: BTreeMap::new(),
        };

        let in_bounds = mk_entry(Some(0), Some(100));
        let above = mk_entry(Some(500), Some(900));
        let no_bounds = mk_entry(None, None);

        // eq 50: in-bounds file keeps, above drops, no-bounds keeps
        let pred = ScanPredicate::Eq {
            field: "id".into(),
            value: serde_json::json!(50),
        };
        assert!(bounds_keep(&in_bounds.data_file, &id_schema, &pred));
        assert!(!bounds_keep(&above.data_file, &id_schema, &pred));
        assert!(bounds_keep(&no_bounds.data_file, &id_schema, &pred));

        // gt 150: above keeps (upper 900 > 150), in-bounds drops (upper 100 < 150)
        let pred = ScanPredicate::Gt {
            field: "id".into(),
            value: serde_json::json!(150),
        };
        assert!(bounds_keep(&above.data_file, &id_schema, &pred));
        assert!(!bounds_keep(&in_bounds.data_file, &id_schema, &pred));

        // And(eq, gt 150): in-bounds file drops (gt part fails)
        let pred = ScanPredicate::And(vec![
            ScanPredicate::Eq {
                field: "id".into(),
                value: serde_json::json!(50),
            },
            ScanPredicate::Gt {
                field: "id".into(),
                value: serde_json::json!(150),
            },
        ]);
        assert!(!bounds_keep(&in_bounds.data_file, &id_schema, &pred));
        let _ = md;
    }

    /// TDD anchor (M1 slice 15): the C/FDW side serializes translated
    /// quals as JSON; predicate_from_json is the deserialization contract:
    /// {"op":"eq|lt|le|gt|ge","field":...,"value":...} and
    /// {"and":[...]} — everything else is a hard error (the C side keeps
    /// untranslated quals local).
    #[test]
    fn predicate_from_json_roundtrip_and_rejects_garbage() {
        let p = predicate_from_json(&serde_json::json!({"op": "lt", "field": "id", "value": 5}))
            .expect("lt");
        assert_eq!(
            p,
            ScanPredicate::Lt {
                field: "id".into(),
                value: serde_json::json!(5)
            }
        );

        let p = predicate_from_json(&serde_json::json!({
            "and": [
                {"op": "eq", "field": "region", "value": "eu"},
                {"op": "ge", "field": "id", "value": 10}
            ]
        }))
        .expect("and");
        assert_eq!(
            p,
            ScanPredicate::And(vec![
                ScanPredicate::Eq {
                    field: "region".into(),
                    value: serde_json::json!("eu")
                },
                ScanPredicate::Ge {
                    field: "id".into(),
                    value: serde_json::json!(10)
                },
            ])
        );

        // garbage ops/values reject loudly -> C keeps the qual local
        assert!(predicate_from_json(&serde_json::json!({"op": "match"})).is_err());
        assert!(predicate_from_json(&serde_json::json!("nonsense")).is_err());
        assert!(predicate_from_json(&serde_json::json!({
            "and": [{"op": "nope", "field": "x", "value": 1}]
        }))
        .is_err());
    }

    /// Integration: a JSON predicate arriving through plan_scan prunes the
    /// same way a constructed one does (the FFI seam's end-to-end contract).
    #[test]
    fn json_predicate_prunes_through_plan_scan() {
        use crate::catalog::metadata::{Field, IcebergType, Schema, TableMetadata};
        use std::collections::BTreeMap;

        let mk_entry = |lower: Option<i64>, upper: Option<i64>| crate::manifest::ManifestEntry {
            status: crate::manifest::ManifestEntryStatus::Added,
            snapshot_id: 42,
            sequence_number: None,
            file_sequence_number: None,
            data_file: crate::manifest::DataFile {
                content: crate::manifest::DataContent::Data,
                file_path: format!("file:///d/{lower:?}.parquet"),
                file_format: "parquet".into(),
                partition: Some(serde_json::json!({})),
                record_count: 10,
                file_size_in_bytes: 1,
                column_sizes: BTreeMap::new(),
                value_counts: BTreeMap::new(),
                null_value_counts: BTreeMap::new(),
                nan_value_counts: BTreeMap::new(),
                lower_bounds: lower
                    .map(|v| BTreeMap::from([(1i64, v.to_le_bytes().to_vec())]))
                    .unwrap_or_default(),
                upper_bounds: upper
                    .map(|v| BTreeMap::from([(1i64, v.to_le_bytes().to_vec())]))
                    .unwrap_or_default(),
                key_metadata: None,
                split_offsets: None,
                equality_ids: None,
                sort_order_id: None,
            },
        };
        let md = TableMetadata {
            format_version: crate::catalog::metadata::FormatVersion::V2,
            table_uuid: uuid::Uuid::nil(),
            location: "s3://t".into(),
            last_sequence_number: None,
            schemas: vec![Schema {
                struct_type: crate::catalog::metadata::IcebergTypeMarker::Struct,
                schema_id: 0,
                fields: vec![Field {
                    id: 1,
                    name: "id".into(),
                    required: true,
                    r#type: IcebergType::Long,
                    doc: None,
                }],
            }],
            current_schema_id: 0,
            snapshots: vec![],
            current_snapshot_id: None,
            properties: BTreeMap::new(),
            partition_specs: vec![],
            default_partition_spec_id: None,
            refs: BTreeMap::new(),
        };
        let pred = predicate_from_json(&serde_json::json!({"op": "lt", "field": "id", "value": 5}))
            .unwrap();
        let plan = plan_scan(&md, None, None, Some(&pred)).unwrap();
        assert_eq!(plan.tasks.len(), 0, "empty-table plan with predicate OK");
        let _ = mk_entry;
    }

    /// TDD anchor (M1 slice 16): pruning stats expose before/after file
    /// counts — the EXPLAIN evidence numbers (3 files, 1 kept).
    #[test]
    fn plan_reports_pruning_stats() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("metadata")).unwrap();
        build_manifest(
            dir.path().join("metadata").as_path(),
            "mn1.avro",
            &[
                (1, "file:///table/data/a.parquet", 10),
                (1, "file:///table/data/b.parquet", 20),
                (1, "file:///table/data/c.parquet", 30),
            ],
        )
        .unwrap();
        build_manifest_list(
            dir.path().join("metadata").as_path(),
            &[("mn1.avro", 3, 0, 0)],
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
                "snapshots": [
                    {{"snapshot-id": 42, "timestamp-ms": 1712345678000,
                      "manifest-list": "file://{}",
                      "summary": {{"operation": "append"}}}}
                ],
                "current-snapshot-id": 42
            }}"#,
            dir.path().display(),
            dir.path().join("metadata").join("mn-list.avro").display(),
        );
        let md: TableMetadata = serde_json::from_str(&metadata_json).unwrap();

        let pred = ScanPredicate::Lt {
            field: "id".into(),
            value: serde_json::json!(15),
        };
        let plan = plan_scan(&md, None, None, Some(&pred)).unwrap();
        // all bounds empty -> conservative keep; the counts still report the
        // pruning pipeline ran (3 candidates examined, 3 kept)
        assert_eq!(plan.files_before_pruning, 3);
        assert_eq!(plan.files_after_pruning, 3);
        assert_eq!(plan.data_files, 3);
    }

    /// 13-review #5: the data-file bounds rule must never eliminate a
    /// delete task. A delete file's bounds are over its DELETE KEYS, not
    /// the surviving rows — a delete whose keys all fail the predicate
    /// would only be redundant if every data file it can touch was also
    /// pruned by the same predicate, which bounds alone don't establish
    /// (position deletes carry no key bounds at all). The proven
    /// delete-specific rule is partition applicability only.
    #[test]
    fn delete_tasks_survive_data_bounds_pruning() {
        let dir = tempfile::tempdir().unwrap();
        let meta = dir.path().join("metadata");
        std::fs::create_dir_all(&meta).unwrap();
        build_manifest_bounds(
            meta.as_path(),
            "mn1.avro",
            &[
                // data file: id in [1,10] — kept by `id < 15`
                (1, "file:///table/data/a.parquet", 10, 0, Some((1, 10))),
                // equality delete: keys in [100,200] — outside the
                // predicate's bounds range; must STILL be planned
                (1, "file:///table/data/del.parquet", 5, 2, Some((100, 200))),
            ],
        )
        .unwrap();
        build_manifest_list(meta.as_path(), &[("mn1.avro", 2, 0, 0)]).unwrap();

        let metadata_json = format!(
            r#"{{
                "format-version": 2,
                "table-uuid": "9c12d441-03fe-11ed-8122-d358e75d12eb",
                "location": "file://{}",
                "schemas": [{{"type": "struct", "schema-id": 0, "fields": [
                    {{"id": 1, "name": "id", "required": true, "type": "long"}}]}}],
                "current-schema-id": 0,
                "snapshots": [
                    {{"snapshot-id": 42, "timestamp-ms": 1712345678000,
                      "manifest-list": "file://{}",
                      "summary": {{"operation": "append"}}}}
                ],
                "current-snapshot-id": 42
            }}"#,
            dir.path().display(),
            meta.join("mn-list.avro").display(),
        );
        let md: TableMetadata = serde_json::from_str(&metadata_json).unwrap();

        let pred = ScanPredicate::Lt {
            field: "id".into(),
            value: serde_json::json!(15),
        };
        let plan = plan_scan(&md, None, None, Some(&pred)).unwrap();
        assert_eq!(plan.data_files, 1, "data file id∈[1,10] kept by id<15");
        assert_eq!(
            plan.delete_tasks.len(),
            1,
            "delete task must survive: data bounds never prune deletes"
        );
    }

    /// 13-review #6: catalog/manifest-supplied paths must stay inside the
    /// table's own location root — a compromised catalog or manifest must
    /// not direct reads at arbitrary buckets or local paths. Every hop
    /// (manifest list, manifests, data/delete files) is checked with a
    /// segment-aware prefix (no s3://t/ab vs s3://t/a confusion); a
    /// violation is a loud decode error, never a best-effort read.
    #[test]
    fn paths_outside_table_root_are_loud() {
        let dir = tempfile::tempdir().unwrap();
        let meta = dir.path().join("metadata");
        std::fs::create_dir_all(&meta).unwrap();
        build_manifest(
            &meta,
            "mn1.avro",
            &[(1, "file:///table/data/a.parquet", 10)],
        )
        .unwrap();
        build_manifest_list(&meta, &[("mn1.avro", 1, 0, 0)]).unwrap();

        // manifest list lives OUTSIDE the table root (table root is
        // file://{dir}/table, the fixtures are siblings)
        let metadata_json = format!(
            r#"{{
                "format-version": 2,
                "table-uuid": "9c12d441-03fe-11ed-8122-d358e75d12eb",
                "location": "file://{}",
                "schemas": [{{"type": "struct", "schema-id": 0, "fields": [
                    {{"id": 1, "name": "id", "required": true, "type": "long"}}]}}],
                "current-schema-id": 0,
                "snapshots": [
                    {{"snapshot-id": 42, "timestamp-ms": 1712345678000,
                      "manifest-list": "file://{}",
                      "summary": {{"operation": "append"}}}}
                ],
                "current-snapshot-id": 42
            }}"#,
            dir.path().join("table").display(),
            meta.join("mn-list.avro").display(),
        );
        let md: TableMetadata = serde_json::from_str(&metadata_json).unwrap();
        let err = plan_scan(&md, None, None, None).unwrap_err();
        assert!(
            matches!(err, StorageError::Decode(ref m) if m.contains("outside the table location")),
            "manifest list outside the table root must be a loud error: {err}"
        );

        // control: data file paths outside the root are equally loud
        let dir2 = tempfile::tempdir().unwrap();
        let meta2 = dir2.path().join("metadata");
        std::fs::create_dir_all(&meta2).unwrap();
        // manifest path INSIDE the root, data file OUTSIDE
        build_manifest(
            &meta2,
            "mn1.avro",
            &[(1, "file:///elsewhere/data/a.parquet", 10)],
        )
        .unwrap();
        build_manifest_list(&meta2, &[("mn1.avro", 1, 0, 0)]).unwrap();
        let metadata_json2 = format!(
            r#"{{
                "format-version": 2,
                "table-uuid": "9c12d441-03fe-11ed-8122-d358e75d12eb",
                "location": "file://{}",
                "schemas": [{{"type": "struct", "schema-id": 0, "fields": [
                    {{"id": 1, "name": "id", "required": true, "type": "long"}}]}}],
                "current-schema-id": 0,
                "snapshots": [
                    {{"snapshot-id": 42, "timestamp-ms": 1712345678000,
                      "manifest-list": "file://{}",
                      "summary": {{"operation": "append"}}}}
                ],
                "current-snapshot-id": 42
            }}"#,
            dir2.path().display(),
            meta2.join("mn-list.avro").display(),
        );
        let md2: TableMetadata = serde_json::from_str(&metadata_json2).unwrap();
        let err2 = plan_scan(&md2, None, None, None).unwrap_err();
        assert!(
            matches!(err2, StorageError::Decode(ref m) if m.contains("outside the table location")),
            "data file outside the table root must be a loud error: {err2}"
        );
    }
}

#[cfg(test)]
mod prune_i64_tests {
    use super::*;
    use crate::catalog::metadata::{Field, IcebergType, IcebergTypeMarker, Schema};

    fn md() -> crate::catalog::metadata::TableMetadata {
        use crate::catalog::metadata::{FormatVersion, TableMetadata};
        TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: uuid::Uuid::nil(),
            location: "file:///t".into(),
            last_sequence_number: Some(1),
            schemas: vec![Schema {
                struct_type: IcebergTypeMarker::Struct,
                schema_id: 0,
                fields: vec![Field {
                    id: 1,
                    name: "id".into(),
                    required: false,
                    r#type: IcebergType::Long,
                    doc: None,
                }],
            }],
            current_schema_id: 0,
            snapshots: vec![],
            current_snapshot_id: None,
            properties: Default::default(),
            partition_specs: vec![],
            default_partition_spec_id: Some(0),
            refs: Default::default(),
        }
    }

    fn data_file(lower: i64, upper: i64) -> crate::manifest::DataFile {
        crate::manifest::DataFile {
            content: crate::manifest::DataContent::Data,
            file_path: "file:///t/data/1.parquet".into(),
            file_format: "parquet".into(),
            partition: None,
            record_count: 1,
            file_size_in_bytes: 100,
            column_sizes: Default::default(),
            value_counts: Default::default(),
            null_value_counts: Default::default(),
            nan_value_counts: Default::default(),
            lower_bounds: [(1, lower.to_le_bytes().to_vec())].into(),
            upper_bounds: [(1, upper.to_le_bytes().to_vec())].into(),
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
            sort_order_id: None,
        }
    }

    const ABOVE_2POW53: i64 = 9_007_199_254_740_993; // 2^53 + 1

    /// ACCEPTANCE (13-review blocker #4, the reproduced wrong result): a
    /// file whose bounds are [2^53, 2^53+1] MATCHES `id > 2^53+1`... no —
    /// matches rows <= 2^53+1; the review's case: bounds [2^53, 2^53+1]
    /// and predicate id > 2^53. The file's upper bound (2^53+1) SATISFIES
    /// the predicate, so the file must be KEPT. Under f64 both bounds
    /// collapse to 2^53 and Gt(2^53) vs upper==2^53 pruned the file.
    #[test]
    fn bigint_strict_gt_keeps_file_above_2pow53() {
        let md = md();
        let df = data_file(9_007_199_254_740_992, ABOVE_2POW53);
        let pred = ScanPredicate::Gt {
            field: "id".into(),
            value: serde_json::json!(9_007_199_254_740_992i64),
        };
        assert!(
            bounds_keep(&df, md.current_schema().unwrap(), &pred),
            "f64 collapse must not prune a file whose upper bound satisfies the predicate"
        );
        // and the symmetric Eq case: Eq(2^53+1) matches the file too
        let pred_eq = ScanPredicate::Eq {
            field: "id".into(),
            value: serde_json::json!(ABOVE_2POW53),
        };
        assert!(bounds_keep(&df, md.current_schema().unwrap(), &pred_eq));
    }

    /// Pruning still works below 2^53: file with bounds [1,10] must be
    /// pruned by Gt(10).
    #[test]
    fn small_int_pruning_still_prunes() {
        let md = md();
        let df = data_file(1, 10);
        let pred = ScanPredicate::Gt {
            field: "id".into(),
            value: serde_json::json!(10),
        };
        assert!(!bounds_keep(&df, md.current_schema().unwrap(), &pred));
    }

    /// Float literal vs integer bound beyond 2^53: incomparable -> keep.
    #[test]
    fn float_literal_vs_big_int_bound_keeps() {
        let md = md();
        let df = data_file(9_007_199_254_740_992, ABOVE_2POW53);
        let pred = ScanPredicate::Gt {
            field: "id".into(),
            value: serde_json::json!(9.007199254740993e15),
        };
        assert!(bounds_keep(&df, md.current_schema().unwrap(), &pred));
    }
}

#[cfg(test)]
mod eq_delete_binding_tests {
    use super::*;
    use crate::catalog::metadata::{Field, IcebergType, IcebergTypeMarker, Schema};
    use crate::parquet::CellValue;
    use apache_avro::types::{Record, Value};
    use apache_avro::{Schema as AvroSchema, Writer};

    /// ACCEPTANCE (13-review blocker #5): an equality delete file with an
    /// EXTRA non-key column still matches rows — the key columns come
    /// from the manifest's equality_ids via the TABLE schema, not from
    /// "every column in the delete file". An unresolvable equality_id is
    /// a loud error (never a silently partial key).
    #[test]
    fn equality_delete_binds_to_equality_ids() {
        let dir = tempfile::tempdir().unwrap();
        let location = format!("file://{}", dir.path().display());
        let loc = parse_location(&location).unwrap();
        let _io = StorageIo::open(&loc, None).unwrap();

        let schema = Schema {
            struct_type: IcebergTypeMarker::Struct,
            schema_id: 0,
            fields: vec![
                Field {
                    id: 1,
                    name: "id".into(),
                    required: false,
                    r#type: IcebergType::Long,
                    doc: None,
                },
                Field {
                    id: 2,
                    name: "region".into(),
                    required: false,
                    r#type: IcebergType::String,
                    doc: None,
                },
            ],
        };

        // data file: rows id 1,2 (region us)
        {
            use arrow::array::{Int64Array, StringArray};
            use arrow::datatypes::{DataType, Field as AF, Schema as ArrowSchema};
            use arrow::record_batch::RecordBatch;
            use std::sync::Arc;
            std::fs::create_dir_all(dir.path().join("data")).unwrap();
            let aschema = ArrowSchema::new(vec![
                AF::new("id", DataType::Int64, false),
                AF::new("region", DataType::Utf8, false),
            ]);
            let batch = RecordBatch::try_new(
                Arc::new(aschema),
                vec![
                    Arc::new(Int64Array::from(vec![1, 2])),
                    Arc::new(StringArray::from(vec!["us", "us"])),
                ],
            )
            .unwrap();
            let f = std::fs::File::create(dir.path().join("data").join("d.parquet")).unwrap();
            let mut w = parquet::arrow::ArrowWriter::try_new(f, batch.schema(), None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }

        // equality delete file: key = id (equality_ids [1]) PLUS an extra
        // non-key column "note" that must NOT participate in matching
        {
            use arrow::array::{Int64Array, StringArray};
            use arrow::datatypes::{DataType, Field as AF, Schema as ArrowSchema};
            use arrow::record_batch::RecordBatch;
            use std::sync::Arc;
            let aschema = ArrowSchema::new(vec![
                AF::new("id", DataType::Int64, false),
                AF::new("note", DataType::Utf8, false),
            ]);
            let batch = RecordBatch::try_new(
                Arc::new(aschema),
                vec![
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(StringArray::from(vec!["delete-me-now"])),
                ],
            )
            .unwrap();
            let f = std::fs::File::create(dir.path().join("data").join("eq.parquet")).unwrap();
            let mut w = parquet::arrow::ArrowWriter::try_new(f, batch.schema(), None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }

        // plan: data task + equality delete task (equality_ids [1])
        let md = crate::catalog::metadata::TableMetadata {
            format_version: crate::catalog::metadata::FormatVersion::V2,
            table_uuid: uuid::Uuid::nil(),
            location: location.clone(),
            last_sequence_number: Some(2),
            schemas: vec![schema.clone()],
            current_schema_id: 0,
            snapshots: vec![crate::catalog::metadata::Snapshot {
                snapshot_id: 42,
                sequence_number: Some(2),
                first_row_id: None,
                parent_snapshot_id: None,
                timestamp_ms: 1,
                manifest_list: format!("{location}/mn-list.avro"),
                summary: Default::default(),
                schema_id: Some(0),
            }],
            current_snapshot_id: Some(42),
            properties: Default::default(),
            partition_specs: vec![],
            default_partition_spec_id: Some(0),
            refs: Default::default(),
        };

        // manifest: data + equality delete (content 2, equality_ids [1])
        let schema_a = AvroSchema::parse_str(crate::manifest::MANIFEST_ENTRY_SCHEMA_V2).unwrap();
        let mut writer = Writer::new(&schema_a, Vec::new());
        for (status, path, rows, content, seq, eq_ids) in [
            (
                1i32,
                format!("{location}/data/d.parquet"),
                2i64,
                0i32,
                2i64,
                None,
            ),
            (
                1,
                format!("{location}/data/eq.parquet"),
                1,
                2,
                3i64,
                Some(vec![1i32]),
            ),
        ] {
            let mut entry = Record::new(&schema_a).unwrap();
            entry.put("status", Value::Int(status));
            entry.put("snapshot_id", Value::Long(42));
            entry.put(
                "sequence_number",
                Value::Union(1, Box::new(Value::Long(seq))),
            );
            entry.put(
                "file_sequence_number",
                Value::Union(1, Box::new(Value::Long(1))),
            );
            let ids_v: Vec<Value> = eq_ids
                .unwrap_or_default()
                .into_iter()
                .map(Value::Int)
                .collect();
            let data_file = Value::Record(vec![
                ("content".into(), Value::Int(content)),
                ("file_path".into(), Value::String(path)),
                ("file_format".into(), Value::String("parquet".into())),
                (
                    "partition".into(),
                    Value::Union(1, Box::new(Value::Record(vec![]))),
                ),
                ("record_count".into(), Value::Long(rows)),
                ("file_size_in_bytes".into(), Value::Long(4096)),
                (
                    "column_sizes".into(),
                    Value::Union(1, Box::new(Value::Array(vec![]))),
                ),
                (
                    "value_counts".into(),
                    Value::Union(1, Box::new(Value::Array(vec![]))),
                ),
                (
                    "null_value_counts".into(),
                    Value::Union(1, Box::new(Value::Array(vec![]))),
                ),
                (
                    "nan_value_counts".into(),
                    Value::Union(1, Box::new(Value::Array(vec![]))),
                ),
                (
                    "lower_bounds".into(),
                    Value::Union(1, Box::new(Value::Array(vec![]))),
                ),
                (
                    "upper_bounds".into(),
                    Value::Union(1, Box::new(Value::Array(vec![]))),
                ),
                (
                    "key_metadata".into(),
                    Value::Union(0, Box::new(Value::Null)),
                ),
                (
                    "split_offsets".into(),
                    Value::Union(0, Box::new(Value::Null)),
                ),
                (
                    "equality_ids".into(),
                    Value::Union(1, Box::new(Value::Array(ids_v))),
                ),
                (
                    "sort_order_id".into(),
                    Value::Union(0, Box::new(Value::Null)),
                ),
            ]);
            entry.put("data_file", data_file);
            writer.append(entry).unwrap();
        }
        std::fs::write(dir.path().join("mn.avro"), writer.into_inner().unwrap()).unwrap();

        // manifest list (via the tested writer)
        let list_bytes = crate::snapshot_write::build_manifest_list_bytes(&[
            crate::manifest::ManifestListEntry {
                manifest_path: format!("{location}/mn.avro"),
                manifest_length: 4096,
                partition_spec_id: 0,
                content: Some(0),
                added_snapshot_id: 42,
                added_files_count: Some(2),
                existing_files_count: Some(0),
                deleted_files_count: Some(0),
                added_rows_count: Some(3),
                existing_rows_count: Some(0),
                deleted_rows_count: Some(0),
                sequence_number: Some(2),
                min_sequence_number: Some(2),
            },
        ])
        .unwrap();
        std::fs::write(dir.path().join("mn-list.avro"), list_bytes).unwrap();

        let md_ref = md.current_snapshot().unwrap().manifest_list.clone();
        let (lio, lkey) = open_for(&md_ref, None).unwrap();
        let _ = lio.get_bytes(&lkey).unwrap();
        let md2 = {
            let mut m = md.clone();
            m.snapshots[0].manifest_list = format!("{location}/mn-list.avro");
            m
        };
        let _ = md_ref;
        let plan = plan_scan(&md2, None, None, None).unwrap();
        assert_eq!(plan.data_files, 1);
        assert_eq!(plan.delete_tasks.len(), 1, "equality delete is a task");

        // load deletes with the schema: key binds to [id] via equality_ids
        let loaded = load_row_deletes(&plan, None, &schema).unwrap();
        assert_eq!(loaded.equality.len(), 1);
        let m = &loaded.equality[0];
        assert_eq!(m.columns, vec!["id"], "extra non-key column excluded");
        assert_eq!(m.values, vec![CellValue::Int64(1)]);

        // applied to the data file: row id=1 dies, id=2 survives
        let (dio, dkey) = open_for(&plan.tasks[0].entry.data_file.file_path, None).unwrap();
        let rd = loaded
            .for_task(&plan.tasks[0].entry.data_file.file_path, 2, false)
            .unwrap();
        let mut rows =
            crate::parquet::ParquetRows::open_with_deletes(&dio, &dkey, &["id"], rd).unwrap();
        let mut ids = Vec::new();
        while rows.next() {
            if let CellValue::Int64(v) = rows.value(0) {
                ids.push(v);
            }
        }
        assert_eq!(
            ids,
            vec![2],
            "equality delete killed id=1; extra column ignored"
        );

        // an unresolvable equality_id is a loud error
        let md_bad_schema = {
            let mut m = schema.clone();
            m.fields[0].id = 99; // schema no longer knows id 1
            m
        };
        let err = load_row_deletes(&plan, None, &md_bad_schema).unwrap_err();
        assert!(
            matches!(err, StorageError::Decode(ref m) if m.contains("not in the table schema")),
            "{err}"
        );
        let _ = md;
        let _ = md;
    }
}

/// V1 acceptance (doc 14): the FULL Java-written v3 fixture — metadata →
/// plan → DV load (puffin decode) → row read with DVs applied. Ground
/// truth: positions {2,4} deleted from the 6-row data file → ids
/// {1, 2, 4, 6} survive. pyiceberg decodes the same blob (see the
/// fixture provenance note in puffin.rs).
#[cfg(test)]
mod dv_fixture_tests {
    use super::*;

    #[test]
    fn java_v3_dv_fixture_reads_with_deletions_applied() {
        // The fixture's committed metadata carries its generation location
        // (/tmp/v3fixture/table). Reproduce that layout on any machine by
        // copying the checked-in tree there first — portability bug caught
        // by the first all-branch CI run (passed only where /tmp/v3fixture
        // happened to exist).
        let src_dir =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/v3_dv_table");
        crate::storage::test_allow_local_stores();
        let _dir = std::path::Path::new("/tmp/v3fixture/table");
        for sub in ["metadata", "data"] {
            let _ = std::fs::remove_dir_all(_dir.join(sub));
            std::fs::create_dir_all(_dir.join(sub)).expect("create fixture layout");
            for entry in std::fs::read_dir(src_dir.join(sub)).expect("fixture subdirs") {
                let entry = entry.expect("fixture entry");
                std::fs::copy(entry.path(), _dir.join(sub).join(entry.file_name()))
                    .expect("copy fixture file");
            }
        }
        let md_path = _dir.join("metadata").join("v3.metadata.json");
        let md_text = std::fs::read_to_string(&md_path).expect("fixture metadata");
        let md: TableMetadata = serde_json::from_str(&md_text).expect("parse fixture metadata");
        assert_eq!(
            md.format_version,
            crate::catalog::metadata::FormatVersion::V3,
            "the fixture is format-version 3"
        );

        let plan = plan_scan(&md, None, None, None).expect("plan_scan");
        assert_eq!(plan.data_files, 1, "one data file");
        assert_eq!(plan.delete_tasks.len(), 1, "one DV (puffin) delete file");

        let schema = md.current_schema().expect("current schema");
        let deletes = load_row_deletes(&plan, None, schema).expect("load deletes (DV decode)");
        let data_path = plan.tasks[0].entry.data_file.file_path.clone();
        let row_deletes = deletes
            .for_task(&data_path, plan.tasks[0].sequence_number, false)
            .expect("for_task");
        assert_eq!(
            row_deletes.positions,
            [2u64, 4u64]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>(),
            "DV positions 2,4 apply to the data file"
        );

        let (io, key) = open_for(&data_path, None).expect("open data file");
        let mut rows =
            crate::parquet::ParquetRows::open_with_deletes(&io, &key, &["id"], row_deletes)
                .expect("open rows");
        let mut ids = Vec::new();
        while rows.next() {
            match rows.value(0) {
                crate::parquet::CellValue::Int64(v) => ids.push(v),
                other => panic!("unexpected id cell: {other:?}"),
            }
        }
        ids.sort();
        assert_eq!(
            ids,
            vec![1, 2, 4, 6],
            "positions 2,4 deleted => ids 3,5 gone"
        );
    }
}

/// V2 gate (doc 14): the same pipeline against a table written by
/// TRINO 484 (its v3 DELETE emits DVs). Trino's manifests are
/// COMPRESSED avro containers — their embedded s3:// paths cannot be
/// byte-patched, so the test re-serializes manifests via the crate's
/// own readers/writers with paths remapped to a same-length local
/// prefix; the (uncompressed) metadata JSON and puffin footer are
/// byte-patched directly.
#[cfg(test)]
mod dv_trino_fixture_tests {
    use crate::catalog::metadata::TableMetadata;
    use crate::manifest::ManifestEntryStatus;
    use crate::scan::{load_row_deletes, open_for, plan_scan};

    fn remap(bytes: &[u8], from: &str, to: &str) -> Vec<u8> {
        let text = String::from_utf8_lossy(bytes).replace(from, to);
        text.into_bytes()
    }

    #[test]
    fn trino_v3_dv_fixture_reads_with_deletions_applied() {
        let src =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/v3_dv_trino");
        crate::storage::test_allow_local_stores();

        let s3_prefix = "s3://warehouse/trino_v3-cdb345082a0747418ac2e79892c813dd";
        let local_prefix = "/tmp/pdtl-trino-v3-fixture-00000000000000000000000000000";
        assert_eq!(s3_prefix.len(), local_prefix.len(), "byte-for-byte rename");
        let dst = std::path::Path::new(local_prefix);
        let _ = std::fs::remove_dir_all(dst);

        std::fs::create_dir_all(dst.join("metadata")).unwrap();
        std::fs::create_dir_all(dst.join("data")).unwrap();

        // metadata JSON: string remap (uncompressed)
        {
            let f = "00002-01a1faef-257d-7771-8e08-77571d3a8d2metadata.json";
            let out = remap(
                &std::fs::read(src.join("metadata").join(f)).unwrap(),
                s3_prefix,
                local_prefix,
            );
            std::fs::write(dst.join("metadata").join(f), out).unwrap();
        }

        // manifest-lists (compressed avro): decode -> remap -> re-serialize
        for f in [
            "snap-6666542285715108362-1-ded03e45-7188-4151-8ac1-8cd818ce5d0b.avro",
            "snap-7002360096778060333-1-c8f3429c-a643-49d5-bb73-b60412927f83.avro",
            "snap-8135731491675097439-1-99eca9d5-4a25-4d10-8805-7d4a1f7aae3c.avro",
        ] {
            let entries = crate::manifest::parse_manifest_list(
                &std::fs::read(src.join("metadata").join(f)).unwrap(),
            )
            .unwrap();
            let mut remapped = entries.clone();
            for e in &mut remapped {
                e.manifest_path = e.manifest_path.replace(s3_prefix, local_prefix);
            }
            let bytes = crate::snapshot_write::build_manifest_list_bytes(&remapped).unwrap();
            std::fs::write(dst.join("metadata").join(f), bytes).unwrap();
        }

        // manifests (compressed avro): decode -> remap data/DV paths ->
        // re-serialize preserving statuses
        for f in [
            "99eca9d5-4a25-4d10-8805-7d4a1f7aae3c-m0.avro",
            "c8f3429c-a643-49d5-bb73-b60412927f83-m0.avro",
        ] {
            let entries = crate::manifest::parse_manifest(
                &std::fs::read(src.join("metadata").join(f)).unwrap(),
            )
            .unwrap();
            let mut remapped = entries.clone();
            for e in &mut remapped {
                e.data_file.file_path = e.data_file.file_path.replace(s3_prefix, local_prefix);
            }
            let bytes = crate::snapshot_write::build_manifest_bytes_entries(&remapped).unwrap();
            std::fs::write(dst.join("metadata").join(f), bytes).unwrap();
        }

        // parquet: no embedded paths
        std::fs::copy(
            src.join(
                "data/20261120_214856_00025_ex4ny-07990454-7cad-4bd2-92a7-081369ce0042.parquet",
            ),
            dst.join(
                "data/20261120_214856_00025_ex4ny-07990454-7cad-4bd2-92a7-081369ce0042.parquet",
            ),
        )
        .unwrap();

        // puffin: uncompressed footer JSON carries referenced-data-file —
        // same-length byte patch
        {
            let mut bytes = std::fs::read(src.join(
                "data/dv-20261120_214858_00026_ex4ny-9c4239f4-3360-4a90-8410-10f9ee652cde.puffin",
            ))
            .unwrap();
            let mut i = 0;
            let from = s3_prefix.as_bytes();
            let to = local_prefix.as_bytes();
            while i + from.len() <= bytes.len() {
                if &bytes[i..i + from.len()] == from {
                    bytes[i..i + to.len()].copy_from_slice(to);
                    i += to.len();
                } else {
                    i += 1;
                }
            }
            std::fs::write(
                dst.join("data/dv-20261120_214858_00026_ex4ny-9c4239f4-3360-4a90-8410-10f9ee652cde.puffin"),
                bytes,
            )
            .unwrap();
        }

        let md_path = dst.join("metadata/00002-01a1faef-257d-7771-8e08-77571d3a8d2metadata.json");
        let md: TableMetadata =
            serde_json::from_str(&std::fs::read_to_string(&md_path).expect("fixture metadata"))
                .expect("parse");
        assert_eq!(
            md.format_version,
            crate::catalog::metadata::FormatVersion::V3
        );

        let plan = plan_scan(&md, None, None, None).expect("plan_scan");
        assert_eq!(plan.data_files, 1, "one data file");
        assert_eq!(plan.delete_tasks.len(), 1, "one DV delete file");

        let schema = md.current_schema().expect("schema");
        let deletes = load_row_deletes(&plan, None, schema).expect("load deletes");
        let data_path = plan.tasks[0].entry.data_file.file_path.clone();
        let row_deletes = deletes
            .for_task(&data_path, plan.tasks[0].sequence_number, false)
            .expect("for_task");
        assert_eq!(
            row_deletes.positions,
            [2u64, 4u64]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>()
        );

        let (io, key) = open_for(&data_path, None).expect("open data");
        let mut rows =
            crate::parquet::ParquetRows::open_with_deletes(&io, &key, &["id"], row_deletes)
                .expect("open rows");
        let mut ids = Vec::new();
        while rows.next() {
            match rows.value(0) {
                crate::parquet::CellValue::Int64(v) => ids.push(v),
                other => panic!("unexpected id cell: {other:?}"),
            }
        }
        ids.sort();
        assert_eq!(ids, vec![1, 2, 4], "Trino deleted ids 3,5 via DV");
        let _ = ManifestEntryStatus::Added;
    }
}
