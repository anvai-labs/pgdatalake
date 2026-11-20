//! Iceberg metadata WRITERS (M2 slice 27a): manifest avro, manifest-list
//! avro, and snapshot assembly — the bridge between slice 24 (parquet data
//! files) and slice 25 (commitTable client). Per doc 07 §2 step 1-2: flush
//! staged batches, build the snapshot (AddSnapshot + SetSnapshotRef
//! updates ride to the catalog; the client never rewrites metadata.json).
//!
//! Write schemas are the spec-shaped manifest structures with explicit
//! `field-id` annotations (matching pyiceberg/Trino id assignments) —
//! other engines' readers map manifest fields by field-id, so missing
//! annotations would break the slice-29 "Trino reads a table we wrote"
//! gate even though OUR reader parses by name from the embedded schema.

use std::collections::BTreeMap;

use apache_avro::{Schema as AvroSchema, Writer};
use uuid::Uuid;

use crate::catalog::metadata::Snapshot;
use crate::manifest::DataContent;
use crate::storage::{StorageError, StorageIo};

/// The manifest WRITE schema: same shape as the read-side reference
/// (manifest.rs) but with `field-id` annotations per the Iceberg spec /
/// pyiceberg id assignments. Field ORDER matters for avro and matches the
/// read-side constant.
pub const MANIFEST_ENTRY_WRITE_SCHEMA_V2: &str = r#"{
    "type": "record",
    "name": "manifest_entry",
    "fields": [
        {"name": "status", "type": "int", "field-id": 0},
        {"name": "snapshot_id", "type": "long", "field-id": 1},
        {"name": "sequence_number", "type": ["null", "long"], "field-id": 3},
        {"name": "file_sequence_number", "type": ["null", "long"], "field-id": 4},
        {"name": "data_file", "field-id": 2, "type": {
            "type": "record", "name": "data_file",
            "fields": [
                {"name": "content", "type": "int", "field-id": 134},
                {"name": "file_path", "type": "string", "field-id": 100},
                {"name": "file_format", "type": "string", "field-id": 101},
                {"name": "partition", "type": ["null", {"type": "record", "name": "r102", "fields": []}], "field-id": 102},
                {"name": "record_count", "type": "long", "field-id": 103},
                {"name": "file_size_in_bytes", "type": "long", "field-id": 104},
                {"name": "column_sizes", "type": ["null", {"type": "array", "logicalType": "map", "items": {"type": "record", "name": "kv_long", "fields": [{"name": "key", "type": "int", "field-id": 117}, {"name": "value", "type": "long", "field-id": 118}]}}], "field-id": 108},
                {"name": "value_counts", "type": ["null", {"type": "array", "logicalType": "map", "items": {"type": "record", "name": "kv_long_v2", "fields": [{"name": "key", "type": "int", "field-id": 119}, {"name": "value", "type": "long", "field-id": 120}]}}], "field-id": 109},
                {"name": "null_value_counts", "type": ["null", {"type": "array", "logicalType": "map", "items": {"type": "record", "name": "kv_long_v3", "fields": [{"name": "key", "type": "int", "field-id": 121}, {"name": "value", "type": "long", "field-id": 122}]}}], "field-id": 110},
                {"name": "nan_value_counts", "type": ["null", {"type": "array", "logicalType": "map", "items": {"type": "record", "name": "kv_long_v4", "fields": [{"name": "key", "type": "int", "field-id": 138}, {"name": "value", "type": "long", "field-id": 139}]}}], "field-id": 137},
                {"name": "lower_bounds", "type": ["null", {"type": "array", "logicalType": "map", "items": {"type": "record", "name": "kv_bytes_v1", "fields": [{"name": "key", "type": "int", "field-id": 126}, {"name": "value", "type": "bytes", "field-id": 127}]}}], "field-id": 125},
                {"name": "upper_bounds", "type": ["null", {"type": "array", "logicalType": "map", "items": {"type": "record", "name": "kv_bytes_v2", "fields": [{"name": "key", "type": "int", "field-id": 129}, {"name": "value", "type": "bytes", "field-id": 130}]}}], "field-id": 128},
                {"name": "key_metadata", "type": ["null", "bytes"], "field-id": 131},
                {"name": "split_offsets", "type": ["null", {"type": "array", "element-id": 133, "items": "long"}], "field-id": 132},
                {"name": "equality_ids", "type": ["null", {"type": "array", "element-id": 136, "items": "int"}], "field-id": 135},
                {"name": "sort_order_id", "type": ["null", "int"], "field-id": 140}
            ]
        }}
    ]
}"#;

/// The manifest-list WRITE schema (v2: adds content/sequence fields over
/// v1). Field-ids per pyiceberg's MANIFEST_LIST_FILE_SCHEMAS[2].
pub const MANIFEST_LIST_WRITE_SCHEMA_V2: &str = r#"{
    "type": "record",
    "name": "manifest_file",
    "fields": [
        {"name": "manifest_path", "type": "string", "field-id": 500},
        {"name": "manifest_length", "type": "long", "field-id": 501},
        {"name": "partition_spec_id", "type": "int", "field-id": 502},
        {"name": "content", "type": "int", "field-id": 517},
        {"name": "sequence_number", "type": "long", "field-id": 515},
        {"name": "min_sequence_number", "type": "long", "field-id": 516},
        {"name": "added_snapshot_id", "type": "long", "field-id": 503},
        {"name": "added_files_count", "type": "int", "field-id": 504},
        {"name": "existing_files_count", "type": "int", "field-id": 505},
        {"name": "deleted_files_count", "type": "int", "field-id": 506},
        {"name": "added_rows_count", "type": "long", "field-id": 512},
        {"name": "existing_rows_count", "type": "long", "field-id": 513},
        {"name": "deleted_rows_count", "type": "long", "field-id": 514},
        {"name": "partitions", "type": ["null", {"type": "array", "element-id": 508, "items": {"type": "record", "name": "r508", "fields": [
            {"name": "contains_null", "type": "boolean", "field-id": 509},
            {"name": "contains_nan", "type": ["null", "boolean"], "field-id": 518},
            {"name": "lower_bound", "type": ["null", "bytes"], "field-id": 510},
            {"name": "upper_bound", "type": ["null", "bytes"], "field-id": 511}
        ]}}], "field-id": 507},
        {"name": "key_metadata", "type": ["null", "bytes"], "field-id": 519}
    ]
}"#;

/// One new data file to include in the snapshot (metrics computed at
/// flush time by the write path). Metric maps may be empty — spec-legal,
/// just no pruning/statistics from them yet.
#[derive(Debug, Clone, Default)]
pub struct NewDataFile {
    /// Full URI used in manifests (what reads resolve via open_for).
    pub file_path: String,
    pub record_count: i64,
    pub file_size_in_bytes: i64,
    pub column_sizes: BTreeMap<i64, i64>,
    pub value_counts: BTreeMap<i64, i64>,
    pub null_value_counts: BTreeMap<i64, i64>,
    pub lower_bounds: BTreeMap<i64, Vec<u8>>,
    pub upper_bounds: BTreeMap<i64, Vec<u8>>,
}

use apache_avro::types::Value as V;

/// One existing data file the new snapshot REMOVES (COW overwrite, M2
/// slice 28). Emitted as a DELETED manifest entry in the new snapshot's
/// manifest — the file's bytes are never touched (time travel and
/// concurrent readers of older snapshots stay valid; expiry is a later
/// slice's job).
#[derive(Debug, Clone)]
pub struct DeletedDataFile {
    pub file_path: String,
    pub record_count: i64,
    pub file_size_in_bytes: i64,
}

fn avro_long_map(schema: &AvroSchema, map: &BTreeMap<i64, i64>) -> V {
    // items record schema: {"key": int, "value": long}
    let items = match schema {
        AvroSchema::Array(a) => (*a.items).clone(),
        other => other.clone(),
    };
    V::Array(
        map.iter()
            .map(|(k, v)| {
                V::Record(vec![
                    ("key".into(), V::Int(*k as i32)),
                    ("value".into(), V::Long(*v)),
                ])
                .resolve(&items)
                .unwrap_or_else(|_| {
                    V::Record(vec![
                        ("key".into(), V::Int(*k as i32)),
                        ("value".into(), V::Long(*v)),
                    ])
                })
            })
            .collect(),
    )
}

fn avro_bytes_map(schema: &AvroSchema, map: &BTreeMap<i64, Vec<u8>>) -> V {
    let items = match schema {
        AvroSchema::Array(a) => (*a.items).clone(),
        other => other.clone(),
    };
    V::Array(
        map.iter()
            .map(|(k, v)| {
                V::Record(vec![
                    ("key".into(), V::Int(*k as i32)),
                    ("value".into(), V::Bytes(v.clone())),
                ])
                .resolve(&items)
                .unwrap_or_else(|_| {
                    V::Record(vec![
                        ("key".into(), V::Int(*k as i32)),
                        ("value".into(), V::Bytes(v.clone())),
                    ])
                })
            })
            .collect(),
    )
}

/// Field schema of `name` inside the data_file record, unwrapping unions.
fn field_schema(schema: &AvroSchema, name: &str) -> Option<AvroSchema> {
    let AvroSchema::Record(r) = schema else {
        return None;
    };
    let df = r.fields.iter().find(|f| f.name == "data_file")?;
    let AvroSchema::Record(df_rec) = &df.schema else {
        return None;
    };
    df_rec
        .fields
        .iter()
        .find(|f| f.name == name)
        .map(|f| match &f.schema {
            AvroSchema::Union(u) => u.variants()[1].clone(),
            other => other.clone(),
        })
}

/// Serialize one manifest: ADDED entries for `files` plus DELETED entries
/// for `deleted` (v2 shape, unpartitioned).
pub fn build_manifest_bytes(
    files: &[NewDataFile],
    deleted: &[DeletedDataFile],
    snapshot_id: i64,
    sequence: i64,
) -> Result<Vec<u8>, StorageError> {
    let schema = AvroSchema::parse_str(MANIFEST_ENTRY_WRITE_SCHEMA_V2)
        .map_err(|e| StorageError::Config(format!("manifest write schema: {e}")))?;
    let col_sizes = field_schema(&schema, "column_sizes")
        .ok_or_else(|| StorageError::Config("manifest write schema: no column_sizes".into()))?;
    let lower = field_schema(&schema, "lower_bounds")
        .ok_or_else(|| StorageError::Config("manifest write schema: no lower_bounds".into()))?;

    let mut writer = Writer::new(&schema, Vec::new());
    for (i, f) in files.iter().enumerate() {
        let data_file = V::Record(vec![
            ("content".into(), V::Int(DataContent::Data as i32)),
            ("file_path".into(), V::String(f.file_path.clone())),
            ("file_format".into(), V::String("parquet".into())),
            // unpartitioned (skeleton 27a); per-table partition structs
            // arrive with partitioned managed tables
            ("partition".into(), V::Union(1, Box::new(V::Record(vec![])))),
            ("record_count".into(), V::Long(f.record_count)),
            ("file_size_in_bytes".into(), V::Long(f.file_size_in_bytes)),
            (
                "column_sizes".into(),
                V::Union(1, Box::new(avro_long_map(&col_sizes, &f.column_sizes))),
            ),
            (
                "value_counts".into(),
                V::Union(1, Box::new(avro_long_map(&col_sizes, &f.value_counts))),
            ),
            (
                "null_value_counts".into(),
                V::Union(1, Box::new(avro_long_map(&col_sizes, &f.null_value_counts))),
            ),
            (
                "nan_value_counts".into(),
                V::Union(1, Box::new(avro_long_map(&col_sizes, &BTreeMap::new()))),
            ),
            (
                "lower_bounds".into(),
                V::Union(1, Box::new(avro_bytes_map(&lower, &f.lower_bounds))),
            ),
            (
                "upper_bounds".into(),
                V::Union(1, Box::new(avro_bytes_map(&lower, &f.upper_bounds))),
            ),
            ("key_metadata".into(), V::Union(0, Box::new(V::Null))),
            ("split_offsets".into(), V::Union(0, Box::new(V::Null))),
            ("equality_ids".into(), V::Union(0, Box::new(V::Null))),
            ("sort_order_id".into(), V::Union(0, Box::new(V::Null))),
        ]);
        let entry = V::Record(vec![
            (
                "status".into(),
                V::Int(crate::manifest::ManifestEntryStatus::Added as i32),
            ),
            ("snapshot_id".into(), V::Long(snapshot_id)),
            (
                "sequence_number".into(),
                V::Union(1, Box::new(V::Long(sequence))),
            ),
            (
                "file_sequence_number".into(),
                V::Union(1, Box::new(V::Long(i as i64))),
            ),
            ("data_file".into(), data_file),
        ]);
        writer
            .append(entry)
            .map_err(|e| StorageError::Config(format!("manifest append: {e}")))?;
    }
    // DELETED entries (slice 28): the removed file's identity travels in
    // the data_file record so other engines' readers process the deletion;
    // our own reader excludes by path across all manifests.
    for f in deleted {
        let data_file = V::Record(vec![
            ("content".into(), V::Int(DataContent::Data as i32)),
            ("file_path".into(), V::String(f.file_path.clone())),
            ("file_format".into(), V::String("parquet".into())),
            ("partition".into(), V::Union(1, Box::new(V::Record(vec![])))),
            ("record_count".into(), V::Long(f.record_count)),
            ("file_size_in_bytes".into(), V::Long(f.file_size_in_bytes)),
            ("column_sizes".into(), V::Union(0, Box::new(V::Null))),
            ("value_counts".into(), V::Union(0, Box::new(V::Null))),
            ("null_value_counts".into(), V::Union(0, Box::new(V::Null))),
            ("nan_value_counts".into(), V::Union(0, Box::new(V::Null))),
            ("lower_bounds".into(), V::Union(0, Box::new(V::Null))),
            ("upper_bounds".into(), V::Union(0, Box::new(V::Null))),
            ("key_metadata".into(), V::Union(0, Box::new(V::Null))),
            ("split_offsets".into(), V::Union(0, Box::new(V::Null))),
            ("equality_ids".into(), V::Union(0, Box::new(V::Null))),
            ("sort_order_id".into(), V::Union(0, Box::new(V::Null))),
        ]);
        let entry = V::Record(vec![
            (
                "status".into(),
                V::Int(crate::manifest::ManifestEntryStatus::Deleted as i32),
            ),
            ("snapshot_id".into(), V::Long(snapshot_id)),
            (
                "sequence_number".into(),
                V::Union(1, Box::new(V::Long(sequence))),
            ),
            ("file_sequence_number".into(), V::Union(0, Box::new(V::Null))),
            ("data_file".into(), data_file),
        ]);
        writer
            .append(entry)
            .map_err(|e| StorageError::Config(format!("manifest append: {e}")))?;
    }
    writer
        .into_inner()
        .map_err(|e| StorageError::Config(format!("manifest close: {e}")))
}

/// Serialize a manifest-list (v2 shape). Content/count fields take the
/// ADDS of this snapshot (existing/deleted are 0; a compaction commit
/// builder will populate them for EXISTING entries in M4).
pub fn build_manifest_list_bytes(
    entries: &[crate::manifest::ManifestListEntry],
) -> Result<Vec<u8>, StorageError> {
    let schema = AvroSchema::parse_str(MANIFEST_LIST_WRITE_SCHEMA_V2)
        .map_err(|e| StorageError::Config(format!("manifest-list write schema: {e}")))?;
    let mut writer = Writer::new(&schema, Vec::new());
    for e in entries {
        let row = V::Record(vec![
            ("manifest_path".into(), V::String(e.manifest_path.clone())),
            ("manifest_length".into(), V::Long(e.manifest_length)),
            ("partition_spec_id".into(), V::Int(e.partition_spec_id)),
            // carry each manifest's own content (0 data / 1 pos-deletes /
            // 2 eq-deletes); new appends are DATA
            ("content".into(), V::Int(e.content.unwrap_or(0))),
            (
                "sequence_number".into(),
                V::Long(e.sequence_number.unwrap_or(0)),
            ),
            (
                "min_sequence_number".into(),
                V::Long(e.min_sequence_number.unwrap_or(0)),
            ),
            ("added_snapshot_id".into(), V::Long(e.added_snapshot_id)),
            (
                "added_files_count".into(),
                V::Int(e.added_files_count.unwrap_or(0) as i32),
            ),
            ("existing_files_count".into(), V::Int(0)),
            ("deleted_files_count".into(), V::Int(0)),
            (
                "added_rows_count".into(),
                V::Long(e.added_rows_count.unwrap_or(0)),
            ),
            ("existing_rows_count".into(), V::Long(0)),
            ("deleted_rows_count".into(), V::Long(0)),
            // unpartitioned: null partition summary
            ("partitions".into(), V::Union(0, Box::new(V::Null))),
            ("key_metadata".into(), V::Union(0, Box::new(V::Null))),
        ]);
        writer
            .append(row)
            .map_err(|e| StorageError::Config(format!("manifest-list append: {e}")))?;
    }
    writer
        .into_inner()
        .map_err(|e| StorageError::Config(format!("manifest-list close: {e}")))
}

/// Identity/lineage parameters of the snapshot being assembled.
#[derive(Debug, Clone)]
pub struct SnapshotParams {
    pub snapshot_id: i64,
    pub timestamp_ms: i64,
    /// The base we planned against (doc 07 §2) — None for a first snapshot.
    pub parent_snapshot_id: Option<i64>,
    pub last_sequence_number: Option<i64>,
    pub schema_id: Option<i64>,
    pub partition_spec_id: i32,
    /// The base snapshot's LIVE manifests, parsed by the caller from its
    /// manifest-list. CRITICAL: an append must carry the base's live data
    /// AND delete manifests forward, or the child snapshot drops
    /// everything the base contained (13-implementation-review blocker
    /// #1). Empty = first snapshot.
    pub carried_manifests: Vec<crate::manifest::ManifestListEntry>,
    /// Files this snapshot removes (COW overwrite). Empty = plain append.
    /// Non-empty flips the summary operation to "overwrite".
    pub deleted_files: Vec<DeletedDataFile>,
}

/// Result of assembling a snapshot on the object store.
pub struct WrittenSnapshot {
    /// The AddSnapshot payload (manifest_list points at the written avro).
    pub snapshot: Snapshot,
    /// Object keys written (manifest + manifest-list) — surfaced so the
    /// caller can audit/clean on abort.
    pub written_keys: Vec<String>,
}

/// Assemble and WRITE a snapshot for `new_files`: manifest avro +
/// manifest-list avro under `<location>/metadata/`, plus the Snapshot
/// struct for the AddSnapshot update. `parent_snapshot_id` is the base we
/// planned against (doc 07 §2); `sequence` = base.last_sequence + 1.
///
pub fn write_snapshot(
    io: &StorageIo,
    location: &str,
    new_files: &[NewDataFile],
    params: SnapshotParams,
) -> Result<WrittenSnapshot, StorageError> {
    let SnapshotParams {
        snapshot_id,
        timestamp_ms,
        parent_snapshot_id,
        last_sequence_number,
        schema_id,
        partition_spec_id,
        carried_manifests,
        deleted_files,
    } = params;
    if new_files.is_empty() && deleted_files.is_empty() {
        return Err(StorageError::Config(
            "write path: no data files in snapshot (empty commits are a no-op)".into(),
        ));
    }
    let sequence = last_sequence_number.unwrap_or(0) + 1;
    let loc = location.trim_end_matches('/').to_string();

    // StorageIo key semantics: S3 keys are relative to the BUCKET (the
    // location's path prefix must be re-appended), Local keys are
    // relative to the location root (no prefix).
    let key_prefix = match crate::storage::parse_location(location)? {
        crate::storage::Location::S3 { key, .. } => key.trim_matches('/').to_string(),
        // Local roots carry the prefix; Http locations aren't writable here
        _ => String::new(),
    };
    let keyed = |rel: String| -> String {
        if key_prefix.is_empty() {
            rel
        } else {
            format!("{key_prefix}/{rel}")
        }
    };

    // manifest avro (the ADDED entries + the DELETED entries of this
    // snapshot)
    let manifest_bytes = build_manifest_bytes(new_files, &deleted_files, snapshot_id, sequence)?;
    let manifest_key = keyed(format!(
        "metadata/{}-{}-m0.avro",
        snapshot_id,
        Uuid::new_v4()
    ));
    io.put_bytes(&manifest_key, manifest_bytes.clone())?;
    let manifest_path = format!(
        "{loc}/{}",
        manifest_key
            .strip_prefix(&format!("{key_prefix}/"))
            .unwrap_or(&manifest_key)
    );

    // manifest-list avro: carried base manifests + this snapshot's
    let list_entry = crate::manifest::ManifestListEntry {
        manifest_path,
        manifest_length: manifest_bytes.len() as i64,
        partition_spec_id,
        content: Some(0),
        added_snapshot_id: snapshot_id,
        added_files_count: Some(new_files.len() as i64),
        existing_files_count: Some(0),
        deleted_files_count: Some(deleted_files.len() as i64),
        added_rows_count: Some(new_files.iter().map(|f| f.record_count).sum()),
        existing_rows_count: Some(0),
        deleted_rows_count: Some(deleted_files.iter().map(|f| f.record_count).sum()),
        sequence_number: Some(sequence),
        min_sequence_number: Some(sequence),
    };
    let carried_len = carried_manifests.len();
    let mut list_entries = carried_manifests;
    list_entries.push(list_entry);
    let list_bytes = build_manifest_list_bytes(&list_entries)?;
    let list_rel = format!("metadata/snap-{}-0-{}.avro", snapshot_id, Uuid::new_v4());
    let list_key = keyed(list_rel.clone());
    io.put_bytes(&list_key, list_bytes)?;

    // summary per the spec's snapshot summary keys (writers may add more);
    // any deletion flips the operation to overwrite (Iceberg convention:
    // a snapshot that removes data files is an overwrite, not an append)
    let total_rows: i64 = new_files.iter().map(|f| f.record_count).sum();
    let total_bytes: i64 = new_files.iter().map(|f| f.file_size_in_bytes).sum();
    let deleted_rows: i64 = deleted_files.iter().map(|f| f.record_count).sum();
    let operation = if deleted_files.is_empty() {
        "append"
    } else {
        "overwrite"
    };
    let mut summary = BTreeMap::from([
        ("operation".to_string(), operation.to_string()),
        ("added-data-files".to_string(), new_files.len().to_string()),
        ("carried-manifests".to_string(), carried_len.to_string()),
        ("added-records".to_string(), total_rows.to_string()),
        ("added-files-size".to_string(), total_bytes.to_string()),
    ]);
    if !deleted_files.is_empty() {
        summary.insert("deleted-data-files".to_string(), deleted_files.len().to_string());
        summary.insert("deleted-records".to_string(), deleted_rows.to_string());
    }
    let snapshot = Snapshot {
        snapshot_id,
        sequence_number: Some(sequence),
        first_row_id: None,
        parent_snapshot_id,
        timestamp_ms,
        manifest_list: format!("{loc}/{list_rel}"),
        summary,
        schema_id,
    };
    Ok(WrittenSnapshot {
        snapshot,
        written_keys: vec![manifest_key, list_key],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::metadata::{
        FormatVersion, IcebergType, IcebergTypeMarker, Schema, SnapshotRef, TableMetadata,
    };
    use crate::storage::parse_location;
    use std::collections::BTreeMap;

    fn base_metadata(location: &str) -> TableMetadata {
        TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: Uuid::nil(),
            location: location.to_string(),
            last_sequence_number: Some(3),
            schemas: vec![Schema {
                struct_type: IcebergTypeMarker::Struct,
                schema_id: 0,
                fields: vec![crate::catalog::metadata::Field {
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
            properties: BTreeMap::new(),
            partition_specs: vec![],
            default_partition_spec_id: Some(0),
            refs: BTreeMap::new(),
        }
    }

    /// TDD anchor (M2 slice 27a): BatchWriter data files -> snapshot
    /// assembly (manifest + manifest-list avro on the store) -> merged
    /// metadata -> OUR OWN plan_scan reads the rows back. The full write
    /// pipeline proven without a catalog.
    #[test]
    fn write_snapshot_end_to_end_readback() {
        let dir = tempfile::tempdir().unwrap();
        crate::storage::test_allow_local_stores();
        let location = format!("file://{}", dir.path().display());
        let loc = parse_location(&location).unwrap();
        let io = StorageIo::open(&loc, None).unwrap();

        let md = base_metadata(&location);

        // 1. data files (slice 24 writer)
        let mut w = crate::writer::BatchWriter::new(vec![crate::writer::WriteColumn {
            name: "id".into(),
            ty: IcebergType::Long,
            field_id: None,
        }])
        .unwrap();
        w.push(&[crate::parquet::CellValue::Int64(1)]).unwrap();
        w.push(&[crate::parquet::CellValue::Int64(2)]).unwrap();
        let wf = w.write_file(&io, "data", 0).unwrap();
        let new_files = vec![NewDataFile {
            file_path: format!("{location}/{}", wf.key),
            record_count: wf.record_count,
            file_size_in_bytes: wf.file_size_in_bytes,
            ..Default::default()
        }];

        // 2. snapshot assembly (this module)
        let snap_id = 900;
        let written = write_snapshot(
            &io,
            &location,
            &new_files,
            SnapshotParams {
                snapshot_id: snap_id,
                timestamp_ms: 1_700_000_000_000,
                parent_snapshot_id: None,
                last_sequence_number: md.last_sequence_number,
                schema_id: Some(0),
                partition_spec_id: 0,
                carried_manifests: Vec::new(),
                deleted_files: Vec::new(),
            },
        )
        .expect("write_snapshot");
        assert_eq!(written.snapshot.snapshot_id, snap_id);
        assert_eq!(
            written.snapshot.parent_snapshot_id, None,
            "first snapshot has no parent"
        );
        assert_eq!(
            written
                .snapshot
                .summary
                .get("operation")
                .map(String::as_str),
            Some("append")
        );
        assert_eq!(written.snapshot.sequence_number, Some(4), "base seq + 1");
        assert_eq!(written.written_keys.len(), 2, "manifest + manifest-list");

        // both avro files exist on the store
        for k in &written.written_keys {
            assert!(!io.get_bytes(k).unwrap().is_empty(), "{k} non-empty");
        }

        // 3. merge into metadata (what the commit update produces
        // remotely; locally we simulate the post-commit state)
        let mut md2 = md;
        md2.snapshots.push(written.snapshot.clone());
        md2.current_snapshot_id = Some(snap_id);
        md2.refs.insert(
            "main".to_string(),
            SnapshotRef {
                snapshot_id: snap_id,
                kind: "branch".into(),
                min_snapshots_to_keep: None,
                max_snapshot_age_ms: None,
            },
        );

        // 4. read back through OUR scan pipeline
        let plan = crate::scan::plan_scan(&md2, None, None, None).expect("plan_scan");
        assert_eq!(plan.snapshot_id, snap_id);
        assert_eq!(plan.data_files, 1);
        assert_eq!(plan.total_rows, 2);
        assert_eq!(plan.tasks.len(), 1);
        assert_eq!(
            plan.tasks[0].entry.data_file.file_path,
            new_files[0].file_path
        );

        let (rio, rkey) =
            crate::scan::open_for(&plan.tasks[0].entry.data_file.file_path, None).unwrap();
        let mut rows = crate::parquet::ParquetRows::open(&rio, &rkey, &["id"]).unwrap();
        assert!(rows.next());
        assert_eq!(rows.value(0), crate::parquet::CellValue::Int64(1));
        assert!(rows.next());
        assert_eq!(rows.value(0), crate::parquet::CellValue::Int64(2));
        assert!(!rows.next());
    }

    /// The written manifest parses back through OUR reader with the
    /// expected ADDED entry + metrics (embedded-schema round trip).
    #[test]
    fn written_manifest_parses_back() {
        let files = vec![NewDataFile {
            file_path: "file://w/t/data/5-0.parquet".into(),
            record_count: 7,
            file_size_in_bytes: 4096,
            column_sizes: BTreeMap::from([(1, 64)]),
            value_counts: BTreeMap::from([(1, 7)]),
            null_value_counts: BTreeMap::from([(1, 2)]),
            lower_bounds: BTreeMap::from([(1, 1i64.to_le_bytes().to_vec())]),
            upper_bounds: BTreeMap::from([(1, 9i64.to_le_bytes().to_vec())]),
        }];
        let bytes = build_manifest_bytes(&files, &[], 42, 5).unwrap();

        let entries = crate::manifest::parse_manifest(&bytes).unwrap();
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(e.status, crate::manifest::ManifestEntryStatus::Added);
        assert_eq!(e.snapshot_id, 42);
        assert_eq!(e.sequence_number, Some(5));
        assert_eq!(e.file_sequence_number, Some(0));
        assert_eq!(e.data_file.content, DataContent::Data);
        assert_eq!(e.data_file.file_path, files[0].file_path);
        assert_eq!(e.data_file.record_count, 7);
        assert_eq!(e.data_file.column_sizes.get(&1), Some(&64));
        assert_eq!(e.data_file.value_counts.get(&1), Some(&7));
        assert_eq!(e.data_file.null_value_counts.get(&1), Some(&2));
        assert_eq!(
            e.data_file.lower_bounds.get(&1),
            Some(&1i64.to_le_bytes().to_vec())
        );
        assert_eq!(
            e.data_file.upper_bounds.get(&1),
            Some(&9i64.to_le_bytes().to_vec())
        );
    }

    /// The written manifest-list parses back through OUR reader.
    #[test]
    fn written_manifest_list_parses_back() {
        let manifest_bytes = build_manifest_bytes(
            &[NewDataFile {
                file_path: "file://w/t/data/1-0.parquet".into(),
                record_count: 3,
                file_size_in_bytes: 2048,
                ..Default::default()
            }],
            &[],
            42,
            5,
        )
        .unwrap();
        let bytes = build_manifest_list_bytes(&[crate::manifest::ManifestListEntry {
            manifest_path: "file://w/t/metadata/m0.avro".into(),
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
            sequence_number: Some(5),
            min_sequence_number: Some(5),
        }])
        .unwrap();

        let list = crate::manifest::parse_manifest_list(&bytes).unwrap();
        assert_eq!(list.len(), 1);
        let e = &list[0];
        assert_eq!(e.manifest_path, "file://w/t/metadata/m0.avro");
        assert_eq!(e.manifest_length, manifest_bytes.len() as i64);
        assert_eq!(e.partition_spec_id, 0);
        assert_eq!(e.added_snapshot_id, 42);
    }
}

#[cfg(test)]
mod carry_tests {
    use super::*;
    use crate::catalog::metadata::{
        FormatVersion, IcebergType, IcebergTypeMarker, Schema, SnapshotRef, TableMetadata,
    };
    use crate::storage::parse_location;
    use std::collections::BTreeMap;

    fn one_col_metadata(location: &str) -> TableMetadata {
        TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: uuid::Uuid::nil(),
            location: location.to_string(),
            last_sequence_number: Some(0),
            schemas: vec![Schema {
                struct_type: IcebergTypeMarker::Struct,
                schema_id: 0,
                fields: vec![crate::catalog::metadata::Field {
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
            properties: BTreeMap::new(),
            partition_specs: vec![],
            default_partition_spec_id: Some(0),
            refs: BTreeMap::new(),
        }
    }

    /// ACCEPTANCE (13-implementation-review blocker #1): append A, then B
    /// (separate sessions/keys). The child snapshot B must CARRY A's
    /// manifest forward; A's data file must be intact and distinct from
    /// B's; a pinned read of A still yields A's row and the current read
    /// yields A+B. A manifest with a DELETE entry is carried with its
    /// content preserved.
    #[test]
    fn append_a_then_b_preserves_history() {
        let dir = tempfile::tempdir().unwrap();
        crate::storage::test_allow_local_stores();
        let location = format!("file://{}", dir.path().display());
        let loc = parse_location(&location).unwrap();
        let io = StorageIo::open(&loc, None).unwrap();

        let mut w = crate::writer::BatchWriter::new(vec![crate::writer::WriteColumn {
            name: "id".into(),
            ty: IcebergType::Long,
            field_id: Some(1),
        }])
        .unwrap();
        w.push(&[crate::parquet::CellValue::Int64(1)]).unwrap();
        let wf_a = w.write_file(&io, "data", 0).unwrap();
        let file_a = NewDataFile {
            file_path: format!("{location}/{}", wf_a.key),
            record_count: wf_a.record_count,
            file_size_in_bytes: wf_a.file_size_in_bytes,
            ..Default::default()
        };

        // snapshot A
        let written_a = write_snapshot(
            &io,
            &location,
            std::slice::from_ref(&file_a),
            SnapshotParams {
                snapshot_id: 100,
                timestamp_ms: 1,
                parent_snapshot_id: None,
                last_sequence_number: Some(0),
                schema_id: Some(0),
                partition_spec_id: 0,
                carried_manifests: Vec::new(),
                deleted_files: Vec::new(),
            },
        )
        .unwrap();

        // append B in a FRESH session: the keys must differ from A's
        let mut w2 = crate::writer::BatchWriter::new(vec![crate::writer::WriteColumn {
            name: "id".into(),
            ty: IcebergType::Long,
            field_id: Some(1),
        }])
        .unwrap();
        w2.push(&[crate::parquet::CellValue::Int64(2)]).unwrap();
        let wf_b = w2.write_file(&io, "data", 0).unwrap();
        assert_ne!(wf_a.key, wf_b.key, "equal-size batches MUST NOT collide");
        let file_b = NewDataFile {
            file_path: format!("{location}/{}", wf_b.key),
            record_count: wf_b.record_count,
            file_size_in_bytes: wf_b.file_size_in_bytes,
            ..Default::default()
        };

        // B carries A's live manifests forward (parsed from A's list)
        let list_a_key = written_a
            .snapshot
            .manifest_list
            .trim_start_matches(&format!("{location}/"))
            .to_string();
        let carried =
            crate::manifest::parse_manifest_list(&io.get_bytes(&list_a_key).unwrap()).unwrap();
        assert_eq!(carried.len(), 1);
        let written_b = write_snapshot(
            &io,
            &location,
            std::slice::from_ref(&file_b),
            SnapshotParams {
                snapshot_id: 101,
                timestamp_ms: 2,
                parent_snapshot_id: Some(100),
                last_sequence_number: Some(1),
                schema_id: Some(0),
                partition_spec_id: 0,
                carried_manifests: carried,
                deleted_files: Vec::new(),
            },
        )
        .unwrap();

        // B's list = A's manifest + B's manifest; A's bytes intact
        let list_b = crate::manifest::parse_manifest_list(
            &io.get_bytes(
                written_b
                    .snapshot
                    .manifest_list
                    .trim_start_matches(&format!("{location}/")),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(list_b.len(), 2, "A carried + B new");
        let paths: Vec<String> = list_b.iter().map(|e| e.manifest_path.clone()).collect();
        assert!(
            paths.iter().any(|p| p.contains("/100-")),
            "A manifest carried; paths: {paths:?}"
        );
        assert!(
            paths.iter().any(|p| p.contains("/101-")),
            "B manifest present; paths: {paths:?}"
        );
        // A's data file bytes: still row 1 (not overwritten by B)
        let a_key = file_a.file_path.trim_start_matches(&format!("{location}/"));
        let mut rows = crate::parquet::ParquetRows::open(&io, a_key, &["id"]).unwrap();
        assert!(rows.next());
        assert_eq!(
            rows.value(0),
            crate::parquet::CellValue::Int64(1),
            "A's file intact"
        );
        assert!(!rows.next());

        // pinned read of A vs current: plan_scan per snapshot
        let mut md = one_col_metadata(&location);
        for (sid, wsnap) in [(100i64, &written_a.snapshot), (101, &written_b.snapshot)] {
            md.snapshots.push(wsnap.clone());
            md.current_snapshot_id = Some(sid);
            md.refs.insert(
                "main".into(),
                SnapshotRef {
                    snapshot_id: sid,
                    kind: "branch".into(),
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                },
            );
        }
        let plan_cur = crate::scan::plan_scan(&md, None, None, None).unwrap();
        assert_eq!(plan_cur.data_files, 2, "current sees A+B");
        let plan_a = crate::scan::plan_scan(
            &md,
            Some(&crate::catalog::metadata::SnapshotSelector::Id { value: 100 }),
            None,
            None,
        )
        .unwrap();
        assert_eq!(plan_a.data_files, 1, "pinned A sees only A");
        let (io_a, key_a) =
            crate::scan::open_for(&plan_a.tasks[0].entry.data_file.file_path, None).unwrap();
        let mut rows = crate::parquet::ParquetRows::open(&io_a, &key_a, &["id"]).unwrap();
        assert!(rows.next());
        assert_eq!(rows.value(0), crate::parquet::CellValue::Int64(1));
        assert!(!rows.next());
        let _ = written_b.written_keys;
    }

    /// COW overwrite (M2 slice 28): snapshot B DELETES A's data file and
    /// ADDS a rewritten one. The written manifest carries a DELETED entry
    /// for A's file (path + counts preserved), the manifest-list entry
    /// reports the deletion counts, the summary says operation=overwrite,
    /// and plan_scan over the merged metadata sees ONLY the rewrite —
    /// read-your-writes through our own pipeline, A's rows gone.
    #[test]
    fn overwrite_snapshot_deletes_and_replaces() {
        let dir = tempfile::tempdir().unwrap();
        crate::storage::test_allow_local_stores();
        let location = format!("file://{}", dir.path().display());
        let loc = parse_location(&location).unwrap();
        let io = StorageIo::open(&loc, None).unwrap();

        // A: one file, rows 1 and 2
        let mut w = crate::writer::BatchWriter::new(vec![crate::writer::WriteColumn {
            name: "id".into(),
            ty: IcebergType::Long,
            field_id: Some(1),
        }])
        .unwrap();
        w.push(&[crate::parquet::CellValue::Int64(1)]).unwrap();
        w.push(&[crate::parquet::CellValue::Int64(2)]).unwrap();
        let wf_a = w.write_file(&io, "data", 0).unwrap();
        let file_a_path = format!("{location}/{}", wf_a.key);
        let written_a = write_snapshot(
            &io,
            &location,
            &[NewDataFile {
                file_path: file_a_path.clone(),
                record_count: wf_a.record_count,
                file_size_in_bytes: wf_a.file_size_in_bytes,
                ..Default::default()
            }],
            SnapshotParams {
                snapshot_id: 200,
                timestamp_ms: 1,
                parent_snapshot_id: None,
                last_sequence_number: Some(0),
                schema_id: Some(0),
                partition_spec_id: 0,
                carried_manifests: Vec::new(),
                deleted_files: Vec::new(),
            },
        )
        .unwrap();

        // B: rewrite file (row 2 only) + DELETE A's file
        let mut w2 = crate::writer::BatchWriter::new(vec![crate::writer::WriteColumn {
            name: "id".into(),
            ty: IcebergType::Long,
            field_id: Some(1),
        }])
        .unwrap();
        w2.push(&[crate::parquet::CellValue::Int64(2)]).unwrap();
        let wf_b = w2.write_file(&io, "data", 1).unwrap();
        let file_b_path = format!("{location}/{}", wf_b.key);
        let deleted = vec![crate::snapshot_write::DeletedDataFile {
            file_path: file_a_path.clone(),
            record_count: wf_a.record_count,
            file_size_in_bytes: wf_a.file_size_in_bytes,
        }];
        let carried = {
            let list_a_key = written_a
                .snapshot
                .manifest_list
                .trim_start_matches(&format!("{location}/"))
                .to_string();
            crate::manifest::parse_manifest_list(&io.get_bytes(&list_a_key).unwrap()).unwrap()
        };
        let written_b = write_snapshot(
            &io,
            &location,
            &[NewDataFile {
                file_path: file_b_path.clone(),
                record_count: wf_b.record_count,
                file_size_in_bytes: wf_b.file_size_in_bytes,
                ..Default::default()
            }],
            SnapshotParams {
                snapshot_id: 201,
                timestamp_ms: 2,
                parent_snapshot_id: Some(200),
                last_sequence_number: Some(1),
                schema_id: Some(0),
                partition_spec_id: 0,
                carried_manifests: carried,
                deleted_files: deleted,
            },
        )
        .unwrap();

        // summary: overwrite operation + deletion counts
        assert_eq!(
            written_b.snapshot.summary.get("operation").map(String::as_str),
            Some("overwrite")
        );
        assert_eq!(
            written_b.snapshot.summary.get("deleted-data-files").map(String::as_str),
            Some("1")
        );
        assert_eq!(
            written_b.snapshot.summary.get("deleted-records").map(String::as_str),
            Some("2")
        );

        // merged metadata: current view sees ONLY the rewrite
        let mut md = one_col_metadata(&location);
        for (sid, wsnap) in [(200i64, &written_a.snapshot), (201, &written_b.snapshot)] {
            md.snapshots.push(wsnap.clone());
            md.current_snapshot_id = Some(sid);
            md.refs.insert(
                "main".into(),
                SnapshotRef {
                    snapshot_id: sid,
                    kind: "branch".into(),
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                },
            );
        }
        let plan = crate::scan::plan_scan(&md, None, None, None).unwrap();
        assert_eq!(plan.data_files, 1, "A deleted, only B's rewrite lives");
        assert_eq!(plan.tasks[0].entry.data_file.file_path, file_b_path);
        assert_eq!(plan.total_rows, 1);

        // time travel to A still works (the DELETED entry lives in B's
        // manifest; A's snapshot manifest-list doesn't reference it)
        let plan_a = crate::scan::plan_scan(
            &md,
            Some(&crate::catalog::metadata::SnapshotSelector::Id { value: 200 }),
            None,
            None,
        )
        .unwrap();
        assert_eq!(plan_a.data_files, 1, "pinned A is untouched");
        assert_eq!(plan_a.tasks[0].entry.data_file.file_path, file_a_path);

        // read the rewrite back
        let b_key = file_b_path.trim_start_matches(&format!("{location}/"));
        let mut rows = crate::parquet::ParquetRows::open(&io, b_key, &["id"]).unwrap();
        assert!(rows.next());
        assert_eq!(rows.value(0), crate::parquet::CellValue::Int64(2));
        assert!(!rows.next());
    }
}
