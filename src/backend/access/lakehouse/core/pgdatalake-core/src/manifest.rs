//! Manifest-list / manifest Avro readers (Iceberg spec, specs/lakehouse/
//! 06-execution.md scan planning). Slice 5: manifest-list entries.

//! Manifest-list / manifest Avro readers (Iceberg spec, specs/lakehouse/
//! 06-execution.md scan planning). Slice 5: manifest-list entries. Manifest
//! (data-file) records land with the scan-planning slice.

use apache_avro::{Reader as AvroReader, Schema as AvroSchema};
use serde::{Deserialize, Serialize};

use crate::storage::StorageError;

/// Iceberg manifest-list Avro schema, v2 shape (spec § manifest-list).
/// Same reader handles v1 files: v1-only field names (added_rows_count vs
/// added_rows etc.) differ — see parse note below.
pub const MANIFEST_LIST_SCHEMA_V2: &str = r#"{
    "type": "record",
    "name": "manifest_file",
    "fields": [
        {"name": "manifest_path", "type": "string"},
        {"name": "manifest_length", "type": "long"},
        {"name": "partition_spec_id", "type": "int"},
        {"name": "added_snapshot_id", "type": "long"},
        {"name": "added_files_count", "type": ["null", "long"], "default": null},
        {"name": "existing_files_count", "type": ["null", "long"], "default": null},
        {"name": "deleted_files_count", "type": ["null", "long"], "default": null},
        {"name": "added_rows_count", "type": ["null", "long"], "default": null},
        {"name": "existing_rows_count", "type": ["null", "long"], "default": null},
        {"name": "deleted_rows_count", "type": ["null", "long"], "default": null},
        {"name": "partitions", "type": ["null", {"type": "array", "items": {
            "name": "r2", "type": "record", "fields": [
                {"name": "contains_null", "type": "boolean"},
                {"name": "contains_nan", "type": "boolean", "default": false},
                {"name": "lower_bound", "type": ["null", "string"]},
                {"name": "upper_bound", "type": ["null", "string"]}
            ]}}], "default": null},
        {"name": "key_metadata", "type": ["null", "bytes"], "default": null},
        {"name": "sequence_number", "type": ["null", "long"], "default": null},
        {"name": "min_sequence_number", "type": ["null", "long"], "default": null}
    ]
}"#;

/// One entry of a manifest list — a pointer to a manifest file plus the
/// file/row counts the scan planner prunes with (doc 06 §2).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManifestListEntry {
    pub manifest_path: String,
    pub manifest_length: i64,
    pub partition_spec_id: i32,
    /// v2: 0 = data, 1 = position deletes, 2 = equality deletes (None for
    /// v1 files). Needed to carry manifests forward across snapshots.
    pub content: Option<i32>,
    pub added_snapshot_id: i64,
    pub added_files_count: Option<i64>,
    pub existing_files_count: Option<i64>,
    pub deleted_files_count: Option<i64>,
    pub added_rows_count: Option<i64>,
    pub existing_rows_count: Option<i64>,
    pub deleted_rows_count: Option<i64>,
    /// v2: sequence number the manifest was written with (None for v1 files).
    pub sequence_number: Option<i64>,
    /// v2: minimum sequence number of any file in the manifest.
    pub min_sequence_number: Option<i64>,
}

/// Parse a manifest-list Avro blob. Avro files embed their own writer
/// schema; we validate it against the spec shape rather than trusting the
/// embedded schema blindly (metadata fuzz target, doc 10 §4).
pub fn parse_manifest_list(bytes: &[u8]) -> Result<Vec<ManifestListEntry>, StorageError> {
    let reader = AvroReader::new(bytes)
        .map_err(|e| StorageError::Decode(format!("manifest-list avro: {e}")))?;
    validate_schema(reader.writer_schema())?;
    let mut out = Vec::new();
    for value in reader {
        let v = value.map_err(|e| StorageError::Decode(format!("manifest-list entry: {e}")))?;
        out.push(entry_from_value(&v)?);
    }
    Ok(out)
}

fn entry_from_value(v: &apache_avro::types::Value) -> Result<ManifestListEntry, StorageError> {
    if let apache_avro::types::Value::Record(fields) = v {
        // counts/ids are `long` per spec; real writers (pyiceberg 0.12)
        // sometimes emit `int` — accept the Avro promotion (slice 12 interop)
        let get_i64 = |k: &str| -> Result<Option<i64>, StorageError> {
            let as_i64 = |v: &apache_avro::types::Value| -> Result<Option<i64>, StorageError> {
                match v {
                    apache_avro::types::Value::Long(n) => Ok(Some(*n)),
                    apache_avro::types::Value::Int(n) => Ok(Some(*n as i64)),
                    apache_avro::types::Value::Null => Ok(None),
                    other => Err(StorageError::Decode(format!("{k}: {other:?}"))),
                }
            };
            match fields.iter().find(|(fk, _)| fk == k) {
                Some((_, apache_avro::types::Value::Union(_, inner))) => as_i64(inner),
                plain => {
                    let default_null = apache_avro::types::Value::Null;
                    let v = plain.map(|(_, v)| v).unwrap_or(&default_null);
                    as_i64(v)
                }
            }
        };
        let get_str = |k: &str| -> Result<String, StorageError> {
            match fields.iter().find(|(fk, _)| fk == k) {
                Some((_, apache_avro::types::Value::String(s))) => Ok(s.clone()),
                other => Err(StorageError::Decode(format!("{k}: {other:?}"))),
            }
        };
        let spec = match fields.iter().find(|(fk, _)| fk == "partition_spec_id") {
            Some((_, apache_avro::types::Value::Int(i))) => *i,
            other => {
                return Err(StorageError::Decode(format!(
                    "partition_spec_id: {other:?}"
                )))
            }
        };
        let content = match fields.iter().find(|(fk, _)| fk == "content") {
            Some((_, apache_avro::types::Value::Union(_, inner))) => match **inner {
                apache_avro::types::Value::Int(i) => Some(i),
                apache_avro::types::Value::Null => None,
                ref o => return Err(StorageError::Decode(format!("content: {o:?}"))),
            },
            Some((_, apache_avro::types::Value::Int(i))) => Some(*i),
            _ => None,
        };
        return Ok(ManifestListEntry {
            manifest_path: get_str("manifest_path")?,
            manifest_length: get_i64("manifest_length")?
                .ok_or_else(|| StorageError::Decode("manifest_length: null".into()))?,
            partition_spec_id: spec,
            content,
            added_snapshot_id: get_i64("added_snapshot_id")?
                .ok_or_else(|| StorageError::Decode("added_snapshot_id: null".into()))?,
            added_files_count: get_i64("added_files_count")?,
            existing_files_count: get_i64("existing_files_count")?,
            deleted_files_count: get_i64("deleted_files_count")?,
            added_rows_count: get_i64("added_rows_count")?,
            existing_rows_count: get_i64("existing_rows_count")?,
            deleted_rows_count: get_i64("deleted_rows_count")?,
            sequence_number: get_i64("sequence_number")?,
            min_sequence_number: get_i64("min_sequence_number")?,
        });
    }
    Err(StorageError::Decode(format!(
        "manifest-list entry is not a record: {v:?}"
    )))
}

/// Validate that the embedded writer schema has the required top-level
/// manifest-list fields; tolerant of profile/spec extra fields.
fn validate_schema(schema: &AvroSchema) -> Result<(), StorageError> {
    let names: Vec<&str> = match schema {
        AvroSchema::Record(r) => r.fields.iter().map(|f| f.name.as_str()).collect(),
        _ => return Err(StorageError::Decode("manifest-list is not a record".into())),
    };
    for required in [
        "manifest_path",
        "manifest_length",
        "partition_spec_id",
        "added_snapshot_id",
    ] {
        if !names.contains(&required) {
            return Err(StorageError::Decode(format!(
                "manifest-list schema missing required field {required}"
            )));
        }
    }
    Ok(())
}

/// Iceberg manifest Avro schema, v2 shape (spec § manifest). NOTE: unlike
/// manifest-list, real manifests inject per-table partition fields into
/// `data_file.partition`; production parsing always uses the EMBEDDED
/// writer schema (apache-avro does this), so this constant is the
/// spec-shaped reference used for validation and tests.
pub const MANIFEST_ENTRY_SCHEMA_V2: &str = r#"{
    "type": "record",
    "name": "manifest_entry",
    "fields": [
        {"name": "status", "type": "int", "default": 1},
        {"name": "snapshot_id", "type": "long", "default": -1},
        {"name": "sequence_number", "type": ["null", "long"]},
        {"name": "file_sequence_number", "type": ["null", "long"]},
        {"name": "data_file", "type": {
            "type": "record", "name": "data_file",
            "fields": [
                {"name": "content", "type": "int", "default": 0},
                {"name": "file_path", "type": "string"},
                {"name": "file_format", "type": "string"},
                {"name": "partition", "type": ["null", {"type": "record", "name": "r_part", "fields": []}], "default": null},
                {"name": "record_count", "type": "long"},
                {"name": "file_size_in_bytes", "type": "long"},
                {"name": "column_sizes", "type": ["null", {"type": "array", "items": {"type": "record", "name": "kv_long", "fields": [{"name": "key", "type": "int"}, {"name": "value", "type": "long"}]}}], "default": null},
                {"name": "value_counts", "type": ["null", {"type": "array", "items": {"type": "record", "name": "kv_long_v2", "fields": [{"name": "key", "type": "int"}, {"name": "value", "type": "long"}]}}], "default": null},
                {"name": "null_value_counts", "type": ["null", {"type": "array", "items": {"type": "record", "name": "kv_long_v3", "fields": [{"name": "key", "type": "int"}, {"name": "value", "type": "long"}]}}], "default": null},
                {"name": "nan_value_counts", "type": ["null", {"type": "array", "items": {"type": "record", "name": "kv_long_v4", "fields": [{"name": "key", "type": "int"}, {"name": "value", "type": "long"}]}}], "default": null},
                {"name": "lower_bounds", "type": ["null", {"type": "array", "items": {"type": "record", "name": "kv_bytes_v1", "fields": [{"name": "key", "type": "int"}, {"name": "value", "type": "bytes"}]}}], "default": null},
                {"name": "upper_bounds", "type": ["null", {"type": "array", "items": {"type": "record", "name": "kv_bytes_v2", "fields": [{"name": "key", "type": "int"}, {"name": "value", "type": "bytes"}]}}], "default": null},
                {"name": "key_metadata", "type": ["null", "bytes"], "default": null},
                {"name": "split_offsets", "type": ["null", {"type": "array", "items": "int"}], "default": null},
                {"name": "equality_ids", "type": ["null", {"type": "array", "items": "int"}], "default": null},
                {"name": "sort_order_id", "type": ["null", "int"], "default": null}
            ]
        }}
    ]
}"#;

/// Entry status (spec § manifests).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestEntryStatus {
    Existing = 0,
    Added = 1,
    Deleted = 2,
}

/// File content type: data, position deletes, or equality deletes (v2
/// read-compat; v3 deletion vectors live in the snapshot summary and land
/// with their own slice).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataContent {
    Data = 0,
    PositionDeletes = 1,
    EqualityDeletes = 2,
}

/// One row of a manifest: a data/delete file with the column-level stats
/// the scan planner prunes with (doc 06 §2).
#[derive(Debug, Clone, PartialEq)]
pub struct ManifestEntry {
    pub status: ManifestEntryStatus,
    pub snapshot_id: i64,
    pub sequence_number: Option<i64>,
    pub file_sequence_number: Option<i64>,
    pub data_file: DataFile,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DataFile {
    pub content: DataContent,
    pub file_path: String,
    pub file_format: String,
    /// Per-table partition values; kept opaque until partition-spec-aware
    /// pruning (slice with partition_specs, doc 06 §2).
    pub partition: Option<serde_json::Value>,
    pub record_count: i64,
    pub file_size_in_bytes: i64,
    pub column_sizes: std::collections::BTreeMap<i64, i64>,
    pub value_counts: std::collections::BTreeMap<i64, i64>,
    pub null_value_counts: std::collections::BTreeMap<i64, i64>,
    pub nan_value_counts: std::collections::BTreeMap<i64, i64>,
    pub lower_bounds: std::collections::BTreeMap<i64, Vec<u8>>,
    pub upper_bounds: std::collections::BTreeMap<i64, Vec<u8>>,
    pub key_metadata: Option<Vec<u8>>,
    pub split_offsets: Option<Vec<i32>>,
    pub equality_ids: Option<Vec<i32>>,
    pub sort_order_id: Option<i32>,
}

/// Parse a manifest Avro blob using its EMBEDDED writer schema (required:
/// the partition struct is per-table). Known fields are mapped by name;
/// unknown extra fields are ignored (forward tolerance).
pub fn parse_manifest(bytes: &[u8]) -> Result<Vec<ManifestEntry>, StorageError> {
    let reader =
        AvroReader::new(bytes).map_err(|e| StorageError::Decode(format!("manifest avro: {e}")))?;
    validate_manifest_schema(reader.writer_schema())?;
    let mut out = Vec::new();
    for value in reader {
        let v = value.map_err(|e| StorageError::Decode(format!("manifest entry: {e}")))?;
        out.push(manifest_entry_from_value(&v)?);
    }
    Ok(out)
}

fn validate_manifest_schema(schema: &AvroSchema) -> Result<(), StorageError> {
    let df_required = |names: &[&str], req: &[&str]| req.iter().all(|r| names.contains(r));
    let AvroSchema::Record(top) = schema else {
        return Err(StorageError::Decode("manifest is not a record".into()));
    };
    let names: Vec<&str> = top.fields.iter().map(|f| f.name.as_str()).collect();
    if !df_required(&names, &["status", "snapshot_id", "data_file"]) {
        return Err(StorageError::Decode(
            "manifest schema missing required entry fields".into(),
        ));
    }
    let df = top
        .fields
        .iter()
        .find(|f| f.name == "data_file")
        .map(|f| &f.schema);
    let Some(AvroSchema::Record(df_rec)) = df else {
        return Err(StorageError::Decode("data_file is not a record".into()));
    };
    let df_names: Vec<&str> = df_rec.fields.iter().map(|f| f.name.as_str()).collect();
    if !df_required(
        &df_names,
        &[
            "file_path",
            "file_format",
            "record_count",
            "file_size_in_bytes",
        ],
    ) {
        return Err(StorageError::Decode(
            "manifest schema missing required data_file fields".into(),
        ));
    }
    Ok(())
}

fn manifest_entry_from_value(v: &apache_avro::types::Value) -> Result<ManifestEntry, StorageError> {
    use apache_avro::types::Value as V;
    let V::Record(fields) = v else {
        return Err(StorageError::Decode(
            "manifest entry is not a record".into(),
        ));
    };
    let find = |k: &str| fields.iter().find(|(fk, _)| fk == k);
    let status = match find("status") {
        Some((_, V::Int(i))) => match *i {
            0 => ManifestEntryStatus::Existing,
            1 => ManifestEntryStatus::Added,
            2 => ManifestEntryStatus::Deleted,
            other => return Err(StorageError::Decode(format!("status {other} out of range"))),
        },
        other => return Err(StorageError::Decode(format!("status: {other:?}"))),
    };
    // pyiceberg writes snapshot_id as a nullable union; accept bare
    // Int/Long too (same promotion rules as the manifest-list counts)
    let snapshot_id = match find("snapshot_id") {
        Some((_, V::Union(_, inner))) => match inner.as_ref() {
            V::Long(n) => *n,
            V::Int(n) => *n as i64,
            other => return Err(StorageError::Decode(format!("snapshot_id: {other:?}"))),
        },
        Some((_, V::Long(n))) => *n,
        Some((_, V::Int(n))) => *n as i64,
        other => return Err(StorageError::Decode(format!("snapshot_id: {other:?}"))),
    };
    let opt_long = |k: &str| -> Result<Option<i64>, StorageError> {
        match find(k) {
            None => Ok(None),
            Some((_, V::Null)) => Ok(None),
            Some((_, V::Union(_, inner))) => match inner.as_ref() {
                V::Long(n) => Ok(Some(*n)),
                V::Null => Ok(None),
                o => Err(StorageError::Decode(format!("{k}: {o:?}"))),
            },
            Some((_, V::Long(n))) => Ok(Some(*n)),
            Some((k2, o)) => Err(StorageError::Decode(format!("{k2}: {o:?}"))),
        }
    };
    let data_file_val = match find("data_file") {
        Some((_, d)) => d,
        None => {
            return Err(StorageError::Decode(
                "manifest entry missing data_file".into(),
            ))
        }
    };
    let V::Record(df_fields) = data_file_val else {
        return Err(StorageError::Decode("data_file is not a record".into()));
    };
    let df_find = |k: &str| df_fields.iter().find(|(fk, _)| fk == k);
    let df_str = |k: &str| -> Result<String, StorageError> {
        match df_find(k) {
            Some((_, V::String(x))) => Ok(x.clone()),
            o => Err(StorageError::Decode(format!("{k}: {o:?}"))),
        }
    };
    let df_long = |k: &str| -> Result<i64, StorageError> {
        match df_find(k) {
            Some((_, V::Long(x))) => Ok(*x),
            o => Err(StorageError::Decode(format!("{k}: {o:?}"))),
        }
    };
    let content = match df_find("content") {
        Some((_, V::Int(0))) | None => DataContent::Data,
        Some((_, V::Int(1))) => DataContent::PositionDeletes,
        Some((_, V::Int(2))) => DataContent::EqualityDeletes,
        Some((_, o)) => return Err(StorageError::Decode(format!("content: {o:?}"))),
    };
    let i64_map = |k: &str| -> Result<std::collections::BTreeMap<i64, i64>, StorageError> {
        let mut m = std::collections::BTreeMap::new();
        // metrics live in the array branch of the nullable union (the
        // spec Avro schema encodes them as arrays of kv records);
        // plain Avro maps are accepted leniently
        let unwrapped = match df_find(k) {
            Some((_, V::Union(_, inner))) => inner.as_ref(),
            Some((_, other)) => other,
            None => &V::Null,
        };
        match unwrapped {
            V::Array(items) => {
                for it in items {
                    let V::Record(kv) = it else {
                        return Err(StorageError::Decode(format!("{k} item: {it:?}")));
                    };
                    let key = kv.iter().find(|(fk, _)| fk == "key");
                    let value = kv.iter().find(|(fk, _)| fk == "value");
                    let (Some((_, V::Int(fid))), Some((_, V::Long(n)))) = (key, value) else {
                        return Err(StorageError::Decode(format!("{k} kv: {kv:?}")));
                    };
                    m.insert(*fid as i64, *n);
                }
            }
            V::Map(pairs) => {
                for (key, val) in pairs {
                    let V::Long(n) = val else {
                        return Err(StorageError::Decode(format!("{k}[{key}]: {val:?}")));
                    };
                    m.insert(
                        key.parse::<i64>().map_err(|e| {
                            StorageError::Decode(format!("{k}[{key}]: field id not an int: {e}"))
                        })?,
                        *n,
                    );
                }
            }
            _ => {}
        }
        Ok(m)
    };
    let bytes_map = |k: &str| -> Result<std::collections::BTreeMap<i64, Vec<u8>>, StorageError> {
        let mut m = std::collections::BTreeMap::new();
        let unwrapped = match df_find(k) {
            Some((_, V::Union(_, inner))) => inner.as_ref(),
            Some((_, other)) => other,
            None => &V::Null,
        };
        match unwrapped {
            V::Array(items) => {
                for it in items {
                    let V::Record(kv) = it else {
                        return Err(StorageError::Decode(format!("{k} item: {it:?}")));
                    };
                    let key = kv.iter().find(|(fk, _)| fk == "key");
                    let value = kv.iter().find(|(fk, _)| fk == "value");
                    let (Some((_, V::Int(fid))), Some((_, V::Bytes(b)))) = (key, value) else {
                        return Err(StorageError::Decode(format!("{k} kv: {kv:?}")));
                    };
                    m.insert(*fid as i64, b.clone());
                }
            }
            V::Map(pairs) => {
                for (key, val) in pairs {
                    let V::Bytes(b) = val else {
                        return Err(StorageError::Decode(format!("{k}[{key}]: {val:?}")));
                    };
                    m.insert(
                        key.parse::<i64>().map_err(|e| {
                            StorageError::Decode(format!("{k}[{key}]: field id not an int: {e}"))
                        })?,
                        b.clone(),
                    );
                }
            }
            _ => {}
        }
        Ok(m)
    };
    let opt_i32_list = |k: &str| -> Result<Option<Vec<i32>>, StorageError> {
        match df_find(k) {
            None | Some((_, V::Null)) => Ok(None),
            Some((_, V::Union(_, inner))) => match inner.as_ref() {
                V::Null => Ok(None),
                // single-int optional fields (sort_order_id): Trino writes
                // Int inside the union; pyiceberg writes Array or Null
                V::Int(i) => Ok(Some(vec![*i])),
                V::Long(l) => Ok(Some(vec![*l as i32])),
                V::Array(items) => {
                    // spec says int; pyiceberg writes long — accept promotion
                    let mut out = Vec::with_capacity(items.len());
                    for it in items {
                        match it {
                            V::Int(i) => out.push(*i),
                            V::Long(l) => {
                                let v = i32::try_from(*l).map_err(|e| {
                                    StorageError::Decode(format!("{k} item {l}: {e}"))
                                })?;
                                out.push(v);
                            }
                            other => {
                                return Err(StorageError::Decode(format!("{k} item: {other:?}")))
                            }
                        }
                    }
                    Ok(Some(out))
                }
                o => Err(StorageError::Decode(format!("{k}: {o:?}"))),
            },
            // Trino writes optional-typed fields (sort_order_id etc.) with a
            // PLAIN int schema and plain Int values — no union (M1 interop
            // gate, Trino-written manifests)
            Some((_, V::Int(i))) => Ok(Some(vec![*i])),
            Some((_, V::Long(l))) => Ok(Some(vec![*l as i32])),
            o => Err(StorageError::Decode(format!("{k}: {o:?}"))),
        }
    };
    let partition = match df_find("partition") {
        None => None,
        Some((_, p)) => avro_to_json(p),
    };
    Ok(ManifestEntry {
        status,
        snapshot_id,
        sequence_number: opt_long("sequence_number")?,
        file_sequence_number: opt_long("file_sequence_number")?,
        data_file: DataFile {
            content,
            file_path: df_str("file_path")?,
            file_format: df_str("file_format")?,
            partition,
            record_count: df_long("record_count")?,
            file_size_in_bytes: df_long("file_size_in_bytes")?,
            column_sizes: i64_map("column_sizes")?,
            value_counts: i64_map("value_counts")?,
            null_value_counts: i64_map("null_value_counts")?,
            nan_value_counts: i64_map("nan_value_counts")?,
            lower_bounds: bytes_map("lower_bounds")?,
            upper_bounds: bytes_map("upper_bounds")?,
            key_metadata: match df_find("key_metadata") {
                None | Some((_, V::Null)) => None,
                Some((_, V::Union(_, inner))) => match inner.as_ref() {
                    V::Null => None,
                    V::Bytes(b) => Some(b.clone()),
                    o => return Err(StorageError::Decode(format!("key_metadata: {o:?}"))),
                },
                Some((_, V::Bytes(b))) => Some(b.clone()),
                o => return Err(StorageError::Decode(format!("key_metadata: {o:?}"))),
            },
            split_offsets: opt_i32_list("split_offsets")?,
            equality_ids: opt_i32_list("equality_ids")?,
            sort_order_id: opt_i32_list("sort_order_id")?.and_then(|v| v.first().copied()),
        },
    })
}

/// Best-effort Avro Value -> JSON conversion (partition values are opaque
/// until the partition-spec slice; longs stay ints, bytes stay base64-free
/// hex arrays so nothing is lost).
fn avro_to_json(v: &apache_avro::types::Value) -> Option<serde_json::Value> {
    use apache_avro::types::Value as V;
    Some(match v {
        V::Null => serde_json::Value::Null,
        V::Boolean(b) => serde_json::Value::Bool(*b),
        V::Int(i) => serde_json::json!(i),
        V::Long(n) => serde_json::json!(n),
        V::Float(f) => serde_json::json!(f),
        V::Double(d) => serde_json::json!(d),
        V::Bytes(b) | V::Fixed(_, b) => {
            serde_json::Value::Array(b.iter().map(|x| serde_json::json!(x)).collect())
        }
        V::String(s) => serde_json::json!(s),
        V::Array(items) => {
            serde_json::Value::Array(items.iter().filter_map(avro_to_json).collect())
        }
        V::Map(pairs) => {
            let mut m = serde_json::Map::new();
            for (k, val) in pairs {
                if let Some(jv) = avro_to_json(val) {
                    m.insert(k.clone(), jv);
                }
            }
            serde_json::Value::Object(m)
        }
        V::Record(fields) => {
            let mut m = serde_json::Map::new();
            for (k, val) in fields {
                if let Some(jv) = avro_to_json(val) {
                    m.insert(k.clone(), jv);
                }
            }
            serde_json::Value::Object(m)
        }
        V::Union(_, inner) => return avro_to_json(inner),
        V::Date(d) => serde_json::json!(d),
        V::TimestampMicros(t) => serde_json::json!(t),
        V::TimestampMillis(t) => serde_json::json!(t),
        V::TimeMicros(t) => serde_json::json!(t),
        V::TimeMillis(t) => serde_json::json!(t),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// TDD anchor (M1 slice 5): an Avro file written with the Iceberg
    /// manifest-list schema parses into ManifestListEntry records. The
    /// fixture is written with the same schema the spec mandates (v2 shape:
    /// sequence numbers present).
    #[test]
    fn parses_manifest_list_avro() {
        use apache_avro::{Schema as AvroSchema, Writer};

        let schema = AvroSchema::parse_str(MANIFEST_LIST_SCHEMA_V2).unwrap();
        let mut writer = Writer::new(&schema, Vec::new());
        let mut record = apache_avro::types::Record::new(&schema).unwrap();
        record.put("manifest_path", "s3://warehouse/analytics/events/mn1.avro");
        record.put("manifest_length", 8221i64);
        record.put("partition_spec_id", 0i32);
        record.put("added_snapshot_id", 3051729675574597005i64);
        record.put("added_files_count", Some(3i64));
        record.put("existing_files_count", Some(1i64));
        record.put("deleted_files_count", Some(0i64));
        record.put("added_rows_count", Some(100i64));
        record.put("existing_rows_count", Some(5i64));
        record.put("deleted_rows_count", Some(0i64));
        record.put("partitions", None::<apache_avro::types::Value>);
        record.put("key_metadata", None::<apache_avro::types::Value>);
        writer.append(record).unwrap();
        let avro_bytes = writer.into_inner().unwrap();

        let entries = parse_manifest_list(&avro_bytes).expect("parse manifest list");
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(e.manifest_path, "s3://warehouse/analytics/events/mn1.avro");
        assert_eq!(e.manifest_length, 8221);
        assert_eq!(e.partition_spec_id, 0);
        assert_eq!(e.added_snapshot_id, 3051729675574597005);
        assert_eq!(e.added_files_count, Some(3));
        assert_eq!(e.added_rows_count, Some(100));
    }

    /// TDD anchor (M1 slice 6): a manifest Avro (embedded writer schema,
    /// per-table partition struct) parses into ManifestEntry records with
    /// data-file stats — the inputs to scan planning (doc 06 §2).
    #[test]
    fn parses_manifest_avro_entries() {
        use apache_avro::types::{Record, Value};
        use apache_avro::{Schema as AvroSchema, Writer};
        let map = |pairs: &[(&str, i64)]| {
            Value::Array(
                pairs
                    .iter()
                    .map(|(k, v)| {
                        Value::Record(vec![
                            ("key".into(), Value::Int(k.parse::<i32>().unwrap())),
                            ("value".into(), Value::Long(*v)),
                        ])
                    })
                    .collect(),
            )
        };
        let bmap = |pairs: &[(&str, u8)]| {
            Value::Array(
                pairs
                    .iter()
                    .map(|(k, v)| {
                        Value::Record(vec![
                            ("key".into(), Value::Int(k.parse::<i32>().unwrap())),
                            ("value".into(), Value::Bytes(vec![*v])),
                        ])
                    })
                    .collect(),
            )
        };

        let schema = AvroSchema::parse_str(MANIFEST_ENTRY_SCHEMA_V2).unwrap();
        let mut writer = Writer::new(&schema, Vec::new());

        let mut entry = Record::new(&schema).unwrap();
        entry.put("status", Value::Int(1)); // ADDED
        entry.put("snapshot_id", Value::Long(3051729675574597005));
        entry.put(
            "sequence_number",
            Value::Union(1, Box::new(Value::Long(34))),
        );
        entry.put(
            "file_sequence_number",
            Value::Union(1, Box::new(Value::Long(2))),
        );

        let data_file = Value::Record(vec![
            ("content".into(), Value::Int(0)), // DATA
            (
                "file_path".into(),
                Value::String("s3://warehouse/analytics/events/data/00001.parquet".into()),
            ),
            ("file_format".into(), Value::String("parquet".into())),
            (
                "partition".into(),
                Value::Union(1, Box::new(Value::Record(vec![]))),
            ),
            ("record_count".into(), Value::Long(100)),
            ("file_size_in_bytes".into(), Value::Long(5242880)),
            (
                "column_sizes".into(),
                Value::Union(1, Box::new(map(&[("1", 1024), ("2", 2048)]))),
            ),
            (
                "value_counts".into(),
                Value::Union(1, Box::new(map(&[("1", 100), ("2", 97)]))),
            ),
            (
                "null_value_counts".into(),
                Value::Union(1, Box::new(map(&[("2", 3)]))),
            ),
            (
                "nan_value_counts".into(),
                Value::Union(1, Box::new(map(&[]))),
            ),
            (
                "lower_bounds".into(),
                Value::Union(1, Box::new(bmap(&[("1", 1)]))),
            ),
            (
                "upper_bounds".into(),
                Value::Union(1, Box::new(bmap(&[("1", 64)]))),
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
        let avro_bytes = writer.into_inner().unwrap();

        let entries = parse_manifest(&avro_bytes).expect("parse manifest");
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(e.status, ManifestEntryStatus::Added);
        assert_eq!(e.snapshot_id, 3051729675574597005);
        assert_eq!(e.sequence_number, Some(34));
        assert_eq!(e.data_file.content, DataContent::Data);
        assert_eq!(
            e.data_file.file_path,
            "s3://warehouse/analytics/events/data/00001.parquet"
        );
        assert_eq!(e.data_file.record_count, 100);
        assert_eq!(e.data_file.file_size_in_bytes, 5242880);
        assert_eq!(e.data_file.column_sizes.get(&1), Some(&1024));
        assert_eq!(e.data_file.null_value_counts.get(&2), Some(&3));
        assert_eq!(e.data_file.split_offsets, Some(vec![4]));
    }

    /// Interop regression (slice 12): PyIceberg 0.12 writes manifest-list
    /// counts as Avro INT in some versions; the reader must accept both
    /// INT and LONG per Avro promotion rules (spec says long, real writers
    /// vary — be liberal in what you accept for counts).
    #[test]
    fn parses_int_typed_counts_from_pyiceberg() {
        use apache_avro::types::Record;
        use apache_avro::Writer;
        // counts declared as int in the writer schema (pyiceberg shape)
        let schema_str = MANIFEST_LIST_SCHEMA_V2
            .replace(
                "\"added_files_count\", \"type\": [\"null\", \"long\"]",
                "\"added_files_count\", \"type\": [\"null\", \"int\"]",
            )
            .replace(
                "\"added_rows_count\", \"type\": [\"null\", \"long\"]",
                "\"added_rows_count\", \"type\": [\"null\", \"int\"]",
            );
        let schema = AvroSchema::parse_str(&schema_str).unwrap();
        let mut writer = Writer::new(&schema, Vec::new());
        let mut rec = Record::new(&schema).unwrap();
        rec.put("manifest_path", "s3://warehouse/t/mn.avro");
        rec.put("manifest_length", 8221i64);
        rec.put("partition_spec_id", 0i32);
        rec.put("added_snapshot_id", 42i64);
        rec.put("added_files_count", Some(1i32));
        rec.put("added_rows_count", Some(100i32));
        rec.put("sequence_number", Some(1i64));
        writer.append(rec).unwrap();

        let entries = parse_manifest_list(&writer.into_inner().unwrap()).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].added_files_count, Some(1));
        assert_eq!(entries[0].added_rows_count, Some(100));
    }
    #[test]
    fn debug_union_decode() {
        use apache_avro::types::{Record, Value};
        use apache_avro::{Schema as AvroSchema, Writer};
        let schema = AvroSchema::parse_str(MANIFEST_ENTRY_SCHEMA_V2).unwrap();
        let mut writer = Writer::new(&schema, Vec::new());
        let mut entry = Record::new(&schema).unwrap();
        entry.put("status", Value::Int(1));
        entry.put("snapshot_id", Value::Long(42));
        entry.put(
            "sequence_number",
            Value::Union(1, Box::new(Value::Long(34))),
        );
        entry.put(
            "file_sequence_number",
            Value::Union(1, Box::new(Value::Long(2))),
        );
        let data_file = Value::Record(vec![
            ("content".into(), Value::Int(0)),
            ("file_path".into(), Value::String("p".into())),
            ("file_format".into(), Value::String("parquet".into())),
            (
                "partition".into(),
                Value::Union(1, Box::new(Value::Record(vec![]))),
            ),
            ("record_count".into(), Value::Long(100)),
            ("file_size_in_bytes".into(), Value::Long(1)),
            (
                "column_sizes".into(),
                Value::Union(
                    1,
                    Box::new(Value::Array(vec![Value::Record(vec![
                        ("key".into(), Value::Int(1)),
                        ("value".into(), Value::Long(1024)),
                    ])])),
                ),
            ),
            (
                "value_counts".into(),
                Value::Union(
                    1,
                    Box::new(Value::Array(vec![Value::Record(vec![
                        ("key".into(), Value::Int(1)),
                        ("value".into(), Value::Long(100)),
                    ])])),
                ),
            ),
            (
                "null_value_counts".into(),
                Value::Union(
                    1,
                    Box::new(Value::Array(vec![Value::Record(vec![
                        ("key".into(), Value::Int(1)),
                        ("value".into(), Value::Long(0)),
                    ])])),
                ),
            ),
            (
                "nan_value_counts".into(),
                Value::Union(1, Box::new(Value::Array(vec![]))),
            ),
            (
                "lower_bounds".into(),
                Value::Union(
                    1,
                    Box::new(Value::Array(vec![Value::Record(vec![
                        ("key".into(), Value::Int(1)),
                        ("value".into(), Value::Bytes(vec![1])),
                    ])])),
                ),
            ),
            (
                "upper_bounds".into(),
                Value::Union(
                    1,
                    Box::new(Value::Array(vec![Value::Record(vec![
                        ("key".into(), Value::Int(1)),
                        ("value".into(), Value::Bytes(vec![2])),
                    ])])),
                ),
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
        let bytes = writer.into_inner().unwrap();
        let entries = parse_manifest(&bytes).unwrap();
        println!("DEBUG entry: {:?}", entries[0].data_file.column_sizes);
        assert_eq!(entries[0].data_file.column_sizes.get(&1), Some(&1024));
    }
}
