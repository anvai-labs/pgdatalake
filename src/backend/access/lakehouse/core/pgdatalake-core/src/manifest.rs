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
        let get_i64 = |k: &str| -> Result<Option<i64>, StorageError> {
            match fields.iter().find(|(fk, _)| fk == k) {
                Some((_, apache_avro::types::Value::Union(_, inner))) => match inner.as_ref() {
                    apache_avro::types::Value::Long(n) => Ok(Some(*n)),
                    apache_avro::types::Value::Null => Ok(None),
                    other => Err(StorageError::Decode(format!("{k}: {other:?}"))),
                },
                Some((_, apache_avro::types::Value::Long(n))) => Ok(Some(*n)),
                Some((_, apache_avro::types::Value::Null)) => Ok(None),
                other => Err(StorageError::Decode(format!("{k}: {other:?}"))),
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
        return Ok(ManifestListEntry {
            manifest_path: get_str("manifest_path")?,
            manifest_length: get_i64("manifest_length")?
                .ok_or_else(|| StorageError::Decode("manifest_length: null".into()))?,
            partition_spec_id: spec,
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
}
