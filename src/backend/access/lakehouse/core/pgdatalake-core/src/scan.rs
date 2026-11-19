//! Scan planning (specs/lakehouse/06-execution.md §2). Slice 7: snapshot
//! selection + manifest-list/manifest composition into a task list.
//! Partition/bounds predicate pruning lands in the next slice.

//! Scan planning (specs/lakehouse/06-execution.md §2). Slice 7 composes
//! the pipeline: metadata -> snapshot selection -> manifest list -> manifests
//! -> task list (ADDED + EXISTING entries; DELETED dropped). Partition and
//! metrics pruning are the next slice; the FFI exposes this to the FDW.

use crate::catalog::metadata::TableMetadata;
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
    /// Sum of record_count over surviving DATA files (delete files add rows
    /// at read time; the row-count story is completed by the delete slice).
    pub total_rows: i64,
    /// Number of surviving DATA files.
    pub data_files: usize,
}

/// Open a store for an absolute file location and return (io, key). For
/// file:// FILE locations the local store must be rooted at the parent
/// directory with the file name as the key; S3 locations carry the full
/// bucket-relative key already.
fn open_for(path_uri: &str, s3: Option<&S3Options>) -> Result<(StorageIo, String), StorageError> {
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

/// Plan a scan of the given table metadata. `snapshot_id = None` selects the
/// current snapshot; pinning an unknown id is a NotFound (doc 02 §2 pin
/// semantics). `s3` carries connection options for locations that need them
/// (file:// local fixtures pass None).
pub fn plan_scan(
    md: &TableMetadata,
    snapshot_id: Option<i64>,
    s3: Option<&S3Options>,
) -> Result<ScanPlan, StorageError> {
    let snapshot = match snapshot_id.or(md.current_snapshot_id) {
        Some(id) => md.snapshot(id).ok_or_else(|| {
            StorageError::NotFound(format!("snapshot {id} not in table metadata"))
        })?,
        None => {
            // empty table (no snapshots at all): SELECT plans to zero tasks
            return Ok(ScanPlan {
                snapshot_id: md.current_snapshot_id.unwrap_or_default(),
                tasks: Vec::new(),
                total_rows: 0,
                data_files: 0,
            });
        }
    };

    let (list_io, list_key) = open_for(&snapshot.manifest_list, s3)?;
    let list_bytes = list_io.get_bytes(&list_key)?;
    let list_entries = parse_manifest_list(&list_bytes)?;

    let mut tasks = Vec::new();
    let mut total_rows = 0i64;
    let mut data_files = 0usize;

    for mle in &list_entries {
        let (io, key) = open_for(&mle.manifest_path, s3)?;
        let bytes = io.get_bytes(&key)?;
        for e in parse_manifest(&bytes)? {
            // DELETED entries are history, not data (doc 06 §2)
            if e.status == crate::manifest::ManifestEntryStatus::Deleted {
                continue;
            }
            if e.data_file.content == crate::manifest::DataContent::Data {
                total_rows += e.data_file.record_count;
                data_files += 1;
            }
            tasks.push(ScanTask {
                sequence_number: e.sequence_number.or(mle.sequence_number).unwrap_or(0),
                entry: e,
            });
        }
    }

    Ok(ScanPlan {
        snapshot_id: snapshot.snapshot_id,
        tasks,
        total_rows,
        data_files,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::metadata::{FormatVersion, TableMetadata};
    use apache_avro::types::{Record, Value};
    use apache_avro::{Schema as AvroSchema, Writer};
    use std::collections::HashMap;

    fn avro_map(pairs: &[(&str, i64)]) -> Value {
        Value::Map(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), Value::Long(*v)))
                .collect::<HashMap<_, _>>(),
        )
    }

    /// Build one manifest (avro) with the given (status, path, rows) files.
    fn build_manifest(
        dir: &std::path::Path,
        name: &str,
        files: &[(i32, &str, i64)],
    ) -> Result<(), StorageError> {
        let schema = AvroSchema::parse_str(crate::manifest::MANIFEST_ENTRY_SCHEMA_V2).unwrap();
        let mut writer = Writer::new(&schema, Vec::new());
        for (status, path, rows) in files {
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
                ("content".into(), Value::Int(0)),
                ("file_path".into(), Value::String(path.to_string())),
                ("file_format".into(), Value::String("parquet".into())),
                (
                    "partition".into(),
                    Value::Union(1, Box::new(Value::Record(vec![]))),
                ),
                ("record_count".into(), Value::Long(*rows)),
                ("file_size_in_bytes".into(), Value::Long(4096)),
                ("column_sizes".into(), avro_map(&[("1", 64)])),
                ("value_counts".into(), avro_map(&[("1", *rows)])),
                ("null_value_counts".into(), avro_map(&[])),
                ("nan_value_counts".into(), avro_map(&[])),
                ("lower_bounds".into(), Value::Map(HashMap::new())),
                ("upper_bounds".into(), Value::Map(HashMap::new())),
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
        std::fs::write(dir.join("mn-list.avro"), writer.into_inner().unwrap())
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

        let plan = plan_scan(&md, None, None).expect("plan_scan");
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
                "file:///table/data/a.parquet",
                "file:///table/data/b.parquet"
            ]
        );
        assert_eq!(plan.total_rows, 130);
        assert_eq!(plan.data_files, 2);
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
        let plan = plan_scan(&md, None, None).expect("plan_scan");
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
        let err = plan_scan(&md, Some(999), None).unwrap_err();
        assert!(matches!(err, StorageError::NotFound(_)), "{err}");
    }
}
