//! The single C ABI boundary of pgdatalake-core (specs/lakehouse/RULES.md §4).
//!
//! Contract:
//! - no Rust types cross the boundary other than opaque handles, plain C
//!   strings, and caller-owned error buffers;
//! - no panics unwind across the boundary — every entry point runs inside
//!   catch_unwind and failures return NULL + a message in the caller buffer.

// Only module allowed to relax the crate-level `deny(unsafe_code)`: the
// `#[no_mangle]` export below is an unsafe attribute.
#![allow(unsafe_code)]

use std::ffi::{c_char, CStr, CString};

use crate::scan::ScanPlan;
use crate::session::{CatalogClient, S3ClientOptions};
use crate::storage::S3Options;

/// Static, NUL-terminated version string of the core, in the form
/// `pgdatalake-core <major>.<minor>.<patch> (<milestone>)`.
///
/// # C signature (cbindgen-generated `pg_datalake_core.h`)
/// `const char *pg_datalake_core_version(void);`
#[no_mangle]
pub extern "C" fn pg_datalake_core_version() -> *const c_char {
    version_cstr().as_ptr()
}

/// The version string as a borrowed C string; single source of truth for the
/// exported ABI value and its unit test.
fn version_cstr() -> &'static CStr {
    c"pgdatalake-core 0.1.0 (M0)"
}

/// An open planned scan; opaque handle owned by the C side, freed with
/// pg_datalake_scan_free. File-path CStrings are precomputed so accessors
/// return stable pointers for the session's lifetime.
pub struct ScanSession {
    plan: ScanPlan,
    file_paths: Vec<CString>,
    s3: S3Options,
}

impl ScanSession {
    fn from_plan(plan: ScanPlan, s3: S3Options) -> Self {
        Self {
            file_paths: plan
                .tasks
                .iter()
                .map(|t| CString::new(t.entry.data_file.file_path.as_str()).unwrap_or_default())
                .collect(),
            plan,
            s3,
        }
    }

    fn s3_options(&self) -> S3Options {
        self.s3.clone()
    }
}

/// Copy `msg` into the caller-owned error buffer (truncated, NUL-terminated).
/// NULL buffer = caller ignores error text.
unsafe fn set_err(buf: *mut c_char, len: usize, msg: &str) {
    if buf.is_null() || len == 0 {
        return;
    }
    let bytes = msg.as_bytes();
    let n = bytes.len().min(len - 1);
    let dst = std::slice::from_raw_parts_mut(buf.cast::<u8>(), len);
    dst[..n].copy_from_slice(&bytes[..n]);
    dst[n] = 0;
}

/// Null-tolerant C string read; allocates a lossy String (boundary calls are
/// per-user-statement, so the copy is trivial next to the scan itself).
unsafe fn borrow_string(p: *const c_char) -> String {
    if p.is_null() {
        return String::new();
    }
    CStr::from_ptr(p).to_string_lossy().into_owned()
}

/// Plan a scan over one catalog table. Returns an opaque session handle, or
/// NULL with `err_buf` populated. All string args are null-tolerant.
///
/// # Safety
/// - all `*const c_char` args must be NULL or valid NUL-terminated strings;
/// - `err_buf` must be NULL or writable for `err_len` bytes;
/// - the returned handle must be freed exactly once with
///   `pg_datalake_scan_free`.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_scan_new(
    endpoint: *const c_char,
    warehouse: *const c_char,
    token: *const c_char,
    namespace: *const c_char,
    table: *const c_char,
    s3_endpoint: *const c_char,
    s3_access_key: *const c_char,
    s3_secret_key: *const c_char,
    predicate_json: *const c_char,
    err_buf: *mut c_char,
    err_len: usize,
) -> *mut ScanSession {
    let result = std::panic::catch_unwind(|| -> Result<ScanSession, String> {
        let endpoint = unsafe { borrow_string(endpoint) };
        let warehouse = unsafe { borrow_string(warehouse) };
        let token = unsafe { borrow_string(token) };
        let namespace = unsafe { borrow_string(namespace) };
        let table = unsafe { borrow_string(table) };
        let s3 = S3ClientOptions {
            endpoint: Some(unsafe { borrow_string(s3_endpoint) }).filter(|s| !s.is_empty()),
            region: None,
            access_key_id: Some(unsafe { borrow_string(s3_access_key) }).filter(|s| !s.is_empty()),
            secret_access_key: Some(unsafe { borrow_string(s3_secret_key) })
                .filter(|s| !s.is_empty()),
            path_style: true,
        };
        let pred_json = unsafe { borrow_string(predicate_json) };
        let predicate = if pred_json.is_empty() {
            None
        } else {
            let v: serde_json::Value =
                serde_json::from_str(&pred_json).map_err(|e| format!("predicate JSON: {e}"))?;
            Some(crate::scan::predicate_from_json(&v)?)
        };
        let client =
            CatalogClient::new(&endpoint, &warehouse, &token, &s3).map_err(|e| e.to_string())?;
        let ns_levels: Vec<&str> = namespace.split('.').collect();
        client
            .scan_table(&ns_levels, &table, predicate.as_ref())
            .map(|(p, s3)| ScanSession::from_plan(p, s3))
            .map_err(|e| e.to_string())
    });
    let session = result.unwrap_or_else(|panic| {
        let msg = if let Some(s) = panic.downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = panic.downcast_ref::<String>() {
            s.clone()
        } else {
            "unexpected panic in pgdatalake-core".to_string()
        };
        Err(format!("pgdatalake-core panic: {msg}"))
    });
    match session {
        Ok(s) => Box::into_raw(Box::new(s)),
        Err(e) => {
            unsafe { set_err(err_buf, err_len, &e) };
            std::ptr::null_mut()
        }
    }
}

/// Free a scan session. NULL is a no-op; the handle must not be used again.
///
/// # Safety
/// `session` must be NULL or a pointer returned by `pg_datalake_scan_new`
/// that has not been freed yet.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_scan_free(session: *mut ScanSession) {
    if !session.is_null() {
        drop(unsafe { Box::from_raw(session) });
    }
}

/// Number of planned tasks (ADDED + EXISTING data/delete files).
///
/// # Safety
/// `session` must be NULL or a live handle from `pg_datalake_scan_new`.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_scan_task_count(session: *mut ScanSession) -> u64 {
    match unsafe { session.as_ref() } {
        Some(s) => s.plan.tasks.len() as u64,
        None => 0,
    }
}

/// File path of task `idx` (stable for the session's lifetime); NULL when
/// out of range or session NULL.
///
/// # Safety
/// `session` must be NULL or a live handle from `pg_datalake_scan_new`.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_scan_task_file(
    session: *mut ScanSession,
    idx: u64,
) -> *const c_char {
    let Some(s) = (unsafe { session.as_ref() }) else {
        return std::ptr::null();
    };
    match s.file_paths.get(idx as usize) {
        Some(c) => c.as_ptr(),
        None => std::ptr::null(),
    }
}

/// record_count of task `idx`; 0 when out of range or session NULL.
///
/// # Safety
/// `session` must be NULL or a live handle from `pg_datalake_scan_new`.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_scan_task_rows(session: *mut ScanSession, idx: u64) -> i64 {
    match unsafe { session.as_ref() } {
        Some(s) => s
            .plan
            .tasks
            .get(idx as usize)
            .map(|t| t.entry.data_file.record_count)
            .unwrap_or(0),
        None => 0,
    }
}

/// A projected row iterator over one planned task's parquet file.
pub struct RowsHandle {
    rows: crate::parquet::ParquetRows,
    column_names: Vec<CString>,
    current_row: Vec<CString>,
    current_null: Vec<bool>,
}

/// Render the current row into C strings; nulls become empty strings with
/// the null flag set in parallel.
fn render_row(rows: &mut crate::parquet::ParquetRows) -> (Vec<CString>, Vec<bool>) {
    let mut vals = Vec::with_capacity(rows.column_names().len());
    let mut nulls = Vec::with_capacity(rows.column_names().len());
    for i in 0..rows.column_names().len() {
        match rows.value(i) {
            crate::parquet::CellValue::Null => {
                vals.push(CString::new("").unwrap_or_default());
                nulls.push(true);
            }
            crate::parquet::CellValue::Bool(b) => {
                vals.push(CString::new(if b { "t" } else { "f" }).unwrap_or_default());
                nulls.push(false);
            }
            crate::parquet::CellValue::Int64(n) => {
                vals.push(CString::new(n.to_string()).unwrap_or_default());
                nulls.push(false);
            }
            crate::parquet::CellValue::TimestampMicros(us) => {
                vals.push(
                    CString::new(crate::parquet::format_timestamp_micros(us)).unwrap_or_default(),
                );
                nulls.push(false);
            }
            crate::parquet::CellValue::Float64(f) => {
                vals.push(CString::new(f.to_string()).unwrap_or_default());
                nulls.push(false);
            }
            crate::parquet::CellValue::Str(s) => {
                vals.push(CString::new(s).unwrap_or_default());
                nulls.push(false);
            }
            crate::parquet::CellValue::Bytes(b) => {
                // text form: hex — typed binary Datum conversion is the FDW's job
                let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
                vals.push(CString::new(hex).unwrap_or_default());
                nulls.push(false);
            }
        }
    }
    (vals, nulls)
}

/// Open a projected row iterator over task `task_idx` of `session`.
/// `projection` is a comma-separated column-name list (empty = all columns).
/// Returns NULL + err_buf on failure.
///
/// # Safety
/// Same pointer rules as pg_datalake_scan_new; `rows` returned here must be
/// freed exactly once with pg_datalake_rows_close, and is invalid after the
/// session is freed.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_rows_open(
    session: *mut ScanSession,
    task_idx: u64,
    projection: *const c_char,
    err_buf: *mut c_char,
    err_len: usize,
) -> *mut RowsHandle {
    let result = std::panic::catch_unwind(|| -> Result<RowsHandle, String> {
        let Some(s) = session.as_ref() else {
            return Err("NULL scan session".into());
        };
        let projection = borrow_string(projection);
        let projection: Vec<&str> = if projection.is_empty() {
            Vec::new()
        } else {
            projection.split(',').map(|c| c.trim()).collect()
        };
        let file_path = s
            .plan
            .tasks
            .get(task_idx as usize)
            .map(|t| t.entry.data_file.file_path.as_str())
            .ok_or_else(|| format!("task {task_idx} out of range"))?
            .to_string();
        let (io, key) =
            crate::scan::open_for(&file_path, Some(&s.s3_options())).map_err(|e| e.to_string())?;
        let rows = if projection.is_empty() {
            crate::parquet::ParquetRows::open_all(&io, &key).map_err(|e| e.to_string())?
        } else {
            crate::parquet::ParquetRows::open(&io, &key, &projection).map_err(|e| e.to_string())?
        };
        let column_names = rows
            .column_names()
            .iter()
            .map(|n| CString::new(n.as_str()).unwrap_or_default())
            .collect();
        Ok(RowsHandle {
            rows,
            column_names,
            current_row: Vec::new(),
            current_null: Vec::new(),
        })
    });
    let handle = result.unwrap_or_else(|panic| {
        Err(if let Some(x) = panic.downcast_ref::<&str>() {
            format!("pgdatalake-core panic: {x}")
        } else if let Some(x) = panic.downcast_ref::<String>() {
            format!("pgdatalake-core panic: {x}")
        } else {
            "pgdatalake-core panic".to_string()
        })
    });
    match handle {
        Ok(h) => Box::into_raw(Box::new(h)),
        Err(e) => {
            unsafe { set_err(err_buf, err_len, &e) };
            std::ptr::null_mut()
        }
    }
}

/// Advance to the next row; true while a row is available. Column accessors
/// below refer to the row this selects.
///
/// # Safety
/// `rows` must be a live handle from pg_datalake_rows_open.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_rows_next(rows: *mut RowsHandle) -> bool {
    let Some(h) = rows.as_mut() else {
        return false;
    };
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if h.rows.next() {
            let (vals, nulls) = render_row(&mut h.rows);
            h.current_row = vals;
            h.current_null = nulls;
            true
        } else {
            false
        }
    }))
    .unwrap_or(false)
}

/// Column count of the projection.
///
/// # Safety
/// `rows` must be a live handle from pg_datalake_rows_open.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_rows_column_count(rows: *mut RowsHandle) -> u64 {
    match rows.as_ref() {
        Some(h) => h.rows.column_names().len() as u64,
        None => 0,
    }
}

/// Name of column `idx`; NULL when out of range.
///
/// # Safety
/// `rows` must be a live handle from pg_datalake_rows_open.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_rows_column_name(
    rows: *mut RowsHandle,
    idx: u64,
) -> *const c_char {
    match rows.as_ref().and_then(|h| h.column_names.get(idx as usize)) {
        Some(c) => c.as_ptr(),
        None => std::ptr::null(),
    }
}

/// Whether column `idx` of the current row is NULL.
///
/// # Safety
/// `rows` must be a live handle from pg_datalake_rows_open.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_rows_col_null(rows: *mut RowsHandle, idx: u64) -> bool {
    match rows.as_ref() {
        Some(h) => h.current_null.get(idx as usize).copied().unwrap_or(true),
        None => true,
    }
}

/// Text form of column `idx` of the current row; NULL for NULL cells.
///
/// # Safety
/// `rows` must be a live handle from pg_datalake_rows_open.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_rows_col_text(
    rows: *mut RowsHandle,
    idx: u64,
) -> *const c_char {
    match rows.as_ref().and_then(|h| h.current_row.get(idx as usize)) {
        Some(c) => c.as_ptr(),
        None => std::ptr::null(),
    }
}

/// Non-deleted manifest entries examined before predicate pruning.
///
/// # Safety
/// `session` must be NULL or a live handle from `pg_datalake_scan_new`.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_scan_files_before_pruning(session: *mut ScanSession) -> u64 {
    match unsafe { session.as_ref() } {
        Some(s) => s.plan.files_before_pruning as u64,
        None => 0,
    }
}

/// Entries surviving pruning (the tasks that will be read).
///
/// # Safety
/// `session` must be NULL or a live handle from `pg_datalake_scan_new`.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_scan_files_after_pruning(session: *mut ScanSession) -> u64 {
    match unsafe { session.as_ref() } {
        Some(s) => s.plan.files_after_pruning as u64,
        None => 0,
    }
}

/// Free a rows handle. NULL is a no-op.
///
/// # Safety
/// `rows` must be NULL or a pointer returned by pg_datalake_rows_open.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_rows_close(rows: *mut RowsHandle) {
    if !rows.is_null() {
        drop(Box::from_raw(rows));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The exported version ABI keeps its documented format (M0 anchor).
    #[test]
    fn version_abi_format_stable() {
        let v = unsafe { CStr::from_ptr(pg_datalake_core_version()) };
        assert!(v.to_string_lossy().starts_with("pgdatalake-core "), "{v:?}");
    }

    /// End-to-end FFI test over an in-process mock catalog + local store:
    /// scan_new -> task accessors -> free (RULES §4 panic-free boundary).
    #[test]
    fn ffi_scan_roundtrip_with_mock_catalog() {
        use apache_avro::types::{Record, Value};
        use apache_avro::{Schema as AvroSchema, Writer};

        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("metadata")).unwrap();

        // manifest with one ADDED file
        let schema = AvroSchema::parse_str(crate::manifest::MANIFEST_ENTRY_SCHEMA_V2).unwrap();
        let mut writer = Writer::new(&schema, Vec::new());
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
                Value::String("file:///table/data/a.parquet".into()),
            ),
            ("file_format".into(), Value::String("parquet".into())),
            ("partition".into(), Value::Union(0, Box::new(Value::Null))),
            ("record_count".into(), Value::Long(77)),
            ("file_size_in_bytes".into(), Value::Long(4096)),
            // real column stats: id (field 1) spans [1, 3] — needed for the
            // predicate-pruning assertion below (bounds must be present or
            // pruning conservatively keeps the file)
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
                Value::Union(
                    1,
                    Box::new(Value::Array(vec![Value::Record(vec![
                        ("key".into(), Value::Int(1)),
                        ("value".into(), Value::Bytes(1i64.to_le_bytes().to_vec())),
                    ])])),
                ),
            ),
            (
                "upper_bounds".into(),
                Value::Union(
                    1,
                    Box::new(Value::Array(vec![Value::Record(vec![
                        ("key".into(), Value::Int(1)),
                        ("value".into(), Value::Bytes(3i64.to_le_bytes().to_vec())),
                    ])])),
                ),
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
                Value::Union(0, Box::new(Value::Null)),
            ),
            (
                "sort_order_id".into(),
                Value::Union(0, Box::new(Value::Null)),
            ),
        ]);
        entry.put("data_file", data_file);
        writer.append(entry).unwrap();
        std::fs::write(
            dir.path().join("metadata").join("mn1.avro"),
            writer.into_inner().unwrap(),
        )
        .unwrap();

        // empty manifest list
        let lschema = AvroSchema::parse_str(crate::manifest::MANIFEST_LIST_SCHEMA_V2).unwrap();
        let lwriter = Writer::new(&lschema, Vec::new());
        let _ = lwriter; // empty writer serializes a zero-row avro on into_inner
        std::fs::write(
            dir.path().join("metadata").join("mn-list.avro"),
            lwriter.into_inner().unwrap(),
        )
        .unwrap();

        // hmm: empty list loses the added file — use a list WITH the entry instead
        let mut lwriter = Writer::new(&lschema, Vec::new());
        let mut mrec = Record::new(&lschema).unwrap();
        mrec.put(
            "manifest_path",
            format!("file://{}/metadata/mn1.avro", dir.path().display()),
        );
        mrec.put("manifest_length", 100i64);
        mrec.put("partition_spec_id", 0i32);
        mrec.put("added_snapshot_id", 42i64);
        mrec.put("added_files_count", Some(1i64));
        mrec.put("existing_files_count", Some(0i64));
        mrec.put("deleted_files_count", Some(0i64));
        mrec.put("added_rows_count", Some(77i64));
        mrec.put("existing_rows_count", Some(0i64));
        mrec.put("deleted_rows_count", Some(0i64));
        mrec.put("partitions", None::<Value>);
        mrec.put("key_metadata", None::<Value>);
        mrec.put("sequence_number", Some(1i64));
        mrec.put("min_sequence_number", Some(1i64));
        lwriter.append(mrec).unwrap();
        std::fs::write(
            dir.path().join("metadata").join("mn-list.avro"),
            lwriter.into_inner().unwrap(),
        )
        .unwrap();

        let metadata_json = format!(
            r#"{{"format-version": 2,
                "table-uuid": "9c12d441-03fe-11ed-8122-d358e75d12eb",
                "location": "file://{}/table",
                "schemas": [{{"type": "struct", "schema-id": 0, "fields": [
                    {{"id": 1, "name": "id", "required": true, "type": "long"}}]}}],
                "current-schema-id": 0,
                "snapshots": [{{"snapshot-id": 42, "timestamp-ms": 1712345678000,
                                "manifest-list": "file://{}/metadata/mn-list.avro",
                                "summary": {{"operation": "append"}}}}],
                "current-snapshot-id": 42}}"#,
            dir.path().display(),
            dir.path().display(),
        );
        std::fs::write(
            dir.path().join("metadata").join("00000-x.metadata.json"),
            metadata_json,
        )
        .unwrap();

        // mock catalog serving config + loadTable
        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let addr = server.server_addr().to_string();
        let md_path = dir.path().join("metadata").join("00000-x.metadata.json");
        std::thread::spawn(move || {
            // serve config + loadTable pairs; enough for the session
            // variants in this test (plain + predicate + failure paths)
            for _ in 0..6 {
                let Some(req) = server.recv().ok() else {
                    break;
                };
                let is_config = req.url().contains("/v1/config");
                let body = if is_config {
                    r#"{"defaults": {"prefix": "pfx"}, "overrides": {}}"#.to_string()
                } else {
                    format!(
                        r#"{{"metadata-location": "file://{}", "config": {{}}}}"#,
                        md_path.display()
                    )
                };
                let resp = tiny_http::Response::from_string(body).with_header(
                    tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                        .unwrap(),
                );
                let _ = req.respond(resp);
            }
        });

        let endpoint = CString::new(format!("http://{addr}")).unwrap();
        let warehouse = CString::new("smoke").unwrap();
        let token = CString::new("").unwrap();
        let namespace = CString::new("analytics").unwrap();
        let table = CString::new("events").unwrap();
        let mut err = [0 as c_char; 1024];

        let session = unsafe {
            pg_datalake_scan_new(
                endpoint.as_ptr(),
                warehouse.as_ptr(),
                token.as_ptr(),
                namespace.as_ptr(),
                table.as_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                err.as_mut_ptr(),
                err.len(),
            )
        };
        assert!(!session.is_null(), "error: {:?}", unsafe {
            CStr::from_ptr(err.as_ptr()).to_string_lossy()
        });
        assert_eq!(unsafe { pg_datalake_scan_task_count(session) }, 1);
        let file = unsafe { pg_datalake_scan_task_file(session, 0) };
        assert!(!file.is_null());
        let f = unsafe { CStr::from_ptr(file) };
        assert!(f.to_string_lossy().ends_with("a.parquet"));
        assert_eq!(unsafe { pg_datalake_scan_task_rows(session, 0) }, 77);
        assert_eq!(unsafe { pg_datalake_scan_task_rows(session, 99) }, 0);
        unsafe {
            pg_datalake_scan_free(session);
        }

        // predicate JSON prunes at plan time: id > 100 drops the only file
        // (bounds 1..3) -> zero tasks, no error
        let pred = CString::new(r#"{"op":"gt","field":"id","value":100}"#).unwrap();
        let pruned = unsafe {
            pg_datalake_scan_new(
                endpoint.as_ptr(),
                warehouse.as_ptr(),
                token.as_ptr(),
                namespace.as_ptr(),
                table.as_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                pred.as_ptr(),
                err.as_mut_ptr(),
                err.len(),
            )
        };
        assert!(!pruned.is_null(), "pruned error: {:?}", unsafe {
            CStr::from_ptr(err.as_ptr()).to_string_lossy()
        });
        assert_eq!(unsafe { pg_datalake_scan_task_count(pruned) }, 0);
        unsafe {
            pg_datalake_scan_free(pruned);
            pg_datalake_scan_free(std::ptr::null_mut()); // no-op
        }

        // failure path: unreachable endpoint -> NULL + populated err buffer
        let bad = CString::new("http://127.0.0.1:9").unwrap();
        let session = unsafe {
            pg_datalake_scan_new(
                bad.as_ptr(),
                warehouse.as_ptr(),
                token.as_ptr(),
                namespace.as_ptr(),
                table.as_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                err.as_mut_ptr(),
                err.len(),
            )
        };
        assert!(session.is_null());
        let msg = unsafe { CStr::from_ptr(err.as_ptr()).to_string_lossy() };
        assert!(!msg.is_empty(), "error buffer populated");
    }
}
