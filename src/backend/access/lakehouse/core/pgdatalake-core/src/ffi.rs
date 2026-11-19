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
}

impl ScanSession {
    fn from_plan(plan: ScanPlan) -> Self {
        Self {
            file_paths: plan
                .tasks
                .iter()
                .map(|t| CString::new(t.entry.data_file.file_path.as_str()).unwrap_or_default())
                .collect(),
            plan,
        }
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
        let client =
            CatalogClient::new(&endpoint, &warehouse, &token, &s3).map_err(|e| e.to_string())?;
        let ns_levels: Vec<&str> = namespace.split('.').collect();
        client
            .scan_table(&ns_levels, &table)
            .map(ScanSession::from_plan)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

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
            ("column_sizes".into(), Value::Map(HashMap::new())),
            ("value_counts".into(), Value::Map(HashMap::new())),
            ("null_value_counts".into(), Value::Map(HashMap::new())),
            ("nan_value_counts".into(), Value::Map(HashMap::new())),
            ("lower_bounds".into(), Value::Map(HashMap::new())),
            ("upper_bounds".into(), Value::Map(HashMap::new())),
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
            for body in [
                r#"{"defaults": {"prefix": "pfx"}, "overrides": {}}"#.to_string(),
                format!(
                    r#"{{"metadata-location": "file://{}", "config": {{}}}}"#,
                    md_path.display()
                ),
            ] {
                let req = server.recv().unwrap();
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
                err.as_mut_ptr(),
                err.len(),
            )
        };
        assert!(session.is_null());
        let msg = unsafe { CStr::from_ptr(err.as_ptr()).to_string_lossy() };
        assert!(!msg.is_empty(), "error buffer populated");
    }
}
