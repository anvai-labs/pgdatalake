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
    deletes: crate::parquet::LoadedDeletes,
}

impl ScanSession {
    fn from_plan(plan: ScanPlan, s3: S3Options, deletes: crate::parquet::LoadedDeletes) -> Self {
        Self {
            file_paths: plan
                .tasks
                .iter()
                .map(|t| CString::new(t.entry.data_file.file_path.as_str()).unwrap_or_default())
                .collect(),
            plan,
            s3,
            deletes,
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
    snapshot_selector_json: *const c_char,
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
        // NULL/empty selector = current snapshot (time travel: slices 18/19)
        let sel_json = unsafe { borrow_string(snapshot_selector_json) };
        let selector = if sel_json.is_empty() {
            None
        } else {
            let v: serde_json::Value = serde_json::from_str(&sel_json)
                .map_err(|e| format!("snapshot selector JSON: {e}"))?;
            Some(
                serde_json::from_value::<crate::catalog::metadata::SnapshotSelector>(v)
                    .map_err(|e| format!("snapshot selector: {e}"))?,
            )
        };
        client
            .scan_table(&ns_levels, &table, predicate.as_ref(), selector.as_ref())
            .and_then(|(p, s3, schema)| {
                // slice 20: load the snapshot's delete files up front (they
                // are small relative to data; every read of this scan needs
                // them). The schema binds equality deletes to their key
                // columns (13-review blocker #5).
                crate::scan::load_row_deletes(&p, Some(&s3), &schema)
                    .map(|d| ScanSession::from_plan(p, s3, d))
            })
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
        // slice 20: apply this task's delete set (position positions for the
        // file + sequence-applicable equality deletes)
        let task = s
            .plan
            .tasks
            .get(task_idx as usize)
            .ok_or_else(|| format!("task {task_idx} out of range"))?;
        // partitioned means the partition struct carries REAL fields —
        // an empty record is the unpartitioned marker (13-review blocker
        // #5: is_some() treated every unpartitioned file as partitioned,
        // wrongly rejecting equality deletes on our own writes)
        let partitioned = task
            .entry
            .data_file
            .partition
            .as_ref()
            .map(|p| match p {
                serde_json::Value::Object(o) => !o.is_empty(),
                serde_json::Value::Null => false,
                _ => true,
            })
            .unwrap_or(false);
        let row_deletes = s
            .deletes
            .for_task(&file_path, task.sequence_number, partitioned)
            .map_err(|e| e.to_string())?;
        // ALWAYS through open_with_deletes: the old empty-projection
        // open_all branch silently dropped position deletes from
        // count(*)-style scans (13-review blocker #5)
        let rows =
            crate::parquet::ParquetRows::open_with_deletes(&io, &key, &projection, row_deletes)
                .map_err(|e| e.to_string())?;
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

/// An open managed-append write session (M2 slice 27b): accumulates rows
/// into the slice-24 BatchWriter and flushes/commits via session.rs's
/// write pipeline. Opaque handle owned by the C side.
pub struct WriteSession {
    client: crate::session::CatalogClient,
    namespace: Vec<String>,
    table: String,
    state: crate::session::TableWriteState,
    columns: Vec<crate::writer::WriteColumn>,
    writer: crate::writer::BatchWriter,
    io: crate::storage::StorageIo,
    /// S3 keys are bucket-relative: the location's path prefix must ride
    /// on every data-file put (write_snapshot handles its own metadata
    /// keys; this covers the data files).
    key_prefix: String,
    flushed: Vec<crate::snapshot_write::NewDataFile>,
    file_seq: usize,
    staged_rows: usize,
}

/// Open a write session. `schema_json` is `[{"name": ..., "type":
/// "<iceberg type name>"}...]` — names must exist in the table's current
/// schema (field-ids resolved server-side). Returns NULL + err message on
/// any failure (no panics across the FFI).
///
/// # Safety
/// All CStrings must be valid NUL-terminated pointers.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_write_new(
    endpoint: *const c_char,
    warehouse: *const c_char,
    token: *const c_char,
    namespace: *const c_char,
    table: *const c_char,
    s3_endpoint: *const c_char,
    s3_access_key: *const c_char,
    s3_secret_key: *const c_char,
    schema_json: *const c_char,
    err_buf: *mut c_char,
    err_len: usize,
) -> *mut WriteSession {
    use std::ffi::CStr;
    let cstr = |p: *const c_char| -> String {
        if p.is_null() {
            String::new()
        } else {
            unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned()
        }
    };
    let fail = |msg: String, err_buf: *mut c_char, err_len: usize| -> *mut WriteSession {
        unsafe { set_err(err_buf, err_len, &msg) };
        std::ptr::null_mut()
    };
    let (endpoint, warehouse, token, ns, table) = (
        cstr(endpoint),
        cstr(warehouse),
        cstr(token),
        cstr(namespace),
        cstr(table),
    );
    let (s3_endpoint_c, s3_access_key_c, s3_secret_key_c) =
        (cstr(s3_endpoint), cstr(s3_access_key), cstr(s3_secret_key));
    let schema_json = cstr(schema_json);
    let cols: Vec<(String, crate::catalog::metadata::IcebergType)> =
        match serde_json::from_str::<serde_json::Value>(&schema_json) {
            Ok(v) => match v.as_array() {
                Some(arr) => {
                    let mut out = Vec::with_capacity(arr.len());
                    for c in arr {
                        let (Some(name), Some(t)) = (c.get("name"), c.get("type")) else {
                            return fail(
                                "write path: schema entries need name+type".into(),
                                err_buf,
                                err_len,
                            );
                        };
                        let (Some(name), Some(t)) = (name.as_str(), t.as_str()) else {
                            return fail(
                                "write path: schema entries need name+type strings".into(),
                                err_buf,
                                err_len,
                            );
                        };
                        // type names go through the same parser the metadata
                        // reader uses (canonical text form)
                        let ty: crate::catalog::metadata::IcebergType =
                            match serde_json::from_str(&serde_json::json!(t).to_string()) {
                                Ok(t) => t,
                                Err(e) => {
                                    return fail(
                                        format!("write path: bad type {t:?}: {e}"),
                                        err_buf,
                                        err_len,
                                    )
                                }
                            };
                        out.push((name.to_string(), ty));
                    }
                    out
                }
                None => {
                    return fail(
                        "write path: schema_json is not an array".into(),
                        err_buf,
                        err_len,
                    );
                }
            },
            Err(e) => {
                return fail(
                    format!("write path: bad schema_json: {e}"),
                    err_buf,
                    err_len,
                )
            }
        };

    let ns_levels: Vec<String> = ns
        .split('.')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    let ns_refs: Vec<&str> = ns_levels.iter().map(|s| s.as_str()).collect();

    let client = match crate::session::CatalogClient::new(
        &endpoint,
        &warehouse,
        &token,
        &crate::session::S3ClientOptions {
            endpoint: (!cstr(s3_endpoint).is_empty()).then(|| s3_endpoint_c.clone()),
            region: None,
            access_key_id: (!s3_access_key_c.is_empty()).then(|| s3_access_key_c.clone()),
            secret_access_key: (!s3_secret_key_c.is_empty()).then(|| s3_secret_key_c.clone()),
            path_style: true,
        },
    ) {
        Ok(c) => c,
        Err(e) => return fail(format!("lakehouse write open: {e}"), err_buf, err_len),
    };
    let state = match client.write_table_open(&ns_refs, &table, &cols) {
        Ok(s) => s,
        Err(e) => return fail(format!("lakehouse write open: {e}"), err_buf, err_len),
    };
    let columns: Vec<crate::writer::WriteColumn> = cols
        .into_iter()
        .map(|(name, ty)| crate::writer::WriteColumn {
            field_id: state.field_ids.get(&name).copied(),
            name,
            ty,
        })
        .collect();
    let writer = match crate::writer::BatchWriter::new(columns.clone()) {
        Ok(w) => w,
        Err(e) => return fail(format!("lakehouse write open: {e}"), err_buf, err_len),
    };
    let io = match crate::storage::StorageIo::open(
        &crate::storage::parse_location(&state.location)
            .unwrap_or_else(|_| crate::storage::parse_location("file:///nonexistent").unwrap()),
        Some(&state.s3),
    ) {
        Ok(io) => io,
        Err(e) => return fail(format!("lakehouse write open: {e}"), err_buf, err_len),
    };
    let key_prefix = match crate::storage::parse_location(&state.location) {
        Ok(crate::storage::Location::S3 { key, .. }) => key.trim_matches('/').to_string(),
        _ => String::new(),
    };
    let ws = Box::new(WriteSession {
        client,
        namespace: ns_levels,
        table,
        state,
        columns,
        writer,
        io,
        key_prefix,
        flushed: Vec::new(),
        file_seq: 0,
        staged_rows: 0,
    });
    Box::into_raw(ws)
}

/// Push one row. Returns false + err message on arity/type mismatch.
///
/// # Safety
/// `ws` must be a live handle; `row_json` a valid NUL-terminated string.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_write_push(
    ws: *mut WriteSession,
    row_json: *const c_char,
    err_buf: *mut c_char,
    err_len: usize,
) -> bool {
    let Some(ws) = (unsafe { ws.as_mut() }) else {
        unsafe { set_err(err_buf, err_len, "write push: NULL session") };
        return false;
    };
    let row = unsafe { std::ffi::CStr::from_ptr(row_json) }.to_string_lossy();
    let parsed: serde_json::Value = match serde_json::from_str(&row) {
        Ok(v) => v,
        Err(e) => {
            unsafe { set_err(err_buf, err_len, &format!("write push: bad row json: {e}")) };
            return false;
        }
    };
    let cells = match crate::writer::parse_row_json(&ws.columns, &parsed) {
        Ok(c) => c,
        Err(e) => {
            unsafe { set_err(err_buf, err_len, &e.to_string()) };
            return false;
        }
    };
    match ws.writer.push(&cells) {
        Ok(()) => {
            ws.staged_rows += 1;
            true
        }
        Err(e) => {
            unsafe { set_err(err_buf, err_len, &e.to_string()) };
            false
        }
    }
}

/// Flush the accumulated batch to a data file (C calls this at batch-size
/// thresholds; commit flushes any remainder). Returns false + err on IO
/// failure.
///
/// # Safety
/// `ws` must be a live handle.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_write_flush(
    ws: *mut WriteSession,
    err_buf: *mut c_char,
    err_len: usize,
) -> bool {
    let Some(ws) = (unsafe { ws.as_mut() }) else {
        unsafe { set_err(err_buf, err_len, "write flush: NULL session") };
        return false;
    };
    if ws.writer.num_rows() == 0 {
        return true;
    }
    let data_dir = if ws.key_prefix.is_empty() {
        "data".to_string()
    } else {
        format!("{}/data", ws.key_prefix)
    };
    let wf = match ws.writer.write_file(&ws.io, &data_dir, ws.file_seq) {
        Ok(w) => w,
        Err(e) => {
            unsafe { set_err(err_buf, err_len, &format!("write flush: {e}")) };
            return false;
        }
    };
    ws.file_seq += 1;
    // the manifest stores the LOCATION URI; the put key may carry the S3
    // bucket-root prefix, which the location already includes
    let rel_key = if ws.key_prefix.is_empty() {
        wf.key.as_str()
    } else {
        wf.key
            .strip_prefix(&format!("{}/", ws.key_prefix))
            .unwrap_or(&wf.key)
    };
    // metric maps keyed by the TABLE's field ids
    let ids: Vec<i64> = ws
        .columns
        .iter()
        .filter_map(|c| ws.state.field_ids.get(&c.name).copied())
        .collect();
    let mut file = crate::snapshot_write::NewDataFile {
        file_path: format!("{}/{rel_key}", ws.state.location.trim_end_matches('/')),
        record_count: wf.record_count,
        file_size_in_bytes: wf.file_size_in_bytes,
        ..Default::default()
    };
    for id in &ids {
        file.value_counts.insert(*id, file.record_count);
    }
    ws.flushed.push(file);
    ws.staged_rows = 0;
    true
}

/// Flush remainder + commit the append snapshot. Returns a heap CString
/// with the new metadata-location, or NULL + err. THE CALLER FREES the
/// returned string with pg_datalake_write_commit_result_free.
///
/// # Safety
/// `ws` must be a live handle.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_write_commit(
    ws: *mut WriteSession,
    retries: u32,
    err_buf: *mut c_char,
    err_len: usize,
) -> *mut c_char {
    let Some(ws) = (unsafe { ws.as_mut() }) else {
        unsafe { set_err(err_buf, err_len, "write commit: NULL session") };
        return std::ptr::null_mut();
    };
    if !unsafe { pg_datalake_write_flush(ws, err_buf, err_len) } {
        return std::ptr::null_mut();
    }
    if ws.flushed.is_empty() {
        unsafe { set_err(err_buf, err_len, "write commit: no rows staged") };
        return std::ptr::null_mut();
    }
    let ns_refs: Vec<&str> = ws.namespace.iter().map(|s| s.as_str()).collect();
    match ws
        .client
        .commit_append(&ws.state, &ns_refs, &ws.table, &ws.flushed, retries)
    {
        Ok(loc) => {
            ws.flushed.clear();
            match std::ffi::CString::new(loc) {
                Ok(c) => c.into_raw(),
                Err(_) => std::ptr::null_mut(),
            }
        }
        Err(e) => {
            unsafe { set_err(err_buf, err_len, &format!("lakehouse commit: {e}")) };
            std::ptr::null_mut()
        }
    }
}

/// Create the remote Iceberg table from a column list (managed CREATE,
/// slice 27c). Namespace is created if missing. Returns 0 + err on
/// failure; on success returns a heap CString with the server-assigned
/// metadata-location (freed via pg_datalake_write_commit_result_free).
///
/// # Safety
/// All CStrings must be valid NUL-terminated pointers.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_create_table(
    endpoint: *const c_char,
    warehouse: *const c_char,
    token: *const c_char,
    namespace: *const c_char,
    table: *const c_char,
    s3_endpoint: *const c_char,
    s3_access_key: *const c_char,
    s3_secret_key: *const c_char,
    schema_json: *const c_char,
    err_buf: *mut c_char,
    err_len: usize,
    err_kind: *mut i32,
) -> *mut c_char {
    let cstr = |p: *const c_char| -> String {
        if p.is_null() {
            String::new()
        } else {
            unsafe { std::ffi::CStr::from_ptr(p) }
                .to_string_lossy()
                .into_owned()
        }
    };
    // error KIND (13-review #6): the caller must never classify failures
    // by searching the message for "409" — 0 = other, 1 = conflict.
    let set_kind = |k: i32| {
        if !err_kind.is_null() {
            unsafe { *err_kind = k };
        }
    };
    set_kind(0);
    let fail = |msg: String, err_buf: *mut c_char, err_len: usize| -> *mut c_char {
        unsafe { set_err(err_buf, err_len, &msg) };
        std::ptr::null_mut()
    };
    let (endpoint, warehouse, token, ns, table) = (
        cstr(endpoint),
        cstr(warehouse),
        cstr(token),
        cstr(namespace),
        cstr(table),
    );
    let (s3_e, s3_ak, s3_sk) = (cstr(s3_endpoint), cstr(s3_access_key), cstr(s3_secret_key));
    let schema_json = cstr(schema_json);
    let cols: Vec<(String, crate::catalog::metadata::IcebergType)> =
        match serde_json::from_str::<serde_json::Value>(&schema_json) {
            Ok(serde_json::Value::Array(arr)) => {
                let mut out = Vec::with_capacity(arr.len());
                for c in arr {
                    let (Some(name), Some(t)) = (c.get("name"), c.get("type")) else {
                        return fail(
                            "write path: schema entries need name+type".into(),
                            err_buf,
                            err_len,
                        );
                    };
                    let (Some(name), Some(t)) = (name.as_str(), t.as_str()) else {
                        return fail(
                            "write path: schema entries need name+type strings".into(),
                            err_buf,
                            err_len,
                        );
                    };
                    let ty: crate::catalog::metadata::IcebergType =
                        match serde_json::from_str(&serde_json::json!(t).to_string()) {
                            Ok(t) => t,
                            Err(e) => {
                                return fail(
                                    format!("write path: bad type {t:?}: {e}"),
                                    err_buf,
                                    err_len,
                                )
                            }
                        };
                    out.push((name.to_string(), ty));
                }
                out
            }
            _ => {
                return fail(
                    "write path: schema_json is not an array".into(),
                    err_buf,
                    err_len,
                );
            }
        };
    let ns_levels: Vec<String> = ns
        .split('.')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    let client = match crate::session::CatalogClient::new(
        &endpoint,
        &warehouse,
        &token,
        &crate::session::S3ClientOptions {
            endpoint: (!s3_e.is_empty()).then(|| s3_e.clone()),
            region: None,
            access_key_id: (!s3_ak.is_empty()).then(|| s3_ak.clone()),
            secret_access_key: (!s3_sk.is_empty()).then(|| s3_sk.clone()),
            path_style: true,
        },
    ) {
        Ok(c) => c,
        Err(e) => return fail(format!("lakehouse create: {e}"), err_buf, err_len),
    };
    let ns_refs: Vec<&str> = ns_levels.iter().map(|s| s.as_str()).collect();
    match client.create_table(&ns_refs, &table, &cols) {
        Ok(loc) => match std::ffi::CString::new(loc) {
            Ok(c) => c.into_raw(),
            Err(_) => std::ptr::null_mut(),
        },
        Err(crate::storage::StorageError::Conflict(m)) => {
            set_kind(1);
            fail(format!("lakehouse create conflict: {m}"), err_buf, err_len)
        }
        Err(e) => fail(format!("lakehouse create: {e}"), err_buf, err_len),
    }
}

/// Free a commit result string.
///
/// # Safety
/// `ptr` must be NULL or a pointer returned by pg_datalake_write_commit.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_write_commit_result_free(ptr: *mut c_char) {
    if !ptr.is_null() {
        drop(unsafe { std::ffi::CString::from_raw(ptr) });
    }
}

/// Free a write session WITHOUT committing (abort semantics: staged
/// unflushed rows are dropped; flushed files are orphans cleaned by
/// orphan GC per doc 07 §2 ABORT).
///
/// # Safety
/// `ws` must be NULL or a live handle from pg_datalake_write_new.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_write_free(ws: *mut WriteSession) {
    if !ws.is_null() {
        drop(unsafe { Box::from_raw(ws) });
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
        crate::storage::test_allow_local_stores();
        std::fs::create_dir_all(dir.path().join("table").join("metadata")).unwrap();
        std::fs::create_dir_all(dir.path().join("metadata")).unwrap();

        // slice 20: real data parquet (3 rows) + a position-delete parquet
        // removing absolute position 1 (id=2)
        {
            use arrow::array::Int64Array;
            use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
            use arrow::record_batch::RecordBatch;
            use std::sync::Arc;
            std::fs::create_dir_all(dir.path().join("table").join("data")).unwrap();

            let schema = ArrowSchema::new(vec![Field::new("id", DataType::Int64, false)]);
            let batch = RecordBatch::try_new(
                Arc::new(schema),
                vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
            )
            .unwrap();
            let f = std::fs::File::create(dir.path().join("table").join("data").join("a.parquet"))
                .unwrap();
            let mut w = parquet::arrow::ArrowWriter::try_new(f, batch.schema(), None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();

            let del_schema = ArrowSchema::new(vec![
                Field::new("file_path", DataType::Utf8, false),
                Field::new("pos", DataType::Int64, false),
            ]);
            let del_batch = RecordBatch::try_new(
                Arc::new(del_schema),
                vec![
                    Arc::new(arrow::array::StringArray::from(vec![format!(
                        "file://{}/table/data/a.parquet",
                        dir.path().display()
                    )])),
                    Arc::new(Int64Array::from(vec![1i64])),
                ],
            )
            .unwrap();
            let df = std::fs::File::create(
                dir.path()
                    .join("table")
                    .join("data")
                    .join("del-pos.parquet"),
            )
            .unwrap();
            let mut dw =
                parquet::arrow::ArrowWriter::try_new(df, del_batch.schema(), None).unwrap();
            dw.write(&del_batch).unwrap();
            dw.close().unwrap();
        }

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
                Value::String(format!(
                    "file://{}/table/data/a.parquet",
                    dir.path().display()
                )),
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
            dir.path().join("table").join("metadata").join("mn1.avro"),
            writer.into_inner().unwrap(),
        )
        .unwrap();

        // slice 20: a second manifest holding the position-delete file
        // (content = 1) — it must NOT become a scan task
        let mut del_writer = Writer::new(&schema, Vec::new());
        let mut del_entry = Record::new(&schema).unwrap();
        del_entry.put("status", Value::Int(1));
        del_entry.put("snapshot_id", Value::Long(42));
        del_entry.put("sequence_number", Value::Union(1, Box::new(Value::Long(2))));
        del_entry.put(
            "file_sequence_number",
            Value::Union(1, Box::new(Value::Long(2))),
        );
        let del_data_file = Value::Record(vec![
            ("content".into(), Value::Int(1)),
            (
                "file_path".into(),
                Value::String(format!(
                    "file://{}/table/data/del-pos.parquet",
                    dir.path().display()
                )),
            ),
            ("file_format".into(), Value::String("parquet".into())),
            ("partition".into(), Value::Union(0, Box::new(Value::Null))),
            ("record_count".into(), Value::Long(1)),
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
                Value::Union(0, Box::new(Value::Null)),
            ),
            (
                "upper_bounds".into(),
                Value::Union(0, Box::new(Value::Null)),
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
        del_entry.put("data_file", del_data_file);
        del_writer.append(del_entry).unwrap();
        std::fs::write(
            dir.path().join("table").join("metadata").join("mn2.avro"),
            del_writer.into_inner().unwrap(),
        )
        .unwrap();

        // empty manifest list
        let lschema = AvroSchema::parse_str(crate::manifest::MANIFEST_LIST_SCHEMA_V2).unwrap();
        let lwriter = Writer::new(&lschema, Vec::new());
        let _ = lwriter; // empty writer serializes a zero-row avro on into_inner
        std::fs::write(
            dir.path()
                .join("table")
                .join("metadata")
                .join("mn-list.avro"),
            lwriter.into_inner().unwrap(),
        )
        .unwrap();

        // hmm: empty list loses the added file — use a list WITH the entry instead
        let mut lwriter = Writer::new(&lschema, Vec::new());
        let mut mrec = Record::new(&lschema).unwrap();
        mrec.put(
            "manifest_path",
            format!("file://{}/table/metadata/mn1.avro", dir.path().display()),
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
        let mut mrec2 = Record::new(&lschema).unwrap();
        mrec2.put(
            "manifest_path",
            format!("file://{}/table/metadata/mn2.avro", dir.path().display()),
        );
        mrec2.put("manifest_length", 100i64);
        mrec2.put("partition_spec_id", 0i32);
        mrec2.put("added_snapshot_id", 42i64);
        mrec2.put("added_files_count", Some(1i64));
        mrec2.put("existing_files_count", Some(0i64));
        mrec2.put("deleted_files_count", Some(0i64));
        mrec2.put("added_rows_count", Some(1i64));
        mrec2.put("existing_rows_count", Some(0i64));
        mrec2.put("deleted_rows_count", Some(0i64));
        mrec2.put("partitions", None::<Value>);
        mrec2.put("key_metadata", None::<Value>);
        mrec2.put("sequence_number", Some(2i64));
        mrec2.put("min_sequence_number", Some(2i64));
        lwriter.append(mrec2).unwrap();
        std::fs::write(
            dir.path()
                .join("table")
                .join("metadata")
                .join("mn-list.avro"),
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
                                "manifest-list": "file://{}/table/metadata/mn-list.avro",
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
            // variants in this test (plain + predicate + selector + failure
            // paths)
            for _ in 0..8 {
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

        // slice 20: rows_open applies the position deletes — the delete
        // manifest entry is NOT a task (count stayed 1) and reading task 0
        // skips absolute position 1 (id=2)
        {
            let proj = CString::new("id").unwrap();
            let rows = unsafe {
                pg_datalake_rows_open(session, 0, proj.as_ptr(), err.as_mut_ptr(), err.len())
            };
            assert!(!rows.is_null(), "rows_open: {:?}", unsafe {
                CStr::from_ptr(err.as_ptr()).to_string_lossy()
            });
            let mut ids = Vec::new();
            while unsafe { pg_datalake_rows_next(rows) } {
                let n = unsafe { pg_datalake_rows_column_count(rows) };
                assert_eq!(n, 1);
                let v = unsafe { pg_datalake_rows_col_text(rows, 0) };
                assert!(!v.is_null());
                ids.push(unsafe { CStr::from_ptr(v) }.to_string_lossy().into_owned());
            }
            unsafe { pg_datalake_rows_close(rows) };
            assert_eq!(ids, vec!["1", "3"], "position delete removed id=2");
        }
        unsafe {
            pg_datalake_scan_free(session);
        }

        // predicate JSON prunes at plan time: id > 100 drops the only file
        // (bounds 1..3) -> zero tasks, no error
        let pred = CString::new(r#"{"op":"gt","field":"id","value":100}"#).unwrap();
        let sel_ok = CString::new(r#"{"kind":"id","value":42}"#).unwrap();
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
                std::ptr::null(),
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

        // slice 19: a snapshot selector JSON pins the scan (id form), and a
        // selector matching no snapshot is a NULL + error message
        let pinned = unsafe {
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
                sel_ok.as_ptr(),
                err.as_mut_ptr(),
                err.len(),
            )
        };
        assert!(!pinned.is_null(), "pinned error: {:?}", unsafe {
            CStr::from_ptr(err.as_ptr()).to_string_lossy()
        });
        assert_eq!(unsafe { pg_datalake_scan_task_count(pinned) }, 1);
        unsafe {
            pg_datalake_scan_free(pinned);
        }

        let sel_bad = CString::new(r#"{"kind":"id","value":999}"#).unwrap();
        let unpinned = unsafe {
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
                sel_bad.as_ptr(),
                err.as_mut_ptr(),
                err.len(),
            )
        };
        assert!(unpinned.is_null());
        let msg = unsafe { CStr::from_ptr(err.as_ptr()).to_string_lossy() };
        assert!(msg.contains("matches no snapshot"), "{msg}");

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
                std::ptr::null(),
                err.as_mut_ptr(),
                err.len(),
            )
        };
        assert!(session.is_null());
        let msg = unsafe { CStr::from_ptr(err.as_ptr()).to_string_lossy() };
        assert!(!msg.is_empty(), "error buffer populated");
    }
    /// TDD anchor (M2 slice 27b-i): the FFI WRITE path end-to-end against
    /// the mock catalog — write_new -> push rows -> write_commit (mock
    /// captures the commitTable body: requirements pin the base, updates
    /// carry the AddSnapshot + SetSnapshotRef) -> the captured snapshot is
    /// merged into the on-disk metadata -> pg_datalake_scan_new reads OUR
    /// OWN written rows back. Write->commit->read, full circle, no
    /// external engine.
    #[test]
    fn ffi_write_commit_read_back_with_mock_catalog() {
        let dir = tempfile::tempdir().unwrap();
        crate::storage::test_allow_local_stores();
        std::fs::create_dir_all(dir.path().join("table").join("metadata")).unwrap();
        std::fs::create_dir_all(dir.path().join("metadata")).unwrap();

        // fresh table: no snapshots, no manifest list yet; the table
        // location must exist for the local store root to canonicalize
        std::fs::create_dir_all(dir.path().join("table").join("data")).unwrap();
        std::fs::create_dir_all(dir.path().join("table").join("metadata")).unwrap();
        let metadata_json = format!(
            r#"{{"format-version": 2,
                "table-uuid": "1ca92afa-70b9-4d02-9ba6-7ee1a3ac0f5e",
                "location": "file://{}/table",
                "last-sequence-number": 3,
                "schemas": [{{"type": "struct", "schema-id": 0, "fields": [
                    {{"id": 1, "name": "id", "required": false, "type": "long"}}]}}],
                "current-schema-id": 0}}"#,
            dir.path().display(),
        );
        let md_path = dir.path().join("metadata").join("00000-x.metadata.json");
        std::fs::write(&md_path, &metadata_json).unwrap();

        // scripted mock: config, loadTable, POST commitTable (capture),
        // config, loadTable (read-back pass)
        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let addr = server.server_addr().to_string();
        let md_capture = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
        let md_capture_t = std::sync::Arc::clone(&md_capture);
        let md_path_srv = md_path.clone();
        // detached, like the scan mock above: the request count here is
        // config+loadTable (open) + loadTable (commit pre-load, slice-25
        // retry protocol) + POST commitTable + config+loadTable (read-back)
        std::thread::spawn(move || {
            for _ in 0..6 {
                let Some(mut req) = server.recv().ok() else {
                    break;
                };
                let url = req.url().to_string();
                let body = if url.contains("/v1/config") {
                    r#"{"defaults": {"prefix": "pfx"}, "overrides": {}}"#.to_string()
                } else if url.contains("namespaces") && req.method().as_str() == "POST" {
                    let mut body_s = String::new();
                    {
                        let _ = req.as_reader().read_to_string(&mut body_s);
                    }
                    *md_capture_t.lock().unwrap() = body_s;
                    r#"{"metadata-location": "file:///committed/00005.metadata.json", "config": {}}"#.to_string()
                } else {
                    format!(
                        r#"{{"metadata-location": "file://{}", "config": {{}}}}"#,
                        md_path_srv.display()
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
        let namespace = CString::new("analytics").unwrap();
        let table = CString::new("events").unwrap();
        let schema_json = CString::new(r#"[{"name": "id", "type": "long"}]"#).unwrap();
        let mut err = [0 as c_char; 1024];
        fn errmsg(err: &[c_char]) -> String {
            unsafe { CStr::from_ptr(err.as_ptr()) }
                .to_string_lossy()
                .into_owned()
        }

        let ws = unsafe {
            pg_datalake_write_new(
                endpoint.as_ptr(),
                warehouse.as_ptr(),
                std::ptr::null(), // token
                namespace.as_ptr(),
                table.as_ptr(),
                std::ptr::null(), // s3 endpoint (file:// store)
                std::ptr::null(),
                std::ptr::null(),
                schema_json.as_ptr(),
                err.as_mut_ptr(),
                err.len(),
            )
        };
        assert!(!ws.is_null(), "write_new: {}", errmsg(&err));

        // push rows: bare JSON numbers for a long column
        for row in ["[1]", "[2]"] {
            let rj = CString::new(row).unwrap();
            assert!(
                unsafe { pg_datalake_write_push(ws, rj.as_ptr(), err.as_mut_ptr(), err.len()) },
                "push {row}: {}",
                errmsg(&err)
            );
        }
        // arity mismatch fails cleanly
        let bad = CString::new("[1, 2]").unwrap();
        assert!(!unsafe { pg_datalake_write_push(ws, bad.as_ptr(), err.as_mut_ptr(), err.len()) });

        // commit
        let loc = unsafe { pg_datalake_write_commit(ws, 0, err.as_mut_ptr(), err.len()) };
        assert!(!loc.is_null(), "commit: {}", errmsg(&err));
        let loc_s = unsafe { CStr::from_ptr(loc) }
            .to_string_lossy()
            .into_owned();
        unsafe { pg_datalake_write_commit_result_free(loc) };
        assert!(loc_s.ends_with("00005.metadata.json"), "{loc_s}");

        // captured commitTable body: requirements + updates, pinned base
        let body = md_capture.lock().unwrap().clone();
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        let reqs = v["requirements"].as_array().unwrap();
        assert_eq!(reqs.len(), 2, "{body}");
        assert_eq!(reqs[0]["type"], "assert-table-uuid");
        assert_eq!(reqs[1]["type"], "assert-ref-snapshot-id");
        assert!(
            reqs[1]["snapshot-id"].is_null(),
            "fresh table: main must not exist: {body}"
        );
        let updates = v["updates"].as_array().unwrap();
        assert_eq!(updates.len(), 2, "{body}");
        assert_eq!(updates[0]["action"], "add-snapshot");
        let snap = &updates[0]["snapshot"];
        assert_eq!(snap["sequence-number"], 4, "base seq 3 + 1: {body}");
        assert!(snap["manifest-list"]
            .as_str()
            .unwrap()
            .starts_with("file://"));
        let ml_path = snap["manifest-list"].as_str().unwrap().to_string();
        assert_eq!(updates[1]["action"], "set-snapshot-ref");
        assert_eq!(updates[1]["ref-name"], "main");
        assert_eq!(updates[1]["type"], "branch");

        // data file + avros exist on the local store
        let snap_id = snap["snapshot-id"].as_i64().unwrap();
        assert!(dir
            .path()
            .join("table")
            .join("data")
            .read_dir()
            .unwrap()
            .next()
            .is_some());
        let ml_key = ml_path.trim_start_matches("file://");
        assert!(std::path::Path::new(ml_key).exists(), "{ml_path}");
        let manifest_list =
            crate::manifest::parse_manifest_list(&std::fs::read(ml_key).unwrap()).unwrap();
        assert_eq!(manifest_list.len(), 1);
        let manifest_path = manifest_list[0].manifest_path.clone();
        let entries = crate::manifest::parse_manifest(
            &std::fs::read(manifest_path.trim_start_matches("file://")).unwrap(),
        )
        .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].data_file.record_count, 2, "value_counts rows");
        assert_eq!(entries[0].data_file.value_counts.get(&1), Some(&2));

        unsafe { pg_datalake_write_free(ws) };

        // read-back: merge the committed snapshot into the on-disk metadata
        // (what the catalog does remotely), then scan through the FFI
        let updated = format!(
            r#"{{"format-version": 2,
                "table-uuid": "1ca92afa-70b9-4d02-9ba6-7ee1a3ac0f5e",
                "location": "file://{}/table",
                "last-sequence-number": 4,
                "schemas": [{{"type": "struct", "schema-id": 0, "fields": [
                    {{"id": 1, "name": "id", "required": false, "type": "long"}}]}}],
                "current-schema-id": 0,
                "snapshots": [{{"snapshot-id": {}, "timestamp-ms": 1712345678000,
                                "manifest-list": "{}",
                                "summary": {{"operation": "append"}}}}],
                "current-snapshot-id": {}}}"#,
            dir.path().display(),
            snap_id,
            ml_path,
            snap_id,
        );
        std::fs::write(&md_path, updated).unwrap();

        let session = unsafe {
            pg_datalake_scan_new(
                endpoint.as_ptr(),
                warehouse.as_ptr(),
                std::ptr::null(),
                namespace.as_ptr(),
                table.as_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                err.as_mut_ptr(),
                err.len(),
            )
        };
        assert!(!session.is_null(), "scan_new: {}", errmsg(&err));
        assert_eq!(unsafe { pg_datalake_scan_task_count(session) }, 1);
        assert_eq!(unsafe { pg_datalake_scan_task_rows(session, 0) }, 2);
        {
            let proj = CString::new("id").unwrap();
            let rows = unsafe {
                pg_datalake_rows_open(session, 0, proj.as_ptr(), err.as_mut_ptr(), err.len())
            };
            assert!(!rows.is_null(), "rows_open: {}", errmsg(&err));
            let mut ids = Vec::new();
            while unsafe { pg_datalake_rows_next(rows) } {
                let v = unsafe { pg_datalake_rows_col_text(rows, 0) };
                ids.push(unsafe { CStr::from_ptr(v) }.to_string_lossy().into_owned());
            }
            unsafe { pg_datalake_rows_close(rows) };
            assert_eq!(ids, vec!["1", "2"], "WROTE 1,2; READ 1,2");
        }
        unsafe { pg_datalake_scan_free(session) };
    }
}
