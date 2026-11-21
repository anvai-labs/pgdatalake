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

/// The scan's pinned Iceberg snapshot id — the basis of the parallel
/// stable-task-mapping contract (13-review #7): workers plan THIS id,
/// so concurrent commits can never make task index N mean different
/// files on different processes.
///
/// # Safety
/// `session` must be NULL or a live handle from `pg_datalake_scan_new`.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_scan_snapshot_id(session: *mut ScanSession) -> i64 {
    match unsafe { session.as_ref() } {
        Some(s) => s.plan.snapshot_id,
        None => 0,
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
    /// Diagnostic of the last ERROR status (13-review #8 ROW/EOF/ERROR).
    last_err: Option<String>,
    /// M3-R1 batch view: the materialized batch (keeps arrow buffers
    /// alive for the spans) plus the flat C-facing column array.
    current_batch: Option<crate::parquet::BatchView>,
    batch_cols: Vec<PgDatalakeBatchColumn>,
    /// backing store for pg_datalake_rows_batch_cell_text
    fallback_cell: CString,
}

/// C view of one materialized batch column (M3-R1). `kind` selects the
/// buffer: Bool → `values` as *const u8 (0/1 per row); Int64/TimestampUs/
/// Date32 → `values` as *const i64; Float64 → `values` as *const f64;
/// Text/Bytes/Rendered → `spans`. `nulls` is a byte per batch row (1 =
/// NULL). All pointers are valid until the next batch call or close.
#[repr(C)]
pub struct PgDatalakeBatchColumn {
    pub kind: i32,
    pub values: *const std::ffi::c_void,
    pub nulls: *const u8,
    pub spans: *const crate::parquet::BatchSpan,
}

/// C view of one materialized arrow batch (M3-R1): total rows, the
/// ascending live-row list (deletes applied in Rust), and one column
/// entry per projected column.
#[repr(C)]
pub struct PgDatalakeRowBatch {
    pub rows: u64,
    pub live_count: u64,
    /// batch-LOCAL positions of surviving rows (buffer indexes)
    pub live_idx: *const u32,
    /// file-absolute position of batch row 0 (tombstone/TID identity:
    /// absolute = start_abs + live_idx[k]; position deletes are
    /// file-scoped, doc 07)
    pub start_abs: u64,
    pub n_cols: u64,
    pub cols: *const PgDatalakeBatchColumn,
}

/// Render the current row into C strings; nulls become empty strings with
/// the null flag set in parallel.
/// Render one row; Err on any cell that cannot cross the C boundary
/// (interior NUL bytes previously became EMPTY strings — 13-review #8).
/// Uniform panic-to-message formatting for the C boundary (13-review #8).
fn panic_message(panic: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(x) = panic.downcast_ref::<&str>() {
        (*x).to_string()
    } else if let Some(x) = panic.downcast_ref::<String>() {
        x.clone()
    } else {
        "pgdatalake-core panic".to_string()
    }
}

fn render_row(rows: &mut crate::parquet::ParquetRows) -> Result<(Vec<CString>, Vec<bool>), String> {
    let mut vals = Vec::with_capacity(rows.column_names().len());
    let mut nulls = Vec::with_capacity(rows.column_names().len());
    for i in 0..rows.column_names().len() {
        let c = match rows.value(i) {
            crate::parquet::CellValue::Null => {
                vals.push(CString::new("").unwrap_or_default());
                nulls.push(true);
                continue;
            }
            crate::parquet::CellValue::Bool(b) => if b { "t" } else { "f" }.to_string(),
            crate::parquet::CellValue::Int64(n) => n.to_string(),
            crate::parquet::CellValue::TimestampMicros(us) => {
                crate::parquet::format_timestamp_micros(us)
            }
            crate::parquet::CellValue::Float64(f) => f.to_string(),
            crate::parquet::CellValue::Str(s) => s,
            crate::parquet::CellValue::Bytes(b) => {
                // text form: hex — typed binary Datum conversion is the FDW's job
                b.iter().map(|x| format!("{x:02x}")).collect()
            }
        };
        let cstr = CString::new(c).map_err(|_| {
            format!(
                "column {:?} value contains an interior NUL byte and cannot be \
                 returned as text",
                rows.column_names().get(i).cloned().unwrap_or_default()
            )
        })?;
        vals.push(cstr);
        nulls.push(false);
    }
    Ok((vals, nulls))
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
        // counted JSON ARRAY (13-review #8b): comma-separated names broke
        // on column names containing commas
        let projection_owned: Vec<String> = if projection.is_empty() {
            Vec::new()
        } else {
            serde_json::from_str(&projection)
                .map_err(|e| format!("projection is not a JSON array: {e}"))?
        };
        let projection: Vec<&str> = projection_owned.iter().map(|s| s.as_str()).collect();
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
            last_err: None,
            current_batch: None,
            batch_cols: Vec::new(),
            fallback_cell: CString::new("").unwrap_or_default(),
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
pub unsafe extern "C" fn pg_datalake_rows_next(
    rows: *mut RowsHandle,
    err_buf: *mut c_char,
    err_len: usize,
) -> i32 {
    /// Explicit advance status (13-review #8): a caught panic used to
    /// return `false` — INDISTINGUISHABLE FROM EOF, silently truncating
    /// result sets.
    pub const ROW: i32 = 0;
    pub const EOF: i32 = 1;
    pub const ERROR: i32 = 2;
    let Some(h) = rows.as_mut() else {
        unsafe { set_err(err_buf, err_len, "NULL rows handle") };
        return ERROR;
    };
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if h.rows.next() {
            render_row(&mut h.rows).map(|(vals, nulls)| {
                h.current_row = vals;
                h.current_null = nulls;
                ROW
            })
        } else {
            Ok(EOF)
        }
    }));
    match outcome {
        Ok(Ok(status)) => status,
        Ok(Err(e)) => {
            h.last_err = Some(e.clone());
            unsafe { set_err(err_buf, err_len, &e) };
            ERROR
        }
        Err(panic) => {
            let m = if let Some(x) = panic.downcast_ref::<&str>() {
                format!("pgdatalake-core panic: {x}")
            } else if let Some(x) = panic.downcast_ref::<String>() {
                format!("pgdatalake-core panic: {x}")
            } else {
                "pgdatalake-core panic".to_string()
            };
            h.last_err = Some(m.clone());
            unsafe { set_err(err_buf, err_len, &m) };
            ERROR
        }
    }
}

/// Advance the batch cursor (M3-R1, 13-review perf #3): materialize the
/// next arrow batch with surviving rows and fill `out` with typed
/// per-column views plus the live-row index list. Status: 0 = batch
/// delivered, 1 = EOF, 2 = ERROR (message in err_buf). Pointers inside
/// `out` are valid until the next batch call or rows_close — the C
/// consumer must copy any varlena it keeps. Deletes (position/equality/
/// DV) are already applied via live_idx; the C side never sees dead rows.
///
/// # Safety
/// `rows` must be a live handle from pg_datalake_rows_open; `out` must
/// point to writable memory owned by the caller.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_rows_batch_next(
    rows: *mut RowsHandle,
    out: *mut PgDatalakeRowBatch,
    err_buf: *mut c_char,
    err_len: usize,
) -> i32 {
    pub const BATCH: i32 = 0;
    pub const EOF: i32 = 1;
    pub const ERROR: i32 = 2;
    let Some(h) = rows.as_mut() else {
        unsafe { set_err(err_buf, err_len, "NULL rows handle") };
        return ERROR;
    };
    if out.is_null() {
        unsafe { set_err(err_buf, err_len, "NULL batch out pointer") };
        return ERROR;
    }
    let outcome =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match h.rows.next_batch() {
            Ok(Some(view)) => {
                h.batch_cols.clear();
                h.batch_cols.reserve(view.cols.len());
                for c in &view.cols {
                    let (values, spans) = match c.kind {
                        crate::parquet::BatchKind::Bool => (
                            c.bools.as_ptr() as *const std::ffi::c_void,
                            std::ptr::null(),
                        ),
                        crate::parquet::BatchKind::Int64
                        | crate::parquet::BatchKind::TimestampUs
                        | crate::parquet::BatchKind::Date32 => {
                            (c.i64s.as_ptr() as *const std::ffi::c_void, std::ptr::null())
                        }
                        crate::parquet::BatchKind::Float64 => {
                            (c.f64s.as_ptr() as *const std::ffi::c_void, std::ptr::null())
                        }
                        _ => (std::ptr::null(), c.spans.as_ptr()),
                    };
                    h.batch_cols.push(PgDatalakeBatchColumn {
                        kind: c.kind as i32,
                        values,
                        nulls: c.nulls.as_ptr(),
                        spans,
                    });
                }
                *out = PgDatalakeRowBatch {
                    rows: view.rows as u64,
                    live_count: view.live_idx.len() as u64,
                    live_idx: view.live_idx.as_ptr(),
                    start_abs: view.start_abs,
                    n_cols: view.cols.len() as u64,
                    cols: h.batch_cols.as_ptr(),
                };
                h.current_batch = Some(view);
                BATCH
            }
            Ok(None) => EOF,
            Err(e) => {
                h.last_err = Some(e.to_string());
                unsafe { set_err(err_buf, err_len, &e.to_string()) };
                ERROR
            }
        }));
    match outcome {
        Ok(status) => status,
        Err(panic) => {
            let m = panic_message(&panic);
            h.last_err = Some(m.clone());
            unsafe { set_err(err_buf, err_len, &m) };
            ERROR
        }
    }
}

/// Text form of one cell of the CURRENT batch (M3-R1 fallback path): the
/// C consumers use this for (kind, type) combinations without an exact
/// fast mapping, feeding InputFunctionCall exactly as the old per-row
/// path did. The bytes are identical to the per-row renderer. The
/// returned pointer is valid until the next call on this handle.
///
/// # Safety
/// Same pointer rules as pg_datalake_rows_batch_next.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_rows_batch_cell_text(
    rows: *mut RowsHandle,
    col: u64,
    batch_row: u64,
    err_buf: *mut c_char,
    err_len: usize,
) -> *const c_char {
    let Some(h) = rows.as_mut() else {
        unsafe { set_err(err_buf, err_len, "NULL rows handle") };
        return std::ptr::null();
    };
    let rendered = {
        let Some(view) = h.current_batch.as_ref() else {
            unsafe { set_err(err_buf, err_len, "no current batch") };
            return std::ptr::null();
        };
        let Some(cview) = view.cols.get(col as usize) else {
            unsafe { set_err(err_buf, err_len, "column out of range") };
            return std::ptr::null();
        };
        if batch_row as usize >= view.rows {
            unsafe { set_err(err_buf, err_len, "batch row out of range") };
            return std::ptr::null();
        }
        let r = batch_row as usize;
        use crate::parquet::BatchKind;
        match cview.kind {
            BatchKind::Bool => {
                if cview.bools[r] != 0 {
                    "t".into()
                } else {
                    "f".into()
                }
            }
            BatchKind::Int64 => cview.i64s[r].to_string(),
            BatchKind::TimestampUs => crate::parquet::format_timestamp_micros(cview.i64s[r]),
            BatchKind::Date32 => crate::parquet::format_date32(cview.i64s[r] as i32),
            BatchKind::Float64 => crate::parquet::format_f64(cview.f64s[r]),
            BatchKind::Text | BatchKind::Rendered => {
                let sp = &cview.spans[r];
                std::str::from_utf8(unsafe { std::slice::from_raw_parts(sp.ptr, sp.len as usize) })
                    .unwrap_or("")
                    .to_string()
            }
            BatchKind::Bytes => {
                let sp = &cview.spans[r];
                let b = unsafe { std::slice::from_raw_parts(sp.ptr, sp.len as usize) };
                let mut hex = String::with_capacity(2 + b.len() * 2);
                hex.push_str("\\x");
                for x in b {
                    hex.push_str(&format!("{x:02x}"));
                }
                hex
            }
        }
    };
    h.fallback_cell = CString::new(rendered).unwrap_or_default();
    h.fallback_cell.as_ptr()
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
    /// P1 rollover threshold in bytes (0 = no rollover: single file per
    /// session, legacy behavior). The C side passes
    /// lakehouse.target_file_size * 1024 (GUC is kB).
    target_file_size_bytes: i64,
    /// measured bytes/row from the last flush — the rollover estimate
    last_avg_row_bytes: usize,
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
        target_file_size_bytes: 0,
        last_avg_row_bytes: 64,
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
    // 13-review #8: consistent panic boundary — a Rust-side panic must
    // surface as a loud error, never unwind into C.
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| ws.writer.push(&cells))) {
        Ok(Ok(())) => {
            ws.staged_rows += 1;
            // P1 rollover (spec 15 G3): estimated buffered bytes reached
            // the threshold -> roll a file NOW, inside the same snapshot
            if ws.target_file_size_bytes > 0 {
                let est = ws.writer.num_rows() as i64 * ws.last_avg_row_bytes as i64;
                if est >= ws.target_file_size_bytes {
                    if let Err(e) = flush_writer_buffer(ws) {
                        unsafe { set_err(err_buf, err_len, &format!("write push: rollover: {e}")) };
                        return false;
                    }
                }
            }
            true
        }
        Ok(Err(e)) => {
            unsafe { set_err(err_buf, err_len, &e.to_string()) };
            false
        }
        Err(panic) => {
            let m = panic_message(&panic);
            unsafe { set_err(err_buf, err_len, &format!("write push: {m}")) };
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
    match flush_writer_buffer(ws) {
        Ok(()) => true,
        Err(e) => {
            unsafe { set_err(err_buf, err_len, &format!("write flush: {e}")) };
            false
        }
    }
}

/// Delta-segment payload reader (P3 core, spec 15 §3.3 D1-rev): the
/// payload is ONE JSON document — an array of row arrays in the write
/// path's typed row-JSON (bare numbers, quoted strings, null). Cells are
/// rendered to the EXACT text forms the per-row renderer produces, so
/// the C consumer feeds them to the same input functions with identical
/// results.
pub struct DeltaRowsHandle {
    rows: Vec<Vec<(CString, bool)>>, // row -> col -> (text, isnull)
    row_pos: usize,
    column_names: Vec<CString>,
}

/// CellValue -> text, byte-identical to render_row's match.
fn cell_text(v: &crate::parquet::CellValue) -> String {
    use crate::parquet::CellValue::*;
    match v {
        Null => String::new(),
        Bool(b) => if *b { "t".into() } else { "f".into() },
        Int64(n) => n.to_string(),
        TimestampMicros(us) => crate::parquet::format_timestamp_micros(*us),
        Float64(f) => f.to_string(),
        Str(s) => s.clone(),
        Bytes(b) => {
            let mut hex = String::with_capacity(2 + b.len() * 2);
            hex.push_str("\\x");
            for x in b {
                hex.push_str(&format!("{x:02x}"));
            }
            hex
        }
    }
}

/// Open a delta payload (JSON array of row arrays). Column names/types
/// ride in the same schema JSON the write path uses.
/// # Safety
/// Both C strings must be valid NUL-terminated pointers; the returned
/// handle is freed exactly once with pg_datalake_delta_rows_close.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_delta_rows_open(
    payload_json: *const c_char,
    schema_json: *const c_char,
    err_buf: *mut c_char,
    err_len: usize,
) -> *mut DeltaRowsHandle {
    let outcome = std::panic::catch_unwind(|| -> Result<Box<DeltaRowsHandle>, String> {
        let payload = unsafe { CStr::from_ptr(payload_json) }.to_string_lossy();
        let schema = unsafe { CStr::from_ptr(schema_json) }.to_string_lossy();
        let cols: Vec<crate::writer::WriteColumn> = serde_json::from_str(&schema)
            .map_err(|e| format!("delta rows: schema json: {e}"))?;
        let doc: serde_json::Value =
            serde_json::from_str(&payload).map_err(|e| format!("delta rows: payload json: {e}"))?;
        let rows_json = doc
            .as_array()
            .ok_or_else(|| "delta rows: payload is not a JSON array".to_string())?;
        let mut out = Vec::with_capacity(rows_json.len());
        for (ri, rj) in rows_json.iter().enumerate() {
            let cells = crate::writer::parse_row_json(&cols, rj)
                .map_err(|e| format!("delta rows: row {ri}: {e}"))?;
            if cells.len() != cols.len() {
                return Err(format!(
                    "delta rows: row {ri}: {} cells for {} columns",
                    cells.len(),
                    cols.len()
                ));
            }
            let mut row = Vec::with_capacity(cols.len());
            for c in &cells {
                if matches!(c, crate::parquet::CellValue::Null) {
                    row.push((CString::new("").unwrap(), true));
                } else {
                    row.push((
                        CString::new(cell_text(c)).map_err(|_| {
                            format!("delta rows: row {ri}: interior NUL in cell")
                        })?,
                        false,
                    ));
                }
            }
            out.push(row);
        }
        let column_names = cols
            .iter()
            .map(|c| CString::new(c.name.as_str()).unwrap_or_default())
            .collect();
        Ok(Box::new(DeltaRowsHandle {
            rows: out,
            row_pos: 0,
            column_names,
        }))
    });
    match outcome {
        Ok(Ok(h)) => Box::into_raw(h),
        Ok(Err(e)) => {
            unsafe { set_err(err_buf, err_len, &e) };
            std::ptr::null_mut()
        }
        Err(panic) => {
            let m = panic_message(&panic);
            unsafe { set_err(err_buf, err_len, &m) };
            std::ptr::null_mut()
        }
    }
}

/// # Safety
/// `h` must be a live handle from pg_datalake_delta_rows_open.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_delta_rows_close(h: *mut DeltaRowsHandle) {
    if !h.is_null() {
        drop(unsafe { Box::from_raw(h) });
    }
}

/// # Safety
/// `h` must be a live handle.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_delta_rows_column_count(h: *mut DeltaRowsHandle) -> u64 {
    match unsafe { h.as_ref() } {
        Some(d) => d.column_names.len() as u64,
        None => 0,
    }
}

/// Advance: 0 = row, 1 = EOF, 2 = error (13-review #8 tri-state).
///
/// # Safety
/// `h` must be a live handle.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_delta_rows_next(
    h: *mut DeltaRowsHandle,
    err_buf: *mut c_char,
    err_len: usize,
) -> i32 {
    match unsafe { h.as_mut() } {
        Some(d) => {
            if d.row_pos < d.rows.len() {
                d.row_pos += 1;
                0
            } else {
                1
            }
        }
        None => {
            unsafe { set_err(err_buf, err_len, "delta rows: NULL handle") };
            2
        }
    }
}

/// # Safety
/// `h` must be a live handle; call after a 0 from next().
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_delta_rows_col_null(
    h: *mut DeltaRowsHandle,
    idx: u64,
) -> bool {
    unsafe { h.as_ref() }
        .and_then(|d| d.rows.get(d.row_pos.saturating_sub(1)))
        .and_then(|r| r.get(idx as usize))
        .map(|(_, isnull)| *isnull)
        .unwrap_or(true)
}

/// # Safety
/// `h` must be a live handle; call after a 0 from next().
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_delta_rows_col_text(
    h: *mut DeltaRowsHandle,
    idx: u64,
) -> *const c_char {
    unsafe { h.as_ref() }
        .and_then(|d| d.rows.get(d.row_pos.saturating_sub(1)))
        .and_then(|r| r.get(idx as usize))
        .map(|(t, _)| t.as_ptr())
        .unwrap_or(std::ptr::null())
}

/// Initialize the global per-backend block cache capacity (spec 15
/// §6.2/P0): called once from _PG_init with lakehouse.cache_max_bytes.
/// First call wins (the cache is process-lifetime; no re-cap).
///
/// # Safety
/// No pointers; always safe.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_core_cache_init(max_bytes: i64) {
    crate::cache::global_init(max_bytes.max(0) as u64);
}

/// Set the rollover threshold (P1, spec 15 §7.2): when the buffered
/// rows' estimated bytes reach this size, push() rolls a file — one
/// transaction then emits MANY size-bounded files in ONE snapshot
/// (small-file prevention, G3). bytes <= 0 disables rollover (legacy
/// single-file behavior). The C side passes
/// lakehouse.target_file_size * 1024 (the GUC is kB).
///
/// # Safety
/// Same pointer rules as pg_datalake_write_push.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_write_set_target_file_size(
    ws: *mut WriteSession,
    bytes: i64,
    err_buf: *mut c_char,
    err_len: usize,
) -> bool {
    let Some(ws) = (unsafe { ws.as_mut() }) else {
        unsafe { set_err(err_buf, err_len, "set target file size: NULL session") };
        return false;
    };
    ws.target_file_size_bytes = bytes.max(0);
    true
}

/// Flush the writer's buffered rows as one data file + record its
/// manifest entry (with M3-R2 stats). Shared by pg_datalake_write_flush
/// and the P1 rollover in push.
fn flush_writer_buffer(ws: &mut WriteSession) -> Result<(), String> {
    if ws.writer.num_rows() == 0 {
        return Ok(());
    }
    let data_dir = if ws.key_prefix.is_empty() {
        "data".to_string()
    } else {
        format!("{}/data", ws.key_prefix)
    };
    let rows_before = ws.writer.num_rows();
    let wf = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        ws.writer.write_file(&ws.io, &data_dir, ws.file_seq)
    })) {
        Ok(Ok(w)) => w,
        Ok(Err(e)) => return Err(e.to_string()),
        Err(panic) => return Err(panic_message(&panic)),
    };
    ws.file_seq += 1;
    if let Some(avg) = (rows_before > 0)
        .then_some(wf.file_size_in_bytes as usize / rows_before)
        .map(|v| v.max(1))
    {
        ws.last_avg_row_bytes = avg;
    }
    // the manifest stores the LOCATION URI; the put key may carry the S3
    // bucket-root prefix, which the location already includes
    let rel_key = if ws.key_prefix.is_empty() {
        wf.key.as_str()
    } else {
        wf.key
            .strip_prefix(&format!("{}/", ws.key_prefix))
            .unwrap_or(&wf.key)
    };
    // metric maps keyed by the TABLE's field ids, IN COLUMN ORDER
    // (writer stats arrays are parallel to ws.columns)
    let mut file = crate::snapshot_write::NewDataFile {
        file_path: format!("{}/{rel_key}", ws.state.location.trim_end_matches('/')),
        record_count: wf.record_count,
        file_size_in_bytes: wf.file_size_in_bytes,
        // M3-R2: real manifest metrics from write_file — prunable bounds,
        // footer column sizes, exact counts. Empty maps for columns whose
        // name never resolved a field id (conservative: absent bounds
        // never prune, never misprune).
        column_sizes: wf.column_sizes,
        value_counts: wf.value_counts,
        null_value_counts: wf.null_value_counts,
        lower_bounds: wf.lower_bounds,
        upper_bounds: wf.upper_bounds,
    };
    for (c, _) in ws.columns.iter().enumerate() {
        if let Some(id) = ws.state.field_ids.get(&ws.columns[c].name) {
            file.value_counts.entry(*id).or_insert(file.record_count);
        }
    }
    ws.flushed.push(file);
    ws.staged_rows = 0;
    Ok(())
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
    commit_internal(ws, &[], retries, err_buf, err_len)
}

/// Shared commit tail (append or overwrite): flush remainder, commit via
/// session.rs, hand back the metadata-location. `deleted` non-empty makes
/// the snapshot an overwrite (M2 slice 28 COW UPDATE/DELETE).
///
/// # Safety
/// `ws` must be a live handle; err_buf rules as usual.
unsafe fn commit_internal(
    ws: &mut WriteSession,
    deleted: &[crate::snapshot_write::DeletedDataFile],
    retries: u32,
    err_buf: *mut c_char,
    err_len: usize,
) -> *mut c_char {
    if !unsafe { pg_datalake_write_flush(ws, err_buf, err_len) } {
        return std::ptr::null_mut();
    }
    if ws.flushed.is_empty() && deleted.is_empty() {
        unsafe { set_err(err_buf, err_len, "write commit: no rows staged") };
        return std::ptr::null_mut();
    }
    let ns_refs: Vec<&str> = ws.namespace.iter().map(|s| s.as_str()).collect();
    let commit = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        ws.client.commit_append(
            &ws.state,
            &ns_refs,
            &ws.table,
            &ws.flushed,
            deleted,
            retries,
        )
    }));
    match commit {
        Ok(Ok(loc)) => {
            ws.flushed.clear();
            match std::ffi::CString::new(loc) {
                Ok(c) => c.into_raw(),
                Err(_) => std::ptr::null_mut(),
            }
        }
        Ok(Err(e)) => {
            unsafe { set_err(err_buf, err_len, &format!("lakehouse commit: {e}")) };
            std::ptr::null_mut()
        }
        Err(panic) => {
            let m = panic_message(&panic);
            unsafe { set_err(err_buf, err_len, &format!("lakehouse commit: {m}")) };
            std::ptr::null_mut()
        }
    }
}

/// Commit an OVERWRITE snapshot (M2 slice 28 COW UPDATE/DELETE):
/// flushed rows become ADDED files, `deleted_json` rows become DELETED
/// manifest entries. `deleted_json` is `[["path", record_count, size],
/// ...]` — identity of files REMOVED by this statement, as read from the
/// base snapshot's task list. Returns/ownership like pg_datalake_write_commit.
///
/// # Safety
/// Same rules as pg_datalake_write_commit.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_write_commit_overwrite(
    ws: *mut WriteSession,
    deleted_json: *const c_char,
    retries: u32,
    err_buf: *mut c_char,
    err_len: usize,
) -> *mut c_char {
    let Some(ws) = (unsafe { ws.as_mut() }) else {
        unsafe { set_err(err_buf, err_len, "overwrite commit: NULL session") };
        return std::ptr::null_mut();
    };
    let raw = borrow_string(deleted_json);
    let deleted: Vec<crate::snapshot_write::DeletedDataFile> = match serde_json::from_str::<
        Vec<(String, i64, i64)>,
    >(&raw)
    {
        Ok(rows) => rows
            .into_iter()
            .map(|(file_path, record_count, file_size_in_bytes)| {
                crate::snapshot_write::DeletedDataFile {
                    file_path,
                    record_count,
                    file_size_in_bytes,
                }
            })
            .collect(),
        Err(e) => {
            unsafe {
                set_err(
                    err_buf,
                    err_len,
                    &format!("overwrite commit: deleted list must be [[path,count,size],...]: {e}"),
                )
            };
            return std::ptr::null_mut();
        }
    };
    unsafe { commit_internal(ws, &deleted, retries, err_buf, err_len) }
}

/// Open one data file BY PATH for a COW rewrite pass (slice 28): rows are
/// re-read from the BASE snapshot's files (never the catalog's current
/// snapshot — a concurrent writer may have moved it) using the write
/// session's storage credentials. The path is scope-gated to the
/// session's table root. Projection is a counted JSON array of column
/// names, as in pg_datalake_rows_open.
///
/// # Safety
/// `ws` must be a live handle; the returned handle is freed exactly once
/// with pg_datalake_rows_close.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_rows_open_path(
    ws: *mut WriteSession,
    path: *const c_char,
    projection: *const c_char,
    err_buf: *mut c_char,
    err_len: usize,
) -> *mut RowsHandle {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
        || -> Result<RowsHandle, String> {
            let Some(w) = ws.as_ref() else {
                return Err("NULL write session".into());
            };
            let file_path = borrow_string(path);
            if file_path.is_empty() {
                return Err("empty file path".into());
            }
            // transport policy: every hop stays inside the table root
            crate::scan::ensure_within_table_root(&file_path, &w.state.location)
                .map_err(|e| e.to_string())?;
            let projection = borrow_string(projection);
            let projection_owned: Vec<String> = if projection.is_empty() {
                Vec::new()
            } else {
                serde_json::from_str(&projection)
                    .map_err(|e| format!("projection is not a JSON array: {e}"))?
            };
            let projection: Vec<&str> = projection_owned.iter().map(|s| s.as_str()).collect();
            let (io, key) =
                crate::scan::open_for(&file_path, Some(&w.state.s3)).map_err(|e| e.to_string())?;
            let rows = crate::parquet::ParquetRows::open(&io, &key, &projection)
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
                last_err: None,
                current_batch: None,
                batch_cols: Vec::new(),
                fallback_cell: CString::new("").unwrap_or_default(),
            })
        },
    ));
    let handle = result.unwrap_or_else(|panic| Err(panic_message(&panic)));
    match handle {
        Ok(h) => Box::into_raw(Box::new(h)),
        Err(e) => {
            unsafe { set_err(err_buf, err_len, &e) };
            std::ptr::null_mut()
        }
    }
}

/// Size in bytes of one task's data file (deleted manifest entries carry
/// the ORIGINAL file's size; 0 when NULL session or out of range).
///
/// # Safety
/// `session` must be NULL or a live handle from pg_datalake_scan_new.
#[no_mangle]
pub unsafe extern "C" fn pg_datalake_scan_task_size(session: *mut ScanSession, idx: u64) -> i64 {
    let Some(s) = (unsafe { session.as_ref() }) else {
        return 0;
    };
    s.plan
        .tasks
        .get(idx as usize)
        .map(|t| t.entry.data_file.file_size_in_bytes)
        .unwrap_or(0)
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
            let proj = CString::new(r#"["id"]"#).unwrap();
            let rows = unsafe {
                pg_datalake_rows_open(session, 0, proj.as_ptr(), err.as_mut_ptr(), err.len())
            };
            assert!(!rows.is_null(), "rows_open: {:?}", unsafe {
                CStr::from_ptr(err.as_ptr()).to_string_lossy()
            });
            let mut ids = Vec::new();
            let mut next_err = [0i8; 1024];
            while unsafe { pg_datalake_rows_next(rows, next_err.as_mut_ptr() as *mut _, 1024) } == 0
            {
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
            let proj = CString::new(r#"["id"]"#).unwrap();
            let rows = unsafe {
                pg_datalake_rows_open(session, 0, proj.as_ptr(), err.as_mut_ptr(), err.len())
            };
            assert!(!rows.is_null(), "rows_open: {}", errmsg(&err));
            let mut ids = Vec::new();
            let mut next_err = [0i8; 1024];
            while unsafe { pg_datalake_rows_next(rows, next_err.as_mut_ptr() as *mut _, 1024) } == 0
            {
                let v = unsafe { pg_datalake_rows_col_text(rows, 0) };
                ids.push(unsafe { CStr::from_ptr(v) }.to_string_lossy().into_owned());
            }
            unsafe { pg_datalake_rows_close(rows) };
            assert_eq!(ids, vec!["1", "2"], "WROTE 1,2; READ 1,2");
        }
        unsafe { pg_datalake_scan_free(session) };
    }

    /// 13-review #8: unsupported Arrow types fail AT OPEN with a loud
    /// message — they used to surface as SQL NULL per cell (fabricated
    /// empty data).
    #[test]
    fn unsupported_arrow_type_is_loud_at_open() {
        use arrow::array::{ArrayRef, Int64Array};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        crate::storage::test_allow_local_stores();
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "nested",
                DataType::Struct(vec![Field::new("x", DataType::Int64, true)].into()),
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                arrow::array::new_null_array(
                    &DataType::Struct(vec![Field::new("x", DataType::Int64, true)].into()),
                    1,
                ),
            ],
        )
        .unwrap();
        let f = std::fs::File::create(dir.path().join("nested.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();

        let io = crate::storage::StorageIo::open(
            &crate::storage::parse_location(&format!("file://{}", dir.path().display())).unwrap(),
            None,
        )
        .unwrap();
        let err = crate::parquet::ParquetRows::open(&io, "nested.parquet", &["id", "nested"])
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("unsupported type"),
            "unsupported type must be a loud open error: {msg}"
        );
    }

    /// 13-review #8: an interior NUL in a text value is an ERROR status —
    /// it used to become an EMPTY string silently.
    #[test]
    fn interior_nul_is_error_status() {
        use arrow::array::{ArrayRef, StringArray};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::ffi::CStr;
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        crate::storage::test_allow_local_stores();
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "s",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![Some("a\0b")])) as ArrayRef],
        )
        .unwrap();
        let f = std::fs::File::create(dir.path().join("nul.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();

        let io = crate::storage::StorageIo::open(
            &crate::storage::parse_location(&format!("file://{}", dir.path().display())).unwrap(),
            None,
        )
        .unwrap();
        let rows = crate::parquet::ParquetRows::open(&io, "nul.parquet", &["s"]).expect("open ok");
        let h = Box::into_raw(Box::new(RowsHandle {
            rows,
            column_names: vec![CString::new("s").unwrap()],
            current_row: Vec::new(),
            current_null: Vec::new(),
            last_err: None,
            current_batch: None,
            batch_cols: Vec::new(),
            fallback_cell: CString::new("").unwrap_or_default(),
        }));
        let mut err = [0i8; 1024];
        let st = unsafe { pg_datalake_rows_next(h, err.as_mut_ptr() as *mut _, 1024) };
        assert_eq!(st, 2, "NUL value must be ERROR status, got {st}");
        let msg = unsafe { CStr::from_ptr(err.as_ptr()) }.to_string_lossy();
        assert!(
            msg.contains("NUL"),
            "diagnostic must name the problem: {msg}"
        );
        unsafe { drop(Box::from_raw(h)) };
    }

    /// 13-review #8: LargeUtf8 columns decode — downcasting them to
    /// StringArray silently produced SQL NULL.
    #[test]
    fn large_utf8_decodes() {
        use arrow::array::{ArrayRef, LargeStringArray};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::ffi::CStr;
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        crate::storage::test_allow_local_stores();
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "s",
            DataType::LargeUtf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(LargeStringArray::from(vec![Some("big")])) as ArrayRef],
        )
        .unwrap();
        let f = std::fs::File::create(dir.path().join("large.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();

        let io = crate::storage::StorageIo::open(
            &crate::storage::parse_location(&format!("file://{}", dir.path().display())).unwrap(),
            None,
        )
        .unwrap();
        let rows = crate::parquet::ParquetRows::open(&io, "large.parquet", &["s"])
            .expect("LargeUtf8 must open");
        let h = Box::into_raw(Box::new(RowsHandle {
            rows,
            column_names: vec![CString::new("s").unwrap()],
            current_row: Vec::new(),
            current_null: Vec::new(),
            last_err: None,
            current_batch: None,
            batch_cols: Vec::new(),
            fallback_cell: CString::new("").unwrap_or_default(),
        }));
        let mut err = [0i8; 1024];
        assert_eq!(
            unsafe { pg_datalake_rows_next(h, err.as_mut_ptr() as *mut _, 1024) },
            0,
            "row status"
        );
        let v = unsafe { pg_datalake_rows_col_text(h, 0) };
        assert_eq!(
            unsafe { CStr::from_ptr(v) }.to_string_lossy(),
            "big",
            "LargeUtf8 value must decode, not NULL"
        );
        unsafe { drop(Box::from_raw(h)) };
    }

    /// COW rewrite plumbing (M2 slice 28): rows_open_path re-reads a BASE
    /// snapshot file through the write session's credentials, scope-gated
    /// to the table root; commit_overwrite emits the overwrite snapshot
    /// with the deleted file's identity.
    #[test]
    fn ffi_rows_open_path_and_overwrite_commit() {
        let dir = tempfile::tempdir().unwrap();
        crate::storage::test_allow_local_stores();
        let location = format!("file://{}", dir.path().join("table").display());
        std::fs::create_dir_all(dir.path().join("table").join("data")).unwrap();
        std::fs::create_dir_all(dir.path().join("table").join("metadata")).unwrap();
        std::fs::create_dir_all(dir.path().join("metadata")).unwrap();

        // base data file: rows 1 and 2 (the file an UPDATE/DELETE rewrites)
        let mut w = crate::writer::BatchWriter::new(vec![crate::writer::WriteColumn {
            name: "id".into(),
            ty: crate::catalog::metadata::IcebergType::Long,
            field_id: Some(1),
        }])
        .unwrap();
        w.push(&[crate::parquet::CellValue::Int64(1)]).unwrap();
        w.push(&[crate::parquet::CellValue::Int64(2)]).unwrap();
        let wf = w
            .write_file(
                &crate::storage::StorageIo::open(
                    &crate::storage::parse_location(&location).unwrap(),
                    None,
                )
                .unwrap(),
                "data",
                0,
            )
            .unwrap();
        let data_path = format!("{location}/{}", wf.key);
        let data_size = wf.file_size_in_bytes;

        // base snapshot 42: the data file is LIVE (the liveness check
        // re-verifies deleted paths against this base before committing)
        let manifest_bytes = crate::snapshot_write::build_manifest_bytes(
            &[crate::snapshot_write::NewDataFile {
                file_path: data_path.clone(),
                record_count: 2,
                file_size_in_bytes: data_size,
                ..Default::default()
            }],
            &[],
            42,
            3,
        )
        .unwrap();
        std::fs::create_dir_all(dir.path().join("table").join("metadata")).unwrap();
        std::fs::write(
            dir.path().join("table").join("metadata").join("m0.avro"),
            &manifest_bytes,
        )
        .unwrap();
        let list_bytes = crate::snapshot_write::build_manifest_list_bytes(&[
            crate::manifest::ManifestListEntry {
                manifest_path: format!("{location}/metadata/m0.avro"),
                manifest_length: manifest_bytes.len() as i64,
                partition_spec_id: 0,
                content: Some(0),
                added_snapshot_id: 42,
                added_files_count: Some(1),
                existing_files_count: None,
                deleted_files_count: None,
                added_rows_count: Some(2),
                existing_rows_count: None,
                deleted_rows_count: None,
                sequence_number: Some(3),
                min_sequence_number: Some(3),
            },
        ])
        .unwrap();
        std::fs::write(
            dir.path().join("table").join("metadata").join("snap.avro"),
            list_bytes,
        )
        .unwrap();

        let metadata_json = format!(
            r#"{{"format-version": 2,
                "table-uuid": "1ca92afa-70b9-4d02-9ba6-7ee1a3ac0f5e",
                "location": "{location}",
                "last-sequence-number": 3,
                "schemas": [{{"type": "struct", "schema-id": 0, "fields": [
                    {{"id": 1, "name": "id", "required": false, "type": "long"}}]}}],
                "current-schema-id": 0,
                "snapshots": [{{"snapshot-id": 42, "timestamp-ms": 1712345678000,
                                "manifest-list": "{location}/metadata/snap.avro",
                                "summary": {{"operation": "append", "added-records": "2"}}}}],
                "current-snapshot-id": 42}}"#,
        );
        let md_path = dir.path().join("metadata").join("00000-x.metadata.json");
        std::fs::write(&md_path, &metadata_json).unwrap();

        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let addr = server.server_addr().to_string();
        let md_capture = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
        let md_capture_t = std::sync::Arc::clone(&md_capture);
        let md_path_srv = md_path.clone();
        std::thread::spawn(move || {
            for _ in 0..4 {
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
                std::ptr::null(),
                namespace.as_ptr(),
                table.as_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                schema_json.as_ptr(),
                err.as_mut_ptr(),
                err.len(),
            )
        };
        assert!(!ws.is_null(), "write_new: {}", errmsg(&err));

        // rows_open_path: read the base file back (the C rewrite pass's
        // "survivors" source), through the session's storage credentials
        {
            let path = CString::new(data_path.as_str()).unwrap();
            let proj = CString::new(r#"["id"]"#).unwrap();
            let rows = unsafe {
                pg_datalake_rows_open_path(
                    ws,
                    path.as_ptr(),
                    proj.as_ptr(),
                    err.as_mut_ptr(),
                    err.len(),
                )
            };
            assert!(!rows.is_null(), "rows_open_path: {}", errmsg(&err));
            let mut got = |h: *mut RowsHandle| -> i64 {
                let st = unsafe { pg_datalake_rows_next(h, err.as_mut_ptr() as *mut _, err.len()) };
                assert_eq!(st, 0, "row expected, status {st}: {}", errmsg(&err));
                unsafe { CStr::from_ptr(pg_datalake_rows_col_text(h, 0)) }
                    .to_string_lossy()
                    .parse::<i64>()
                    .unwrap()
            };
            assert_eq!(got(rows), 1);
            assert_eq!(got(rows), 2);
            let st = unsafe { pg_datalake_rows_next(rows, err.as_mut_ptr() as *mut _, err.len()) };
            assert_eq!(st, 1, "EOF");
            unsafe { pg_datalake_rows_close(rows) };

            // transport policy: OUT-OF-ROOT paths are rejected
            let evil = CString::new("file:///etc/passwd").unwrap();
            let blocked = unsafe {
                pg_datalake_rows_open_path(
                    ws,
                    evil.as_ptr(),
                    proj.as_ptr(),
                    err.as_mut_ptr(),
                    err.len(),
                )
            };
            assert!(blocked.is_null(), "out-of-root path must be rejected");
            assert!(errmsg(&err).contains("outside") || errmsg(&err).contains("root"));
        }

        // commit_overwrite: deleted identity travels to the snapshot
        let deleted_json =
            CString::new(format!(r#"[["{}", 2, {}]]"#, data_path, data_size)).unwrap();
        let loc = unsafe {
            pg_datalake_write_commit_overwrite(
                ws,
                deleted_json.as_ptr(),
                0,
                err.as_mut_ptr(),
                err.len(),
            )
        };
        assert!(!loc.is_null(), "overwrite commit: {}", errmsg(&err));
        unsafe { pg_datalake_write_commit_result_free(loc) };

        let body = md_capture.lock().unwrap().clone();
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        let snap = &v["updates"][0]["snapshot"];
        assert_eq!(snap["summary"]["operation"], "overwrite", "{body}");
        assert_eq!(snap["summary"]["deleted-data-files"], "1", "{body}");
        assert_eq!(snap["summary"]["deleted-records"], "2", "{body}");

        unsafe { pg_datalake_write_free(ws) };
    }

    // ===== M3-R1: typed batch read ABI (13-review perf #3) =====
    // pg_datalake_rows_batch_next: one call per arrow batch returns typed
    // per-column views + a live-row index list (deletes applied in Rust),
    // so the C consumers build Datums directly — no per-row CString, no
    // InputFunctionCall on the hot path.

    /// Build a local multi-type table + mock catalog; returns (session,
    /// endpoint). Caller frees the session. Modelled on
    /// ffi_scan_roundtrip_with_mock_catalog (slice 20 harness).
    fn batch_fixture(
        dir: &tempfile::TempDir,
        fields_json: &str,
        batches: &[arrow::record_batch::RecordBatch],
        delete_positions: &[i64],
    ) -> (*mut ScanSession, CString) {
        use apache_avro::types::{Record, Value};
        use apache_avro::{Schema as AvroSchema, Writer};

        crate::storage::test_allow_local_stores();
        std::fs::create_dir_all(dir.path().join("table").join("data")).unwrap();
        std::fs::create_dir_all(dir.path().join("table").join("metadata")).unwrap();
        std::fs::create_dir_all(dir.path().join("metadata")).unwrap();

        let total_rows: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();
        let data_path = format!("file://{}/table/data/m.parquet", dir.path().display());
        let f =
            std::fs::File::create(dir.path().join("table").join("data").join("m.parquet")).unwrap();
        let props = parquet::file::properties::WriterProperties::builder()
            .set_max_row_group_size(batches[0].num_rows())
            .build();
        let mut w =
            parquet::arrow::ArrowWriter::try_new(f, batches[0].schema(), Some(props)).unwrap();
        for b in batches {
            w.write(b).unwrap();
        }
        w.close().unwrap();

        if !delete_positions.is_empty() {
            let del_schema = arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("file_path", arrow::datatypes::DataType::Utf8, false),
                arrow::datatypes::Field::new("pos", arrow::datatypes::DataType::Int64, false),
            ]);
            let n = delete_positions.len();
            let del_batch = arrow::record_batch::RecordBatch::try_new(
                std::sync::Arc::new(del_schema),
                vec![
                    std::sync::Arc::new(arrow::array::StringArray::from(vec![
                        data_path.as_str();
                        n
                    ])),
                    std::sync::Arc::new(arrow::array::Int64Array::from(delete_positions.to_vec())),
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

        // manifest with one ADDED data entry (+ the delete entry when present)
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
            ("file_path".into(), Value::String(data_path.clone())),
            ("file_format".into(), Value::String("parquet".into())),
            ("partition".into(), Value::Union(0, Box::new(Value::Null))),
            ("record_count".into(), Value::Long(total_rows)),
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
        entry.put("data_file", data_file);
        writer.append(entry).unwrap();
        std::fs::write(
            dir.path().join("table").join("metadata").join("mn1.avro"),
            writer.into_inner().unwrap(),
        )
        .unwrap();

        let mn2 = !delete_positions.is_empty();
        if mn2 {
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
                (
                    "record_count".into(),
                    Value::Long(delete_positions.len() as i64),
                ),
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
        }

        // manifest list: mn1 (+ mn2)
        let lschema = AvroSchema::parse_str(crate::manifest::MANIFEST_LIST_SCHEMA_V2).unwrap();
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
        mrec.put("added_rows_count", Some(total_rows));
        mrec.put("existing_rows_count", Some(0i64));
        mrec.put("deleted_rows_count", Some(0i64));
        mrec.put("partitions", None::<Value>);
        mrec.put("key_metadata", None::<Value>);
        mrec.put("sequence_number", Some(1i64));
        mrec.put("min_sequence_number", Some(1i64));
        lwriter.append(mrec).unwrap();
        if mn2 {
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
            mrec2.put("added_rows_count", Some(delete_positions.len() as i64));
            mrec2.put("existing_rows_count", Some(0i64));
            mrec2.put("deleted_rows_count", Some(0i64));
            mrec2.put("partitions", None::<Value>);
            mrec2.put("key_metadata", None::<Value>);
            mrec2.put("sequence_number", Some(2i64));
            mrec2.put("min_sequence_number", Some(2i64));
            lwriter.append(mrec2).unwrap();
        }
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
                "schemas": [{{"type": "struct", "schema-id": 0, "fields": [{}]}}],
                "current-schema-id": 0,
                "snapshots": [{{"snapshot-id": 42, "timestamp-ms": 1712345678000,
                                "manifest-list": "file://{}/table/metadata/mn-list.avro",
                                "summary": {{"operation": "append"}}}}],
                "current-snapshot-id": 42}}"#,
            dir.path().display(),
            fields_json,
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
            for _ in 0..4 {
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
        assert!(!session.is_null(), "scan_new: {:?}", unsafe {
            CStr::from_ptr(err.as_ptr()).to_string_lossy()
        });
        (session, endpoint)
    }

    /// The batch ABI acceptance anchor: one arrow batch, nine column types,
    /// a null in text and bool columns, an empty (non-null) binary, a
    /// position delete removing absolute position 1. Kinds, live indices,
    /// null bytes, values and rendered spans must all match the per-row
    /// path's semantics.
    #[test]
    fn batch_ffi_multitype_with_nulls_and_position_delete() {
        use arrow::array::{
            BinaryArray, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array,
            StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
        };
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let fields = r#"
            {"id": 1, "name": "b", "required": false, "type": "boolean"},
            {"id": 2, "name": "i4", "required": false, "type": "int"},
            {"id": 3, "name": "i8", "required": false, "type": "long"},
            {"id": 4, "name": "f8", "required": false, "type": "double"},
            {"id": 5, "name": "s", "required": false, "type": "string"},
            {"id": 6, "name": "ts", "required": false, "type": "timestamp"},
            {"id": 7, "name": "bin", "required": false, "type": "binary"},
            {"id": 8, "name": "d32", "required": false, "type": "date"},
            {"id": 9, "name": "t64", "required": false, "type": "time"}"#;
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("b", DataType::Boolean, true),
            Field::new("i4", DataType::Int32, true),
            Field::new("i8", DataType::Int64, true),
            Field::new("f8", DataType::Float64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("bin", DataType::Binary, true),
            Field::new("d32", DataType::Date32, true),
            Field::new("t64", DataType::Time64(TimeUnit::Microsecond), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Float64Array::from(vec![1.5, 2.5, 3.5])),
                Arc::new(StringArray::from(vec![Some("alpha"), None, Some("gamma")])),
                Arc::new(TimestampMicrosecondArray::from(
                    vec![1_600_000_000_000_000i64; 3],
                )),
                Arc::new(BinaryArray::from(vec![
                    vec![0x01u8, 0xff].as_slice(),
                    [].as_slice(),
                    [0x00u8, 0x7f].as_slice(),
                ])),
                Arc::new(Date32Array::from(vec![19000, 19001, 19002])),
                Arc::new(Time64MicrosecondArray::from(vec![
                    3_600_000_000i64,
                    0,
                    86_399_000_000,
                ])),
            ],
        )
        .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let (session, _endpoint) = batch_fixture(&dir, fields, &[batch], &[1]);

        let proj = CString::new(r#"["b","i4","i8","f8","s","ts","bin","d32","t64"]"#).unwrap();
        let mut err = [0 as c_char; 1024];
        let rows = unsafe {
            pg_datalake_rows_open(session, 0, proj.as_ptr(), err.as_mut_ptr(), err.len())
        };
        assert!(!rows.is_null(), "rows_open: {:?}", unsafe {
            CStr::from_ptr(err.as_ptr()).to_string_lossy()
        });

        let mut rb: PgDatalakeRowBatch = unsafe { std::mem::zeroed() };
        let st = unsafe { pg_datalake_rows_batch_next(rows, &mut rb, err.as_mut_ptr(), err.len()) };
        assert_eq!(st, 0, "first batch: {:?}", unsafe {
            CStr::from_ptr(err.as_ptr()).to_string_lossy()
        });
        assert_eq!(rb.rows, 3);
        assert_eq!(rb.live_count, 2, "position delete removed local row 1");
        let live = unsafe { std::slice::from_raw_parts(rb.live_idx, rb.live_count as usize) };
        assert_eq!(live, &[0, 2]);
        let cols = unsafe { std::slice::from_raw_parts(rb.cols, rb.n_cols as usize) };

        // kinds: bool, widened i32, i64, f64, text, ts_us, bytes, date32, rendered(time)
        let expect_kinds = [1i32, 2, 2, 3, 5, 4, 6, 7, 8];
        for (i, k) in expect_kinds.iter().enumerate() {
            assert_eq!(cols[i].kind, *k, "column {i} kind");
        }

        // nulls: bool[2] and text[1]
        let b_nulls = unsafe { std::slice::from_raw_parts(cols[0].nulls, 3) };
        assert_eq!(b_nulls, &[0, 0, 1]);
        let s_nulls = unsafe { std::slice::from_raw_parts(cols[4].nulls, 3) };
        assert_eq!(s_nulls, &[0, 1, 0]);

        // widened int32 buffer
        let i4 = unsafe { std::slice::from_raw_parts(cols[1].values as *const i64, 3) };
        assert_eq!(i4, &[10, 20, 30]);
        let i8 = unsafe { std::slice::from_raw_parts(cols[2].values as *const i64, 3) };
        assert_eq!(i8, &[1, 2, 3]);
        let f8 = unsafe { std::slice::from_raw_parts(cols[3].values as *const f64, 3) };
        assert_eq!(f8, &[1.5, 2.5, 3.5]);
        let ts = unsafe { std::slice::from_raw_parts(cols[5].values as *const i64, 3) };
        assert_eq!(ts, &[1_600_000_000_000_000i64; 3]);
        let d32 = unsafe { std::slice::from_raw_parts(cols[7].values as *const i64, 3) };
        assert_eq!(d32, &[19000, 19001, 19002]);

        // text spans (zero-copy) + binary spans incl. EMPTY NON-NULL and
        // an interior-NUL byte (allowed in binary — the loud check is for
        // TEXT columns only)
        let spans = unsafe { std::slice::from_raw_parts(cols[4].spans, 3) };
        assert_eq!(
            unsafe { std::slice::from_raw_parts(spans[0].ptr, 5) },
            b"alpha"
        );
        assert_eq!(spans[1].len, 0, "null text has a span but len 0");
        assert_eq!(
            unsafe { std::slice::from_raw_parts(spans[2].ptr, 5) },
            b"gamma"
        );
        let bspans = unsafe { std::slice::from_raw_parts(cols[6].spans, 3) };
        assert_eq!(
            unsafe { std::slice::from_raw_parts(bspans[0].ptr, 2) },
            &[0x01u8, 0xff]
        );
        assert_eq!(bspans[1].len, 0, "empty binary is NOT null");
        assert_eq!(
            unsafe { std::slice::from_raw_parts(bspans[2].ptr, 2) },
            &[0x00u8, 0x7f]
        );

        // rendered time spans match the per-row path exactly
        let tspans = unsafe { std::slice::from_raw_parts(cols[8].spans, 3) };
        assert_eq!(
            unsafe { std::slice::from_raw_parts(tspans[0].ptr, 15) },
            b"01:00:00.000000"
        );
        assert_eq!(
            unsafe { std::slice::from_raw_parts(tspans[2].ptr, 15) },
            b"23:59:59.000000"
        );

        // second call: EOF
        let st = unsafe { pg_datalake_rows_batch_next(rows, &mut rb, err.as_mut_ptr(), err.len()) };
        assert_eq!(st, 1, "EOF after the single batch");
        unsafe { pg_datalake_rows_close(rows) };
        unsafe { pg_datalake_scan_free(session) };
    }

    /// Batches advance across the reader's RecordBatch boundaries
    /// (arrow-rs yields up to 1024 rows per batch, merging row groups);
    /// a position delete must die in whichever batch holds its absolute
    /// position, and live indices are batch-local (never misread).
    #[test]
    fn batch_ffi_advances_across_batches() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let fields = r#"{"id": 1, "name": "id", "required": false, "type": "long"}"#;
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            true,
        )]));
        let b1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(
                (1..=1025i64).collect::<Vec<_>>(),
            ))],
        )
        .unwrap();
        let b2 = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(
                (1026..=2050i64).collect::<Vec<_>>(),
            ))],
        )
        .unwrap();

        let dir = tempfile::tempdir().unwrap();
        // absolute position 1024 (id=1025) dies: it sits in the SECOND
        // reader batch (batch_size 1024), local index 0
        let (session, _endpoint) = batch_fixture(&dir, fields, &[b1, b2], &[1024]);

        let proj = CString::new(r#"["id"]"#).unwrap();
        let mut err = [0 as c_char; 1024];
        let rows = unsafe {
            pg_datalake_rows_open(session, 0, proj.as_ptr(), err.as_mut_ptr(), err.len())
        };
        assert!(!rows.is_null(), "rows_open: {:?}", unsafe {
            CStr::from_ptr(err.as_ptr()).to_string_lossy()
        });

        let mut rb: PgDatalakeRowBatch = unsafe { std::mem::zeroed() };
        let st = unsafe { pg_datalake_rows_batch_next(rows, &mut rb, err.as_mut_ptr(), err.len()) };
        assert_eq!(st, 0);
        assert_eq!(rb.rows, 1024, "first reader batch");
        assert_eq!(rb.live_count, 1024);
        let live = unsafe { std::slice::from_raw_parts(rb.live_idx, 2) };
        assert_eq!(&live[..2], &[0, 1]);
        let cols = unsafe { std::slice::from_raw_parts(rb.cols, 1) };
        let ids = unsafe { std::slice::from_raw_parts(cols[0].values as *const i64, 2) };
        assert_eq!(ids, &[1, 2]);

        let st = unsafe { pg_datalake_rows_batch_next(rows, &mut rb, err.as_mut_ptr(), err.len()) };
        assert_eq!(st, 0);
        assert_eq!(rb.rows, 1024, "second reader batch");
        assert_eq!(
            rb.start_abs, 1024,
            "second batch starts at absolute row 1024"
        );
        assert_eq!(rb.live_count, 1023, "abs position 1024 died in this batch");
        let live = unsafe { std::slice::from_raw_parts(rb.live_idx, rb.live_count as usize) };
        assert_eq!(
            live[0], 1,
            "local index 0 (id=1024) is dead; 1 (id=1025) survives"
        );
        assert_eq!(*live.last().unwrap(), 1023);
        let cols = unsafe { std::slice::from_raw_parts(rb.cols, 1) };
        let ids = unsafe { std::slice::from_raw_parts(cols[0].values as *const i64, 2) };
        assert_eq!(ids[0], 1025, "dead row's raw buffer slot still holds 1025");
        assert_eq!(ids[1], 1026);

        let st = unsafe { pg_datalake_rows_batch_next(rows, &mut rb, err.as_mut_ptr(), err.len()) };
        assert_eq!(st, 0, "third batch (46 remaining rows)");
        assert_eq!(rb.rows, 2, "2050 - 2*1024 = 2");
        assert_eq!(rb.live_count, 2);

        let st = unsafe { pg_datalake_rows_batch_next(rows, &mut rb, err.as_mut_ptr(), err.len()) };
        assert_eq!(st, 1, "EOF");
        unsafe { pg_datalake_rows_close(rows) };
        unsafe { pg_datalake_scan_free(session) };
    }

    /// The render_row law carries over: interior NUL in a TEXT column is a
    /// loud batch error, never a truncated string (a silent truncation
    /// would fabricate a value).
    #[test]
    fn batch_interior_nul_text_is_loud() {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let fields = r#"{"id": 1, "name": "s", "required": false, "type": "string"}"#;
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "s",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                "ok".to_string(),
                "a\u{0}b".to_string(),
            ]))],
        )
        .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let (session, _endpoint) = batch_fixture(&dir, fields, &[batch], &[]);

        let proj = CString::new(r#"["s"]"#).unwrap();
        let mut err = [0 as c_char; 1024];
        let rows = unsafe {
            pg_datalake_rows_open(session, 0, proj.as_ptr(), err.as_mut_ptr(), err.len())
        };
        assert!(!rows.is_null(), "rows_open: {:?}", unsafe {
            CStr::from_ptr(err.as_ptr()).to_string_lossy()
        });

        let mut rb: PgDatalakeRowBatch = unsafe { std::mem::zeroed() };
        let st = unsafe { pg_datalake_rows_batch_next(rows, &mut rb, err.as_mut_ptr(), err.len()) };
        assert_eq!(st, 2, "loud ERROR, not a batch");
        let msg = unsafe { CStr::from_ptr(err.as_ptr()) }.to_string_lossy();
        assert!(msg.contains("interior NUL"), "error names the cause: {msg}");
        unsafe { pg_datalake_rows_close(rows) };
        unsafe { pg_datalake_scan_free(session) };
    }

    /// M3-R2 e2e anchor: OUR OWN committed table must carry prunable
    /// bounds. Write ids 1..5 via the FFI write path, commit against the
    /// mock, then plan two scans: `id > 1000` MUST prune to zero tasks
    /// (bounds present in OUR manifest), no predicate reads all 5 rows.
    /// Before this slice the writer emitted EMPTY metric maps — pruning
    /// was inert on every PG-written table (conservative keep forever).
    #[test]
    fn ffi_write_commit_stats_prune_predicate() {
        let dir = tempfile::tempdir().unwrap();
        crate::storage::test_allow_local_stores();
        std::fs::create_dir_all(dir.path().join("table").join("data")).unwrap();
        std::fs::create_dir_all(dir.path().join("table").join("metadata")).unwrap();
        std::fs::create_dir_all(dir.path().join("metadata")).unwrap();
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

        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let addr = server.server_addr().to_string();
        let md_path_srv = md_path.clone();
        let md_capture = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
        let md_capture_t2 = std::sync::Arc::clone(&md_capture);
        std::thread::spawn(move || {
            for _ in 0..10 {
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
                    *md_capture_t2.lock().unwrap() = body_s;
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
        fn errmsg2(err: &[c_char]) -> String {
            unsafe { CStr::from_ptr(err.as_ptr()) }
                .to_string_lossy()
                .into_owned()
        }

        let ws = unsafe {
            pg_datalake_write_new(
                endpoint.as_ptr(),
                warehouse.as_ptr(),
                std::ptr::null(),
                namespace.as_ptr(),
                table.as_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                schema_json.as_ptr(),
                err.as_mut_ptr(),
                err.len(),
            )
        };
        assert!(!ws.is_null(), "write_new: {}", errmsg2(&err));
        for row in ["[1]", "[2]", "[3]", "[4]", "[5]"] {
            let rj = CString::new(row).unwrap();
            assert!(
                unsafe { pg_datalake_write_push(ws, rj.as_ptr(), err.as_mut_ptr(), err.len()) },
                "push {row}: {}",
                errmsg2(&err)
            );
        }
        let loc = unsafe { pg_datalake_write_commit(ws, 0, err.as_mut_ptr(), err.len()) };
        assert!(!loc.is_null(), "commit: {}", errmsg2(&err));
        unsafe { pg_datalake_write_commit_result_free(loc) };

        // read-back: merge the committed snapshot into the on-disk metadata
        // (what the catalog does remotely) so the scans re-plan from it
        {
            let body = md_capture.lock().unwrap().clone();
            let v: serde_json::Value = serde_json::from_str(&body).unwrap();
            let snap = &v["updates"][0]["snapshot"];
            let snap_id = snap["snapshot-id"].as_i64().unwrap();
            let ml_path = snap["manifest-list"].as_str().unwrap().to_string();
            // wire-level proof: the manifest ENTRY carries the bounds
            // (LE i64, keyed by field id) that the prune above decoded
            let manifest_list = crate::manifest::parse_manifest_list(
                &std::fs::read(ml_path.trim_start_matches("file://")).unwrap(),
            )
            .unwrap();
            let entries = crate::manifest::parse_manifest(
                &std::fs::read(manifest_list[0].manifest_path.trim_start_matches("file://"))
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(
                entries[0].data_file.lower_bounds.get(&1),
                Some(&1i64.to_le_bytes().to_vec()),
                "lower bound"
            );
            assert_eq!(
                entries[0].data_file.upper_bounds.get(&1),
                Some(&5i64.to_le_bytes().to_vec()),
                "upper bound"
            );
            assert_eq!(entries[0].data_file.value_counts.get(&1), Some(&5));
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
        }

        let pred = CString::new(r#"{"op":"gt","field":"id","value":1000}"#).unwrap();
        let pruned = unsafe {
            pg_datalake_scan_new(
                endpoint.as_ptr(),
                warehouse.as_ptr(),
                std::ptr::null(),
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
        assert!(!pruned.is_null(), "pruned scan: {}", errmsg2(&err));
        assert_eq!(
            unsafe { pg_datalake_scan_task_count(pruned) },
            0,
            "id > 1000 must prune OUR file (bounds 1..5)"
        );
        unsafe { pg_datalake_scan_free(pruned) };

        // sanity: no predicate keeps the file with all 5 rows
        let full = unsafe {
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
        assert!(!full.is_null(), "full scan: {}", errmsg2(&err));
        assert_eq!(unsafe { pg_datalake_scan_task_count(full) }, 1);
        assert_eq!(unsafe { pg_datalake_scan_task_rows(full, 0) }, 5);
        unsafe { pg_datalake_scan_free(full) };
    }

    /// P1 (spec 15 §7.2): writer rollover at the target-file-size
    /// threshold — one transaction emitting MANY size-bounded files in
    /// ONE append snapshot (small-file prevention, G3). With a 1-byte
    /// threshold every push rolls a file: 6 rows -> 6 files, each with
    /// exact record_count and per-file bounds, committed as ONE
    /// snapshot; read-back sees all 6 rows.
    #[test]
    fn ffi_write_commit_rollover_multi_file() {
        let dir = tempfile::tempdir().unwrap();
        crate::storage::test_allow_local_stores();
        std::fs::create_dir_all(dir.path().join("table").join("data")).unwrap();
        std::fs::create_dir_all(dir.path().join("table").join("metadata")).unwrap();
        std::fs::create_dir_all(dir.path().join("metadata")).unwrap();
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

        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let addr = server.server_addr().to_string();
        let md_capture = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
        let md_capture_t = std::sync::Arc::clone(&md_capture);
        let md_path_srv = md_path.clone();
        std::thread::spawn(move || {
            for _ in 0..10 {
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
        fn errmsg3(err: &[c_char]) -> String {
            unsafe { CStr::from_ptr(err.as_ptr()) }
                .to_string_lossy()
                .into_owned()
        }

        let ws = unsafe {
            pg_datalake_write_new(
                endpoint.as_ptr(),
                warehouse.as_ptr(),
                std::ptr::null(),
                namespace.as_ptr(),
                table.as_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                schema_json.as_ptr(),
                err.as_mut_ptr(),
                err.len(),
            )
        };
        assert!(!ws.is_null(), "write_new: {}", errmsg3(&err));
        // 1-byte threshold: every buffered row rolls its own file
        assert!(
            unsafe { pg_datalake_write_set_target_file_size(ws, 1, err.as_mut_ptr(), err.len()) },
            "set threshold: {}",
            errmsg3(&err)
        );

        for row in ["[1]", "[2]", "[3]", "[4]", "[5]", "[6]"] {
            let rj = CString::new(row).unwrap();
            assert!(
                unsafe { pg_datalake_write_push(ws, rj.as_ptr(), err.as_mut_ptr(), err.len()) },
                "push {row}: {}",
                errmsg3(&err)
            );
        }
        let loc = unsafe { pg_datalake_write_commit(ws, 0, err.as_mut_ptr(), err.len()) };
        assert!(!loc.is_null(), "commit: {}", errmsg3(&err));
        unsafe { pg_datalake_write_commit_result_free(loc) };

        // ONE snapshot carrying SIX files
        {
            let body = md_capture.lock().unwrap().clone();
            let v: serde_json::Value = serde_json::from_str(&body).unwrap();
            let snap = &v["updates"][0]["snapshot"];
            assert_eq!(snap["summary"]["added-data-files"], "6", "{body}");
            let ml_path = snap["manifest-list"].as_str().unwrap().to_string();
            let manifest_list = crate::manifest::parse_manifest_list(
                &std::fs::read(ml_path.trim_start_matches("file://")).unwrap(),
            )
            .unwrap();
            let entries = crate::manifest::parse_manifest(
                &std::fs::read(manifest_list[0].manifest_path.trim_start_matches("file://"))
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(entries.len(), 6, "six manifest entries");
            for (i, e) in entries.iter().enumerate() {
                assert_eq!(e.data_file.record_count, 1, "entry {i} rows");
                assert_eq!(
                    e.data_file.lower_bounds.get(&1),
                    Some(&((i + 1) as i64).to_le_bytes().to_vec()),
                    "entry {i} lower bound"
                );
            }
        }
        unsafe { pg_datalake_write_free(ws) };
    }

    /// P0 integration proof: the scan path's manifest-list/manifest/puffin
    /// reads go through the immutable-path block cache — after one scan
    /// has cached them, the FILES can be deleted from the store and a
    /// second scan STILL plans correctly (no second GET exists).
    /// metadata.json itself is deliberately uncached (the catalog's
    /// pointer target — spec 15 §2a correction).
    #[test]
    fn scan_reads_served_from_block_cache_after_store_deletion() {
        let dir = tempfile::tempdir().unwrap();
        crate::storage::test_allow_local_stores();
        std::fs::create_dir_all(dir.path().join("table").join("data")).unwrap();
        std::fs::create_dir_all(dir.path().join("table").join("metadata")).unwrap();
        std::fs::create_dir_all(dir.path().join("metadata")).unwrap();
        // data parquet (3 rows)
        {
            use arrow::array::Int64Array;
            use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
            use arrow::record_batch::RecordBatch;
            use std::sync::Arc;
            let schema = Arc::new(ArrowSchema::new(vec![Field::new(
                "id",
                DataType::Int64,
                true,
            )]));
            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))])
                    .unwrap();
            let f = std::fs::File::create(dir.path().join("table").join("data").join("a.parquet"))
                .unwrap();
            let mut w = parquet::arrow::ArrowWriter::try_new(f, batch.schema(), None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }
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

        // build manifest list + manifest on disk via the write-side helpers
        let manifest_bytes = crate::snapshot_write::build_manifest_bytes(
            &[crate::snapshot_write::NewDataFile {
                file_path: format!("file://{}/table/data/a.parquet", dir.path().display()),
                record_count: 3,
                file_size_in_bytes: 4096,
                ..Default::default()
            }],
            &[],
            42,
            1,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("table").join("metadata").join("mn1.avro"),
            &manifest_bytes,
        )
        .unwrap();
        let list_bytes = crate::snapshot_write::build_manifest_list_bytes(&[
            crate::manifest::ManifestListEntry {
                manifest_path: format!("file://{}/table/metadata/mn1.avro", dir.path().display()),
                manifest_length: manifest_bytes.len() as i64,
                partition_spec_id: 0,
                added_snapshot_id: 42,
                added_files_count: Some(1),
                existing_files_count: Some(0),
                deleted_files_count: Some(0),
                added_rows_count: Some(3),
                existing_rows_count: Some(0),
                deleted_rows_count: Some(0),
                content: Some(0),
                sequence_number: Some(1),
                min_sequence_number: Some(1),
            },
        ])
        .unwrap();
        std::fs::write(
            dir.path()
                .join("table")
                .join("metadata")
                .join("mn-list.avro"),
            &list_bytes,
        )
        .unwrap();

        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let addr = server.server_addr().to_string();
        let md_path = dir.path().join("metadata").join("00000-x.metadata.json");
        std::thread::spawn(move || {
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
        let namespace = CString::new("analytics").unwrap();
        let table = CString::new("events").unwrap();
        let mut err = [0 as c_char; 1024];
        let scan_once = |err: &mut [c_char]| -> u64 {
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
            assert!(!session.is_null(), "scan_new failed");
            let n = unsafe { pg_datalake_scan_task_count(session) };
            unsafe { pg_datalake_scan_free(session) };
            n
        };

        // first scan: fills the cache (manifest list + manifest)
        assert_eq!(scan_once(&mut err), 1, "first scan plans 1 task");
        // delete the immutable objects from the store entirely
        std::fs::remove_file(
            dir.path()
                .join("table")
                .join("metadata")
                .join("mn-list.avro"),
        )
        .unwrap();
        std::fs::remove_file(dir.path().join("table").join("metadata").join("mn1.avro")).unwrap();
        // second scan: planned ENTIRELY from cache
        assert_eq!(
            scan_once(&mut err),
            1,
            "second scan plans from cache despite deletion"
        );
    }

    /// P3 core: delta payload round-trip — rows written in the typed
    /// row-JSON come back as the per-row renderer's exact text forms,
    /// nulls flagged, tri-state next.
    #[test]
    fn delta_rows_roundtrip() {
        crate::storage::test_allow_local_stores();
        let payload = CString::new(r#"[[1,"a"],[2,null],[3,"c"]]"#).unwrap();
        let schema =
            CString::new(r#"[{"name":"id","type":"long"},{"name":"name","type":"string"}]"#)
                .unwrap();
        let mut err = [0 as c_char; 1024];
        let h = unsafe {
            pg_datalake_delta_rows_open(
                payload.as_ptr(),
                schema.as_ptr(),
                err.as_mut_ptr(),
                err.len(),
            )
        };
        assert!(!h.is_null(), "open: {}", unsafe {
            CStr::from_ptr(err.as_ptr()).to_string_lossy()
        });
        assert_eq!(unsafe { pg_datalake_delta_rows_column_count(h) }, 2);
        for (id, name) in [("1", "a"), ("2", ""), ("3", "c")] {
            assert_eq!(
                unsafe { pg_datalake_delta_rows_next(h, err.as_mut_ptr() as *mut _, 1024) },
                0
            );
            let got_id =
                unsafe { CStr::from_ptr(pg_datalake_delta_rows_col_text(h, 0)) }
                    .to_string_lossy()
                    .into_owned();
            let name_null = unsafe { pg_datalake_delta_rows_col_null(h, 1) };
            assert_eq!(got_id, id);
            assert_eq!(name_null, name.is_empty() && id == "2", "{id}/{name}");
            if !name_null {
                let got = unsafe { CStr::from_ptr(pg_datalake_delta_rows_col_text(h, 1)) }
                    .to_string_lossy()
                    .into_owned();
                assert_eq!(got, name);
            }
        }
        assert_eq!(
            unsafe { pg_datalake_delta_rows_next(h, err.as_mut_ptr() as *mut _, 1024) },
            1,
            "EOF"
        );
        unsafe { pg_datalake_delta_rows_close(h) };
    }
}
