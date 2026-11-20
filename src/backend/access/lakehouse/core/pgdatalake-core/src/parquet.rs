//! Parquet row reads over object storage (specs/lakehouse/06-execution.md
//! §3). Slice 10: projected row iteration with a simple typed cell model —
//! Int32/Int64 normalize to Int64, Float32/Float64 to Float64; nested types
//! (struct/list/map) decode in the type-matrix slice (doc 05). Iceberg type
//! -> Postgres type mapping happens on the C/FDW side.

use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::storage::{StorageError, StorageIo};

/// A decoded cell of one row.
#[derive(Debug, Clone, PartialEq)]
pub enum CellValue {
    Null,
    Bool(bool),
    Int64(i64),
    /// Microseconds since the epoch, UTC (Iceberg timestamp semantics,
    /// doc 05 §4; nanosecond sources are truncated). The C/FDW side renders
    /// this as an ISO string for the type input function.
    TimestampMicros(i64),
    Float64(f64),
    Str(String),
    Bytes(Vec<u8>),
}

/// Format micros-since-epoch UTC as `YYYY-MM-DD HH:MM:SS.ffffff+00`
/// (proleptic Gregorian civil-from-days, no external deps).
pub fn format_timestamp_micros(micros: i64) -> String {
    let secs = micros.div_euclid(1_000_000);
    let sub = micros.rem_euclid(1_000_000);
    let days = secs.div_euclid(86_400);
    let secs_of_day = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02} {h:02}:{mi:02}:{s:02}.{f:06}+00",
        h = secs_of_day / 3600,
        mi = (secs_of_day % 3600) / 60,
        s = secs_of_day % 60,
        f = sub
    )
}

/// Format days-since-1970 as `YYYY-MM-DD` (identical bytes to
/// cell_from_arrow's Date32 branch — the batch fallback renders through
/// this and the input function, exactly as the per-row path did).
pub fn format_date32(days: i32) -> String {
    let (y, m, d) = civil_from_days(days as i64);
    format!("{y:04}-{m:02}-{d:02}")
}

/// Format an f64 exactly as the per-row renderer does (shortest repr).
pub fn format_f64(v: f64) -> String {
    v.to_string()
}

/// Howard Hinnant's civil_from_days algorithm (days since 1970-01-01).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097); // day of era [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// One equality-delete tuple: a data row dies when every listed column
/// matches this entry's value (slice 20, doc 07). `sequence_number` is the
/// delete file's effective sequence — it applies only to data files with a
/// strictly lower sequence (Iceberg v2 applicability).
#[derive(Debug, Clone, PartialEq)]
pub struct EqDeleteMatch {
    pub columns: Vec<String>,
    pub values: Vec<CellValue>,
    pub sequence_number: i64,
}

/// Delete filters applied while iterating rows of one data file (slice 20).
#[derive(Debug, Default, Clone)]
pub struct RowDeletes {
    /// Absolute row positions to skip — v2 position deletes for THIS data
    /// file (keyed per file at load time).
    pub positions: std::collections::BTreeSet<u64>,
    /// Equality delete tuples (applied to unpartitioned data files only in
    /// M1; partition-scoped application is a later slice).
    pub equality: Vec<EqDeleteMatch>,
}

/// Delete files loaded for one scan (session-level); sliced into a
/// per-data-file [`RowDeletes`] at rows-open time.
#[derive(Debug, Default, Clone)]
pub struct LoadedDeletes {
    /// data file path -> (delete sequence -> deleted absolute positions).
    /// Per-sequence so `for_task` can enforce v2 applicability: a position
    /// delete with sequence N applies to data files with sequence <= N
    /// (13-review blocker #5 — path/position alone over-applies when a
    /// path is reused by a later write).
    pub positions_by_file: std::collections::BTreeMap<
        String,
        std::collections::BTreeMap<i64, std::collections::BTreeSet<u64>>,
    >,
    /// equality delete tuples (from every eq delete file of the snapshot)
    pub equality: Vec<EqDeleteMatch>,
}

impl LoadedDeletes {
    /// The delete set for one data file. Applicability rules (doc 07,
    /// slice 20): position deletes are file-scoped (exact path match);
    /// equality deletes apply only to data files with a strictly lower
    /// sequence number, and only to unpartitioned data files in M1 —
    /// partition-scoped application errors loudly rather than silently
    /// mis-deleting across partitions.
    pub fn for_task(
        &self,
        file_path: &str,
        task_sequence_number: i64,
        partitioned: bool,
    ) -> Result<RowDeletes, StorageError> {
        if partitioned && !self.equality.is_empty() {
            return Err(StorageError::Config(format!(
                "equality deletes on partitioned data file {file_path} are not supported yet \
                 (partition-scoped application is a later M1 slice)"
            )));
        }
        let positions = self
            .positions_by_file
            .get(file_path)
            .map(|by_seq| {
                let mut out = std::collections::BTreeSet::new();
                // v2 applicability: delete seq N applies to data files
                // with sequence <= N
                for (seq, set) in by_seq {
                    if *seq >= task_sequence_number {
                        out.extend(set.iter().copied());
                    }
                }
                out
            })
            .unwrap_or_default();
        Ok(RowDeletes {
            positions,
            equality: self
                .equality
                .iter()
                .filter(|m| m.sequence_number > task_sequence_number)
                .cloned()
                .collect(),
        })
    }
}

/// Typed batch view kinds (M3-R1, 13-review perf #3). The discriminants
/// are the FFI ABI values (cbindgen exports the enum); adding a variant
/// is a breaking change for the C consumers.
#[repr(i32)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BatchKind {
    /// u8 per row, 0/1
    Bool = 1,
    /// i64 per row (Int64/Date64 raw, Int32 widened)
    Int64 = 2,
    /// f64 per row (Float64, Float32 widened)
    Float64 = 3,
    /// i64 micros-since-epoch per row (any timestamp unit, doc 05 §4 rules)
    TimestampUs = 4,
    /// zero-copy UTF-8 spans (`spans`)
    Text = 5,
    /// zero-copy raw spans (`spans`); interior NUL allowed
    Bytes = 6,
    /// i64 days-since-1970 per row
    Date32 = 7,
    /// input-function-compatible text spans, rendered once per batch
    /// (date/time/decimal/uuid) — identical bytes to the per-row path
    Rendered = 8,
}

/// A length-delimited byte range pointing INTO the current arrow batch.
/// Valid only until the next batch call or close — the C consumer must
/// copy anything it keeps.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct BatchSpan {
    pub ptr: *const u8,
    pub len: u64,
}

/// One column of a materialized batch. Exactly one of the buffers is
/// populated per `kind` (checked by `next_batch`'s construction).
#[derive(Debug)]
pub struct BatchColumnView {
    pub kind: BatchKind,
    pub bools: Vec<u8>,
    pub i64s: Vec<i64>,
    pub f64s: Vec<f64>,
    pub spans: Vec<BatchSpan>,
    /// backing store for Rendered spans
    pub rendered: Vec<String>,
    /// byte per row, 1 = NULL
    pub nulls: Vec<u8>,
}

/// One materialized arrow batch: typed column views plus the live-row
/// index list (deletion vectors and position/equality deletes applied —
/// slice 20 + doc 14 V1). C iterates `live_idx` only; positions of dead
/// rows in the buffers are unspecified.
#[derive(Debug)]
pub struct BatchView {
    /// total rows in the arrow batch (live + dead)
    pub rows: usize,
    /// ascending positions of surviving rows WITHIN THE BATCH (buffer
    /// indexes — the file-absolute position of live_idx[k] is
    /// `start_abs + live_idx[k]`)
    pub live_idx: Vec<u32>,
    /// file-absolute position of batch row 0 (position deletes are
    /// file-scoped, doc 07)
    pub start_abs: u64,
    /// one view per PROJECTED column (eq-delete internals excluded)
    pub cols: Vec<BatchColumnView>,
}

/// Projected row iterator over one parquet object.
#[derive(Debug)]
pub struct ParquetRows {
    batches: std::vec::IntoIter<arrow::record_batch::RecordBatch>,
    current: Option<arrow::record_batch::RecordBatch>,
    row: usize,
    column_names: Vec<String>,
    column_indices: Vec<usize>,
    /// width of the caller's projection (column_indices may exceed it —
    /// eq-delete internal columns ride beyond this mark)
    projection_len: usize,
    num_rows_in_current: usize,
    /// Absolute position of the current row within the FILE (position
    /// deletes are file-scoped, doc 07).
    batch_start_abs: u64,
    deletes: RowDeletes,
    /// Arrow schema indices for each equality-delete column (parallel to
    /// `deletes.equality[*].columns`); eq columns may lie outside the
    /// projection — they are read internally, never exposed.
    eq_col_indices: Vec<Vec<usize>>,
}

impl ParquetRows {
    /// Open a parquet object with a projection (column names, in the
    /// requested order). Unknown columns are a Config error (doc 03 §5:
    /// schema drift surfaces cleanly).
    pub fn open(io: &StorageIo, key: &str, projection: &[&str]) -> Result<Self, StorageError> {
        Self::open_internal(io, key, projection, RowDeletes::default())
    }

    /// Open with delete filters applied (slice 20, doc 07). Equality-delete
    /// columns outside the projection join the internal read set (never
    /// exposed); an eq column missing from the file is a Config error.
    pub fn open_with_deletes(
        io: &StorageIo,
        key: &str,
        projection: &[&str],
        deletes: RowDeletes,
    ) -> Result<Self, StorageError> {
        Self::open_internal(io, key, projection, deletes)
    }

    fn open_internal(
        io: &StorageIo,
        key: &str,
        projection: &[&str],
        deletes: RowDeletes,
    ) -> Result<Self, StorageError> {
        let raw = io.get_bytes(key)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(raw))
            .map_err(|e| StorageError::Decode(format!("parquet open: {e}")))?;
        let arrow_schema = builder.schema().clone();

        // internal read set: projection first, then any equality-delete
        // columns not already projected
        let mut read_names: Vec<String> = projection.iter().map(|s| s.to_string()).collect();
        for eq in &deletes.equality {
            for c in &eq.columns {
                if !read_names.contains(c) {
                    read_names.push(c.clone());
                }
            }
        }

        let mut column_indices = Vec::with_capacity(read_names.len());
        let projection_len = projection.len();
        for name in &read_names {
            let idx = arrow_schema
                .fields()
                .iter()
                .position(|f| f.name() == name)
                .ok_or_else(|| {
                    StorageError::Config(format!(
                        "column {name:?} not in parquet schema {:?}",
                        arrow_schema
                            .fields()
                            .iter()
                            .map(|f| f.name().clone())
                            .collect::<Vec<_>>()
                    ))
                })?;
            column_indices.push(idx);
        }

        // 13-review #8: unsupported Arrow types must fail BEFORE rows are
        // returned — the per-cell fallthrough maps unknown types to SQL
        // NULL, which silently fabricates data. eq columns are validated
        // too: an unmatchable key type would make deletes never apply.
        for (i, name) in read_names.iter().enumerate() {
            let ty = arrow_schema.field(column_indices[i]).data_type();
            if !is_supported_arrow_type(ty) {
                return Err(StorageError::Config(format!(
                    "column {name:?} has unsupported type {ty} in this reader; \
                     unsupported columns must not surface as NULL"
                )));
            }
        }

        // eq columns resolve against the arrow schema independently (they
        // may be projected or internal)
        let mut eq_col_indices = Vec::with_capacity(deletes.equality.len());
        for eq in &deletes.equality {
            let mut idxs = Vec::with_capacity(eq.columns.len());
            for c in &eq.columns {
                let idx = arrow_schema
                    .fields()
                    .iter()
                    .position(|f| f.name() == c)
                    .ok_or_else(|| {
                        StorageError::Config(format!(
                            "equality delete column {c:?} not in parquet schema"
                        ))
                    })?;
                idxs.push(idx);
            }
            eq_col_indices.push(idxs);
        }

        let reader = builder
            .build()
            .map_err(|e| StorageError::Decode(format!("parquet reader: {e}")))?;
        let batches: Vec<arrow::record_batch::RecordBatch> = reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| StorageError::Decode(format!("parquet read: {e}")))?;

        let column_names = projection.iter().map(|s| s.to_string()).collect();
        Ok(Self {
            num_rows_in_current: 0,
            batches: batches.into_iter(),
            current: None,
            row: 0,
            column_names,
            column_indices,
            projection_len,
            batch_start_abs: 0,
            deletes,
            eq_col_indices,
        })
    }

    /// Open with ALL columns of the file's own schema (no projection).
    pub fn open_all(io: &StorageIo, key: &str) -> Result<Self, StorageError> {
        let raw = io.get_bytes(key)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(raw))
            .map_err(|e| StorageError::Decode(format!("parquet open: {e}")))?;
        let names: Vec<String> = builder
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let indices: Vec<usize> = (0..names.len()).collect();
        let reader = builder
            .build()
            .map_err(|e| StorageError::Decode(format!("parquet reader: {e}")))?;
        let batches: Vec<arrow::record_batch::RecordBatch> = reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| StorageError::Decode(format!("parquet read: {e}")))?;
        Ok(Self {
            num_rows_in_current: 0,
            batches: batches.into_iter(),
            current: None,
            row: 0,
            column_names: names,
            projection_len: indices.len(),
            column_indices: indices,
            batch_start_abs: 0,
            deletes: RowDeletes::default(),
            eq_col_indices: Vec::new(),
        })
    }

    pub fn column_names(&self) -> &[String] {
        &self.column_names
    }

    /// Advance to the next row across batch boundaries; false when done.
    /// After a `true` return, `row` points at the current row (first call
    /// after open() selects row 0). Named `next` deliberately: the FFI and
    /// FDW drive it imperatively; implementing std::iter::Iterator would
    /// borrow-split `value()` against the mutable cursor.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> bool {
        loop {
            if self.current.is_some() && self.row + 1 < self.num_rows_in_current {
                self.row += 1;
                if self.current_row_deleted() {
                    continue;
                }
                return true;
            }
            match self.batches.next() {
                Some(batch) => {
                    // finish the previous batch's contribution to the file
                    // absolute positions (0 on the first load)
                    self.batch_start_abs += self.num_rows_in_current as u64;
                    self.num_rows_in_current = batch.num_rows();
                    self.current = Some(batch);
                    self.row = 0;
                    if self.num_rows_in_current == 0 {
                        continue;
                    }
                    if self.current_row_deleted() {
                        continue;
                    }
                    return true;
                }
                None => return false,
            }
        }
    }

    /// Is the currently-selected row killed by a delete (slice 20)?
    /// Position deletes match the file-absolute position; equality deletes
    /// match when every listed column equals the delete tuple.
    fn current_row_deleted(&self) -> bool {
        self.row_deleted_at(self.row)
    }

    /// Same check, for an arbitrary row of `current` (the batch path
    /// computes the whole live list up front — M3-R1).
    fn row_deleted_at(&self, row: usize) -> bool {
        let abs = self.batch_start_abs + row as u64;
        if self.deletes.positions.contains(&abs) {
            return true;
        }
        for (eq_idx, eq) in self.deletes.equality.iter().enumerate() {
            let mut all = true;
            for (c_i, want) in eq.values.iter().enumerate() {
                let batch = self.current.as_ref().expect("current row");
                let arr_idx = self.eq_col_indices[eq_idx][c_i];
                let col = batch.column(arr_idx);
                use arrow::array::Array as _;
                let got = if col.is_null(row) {
                    CellValue::Null
                } else {
                    cell_from_arrow(col, row)
                };
                if got != *want {
                    all = false;
                    break;
                }
            }
            if all {
                return true;
            }
        }
        false
    }

    /// Materialize the next arrow batch with live rows as a typed view
    /// (M3-R1, 13-review perf #3): one pass per column instead of one
    /// CString per cell. Skips batches whose every row is dead. The views
    /// borrow `current`'s buffers — valid until the following
    /// `next_batch`/`next` call or drop. Deletes/DVs are already applied
    /// via `live_idx`; the C consumer never sees dead rows.
    pub fn next_batch(&mut self) -> Result<Option<BatchView>, StorageError> {
        use arrow::array::Array as _;
        use arrow::datatypes::DataType as DT;
        loop {
            let Some(batch) = self.batches.next() else {
                // keep the shared `current` cursor semantics: after the
                // last batch, both cursors are exhausted
                return Ok(None);
            };
            self.batch_start_abs += self.num_rows_in_current as u64;
            self.num_rows_in_current = batch.num_rows();
            self.row = 0;
            self.current = Some(batch);
            if self.num_rows_in_current == 0 {
                continue;
            }
            // live list: deletes + DVs applied here, once per batch
            let mut live_idx = Vec::with_capacity(self.num_rows_in_current);
            for r in 0..self.num_rows_in_current {
                if !self.row_deleted_at(r) {
                    live_idx.push(r as u32);
                }
            }
            if live_idx.is_empty() {
                continue;
            }
            let batch = self.current.as_ref().expect("just set");
            let mut cols = Vec::with_capacity(self.projection_len);
            for c in 0..self.projection_len {
                let arr_idx = self.column_indices[c];
                let col = batch.column(arr_idx);
                let mut nulls = Vec::with_capacity(self.num_rows_in_current);
                for r in 0..self.num_rows_in_current {
                    nulls.push(col.is_null(r) as u8);
                }
                let mut view = BatchColumnView {
                    kind: BatchKind::Rendered,
                    bools: Vec::new(),
                    i64s: Vec::new(),
                    f64s: Vec::new(),
                    spans: Vec::new(),
                    rendered: Vec::new(),
                    nulls,
                };
                match col.data_type() {
                    DT::Boolean => {
                        view.kind = BatchKind::Bool;
                        let a = col
                            .as_any()
                            .downcast_ref::<arrow::array::BooleanArray>()
                            .expect("boolean array");
                        for r in 0..self.num_rows_in_current {
                            view.bools.push(a.value(r) as u8);
                        }
                    }
                    DT::Int32 | DT::Int64 | DT::Date64 => {
                        view.kind = BatchKind::Int64;
                        for r in 0..self.num_rows_in_current {
                            let v = match col.data_type() {
                                DT::Int32 => col
                                    .as_any()
                                    .downcast_ref::<arrow::array::Int32Array>()
                                    .expect("int32 array")
                                    .value(r) as i64,
                                _ => col
                                    .as_any()
                                    .downcast_ref::<arrow::array::Int64Array>()
                                    .expect("int64 array")
                                    .value(r),
                            };
                            view.i64s.push(v);
                        }
                    }
                    DT::Float32 | DT::Float64 => {
                        view.kind = BatchKind::Float64;
                        for r in 0..self.num_rows_in_current {
                            let v = match col.data_type() {
                                DT::Float32 => col
                                    .as_any()
                                    .downcast_ref::<arrow::array::Float32Array>()
                                    .expect("float32 array")
                                    .value(r) as f64,
                                _ => col
                                    .as_any()
                                    .downcast_ref::<arrow::array::Float64Array>()
                                    .expect("float64 array")
                                    .value(r),
                            };
                            view.f64s.push(v);
                        }
                    }
                    DT::Timestamp(unit, _) => {
                        view.kind = BatchKind::TimestampUs;
                        // exactly cell_from_arrow's per-unit rules (doc 05 §4);
                        // temporal arrays are their own primitive types
                        use arrow::array::{
                            TimestampMicrosecondArray, TimestampMillisecondArray,
                            TimestampNanosecondArray, TimestampSecondArray,
                        };
                        {
                            let scaled: Vec<i64> = match unit {
                                arrow::datatypes::TimeUnit::Second => {
                                    let a = col
                                        .as_any()
                                        .downcast_ref::<TimestampSecondArray>()
                                        .expect("timestamp array");
                                    (0..a.len()).map(|r| a.value(r) * 1_000_000).collect()
                                }
                                arrow::datatypes::TimeUnit::Millisecond => {
                                    let a = col
                                        .as_any()
                                        .downcast_ref::<TimestampMillisecondArray>()
                                        .expect("timestamp array");
                                    (0..a.len()).map(|r| a.value(r) * 1_000).collect()
                                }
                                arrow::datatypes::TimeUnit::Microsecond => {
                                    let a = col
                                        .as_any()
                                        .downcast_ref::<TimestampMicrosecondArray>()
                                        .expect("timestamp array");
                                    (0..a.len()).map(|r| a.value(r)).collect()
                                }
                                arrow::datatypes::TimeUnit::Nanosecond => {
                                    let a = col
                                        .as_any()
                                        .downcast_ref::<TimestampNanosecondArray>()
                                        .expect("timestamp array");
                                    (0..a.len()).map(|r| a.value(r) / 1_000).collect()
                                }
                            };
                            view.i64s = scaled;
                        }
                    }
                    DT::Date32 => {
                        view.kind = BatchKind::Date32;
                        let a = col
                            .as_any()
                            .downcast_ref::<arrow::array::Date32Array>()
                            .expect("date32 array");
                        for r in 0..self.num_rows_in_current {
                            view.i64s.push(a.value(r) as i64);
                        }
                    }
                    DT::Utf8 | DT::LargeUtf8 => {
                        view.kind = BatchKind::Text;
                        for r in 0..self.num_rows_in_current {
                            let s = if col.is_null(r) {
                                ""
                            } else {
                                match col.data_type() {
                                    DT::LargeUtf8 => col
                                        .as_any()
                                        .downcast_ref::<arrow::array::LargeStringArray>()
                                        .expect("large utf8")
                                        .value(r),
                                    _ => col
                                        .as_any()
                                        .downcast_ref::<arrow::array::StringArray>()
                                        .expect("utf8")
                                        .value(r),
                                }
                            };
                            if s.contains('\0') {
                                return Err(StorageError::Decode(format!(
                                    "column {:?} value contains an interior NUL byte and \
                                     cannot be returned as text",
                                    self.column_names.get(c).cloned().unwrap_or_default()
                                )));
                            }
                            view.rendered.push(s.to_string());
                        }
                        view.spans = view
                            .rendered
                            .iter()
                            .map(|s| BatchSpan {
                                ptr: s.as_ptr(),
                                len: s.len() as u64,
                            })
                            .collect();
                    }
                    DT::Binary | DT::LargeBinary => {
                        view.kind = BatchKind::Bytes;
                        for r in 0..self.num_rows_in_current {
                            let b: &[u8] = if col.is_null(r) {
                                &[]
                            } else {
                                match col.data_type() {
                                    DT::LargeBinary => col
                                        .as_any()
                                        .downcast_ref::<arrow::array::LargeBinaryArray>()
                                        .expect("large binary")
                                        .value(r),
                                    _ => col
                                        .as_any()
                                        .downcast_ref::<arrow::array::BinaryArray>()
                                        .expect("binary")
                                        .value(r),
                                }
                            };
                            view.spans.push(BatchSpan {
                                ptr: b.as_ptr(),
                                len: b.len() as u64,
                            });
                        }
                    }
                    // date/time/decimal/uuid/fixed-binary: the same
                    // cell_from_arrow rendering as the per-row path, once
                    // per batch instead of once per row
                    _ => {
                        view.kind = BatchKind::Rendered;
                        for r in 0..self.num_rows_in_current {
                            let s = match cell_from_arrow(col, r) {
                                CellValue::Str(s) => s,
                                CellValue::Null => String::new(),
                                other => {
                                    return Err(StorageError::Decode(format!(
                                        "batch render: unexpected cell {:?} for column {:?}",
                                        other,
                                        self.column_names.get(c).cloned().unwrap_or_default()
                                    )))
                                }
                            };
                            view.rendered.push(s);
                        }
                        view.spans = view
                            .rendered
                            .iter()
                            .map(|s| BatchSpan {
                                ptr: s.as_ptr(),
                                len: s.len() as u64,
                            })
                            .collect();
                    }
                }
                cols.push(view);
            }
            return Ok(Some(BatchView {
                rows: self.num_rows_in_current,
                live_idx,
                start_abs: self.batch_start_abs,
                cols,
            }));
        }
    }

    /// Cell `col_idx` of the current row. Out-of-range column or a call
    /// before next() returns Null (the FDW drives next() first).
    pub fn value(&self, col_idx: usize) -> CellValue {
        let Some(batch) = &self.current else {
            return CellValue::Null;
        };
        let Some(arr_idx) = self.column_indices.get(col_idx) else {
            return CellValue::Null;
        };
        if self.row >= self.num_rows_in_current {
            return CellValue::Null;
        }
        let col = batch.column(*arr_idx);
        use arrow::array::Array as _;
        if col.is_null(self.row) {
            return CellValue::Null;
        }
        let row = self.row;
        cell_from_arrow(col, row)
    }
}

/// The Arrow types this reader converts WITHOUT loss or fabrication
/// (13-review #8). Open-time validation rejects anything else loudly.
fn is_supported_arrow_type(ty: &arrow::datatypes::DataType) -> bool {
    use arrow::datatypes::DataType as DT;
    match ty {
        DT::Boolean
        | DT::Int32
        | DT::Int64
        | DT::Date64
        | DT::Date32
        | DT::Time32(_)
        | DT::Time64(_)
        | DT::Timestamp(_, _)
        | DT::Float32
        | DT::Float64
        | DT::Utf8
        | DT::LargeUtf8
        | DT::Binary
        | DT::LargeBinary
        | DT::Decimal128(_, _)
        | DT::FixedSizeBinary(_) => true,
        // nested types land with the type-matrix slice (doc 05)
        _ => false,
    }
}

fn cell_from_arrow(col: &dyn arrow::array::Array, row: usize) -> CellValue {
    use arrow::array::*;
    use arrow::datatypes::DataType as DT;
    let down = |a: &dyn arrow::array::Array| {
        a.as_any()
            .downcast_ref::<arrow::array::PrimitiveArray<arrow::datatypes::Int32Type>>()
            .map(|a| a.value(row))
    };
    match col.data_type() {
        DT::Boolean => col
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| CellValue::Bool(a.value(row)))
            .unwrap_or(CellValue::Null),
        DT::Int32 => down(col)
            .map(|v| CellValue::Int64(v as i64))
            .unwrap_or(CellValue::Null),
        DT::Int64 | DT::Date64 => col
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| CellValue::Int64(a.value(row)))
            .unwrap_or(CellValue::Null),
        // temporal arrays are their own primitive types (timestamp columns
        // caught live with PyIceberg-written fixtures, slice 12)
        // date/time -> PostgreSQL-input-compatible TEXT (slice 21)
        DT::Date32 => col
            .as_any()
            .downcast_ref::<Date32Array>()
            .map(|a| {
                let (y, m, d) = civil_from_days(a.value(row) as i64);
                CellValue::Str(format!("{y:04}-{m:02}-{d:02}"))
            })
            .unwrap_or(CellValue::Null),
        DT::Time32(unit) => match unit {
            arrow::datatypes::TimeUnit::Second => col
                .as_any()
                .downcast_ref::<Time32SecondArray>()
                .map(|a| CellValue::Str(format_time(a.value(row) as i64, 6)))
                .unwrap_or(CellValue::Null),
            arrow::datatypes::TimeUnit::Millisecond => col
                .as_any()
                .downcast_ref::<Time32MillisecondArray>()
                .map(|a| CellValue::Str(format_time(a.value(row) as i64 * 1_000, 3)))
                .unwrap_or(CellValue::Null),
            _ => CellValue::Null,
        },
        DT::Time64(unit) => match *unit {
            arrow::datatypes::TimeUnit::Microsecond => col
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .map(|a| CellValue::Str(format_time(a.value(row), 6)))
                .unwrap_or(CellValue::Null),
            arrow::datatypes::TimeUnit::Nanosecond => col
                .as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .map(|a| CellValue::Str(format_time(a.value(row) / 1_000, 6)))
                .unwrap_or(CellValue::Null),
            // Time64 cannot hold second/millisecond precision; kept
            // unreachable for enum exhaustiveness
            _ => CellValue::Null,
        },
        DT::Timestamp(unit, _) => match *unit {
            arrow::datatypes::TimeUnit::Second => col
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .map(|a| CellValue::TimestampMicros(a.value(row) * 1_000_000))
                .unwrap_or(CellValue::Null),
            arrow::datatypes::TimeUnit::Millisecond => col
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .map(|a| CellValue::TimestampMicros(a.value(row) * 1_000))
                .unwrap_or(CellValue::Null),
            arrow::datatypes::TimeUnit::Microsecond => col
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .map(|a| CellValue::TimestampMicros(a.value(row)))
                .unwrap_or(CellValue::Null),
            // nanosecond sources truncate to micros (doc 05 §4)
            arrow::datatypes::TimeUnit::Nanosecond => col
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .map(|a| CellValue::TimestampMicros(a.value(row) / 1_000))
                .unwrap_or(CellValue::Null),
        },
        DT::Float32 => col
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|a| CellValue::Float64(a.value(row) as f64))
            .unwrap_or(CellValue::Null),
        DT::Float64 => col
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| CellValue::Float64(a.value(row)))
            .unwrap_or(CellValue::Null),
        // LargeUtf8 has its OWN array type — downcasting it to StringArray
        // failed silently and surfaced SQL NULL (13-review #8)
        DT::Utf8 => col
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| CellValue::Str(a.value(row).to_string()))
            .unwrap_or(CellValue::Null),
        DT::LargeUtf8 => col
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(|a| CellValue::Str(a.value(row).to_string()))
            .unwrap_or(CellValue::Null),
        // binary -> bytea hex text ('\x…', bytea_in-compatible; slice 21)
        DT::Binary => col
            .as_any()
            .downcast_ref::<BinaryArray>()
            .map(|a| {
                let b = a.value(row);
                let mut hex = String::with_capacity(2 + b.len() * 2);
                hex.push_str("\\x");
                for x in b {
                    hex.push_str(&format!("{x:02x}"));
                }
                CellValue::Str(hex)
            })
            .unwrap_or(CellValue::Null),
        // LargeBinary likewise has its own array type (13-review #8)
        DT::LargeBinary => col
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .map(|a| {
                let b = a.value(row);
                let mut hex = String::with_capacity(2 + b.len() * 2);
                hex.push_str("\\x");
                for x in b {
                    hex.push_str(&format!("{x:02x}"));
                }
                CellValue::Str(hex)
            })
            .unwrap_or(CellValue::Null),
        // decimal -> canonical unscaled/scale TEXT (numeric_in-compatible;
        // slice 21 conformance gate)
        DT::Decimal128(_, scale) => col
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .map(|a| CellValue::Str(format_decimal128(a.value(row), *scale)))
            .unwrap_or(CellValue::Null),
        // 16-byte fixed binary is the parquet shape of Iceberg `uuid`
        // (canonical hyphenated text for uuid_in; `fixed(16)` columns are
        // rare and render the same — noted in HANDOFF). Other sizes are
        // plain `fixed[N]` bytes.
        DT::FixedSizeBinary(16) => col
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .map(|a| CellValue::Str(format_uuid16(a.value(row))))
            .unwrap_or(CellValue::Null),
        DT::FixedSizeBinary(_) => col
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .map(|a| CellValue::Bytes(a.value(row).to_vec()))
            .unwrap_or(CellValue::Null),
        // nested types land with the type-matrix slice (doc 05)
        _ => CellValue::Null,
    }
}

/// Canonical decimal TEXT from an unscaled i128 and scale (trailing scale
/// digits always shown: 12345/2 -> "123.45", -500/2 -> "-5.00").
fn format_decimal128(v: i128, scale: i8) -> String {
    let neg = v < 0;
    let mut digits = v.unsigned_abs().to_string();
    let scale = scale.max(0) as usize;
    let body = if scale == 0 {
        digits
    } else {
        while digits.len() <= scale {
            digits.insert(0, '0');
        }
        let split = digits.len() - scale;
        format!("{}.{}", &digits[..split], &digits[split..])
    };
    if neg {
        format!("-{body}")
    } else {
        body
    }
}

/// `HH:MM:SS.<f-width>` from a time value in microseconds.
fn format_time(us: i64, fwidth: usize) -> String {
    let h = us / 3_600_000_000;
    let mi = (us / 60_000_000) % 60;
    let s = (us / 1_000_000) % 60;
    let f = us % 1_000_000;
    format!("{h:02}:{mi:02}:{s:02}.{:0fwidth$}", f, fwidth = fwidth)
}

/// Canonical lowercase hyphenated UUID from 16 big-endian bytes.
fn format_uuid16(b: &[u8]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-\
         {:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0],
        b[1],
        b[2],
        b[3],
        b[4],
        b[5],
        b[6],
        b[7],
        b[8],
        b[9],
        b[10],
        b[11],
        b[12],
        b[13],
        b[14],
        b[15]
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{parse_location, StorageIo};
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    /// Write a 3-row parquet fixture (id long, name string with a NULL).
    fn write_fixture(dir: &std::path::Path, name: &str) {
        let schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new(
                "at",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                true,
            ),
        ]);
        let at_array = arrow::array::TimestampMicrosecondArray::from(vec![
            Some(1_700_000_000_000_000i64),
            None,
            Some(1_700_000_100_000_000i64),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("alpha"), None, Some("gamma")])),
                Arc::new(at_array),
            ],
        )
        .unwrap();
        let file = std::fs::File::create(dir.join(name)).unwrap();
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    /// TDD anchor (M1 slice 10): projected rows over object storage with
    /// NULL handling — the executor's per-task read path.
    #[test]
    fn reads_projected_rows_with_nulls() {
        let dir = tempfile::tempdir().unwrap();
        crate::storage::test_allow_local_stores();
        write_fixture(dir.path(), "a.parquet");
        let io = StorageIo::open(
            &parse_location(&format!("file://{}", dir.path().display())).unwrap(),
            None,
        )
        .unwrap();

        let mut rows = ParquetRows::open(&io, "a.parquet", &["id", "name", "at"]).unwrap();
        assert_eq!(rows.column_names(), ["id", "name", "at"]);

        assert!(rows.next(), "row 0");
        assert_eq!(rows.value(0), CellValue::Int64(1));
        assert_eq!(rows.value(1), CellValue::Str("alpha".into()));
        assert_eq!(
            rows.value(2),
            CellValue::TimestampMicros(1_700_000_000_000_000),
            "timestamp micros decode (slice 12 regression)"
        );

        assert!(rows.next(), "row 1");
        assert_eq!(rows.value(0), CellValue::Int64(2));
        assert_eq!(rows.value(1), CellValue::Null);
        assert_eq!(rows.value(2), CellValue::Null);

        assert!(rows.next(), "row 2");
        assert_eq!(rows.value(0), CellValue::Int64(3));
        assert_eq!(rows.value(1), CellValue::Str("gamma".into()));
        assert_eq!(
            rows.value(2),
            CellValue::TimestampMicros(1_700_000_100_000_000)
        );
        assert_eq!(
            format_timestamp_micros(1_700_000_000_000_000),
            "2023-11-14 22:13:20.000000+00"
        );

        assert!(!rows.next(), "exhausted");
    }

    /// Projection: only the named columns come back, in the requested order.
    #[test]
    fn projection_selects_and_orders_columns() {
        let dir = tempfile::tempdir().unwrap();
        crate::storage::test_allow_local_stores();
        write_fixture(dir.path(), "a.parquet");
        let io = StorageIo::open(
            &parse_location(&format!("file://{}", dir.path().display())).unwrap(),
            None,
        )
        .unwrap();

        let mut rows = ParquetRows::open(&io, "a.parquet", &["name", "id"]).unwrap();
        assert_eq!(rows.column_names(), ["name", "id"]);
        assert!(rows.next());
        assert_eq!(rows.value(0), CellValue::Str("alpha".into()));
        assert_eq!(rows.value(1), CellValue::Int64(1));
    }

    /// Unknown projection columns are a clean config error (doc 03 §5:
    /// schema drift surfaces with a diff, not a crash).
    #[test]
    fn unknown_projection_column_errors() {
        let dir = tempfile::tempdir().unwrap();
        crate::storage::test_allow_local_stores();
        write_fixture(dir.path(), "a.parquet");
        let io = StorageIo::open(
            &parse_location(&format!("file://{}", dir.path().display())).unwrap(),
            None,
        )
        .unwrap();
        let err = ParquetRows::open(&io, "a.parquet", &["nope"]).unwrap_err();
        assert!(matches!(err, StorageError::Config(_)), "{err}");
    }

    /// Write an N-row single-column (id long) parquet fixture; returns the
    /// values written. Slice 20 delete tests.
    fn write_ids(dir: &std::path::Path, name: &str, ids: &[i64]) -> String {
        let schema = ArrowSchema::new(vec![Field::new("id", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int64Array::from(ids.to_vec()))],
        )
        .unwrap();
        let file = std::fs::File::create(dir.join(name)).unwrap();
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        ids.to_vec()
            .iter()
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(",")
    }

    /// TDD anchor (M1 slice 20): position deletes skip exact absolute row
    /// positions (v2 (file_path, pos) delete files).
    #[test]
    fn position_deletes_skip_rows() {
        let dir = tempfile::tempdir().unwrap();
        crate::storage::test_allow_local_stores();
        write_ids(dir.path(), "a.parquet", &[1, 2, 3, 4, 5]);
        let io = StorageIo::open(
            &parse_location(&format!("file://{}", dir.path().display())).unwrap(),
            None,
        )
        .unwrap();

        let mut deletes = RowDeletes::default();
        deletes.positions.insert(1); // id=2
        deletes.positions.insert(4); // id=5

        let mut rows = ParquetRows::open_with_deletes(&io, "a.parquet", &["id"], deletes).unwrap();
        let mut seen = Vec::new();
        while rows.next() {
            match rows.value(0) {
                CellValue::Int64(v) => seen.push(v),
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(seen, vec![1, 3, 4]);
    }

    /// TDD anchor (M1 slice 20): equality deletes drop rows matching any
    /// delete tuple on the listed columns; the eq column may be outside the
    /// projection (it is read internally, not exposed).
    #[test]
    fn equality_deletes_drop_matching_rows() {
        let dir = tempfile::tempdir().unwrap();
        crate::storage::test_allow_local_stores();
        // data: id 1..=5 with a name column
        let schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
            ],
        )
        .unwrap();
        let path = dir.path().join("data.parquet");
        let f = std::fs::File::create(&path).unwrap();
        let mut writer = parquet::arrow::ArrowWriter::try_new(f, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let io = StorageIo::open(
            &parse_location(&format!("file://{}", dir.path().display())).unwrap(),
            None,
        )
        .unwrap();

        // eq delete: any row with id == 3 dies
        let mut deletes = RowDeletes::default();
        deletes.equality.push(EqDeleteMatch {
            columns: vec!["id".into()],
            values: vec![CellValue::Int64(3)],
            sequence_number: 1,
        });

        // project name only: the eq column (id) is read internally
        let mut rows =
            ParquetRows::open_with_deletes(&io, "data.parquet", &["name"], deletes).unwrap();
        let mut seen = Vec::new();
        while rows.next() {
            match rows.value(0) {
                CellValue::Str(v) => seen.push(v),
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(seen, vec!["a", "b", "d", "e"]);
    }

    /// TDD anchor (M1 slice 21, conformance gate): decimal128 columns
    /// decode to their canonical decimal TEXT (numeric_in-compatible) —
    /// previously Null.
    #[test]
    fn decodes_decimal128_to_canonical_text() {
        use arrow::array::Decimal128Array;

        let dir = tempfile::tempdir().unwrap();
        crate::storage::test_allow_local_stores();
        let schema = ArrowSchema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(12, 2),
            true,
        )]);
        let arr: Decimal128Array = vec![Some(12345i128), Some(-500i128), None]
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(12, 2)
            .unwrap();
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arr)]).unwrap();
        let f = std::fs::File::create(dir.path().join("d.parquet")).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, batch.schema(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let io = StorageIo::open(
            &parse_location(&format!("file://{}", dir.path().display())).unwrap(),
            None,
        )
        .unwrap();

        let mut rows = ParquetRows::open(&io, "d.parquet", &["amount"]).unwrap();
        assert!(rows.next());
        assert_eq!(rows.value(0), CellValue::Str("123.45".into()));
        assert!(rows.next());
        assert_eq!(rows.value(0), CellValue::Str("-5.00".into()));
        assert!(rows.next());
        assert_eq!(rows.value(0), CellValue::Null);
        assert!(!rows.next());
    }

    /// TDD anchor (M1 slice 21, conformance gate): uuid columns (parquet
    /// FLBA(16) with the UUID logical type) decode to the canonical
    /// hyphenated lowercase form — previously Null.
    #[test]
    fn decodes_uuid_to_canonical_text() {
        use arrow::array::FixedSizeBinaryArray;

        let dir = tempfile::tempdir().unwrap();
        crate::storage::test_allow_local_stores();
        let schema = ArrowSchema::new(vec![Field::new("uid", DataType::FixedSizeBinary(16), true)]);
        let b = FixedSizeBinaryArray::from(vec![
            Some(
                {
                    let mut v = vec![0u8; 16];
                    v[6] = 0x12;
                    v
                }
                .as_slice(),
            ),
            None,
        ]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(b)]).unwrap();
        let f = std::fs::File::create(dir.path().join("u.parquet")).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, batch.schema(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let io = StorageIo::open(
            &parse_location(&format!("file://{}", dir.path().display())).unwrap(),
            None,
        )
        .unwrap();

        let mut rows = ParquetRows::open(&io, "u.parquet", &["uid"]).unwrap();
        assert!(rows.next());
        // 16 zero bytes with 0x12 at offset 6 -> group3 = 1200
        assert_eq!(
            rows.value(0),
            CellValue::Str("00000000-0000-1200-0000-000000000000".into())
        );
        assert!(rows.next());
        assert_eq!(rows.value(0), CellValue::Null);
        assert!(!rows.next());
    }

    /// TDD anchor (M1 slice 21, conformance gate): date, time, and binary
    /// cells decode to PostgreSQL-input-compatible TEXT — ISO dates,
    /// `HH:MM:SS.ffffff` times, `\x`-prefixed bytea hex (previously raw
    /// integers / bare hex that the type input functions reject).
    #[test]
    fn decodes_date_time_binary_to_input_compatible_text() {
        use arrow::array::{Date32Array, Time64MicrosecondArray};

        let dir = tempfile::tempdir().unwrap();
        crate::storage::test_allow_local_stores();
        let schema = ArrowSchema::new(vec![
            Field::new("d", DataType::Date32, true),
            Field::new(
                "t",
                DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
                true,
            ),
            Field::new("b", DataType::Binary, true),
        ]);
        // date 0 = 1970-01-01; 19723 days = 2024-01-01
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Date32Array::from(vec![0, 19723])),
                Arc::new(Time64MicrosecondArray::from(vec![
                    36_610_000_000i64, // 10:10:10.000000
                    0,
                ])),
                {
                    let mut b = arrow::array::BinaryBuilder::with_capacity(8, 16);
                    b.append_value([0xdeu8, 0xad, 0xbe, 0xefu8]);
                    b.append_null();
                    Arc::new(b.finish())
                },
            ],
        )
        .unwrap();
        let f = std::fs::File::create(dir.path().join("dtb.parquet")).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, batch.schema(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let io = StorageIo::open(
            &parse_location(&format!("file://{}", dir.path().display())).unwrap(),
            None,
        )
        .unwrap();

        let mut rows = ParquetRows::open(&io, "dtb.parquet", &["d", "t", "b"]).unwrap();
        assert!(rows.next());
        assert_eq!(rows.value(0), CellValue::Str("1970-01-01".into()));
        assert_eq!(rows.value(1), CellValue::Str("10:10:10.000000".into()));
        assert_eq!(rows.value(2), CellValue::Str("\\xdeadbeef".into()));
        assert!(rows.next());
        assert_eq!(rows.value(0), CellValue::Str("2024-01-01".into()));
        assert_eq!(rows.value(1), CellValue::Str("00:00:00.000000".into()));
        assert_eq!(rows.value(2), CellValue::Null);
        assert!(!rows.next());
    }
}

#[cfg(test)]
mod pos_delete_applicability_tests {
    use super::*;

    fn loaded_with_seq(seq: i64, positions: &[u64]) -> LoadedDeletes {
        let mut ld = LoadedDeletes::default();
        let e = ld
            .positions_by_file
            .entry("file:///t/data/f.parquet".into())
            .or_default()
            .entry(seq)
            .or_default();
        for p in positions {
            e.insert(*p);
        }
        ld
    }

    /// ACCEPTANCE (13-review blocker #5, position-delete applicability):
    /// a position delete with sequence 5 applies to data files with
    /// sequence <= 5, and must NOT touch a data file whose sequence is
    /// higher (rows written after the delete was issued). The old
    /// path/position-only map applied unconditionally.
    #[test]
    fn position_delete_sequence_applicability() {
        let ld = loaded_with_seq(5, &[0, 2]);
        // data file seq 3 <= 5: applies
        let rd = ld.for_task("file:///t/data/f.parquet", 3, false).unwrap();
        assert!(rd.positions.contains(&0) && rd.positions.contains(&2));
        // data file seq 7 > 5: written after the delete -> NOT applied
        let rd = ld.for_task("file:///t/data/f.parquet", 7, false).unwrap();
        assert!(rd.positions.is_empty());
    }

    /// Empty-projection scans must still apply position deletes: the old
    /// open_all branch in rows_open silently counted deleted rows. This
    /// exercises open_with_deletes with an empty projection: next()
    /// skips deleted absolute rows even though no columns are read.
    #[test]
    fn empty_projection_still_applies_positions() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field as AF, Schema as ArrowSchema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        crate::storage::test_allow_local_stores();
        let aschema = ArrowSchema::new(vec![AF::new("id", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(aschema),
            vec![Arc::new(Int64Array::from(vec![10, 11, 12]))],
        )
        .unwrap();
        let f = std::fs::File::create(dir.path().join("f.parquet")).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, batch.schema(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();

        let mut ld = LoadedDeletes::default();
        let e = ld
            .positions_by_file
            .entry(format!("file://{}/f.parquet", dir.path().display()))
            .or_default()
            .entry(9)
            .or_default();
        e.insert(1); // row 11 is deleted

        let key = "f.parquet".to_string();
        let io = StorageIo::open(
            &crate::storage::parse_location(&format!("file://{}", dir.path().display())).unwrap(),
            None,
        )
        .unwrap();
        let rd = ld
            .for_task(
                &format!("file://{}/f.parquet", dir.path().display()),
                9,
                false,
            )
            .unwrap();
        let rows = ParquetRows::open_with_deletes(&io, &key, &[], rd).unwrap();
        let mut seen = 0;
        let mut r = rows;
        while r.next() {
            seen += 1;
        }
        assert_eq!(seen, 2, "deleted row 11 skipped even with empty projection");
    }
}
