//! Parquet row reads over object storage (specs/lakehouse/06-execution.md
//! §3). Slice 10: projected row iteration with a simple typed cell model —
//! Int32/Int64 normalize to Int64, Float32/Float64 to Float64; nested types
//! (struct/list/map) decode in the type-matrix slice (doc 05). Iceberg type
//! -> Postgres type mapping happens on the C/FDW side.

use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use parquet::errors::ParquetError;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::{ChunkReader, Length};
use std::io::{self, Read as StdRead};
use std::ops::Range;
use std::sync::{Arc, Mutex, OnceLock};

use crate::cache::BlockCache;
use crate::storage::{StorageError, StorageIo};

// A stream fetches one window at a time. It never speculates past the
// projected column chunk. Exact page-body reads reuse any overlapping bytes
// and fetch just the missing prefix or suffix.
const PARQUET_READ_AHEAD: u64 = 8 * 1024;

/// Parquet footer probes and page ranges use the same authorized storage
/// handle and immutable-object cache. The additional read window is local
/// to this reader and its page-header streams.
#[derive(Clone)]
struct RangedReader {
    io: StorageIo,
    cache: &'static BlockCache,
    key: String,
    len: u64,
    ahead: Arc<ReadAhead>,
}

#[derive(Default)]
struct ReadAhead {
    projected_chunks: OnceLock<Vec<Range<u64>>>,
    last: Mutex<Option<ReadWindow>>,
}

struct ReadWindow {
    start: u64,
    bytes: Bytes,
}

impl ReadWindow {
    fn intersection(&self, range: Range<u64>) -> Option<(u64, Bytes)> {
        let start = self.start.max(range.start);
        let end = (self.start + self.bytes.len() as u64).min(range.end);
        (start < end).then(|| {
            (
                start,
                self.bytes
                    .slice((start - self.start) as usize..(end - self.start) as usize),
            )
        })
    }
}

impl RangedReader {
    fn new(io: &StorageIo, cache: &'static BlockCache, key: &str, len: u64) -> Self {
        Self {
            io: io.clone(),
            cache,
            key: key.to_owned(),
            len,
            ahead: Arc::default(),
        }
    }

    fn fetch(&self, range: Range<u64>) -> io::Result<Vec<u8>> {
        let len = range.end - range.start;
        let bytes = self
            .cache
            .get_range(&self.io, "", &self.key, range.start, len)
            .map_err(io::Error::other)?;
        if bytes.len() as u64 != len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "short Parquet object range",
            ));
        }
        Ok(bytes)
    }

    fn fill_window(&self, range: Range<u64>) -> io::Result<Bytes> {
        let mut last = self.ahead.last.lock().unwrap();
        if let Some((start, overlap)) = last
            .as_ref()
            .and_then(|window| window.intersection(range.clone()))
        {
            return if start == range.start {
                Ok(overlap)
            } else {
                self.fetch(range.start..start).map(Bytes::from)
            };
        }
        let start = range.start;
        let bytes = Bytes::from(self.fetch(range)?);
        *last = Some(ReadWindow {
            start,
            bytes: bytes.clone(),
        });
        Ok(bytes)
    }

    fn chunk_end(&self, start: u64) -> Option<u64> {
        let chunks = self.ahead.projected_chunks.get()?;
        let index = chunks.partition_point(|chunk| chunk.start <= start);
        index
            .checked_sub(1)
            .map(|i| chunks[i].end)
            .filter(|end| start < *end)
    }
}

/// Use column chunk extents from the existing footer. Keep exact reads if
/// the extents cannot prove that a speculative read stays in one chunk.
fn projected_chunks(
    metadata: &ParquetMetaData,
    projection: &ProjectionMask,
    file_len: u64,
) -> Vec<Range<u64>> {
    let mut chunks = Vec::new();
    for group in metadata.row_groups() {
        for (index, column) in group.columns().iter().enumerate() {
            if column.file_path().is_some() {
                return Vec::new();
            }
            let offset = column
                .dictionary_page_offset()
                .unwrap_or_else(|| column.data_page_offset());
            let (Ok(start), Ok(size)) = (
                u64::try_from(offset),
                u64::try_from(column.compressed_size()),
            ) else {
                return Vec::new();
            };
            let Some(end) = start.checked_add(size).filter(|end| *end <= file_len) else {
                return Vec::new();
            };
            if start < end {
                chunks.push((start..end, projection.leaf_included(index)));
            }
        }
    }
    chunks.sort_unstable_by_key(|(range, _)| range.start);
    if chunks
        .windows(2)
        .any(|pair| pair[0].0.end > pair[1].0.start)
    {
        return Vec::new();
    }
    chunks
        .into_iter()
        .filter_map(|(range, included)| included.then_some(range))
        .collect()
}

impl Length for RangedReader {
    fn len(&self) -> u64 {
        self.len
    }
}

impl ChunkReader for RangedReader {
    type T = RangedSeqRead;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        Ok(RangedSeqRead {
            reader: self.clone(),
            pos: start,
            readahead_end: self.chunk_end(start),
            buffer: Bytes::new(),
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        if length == 0 {
            return Ok(Bytes::new());
        }
        let end = start
            .checked_add(length as u64)
            .filter(|end| *end <= self.len)
            .ok_or_else(|| ParquetError::EOF("Parquet object range is outside the file".into()))?;
        let overlap = self
            .ahead
            .last
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|window| window.intersection(start..end));
        let fetch = |range| {
            self.fetch(range)
                .map_err(|e| ParquetError::External(Box::new(e)))
        };
        let Some((buffer_start, bytes)) = overlap else {
            return fetch(start..end).map(Bytes::from);
        };
        if bytes.len() == length {
            return Ok(bytes);
        }
        let buffer_end = buffer_start + bytes.len() as u64;
        let result = if buffer_start == start {
            // Forward reads grow the existing suffix allocation by at most
            // one window rather than allocating a second full page body.
            let mut result = fetch(buffer_end..end)?;
            let suffix_len = result.len();
            result.reserve_exact(bytes.len());
            result.resize(length, 0);
            result.copy_within(..suffix_len, bytes.len());
            result[..bytes.len()].copy_from_slice(&bytes);
            result
        } else if buffer_end == end {
            let mut result = fetch(start..buffer_start)?;
            result.reserve_exact(bytes.len());
            result.extend_from_slice(&bytes);
            result
        } else {
            // A strict interior overlap would require two allocations and
            // two GETs. One exact GET repeats at most the small window.
            fetch(start..end)?
        };
        Ok(Bytes::from(result))
    }
}

struct RangedSeqRead {
    reader: RangedReader,
    pos: u64,
    readahead_end: Option<u64>,
    buffer: Bytes,
}

impl StdRead for RangedSeqRead {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() || self.pos >= self.reader.len {
            return Ok(0);
        }
        if self.buffer.is_empty() {
            self.buffer = if let Some(end) = self.readahead_end.filter(|end| self.pos < *end) {
                let end = self.pos + PARQUET_READ_AHEAD.min(end - self.pos);
                self.reader.fill_window(self.pos..end)?
            } else {
                // The chunk boundary clips speculation only. A reader
                // remains a sequential view through the end of the file.
                let end = self.pos + (buf.len() as u64).min(self.reader.len - self.pos);
                Bytes::from(self.reader.fetch(self.pos..end)?)
            };
        }
        let count = buf.len().min(self.buffer.len());
        buf[..count].copy_from_slice(&self.buffer.split_to(count));
        self.pos += count as u64;
        Ok(count)
    }
}

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
#[must_use]
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
/// `cell_from_arrow`'s Date32 branch — the batch fallback renders through
/// this and the input function, exactly as the per-row path did).
#[must_use]
pub fn format_date32(days: i32) -> String {
    let (y, m, d) = civil_from_days(i64::from(days));
    format!("{y:04}-{m:02}-{d:02}")
}

/// Format an f64 exactly as the per-row renderer does (shortest repr).
#[must_use]
pub fn format_f64(v: f64) -> String {
    v.to_string()
}

/// Postgres `bytea` hex text form (`\x` followed by two hex digits per byte),
/// matching `byteaout` so the per-row renderer and the batch text path agree.
#[must_use]
pub fn hex_escape_bytes(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut hex = String::with_capacity(2 + bytes.len() * 2);
    hex.push_str("\\x");
    for x in bytes {
        let _ = write!(hex, "{x:02x}");
    }
    hex
}

/// Howard Hinnant's `civil_from_days` algorithm (days since 1970-01-01).
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
            .map_or_default(|by_seq| {
                let mut out = std::collections::BTreeSet::new();
                // v2 applicability: delete seq N applies to data files
                // with sequence <= N
                for (seq, set) in by_seq {
                    if *seq >= task_sequence_number {
                        out.extend(set.iter().copied());
                    }
                }
                out
            });
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
    /// indexes — the file-absolute position of `live_idx`[k] is
    /// `start_abs + live_idx[k]`)
    pub live_idx: Vec<u32>,
    /// file-absolute position of batch row 0 (position deletes are
    /// file-scoped, doc 07)
    pub start_abs: u64,
    /// one view per PROJECTED column (eq-delete internals excluded)
    pub cols: Vec<BatchColumnView>,
}

/// Three-valued row-group predicate truth over min/max statistics:
/// `Match` (some row could satisfy the conjunct), `NoMatch` (provably
/// empty), `Unknown` (stats missing, inexact, or type-incomparable —
/// keep).
#[derive(Debug, Clone, Copy, PartialEq)]
enum RgTruth {
    Match,
    NoMatch,
    Unknown,
}

/// Typed comparison value for row-group statistics.
#[derive(Debug)]
enum RgVal {
    I(i64),
    F(f64),
    S(Vec<u8>),
    B(bool),
}

/// Could ANY value in [min, max] satisfy `op: value`?
fn rg_interval_satisfies(op: &str, v: &RgVal, min: &RgVal, max: &RgVal) -> Option<bool> {
    use std::cmp::Ordering;
    // cross-kind compares widen Int to Float (exact for |i64| < 2^53,
    // i.e. all SF-scale data — the intended widening; no From impl exists
    // so the cast stays, clippy-allowed)
    #[allow(clippy::cast_precision_loss)]
    let ord = |a: &RgVal, b: &RgVal| -> Option<std::cmp::Ordering> {
        match (a, b) {
            (RgVal::I(x), RgVal::I(y)) => Some(x.cmp(y)),
            (RgVal::I(x), RgVal::F(y)) => (*x as f64).partial_cmp(y),
            (RgVal::F(x), RgVal::I(y)) => x.partial_cmp(&(*y as f64)),
            (RgVal::F(x), RgVal::F(y)) => x.partial_cmp(y),
            (RgVal::S(x), RgVal::S(y)) => Some(x.cmp(y)),
            (RgVal::B(x), RgVal::B(y)) => Some(x.cmp(y)),
            _ => None,
        }
    };
    match op {
        "eq" => {
            // v inside [min, max]
            Some(ord(min, v)? != Ordering::Greater && ord(v, max)? != Ordering::Greater)
        }
        "lt" => Some(ord(min, v)? == Ordering::Less),
        "le" => Some(ord(min, v)? != Ordering::Greater),
        "gt" => Some(ord(max, v)? == Ordering::Less),
        "ge" => Some(ord(v, max)? != Ordering::Greater),
        _ => None,
    }
}

fn rg_json_val(v: &serde_json::Value) -> Option<RgVal> {
    Some(match v {
        serde_json::Value::Bool(b) => RgVal::B(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                RgVal::I(i)
            } else {
                RgVal::F(n.as_f64()?)
            }
        }
        serde_json::Value::String(st) => RgVal::S(st.as_bytes().to_vec()),
        _ => return None,
    })
}

/// Leaf conjunct vs one row group's statistics (three-valued).
fn rg_leaf_truth(
    field: &str,
    op: &str,
    value: &serde_json::Value,
    rg: &parquet::file::metadata::RowGroupMetaData,
    names: &[&str],
) -> RgTruth {
    use parquet::file::statistics::Statistics;
    let Some(idx) = names.iter().position(|n| *n == field) else {
        return RgTruth::Unknown;
    };
    if idx >= rg.columns().len() {
        return RgTruth::Unknown;
    }
    let Some(st) = rg.column(idx).statistics() else {
        return RgTruth::Unknown;
    };
    let Some(v) = rg_json_val(value) else {
        return RgTruth::Unknown;
    };
    // parquet 60: min/max are Option (absent stats = Unknown); pruning
    // only on EXACT bounds (writer-emitted, not stale/truncated)
    macro_rules! exact_bounds {
        ($t:expr) => {
            match (
                $t.min_opt(),
                $t.max_opt(),
                $t.min_is_exact(),
                $t.max_is_exact(),
            ) {
                (Some(a), Some(b), true, true) => (a, b),
                _ => return RgTruth::Unknown,
            }
        };
    }
    let (min, max) = match st {
        Statistics::Boolean(t) => {
            let (a, b) = exact_bounds!(t);
            (RgVal::B(*a), RgVal::B(*b))
        }
        Statistics::Int32(t) => {
            let (a, b) = exact_bounds!(t);
            (RgVal::I(i64::from(*a)), RgVal::I(i64::from(*b)))
        }
        Statistics::Int64(t) => {
            let (a, b) = exact_bounds!(t);
            (RgVal::I(*a), RgVal::I(*b))
        }
        Statistics::Float(t) => {
            let (a, b) = exact_bounds!(t);
            (RgVal::F(f64::from(*a)), RgVal::F(f64::from(*b)))
        }
        Statistics::Double(t) => {
            let (a, b) = exact_bounds!(t);
            (RgVal::F(*a), RgVal::F(*b))
        }
        Statistics::ByteArray(t) => {
            let (a, b) = exact_bounds!(t);
            (RgVal::S(a.data().to_vec()), RgVal::S(b.data().to_vec()))
        }
        Statistics::FixedLenByteArray(t) => {
            let (a, b) = exact_bounds!(t);
            (RgVal::S(a.data().to_vec()), RgVal::S(b.data().to_vec()))
        }
        Statistics::Int96(_) => return RgTruth::Unknown,
    };
    match rg_interval_satisfies(op, &v, &min, &max) {
        Some(true) => RgTruth::Match,
        Some(false) => RgTruth::NoMatch,
        None => RgTruth::Unknown,
    }
}

/// AND-tree over leaf truths; OR (when added to the vocabulary) composes
/// by dual.
fn rg_pred_truth(
    pred: &crate::scan::ScanPredicate,
    rg: &parquet::file::metadata::RowGroupMetaData,
    names: &[&str],
) -> RgTruth {
    use crate::scan::ScanPredicate as P;
    match pred {
        P::And(v) => {
            let mut t = RgTruth::Match;
            for p in v {
                match rg_pred_truth(p, rg, names) {
                    RgTruth::NoMatch => return RgTruth::NoMatch,
                    RgTruth::Unknown => t = RgTruth::Unknown,
                    RgTruth::Match => {}
                }
            }
            t
        }
        P::Eq { field, value } => rg_leaf_truth(field, "eq", value, rg, names),
        P::Lt { field, value } => rg_leaf_truth(field, "lt", value, rg, names),
        P::Le { field, value } => rg_leaf_truth(field, "le", value, rg, names),
        P::Gt { field, value } => rg_leaf_truth(field, "gt", value, rg, names),
        P::Ge { field, value } => rg_leaf_truth(field, "ge", value, rg, names),
    }
}

/// Projected row iterator over one parquet object.
pub struct ParquetRows {
    /// P0b3: the batch source is LAZY for the scan path — row groups
    /// decode on demand as the consumer walks rows, so early termination
    /// (LIMIT, session abort) skips the remaining groups' decode work
    /// entirely. `open_all` (delete-file processing, small files) stores an
    /// eager iterator instead.
    batches: Box<dyn Iterator<Item = arrow::record_batch::RecordBatch> + Send>,
    current: Option<arrow::record_batch::RecordBatch>,
    row: usize,
    column_names: Vec<String>,
    column_indices: Vec<usize>,
    /// width of the caller's projection (`column_indices` may exceed it —
    /// eq-delete internal columns ride beyond this mark)
    projection_len: usize,
    /// 2a: row groups skipped by exact min/max stats at open (0 when the
    /// predicate kept everything, stats were absent, or the schema is not
    /// flat 1:1)
    pub(crate) row_groups_pruned: usize,
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

impl std::fmt::Debug for ParquetRows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetRows")
            .field("row", &self.row)
            .field("num_rows_in_current", &self.num_rows_in_current)
            .field("batch_start_abs", &self.batch_start_abs)
            .finish_non_exhaustive()
    }
}

impl ParquetRows {
    /// Open a parquet object with a projection (column names, in the
    /// requested order). Unknown columns are a Config error (doc 03 §5:
    /// schema drift surfaces cleanly).
    pub fn open(io: &StorageIo, key: &str, projection: &[&str]) -> Result<Self, StorageError> {
        Self::open_internal(
            io,
            key,
            projection,
            RowDeletes::default(),
            None,
            crate::cache::global(),
        )
    }

    /// Open with delete filters applied (slice 20, doc 07). Equality-delete
    /// columns outside the projection join the internal read set (never
    /// exposed); an eq column missing from the file is a Config error.
    pub fn open_with_deletes(
        io: &StorageIo,
        key: &str,
        projection: &[&str],
        deletes: RowDeletes,
        predicate: Option<&crate::scan::ScanPredicate>,
    ) -> Result<Self, StorageError> {
        Self::open_internal(
            io,
            key,
            projection,
            deletes,
            predicate,
            crate::cache::global(),
        )
    }

    fn open_internal(
        io: &StorageIo,
        key: &str,
        projection: &[&str],
        deletes: RowDeletes,
        predicate: Option<&crate::scan::ScanPredicate>,
        cache: &'static BlockCache,
    ) -> Result<Self, StorageError> {
        // P0b (spec 15 §6.3a): ranged, cached, projection-pruned open.
        // The builder reads ONLY the footer/metadata (two small ranged
        // GETs); column chunks are fetched per projected column when the
        // reader materializes — the whole-file get_bytes this used to do
        // fetched every column of every row group regardless.
        let file_len = io.stat_len(key)?;
        let ranged = RangedReader::new(io, cache, key, file_len);
        let builder = ParquetRecordBatchReaderBuilder::try_new(ranged.clone())
            .map_err(|e| StorageError::Decode(format!("parquet open: {e}")))?;
        let arrow_schema = builder.schema().clone();

        // internal read set: projection first, then any equality-delete
        // columns not already projected
        let mut read_names: Vec<String> = projection
            .iter()
            .map(std::string::ToString::to_string)
            .collect();
        for eq in &deletes.equality {
            for c in &eq.columns {
                if !read_names.contains(c) {
                    read_names.push(c.clone());
                }
            }
        }

        // arrow-schema positions of the internal read set, in read order —
        // used for BOTH type validation and the reader's ProjectionMask
        let mut read_field_indices = Vec::with_capacity(read_names.len());
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
            read_field_indices.push(idx);
        }
        let projection_len = projection.len();

        // 13-review #8: unsupported Arrow types must fail BEFORE rows are
        // returned — the per-cell fallthrough maps unknown types to SQL
        // NULL, which silently fabricates data. eq columns are validated
        // too: an unmatchable key type would make deletes never apply.
        for (i, name) in read_names.iter().enumerate() {
            let ty = arrow_schema.field(read_field_indices[i]).data_type();
            if !is_supported_arrow_type(ty) {
                return Err(StorageError::Config(format!(
                    "column {name:?} has unsupported type {ty} in this reader; \
                     unsupported columns must not surface as NULL"
                )));
            }
        }

        // P0b: push the read set INTO the reader — unprojected columns'
        // chunks are never fetched (the pre-P0b whole-file open decoded
        // every column regardless). Masked batches hold exactly
        // read_names in read order, so all column indices below are
        // positions WITHIN the read set.
        let proj_mask =
            ProjectionMask::roots(builder.parquet_schema(), read_field_indices.iter().copied());
        let chunks = projected_chunks(builder.metadata(), &proj_mask, file_len);
        let _ = ranged.ahead.projected_chunks.set(chunks);
        let builder = builder.with_projection(proj_mask);

        // 2a (spec 15 §6.3a / doc 06 §1 step 5): row-group stats pruning.
        // A row group is SKIPPED only when some conjunct PROVES emptiness
        // against EXACT min/max stats — everything else is kept, and PG
        // re-checks every surviving row locally anyway (never-trust law).
        // Flat 1:1 schemas only: stats are keyed by leaf column ordinal,
        // which for our flat tables equals the arrow field index.
        let mut row_groups_pruned = 0usize;
        let builder = if let Some(pred) = predicate {
            let md = builder.metadata();
            let flat = md.num_row_groups() > 0
                && arrow_schema.fields().len() == md.row_group(0).columns().len();
            if flat {
                let names: Vec<&str> = arrow_schema
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect();
                let mut keep = Vec::new();
                for rg_idx in 0..md.num_row_groups() {
                    match rg_pred_truth(pred, md.row_group(rg_idx), &names) {
                        RgTruth::NoMatch => {}
                        _ => keep.push(rg_idx),
                    }
                }
                row_groups_pruned = md.num_row_groups() - keep.len();
                if row_groups_pruned > 0 {
                    builder.with_row_groups(keep)
                } else {
                    builder
                }
            } else {
                builder
            }
        } else {
            builder
        };

        // P0b3: the reader is stored LAZILY — row groups decode on demand
        // as the consumer walks rows (early termination skips the rest).
        let reader = builder
            .build()
            .map_err(|e| StorageError::Decode(format!("parquet reader: {e}")))?;

        // A ProjectionMask SELECTS columns, it does not reorder them:
        // masked batches hold the selected set in FILE order. Position of
        // a read-set column = rank of its arrow field index among the
        // selected (sorted) field indices.
        let mut selected_sorted = read_field_indices.clone();
        selected_sorted.sort_unstable();
        let rank = |field_idx: usize| -> usize {
            selected_sorted
                .iter()
                .position(|&f| f == field_idx)
                .expect("field index is in the selected set by construction")
        };

        // projected column c -> its position in the masked batch
        let column_indices: Vec<usize> = projection
            .iter()
            .map(|name| {
                let p = read_names
                    .iter()
                    .position(|n| n == name)
                    .expect("projection ⊆ read set by construction");
                rank(read_field_indices[p])
            })
            .collect();

        // eq columns resolve within the masked batch too (they may be
        // projected or internal-only)
        let mut eq_col_indices = Vec::with_capacity(deletes.equality.len());
        for eq in &deletes.equality {
            let mut idxs = Vec::with_capacity(eq.columns.len());
            for c in &eq.columns {
                let pos = read_names
                    .iter()
                    .position(|n| n == c)
                    .expect("eq column ⊆ read set by construction");
                idxs.push(rank(read_field_indices[pos]));
            }
            eq_col_indices.push(idxs);
        }

        let column_names = projection
            .iter()
            .map(std::string::ToString::to_string)
            .collect();
        Ok(Self {
            num_rows_in_current: 0,
            batches: Box::new(
                reader.map(|b| {
                    b.unwrap_or_else(|e| panic!("lakehouse: parquet decode mid-scan: {e}"))
                }),
            ),
            current: None,
            row: 0,
            column_names,
            column_indices,
            projection_len,
            batch_start_abs: 0,
            deletes,
            eq_col_indices,
            row_groups_pruned,
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
            batches: Box::new(batches.into_iter()),
            current: None,
            row: 0,
            column_names: names,
            projection_len: indices.len(),
            column_indices: indices,
            row_groups_pruned: 0,
            batch_start_abs: 0,
            deletes: RowDeletes::default(),
            eq_col_indices: Vec::new(),
        })
    }

    #[must_use]
    pub fn column_names(&self) -> &[String] {
        &self.column_names
    }

    /// Advance to the next row across batch boundaries; false when done.
    /// After a `true` return, `row` points at the current row (first call
    /// after `open()` selects row 0). Named `next` deliberately: the FFI and
    /// FDW drive it imperatively; implementing `std::iter::Iterator` would
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
            // P0b3: the LAZY decode happens HERE — the reader decodes the
            // next row group only when the consumer has drained the
            // previous one. A decode failure panics: the C FFI wraps this
            // call in catch_unwind (13-review #8 tri-state), converting it
            // to a loud ERROR instead of a silent EOF.
            let Some(batch) = self.batches.next() else {
                return false; // genuine EOF
            };
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
        use arrow::array::Array as _;
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
    /// `CString` per cell. Skips batches whose every row is dead. The views
    /// borrow `current`'s buffers — valid until the following
    /// `next_batch`/`next` call or drop. Deletes/DVs are already applied
    /// via `live_idx`; the C consumer never sees dead rows.
    pub fn next_batch(&mut self) -> Result<Option<BatchView>, StorageError> {
        use arrow::array::Array as _;
        use arrow::array::{
            TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
            TimestampSecondArray,
        };
        use arrow::datatypes::DataType as DT;
        loop {
            // P0b3: LAZY decode — the next row group is pulled only when
            // the consumer has drained the previous batch.
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
                    nulls.push(u8::from(col.is_null(r)));
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
                            view.bools.push(u8::from(a.value(r)));
                        }
                    }
                    DT::Int32 | DT::Int64 | DT::Date64 => {
                        view.kind = BatchKind::Int64;
                        for r in 0..self.num_rows_in_current {
                            let v = match col.data_type() {
                                DT::Int32 => i64::from(
                                    col.as_any()
                                        .downcast_ref::<arrow::array::Int32Array>()
                                        .expect("int32 array")
                                        .value(r),
                                ),
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
                                DT::Float32 => f64::from(
                                    col.as_any()
                                        .downcast_ref::<arrow::array::Float32Array>()
                                        .expect("float32 array")
                                        .value(r),
                                ),
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
                            view.i64s.push(i64::from(a.value(r)));
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
    /// before `next()` returns Null (the FDW drives `next()` first).
    #[must_use]
    pub fn value(&self, col_idx: usize) -> CellValue {
        use arrow::array::Array as _;
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
    use arrow::array::{
        BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
        Float32Array, Float64Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
        Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    };
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
            .map_or(CellValue::Null, |a| CellValue::Bool(a.value(row))),
        DT::Int32 => down(col).map_or(CellValue::Null, |v| CellValue::Int64(i64::from(v))),
        DT::Int64 | DT::Date64 => col
            .as_any()
            .downcast_ref::<Int64Array>()
            .map_or(CellValue::Null, |a| CellValue::Int64(a.value(row))),
        // temporal arrays are their own primitive types (timestamp columns
        // caught live with PyIceberg-written fixtures, slice 12)
        // date/time -> PostgreSQL-input-compatible TEXT (slice 21)
        DT::Date32 => col
            .as_any()
            .downcast_ref::<Date32Array>()
            .map_or(CellValue::Null, |a| {
                let (y, m, d) = civil_from_days(i64::from(a.value(row)));
                CellValue::Str(format!("{y:04}-{m:02}-{d:02}"))
            }),
        DT::Time32(unit) => match unit {
            arrow::datatypes::TimeUnit::Second => col
                .as_any()
                .downcast_ref::<Time32SecondArray>()
                .map_or(CellValue::Null, |a| {
                    CellValue::Str(format_time(i64::from(a.value(row)), 6))
                }),
            arrow::datatypes::TimeUnit::Millisecond => col
                .as_any()
                .downcast_ref::<Time32MillisecondArray>()
                .map_or(CellValue::Null, |a| {
                    CellValue::Str(format_time(i64::from(a.value(row)) * 1_000, 3))
                }),
            _ => CellValue::Null,
        },
        DT::Time64(unit) => match *unit {
            arrow::datatypes::TimeUnit::Microsecond => col
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .map_or(CellValue::Null, |a| {
                    CellValue::Str(format_time(a.value(row), 6))
                }),
            arrow::datatypes::TimeUnit::Nanosecond => col
                .as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .map_or(CellValue::Null, |a| {
                    CellValue::Str(format_time(a.value(row) / 1_000, 6))
                }),
            // Time64 cannot hold second/millisecond precision; kept
            // unreachable for enum exhaustiveness
            _ => CellValue::Null,
        },
        DT::Timestamp(unit, _) => match *unit {
            arrow::datatypes::TimeUnit::Second => col
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .map_or(CellValue::Null, |a| {
                    CellValue::TimestampMicros(a.value(row) * 1_000_000)
                }),
            arrow::datatypes::TimeUnit::Millisecond => col
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .map_or(CellValue::Null, |a| {
                    CellValue::TimestampMicros(a.value(row) * 1_000)
                }),
            arrow::datatypes::TimeUnit::Microsecond => col
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .map_or(CellValue::Null, |a| {
                    CellValue::TimestampMicros(a.value(row))
                }),
            // nanosecond sources truncate to micros (doc 05 §4)
            arrow::datatypes::TimeUnit::Nanosecond => col
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .map_or(CellValue::Null, |a| {
                    CellValue::TimestampMicros(a.value(row) / 1_000)
                }),
        },
        DT::Float32 => col
            .as_any()
            .downcast_ref::<Float32Array>()
            .map_or(CellValue::Null, |a| {
                CellValue::Float64(f64::from(a.value(row)))
            }),
        DT::Float64 => col
            .as_any()
            .downcast_ref::<Float64Array>()
            .map_or(CellValue::Null, |a| CellValue::Float64(a.value(row))),
        // LargeUtf8 has its OWN array type — downcasting it to StringArray
        // failed silently and surfaced SQL NULL (13-review #8)
        DT::Utf8 => col
            .as_any()
            .downcast_ref::<StringArray>()
            .map_or(CellValue::Null, |a| {
                CellValue::Str(a.value(row).to_string())
            }),
        DT::LargeUtf8 => col
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map_or(CellValue::Null, |a| {
                CellValue::Str(a.value(row).to_string())
            }),
        // binary -> bytea hex text ('\x…', bytea_in-compatible; slice 21)
        DT::Binary => col
            .as_any()
            .downcast_ref::<BinaryArray>()
            .map_or(CellValue::Null, |a| {
                let b = a.value(row);
                CellValue::Str(crate::parquet::hex_escape_bytes(b))
            }),
        // LargeBinary likewise has its own array type (13-review #8)
        DT::LargeBinary => {
            col.as_any()
                .downcast_ref::<LargeBinaryArray>()
                .map_or(CellValue::Null, |a| {
                    let b = a.value(row);
                    CellValue::Str(crate::parquet::hex_escape_bytes(b))
                })
        }
        // decimal -> canonical unscaled/scale TEXT (numeric_in-compatible;
        // slice 21 conformance gate)
        DT::Decimal128(_, scale) => col
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .map_or(CellValue::Null, |a| {
                CellValue::Str(format_decimal128(a.value(row), *scale))
            }),
        // 16-byte fixed binary is the parquet shape of Iceberg `uuid`
        // (canonical hyphenated text for uuid_in; `fixed(16)` columns are
        // rare and render the same — noted in HANDOFF). Other sizes are
        // plain `fixed[N]` bytes.
        DT::FixedSizeBinary(16) => col
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .map_or(CellValue::Null, |a| {
                CellValue::Str(format_uuid16(a.value(row)))
            }),
        DT::FixedSizeBinary(_) => col
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .map_or(CellValue::Null, |a| CellValue::Bytes(a.value(row).to_vec())),
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
    format!("{h:02}:{mi:02}:{s:02}.{f:0fwidth$}")
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

    /// Pins the bytea-hex text form shared by the per-row, batch, and delta
    /// render paths: `\x` prefix + two lowercase hex digits per byte
    /// (byteaout hex-mode compatible). All three consumers MUST agree —
    /// `hex_escape_bytes` consolidation (lint pass) deliberately changed the
    /// per-row path from bare hex to this form.
    #[test]
    fn hex_escape_bytes_bytea_form() {
        assert_eq!(hex_escape_bytes(b""), "\\x");
        assert_eq!(hex_escape_bytes(b"AB"), "\\x4142");
        assert_eq!(hex_escape_bytes(&[0x00, 0xff, 0x7f]), "\\x00ff7f");
        assert_eq!(hex_escape_bytes(b"abc"), "\\x616263");
    }
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
            .map(std::string::ToString::to_string)
            .collect::<Vec<_>>()
            .join(",")
    }

    /// TDD anchor (M1 slice 20): position deletes skip exact absolute row
    /// positions (v2 (`file_path`, pos) delete files).
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

        let mut rows =
            ParquetRows::open_with_deletes(&io, "a.parquet", &["id"], deletes, None).unwrap();
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
            ParquetRows::open_with_deletes(&io, "data.parquet", &["name"], deletes, None).unwrap();
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
        let b = FixedSizeBinaryArray::try_from(vec![
            Some(
                {
                    let mut v = vec![0u8; 16];
                    v[6] = 0x12;
                    v
                }
                .as_slice(),
            ),
            None,
        ])
        .unwrap();
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
    /// 2a (spec 15 §6.3a / doc 06 §1 step 5): row-group stats pruning.
    /// Two row groups (ids 1..=100, writer `max_row_group_size` = 50): a
    /// predicate provably empty for group 1 (id >= 60) must skip it —
    /// `rows_open` returns only group 2's ids and reports 1 pruned group.
    /// A predicate every group could match reports 0.
    #[test]
    fn row_group_stats_pruning_skips_and_counts() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;

        let dir = tempfile::tempdir().unwrap();
        crate::storage::test_allow_local_stores();
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Int64, false),
            ArrowField::new("grp", DataType::Utf8, false),
        ]));
        let file = std::fs::File::create(dir.path().join("rg.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(
            file,
            schema.clone(),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_row_count(Some(50))
                    .build(),
            ),
        )
        .unwrap();
        let ids: Vec<i64> = (1..=100).collect();
        let grps: Vec<String> = ids.iter().map(|i| format!("g{}", (i - 1) / 50)).collect();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids.clone())),
                Arc::new(StringArray::from(grps.clone())),
            ],
        )
        .unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();

        let io = StorageIo::open(
            &crate::storage::Location::Local {
                root: dir.path().to_str().unwrap().to_string(),
            },
            None,
        )
        .unwrap();
        let ge60 = crate::scan::ScanPredicate::Ge {
            field: "id".into(),
            value: serde_json::json!(60),
        };
        let mut rows = ParquetRows::open_with_deletes(
            &io,
            "rg.parquet",
            &["id"],
            RowDeletes::default(),
            Some(&ge60),
        )
        .unwrap();
        assert_eq!(rows.row_groups_pruned, 1, "group 1 (ids 1..=50) pruned");

        let mut got = Vec::new();
        while rows.next() {
            match rows.value(0) {
                CellValue::Int64(v) => got.push(v),
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(got, (51..=100).collect::<Vec<i64>>());

        // a predicate every group could match keeps both groups
        let le100 = crate::scan::ScanPredicate::Le {
            field: "id".into(),
            value: serde_json::json!(100),
        };
        let rows = ParquetRows::open_with_deletes(
            &io,
            "rg.parquet",
            &["id"],
            RowDeletes::default(),
            Some(&le100),
        )
        .unwrap();
        assert_eq!(rows.row_groups_pruned, 0);
    }

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
    /// `open_all` branch in `rows_open` silently counted deleted rows. This
    /// exercises `open_with_deletes` with an empty projection: `next()`
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
        let rows = ParquetRows::open_with_deletes(&io, &key, &[], rd, None).unwrap();
        let mut seen = 0;
        let mut r = rows;
        while r.next() {
            seen += 1;
        }
        assert_eq!(seen, 2, "deleted row 11 skipped even with empty projection");
    }
    // ---- P0b: ranged, cached, projection-pruned opens (spec 15 §6.3a) ----

    /// A fat second column that is NOT projected must never be fetched,
    /// and a repeat open of the same file must fetch nothing (immutable-
    /// path cache law, §6.2). Deterministic via a TEST-LOCAL cache (the
    /// global cache is shared across test threads).
    #[test]
    fn ranged_open_is_projection_lazy_and_repeat_free() {
        use crate::storage::parse_location;
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        crate::storage::test_allow_local_stores();
        let schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("pad", DataType::Utf8, true),
        ]);
        let n = 2000usize;
        let ids: Vec<i64> = (0..n as i64).collect();
        let pad: Vec<String> = (0..n).map(|i| format!("{i:0>120}")).collect();
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int64Array::from(ids.clone())),
                Arc::new(StringArray::from(pad)),
            ],
        )
        .unwrap();
        let f = std::fs::File::create(dir.path().join("fat.parquet")).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, batch.schema(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();

        let io = StorageIo::open(
            &parse_location(&format!("file://{}", dir.path().display())).unwrap(),
            None,
        )
        .unwrap();
        let cache: &'static BlockCache = Box::leak(Box::new(BlockCache::new(8 << 20)));
        let file_len = io.stat_len("fat.parquet").unwrap();
        assert!(
            file_len > 150_000,
            "fixture too small to prove laziness: {file_len}"
        );

        // open + FULL read of the narrow projection: the fat column must
        // not ride along (whole-file open fetched {file_len} every time)
        let before = cache.fetched_bytes();
        let mut rows = ParquetRows::open_internal(
            &io,
            "fat.parquet",
            &["id"],
            RowDeletes::default(),
            None,
            cache,
        )
        .unwrap();
        let mut count = 0;
        while rows.next() {
            count += 1;
        }
        assert_eq!(count, n);
        let delta = cache.fetched_bytes() - before;
        assert!(
            delta * 4 < file_len,
            "projected open fetched {delta} of {file_len} bytes — not projection-lazy"
        );

        // second full open, same projection: pure cache hits
        let before2 = cache.fetched_bytes();
        let mut rows = ParquetRows::open_internal(
            &io,
            "fat.parquet",
            &["id"],
            RowDeletes::default(),
            None,
            cache,
        )
        .unwrap();
        let mut count2 = 0;
        while rows.next() {
            count2 += 1;
        }
        assert_eq!(count2, n);
        assert_eq!(
            cache.fetched_bytes() - before2,
            0,
            "repeat scan fetched bytes — the immutable-path cache must serve it"
        );
    }

    /// Byte-equivalence: the ranged path returns exactly the rows the
    /// whole-object (Bytes) oracle returns — same ids, same order, same
    /// nulls (500 rows incl. NULL names; `open_all` keeps the oracle role).
    #[test]
    fn ranged_open_rows_match_whole_file_oracle() {
        use crate::storage::parse_location;
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        crate::storage::test_allow_local_stores();
        let schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let n = 500usize;
        let ids: Vec<i64> = (0..n as i64).collect();
        let names: Vec<Option<String>> = (0..n)
            .map(|i| {
                if i % 7 == 0 {
                    None
                } else {
                    Some(format!("n-{i}"))
                }
            })
            .collect();
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names.clone())),
            ],
        )
        .unwrap();
        let f = std::fs::File::create(dir.path().join("eq.parquet")).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, batch.schema(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();

        let io = StorageIo::open(
            &parse_location(&format!("file://{}", dir.path().display())).unwrap(),
            None,
        )
        .unwrap();

        // ranged open (P0b path), both columns
        let cache: &'static BlockCache = Box::leak(Box::new(BlockCache::new(4 << 20)));
        let mut rows = ParquetRows::open_internal(
            &io,
            "eq.parquet",
            &["id", "name"],
            RowDeletes::default(),
            None,
            cache,
        )
        .unwrap();
        let mut got: Vec<(i64, Option<String>)> = Vec::new();
        while rows.next() {
            let id = match rows.value(0) {
                CellValue::Int64(v) => v,
                other => panic!("id cell wrong: {other:?}"),
            };
            let name = match rows.value(1) {
                CellValue::Null => None,
                CellValue::Str(s) => Some(s),
                other => panic!("name cell wrong: {other:?}"),
            };
            got.push((id, name));
        }
        let want: Vec<(i64, Option<String>)> = (0..n as i64)
            .map(|i| (i, names[i as usize].clone()))
            .collect();
        assert_eq!(got, want, "ranged rows differ from the whole-file oracle");
    }

    // Use an HTTP provider so wire requests are counted, including failed
    // reads and requests whose cache behavior has no effect on network I/O.
    struct RangeFixture {
        server: std::sync::Arc<tiny_http::Server>,
        worker: Option<std::thread::JoinHandle<()>>,
        ranges: std::sync::Arc<std::sync::Mutex<Vec<std::ops::Range<usize>>>>,
        fail_next: std::sync::Arc<std::sync::atomic::AtomicBool>,
        io: StorageIo,
    }

    impl RangeFixture {
        fn new(payload: Vec<u8>) -> Self {
            let server = std::sync::Arc::new(tiny_http::Server::http("127.0.0.1:0").unwrap());
            let ranges = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let fail_next = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let listener = server.clone();
            let requests = ranges.clone();
            let fail = fail_next.clone();
            let worker = std::thread::spawn(move || {
                let etag = tiny_http::Header::from_bytes("ETag", "\"fixture\"").unwrap();
                let last_modified =
                    tiny_http::Header::from_bytes("Last-Modified", "Sat, 21 Nov 2026 14:00:00 GMT")
                        .unwrap();
                for req in listener.incoming_requests() {
                    assert_eq!(req.url(), "/fixture/data");
                    let reply = if req.method() == &tiny_http::Method::Head {
                        tiny_http::Response::from_data(payload.clone())
                    } else {
                        assert_eq!(req.method(), &tiny_http::Method::Get);
                        let (start, end) = req
                            .headers()
                            .iter()
                            .find(|h| h.field.equiv("Range"))
                            .unwrap()
                            .value
                            .as_str()
                            .strip_prefix("bytes=")
                            .unwrap()
                            .split_once('-')
                            .unwrap();
                        let start: usize = start.parse().unwrap();
                        let end = end.parse::<usize>().unwrap() + 1;
                        requests.lock().unwrap().push(start..end);
                        assert!(start < end && end <= payload.len());
                        if fail.swap(false, std::sync::atomic::Ordering::Relaxed) {
                            tiny_http::Response::from_data(Vec::new()).with_status_code(403)
                        } else {
                            tiny_http::Response::from_data(payload[start..end].to_vec())
                                .with_status_code(206)
                                .with_header(
                                    tiny_http::Header::from_bytes(
                                        "Content-Range",
                                        format!("bytes {start}-{}/{}", end - 1, payload.len()),
                                    )
                                    .unwrap(),
                                )
                        }
                    };
                    req.respond(
                        reply
                            .with_chunked_threshold(usize::MAX)
                            .with_header(etag.clone())
                            .with_header(last_modified.clone()),
                    )
                    .unwrap();
                }
            });
            let io = StorageIo::open_bound_s3(
                &crate::storage::Location::S3 {
                    bucket: "fixture".into(),
                    key: "data".into(),
                },
                &crate::storage::S3Options {
                    endpoint: Some(format!("http://{}", server.server_addr())),
                    region: Some("us-east-1".into()),
                    access_key_id: Some("test-key".into()),
                    secret_access_key: Some("test-secret".into()),
                    path_style: true,
                },
            )
            .unwrap();
            Self {
                server,
                worker: Some(worker),
                ranges,
                fail_next,
                io,
            }
        }

        fn ranges(&self) -> Vec<std::ops::Range<usize>> {
            self.ranges.lock().unwrap().clone()
        }
    }

    impl Drop for RangeFixture {
        fn drop(&mut self) {
            self.server.unblock();
            self.worker.take().unwrap().join().unwrap();
        }
    }

    #[test]
    fn projected_pages_coalesce_without_fetching_the_adjacent_column() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use arrow::record_batch::RecordBatch;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("pad", DataType::Utf8, false),
        ]));
        let ids = (0..256).collect::<Vec<i64>>();
        let pad = (0..256).map(|id| format!("{id:0>512}")).collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids.clone())),
                Arc::new(StringArray::from(pad)),
            ],
        )
        .unwrap();
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(64))
            .set_data_page_row_count_limit(8)
            .set_write_batch_size(8)
            .set_dictionary_enabled(false)
            .build();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(Vec::new(), schema, Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        let data = writer.into_inner().unwrap();
        let metadata =
            ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(&data)).unwrap();
        let mut unprojected = Vec::new();
        let mut projected = Vec::new();
        for group in metadata.metadata().row_groups() {
            let bounds = |index| {
                let (start, len) = group.column(index).byte_range();
                start as usize..(start + len) as usize
            };
            projected.push(bounds(0));
            unprojected.push(bounds(1));
        }
        let fixture = RangeFixture::new(data);
        let cache = Box::leak(Box::new(BlockCache::new(128 << 10)));
        let mut rows = ParquetRows::open_internal(
            &fixture.io,
            "data",
            &["id"],
            RowDeletes::default(),
            None,
            cache,
        )
        .unwrap();
        let mut got = Vec::new();
        while rows.next() {
            got.push(rows.value(0));
        }
        assert_eq!(
            got,
            ids.into_iter().map(CellValue::Int64).collect::<Vec<_>>()
        );
        let ranges = fixture.ranges();
        let gets = ranges.len();
        let bytes: usize = ranges.iter().map(std::ops::Range::len).sum();
        let tiny_gets = ranges.iter().filter(|range| range.len() <= 4).count();
        eprintln!("projection wire sample gets={gets} bytes={bytes} tiny_gets={tiny_gets}");
        let data_ranges = ranges
            .iter()
            .filter(|range| {
                projected
                    .iter()
                    .any(|chunk| range.start < chunk.end && chunk.start < range.end)
            })
            .collect::<Vec<_>>();
        let actual: usize = data_ranges.iter().map(|range| range.len()).sum();
        let expected: usize = projected.iter().map(std::ops::Range::len).sum();
        assert_eq!(actual, expected, "duplicate or missing projected bytes");
        assert!(!ranges.iter().any(|range| unprojected
            .iter()
            .any(|chunk| range.start < chunk.end && chunk.start < range.end)));
        assert!(
            data_ranges.len() <= projected.len(),
            "expected one GET per small projected chunk, saw {} for {} chunks",
            data_ranges.len(),
            projected.len()
        );
    }

    #[test]
    fn range_window_resumes_and_shares_bytes_after_the_stream_is_dropped() {
        let start = 8;
        let window_end = start + PARQUET_READ_AHEAD as usize;
        let chunk_end = window_end + 1024;
        let file_end = chunk_end + 12;
        let data: Vec<u8> = (0..file_end)
            .map(|position| (position % 127) as u8)
            .collect();
        let fixture = RangeFixture::new(data.clone());
        let cache = Box::leak(Box::new(BlockCache::new(0)));
        let reader = RangedReader::new(&fixture.io, cache, "data", file_end as u64);
        reader
            .ahead
            .projected_chunks
            .set(std::iter::once(start as u64..chunk_end as u64).collect())
            .unwrap();

        let unused = reader.get_read(start as u64).unwrap();
        drop(unused);
        assert_eq!(fixture.ranges().len(), 0);

        let mut header = reader.get_read(start as u64).unwrap();
        let mut byte = [0];
        fixture
            .fail_next
            .store(true, std::sync::atomic::Ordering::Relaxed);
        assert!(header.read(&mut byte).is_err());
        header.read_exact(&mut byte).unwrap();
        assert_eq!(byte[0], data[start]);
        drop(header);

        let mut resumed = reader.get_read((start + 1) as u64).unwrap();
        resumed.read_exact(&mut byte).unwrap();
        assert_eq!(byte[0], data[start + 1]);
        drop(resumed);

        let body_start = start + 3;
        let body_end = window_end + 32;
        assert_eq!(
            reader
                .get_bytes(body_start as u64, body_end - body_start)
                .unwrap()[..],
            data[body_start..body_end]
        );
        assert_eq!(
            fixture.ranges(),
            vec![start..window_end, start..window_end, window_end..body_end],
            "failed requests may retry, a successful buffered prefix must not be fetched again"
        );

        let mut tail = reader.get_read((chunk_end - 3) as u64).unwrap();
        let mut buf = [0; 10];
        assert_eq!(tail.read(&mut buf).unwrap(), 3);
        assert_eq!(&buf[..3], &data[chunk_end - 3..chunk_end]);
        assert_eq!(tail.read(&mut buf[..5]).unwrap(), 5);
        assert_eq!(&buf[..5], &data[chunk_end..chunk_end + 5]);
        assert_eq!(tail.read(&mut buf).unwrap(), 7);
        assert_eq!(&buf[..7], &data[chunk_end + 5..file_end]);
        assert_eq!(tail.read(&mut buf).unwrap(), 0);
        // A request enclosing the full window uses one exact range rather
        // than two large fragments. The stream can still use the old window.
        assert_eq!(reader.get_bytes(0, file_end).unwrap()[..], data[..]);
        let mut eof = reader.get_read((file_end - 2) as u64).unwrap();
        assert_eq!(eof.read(&mut buf).unwrap(), 2);
        assert_eq!(eof.read(&mut buf).unwrap(), 0);
        assert_eq!(&buf[..2], &data[file_end - 2..file_end]);
        assert!(reader.get_bytes((file_end - 1) as u64, 2).is_err());
        assert!(reader.get_bytes(u64::MAX, 2).is_err());
        assert_eq!(
            fixture.ranges(),
            vec![
                start..window_end,
                start..window_end,
                window_end..body_end,
                chunk_end - 3..chunk_end,
                chunk_end..chunk_end + 5,
                chunk_end + 5..file_end,
                0..file_end,
                file_end - 2..file_end,
            ]
        );
    }
}
