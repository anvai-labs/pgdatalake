//! Parquet writer: columnar batch accumulator -> parquet bytes -> object
//! storage (specs/lakehouse/06-execution.md, M2 slice 24). This is the file
//! half of the managed-write path: the M2 commit protocol (slice 25) turns
//! the returned file list into an Iceberg snapshot commit.
//!
//! Types mirror the read side (`CellValue`) so the `TableAM` insert path
//! (slice 27) can feed rows without a second representation. Canonical TEXT
//! forms (dates, times, decimals, uuids) are the same ones the reader
//! produces — round-trip stability by construction.

use crate::catalog::metadata::IcebergType;
use crate::parquet::CellValue;
use crate::storage::{StorageError, StorageIo};

use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use std::sync::Arc;

/// Per-file manifest metric maps (keyed by field ID).
#[derive(Default, Debug, Clone)]
pub struct ColumnStats {
    pub column_sizes: std::collections::BTreeMap<i64, i64>,
    pub value_counts: std::collections::BTreeMap<i64, i64>,
    pub null_value_counts: std::collections::BTreeMap<i64, i64>,
    pub lower_bounds: std::collections::BTreeMap<i64, Vec<u8>>,
    pub upper_bounds: std::collections::BTreeMap<i64, Vec<u8>>,
}

/// Min/max bounds of one typed arrow column, serialized per the Iceberg
/// spec's single-value table: int/date 4-byte LE, long/time/timestamp
/// 8-byte LE, float/double LE IEEE, string UTF-8 bytes (byte order).
/// NULLs are skipped; NaN floats are skipped (spec: bounds must not be
/// NaN); an all-null/all-NaN column yields None (bounds omitted —
/// conservative keep on read, never a misread). Bool/binary/decimal/
/// uuid yield None (this reader's prune decoder doesn't compare them
/// yet — omitting is always safe).
fn column_bounds(
    ty: &IcebergType,
    col: &Arc<dyn arrow::array::Array>,
) -> Option<(Vec<u8>, Vec<u8>)> {
    use arrow::array::{
        Array as _, Date32Array, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
        Time64MicrosecondArray,
    };
    use std::cmp::Ordering;
    macro_rules! prim_minmax {
        ($a:ty, $t:ty, $enc:expr) => {{
            let a = col.as_any().downcast_ref::<$a>()?;
            let mut lo: Option<$t> = None;
            let mut hi: Option<$t> = None;
            for i in 0..a.len() {
                if a.is_null(i) {
                    continue;
                }
                let v = a.value(i);
                match lo {
                    None => {
                        lo = Some(v);
                        hi = Some(v);
                    }
                    Some(l) => {
                        if v < l {
                            lo = Some(v);
                        }
                        if v > hi.unwrap() {
                            hi = Some(v);
                        }
                    }
                }
            }
            lo.and_then(|l| hi.map(|h| ($enc(l), $enc(h))))
        }};
    }
    macro_rules! float_minmax {
        ($a:ty, $t:ty) => {{
            let a = col.as_any().downcast_ref::<$a>()?;
            let mut lo: Option<$t> = None;
            let mut hi: Option<$t> = None;
            for i in 0..a.len() {
                if a.is_null(i) {
                    continue;
                }
                let v = a.value(i);
                if v.is_nan() {
                    continue; // spec: bounds exclude NaN
                }
                match lo {
                    None => {
                        lo = Some(v);
                        hi = Some(v);
                    }
                    Some(l) => {
                        if v < l {
                            lo = Some(v);
                        }
                        if v > hi.unwrap() {
                            hi = Some(v);
                        }
                    }
                }
            }
            lo.map(|l| {
                let h = hi.unwrap_or(l);
                (l.to_le_bytes().to_vec(), h.to_le_bytes().to_vec())
            })
        }};
    }
    match ty {
        IcebergType::Int => prim_minmax!(Int32Array, i32, |v: i32| v.to_le_bytes().to_vec()),
        IcebergType::Long | IcebergType::Timestamp | IcebergType::Timestamptz => {
            prim_minmax!(Int64Array, i64, |v: i64| v.to_le_bytes().to_vec())
        }
        IcebergType::Date => prim_minmax!(Date32Array, i32, |v: i32| v.to_le_bytes().to_vec()),
        IcebergType::Time => prim_minmax!(Time64MicrosecondArray, i64, |v: i64| v
            .to_le_bytes()
            .to_vec()),
        // timestamp arrays are their own primitive types on the write side
        // (mirror of the read-side downcast quirk)
        IcebergType::String => {
            let a = col.as_any().downcast_ref::<StringArray>()?;
            let mut lo: Option<&str> = None;
            let mut hi: Option<&str> = None;
            for i in 0..a.len() {
                if a.is_null(i) {
                    continue;
                }
                let v = a.value(i);
                match lo {
                    None => {
                        lo = Some(v);
                        hi = Some(v);
                    }
                    Some(l) => {
                        if v.as_bytes() < l.as_bytes() {
                            lo = Some(v);
                        }
                        if v.as_bytes() > hi.unwrap().as_bytes() {
                            hi = Some(v);
                        }
                    }
                }
            }
            lo.map(|l| (l.as_bytes().to_vec(), hi.unwrap().as_bytes().to_vec()))
        }
        IcebergType::Float => float_minmax!(Float32Array, f32),
        IcebergType::Double => float_minmax!(Float64Array, f64),
        _ => {
            let _ = Ordering::Less;
            None
        }
    }
}

/// Identity + metrics of one written data file (from `write_file`).
#[derive(Debug, Clone)]
pub struct WrittenFile {
    pub key: String,
    pub record_count: i64,
    pub file_size_in_bytes: i64,
    /// Per-column stats keyed by FIELD ID (Iceberg manifest metrics, M3-R2):
    /// prunable min/max bounds serialized per the spec's single-value
    /// table (LE ints/floats, UTF-8 strings), footer-derived column
    /// sizes, exact value/null counts. Columns with no representable
    /// bound (binary/decimal/uuid/bool) omit their bound keys —
    /// conservative, never wrong.
    pub column_sizes: std::collections::BTreeMap<i64, i64>,
    pub value_counts: std::collections::BTreeMap<i64, i64>,
    pub null_value_counts: std::collections::BTreeMap<i64, i64>,
    pub lower_bounds: std::collections::BTreeMap<i64, Vec<u8>>,
    pub upper_bounds: std::collections::BTreeMap<i64, Vec<u8>>,
}

/// One column of the write schema. `field_id` stamps the parquet footer
/// with `PARQUET:field_id` — other engines' readers (Trino, pyiceberg)
/// map parquet columns to the Iceberg schema BY FIELD ID, so without it
/// they read empty columns (27b live-cycle finding).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WriteColumn {
    pub name: String,
    #[serde(alias = "type")]
    pub ty: IcebergType,
    pub field_id: Option<i64>,
}

/// Columnar accumulator accepting rows of `CellValue` (M2 insert path).
pub struct BatchWriter {
    columns: Vec<WriteColumn>,
    builders: Vec<Builder>,
    rows: usize,
}

#[derive(Debug)]
enum Builder {
    Bool(arrow::array::BooleanBuilder),
    Int32(arrow::array::Int32Builder),
    Int64(arrow::array::Int64Builder),
    Float32(arrow::array::Float32Builder),
    Float64(arrow::array::Float64Builder),
    Str(arrow::array::StringBuilder),
    Binary(arrow::array::BinaryBuilder),
    Date32(arrow::array::Date32Builder),
    Time64Us(arrow::array::Time64MicrosecondBuilder),
    TsUs(arrow::array::TimestampMicrosecondBuilder),
    Decimal(arrow::array::Decimal128Builder),
    Uuid(arrow::array::FixedSizeBinaryBuilder),
}

fn arrow_type(ty: &IcebergType) -> Result<DataType, StorageError> {
    use IcebergType as IT;
    Ok(match ty {
        IT::Boolean => DataType::Boolean,
        IT::Int => DataType::Int32,
        IT::Long => DataType::Int64,
        IT::Float => DataType::Float32,
        IT::Double => DataType::Float64,
        IT::String => DataType::Utf8,
        IT::Binary => DataType::Binary,
        IT::Date => DataType::Date32,
        IT::Time => DataType::Time64(TimeUnit::Microsecond),
        IT::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
        IT::Timestamptz => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        IT::Decimal { precision, scale } => DataType::Decimal128(*precision as u8, *scale as i8),
        IT::Uuid => DataType::FixedSizeBinary(16),
        other => {
            return Err(StorageError::Config(format!(
            "write path: unsupported type {other:?} (nested types land with managed-table slices)"
        )))
        }
    })
}

/// Days since 1970-01-01 from a civil `YYYY-MM-DD` string.
fn parse_date_days(s: &str) -> Option<i32> {
    let mut it = s.split('-');
    let y: i64 = it.next()?.parse().ok()?;
    let m: u32 = it.next()?.parse().ok()?;
    let d: u32 = it.next()?.parse().ok()?;
    Some(days_from_civil(y, m, d) as i32)
}

/// Howard Hinnant's `civil_from_days`: canonical `YYYY-MM-DD` text from
/// days since epoch — the write-side inverse of `parse_date_days` (used by
/// the row-JSON parser for typed {"d": days} date cells).
#[must_use]
pub fn format_date_days(days: i64) -> String {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    format!("{:04}-{:02}-{:02}", if m <= 2 { y + 1 } else { y }, m, d)
}

/// Canonical `HH:MM:SS.ffffff` from microseconds since midnight (the
/// write-side inverse of `parse_time_us`).
#[must_use]
pub fn format_time_us(us: i64) -> String {
    let secs = us.div_euclid(1_000_000);
    let frac = us.rem_euclid(1_000_000);
    format!(
        "{:02}:{:02}:{:02}.{:06}",
        secs / 3600,
        (secs % 3600) / 60,
        secs % 60,
        frac
    )
}

/// Howard Hinnant's `days_from_civil` (inverse of `civil_from_days`).
#[must_use]
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64; // [0, 399]
    let mp = u64::from(if m > 2 { m - 3 } else { m + 9 }); // [3, 14]
    let doy = (153 * mp + 2) / 5 + u64::from(d) - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe as i64 - 719_468
}

/// Microseconds from `HH:MM:SS(.ffffff)` (fraction optional).
fn parse_time_us(s: &str) -> Option<i64> {
    let (hms, frac) = match s.split_once('.') {
        Some((a, b)) => (a, b),
        None => (s, ""),
    };
    let mut it = hms.split(':');
    let h: i64 = it.next()?.parse().ok()?;
    let mi: i64 = it.next()?.parse().ok()?;
    let sec: i64 = it.next()?.parse().ok()?;
    let frac_micros: i64 = if frac.is_empty() {
        0
    } else {
        let mut digits = frac.to_string();
        while digits.len() < 6 {
            digits.push('0');
        }
        digits[..6].parse().ok()?
    };
    Some((h * 3600 + mi * 60 + sec) * 1_000_000 + frac_micros)
}

/// Unscaled i128 from canonical decimal text (`-5.00`, `123.45`).
/// (unscaled value, textual scale) — the caller reconciles the scale to
/// the column's declared target (13-review #8b).
fn parse_decimal_parts(s: &str) -> Option<(i128, u32)> {
    let (sign, body) = match s.strip_prefix('-') {
        Some(b) => (-1i128, b),
        None => (1i128, s.strip_prefix('+').unwrap_or(s)),
    };
    let (int_part, frac_part) = match body.split_once('.') {
        Some((a, b)) => (a, b),
        None => (body, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    let mut unscaled: i128 = int_part.parse().ok()?;
    for c in frac_part.chars() {
        unscaled = unscaled.checked_mul(10)?;
        unscaled = unscaled.checked_add(i128::from(c.to_digit(10)?))?;
    }
    Some((sign * unscaled, frac_part.len() as u32))
}

fn hex_uuid_bytes(s: &str) -> Option<Vec<u8>> {
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    (0..16)
        .map(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok())
        .collect()
}

impl BatchWriter {
    pub fn new(columns: Vec<WriteColumn>) -> Result<Self, StorageError> {
        let builders = columns
            .iter()
            .map(|c| {
                Ok(match arrow_type(&c.ty)? {
                    DataType::Boolean => Builder::Bool(arrow::array::BooleanBuilder::new()),
                    DataType::Int32 => Builder::Int32(arrow::array::Int32Builder::new()),
                    DataType::Int64 => Builder::Int64(arrow::array::Int64Builder::new()),
                    DataType::Float32 => Builder::Float32(arrow::array::Float32Builder::new()),
                    DataType::Float64 => Builder::Float64(arrow::array::Float64Builder::new()),
                    DataType::Utf8 => {
                        Builder::Str(arrow::array::StringBuilder::with_capacity(1024, 8))
                    }
                    DataType::Binary => {
                        Builder::Binary(arrow::array::BinaryBuilder::with_capacity(1024, 8))
                    }
                    DataType::Date32 => Builder::Date32(arrow::array::Date32Builder::new()),
                    DataType::Time64(TimeUnit::Microsecond) => {
                        Builder::Time64Us(arrow::array::Time64MicrosecondBuilder::new())
                    }
                    DataType::Timestamp(TimeUnit::Microsecond, _) => {
                        Builder::TsUs(arrow::array::TimestampMicrosecondBuilder::new())
                    }
                    DataType::Decimal128(p, s) => {
                        let mut b = arrow::array::Decimal128Builder::new();
                        b = b.with_precision_and_scale(p, s).map_err(|e| {
                            StorageError::Config(format!("write path: decimal: {e}"))
                        })?;
                        Builder::Decimal(b)
                    }
                    DataType::FixedSizeBinary(16) => {
                        Builder::Uuid(arrow::array::FixedSizeBinaryBuilder::with_capacity(64, 16))
                    }
                    other => {
                        return Err(StorageError::Config(format!(
                            "write path: unexpected arrow type {other:?}"
                        )))
                    }
                })
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        Ok(Self {
            columns,
            builders,
            rows: 0,
        })
    }

    #[must_use]
    pub fn num_rows(&self) -> usize {
        self.rows
    }

    /// Push one row; column count must match the schema, `CellValue::Null`
    /// for NULLs.
    pub fn push(&mut self, row: &[CellValue]) -> Result<(), StorageError> {
        if row.len() != self.builders.len() {
            return Err(StorageError::Config(format!(
                "write path: row has {} values, schema has {} columns",
                row.len(),
                self.builders.len()
            )));
        }
        // PASS 1 — validate the ENTIRE row before mutating any builder
        // (13-review #8b): a bad cell mid-row used to leave earlier
        // builders advanced, misaligning every subsequent row in the
        // batch. Text-parsing cells (date/time/decimal/uuid) and
        // type-pairing checks run here; PASS 2 can then append without
        // mid-row failure.
        for (i, v) in row.iter().enumerate() {
            use IcebergType as IT;
            match (&self.builders[i], v) {
                (Builder::Int32(_), CellValue::Int64(x)) => {
                    if i32::try_from(*x).is_err() {
                        return Err(StorageError::Config(format!(
                            "write path: column {} value {:?} overflows int32",
                            self.columns[i].name, v
                        )));
                    }
                }
                (
                    Builder::Bool(_)
                    | Builder::Int32(_)
                    | Builder::Int64(_)
                    | Builder::Float32(_)
                    | Builder::Float64(_)
                    | Builder::Str(_)
                    | Builder::Binary(_)
                    | Builder::TsUs(_)
                    | Builder::Date32(_)
                    | Builder::Time64Us(_)
                    | Builder::Decimal(_)
                    | Builder::Uuid(_),
                    CellValue::Null,
                )
                | (Builder::Bool(_), CellValue::Bool(_))
                | (Builder::Int64(_), CellValue::Int64(_))
                | (Builder::Float32(_) | Builder::Float64(_), CellValue::Float64(_))
                | (Builder::Str(_), CellValue::Str(_))
                | (Builder::Binary(_), CellValue::Bytes(_))
                | (Builder::TsUs(_), CellValue::TimestampMicros(_)) => {}
                (Builder::Date32(_), CellValue::Str(x)) => {
                    parse_date_days(x).ok_or_else(|| {
                        StorageError::Config(format!("write path: bad date text {x:?}"))
                    })?;
                }
                (Builder::Time64Us(_), CellValue::Str(x)) => {
                    parse_time_us(x).ok_or_else(|| {
                        StorageError::Config(format!("write path: bad time text {x:?}"))
                    })?;
                }
                (Builder::Decimal(_), CellValue::Str(x)) => {
                    let (unscaled, text_scale) = parse_decimal_parts(x).ok_or_else(|| {
                        StorageError::Config(format!("write path: bad decimal text {x:?}"))
                    })?;
                    let (precision, scale) = match &self.columns[i].ty {
                        IT::Decimal { precision, scale } => (*precision, *scale),
                        other => unreachable!("decimal builder non-decimal column {other:?}"),
                    };
                    if text_scale > scale {
                        return Err(StorageError::Config(format!(
                            "write path: decimal {x:?} has {text_scale} fractional digits, \
                             column scale is {scale} (losing digits is not allowed)"
                        )));
                    }
                    let rescaled = unscaled
                        .checked_mul(10i128.pow(scale - text_scale))
                        .ok_or_else(|| {
                            StorageError::Config(format!(
                                "write path: decimal {x:?} overflows the column's scale"
                            ))
                        })?;
                    let max = 10i128.pow(precision);
                    if rescaled.abs() >= max {
                        return Err(StorageError::Config(format!(
                            "write path: decimal {x:?} exceeds precision {precision}"
                        )));
                    }
                }
                (Builder::Uuid(_), CellValue::Str(x)) => {
                    hex_uuid_bytes(x).ok_or_else(|| {
                        StorageError::Config(format!("write path: bad uuid text {x:?}"))
                    })?;
                }
                (b, v) => {
                    return Err(StorageError::Config(format!(
                        "write path: column {} got mismatched value {v:?} (builder {b:?})",
                        self.columns[i].name
                    )))
                }
            }
        }
        // PASS 2 — append; every possible failure was rejected above.
        for (i, v) in row.iter().enumerate() {
            match (&mut self.builders[i], v) {
                (Builder::Bool(b), CellValue::Bool(x)) => b.append_value(*x),
                (Builder::Bool(b), CellValue::Null) => b.append_null(),
                (Builder::Int32(b), CellValue::Int64(x)) if i32::try_from(*x).is_ok() => {
                    b.append_value(*x as i32);
                }
                (Builder::Int32(b), CellValue::Null) => b.append_null(),
                (Builder::Int64(b), CellValue::Int64(x)) => b.append_value(*x),
                (Builder::Int64(b), CellValue::Null) => b.append_null(),
                (Builder::Float32(b), CellValue::Float64(x)) => b.append_value(*x as f32),
                (Builder::Float32(b), CellValue::Null) => b.append_null(),
                (Builder::Float64(b), CellValue::Float64(x)) => b.append_value(*x),
                (Builder::Float64(b), CellValue::Null) => b.append_null(),
                (Builder::Str(b), CellValue::Str(x)) => b.append_value(x),
                (Builder::Str(b), CellValue::Null) => b.append_null(),
                (Builder::Binary(b), CellValue::Bytes(x)) => b.append_value(x),
                (Builder::Binary(b), CellValue::Null) => b.append_null(),
                (Builder::Date32(b), CellValue::Str(x)) => {
                    let days = parse_date_days(x).ok_or_else(|| {
                        StorageError::Config(format!("write path: bad date text {x:?}"))
                    })?;
                    b.append_value(days);
                }
                (Builder::Date32(b), CellValue::Null) => b.append_null(),
                (Builder::Time64Us(b), CellValue::Str(x)) => {
                    let us = parse_time_us(x).ok_or_else(|| {
                        StorageError::Config(format!("write path: bad time text {x:?}"))
                    })?;
                    b.append_value(us);
                }
                (Builder::Time64Us(b), CellValue::Null) => b.append_null(),
                (Builder::TsUs(b), CellValue::TimestampMicros(x)) => b.append_value(*x),
                (Builder::TsUs(b), CellValue::Null) => b.append_null(),
                (Builder::Decimal(b), CellValue::Str(x)) => {
                    // PASS 1 validated parse + scale reconciliation; redo
                    // the (cheap) conversion for the append
                    let (unscaled, text_scale) = parse_decimal_parts(x)
                        .ok_or_else(|| StorageError::Config("write path: bad decimal".into()))?;
                    let scale = match &self.columns[i].ty {
                        IcebergType::Decimal { scale, .. } => *scale,
                        other => unreachable!("non-decimal column {other:?}"),
                    };
                    b.append_value(unscaled * 10i128.pow(scale - text_scale));
                }
                (Builder::Decimal(b), CellValue::Null) => b.append_null(),
                (Builder::Uuid(b), CellValue::Str(x)) => {
                    let bytes = hex_uuid_bytes(x).ok_or_else(|| {
                        StorageError::Config(format!("write path: bad uuid text {x:?}"))
                    })?;
                    b.append_value(&bytes)
                        .map_err(|e| StorageError::Config(format!("write path: uuid: {e}")))?;
                }
                (Builder::Uuid(b), CellValue::Null) => {
                    b.append_null();
                }
                (b, v) => {
                    return Err(StorageError::Config(format!(
                        "write path: column {} got mismatched value {v:?} (builder {b:?})",
                        self.columns[i].name
                    )))
                }
            }
        }
        self.rows += 1;
        Ok(())
    }

    /// Drain accumulated rows into one parquet byte buffer.
    /// `flush_bytes` + per-column manifest stats (M3-R2): value/null
    /// counts and spec-serialized bounds from the finished arrays;
    /// `column_sizes` from the parquet footer (compressed bytes per
    /// column chunk, summed over row groups).
    pub fn flush_bytes_with_stats(&mut self) -> Result<(Vec<u8>, ColumnStats), StorageError> {
        let mut fields = Vec::with_capacity(self.columns.len());
        for c in &self.columns {
            let mut f = Field::new(&c.name, arrow_type(&c.ty)?, true);
            if let Some(id) = c.field_id {
                f = f.with_metadata(std::collections::HashMap::from([(
                    "PARQUET:field_id".to_string(),
                    id.to_string(),
                )]));
            }
            fields.push(f);
        }
        let schema = Arc::new(ArrowSchema::new(fields));
        let arrays: Vec<Arc<dyn arrow::array::Array>> = self
            .builders
            .iter_mut()
            .map(|b| match b {
                Builder::Bool(b) => Arc::new(b.finish()) as Arc<dyn arrow::array::Array>,
                Builder::Int32(b) => Arc::new(b.finish()),
                Builder::Int64(b) => Arc::new(b.finish()),
                Builder::Float32(b) => Arc::new(b.finish()),
                Builder::Float64(b) => Arc::new(b.finish()),
                Builder::Str(b) => Arc::new(b.finish()),
                Builder::Binary(b) => Arc::new(b.finish()),
                Builder::Date32(b) => Arc::new(b.finish()),
                Builder::Time64Us(b) => Arc::new(b.finish()),
                Builder::TsUs(b) => Arc::new(b.finish()),
                Builder::Decimal(b) => Arc::new(b.finish()),
                Builder::Uuid(b) => Arc::new(b.finish()),
            })
            .collect();
        let batch = arrow::record_batch::RecordBatch::try_new(schema.clone(), arrays.clone())
            .map_err(|e| StorageError::Config(format!("write path: batch build: {e}")))?;
        self.rows = 0;

        // ---- M3-R2 stats (keyed by FIELD ID; columns without one are skipped) ----
        let mut stats = ColumnStats::default();
        for (i, c) in self.columns.iter().enumerate() {
            let Some(id) = c.field_id else { continue };
            let arr = &arrays[i];
            stats.value_counts.insert(id, arr.len() as i64);
            stats.null_value_counts.insert(id, arr.null_count() as i64);
            if let Some((lo, hi)) = column_bounds(&c.ty, arr) {
                stats.lower_bounds.insert(id, lo);
                stats.upper_bounds.insert(id, hi);
            }
        }

        let mut out = Vec::with_capacity(64 * 1024);
        {
            use parquet::arrow::ArrowWriter;
            let mut w = ArrowWriter::try_new(&mut out, schema, None)
                .map_err(|e| StorageError::Config(format!("write path: parquet: {e}")))?;
            w.write(&batch)
                .map_err(|e| StorageError::Config(format!("write path: parquet write: {e}")))?;
            let footer = w
                .close()
                .map_err(|e| StorageError::Config(format!("write path: parquet close: {e}")))?;
            // ArrowWriter::close() hands back the thrift FileMetaData:
            // row_groups -> columns -> meta_data.total_compressed_size
            for rg in &footer.row_groups {
                for (ci, col) in rg.columns.iter().enumerate() {
                    let Some(meta) = &col.meta_data else {
                        continue;
                    };
                    // schema field order == self.columns order (fields built above)
                    if let Some(id) = self.columns.get(ci).and_then(|c| c.field_id) {
                        *stats.column_sizes.entry(id).or_insert(0) += meta.total_compressed_size;
                    }
                }
            }
        }
        Ok((out, stats))
    }

    /// Write accumulated rows as one file under `key_prefix` (a /data/
    /// directory key), returning the identity of the written object. The
    /// name carries a random per-flush UUID: equal-size batches from
    /// different transactions MUST NOT collide on one key, or the second
    /// PUT silently overwrites bytes the first snapshot still references
    /// (13-implementation-review blocker #1). The uuid is also the orphan
    /// GC identity (M4).
    pub fn write_file(
        &mut self,
        io: &StorageIo,
        key_prefix: &str,
        seq: usize,
    ) -> Result<WrittenFile, StorageError> {
        let rows = self.rows;
        let (bytes, stats) = self.flush_bytes_with_stats()?;
        let size = bytes.len() as i64;
        let flush_id = uuid::Uuid::new_v4();
        let key = format!(
            "{}/{}-{}-{}.parquet",
            key_prefix.trim_end_matches('/'),
            rows,
            flush_id,
            seq
        );
        io.put_bytes(&key, bytes)?;
        Ok(WrittenFile {
            key,
            record_count: rows as i64,
            file_size_in_bytes: size,
            column_sizes: stats.column_sizes,
            value_counts: stats.value_counts,
            null_value_counts: stats.null_value_counts,
            lower_bounds: stats.lower_bounds,
            upper_bounds: stats.upper_bounds,
        })
    }
}

/// One cell of the FFI write row format (M2 slice 27b): the C side sends
/// typed JSON so no text formatting happens on the PG side. Bare values
/// map by column type (numbers, bools, strings); objects carry the
/// integer-typed forms the datum arithmetic gives us for free:
/// {"d": days-since-epoch} date, {"t": micros} time, {"ts": micros}
/// timestamp(tz), {"dec": "text"} decimal, {"u": "uuid"} uuid,
/// {"b": "\\x hex"} binary.
pub fn parse_row_json(
    columns: &[WriteColumn],
    row: &serde_json::Value,
) -> Result<Vec<CellValue>, StorageError> {
    use IcebergType as IT;
    let arr = row
        .as_array()
        .ok_or_else(|| StorageError::Config("write path: row is not a JSON array".into()))?;
    if arr.len() != columns.len() {
        return Err(StorageError::Config(format!(
            "write path: row has {} cells, schema has {} columns",
            arr.len(),
            columns.len()
        )));
    }
    let mut out = Vec::with_capacity(arr.len());
    for (col, cell) in columns.iter().zip(arr) {
        let cv = match (&col.ty, cell) {
            (_, serde_json::Value::Null) => CellValue::Null,
            (IT::Boolean, serde_json::Value::Bool(b)) => CellValue::Bool(*b),
            (IT::Int | IT::Long, serde_json::Value::Number(n)) => CellValue::Int64(
                n.as_i64()
                    .ok_or_else(|| StorageError::Config(format!("write path: bad int {n}")))?,
            ),
            (IT::Float | IT::Double, serde_json::Value::Number(n)) => CellValue::Float64(
                n.as_f64()
                    .ok_or_else(|| StorageError::Config(format!("write path: bad float {n}")))?,
            ),
            (IT::String, serde_json::Value::String(v)) => CellValue::Str(v.clone()),
            (IT::Date, serde_json::Value::Object(o)) if o.contains_key("d") => {
                let days = o
                    .get("d")
                    .and_then(serde_json::Value::as_i64)
                    .ok_or_else(|| StorageError::Config("write path: bad date cell".into()))?;
                CellValue::Str(format_date_days(days))
            }
            (IT::Time, serde_json::Value::Object(o)) if o.contains_key("t") => {
                let us = o
                    .get("t")
                    .and_then(serde_json::Value::as_i64)
                    .ok_or_else(|| StorageError::Config("write path: bad time cell".into()))?;
                CellValue::Str(format_time_us(us))
            }
            (IT::Timestamp | IT::Timestamptz, serde_json::Value::Object(o))
                if o.contains_key("ts") =>
            {
                let micros = o
                    .get("ts")
                    .and_then(serde_json::Value::as_i64)
                    .ok_or_else(|| StorageError::Config("write path: bad timestamp cell".into()))?;
                CellValue::TimestampMicros(micros)
            }
            (IT::Decimal { .. }, serde_json::Value::Object(o)) if o.contains_key("dec") => {
                let text = o
                    .get("dec")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| StorageError::Config("write path: bad decimal cell".into()))?;
                CellValue::Str(text.to_string())
            }
            (IT::Uuid, serde_json::Value::Object(o)) if o.contains_key("u") => {
                let text = o
                    .get("u")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| StorageError::Config("write path: bad uuid cell".into()))?;
                CellValue::Str(text.to_string())
            }
            (IT::Binary, serde_json::Value::Object(o)) if o.contains_key("b") => {
                let text = o
                    .get("b")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| StorageError::Config("write path: bad binary cell".into()))?;
                let hex = text.strip_prefix("\\x").ok_or_else(|| {
                    StorageError::Config(format!(
                        "write path: binary cell must be \\x-prefixed: {text:?}"
                    ))
                })?;
                if hex.len() % 2 != 0 {
                    return Err(StorageError::Config(format!(
                        "write path: bad binary hex length: {text:?}"
                    )));
                }
                let mut bytes = Vec::with_capacity(hex.len() / 2);
                for i in (0..hex.len()).step_by(2) {
                    bytes.push(u8::from_str_radix(&hex[i..i + 2], 16).map_err(|_| {
                        StorageError::Config(format!("write path: bad binary hex: {text:?}"))
                    })?);
                }
                CellValue::Bytes(bytes)
            }
            (ty, v) => {
                return Err(StorageError::Config(format!(
                    "write path: column {} (type {ty:?}) got unexpected cell {v}",
                    col.name
                )))
            }
        };
        out.push(cv);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::parse_location;

    fn cols() -> Vec<WriteColumn> {
        vec![
            WriteColumn {
                name: "id".into(),
                ty: IcebergType::Long,
                field_id: None,
            },
            WriteColumn {
                name: "name".into(),
                ty: IcebergType::String,
                field_id: None,
            },
            WriteColumn {
                name: "at".into(),
                ty: IcebergType::Timestamp,
                field_id: None,
            },
        ]
    }

    /// 13-review #8b: decimal text reconciles to the COLUMN's scale —
    /// the textual fractional digits used to become the unscaled value
    /// verbatim, silently rescaling the number (123.45 into a scale-3
    /// column read back as 12.345).
    #[test]
    fn decimal_scale_reconciles_to_column() {
        let cols = vec![WriteColumn {
            name: "amt".into(),
            ty: IcebergType::Decimal {
                precision: 10,
                scale: 3,
            },
            field_id: None,
        }];
        let dir = tempfile::tempdir().unwrap();
        crate::storage::test_allow_local_stores();
        let io = StorageIo::open(
            &parse_location(&format!("file://{}", dir.path().display())).unwrap(),
            None,
        )
        .unwrap();
        let mut w = BatchWriter::new(cols.clone()).unwrap();
        w.push(&[CellValue::Str("123.45".into())]).unwrap();
        let wf = w.write_file(&io, "table/data", 0).unwrap();
        let mut rows = crate::parquet::ParquetRows::open(&io, &wf.key, &["amt"]).unwrap();
        assert!(rows.next());
        match rows.value(0) {
            CellValue::Str(txt) => {
                assert_eq!(txt, "123.450", "value must land at scale 3");
            }
            other => panic!("expected decimal text, got {other:?}"),
        }
        // more fractional digits than the target scale: loud error, no
        // silent truncation
        let mut w2 = BatchWriter::new(cols).unwrap();
        assert!(w2.push(&[CellValue::Str("1.2345".into())]).is_err());
    }

    /// 13-review #8b: the WHOLE row validates before any builder mutates —
    /// a bad cell in the middle used to leave earlier builders advanced,
    /// misaligning every subsequent row in the batch.
    #[test]
    fn failed_row_does_not_corrupt_the_batch() {
        let dir = tempfile::tempdir().unwrap();
        crate::storage::test_allow_local_stores();
        let io = StorageIo::open(
            &parse_location(&format!("file://{}", dir.path().display())).unwrap(),
            None,
        )
        .unwrap();
        let mut w = BatchWriter::new(cols()).unwrap();
        // row 1 OK
        w.push(&[
            CellValue::Int64(1),
            CellValue::Str("ok".into()),
            CellValue::TimestampMicros(1),
        ])
        .unwrap();
        // row 2 has a BAD middle cell (int in a string column)
        assert!(w
            .push(&[
                CellValue::Int64(2),
                CellValue::Int64(99),
                CellValue::TimestampMicros(2),
            ])
            .is_err());
        // row 3 must land correctly aligned
        w.push(&[
            CellValue::Int64(3),
            CellValue::Str("after".into()),
            CellValue::TimestampMicros(3),
        ])
        .unwrap();
        let wf = w.write_file(&io, "table/data", 0).unwrap();
        assert_eq!(wf.record_count, 2, "failed row contributes nothing");
        let mut rows =
            crate::parquet::ParquetRows::open(&io, &wf.key, &["id", "name", "at"]).unwrap();
        assert!(rows.next());
        assert_eq!(rows.value(0), CellValue::Int64(1));
        assert!(rows.next());
        assert_eq!(rows.value(0), CellValue::Int64(3));
        assert_eq!(rows.value(1), CellValue::Str("after".into()));
        assert!(!rows.next());
    }

    /// Perf ground truth (13-review robustness question: is row-JSON the
    /// write bottleneck, or is MessagePack/a binary format warranted?).
    /// #[ignore]d — timing is machine-relative; run explicitly:
    ///   cargo test -p pgdatalake-core --lib -- --ignored bench --nocapture
    #[test]
    #[ignore = "manual throughput benchmark; run explicitly"]
    #[expect(clippy::cast_precision_loss, reason = "test-only metrics formatting")]
    fn bench_write_push_json_throughput() {
        let cols = vec![
            WriteColumn {
                name: "id".into(),
                ty: IcebergType::Long,
                field_id: None,
            },
            WriteColumn {
                name: "name".into(),
                ty: IcebergType::String,
                field_id: None,
            },
            WriteColumn {
                name: "at".into(),
                ty: IcebergType::Timestamptz,
                field_id: None,
            },
            WriteColumn {
                name: "amt".into(),
                ty: IcebergType::Decimal {
                    precision: 10,
                    scale: 3,
                },
                field_id: None,
            },
            WriteColumn {
                name: "flag".into(),
                ty: IcebergType::Boolean,
                field_id: None,
            },
        ];
        let rows_json: Vec<String> = (0..100_000)
            .map(|i| {
                let amount = format!("{}.{}", i % 10_000, i % 1_000);
                format!(
                    "[{},\"row-{i}\",{{\"ts\":{}}},{{\"dec\":\"{amount}\"}},{}]",
                    i,
                    1_700_000_000_000_000u64 + i,
                    i % 2 == 0
                )
            })
            .collect();
        let mut w = BatchWriter::new(cols.clone()).unwrap();
        // full pipeline per row, exactly what the C side pays today:
        // JSON text -> serde DOM -> typed cells -> arrow builders
        let t0 = std::time::Instant::now();
        for r in &rows_json {
            let v: serde_json::Value = serde_json::from_str(r).unwrap();
            let cells = parse_row_json(&cols, &v).unwrap();
            w.push(&cells).unwrap();
        }
        let dt = t0.elapsed();
        let nrows = rows_json.len();
        // arrow-builder append alone (any format must pay this)
        drop(w);
        let mut w2 = BatchWriter::new(cols.clone()).unwrap();
        let t1 = std::time::Instant::now();
        for i in 0..nrows as i64 {
            w2.push(&[
                CellValue::Int64(i),
                CellValue::Str(format!("row-{i}")),
                CellValue::TimestampMicros(1_700_000_000_000_000 + i),
                CellValue::Str(format!("{}.{}", i % 10_000, i % 1_000)),
                CellValue::Bool(i % 2 == 0),
            ])
            .unwrap();
        }
        let builder_dt = t1.elapsed();
        println!(
            "write staging JSON pipeline: {} rows in {:?} = {:.0} rows/s \
             ({:.2}M cells/s); arrow-append-only floor: {:?} = {:.0} rows/s",
            nrows,
            dt,
            nrows as f64 / dt.as_secs_f64(),
            nrows as f64 * 5.0 / dt.as_secs_f64() / 1e6,
            builder_dt,
            nrows as f64 / builder_dt.as_secs_f64(),
        );
    }

    /// TDD anchor (M2 slice 24): write -> read round-trip through OUR reader,
    /// including NULLs.
    #[test]
    fn write_read_roundtrip_with_nulls() {
        let dir = tempfile::tempdir().unwrap();
        let io = StorageIo::open(
            &parse_location(&format!("file://{}", dir.path().display())).unwrap(),
            None,
        )
        .unwrap();

        let mut w = BatchWriter::new(cols()).unwrap();
        w.push(&[
            CellValue::Int64(1),
            CellValue::Str("alpha".into()),
            CellValue::TimestampMicros(1_700_000_000_000_000),
        ])
        .unwrap();
        w.push(&[CellValue::Int64(2), CellValue::Null, CellValue::Null])
            .unwrap();
        assert_eq!(w.num_rows(), 2);

        let wf = w.write_file(&io, "table/data", 0).unwrap();
        assert!(wf.key.starts_with("table/data/2-"), "{}", wf.key);
        assert_eq!(wf.record_count, 2);
        assert!(wf.file_size_in_bytes > 0);

        let mut rows =
            crate::parquet::ParquetRows::open(&io, &wf.key, &["id", "name", "at"]).unwrap();
        assert!(rows.next());
        assert_eq!(rows.value(0), CellValue::Int64(1));
        assert_eq!(rows.value(1), CellValue::Str("alpha".into()));
        assert_eq!(
            rows.value(2),
            CellValue::TimestampMicros(1_700_000_000_000_000)
        );
        assert!(rows.next());
        assert_eq!(rows.value(1), CellValue::Null);
        assert!(!rows.next());
    }

    /// Type-matrix round trip: bool/int32/float/date/time/decimal/uuid/
    /// binary all survive write->read as canonical reader forms.
    #[test]
    fn write_read_type_matrix_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let io = StorageIo::open(
            &parse_location(&format!("file://{}", dir.path().display())).unwrap(),
            None,
        )
        .unwrap();

        let mut w = BatchWriter::new(vec![
            WriteColumn {
                name: "b".into(),
                ty: IcebergType::Boolean,
                field_id: None,
            },
            WriteColumn {
                name: "i".into(),
                ty: IcebergType::Int,
                field_id: None,
            },
            WriteColumn {
                name: "f".into(),
                ty: IcebergType::Float,
                field_id: None,
            },
            WriteColumn {
                name: "d".into(),
                ty: IcebergType::Double,
                field_id: None,
            },
            WriteColumn {
                name: "day".into(),
                ty: IcebergType::Date,
                field_id: None,
            },
            WriteColumn {
                name: "t".into(),
                ty: IcebergType::Time,
                field_id: None,
            },
            WriteColumn {
                name: "dec".into(),
                ty: IcebergType::Decimal {
                    precision: 12,
                    scale: 2,
                },
                field_id: None,
            },
            WriteColumn {
                name: "u".into(),
                ty: IcebergType::Uuid,
                field_id: None,
            },
            WriteColumn {
                name: "bin".into(),
                ty: IcebergType::Binary,
                field_id: None,
            },
        ])
        .unwrap();
        w.push(&[
            CellValue::Bool(true),
            CellValue::Int64(-42),
            CellValue::Float64(1.5),
            CellValue::Float64(-2.25),
            CellValue::Str("2026-11-19".into()),
            CellValue::Str("10:10:10.123456".into()),
            CellValue::Str("1234567890.12".into()),
            CellValue::Str("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11".into()),
            CellValue::Bytes(vec![0xde, 0xad]),
        ])
        .unwrap();
        let wf = w.write_file(&io, "t/data", 0).unwrap();

        let mut rows = crate::parquet::ParquetRows::open(
            &io,
            &wf.key,
            &["b", "i", "f", "d", "day", "t", "dec", "u", "bin"],
        )
        .unwrap();
        assert!(rows.next());
        assert_eq!(rows.value(0), CellValue::Bool(true));
        assert_eq!(rows.value(1), CellValue::Int64(-42));
        assert_eq!(rows.value(3), CellValue::Float64(-2.25));
        assert_eq!(rows.value(4), CellValue::Str("2026-11-19".into()));
        assert_eq!(rows.value(5), CellValue::Str("10:10:10.123456".into()));
        assert_eq!(rows.value(6), CellValue::Str("1234567890.12".into()));
        assert_eq!(
            rows.value(7),
            CellValue::Str("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11".into())
        );
        assert_eq!(rows.value(8), CellValue::Str("\\xdead".into()));
        assert!(!rows.next());
    }

    /// Row-JSON cells map to canonical `CellValues` per column type (27b).
    #[test]
    fn parse_row_json_typed_cells() {
        use serde_json::json;
        let cols = vec![
            WriteColumn {
                name: "i".into(),
                ty: IcebergType::Long,
                field_id: None,
            },
            WriteColumn {
                name: "s".into(),
                ty: IcebergType::String,
                field_id: None,
            },
            WriteColumn {
                name: "d".into(),
                ty: IcebergType::Date,
                field_id: None,
            },
            WriteColumn {
                name: "ts".into(),
                ty: IcebergType::Timestamptz,
                field_id: None,
            },
            WriteColumn {
                name: "dec".into(),
                ty: IcebergType::Decimal {
                    precision: 12,
                    scale: 2,
                },
                field_id: None,
            },
            WriteColumn {
                name: "u".into(),
                ty: IcebergType::Uuid,
                field_id: None,
            },
            WriteColumn {
                name: "b".into(),
                ty: IcebergType::Binary,
                field_id: None,
            },
        ];
        let row = parse_row_json(
            &cols,
            &json!([5, "hi", {"d": 20000}, {"ts": 1_700_000_000_000_000i64},
                    {"dec": "12.34"}, {"u": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"},
                    {"b": "\\xdead"}]),
        )
        .unwrap();
        assert_eq!(row[0], CellValue::Int64(5));
        assert_eq!(row[1], CellValue::Str("hi".into()));
        assert_eq!(row[2], CellValue::Str("2024-10-04".into()));
        assert_eq!(row[3], CellValue::TimestampMicros(1_700_000_000_000_000));
        assert_eq!(row[4], CellValue::Str("12.34".into()));
        assert_eq!(
            row[5],
            CellValue::Str("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11".into())
        );
        assert_eq!(row[6], CellValue::Bytes(vec![0xde, 0xad]));

        // NULL + arity + type mismatches
        let row = parse_row_json(
            &cols,
            &json!([null, "", {"d": 0}, {"ts": 0}, {"dec": "0"}, {"u": "x"}, {"b": "\\x00"}]),
        )
        .unwrap();
        assert_eq!(row[0], CellValue::Null);
        assert!(parse_row_json(&cols, &json!([1, 2])).is_err());
        assert!(parse_row_json(
            &cols,
            &json!(["x", "", {"d": 0}, {"ts": 0}, {"dec": "0"}, {"u": "x"}, {"b": "\\x00"}])
        )
        .is_err());
    }

    /// Date formatter round-trips through `parse_date_days` (Hinnant
    /// `civil_from_days` / `days_from_civil` are inverses).
    #[test]
    fn format_date_roundtrips() {
        for (y, m, d) in [(1970, 1, 1), (2024, 10, 4), (2000, 2, 29), (1969, 12, 31)] {
            let days = days_from_civil(y, m, d);
            let text = format_date_days(days);
            assert_eq!(text, format!("{y:04}-{m:02}-{d:02}"), "{y}-{m}-{d}");
            assert_eq!(parse_date_days(&text), Some(days as i32));
        }
    }

    /// The accumulator accepts many rows before one flush (insert batching).
    #[test]
    fn flush_drains_accumulated_rows() {
        let dir = tempfile::tempdir().unwrap();
        let io = StorageIo::open(
            &parse_location(&format!("file://{}", dir.path().display())).unwrap(),
            None,
        )
        .unwrap();
        let mut w = BatchWriter::new(cols()).unwrap();
        for i in 0..1000i64 {
            w.push(&[CellValue::Int64(i), CellValue::Null, CellValue::Null])
                .unwrap();
        }
        let wf = w.write_file(&io, "t/data", 7).unwrap();
        assert_eq!(wf.record_count, 1000);
        let mut rows = crate::parquet::ParquetRows::open(&io, &wf.key, &["id"]).unwrap();
        let mut seen = 0;
        while rows.next() {
            seen += 1;
        }
        assert_eq!(seen, 1000);
    }

    /// M3-R2 (13-review: pruning inert on our tables): `write_file` must
    /// collect per-column stats — value/null counts, min/max bounds
    /// serialized per the Iceberg single-value table (LE ints/floats,
    /// UTF-8 strings) keyed by FIELD ID — so the manifest entries carry
    /// prunable bounds. Byte-exact expectations.
    #[test]
    fn write_file_collects_column_stats() {
        use crate::catalog::metadata::IcebergType as IT;
        use crate::storage::{parse_location, StorageIo};
        let dir = tempfile::tempdir().unwrap();
        crate::storage::test_allow_local_stores();
        let loc = format!("file://{}", dir.path().display());
        let io = StorageIo::open(&parse_location(&loc).unwrap(), None).unwrap();
        std::fs::create_dir_all(dir.path().join("data")).unwrap();

        let mut w = BatchWriter::new(vec![
            WriteColumn {
                name: "id".into(),
                ty: IT::Long,
                field_id: Some(1),
            },
            WriteColumn {
                name: "score".into(),
                ty: IT::Double,
                field_id: Some(2),
            },
            WriteColumn {
                name: "name".into(),
                ty: IT::String,
                field_id: Some(3),
            },
        ])
        .unwrap();
        w.push(&[
            CellValue::Int64(10),
            CellValue::Null,
            CellValue::Str("zebra".into()),
        ])
        .unwrap();
        w.push(&[
            CellValue::Int64(2),
            CellValue::Float64(1.5),
            CellValue::Str("apple".into()),
        ])
        .unwrap();
        w.push(&[
            CellValue::Int64(7),
            CellValue::Float64(9.25),
            CellValue::Str("mango".into()),
        ])
        .unwrap();

        let wf = w
            .write_file(&io, &format!("{}/data", dir.path().display()), 0)
            .unwrap();

        // counts: 3 values each, one null in `score`... (null is in col 2? col "score" row0)
        assert_eq!(wf.value_counts[&1], 3);
        assert_eq!(wf.value_counts[&3], 3);
        // score has one NULL (row 0) — excluded from bounds, counted here
        assert_eq!(wf.null_value_counts[&2], 1);
        assert_eq!(wf.null_value_counts[&1], 0);
        assert_eq!(wf.null_value_counts[&3], 0);

        // bounds: id in [2, 10], score in [1.5, 9.25], name in ["apple", "zebra"]
        assert_eq!(wf.lower_bounds[&1], 2i64.to_le_bytes().to_vec());
        assert_eq!(wf.upper_bounds[&1], 10i64.to_le_bytes().to_vec());
        assert_eq!(wf.lower_bounds[&2], 1.5f64.to_le_bytes().to_vec());
        assert_eq!(wf.upper_bounds[&2], 9.25f64.to_le_bytes().to_vec());
        assert_eq!(wf.lower_bounds[&3], b"apple".to_vec());
        assert_eq!(wf.upper_bounds[&3], b"zebra".to_vec());

        // column_sizes present for every field and positive (footer-derived)
        for id in [1i64, 2, 3] {
            assert!(*wf.column_sizes.get(&id).unwrap_or(&0) > 0, "col {id} size");
        }
    }

    /// Nulls excluded from bounds, all-null columns omit bounds entirely,
    /// and NaN floats never poison min/max (spec: bounds skip NaN).
    #[test]
    fn write_file_stats_handle_nulls_and_nan() {
        use crate::catalog::metadata::IcebergType as IT;
        use crate::storage::{parse_location, StorageIo};
        let dir = tempfile::tempdir().unwrap();
        crate::storage::test_allow_local_stores();
        let loc = format!("file://{}", dir.path().display());
        let io = StorageIo::open(&parse_location(&loc).unwrap(), None).unwrap();
        std::fs::create_dir_all(dir.path().join("data")).unwrap();

        let mut w = BatchWriter::new(vec![
            WriteColumn {
                name: "x".into(),
                ty: IT::Double,
                field_id: Some(1),
            },
            WriteColumn {
                name: "gone".into(),
                ty: IT::Int,
                field_id: Some(2),
            },
        ])
        .unwrap();
        w.push(&[CellValue::Float64(f64::NAN), CellValue::Null])
            .unwrap();
        w.push(&[CellValue::Float64(4.0), CellValue::Null]).unwrap();
        let wf = w
            .write_file(&io, &format!("{}/data", dir.path().display()), 0)
            .unwrap();

        assert_eq!(wf.null_value_counts[&2], 2, "all-null int column");
        assert!(
            !wf.lower_bounds.contains_key(&2),
            "all-null column omits bounds"
        );
        assert_eq!(
            wf.lower_bounds[&1],
            4.0f64.to_le_bytes().to_vec(),
            "NaN excluded from min; 4.0 is the lower bound"
        );
        assert_eq!(wf.upper_bounds[&1], 4.0f64.to_le_bytes().to_vec());
    }
}
