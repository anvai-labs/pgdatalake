//! Parquet writer: columnar batch accumulator -> parquet bytes -> object
//! storage (specs/lakehouse/06-execution.md, M2 slice 24). This is the file
//! half of the managed-write path: the M2 commit protocol (slice 25) turns
//! the returned file list into an Iceberg snapshot commit.
//!
//! Types mirror the read side (`CellValue`) so the TableAM insert path
//! (slice 27) can feed rows without a second representation. Canonical TEXT
//! forms (dates, times, decimals, uuids) are the same ones the reader
//! produces — round-trip stability by construction.

use crate::catalog::metadata::IcebergType;
use crate::parquet::CellValue;
use crate::storage::{StorageError, StorageIo};

use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use std::sync::Arc;

/// One column of the write schema.
#[derive(Debug, Clone)]
pub struct WriteColumn {
    pub name: String,
    pub ty: IcebergType,
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

/// Howard Hinnant's days_from_civil (inverse of civil_from_days).
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64; // [0, 399]
    let mp = if m > 2 { m - 3 } else { m + 9 } as u64; // [3, 14]
    let doy = (153 * mp + 2) / 5 + d as u64 - 1; // [0, 365]
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
fn parse_decimal_unscaled(s: &str) -> Option<i128> {
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
        unscaled += (c as u8 - b'0') as i128;
    }
    Some(sign * unscaled)
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
        for (i, v) in row.iter().enumerate() {
            match (&mut self.builders[i], v) {
                (Builder::Bool(b), CellValue::Bool(x)) => b.append_value(*x),
                (Builder::Bool(b), CellValue::Null) => b.append_null(),
                (Builder::Int32(b), CellValue::Int64(x))
                    if *x >= i32::MIN as i64 && *x <= i32::MAX as i64 =>
                {
                    b.append_value(*x as i32)
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
                    b.append_value(days)
                }
                (Builder::Date32(b), CellValue::Null) => b.append_null(),
                (Builder::Time64Us(b), CellValue::Str(x)) => {
                    let us = parse_time_us(x).ok_or_else(|| {
                        StorageError::Config(format!("write path: bad time text {x:?}"))
                    })?;
                    b.append_value(us)
                }
                (Builder::Time64Us(b), CellValue::Null) => b.append_null(),
                (Builder::TsUs(b), CellValue::TimestampMicros(x)) => b.append_value(*x),
                (Builder::TsUs(b), CellValue::Null) => b.append_null(),
                (Builder::Decimal(b), CellValue::Str(x)) => {
                    let unscaled = parse_decimal_unscaled(x).ok_or_else(|| {
                        StorageError::Config(format!("write path: bad decimal text {x:?}"))
                    })?;
                    b.append_value(unscaled)
                }
                (Builder::Decimal(b), CellValue::Null) => b.append_null(),
                (Builder::Uuid(b), CellValue::Str(x)) => {
                    let bytes = hex_uuid_bytes(x).ok_or_else(|| {
                        StorageError::Config(format!("write path: bad uuid text {x:?}"))
                    })?;
                    b.append_value(&bytes)
                        .map_err(|e| StorageError::Config(format!("write path: uuid: {e}")))?
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
    pub fn flush_bytes(&mut self) -> Result<Vec<u8>, StorageError> {
        let mut fields = Vec::with_capacity(self.columns.len());
        for c in &self.columns {
            fields.push(Field::new(&c.name, arrow_type(&c.ty)?, true));
        }
        let schema = Arc::new(ArrowSchema::new(fields));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            self.builders
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
                .collect(),
        )
        .map_err(|e| StorageError::Config(format!("write path: batch build: {e}")))?;
        self.rows = 0;

        let mut out = Vec::with_capacity(64 * 1024);
        {
            use parquet::arrow::ArrowWriter;
            let mut w = ArrowWriter::try_new(&mut out, schema, None)
                .map_err(|e| StorageError::Config(format!("write path: parquet: {e}")))?;
            w.write(&batch)
                .map_err(|e| StorageError::Config(format!("write path: parquet write: {e}")))?;
            w.close()
                .map_err(|e| StorageError::Config(format!("write path: parquet close: {e}")))?;
        }
        Ok(out)
    }

    /// Write accumulated rows as one file under `key_prefix` (a /data/
    /// directory key), returning the object key. File naming is
    /// deterministic per M4's orphan-GC rule: <prefix>/<rows>-<seq>.parquet.
    pub fn write_file(
        &mut self,
        io: &StorageIo,
        key_prefix: &str,
        seq: usize,
    ) -> Result<String, StorageError> {
        let rows = self.rows;
        let bytes = self.flush_bytes()?;
        let key = format!(
            "{}/{}-{}.parquet",
            key_prefix.trim_end_matches('/'),
            rows,
            seq
        );
        io.put_bytes(&key, bytes)?;
        Ok(key)
    }
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
            },
            WriteColumn {
                name: "name".into(),
                ty: IcebergType::String,
            },
            WriteColumn {
                name: "at".into(),
                ty: IcebergType::Timestamp,
            },
        ]
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

        let key = w.write_file(&io, "table/data", 0).unwrap();
        assert!(key.starts_with("table/data/2-0.parquet"), "{key}");

        let mut rows = crate::parquet::ParquetRows::open(&io, &key, &["id", "name", "at"]).unwrap();
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
            },
            WriteColumn {
                name: "i".into(),
                ty: IcebergType::Int,
            },
            WriteColumn {
                name: "f".into(),
                ty: IcebergType::Float,
            },
            WriteColumn {
                name: "d".into(),
                ty: IcebergType::Double,
            },
            WriteColumn {
                name: "day".into(),
                ty: IcebergType::Date,
            },
            WriteColumn {
                name: "t".into(),
                ty: IcebergType::Time,
            },
            WriteColumn {
                name: "dec".into(),
                ty: IcebergType::Decimal {
                    precision: 12,
                    scale: 2,
                },
            },
            WriteColumn {
                name: "u".into(),
                ty: IcebergType::Uuid,
            },
            WriteColumn {
                name: "bin".into(),
                ty: IcebergType::Binary,
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
        let key = w.write_file(&io, "t/data", 0).unwrap();

        let mut rows = crate::parquet::ParquetRows::open(
            &io,
            &key,
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
        let key = w.write_file(&io, "t/data", 7).unwrap();
        assert!(key.contains("1000-7.parquet"), "{key}");
        let mut rows = crate::parquet::ParquetRows::open(&io, &key, &["id"]).unwrap();
        let mut seen = 0;
        while rows.next() {
            seen += 1;
        }
        assert_eq!(seen, 1000);
    }
}
