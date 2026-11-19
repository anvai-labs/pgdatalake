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
    Float64(f64),
    Str(String),
    Bytes(Vec<u8>),
}

/// Projected row iterator over one parquet object.
#[derive(Debug)]
pub struct ParquetRows {
    batches: std::vec::IntoIter<arrow::record_batch::RecordBatch>,
    current: Option<arrow::record_batch::RecordBatch>,
    row: usize,
    column_names: Vec<String>,
    column_indices: Vec<usize>,
    num_rows_in_current: usize,
}

impl ParquetRows {
    /// Open a parquet object with a projection (column names, in the
    /// requested order). Unknown columns are a Config error (doc 03 §5:
    /// schema drift surfaces cleanly).
    pub fn open(io: &StorageIo, key: &str, projection: &[&str]) -> Result<Self, StorageError> {
        let raw = io.get_bytes(key)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(raw))
            .map_err(|e| StorageError::Decode(format!("parquet open: {e}")))?;
        let arrow_schema = builder.schema().clone();

        let mut column_indices = Vec::with_capacity(projection.len());
        for name in projection {
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

        let reader = builder
            .build()
            .map_err(|e| StorageError::Decode(format!("parquet reader: {e}")))?;
        let batches: Vec<arrow::record_batch::RecordBatch> = reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| StorageError::Decode(format!("parquet read: {e}")))?;

        let column_names = projection.iter().map(|s| s.to_string()).collect();
        Ok(Self {
            num_rows_in_current: batches.first().map(|b| b.num_rows()).unwrap_or(0),
            batches: batches.into_iter(),
            current: None,
            row: 0,
            column_names,
            column_indices,
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
            num_rows_in_current: batches.first().map(|b| b.num_rows()).unwrap_or(0),
            batches: batches.into_iter(),
            current: None,
            row: 0,
            column_names: names,
            column_indices: indices,
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
                return true;
            }
            match self.batches.next() {
                Some(batch) => {
                    self.num_rows_in_current = batch.num_rows();
                    self.current = Some(batch);
                    self.row = 0;
                    if self.num_rows_in_current == 0 {
                        continue;
                    }
                    return true;
                }
                None => return false,
            }
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
        DT::Int32 | DT::Date32 => down(col)
            .map(|v| CellValue::Int64(v as i64))
            .unwrap_or(CellValue::Null),
        DT::Int64
        | DT::Date64
        | DT::Time64(arrow::datatypes::TimeUnit::Microsecond)
        | DT::Timestamp(_, _) => col
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| CellValue::Int64(a.value(row)))
            .unwrap_or(CellValue::Null),
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
        DT::Utf8 | DT::LargeUtf8 => col
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| CellValue::Str(a.value(row).to_string()))
            .unwrap_or(CellValue::Null),
        DT::Binary | DT::LargeBinary => col
            .as_any()
            .downcast_ref::<BinaryArray>()
            .map(|a| CellValue::Bytes(a.value(row).to_vec()))
            .unwrap_or(CellValue::Null),
        // nested types land with the type-matrix slice (doc 05)
        _ => CellValue::Null,
    }
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
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("alpha"), None, Some("gamma")])),
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
        write_fixture(dir.path(), "a.parquet");
        let io = StorageIo::open(
            &parse_location(&format!("file://{}", dir.path().display())).unwrap(),
            None,
        )
        .unwrap();

        let mut rows = ParquetRows::open(&io, "a.parquet", &["id", "name"]).unwrap();
        assert_eq!(rows.column_names(), ["id", "name"]);

        assert!(rows.next(), "row 0");
        assert_eq!(rows.value(0), CellValue::Int64(1));
        assert_eq!(rows.value(1), CellValue::Str("alpha".into()));

        assert!(rows.next(), "row 1");
        assert_eq!(rows.value(0), CellValue::Int64(2));
        assert_eq!(rows.value(1), CellValue::Null);

        assert!(rows.next(), "row 2");
        assert_eq!(rows.value(0), CellValue::Int64(3));
        assert_eq!(rows.value(1), CellValue::Str("gamma".into()));

        assert!(!rows.next(), "exhausted");
    }

    /// Projection: only the named columns come back, in the requested order.
    #[test]
    fn projection_selects_and_orders_columns() {
        let dir = tempfile::tempdir().unwrap();
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
        write_fixture(dir.path(), "a.parquet");
        let io = StorageIo::open(
            &parse_location(&format!("file://{}", dir.path().display())).unwrap(),
            None,
        )
        .unwrap();
        let err = ParquetRows::open(&io, "a.parquet", &["nope"]).unwrap_err();
        assert!(matches!(err, StorageError::Config(_)), "{err}");
    }
}
