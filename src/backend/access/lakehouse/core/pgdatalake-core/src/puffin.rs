//! Iceberg v3 deletion vectors: Puffin container parsing + the
//! `deletion-vector-v1` blob format (Roaring position sets referenced to
//! data files via the blob's `referenced-data-file` property). Slice V1
//! of the v3 conformance plan (specs/lakehouse/14-v3-conformance.md).
//!
//! Wire format references: pyiceberg 0.12's `table/puffin.py` +
//! `table/deletion_vector.py`; fixtures in
//! `tests/fixtures/v3_dv_table/` are written by the Apache Iceberg Java
//! reference implementation (the v3 writer of record).

use crate::storage::StorageError;
use roaring::RoaringTreemap;
use serde::Deserialize;

/// Puffin magic: "Puffin Fratercula arctica", version 1.
const MAGIC: &[u8; 4] = b"PFA1";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct FooterJson {
    blobs: Vec<BlobJson>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct BlobJson {
    #[serde(rename = "type")]
    blob_type: String,
    offset: u64,
    length: u64,
    #[serde(default)]
    compression_codec: Option<String>,
    #[serde(default)]
    properties: std::collections::BTreeMap<String, String>,
}

/// One decoded deletion vector: the positions to suppress in
/// `referenced_data_file`.
#[derive(Debug)]
pub struct DeletionVector {
    pub referenced_data_file: String,
    pub positions: RoaringTreemap,
}

/// A parsed Puffin container: blob metadata from the JSON footer plus the
/// raw bytes needed to slice payloads.
pub struct PuffinFile<'a> {
    bytes: &'a [u8],
    blobs: Vec<BlobJson>,
}

impl<'a> PuffinFile<'a> {
    /// Parse the container: magic bytes, footer location (trailing
    /// [payload][size: int32 LE][flags: int32 LE][magic]), JSON footer.
    pub fn parse(bytes: &'a [u8]) -> Result<Self, StorageError> {
        if bytes.len() < 12 || &bytes[..4] != MAGIC || &bytes[bytes.len() - 4..] != MAGIC {
            return Err(StorageError::Decode("puffin: bad magic bytes".into()));
        }
        // flags byte 0 bit 0 = compressed footer; others reserved
        if bytes[bytes.len() - 8] != 0 {
            return Err(StorageError::Decode(
                "puffin: compressed footers are not supported".into(),
            ));
        }
        let size = u32::from_le_bytes(
            *bytes[bytes.len() - 12..bytes.len() - 8]
                .first_chunk::<4>()
                .unwrap(),
        ) as usize;
        let payload_start = bytes
            .len()
            .checked_sub(size + 12)
            .ok_or_else(|| StorageError::Decode("puffin: footer size overruns file".into()))?;
        let footer: FooterJson =
            serde_json::from_slice(&bytes[payload_start..payload_start + size])
                .map_err(|e| StorageError::Decode(format!("puffin footer: {e}")))?;
        Ok(PuffinFile {
            bytes,
            blobs: footer.blobs,
        })
    }

    #[must_use]
    pub fn blobs(&self) -> &[BlobJson] {
        &self.blobs
    }

    #[must_use]
    pub fn blob_properties(&self, idx: usize) -> &std::collections::BTreeMap<String, String> {
        &self.blobs[idx].properties
    }

    #[must_use]
    pub fn blob_type(&self, idx: usize) -> &str {
        &self.blobs[idx].blob_type
    }

    /// Raw payload of blob `idx` (still compressed if `compression_codec`
    /// says so — deletion-vector-v1 blobs are written uncompressed).
    pub fn blob_payload(&self, idx: usize) -> Result<&'a [u8], StorageError> {
        let b = &self.blobs[idx];
        if b.compression_codec.is_some() {
            return Err(StorageError::Decode(format!(
                "puffin: unsupported blob compression {:?}",
                b.compression_codec
            )));
        }
        let start = b.offset as usize;
        let end = start
            .checked_add(b.length as usize)
            .ok_or_else(|| StorageError::Decode("puffin: blob length overruns".into()))?;
        self.bytes
            .get(start..end)
            .ok_or_else(|| StorageError::Decode("puffin: blob range out of file".into()))
    }
}

/// Decode a `deletion-vector-v1` blob payload into positions:
/// framing `[data-length: int32 BE][magic "DV1\n"][data][crc: int32 BE]`,
/// data = `[bitmap-count: int64 LE]{[bitmap-key: int32 LE][roaring portable
/// bitmap]}*` — key N covers row ids `N * 2^32 .. (N+1) * 2^32`.
pub fn decode_dv_payload(payload: &[u8]) -> Result<RoaringTreemap, StorageError> {
    if payload.len() < 8 {
        return Err(StorageError::Decode("dv: payload too short".into()));
    }
    let data_len = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
    // bytes 4..8 are the "DV1\n" magic; data_len covers magic + data (CRC excluded)
    let end = 8usize
        .checked_add(data_len)
        .and_then(|e| e.checked_sub(4)) // subtract magic
        .ok_or_else(|| StorageError::Decode("dv: length overruns payload".into()))?;
    if end > payload.len() {
        return Err(StorageError::Decode(
            "dv: data length overruns payload".into(),
        ));
    }
    let data = &payload[8..end];

    if data.len() < 8 {
        return Err(StorageError::Decode("dv: bitmap data too short".into()));
    }
    let n_bitmaps = u64::from_le_bytes(data[0..8].try_into().unwrap());
    let mut pl = &data[8..];
    let mut out = RoaringTreemap::new();
    let mut last_key: i64 = -1;
    for _ in 0..n_bitmaps {
        if pl.len() < 4 {
            return Err(StorageError::Decode("dv: truncated bitmap key".into()));
        }
        let key = i64::from(u32::from_le_bytes(pl[0..4].try_into().unwrap()));
        if key <= last_key {
            return Err(StorageError::Decode("dv: bitmap keys not ascending".into()));
        }
        pl = &pl[4..];
        // gap keys contribute no positions (per the spec's sparse encoding)
        let bm32 = roaring::bitmap::RoaringBitmap::deserialize_from(pl)
            .map_err(|e| StorageError::Decode(format!("dv: roaring decode: {e}")))?;
        let consumed = bm32.serialized_size();
        for v in &bm32 {
            out.insert(((key as u64) << 32) | u64::from(v));
        }
        pl = &pl[consumed..];
        last_key = key;
    }
    Ok(out)
}

/// Decode every `deletion-vector-v1` blob in the container.
pub fn decode_dv_container(bytes: &[u8]) -> Result<Vec<DeletionVector>, StorageError> {
    let pf = PuffinFile::parse(bytes)?;
    let mut out = Vec::new();
    for i in 0..pf.blobs().len() {
        if pf.blob_type(i) != "deletion-vector-v1" {
            continue;
        }
        let referenced = pf
            .blob_properties(i)
            .get("referenced-data-file")
            .cloned()
            .ok_or_else(|| {
                StorageError::Decode("dv: blob has no referenced-data-file property".into())
            })?;
        let positions = decode_dv_payload(pf.blob_payload(i)?)?;
        out.push(DeletionVector {
            referenced_data_file: referenced.clone(),
            positions,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_puffin_bytes() -> Vec<u8> {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        std::fs::read(
            dir.join("tests/fixtures/v3_dv_table/data")
                .read_dir()
                .expect("fixture data dir")
                .filter_map(std::result::Result::ok)
                .map(|e| e.path())
                .find(|p| p.extension().is_some_and(|x| x == "puffin"))
                .expect("fixture puffin file"),
        )
        .expect("read fixture puffin")
    }

    fn fixture_data_file_name() -> String {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        std::fs::read_dir(dir.join("tests/fixtures/v3_dv_table/data"))
            .expect("fixture data dir")
            .filter_map(std::result::Result::ok)
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .find(|n| n.ends_with(".parquet"))
            .expect("fixture parquet name")
    }

    /// V1 acceptance anchor: the Apache Iceberg JAVA reference writer
    /// produced this container; our decoder must yield positions {2, 4}
    /// on the referenced parquet file (ids 3 and 5 suppressed).
    #[test]
    fn decodes_java_written_deletion_vector() {
        let referenced = fixture_data_file_name();
        let bytes = fixture_puffin_bytes();
        let dvs = decode_dv_container(&bytes).expect("decode container");
        assert_eq!(dvs.len(), 1, "one DV blob");
        assert!(
            dvs[0].referenced_data_file.ends_with(&referenced),
            "references the data file: {}",
            dvs[0].referenced_data_file
        );
        let expected: RoaringTreemap = [2u64, 4u64].into();
        assert_eq!(dvs[0].positions, expected);
    }

    /// The framed-payload contract: bad magic/lengths fail loudly, never
    /// silently yield an empty bitmap (a silent empty set would resurrect
    /// deleted rows).
    #[test]
    fn truncated_payload_is_a_loud_error() {
        let bytes = fixture_puffin_bytes();
        let pf = PuffinFile::parse(&bytes).expect("parse");
        let payload = pf.blob_payload(0).expect("payload");
        let short = &payload[..payload.len() - 8.min(payload.len())];
        assert!(decode_dv_payload(short).is_err(), "truncated must error");
    }
}
