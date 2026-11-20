//! Iceberg table metadata model, v2 + v3 (specs/lakehouse/03-catalog.md,
//! Iceberg table spec). Slice 3 covers schema fields (full primitive set +
//! nested struct/list/map), snapshots, and snapshot lookup. Manifest/Avro
//! parsing and deletion vectors are later M1 slices.

//! Iceberg table metadata model — **v3-first, v2 read-compatible**
//! (specs/lakehouse/03-catalog.md; specs target Iceberg v3, v2 read is
//! compatibility only, v1 is rejected). Slice 3 covers schemas (full
//! primitive set + nested struct/list/map + v3-only types), snapshots, and
//! lookups. Manifest/Avro parsing and deletion vectors are later M1 slices.

use serde::Deserialize;
use std::collections::BTreeMap;

use uuid::Uuid;

/// Table format version. v3 is the target; v2 is read-compatible; v1 is
/// rejected at parse time (roadmap: backlog).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormatVersion {
    V2,
    V3,
}

impl<'de> Deserialize<'de> for FormatVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let v = u32::deserialize(deserializer)?;
        match v {
            2 => Ok(FormatVersion::V2),
            3 => Ok(FormatVersion::V3),
            other => Err(serde::de::Error::custom(format!(
                "unsupported table format-version {other} (this reader targets v3, reads v2; v1 is out of scope)"
            ))),
        }
    }
}

impl serde::Serialize for FormatVersion {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u16(match self {
            FormatVersion::V2 => 2,
            FormatVersion::V3 => 3,
        })
    }
}

/// Iceberg primitive + nested types. v3-only types (`Unknown`,
/// nanosecond timestamps, `Variant`) are first-class: the model is v3-shaped,
/// and per-version capability decisions live in the read/write paths.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IcebergType {
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Date,
    Time,
    Timestamp,
    Timestamptz,
    TimestampNanos,
    TimestamptzNanos,
    String,
    Uuid,
    Fixed(u32),
    Binary,
    Decimal { precision: u32, scale: u32 },
    Unknown,
    Variant,
    Struct(Vec<Field>),
    List(ListType),
    Map(MapType),
}

/// A `list` type with its nested field identity (spec § schemas): the
/// element carries its own field ID (bindable through schema evolution)
/// and a requiredness flag defaulting to true when a writer omits it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListType {
    pub element_id: i64,
    pub element_required: bool,
    pub element: Box<IcebergType>,
}

/// A `map` type with its nested field identity (spec § schemas): key and
/// value carry their own field IDs; the value's requiredness flag defaults
/// to true when a writer omits it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapType {
    pub key_id: i64,
    pub key: Box<IcebergType>,
    pub value_id: i64,
    pub value_required: bool,
    pub value: Box<IcebergType>,
}

impl<'de> Deserialize<'de> for IcebergType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let v = serde_json::Value::deserialize(deserializer)?;
        parse_type(&v)
            .ok_or_else(|| serde::de::Error::custom(format!("unrecognized iceberg type: {v}")))
    }
}

/// The canonical type name each variant parses back from (mirror of
/// parse_parameterized) — used by Serialize so commits write the exact
/// wire form other engines read.
fn type_to_value(t: &IcebergType) -> serde_json::Value {
    use IcebergType::*;
    match t {
        Boolean => "boolean".into(),
        Int => "int".into(),
        Long => "long".into(),
        Float => "float".into(),
        Double => "double".into(),
        Date => "date".into(),
        Time => "time".into(),
        Timestamp => "timestamp".into(),
        Timestamptz => "timestamptz".into(),
        TimestampNanos => "timestamp_ns".into(),
        TimestamptzNanos => "timestamptz_ns".into(),
        String => "string".into(),
        Uuid => "uuid".into(),
        Binary => "binary".into(),
        Unknown => "unknown".into(),
        Variant => "variant".into(),
        Fixed(n) => format!("fixed[{n}]").into(),
        Decimal { precision, scale } => format!("decimal({precision},{scale})").into(),
        Struct(fields) => serde_json::json!({ "type": "struct", "fields": fields }),
        List(lt) => serde_json::json!({
            "type": "list",
            "element-id": lt.element_id,
            "element-required": lt.element_required,
            "element": lt.element,
        }),
        Map(mt) => serde_json::json!({
            "type": "map",
            "key-id": mt.key_id,
            "key": mt.key,
            "value-id": mt.value_id,
            "value-required": mt.value_required,
            "value": mt.value,
        }),
    }
}

impl serde::Serialize for IcebergType {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        type_to_value(self).serialize(serializer)
    }
}

/// All type names that appear as plain strings: simple primitives plus the
/// parameterized `fixed[n]` and `decimal(p,s)`.
fn parse_parameterized(s: &str) -> Option<IcebergType> {
    use IcebergType::*;
    Some(match s {
        "boolean" => Boolean,
        "int" => Int,
        "long" => Long,
        "float" => Float,
        "double" => Double,
        "date" => Date,
        "time" => Time,
        "timestamp" => Timestamp,
        "timestamptz" => Timestamptz,
        "timestamp_ns" => TimestampNanos,
        "timestamptz_ns" => TimestamptzNanos,
        "string" => String,
        "uuid" => Uuid,
        "binary" => Binary,
        "unknown" => Unknown,
        "variant" => Variant,
        other => {
            if let Some(n) = other
                .strip_prefix("fixed[")
                .and_then(|r| r.strip_suffix(']'))
            {
                return n.parse().ok().map(Fixed);
            }
            let dec = other.strip_prefix("decimal(")?;
            let inner = dec.strip_suffix(')')?;
            let (p, sc) = inner.split_once(',')?;
            return Some(Decimal {
                precision: p.trim().parse().ok()?,
                scale: sc.trim().parse().ok()?,
            });
        }
    })
}

fn parse_type(v: &serde_json::Value) -> Option<IcebergType> {
    use IcebergType::*;
    match v {
        serde_json::Value::String(s) => parse_parameterized(s),
        serde_json::Value::Object(o) => match o.get("type")?.as_str()? {
            "struct" => {
                let fields = o.get("fields")?;
                Some(Struct(serde_json::from_value(fields.clone()).ok()?))
            }
            // nested field IDs are required on the wire (spec § appendix C):
            // without them the columns cannot bind through schema evolution,
            // so their absence fails the parse loudly. `*`-required flags
            // default to true (v1-era writers omit them).
            "list" => Some(List(ListType {
                element_id: o.get("element-id")?.as_i64()?,
                element_required: o
                    .get("element-required")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true),
                element: Box::new(parse_type(o.get("element")?)?),
            })),
            "map" => Some(Map(MapType {
                key_id: o.get("key-id")?.as_i64()?,
                key: Box::new(parse_type(o.get("key").or_else(|| o.get("keys"))?)?),
                value_id: o.get("value-id")?.as_i64()?,
                value_required: o
                    .get("value-required")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true),
                value: Box::new(parse_type(o.get("value").or_else(|| o.get("values"))?)?),
            })),
            _ => None,
        },
        _ => None,
    }
}

/// A schema field (v2/v3 shape: id, name, required, type, optional doc).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Field {
    pub id: i64,
    pub name: String,
    pub required: bool,
    pub r#type: IcebergType,
    #[serde(default)]
    pub doc: Option<String>,
}

/// A schema (ordered field list with an id).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Schema {
    #[serde(rename = "type")]
    pub struct_type: IcebergTypeMarker,
    #[serde(default)]
    pub schema_id: i64,
    pub fields: Vec<Field>,
}

/// Marker for the schema's `"type": "struct"`; parsing fails otherwise.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, Deserialize)]
pub enum IcebergTypeMarker {
    #[serde(rename = "struct")]
    Struct,
}

/// One partitioned field of a partition spec (spec § partition-specs).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionField {
    /// Partition field id (unique across the table's partition evolution).
    pub field_id: i64,
    pub name: String,
    /// Transform name as written in metadata: identity, bucket[16], day, …
    pub transform: String,
    /// Source schema field id the transform reads.
    pub source_id: i64,
}

/// A partition spec: an ordered list of partition fields. The ORDER of the
/// fields defines the layout of data_file.partition values in manifests —
/// pruning resolves positionally against this list (slice 8).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionSpec {
    pub spec_id: i32,
    #[serde(default)]
    pub fields: Vec<PartitionField>,
}

/// A table snapshot. Serializes for the commitTable `add-snapshot` update
/// (M2 slice 25): fields are kebab-case per the table spec, and
/// `schema-id` is tolerated-absent on read (v2 tables often omit it).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Snapshot {
    pub snapshot_id: i64,
    #[serde(default)]
    pub sequence_number: Option<i64>,
    #[serde(default)]
    pub first_row_id: Option<i64>,
    #[serde(default)]
    pub parent_snapshot_id: Option<i64>,
    pub timestamp_ms: i64,
    pub manifest_list: String,
    #[serde(default)]
    pub summary: BTreeMap<String, String>,
    #[serde(default)]
    pub schema_id: Option<i64>,
}

/// An entry of the metadata `refs` map (named snapshot pointers: tags and
/// branches). Branch-only fields (`min-snapshots-to-keep`,
/// `max-snapshot-age-ms`) are read-tolerated but unused by reads.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SnapshotRef {
    pub snapshot_id: i64,
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub min_snapshots_to_keep: Option<i32>,
    #[serde(default)]
    pub max_snapshot_age_ms: Option<i64>,
}

/// Time-travel selector (slice 19): how a `FOR … AS OF` clause picks the
/// snapshot a scan reads. This is the wire form the FFI passes down as
/// selector JSON — `{"kind": "id", "value": 42}`, `{"kind": "ts",
/// "value-ms": 1712345678000}`, `{"kind": "ref", "name": "audit-tag"}`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum SnapshotSelector {
    /// FOR VERSION AS OF <snapshot id>
    Id { value: i64 },
    /// FOR TIMESTAMP AS OF <ts>: epoch milliseconds (resolved from the SQL
    /// literal at parse-analysis time, so views/rules store a stable value).
    Ts {
        #[serde(rename = "value-ms")]
        value_ms: i64,
    },
    /// FOR TAG AS OF <name>: resolves through the metadata `refs` map
    /// (tags and branches both name a snapshot).
    Ref { name: String },
}

/// Parsed Iceberg table metadata (metadata.json). Unknown keys are ignored
/// for forward tolerance; v3-first means v3 additions are typed here, and
/// v2 documents parse into the same model.
#[derive(Debug, Clone, PartialEq, serde::Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct TableMetadata {
    pub format_version: FormatVersion,
    pub table_uuid: Uuid,
    pub location: String,
    #[serde(default)]
    pub last_sequence_number: Option<i64>,
    pub schemas: Vec<Schema>,
    pub current_schema_id: i64,
    #[serde(default)]
    pub snapshots: Vec<Snapshot>,
    #[serde(default)]
    pub current_snapshot_id: Option<i64>,
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
    #[serde(default)]
    pub partition_specs: Vec<PartitionSpec>,
    /// Iceberg's wire name is `default-spec-id`; some writers emitted
    /// `default-partition-spec-id` and we read that legacy spelling too
    /// (13-review blocker #5: the misspelling made every real table's
    /// default spec silently None, and commits fell back to spec zero).
    #[serde(
        default,
        rename = "default-spec-id",
        alias = "default-partition-spec-id"
    )]
    pub default_partition_spec_id: Option<i32>,
    #[serde(default)]
    pub refs: BTreeMap<String, SnapshotRef>,
}

impl TableMetadata {
    /// The schema `current-schema-id` points at, or None (stale/hand-made
    /// metadata must not panic — doc 03 §5).
    pub fn current_schema(&self) -> Option<&Schema> {
        self.schema_by_id(self.current_schema_id)
    }

    pub fn schema_by_id(&self, id: i64) -> Option<&Schema> {
        self.schemas.iter().find(|s| s.schema_id == id)
    }

    /// The current snapshot, if the table has one (empty tables have none).
    pub fn current_snapshot(&self) -> Option<&Snapshot> {
        let id = self.current_snapshot_id?;
        self.snapshot(id)
    }

    pub fn snapshot(&self, id: i64) -> Option<&Snapshot> {
        self.snapshots.iter().find(|s| s.snapshot_id == id)
    }

    /// The partition spec with the given id, or None (doc 03 §5: no panics).
    pub fn partition_spec(&self, id: i32) -> Option<&PartitionSpec> {
        self.partition_specs.iter().find(|s| s.spec_id == id)
    }

    /// Resolve a time-travel selector to a snapshot (slice 19). A timestamp
    /// picks the latest snapshot at or before it (the snapshot that was
    /// current at that time); a ref resolves through the refs map; an id is
    /// an exact lookup. None = selector matches no snapshot (the caller
    /// maps this to NotFound — no panics across the FFI).
    pub fn resolve_snapshot(&self, selector: &SnapshotSelector) -> Option<&Snapshot> {
        match selector {
            SnapshotSelector::Id { value } => self.snapshot(*value),
            SnapshotSelector::Ts { value_ms } => self
                .snapshots
                .iter()
                .filter(|s| s.timestamp_ms <= *value_ms)
                .max_by_key(|s| s.timestamp_ms),
            SnapshotSelector::Ref { name } => self
                .refs
                .get(name)
                .and_then(|r| self.snapshot(r.snapshot_id)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// TDD anchor (M1 slice 3): a realistic v2 metadata.json parses fully —
    /// schema fields with the primitive types, snapshots, current pointers.
    #[test]
    fn parses_v2_metadata_with_schema_and_snapshots() {
        let json = r#"{
            "format-version": 2,
            "table-uuid": "9c12d441-03fe-11ed-8122-d358e75d12eb",
            "location": "s3://warehouse/analytics/events",
            "last-sequence-number": 34,
            "schemas": [
                {
                    "type": "struct",
                    "schema-id": 0,
                    "fields": [
                        {"id": 1, "name": "event_id", "required": true, "type": "uuid"},
                        {"id": 2, "name": "occurred_at", "required": true, "type": "timestamptz"},
                        {"id": 3, "name": "payload_size", "required": false, "type": "long"},
                        {"id": 4, "name": "level", "required": false, "type": "string"},
                        {"id": 5, "name": "score", "required": false,
                         "type": "decimal(12,4)"},
                        {"id": 6, "name": "blob", "required": false, "type": "binary"},
                        {"id": 7, "name": "host", "required": false, "type": "string",
                         "doc": "origin host"}
                    ]
                }
            ],
            "current-schema-id": 0,
            "snapshots": [
                {
                    "snapshot-id": 3051729675574597004,
                    "timestamp-ms": 1712345678000,
                    "manifest-list": "s3://warehouse/analytics/events/m1.avro",
                    "summary": {"operation": "append", "added-files": "3"}
                },
                {
                    "snapshot-id": 3051729675574597005,
                    "parent-snapshot-id": 3051729675574597004,
                    "timestamp-ms": 1712345999000,
                    "manifest-list": "s3://warehouse/analytics/events/m2.avro",
                    "summary": {"operation": "overwrite", "deleted-files": "1"}
                }
            ],
            "current-snapshot-id": 3051729675574597005,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "default-sort-order-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "default-spec-id": 0,
            "properties": {"write.format.default": "parquet"}
        }"#;
        let md: TableMetadata = serde_json::from_str(json).expect("parse v2 metadata");
        assert_eq!(md.format_version, FormatVersion::V2);
        assert_eq!(md.current_schema().unwrap().fields.len(), 7);
        let schema = md.current_schema().unwrap();
        assert_eq!(schema.fields[0].name, "event_id");
        assert_eq!(schema.fields[0].r#type, IcebergType::Uuid);
        assert!(schema.fields[0].required);
        assert_eq!(schema.fields[2].r#type, IcebergType::Long);
        assert_eq!(
            schema.fields[4].r#type,
            IcebergType::Decimal {
                precision: 12,
                scale: 4
            }
        );
        assert_eq!(schema.fields[6].doc.as_deref(), Some("origin host"));
        assert_eq!(md.current_snapshot_id, Some(3051729675574597005));
        let cur = md.current_snapshot().unwrap();
        assert_eq!(
            cur.summary.get("operation").map(String::as_str),
            Some("overwrite")
        );
        assert_eq!(md.snapshots.len(), 2);
    }

    /// v3-first: format-version 3 parses with v3-only additions as
    /// first-class citizens — `unknown` / nanosecond timestamps / `variant`
    /// types, snapshot `sequence-number` and `first-row-id` — so the read
    /// path makes version-aware decisions from day one instead of carrying
    /// v2-shaped debt. v3 adds per-snapshot deletion vectors via summary
    /// keys we keep forward-tolerant.
    #[test]
    fn parses_v3_metadata_with_v3_only_additions() {
        let json = r#"{
            "format-version": 3,
            "table-uuid": "9c12d441-03fe-11ed-8122-d358e75d12eb",
            "location": "s3://warehouse/analytics/events",
            "last-sequence-number": 41,
            "schemas": [
                {"type": "struct", "schema-id": 0, "fields": [
                    {"id": 1, "name": "id", "required": true, "type": "long"},
                    {"id": 2, "name": "ts_nanos", "required": false,
                     "type": "timestamp_ns"},
                    {"id": 3, "name": "opaque", "required": false, "type": "unknown"},
                    {"id": 4, "name": "signal", "required": false, "type": "variant"}
                ]}
            ],
            "current-schema-id": 0,
            "snapshots": [
                {"snapshot-id": 42, "sequence-number": 41, "first-row-id": 0,
                 "timestamp-ms": 1712345678000,
                 "manifest-list": "s3://warehouse/analytics/events/m1.avro",
                 "summary": {"operation": "append"}}
            ],
            "current-snapshot-id": 42
        }"#;
        let md: TableMetadata = serde_json::from_str(json).expect("parse v3 metadata");
        assert_eq!(md.format_version, FormatVersion::V3);
        let fields = &md.current_schema().unwrap().fields;
        assert_eq!(fields[1].r#type, IcebergType::TimestampNanos);
        assert_eq!(fields[2].r#type, IcebergType::Unknown);
        assert_eq!(fields[3].r#type, IcebergType::Variant);
        let snap = md.snapshot(42).unwrap();
        assert_eq!(snap.sequence_number, Some(41));
        assert_eq!(snap.first_row_id, Some(0));
        assert_eq!(md.current_snapshot().unwrap().snapshot_id, 42);
    }

    /// v1 tables are out of scope (roadmap: backlog) and rejected loudly at
    /// parse time rather than mis-read.
    #[test]
    fn v1_metadata_is_rejected() {
        let json = r#"{"format-version": 1, "table-uuid":
            "9c12d441-03fe-11ed-8122-d358e75d12eb", "location": "s3://x",
            "schema": {"type": "struct", "fields": []}}"#;
        let err = serde_json::from_str::<TableMetadata>(json).expect_err("v1 must be rejected");
        assert!(err.to_string().contains("format-version 1"), "{err}");
    }

    /// Nested types: struct recursion, list-of-primitive, map keys/values.
    #[test]
    fn parses_nested_types() {
        let json = r#"{
            "format-version": 2,
            "table-uuid": "9c12d441-03fe-11ed-8122-d358e75d12eb",
            "location": "s3://warehouse/t",
            "schemas": [{"type": "struct", "schema-id": 0, "fields": [
                {"id": 1, "name": "user", "required": true, "type": {
                    "type": "struct", "fields": [
                        {"id": 2, "name": "uid", "required": true, "type": "long"},
                        {"id": 3, "name": "tags", "required": false, "type": {
                            "type": "list", "element-id": 30,
                            "element-required": false, "element": "string"}}
                    ]}
                },
                {"id": 4, "name": "counts", "required": false, "type": {
                    "type": "map", "key-id": 40, "key": "string",
                    "value-id": 41, "value-required": false, "value": "long"}}
            ]}],
            "current-schema-id": 0
        }"#;
        let md: TableMetadata = serde_json::from_str(json).expect("parse nested");
        let f = &md.current_schema().unwrap().fields[0];
        match &f.r#type {
            IcebergType::Struct(fields) => {
                assert_eq!(fields[1].name, "tags");
                assert_eq!(
                    fields[1].r#type,
                    IcebergType::List(ListType {
                        element_id: 30,
                        element_required: false,
                        element: Box::new(IcebergType::String),
                    })
                );
            }
            other => panic!("expected struct, got {other:?}"),
        }
        assert_eq!(
            md.current_schema().unwrap().fields[1].r#type,
            IcebergType::Map(MapType {
                key_id: 40,
                key: Box::new(IcebergType::String),
                value_id: 41,
                value_required: false,
                value: Box::new(IcebergType::Long),
            })
        );
    }

    /// 13-review #5: list and map models keep their nested field IDs and
    /// requiredness — projection and delete binding happen by Iceberg ID,
    /// so discarding `element-id`/`key-id`/`value-id` (and the `*-required`
    /// flags) makes nested columns unbindable and silently optional. The
    /// `*`-required flags default to true per spec when a v1-era writer
    /// omits them; the IDs themselves are required (a nested type without
    /// IDs cannot participate in schema evolution).
    #[test]
    fn nested_types_keep_field_ids_and_requiredness() {
        let json = r#"{
            "format-version": 2,
            "table-uuid": "9c12d441-03fe-11ed-8122-d358e75d12eb",
            "location": "s3://warehouse/t",
            "schemas": [{"type": "struct", "schema-id": 0, "fields": [
                {"id": 1, "name": "tags", "required": false, "type": {
                    "type": "list", "element-id": 2, "element": "string"}},
                {"id": 3, "name": "counts", "required": false, "type": {
                    "type": "map", "key-id": 4, "key": "string",
                    "value-id": 5, "value": "long"}}
            ]}],
            "current-schema-id": 0
        }"#;
        let md: TableMetadata = serde_json::from_str(json).expect("parse nested ids");
        let fields = &md.current_schema().unwrap().fields;
        assert_eq!(
            fields[0].r#type,
            IcebergType::List(ListType {
                element_id: 2,
                element_required: true, // spec default when flag omitted
                element: Box::new(IcebergType::String),
            })
        );
        assert_eq!(
            fields[1].r#type,
            IcebergType::Map(MapType {
                key_id: 4,
                key: Box::new(IcebergType::String),
                value_id: 5,
                value_required: true, // spec default when flag omitted
                value: Box::new(IcebergType::Long),
            })
        );

        // loud failure: a nested type with no field IDs cannot bind by ID
        let bad = r#"{
            "format-version": 2,
            "table-uuid": "9c12d441-03fe-11ed-8122-d358e75d12eb",
            "location": "s3://warehouse/t",
            "schemas": [{"type": "struct", "schema-id": 0, "fields": [
                {"id": 1, "name": "tags", "required": false, "type": {
                    "type": "list", "element": "string"}}]}],
            "current-schema-id": 0
        }"#;
        assert!(
            serde_json::from_str::<TableMetadata>(bad).is_err(),
            "list without element-id must fail loudly"
        );
    }

    /// TDD anchor (M1 slice 8): partition-specs parse — identity transform
    /// first (bucket/truncate/temporal transforms land with their pruning
    /// slices); unknown transforms are preserved but flagged, never dropped.
    #[test]
    fn parses_partition_specs() {
        let json = r#"{
            "format-version": 2,
            "table-uuid": "9c12d441-03fe-11ed-8122-d358e75d12eb",
            "location": "s3://warehouse/t",
            "schemas": [{"type": "struct", "schema-id": 0, "fields": [
                {"id": 1, "name": "id", "required": true, "type": "long"}]}],
            "current-schema-id": 0,
            "partition-specs": [
                {"spec-id": 0, "fields": []},
                {"spec-id": 1, "fields": [
                    {"name": "region", "transform": "identity", "source-id": 3,
                     "field-id": 1000},
                    {"name": "id_bucket", "transform": "bucket[16]",
                     "source-id": 1, "field-id": 1001},
                    {"name": "at_day", "transform": "day", "source-id": 4,
                     "field-id": 1002}
                ]}
            ],
            "default-spec-id": 1
        }"#;
        let md: TableMetadata = serde_json::from_str(json).expect("parse partition specs");
        assert_eq!(md.partition_specs.len(), 2);
        let spec = md.partition_spec(1).unwrap();
        assert_eq!(spec.fields.len(), 3);
        assert_eq!(spec.fields[0].name, "region");
        assert_eq!(spec.fields[0].transform, "identity");
        assert_eq!(spec.fields[0].source_id, 3);
        assert_eq!(spec.fields[0].field_id, 1000);
        assert_eq!(spec.fields[1].transform, "bucket[16]");
        assert_eq!(spec.fields[2].transform, "day");
        assert_eq!(md.default_partition_spec_id, Some(1));
        assert!(md.partition_spec(9).is_none());
    }

    /// Failed lookups return None, not panics.
    #[test]
    fn missing_schema_and_snapshot_lookups_return_none() {
        let json = r#"{
            "format-version": 2,
            "table-uuid": "9c12d441-03fe-11ed-8122-d358e75d12eb",
            "location": "s3://warehouse/t",
            "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
            "current-schema-id": 7
        }"#;
        let md: TableMetadata = serde_json::from_str(json).expect("parse");
        assert!(md.current_schema().is_none());
        assert!(md.snapshot(1).is_none());
    }

    /// TDD anchor (M1 slice 19): `refs` parse and time-travel selector
    /// resolution — an id picks the exact snapshot; a timestamp picks the
    /// latest snapshot at or before that time (Iceberg "snapshot current at
    /// timestamp" semantics); a ref resolves through the refs map. Unknown
    /// ids/refs and too-early timestamps yield None (plan_scan maps them to
    /// NotFound — doc 03 §5).
    #[test]
    fn resolves_time_travel_selectors() {
        let json = r#"{
            "format-version": 2,
            "table-uuid": "9c12d441-03fe-11ed-8122-d358e75d12eb",
            "location": "s3://warehouse/t",
            "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
            "current-schema-id": 0,
            "snapshots": [
                {"snapshot-id": 10, "timestamp-ms": 1000,
                 "manifest-list": "file:///m10.avro",
                 "summary": {"operation": "append"}},
                {"snapshot-id": 11, "timestamp-ms": 2000,
                 "manifest-list": "file:///m11.avro",
                 "summary": {"operation": "append"}},
                {"snapshot-id": 12, "timestamp-ms": 3000,
                 "manifest-list": "file:///m12.avro",
                 "summary": {"operation": "append"}}
            ],
            "current-snapshot-id": 12,
            "refs": {
                "audit-tag": {"snapshot-id": 10, "type": "tag"},
                "main": {"snapshot-id": 12, "type": "branch"}
            }
        }"#;
        let md: TableMetadata = serde_json::from_str(json).expect("parse refs");

        assert_eq!(md.refs["audit-tag"].snapshot_id, 10);
        assert_eq!(md.refs["audit-tag"].kind, "tag");
        assert_eq!(md.refs["main"].kind, "branch");

        // id: exact match only
        assert_eq!(
            md.resolve_snapshot(&SnapshotSelector::Id { value: 11 })
                .unwrap()
                .snapshot_id,
            11
        );
        assert!(md
            .resolve_snapshot(&SnapshotSelector::Id { value: 999 })
            .is_none());

        // ts: latest snapshot at or before the timestamp
        assert_eq!(
            md.resolve_snapshot(&SnapshotSelector::Ts { value_ms: 2500 })
                .unwrap()
                .snapshot_id,
            11
        );
        assert_eq!(
            md.resolve_snapshot(&SnapshotSelector::Ts { value_ms: 1000 })
                .unwrap()
                .snapshot_id,
            10
        );
        assert_eq!(
            md.resolve_snapshot(&SnapshotSelector::Ts { value_ms: 99_999 })
                .unwrap()
                .snapshot_id,
            12
        );
        assert!(md
            .resolve_snapshot(&SnapshotSelector::Ts { value_ms: 999 })
            .is_none());

        // ref: via the refs map; unknown ref or dangling ref-id is None
        assert_eq!(
            md.resolve_snapshot(&SnapshotSelector::Ref {
                name: "audit-tag".into()
            })
            .unwrap()
            .snapshot_id,
            10
        );
        assert!(md
            .resolve_snapshot(&SnapshotSelector::Ref {
                name: "nope".into()
            })
            .is_none());
    }
}

#[cfg(test)]
mod wire_name_tests {
    use super::*;

    /// ACCEPTANCE (13-review blocker #5): the SPEC wire names parse —
    /// `default-spec-id` (nonzero!) and map types with `key`/`value`
    /// members. The old code only knew the misspelled
    /// `default-partition-spec-id` and `keys`/`values`, silently zeroing
    /// the default spec and failing to load otherwise-readable tables
    /// that contain a map column.
    #[test]
    fn spec_wire_names_parse() {
        let json = r#"{
            "format-version": 2,
            "table-uuid": "9c12d441-03fe-11ed-8122-d358e75d12eb",
            "location": "s3://warehouse/t",
            "schemas": [{"type": "struct", "schema-id": 0, "fields": [
                {"id": 1, "name": "tags", "required": false,
                 "type": {"type": "map",
                          "key-id": 3, "key": "string",
                          "value-id": 4, "value": "long",
                          "value-required": false}},
                {"id": 2, "name": "part", "required": false, "type": "long"}
            ]}],
            "current-schema-id": 0,
            "partition-specs": [{"spec-id": 1, "fields": [
                {"name": "part", "transform": "identity", "source-id": 2, "field-id": 5}
            ]}],
            "default-spec-id": 1
        }"#;
        let md: TableMetadata = serde_json::from_str(json).expect("parse spec-shaped metadata");
        assert_eq!(md.default_partition_spec_id, Some(1), "spec wire name");
        let schema = md.current_schema().unwrap();
        match &schema.fields[0].r#type {
            IcebergType::Map(mt) => {
                assert_eq!(*mt.key, IcebergType::String);
                assert_eq!(*mt.value, IcebergType::Long);
            }
            other => panic!("expected map, got {other:?}"),
        }
        assert_eq!(md.partition_spec(1).unwrap().fields.len(), 1);
        // the writer must emit the parsed default spec, not fall back to 0
        assert_eq!(
            md.default_partition_spec_id,
            Some(md.partition_spec(1).unwrap().spec_id)
        );
    }

    /// Round-trip: a map type serializes the SPEC members, including the
    /// nested field IDs (13-review #5).
    #[test]
    fn map_type_serializes_spec_members() {
        let t = IcebergType::Map(MapType {
            key_id: 4,
            key: Box::new(IcebergType::String),
            value_id: 5,
            value_required: true,
            value: Box::new(IcebergType::Long),
        });
        let json = serde_json::to_string(&t).unwrap();
        assert!(json.contains("\"key\""), "{json}");
        assert!(json.contains("\"value\""), "{json}");
        assert!(json.contains("\"key-id\":4"), "{json}");
        assert!(json.contains("\"value-id\":5"), "{json}");
        assert!(!json.contains("\"keys\""), "{json}");
    }

    /// Legacy misspelling still parses (tolerance for old writers). The
    /// member NAMES are tolerated; nested field IDs remain required
    /// (13-review #5), so this fixture carries them.
    #[test]
    fn legacy_misspellings_tolerated() {
        let json = r#"{
            "format-version": 2,
            "table-uuid": "9c12d441-03fe-11ed-8122-d358e75d12eb",
            "location": "s3://warehouse/t",
            "schemas": [{"type": "struct", "schema-id": 0, "fields": [
                {"id": 1, "name": "m", "required": false,
                 "type": {"type": "map", "keys": "string", "values": "long",
                          "key-id": 2, "value-id": 3}}
            ]}],
            "current-schema-id": 0,
            "default-partition-spec-id": 2
        }"#;
        let md: TableMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(md.default_partition_spec_id, Some(2));
        let schema = md.current_schema().unwrap();
        assert!(matches!(schema.fields[0].r#type, IcebergType::Map { .. }));
    }
}
