# 05 — Type Mapping & Schema Evolution

## 1. Mapping table (canonical)

Iceberg types per v3 spec. Postgres mapping is **bidirectional**: every Iceberg
type read must map to exactly one PG type, and every PG type must either map or be
rejected at DDL with a suggestion.

| Iceberg | → Postgres | Postgres → Iceberg | Notes |
|---|---|---|---|
| `boolean` | `boolean` | `boolean` | |
| `int` (32-bit) | `integer` | `integer` | |
| `long` | `bigint` | `bigint` | |
| `float` | `real` | `real` | IEEE754 both sides; NaN payloads not preserved |
| `double` | `double precision` | `double precision` | |
| `decimal(P,S)` | `numeric(P,S)` | `numeric(P,S)` | Iceberg max P=38 = PG numeric unscaled base-10000 compatible; enforce precision match, reject unconstrained `numeric` (suggest explicit P,S) |
| `date` | `date` | `date` | days-from-epoch both sides |
| `time` | `time without time zone` | `time` | micros-from-midnight |
| `timestamp` | `timestamp without time zone` | `timestamp` | |
| `timestamptz` | `timestamp with time zone` | `timestamptz` | v3 supports nanos; PG is µs — see §2 |
| `nanotimestamp`/`nanotimestamptz` (v3) | `timestamp(6)` + WARNING on write (truncation) | read: exact ns retained in Parquet stats, returned truncated | documented truncation, `lakehouse.nanosecond_mode = truncate|error` |
| `string` | `text` | `text` | PG `varchar(n)` writes as string; length constraint NOT enforced by Iceberg — enforced by PG-side CHECK that we synthesize (managed tables) or warn (external) |
| `uuid` | `uuid` | `uuid` | |
| `fixed[L]` | `bytea` with CHECK (octet_length = L) | `bytea` (CREATE) | no PG-native fixed-length binary; length via synthesized constraint |
| `binary` | `bytea` | `bytea` | |
| `list<T>` | `T[]` | `T[]` | 0-based PG arrays vs 0-based list — compatible for 1-D; nested arrays → `list<list<T>>` |
| `map<K,V>` | `jsonb` (v1) / `lakehouse.map<K,V>` (v2 domain type) | explicit cast required | see §3 |
| `struct<F1 T1,…>` | composite type (managed: auto `CREATE TYPE …_row`; external: `lakehouse.struct` pseudo) | composite type | field names preserved |
| `variant` (v3) | `jsonb` (read) / write via `lakehouse.variant_unshredded` cast (v2) | deferred | fast-follow |

Integer width policy: PG `smallint` has no Iceberg counterpart — maps to `int`
(widening, safe on read; on write, value range checked, overflow → error).
Unsigned types don't exist in PG; Iceberg's are all signed. Good.

*(Slice 21 amendment, 2026-11-19 — read-half conformance status.)* All
primitive types decode to their PG input-function-compatible TEXT on the
read path, verified live against PyIceberg-written fixtures
(`scripts/gen_conformance_fixtures.py`): decimal → canonical unscaled/scale
text, uuid → canonical lowercase hyphenated text (parquet FLBA(16); other
sizes render as raw bytes for `fixed[L]`), date → ISO `YYYY-MM-DD`, time →
`HH:MM:SS.ffffff`, binary → `\x`-prefixed bytea hex. Interop caveat:
PyIceberg 0.12 writes Iceberg `fixed[L]` values as ASCII hex STRINGS (2L
bytes) rather than spec-compliant FLBA(L); Trino/Spark write FLBA, so
spec-written tables read correctly and the pyiceberg-fixed case is a
known upstream quirk, not a reader bug.

## 2. Precision rules

- `timestamptz`: PG stores µs since epoch UTC. Iceberg `timestamptz` (v2) is µs;
  v3 adds `nanotimestamptz`. Read: if ns field ≠ 0 and `lakehouse.nanosecond_mode =
  truncate` (default), truncate with a DEBUG log; `= error` raises
  `iceberg_precision_loss`. Never round.
- `numeric`: PG numeric has no fixed precision internally; Iceberg requires
  `(P,S)`. Unconstrained `numeric` columns are rejected on CREATE with hint
  `numeric(38,S)`; a computed scale from existing data is suggested by
  `lakehouse.suggest_type()` helper.
- PG `money`, `interval`, `inet`, `cidr`, `macaddr`, `xml`, `json` (see below),
  `bit`, `varbit`, `tsvector`, `geometric`, `circle` etc.: **no mapping, rejected
  at CREATE** with suggestion (`jsonb` for json, `text` for xml/inet when the user
  accepts string round-trip).

## 3. Maps and structs: the two-phase plan

v1: `map<K,V>` ↔ `jsonb`. Object keys become JSON string keys (K must be string-ish
on read; non-string-keyed maps are read as JSON arrays of `{"key":…, "value":…}`
pairs — lossy, documented). This unblocks adoption without inventing syntax.

v2: introduce **domain types** `lakehouse.map(keytype anyelement, valtype anyelement)`
and `lakehouse.struct(...)` stored as `jsonb` underneath but carrying type identity,
so casts and operator classes behave. This is additive Postgres-level machinery
(no core changes).

## 4. Schema evolution (Iceberg rules adopted verbatim)

Allowed and transparent (both table kinds):
- Add optional column (any position — Iceberg field-IDs make positions moot).
- Add column with initial default (Iceberg v2 initial-default semantics).
- Widen int→long, float→double, decimal P widening.
- Reorder columns.
- Drop column (becomes hidden; data retained in old files).

Rejected (error, not degrade): narrowing, type change not in Iceberg's allowed
set, dropping a required column, renaming (Iceberg rename = new field-id mapping —
we support rename since v2 field-IDs decouple name from data).

**Field-ID discipline:** managed-table writer stores Iceberg field-IDs as the
sole schema truth; PG attnums are a view. Renaming a column in PG renames in
Iceberg (field-ID preserved) — data files untouched.

## 5. Defaults, nullability, constraints

- Iceberg `required` ↔ PG `NOT NULL` — enforced.
- PG `DEFAULT` clauses: evaluated by Postgres at insert time (write-out), *not*
  stored as Iceberg schema defaults (except initial-default which Iceberg reads —
  we set both so other engines see the same values).
- CHECK constraints: enforced by Postgres at insert (managed tables) — they are a
  Postgres layer on top, invisible to other engines. Same for PK/UNIQUE **unless**
  PK maps to a table property we set (`write.metadata.metrics.default` unaffected);
  PK/UNIQUE are enforced by PG (no index; a uniqueness violation is detected at
  commit by merge-on-read... v1: PK/UNIQUE on managed Iceberg tables → ERROR
  "not supported"; use equality-delete-based MERGE patterns instead).
- Collations: PG collation columns on Iceberg `string` are rejected (sort order is
  defined by Iceberg SortOrder, ICU-independent byte/UTF-8 order).

## 6. Migration helper

```sql
SELECT * FROM lakehouse.suggest_type('local_tbl');          -- report + DDL suggestion
SELECT lakehouse.migrate_table('local_tbl', dest => 's3://wh/tbl',
                             catalog => 'prod', batch => '100000');
-- copies a heap table into a new managed Iceberg table in batches,
-- resumable via pg_lakehouse.maintenance_log
```

## Type-boundary hardening (2026-11-20, 13-review blocker #8a — read path)

The C/Rust row boundary has an explicit ROW/EOF/ERROR status
(`pg_datalake_rows_next`): a caught Rust panic used to return `false` —
indistinguishable from EOF, silently truncating result sets. Errors
abort the statement with ERRCODE_DATA_EXCEPTION and a bounded diagnostic
naming the column and the problem. Cell-level rules:

- Unsupported Arrow types **fail at open** (`is_supported_arrow_type`
  validation over the full read set, equality-delete columns included) —
  unknown types used to surface as SQL NULL per cell, fabricating empty
  data. Nested types remain unsupported (loud) until the type-matrix
  slice.
- `LargeUtf8`/`LargeBinary` have their OWN arrow array types — the
  shared small-offset downcast silently NULLed them; both decode now.
- An interior NUL byte in a text value is an ERROR (it used to become an
  EMPTY string).
- Write-side exported functions (`write_push`, `write_flush`,
  `write_commit`) carry the same catch_unwind boundary as scan-open.

Remaining #8 write-path items (typmod, name-Datum, dropped-attribute
row/schema mismatch, decimal scale reconciliation, whole-row validation
before builder mutation, counted projection fields, bytea_output
independence) are the 8b slice.

## Write-path type fixes (2026-11-20, 13-review blocker #8b)

- **`name` columns routed through `textout`** — name Datums are
  `NameData*`, not varlena text; they now go through `nameout` (and
  bpchar through `bpcharout`, keeping its padded-blank semantics).
- **bytea independent of `bytea_output`** — the write path hex-encodes
  directly; `bytea_output=escape` used to silently change the stored
  bytes.
- **Decimal scale reconciliation** — the textual fractional digits
  became the unscaled value verbatim, silently rescaling numbers
  (123.45 into a scale-3 column read back as 12.345). Text now
  reconciles to the column's declared scale; more fractional digits
  than the scale is a loud error (digits are never dropped), with
  precision overflow checked.
- **Whole-row validation before builder mutation** — a bad cell
  mid-row used to leave earlier arrow builders advanced, misaligning
  every subsequent row in the batch. `push` now validates the entire
  row (type pairing, text parses, scale/precision, int32 range)
  before appending; a rejected row contributes nothing.
- **Dropped attributes** — the schema omits them but the row carried
  them (cell/column count mismatch); the row loop skips them too.
- **Counted projection fields** — the reader projection is a JSON
  array instead of comma-separated names, which broke on column names
  containing commas.
- **typmod pass-through** — input conversions receive the attribute's
  real `atttypmod` instead of -1.

Live acceptance (fixture stack): `name`/`varchar`/`bytea`/`numeric(10,3)`
managed table written under `bytea_output=escape`, read back through
PostgreSQL AND an independent pyiceberg reader — bytes exact
(`b'\xde\x00\xad'`), decimals at scale 3, comma-containing names intact.
