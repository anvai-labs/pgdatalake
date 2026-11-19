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
