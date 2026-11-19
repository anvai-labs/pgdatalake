# 02 — SQL Surface, Configuration, Metadata

## 1. Catalog management

Iceberg catalogs are registered server-wide objects, stored in a new catalog
`pg_lakehouse_catalog` (name, type, options JSONB, credential *references* — never
secrets, see [09-security.md](09-security.md)).

```sql
-- REST (Polaris, Unity, Nessie-REST, S3 Tables, Glue-Lake-Formation-REST, custom)
SELECT lakehouse.create_catalog('prod', type => 'rest',
           endpoint    => 'https://rest.lakehouse.example.com/v1',
           warehouse   => 'anvai-warehouse',
           credential  => 'env:ICEBERG_PROD_TOKEN');   -- reference, resolved at use

-- AWS Glue (region-scoped)
SELECT lakehouse.create_catalog('aws', type => 'glue', region => 'us-east-2');

-- Hadoop/Hive metastore (read-only metadata support in v1)
SELECT lakehouse.create_catalog('hms', type => 'hadoop', uri => 'hdfs://nn:8020');

SELECT lakehouse.alter_catalog('prod', options => '{...}');
SELECT lakehouse.drop_catalog('prod', restrict => true);
SELECT * FROM lakehouse.catalogs;
```

Rules:
- Catalog names are unique per server. A table's catalog is fixed at creation.
- `type => 'rest'` profiles are auto-detected from `GET /v1/config` (server
  capabilities header) and validated against the REST spec conformance list.
- Unnamed catalog: `SET lakehouse.default_catalog = 'prod'` or GUC chain fallback;
  `location`-only managed tables use the built-in `sql` catalog if configured with
  `lakehouse.sql_catalog_dsn` (v2 feature; v1 requires an explicit catalog).

## 2. External tables (attach)

```sql
CREATE FOREIGN TABLE analytics.events ()
  SERVER prod
  OPTIONS (table 'warehouse.events',   -- fully-qualified Iceberg table name
           branch 'main',              -- optional: pin a ref
           'lakehouse.mvcc' 'statement');  -- visibility model, see doc 07

IMPORT FOREIGN SCHEMA warehouse
  FROM SERVER prod INTO analytics
  LIMIT TO (events, clicks);
```

Options (all optional):
| Option | Meaning | Default |
|---|---|---|
| `table` | fully-qualified Iceberg table name | required |
| `branch` / `tag` | pin reads/writes to a ref | catalog default ref |
| `snapshot_id` | pin reads to a snapshot (read-only) | latest |
| `format_version` | max accepted metadata version | v3 |
| `readonly` | forbid DML at FDW level | false |
| `scan_tasks_per_worker` | parallel scan granularity | 8 |

`ALTER FOREIGN TABLE … OPTIONS (SET branch 'audit-2026')` re-pins without re-attach.

## 3. Managed tables

```sql
CREATE TABLE public.sessions (
    sid     bigint,
    uid     int NOT NULL,
    payload jsonb,
    at      timestamptz NOT NULL,
    country text
)
USING lakehouse FORMAT iceberg
PARTITION BY RANGE (at)            -- translated to Iceberg partition spec
WITH (catalog = 'prod', location = 's3://anvai-warehouse/public/sessions',
      format_version = 3, target_file_size = '128MB',
      parquet_compression = zstd, write_mode = copy_on_write);
```

- `USING lakehouse FORMAT iceberg` + `DEFAULT_TABLE_ACCESS_METHOD = lakehouse` makes it the
  default for a database (opt-in per-DB via ALTER DATABASE).
- Iceberg partition spec derived from `PARTITION BY`; supported partition
  transforms v1: identity, year, month, day, hour, bucket[N], truncate[W].
  Syntax: `PARTITION BY RANGE (month(at)), HASH (bucket(16, uid))` — parsed by the
  AM's DDL hook, not by core grammar (core only validates identifier lists).
- Unsupported columns/types are rejected at CREATE time with a clear error and a
  suggested mapping ([05-types.md](05-types.md)).

```sql
ALTER TABLE public.sessions SET (target_file_size = '64MB');
ALTER TABLE public.sessions ADD COLUMN ip inet;          -- schema evolution
ALTER TABLE public.sessions SET PARTITION BY ...         -- spec evolution (new specs, v2 style)
SELECT lakehouse.write_to_branch('public.sessions', 'etl-2026-11-19', from => 'main');
```

DROP: `DROP TABLE` on managed tables deletes catalog entry and (async) removes the
Iceberg table from the catalog with `purge` semantics — governed by
`lakehouse.drop_purge = true|false` (default: unpurge — table removed from catalog,
files left for orphan GC; explicit `SELECT lakehouse.purge_table(...)` does hard
delete).

## 4. Time travel & refs (both table kinds)

```sql
SELECT id FROM analytics.events
  WHERE id < 100
  FOR TIMESTAMP AS OF '2026-11-01 00:00+00';
SELECT id FROM analytics.events FOR VERSION AS OF 8995342215659429317;
SELECT id FROM analytics.events FOR TAG AS OF 'weekly-2026-45';
SELECT lakehouse.rollback('analytics.events', to_snapshot => 8995342215659429317);
```

*(Amended 2026-11-19, slice 19 — implementation replaces the original
"parser hook + ParseNamespaceItem field" sketch below.)*

**Implemented surface (slice 19).** The `FOR … AS OF` clause parses in the
**locking-clause position** (same slot as `FOR UPDATE`: after WHERE/ORDER BY,
before LIMIT). It cannot attach to a single table_ref: after a from-item a
bare `FOR` already starts the row-locking clause, and LALR(1) cannot
distinguish the two — so the pin is a query-level clause instead.

Semantics:

- The pin applies to **every lakehouse foreign table of that SELECT**
  (plain tables are simply unpinned; other FDWs ignore it). One pin per
  SELECT — it rides in `SelectStmt.lockingClause` as a raw `DefElem`, and
  consumers of that list skip `DefElem` entries.
- Per-table pinning stays available via the `snapshot_id` foreign-table
  option (slice 18); a query-level `FOR … AS OF` wins over it.
- `FOR TIMESTAMP AS OF` accepts one constant timestamp string (resolved at
  parse-analysis time via `timestamptz_in` into epoch milliseconds, so a
  stored view's meaning does not drift with the session timezone); it
  selects the **latest snapshot at or before** that timestamp.
- `FOR VERSION AS OF` accepts an integer snapshot id (int64; ids beyond
  int32 lex as float literals and are validated at parse time).
- `FOR TAG AS OF` accepts a quoted ref name — tags and branches both name a
  snapshot (the metadata `refs` map is the source of truth).
- A selector matching no snapshot (unknown id/ref, timestamp before the
  first snapshot) fails at plan time with `object not found`; invalid
  literals fail at parse-analysis time.
- The pin survives rule storage (views/rules keep it verbatim — the RTE
  carries the canonical selector string `ts:<ms>` / `id:<n>` / `ref:<name>`).
- Not applicable to bare `VALUES` or to set operations (the grammar only
  accepts it in the plain-select position).

Implemented in the `[core-allowlist]` patch: `gram.y` (one alternative in
`for_locking_clause`, ~12 lines), `kwlist.h` (`TAG` unreserved keyword),
`parse_clause.c` (selector resolution + `applyLakehouseTimeTravel`),
`parse_relation.c` (DefElem skip in `isLockedRefname`), `analyze.c`
(DefElem skips + apply call), RTE field `lakehouse_snapshot`
(`parsenodes.h` + `outfuncs/readfuncs`), catversion bump.

*(Original design — superseded:)* planner-level hook producing a
snapshot-pinned scan descriptor; works identically for managed and external
tables. `FOR …` clause was sketched as ~15 lines of grammar plus a
`ParseNamespaceItem` field. The TableAM (managed tables) path will consume
the same RTE field once M2 lands; external-table semantics above are final.

## 5. GUCs

```ini
lakehouse.default_catalog = 'prod'          -- unset = error on unqualified USING
lakehouse.enable = on
lakehouse.max_open_files_per_scan = 64
lakehouse.scan_task_target_size = '128MB'
lakehouse.commit_retries = 8
lakehouse.commit_retry_backoff = '100ms..10s'
lakehouse.orphan_grace_period = '3d'
lakehouse.maintenance_queue_concurrency = 2
lakehouse.s3.max_connections, lakehouse.s3.anonymous, lakehouse.s3.endpoint_style
lakehouse.adls.account_url, lakehouse.gcs.endpoint
lakehouse.log_scan_planning = off           -- EXPLAIN-friendly planning trace
lakehouse.statement_snapshot = 'latest'     -- | snapshot id | 'tag:NAME'
```

All GUCs are `PGC_SUSET`-or-lower with per-role overrides; storage credentials are
**never** GUC values (references only) — [09-security.md](09-security.md).

## 6. Metadata views (`pg_lakehouse` schema)

| View | Source | Notes |
|---|---|---|
| `pg_lakehouse.catalogs` | `pg_lakehouse_catalog` + live probe cache | includes `server_capabilities` |
| `pg_lakehouse.tables` | join of pg_class + catalog metadata | `kind`, `format_version`, `location` |
| `pg_lakehouse.snapshots` | catalog metadata per table | `committed_at`, `operation`, `summary` |
| `pg_lakehouse.refs` | branches + tags | `is_branch`, `snapshot_id`, retention |
| `pg_lakehouse.files` | manifests (lazily loaded, cost-gated) | per-file: path, size, record count, NDV stats |
| `pg_lakehouse.partitions` | partition stats | files per partition, bytes |
| `pg_lakehouse.delete_files` | manifests | `eq_delete` / `pos_delete` / `dv` counts |
| `pg_lakehouse.maintenance_log` | local | per-table runs of rewrite/expire/orphan |

All views are permission-filtered to tables the user can `SELECT`.
