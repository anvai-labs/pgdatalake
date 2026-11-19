# 02 — SQL Surface, Configuration, Metadata

## 1. Catalog management

Iceberg catalogs are registered server-wide objects, stored in a new catalog
`pg_iceberg_catalog` (name, type, options JSONB, credential *references* — never
secrets, see [09-security.md](09-security.md)).

```sql
-- REST (Polaris, Unity, Nessie-REST, S3 Tables, Glue-Lake-Formation-REST, custom)
SELECT iceberg.create_catalog('prod', type => 'rest',
           endpoint    => 'https://rest.iceberg.example.com/v1',
           warehouse   => 'anvai-warehouse',
           credential  => 'env:ICEBERG_PROD_TOKEN');   -- reference, resolved at use

-- AWS Glue (region-scoped)
SELECT iceberg.create_catalog('aws', type => 'glue', region => 'us-east-2');

-- Hadoop/Hive metastore (read-only metadata support in v1)
SELECT iceberg.create_catalog('hms', type => 'hadoop', uri => 'hdfs://nn:8020');

SELECT iceberg.alter_catalog('prod', options => '{...}');
SELECT iceberg.drop_catalog('prod', restrict => true);
SELECT * FROM iceberg.catalogs;
```

Rules:
- Catalog names are unique per server. A table's catalog is fixed at creation.
- `type => 'rest'` profiles are auto-detected from `GET /v1/config` (server
  capabilities header) and validated against the REST spec conformance list.
- Unnamed catalog: `SET iceberg.default_catalog = 'prod'` or GUC chain fallback;
  `location`-only managed tables use the built-in `sql` catalog if configured with
  `iceberg.sql_catalog_dsn` (v2 feature; v1 requires an explicit catalog).

## 2. External tables (attach)

```sql
CREATE FOREIGN TABLE analytics.events ()
  SERVER prod
  OPTIONS (table 'warehouse.events',                 -- fully-qualified Iceberg name
           branch 'main',                            -- optional: pin a ref
           'iceberg.mvcc' 'statement');              -- visibility model, see doc 07

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
USING iceberg
PARTITION BY RANGE (at)            -- translated to Iceberg partition spec
WITH (catalog = 'prod', location = 's3://anvai-warehouse/public/sessions',
      format_version = 3, target_file_size = '128MB',
      parquet_compression = zstd, write_mode = copy_on_write);
```

- `USING iceberg` + `DEFAULT_TABLE_ACCESS_METHOD = iceberg` makes it the default
  for a database (opt-in per-DB via ALTER DATABASE).
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
SELECT iceberg.write_to_branch('public.sessions', 'etl-2026-11-19', from => 'main');
```

DROP: `DROP TABLE` on managed tables deletes catalog entry and (async) removes the
Iceberg table from the catalog with `purge` semantics — governed by
`iceberg.drop_purge = true|false` (default: unpurge — table removed from catalog,
files left for orphan GC; explicit `SELECT iceberg.purge_table(...)` does hard
delete).

## 4. Time travel & refs (both table kinds)

```sql
SELECT * FROM analytics.events
  FOR TIMESTAMP AS OF '2026-11-01 00:00+00';   -- syntax via parser hook in AM/FDW
SELECT * FROM analytics.events FOR VERSION AS OF 8995342215659429317;
SELECT * FROM analytics.events FOR TAG AS OF 'weekly-2026-45';
SELECT iceberg.rollback('analytics.events', to_snapshot => 8995342215659429317);
```

Implemented as a planner-level hook producing a snapshot-pinned scan descriptor;
works identically for managed and external tables. `FOR …` clause is added in the
`select_no_parens` grammar path *within the allowlisted core patch* — kept to ~15
lines of grammar + a `ParseNamespaceItem` field, marked `[core-allowlist]`.

## 5. GUCs

```ini
iceberg.default_catalog = 'prod'          -- unset = error on unqualified USING
iceberg.enable = on
iceberg.max_open_files_per_scan = 64
iceberg.scan_task_target_size = '128MB'
iceberg.commit_retries = 8
iceberg.commit_retry_backoff = '100ms..10s'
iceberg.orphan_grace_period = '3d'
iceberg.maintenance_queue_concurrency = 2
iceberg.s3.max_connections, iceberg.s3.anonymous, iceberg.s3.endpoint_style
iceberg.adls.account_url, iceberg.gcs.endpoint
iceberg.log_scan_planning = off           -- EXPLAIN-friendly planning trace
iceberg.statement_snapshot = 'latest'     -- | snapshot id | 'tag:NAME'
```

All GUCs are `PGC_SUSET`-or-lower with per-role overrides; storage credentials are
**never** GUC values (references only) — [09-security.md](09-security.md).

## 6. Metadata views (`pg_iceberg` schema)

| View | Source | Notes |
|---|---|---|
| `pg_iceberg.catalogs` | `pg_iceberg_catalog` + live probe cache | includes `server_capabilities` |
| `pg_iceberg.tables` | join of pg_class + catalog metadata | `kind`, `format_version`, `location` |
| `pg_iceberg.snapshots` | catalog metadata per table | `committed_at`, `operation`, `summary` |
| `pg_iceberg.refs` | branches + tags | `is_branch`, `snapshot_id`, retention |
| `pg_iceberg.files` | manifests (lazily loaded, cost-gated) | per-file: path, size, record count, NDV stats |
| `pg_iceberg.partitions` | partition stats | files per partition, bytes |
| `pg_iceberg.delete_files` | manifests | `eq_delete` / `pos_delete` / `dv` counts |
| `pg_iceberg.maintenance_log` | local | per-table runs of rewrite/expire/orphan |

All views are permission-filtered to tables the user can `SELECT`.
