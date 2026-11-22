# Stock PostgreSQL 19 read-only FDW

This source-build preview targets Linux and an unmodified PostgreSQL 19 installation with PGXS and server headers. It registers the `lakehouse` extension (version `1.0-fdw`), the `lakehouse_fdw` wrapper and `lakehouse.version()`. It uses the same Iceberg reader as the fork. Use a separate PostgreSQL installation prefix for the managed distribution, which has its own `lakehouse.control` and extension upgrade graph.

From this directory, with GNU Make 4.3+, a C compiler, Python 3, rustup and cbindgen on PATH:

```sh
make -j4 PG_CONFIG=/path/to/postgresql-19/bin/pg_config
make install PG_CONFIG=/path/to/postgresql-19/bin/pg_config
```

Set `CARGO_TARGET_DIR` on the build if reusing a Rust build cache. The build uses the workspace toolchain and Cargo.lock. For the following SQL, replace the endpoints, warehouse, table, role and example snapshot with the values from your catalog. Supply credentials through your normal secret provisioning process.

```sql
CREATE EXTENSION lakehouse;
CREATE SERVER analytics FOREIGN DATA WRAPPER lakehouse_fdw
  OPTIONS (endpoint 'https://catalog.example/catalog', warehouse 'analytics',
           s3_endpoint 'https://storage.example');
CREATE USER MAPPING FOR CURRENT_USER SERVER analytics
  OPTIONS (s3_access_key '<access key>', s3_secret_key '<secret key>');
CREATE FOREIGN TABLE events_at_snapshot (id bigint) SERVER analytics
  OPTIONS (table 'events.daily', snapshot_id '123456789');
SELECT id FROM events_at_snapshot ORDER BY id;
```

Omit `snapshot_id` to resolve the current table state at execution. A stock read uses foreign table options. Native `FOR ... AS OF` grammar belongs to the fork. The package exposes serial foreign scans. Parallel workers require a separate proof of shared table UUID and snapshot identity across ref movement, including an empty ref. There is no released stock predecessor or cross-flavor in-place upgrade. The first live fixture covers a Lakekeeper REST catalog, S3-compatible storage, v2 position deletes and a schema-only dump/restore. See the [release gates](../../../../../specs/lakehouse/modules/08a-core-release-gates.md) for the remaining qualification work.
