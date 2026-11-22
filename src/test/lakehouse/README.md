# Apache Iceberg test fixtures & suites

Entry points (from the repo root, via the root `Makefile` forward):

| Target | What it does |
|---|---|
| `make lakehouse-up` | bring up: SeaweedFS S3, Lakekeeper (REST catalog), Azurite, fake-gcs |
| `make lakehouse-check` | `cargo test` (pgdatalake-core) + `meson test` (upstream regress + lakehouse suites) |
| `make lakehouse-conformance` | interop suite vs PyIceberg/Trino — M1+ |
| `make lakehouse-down` | tear down containers + volumes |
| `make perf-lakehouse` | perf gates (doc 10 §5) — M4+ |

## Endpoints after `lakehouse-up` (fixture-local fake creds — never use real ones)

| Service | Endpoint | Credentials |
|---|---|---|
| S3 (SeaweedFS) | `http://localhost:9000`, bucket `warehouse` | `lakehouse-test` / `lakehouse-test-password` (read-only: `lakehouse-reader` / `lakehouse-reader-password`) |
| Lakekeeper REST | `http://localhost:8181` (v1 config: `/catalog/v1/config`) | none (bootstrap-user auth configured in M1) |
| Azurite blob | `http://localhost:10000` | `devstoreaccount1` / `Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==` (Azurite public dev key) |
| fake-gcs | `http://localhost:4443` | none |

The S3 store is SeaweedFS (`chrislusf/seaweedfs`) — MinIO deleted its Docker
Hub images in Sept 2026; see the Superseded note in specs/lakehouse/10-testing.md §1.

`meson setup build -Ddatalake=enabled && ninja` at the repo root builds the
server + the `lakehouse` extension; see specs/lakehouse/12-roadmap.md for the
milestone gates and specs/lakehouse/RULES.md §9 for the smoke-test ceremony.

## Bound FDW read pilot

Start the fixture services and Trino, then run `python3 src/test/lakehouse/fixtures/scoped-read-check.py`
with `PGHOST`, `PGPORT`, `PGUSER` and `PSQL` pointing to a disposable local
cluster. The fixture needs a superuser to create databases and mappings and a
local authentication rule that allows the two test login roles. Native CI runs
it on both packages. The fixture grants are in `fixtures/seaweedfs-s3.config.json`.
For an isolated gateway backed by the same fixture store, set
`SCOPED_READ_STORE_ADDR` to its `host:port`.

The smoke prints one JSON record per tenant case with the exact requested pin,
row multiplicity, token and storage-key fixture labels, timings, provider
403s, and catalog, object and forbidden-root request counts. A provider 403
incurs a real request to the enrolled object. A PostgreSQL mapping refusal
emits zero network requests.
