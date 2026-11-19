# Apache Iceberg test fixtures & suites

Entry points (from the repo root, via the root `Makefile` forward):

| Target | What it does |
|---|---|
| `make iceberg-up` | `docker compose up --wait`: MinIO, Lakekeeper (REST catalog), Azurite, fake-gcs |
| `make iceberg-check` | `cargo test` (pgdatalake-core) + `meson test` (upstream regress + iceberg suites) |
| `make iceberg-conformance` | interop suite vs PyIceberg/Trino — M1+ |
| `make iceberg-down` | tear down containers + volumes |
| `make perf-iceberg` | perf gates (doc 10 §5) — M4+ |

## Endpoints after `lakehouse-up` (fixture-local fake creds — never use real ones)

| Service | Endpoint | Credentials |
|---|---|---|
| S3 (SeaweedFS) | `http://localhost:9000`, bucket `warehouse` | `lakehouse-test` / `lakehouse-test-password` (read-only: `lakehouse-reader` / `lakehouse-reader-password`) |
| Lakekeeper REST | `http://localhost:8181` (v1 config: `/catalog/v1/config`) | none (bootstrap-user auth configured in M1) |
| Azurite blob | `http://localhost:10000` | `devstoreaccount1` / `Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq` (Azurite public dev key) |
| fake-gcs | `http://localhost:4443` | none |

The S3 store is SeaweedFS (`chrislusf/seaweedfs`) — MinIO deleted its Docker
Hub images in Sept 2026; see the Superseded note in specs/lakehouse/10-testing.md §1.

`meson setup build -Ddatalake=enabled && ninja` at the repo root builds the
server + the `lakehouse` extension; see specs/lakehouse/12-roadmap.md for the
milestone gates and specs/lakehouse/RULES.md §9 for the smoke-test ceremony.
