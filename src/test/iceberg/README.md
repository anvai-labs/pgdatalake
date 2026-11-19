# Apache Iceberg test fixtures & suites

Entry points (from the repo root, via the root `Makefile` forward):

| Target | What it does |
|---|---|
| `make iceberg-up` | `docker compose up --wait`: MinIO, Lakekeeper (REST catalog), Azurite, fake-gcs |
| `make iceberg-check` | `cargo test` (pgiceberg-core) + `meson test` (upstream regress + iceberg suites) |
| `make iceberg-conformance` | interop suite vs PyIceberg/Trino — M1+ |
| `make iceberg-down` | tear down containers + volumes |
| `make perf-iceberg` | perf gates (doc 10 §5) — M4+ |

## Endpoints after `iceberg-up` (fixture-local fake creds — never use real ones)

| Service | Endpoint | Credentials |
|---|---|---|
| MinIO S3 | `http://localhost:9000`, bucket `warehouse` | `iceberg-test` / `iceberg-test-password` |
| MinIO console | `http://localhost:9001` | same |
| Lakekeeper REST | `http://localhost:8181` (v1 config: `/catalog/v1/config`) | none (bootstrap bootstrap-user auth in M1) |
| Azurite blob | `http://localhost:10000` | `devstoreaccount1` / `Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq` (Azurite public dev key) |
| fake-gcs | `http://localhost:4443` | none |

`meson setup build -Diceberg=enabled && ninja` at the repo root builds the
server + the `iceberg` extension; see specs/iceberg/12-roadmap.md for the
milestone gates and specs/iceberg/RULES.md §9 for the smoke-test ceremony.
