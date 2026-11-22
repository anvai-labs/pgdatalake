# 04 — Object Storage File IO: S3, ADLS Gen2, GCS, MinIO

This is a design inventory. The [AWS, Azure and Google Cloud profiles](modules/09-cloud-profiles.md) specify release evidence, identity and create-only semantics for each API. The #45 core provides the S3/S3-compatible and test-local path; ADLS Gen2 and GCS are proposed adapters.

All byte-level IO goes through one abstraction in `pgdatalake-core` backed by the
`object_store` crate (Apache Arrow project, same engine behind DataFusion), with a
Postgres-facing **IO context** so that every read/write runs under the right
resource accounting (statement timeout, `maintenance_work_mem` for writers,
cancel/die checks per batch for backend, per-task for bgworkers).

## 1. URI schemes

| Scheme | Store | Example |
|---|---|---|
| `s3://` | S3 / MinIO / R2 / any S3-compatible | `s3://anvai-warehouse/tbl/data/…` |
| `abfss://` | ADLS Gen2 (HNS) | `abfss://container@account.dfs.core.windows.net/tbl/…` |
| `abfs://` | Outside the initial secure Azure profile | use an explicitly qualified secure endpoint |
| `gs://` | GCS | `gs://anvai-warehouse/tbl/…` |
| `file://` | local FS | MinIO on localhost, tests |

S3 endpoint override (`lakehouse.s3.endpoint` per catalog option or GUC) is how MinIO
and R2 are reached: `endpoint = 'http://minio.local:9000'`, `path_style_access =
true` (required for MinIO). Run the same conformance inputs on each supported endpoint. An S3-compatible fixture supplies protocol evidence; production S3 qualification also requires real AWS identity and storage tests.

## 2. Capability matrix

| Capability | S3 | MinIO | ADLS Gen2 | GCS |
|---|---|---|---|---|
| Auth chains | env/IMDS/STS assume-role/web-identity/MinIO static | static/AssumeRole(minio) | Entra SDK chain (Managed Identity in Azure VMs, client-secret, workload-ID federation) | ADC/metadata-server/SA-key/impersonated SA |
| Atomic rename | no (copy+delete emulation in catalogs only; we never rely on it) | no | no | no |
| Conditional Put (`If-None-Match: *`) | yes | yes | yes (ETag) | yes (`ifGenerationMatch=0`) |
| Multipart upload | yes | yes | yes (block upload) | yes (resumable session) |
| Listing with prefix+delimiter | yes | yes | yes | yes |
| Range reads | yes | yes | yes | yes |
| Server-side checksum | CRC32C (v2 reads sha256 trailer on v1?) — use CRC32C | MD5 | MD5/CRC64 | CRC32C |

**Rule: no operation in the writer ever depends on rename; all durability comes
from the catalog commit (doc 07).** Conditional PUT is used only for metadata.json
write by Hadoop-catalog mode (read-only anyway in v1) — REST/Glue catalogs handle
atomicity server-side.

## 3. Read path requirements

- Range GETs sized to row-group footer granularity: footers fetched first
  (last `footer_read_size` = 256KB, extend on miss), column chunks fetched per
  row-group group of parallel tasks.
- Prefetch depth: `lakehouse.io.prefetch = 4` concurrent chunk GETs per task; cancel
  (`CHECK_FOR_INTERRUPTS`) honored between chunk boundaries, never mid-request.
- Retry policy: exponential backoff with jitter, honor `Retry-After` on 503,
  S3 `SlowDown`, and ADLS `ServerBusy` (503); max 5 retries → ERROR
  `iceberg_io_failure` with the store's request id in DETAIL (S3
  `x-amz-request-id`, ADLS `x-ms-request-id`, GCS `X-GUploader-UploadID`).
- Checksums validated on full-object reads; range reads skip checksums (Parquet's
  page-level CRC32 remains the integrity bound).

## 4. Write path requirements

- All writes are **new objects** (`tbl/data/<uuid>.parquet`,
  `tbl/metadata/<uuid>-<n>.metadata.json`, Puffin DV blobs
  `tbl/data/<uuid>.puffin`). Deterministic layout follows Iceberg table
  properties (`write.data.path`, `write.metadata.path`) when the catalog supplies
  them; otherwise `<location>/data|metadata/`.
- Multipart upload with part size `write.part_size` (default 16MB, MinIO-tested at
  5MB minimum); abort-on-error releases uploaded parts (S3 `AbortMultipartUpload`,
  ADLS block-id TTL handles itself, GCS session delete).
- Object naming: UUIDv7 filenames (time-ordered, sortable, avoids listing hotspots
  in partition prefixes).
- No overwrite of any existing object, ever — a repeated filename is an ERROR
  (collision = corrupted previous commit attempt, surface loudly).

## 5. Per-store configuration surface

Credentials are **resolved at connection of the IO context**, not at GUC set time
(doc 09). Store-scoped GUCs per catalog option take precedence:

```sql
SELECT lakehouse.alter_catalog('minio', options => '{
  "endpoint": "http://minio.local:9000",
  "path_style_access": true,
  "region": "us-east-1",
  "credential_ref": "env:MINIO_ROOT"
}');
```

| Store | Options | GUC fallback |
|---|---|---|
| S3/MinIO | endpoint, region, path_style_access, credential_ref, request_payer, checksum_algorithm | `lakehouse.s3.*` |
| ADLS | account_url, filesystem, authority_host, credential_ref, azure_tenant_id | `lakehouse.adls.*` |
| GCS | endpoint, bucket_bound_host, credential_ref, target_service_account | `lakehouse.gcs.*` |

Virtual-host vs path style auto-detect: path style when endpoint is IP/localhost.
GCS `gs://` bucket mapping through ADLS-style hierarchical namespaces is rejected
early with a clear message.

## 6. Tunables

| Name | Default | Effect |
|---|---|---|
| `lakehouse.io.prefetch` | 4 | concurrent chunk reads per scan task |
| `write.part_size` | 16MB | multipart part size |
| `write.target_file_size` | 128MB | Parquet file roll-over target |
| `write.row_group_size` | 1M rows | Parquet row-group rows |
| `write.parquet_page_size` | 1MB | Parquet page bytes |
| `lakehouse.io.max_concurrency` | 64 | process-wide in-flight object ops cap (semaphore) |

The process-wide semaphore exists because Postgres backends are processes: without
a cap, 100 backends × 4 prefetch = 400 concurrent connections per store — most
SDKs pool per handle, so the handle is per-backend with the semaphore shared via
the DSM/IPC layer only for bgworker coordination (backends self-limit via
`max_concurrency` division).

## 7. Transport policy (2026-11-20, 13-review blocker #6)

Catalogs and manifests supply absolute paths, so every hop is scoped to
the table's own location root and the store surface is constrained:

- **Table-root scope gate** (`ensure_within_table_root`): the manifest
  list, every manifest, and every data/delete file path must share the
  table location's scheme/bucket and live under its key prefix
  (segment-aware — `s3://b/a` does not admit `s3://b/ab`). A violation
  is a loud decode error, never a best-effort read from an unscoped
  location. The catalog-supplied `metadata_location` itself is checked
  only after the metadata is parsed (its location IS the scope anchor's
  source).
- **Local object stores are default-deny.** `file://` paths from
  catalogs/manifests are refused unless the embedding process opted
  in — and the opt-in API is test-only (`cfg(test)`), so the shipped
  binary structurally cannot open local paths. Explicit local-store
  deployments arrive with the M3 storage-profile work as a real
  operator-facing option.
- **Static S3 credentials are REQUIRED.** The ambient provider chain
  (env vars, IMDS probes) is removed: missing keys are a loud config
  error pointing at the user mapping — never a silent instance-role
  trust. This is stricter than the review's "require explicit trust
  for ambient credentials": there is no ambient path at all until a
  profile slice adds an operator-facing opt-in.
- **Plain HTTP only for explicit `http://` endpoints** (fixture
  stores); unconfigured endpoints mean AWS — TLS-only.
- **Redaction (implemented 2026-11-20):** `redact_url` masks userinfo
  and query values in every REST error message; scheme/host/path stay
  for diagnosis.
- **Redirect policy (implemented):** same-origin per hop — a catalog
  redirect that crosses origins fails the request ("catalog redirect
  crosses origins ... refused") instead of silently following and
  sending the bearer token elsewhere.
- **OAuth (implemented):** `TokenProvider::token()` is loud — a failed
  fetch with no cache errors the request (never an empty bearer), and
  a cached token is served stale only within its hard TTL; past TTL a
  refresh failure fails the request.
- **Typed conflicts (implemented):** REST 409s surface as
  `RestError::Conflict` -> `StorageError::Conflict` -> FFI err_kind=1;
  callers branch on the KIND — never by searching messages for
  "409" (create_remote's string search is gone).
