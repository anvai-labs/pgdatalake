# 09 — Security: Credentials, Cloud Auth, RBAC

## 1. Golden rule: no secrets in SQL text or system catalogs

Credentials enter the server only through:
1. **environment variables** of the postmaster/backend (`AWS_*`, `AZURE_*`,
   `GOOGLE_*` or custom, referenced as `env:NAME`),
2. **credential files** with 0600 perms, referenced as `file:/path#key`,
3. **cloud identity** (IMDS / workload identity / Managed Identity) — nothing
   stored at all,
4. `~/.aws`, ADC, Entra SDK chains (equivalent to 3),
5. psql-session-supplied vault secrets via `lakehouse.set_credential('prod',
   token => …)` (session-local, never persisted, SUPERUSER/role-with-grant only).

Catalog options store **references** (`credential_ref => 'env:ICEBERG_PROD'`).
`pg_lakehouse_catalog` and `\d+` output redact anything matching
`(?i)(secret|token|password|key|credential)` in options JSONB — defense in depth.

## 2. Per-cloud credential chains (resolution order)

**AWS (S3, Glue, REST-with-SigV4, S3 Tables):**
env (AWS_ACCESS_KEY_ID/SECRET/SESSION_TOKEN) → shared config/credentials profile
(`lakehouse.aws_profile`) → container/IRSA web-identity → IMDSv2 (token-based,
hop limit respected). STS AssumeRole per catalog option `aws_role_arn` (and
`aws_role_session_name`). Region resolved: option > profile > env > IMDS.

**Azure (ADLS Gen2, Entra-secured REST catalogs):**
Entra SDK chain — client secret/cert from env (`AZURE_CLIENT_SECRET`+TENANT+CLIENT)
→ Managed Identity (system/user-assigned, `azure_msi_client_id` option) → Azure
CLI cred file (dev) → workload identity federation (AKS). Storage-scoped tokens
are requested per-scope (`https://storage.azure.com/.default`); catalog tokens
per `account_url`.

**GCS (GCS, GCP-secured REST):**
service-account key file (`GOOGLE_APPLICATION_CREDENTIALS` or
`gcs_credential_ref`) → VM metadata server → gcloud ADC → impersonated service
account (`target_service_account` option) for least-privilege delegation.

**MinIO / on-prem:** static access/secret from env/file only. Anonymous access
supported (`lakehouse.s3.anonymous = on`) for open dev buckets.

## 3. Catalog auth

| Catalog | Mechanism |
|---|---|
| REST generic | OAuth2 client-credentials (client_id/client_secret refs), bearer token ref, or none |
| REST + AWS | SigV4 request signing (SigV4a supported for S3 Tables multi-region) |
| REST + Polaris | OAuth2 + principal-credential; principal-tag propagation option |
| REST + Unity | OAuth2 M2M (Entra/Databricks token endpoint) |
| Glue | SigV4, optionally Lake Formation credentials-vending (`getTransientCredentials`) — v2 |

Token refresh: proactive at 80% TTL, single-flight per backend, retry on 401 once
then ERROR. Tokens live only in backend-local memory (never in shared memory).

## 4. Postgres-side RBAC

- `CREATE FOREIGN TABLE ... SERVER prod` requires `USAGE` on the foreign server
  (standard PG); iceberg catalogs are exposed as FDW servers internally, so the
  same grant model applies — `lakehouse.create_catalog` grants `USAGE` to `PUBLIC`
  only if `lakehouse.public_catalog_usage = on` (default off).
- Managed tables: full PG GRANT model (`SELECT/INSERT/UPDATE/DELETE/TRUNCATE`)
  enforced by the AM hooks; `TRUNCATE` = fast append-only overwrite commit
  (`OverwriteFiles` on all files) — not a metadata delete.
- Row/column security on external tables: v1 OFF (`CREATE FOREIGN TABLE … RLS`
  → warning "evaluated in Postgres after fetch — do not rely for data
  exfiltration protection across tenants"). Column grants work (projection
  respects ACLs). Row-level security on managed tables: works (enforced in
  AM scan, rows filtered before slot return).
- Superuser-only operations: catalog create/drop, credential resolution changes,
  maintenance force flags.

## 5. Threat notes

- Server-side request forgery: catalog endpoints and storage endpoints validated
  against `lakehouse.allowed_hosts` (default: no restriction for superuser-set
  catalogs; restrict for non-superuser catalog creation).
- Redirect handling: HTTP clients pinned to https (except explicit http endpoints
  for MinIO dev); no cross-host redirects.
- Audit: all catalog create/drop/rename and all maintenance actions write to the
  Postgres log with `LOG` + `pg_audit`-compatible structure when the extension is
  present.
- Immutability abuse: impossible to write into a table whose catalog ref moved —
  AssertRefSnapshotId on every commit prevents "lost update by stale plan".

## Managed-table credentials (2026-11-20, GPT-6 review finding)

AM options live in pg_class.reloptions and are exposed by pg_dump,
schema dumps, and plain catalog reads. Therefore:

- WITH-options on managed tables MUST NOT carry credential values
  (token, s3_access_key, s3_secret_key). The parser rejects them with
  a pointing hint (implemented, slice 28-prep).
- Interim state: RESOLVED for the managed side (13-review #6): the
  attach row carries identity + server oid only; credentials resolve
  from USER MAPPINGS on the referenced server (postgres_fdw pattern).
- Tracked as the doc 09 workstream; blocks M2's production-readiness
  claim, not the M2 gate itself (fixture credentials are fake per
  RULES §6).

## FDW option authority (2026-11-20, 13-review blocker #6)

Each option has exactly one legal home, enforced at DDL by
`lakehouse.lakehouse_fdw_validator` (shipped wired onto the canonical
`lakehouse_fdw` wrapper) and enforced AGAIN on use by `ExtractOptions`,
so legacy pre-validator definitions error instead of silently using
credentials that were never mapped:

| Home | Options | Rationale |
|---|---|---|
| SERVER | `endpoint`, `warehouse`, `s3_endpoint` | transport identity; dumpable by design, contains no secrets |
| USER MAPPING | `token`, `s3_access_key`, `s3_secret_key` | per-user, ACL-controlled, one place to rotate |
| FOREIGN TABLE | `table`, `snapshot_id` | relation identity + time-travel pin |

- A table-level `s3_endpoint` is rejected: a storage destination the
  server's credentials were not scoped to (credential/destination
  repurposing).
- Foreign tables resolve credentials from the user mapping of the
  **invoking user** (postgres_fdw precedent). View semantics: a view
  reader scans under their OWN mapping — a view owner who wants
  delegated access maps their readers on the server; a missing mapping
  is a loud `user mapping not found` at plan time, never anonymous
  access.
- `lakehouse.scan_files` (caller-chosen endpoint + credentials =
  arbitrary-egress primitive) and `lakehouse.create_remote` (external
  DDL; ownership checked in C) are `REVOKE`d from PUBLIC; grant
  explicitly to operators only.
- The drop-gc event trigger must not interfere with unrelated DDL —
  the GC only touches rows for relations the drop actually removed;
  ordinary owners drop their own heap tables unaffected (regress:
  fdw_auth).
