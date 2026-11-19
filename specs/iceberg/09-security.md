# 09 — Security: Credentials, Cloud Auth, RBAC

## 1. Golden rule: no secrets in SQL text or system catalogs

Credentials enter the server only through:
1. **environment variables** of the postmaster/backend (`AWS_*`, `AZURE_*`,
   `GOOGLE_*` or custom, referenced as `env:NAME`),
2. **credential files** with 0600 perms, referenced as `file:/path#key`,
3. **cloud identity** (IMDS / workload identity / Managed Identity) — nothing
   stored at all,
4. `~/.aws`, ADC, Entra SDK chains (equivalent to 3),
5. psql-session-supplied vault secrets via `iceberg.set_credential('prod',
   token => …)` (session-local, never persisted, SUPERUSER/role-with-grant only).

Catalog options store **references** (`credential_ref => 'env:ICEBERG_PROD'`).
`pg_iceberg_catalog` and `\d+` output redact anything matching
`(?i)(secret|token|password|key|credential)` in options JSONB — defense in depth.

## 2. Per-cloud credential chains (resolution order)

**AWS (S3, Glue, REST-with-SigV4, S3 Tables):**
env (AWS_ACCESS_KEY_ID/SECRET/SESSION_TOKEN) → shared config/credentials profile
(`iceberg.aws_profile`) → container/IRSA web-identity → IMDSv2 (token-based,
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
supported (`iceberg.s3.anonymous = on`) for open dev buckets.

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
  same grant model applies — `iceberg.create_catalog` grants `USAGE` to `PUBLIC`
  only if `iceberg.public_catalog_usage = on` (default off).
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
  against `iceberg.allowed_hosts` (default: no restriction for superuser-set
  catalogs; restrict for non-superuser catalog creation).
- Redirect handling: HTTP clients pinned to https (except explicit http endpoints
  for MinIO dev); no cross-host redirects.
- Audit: all catalog create/drop/rename and all maintenance actions write to the
  Postgres log with `LOG` + `pg_audit`-compatible structure when the extension is
  present.
- Immutability abuse: impossible to write into a table whose catalog ref moved —
  AssertRefSnapshotId on every commit prevents "lost update by stale plan".
