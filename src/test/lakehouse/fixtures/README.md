# Fixtures — end-to-end behavior drills

Each `*-check.py` proves one behavior through **ordinary SQL** against a
fixture stack: a Lakekeeper-compatible REST catalog gateway, MinIO
(S3), Trino (independent writer/reader), and a PostgreSQL server built
from `build3/tmp_install`. Design rules live in
[specs/lakehouse/10-testing.md](../../../specs/lakehouse/10-testing.md);
generated data fixtures come from `scripts/gen_fixtures.py` (RULES §6 —
never commit data files or metadata.json copied from real lakes).

Per-script internals (what each drill proves, its environment, and its
gotchas) are in the
[internals guide](../../../src/backend/access/lakehouse/README.md#testing).

## Common environment

The fork fixture server listens on a unix socket dir + port 5599 with
its data at `/tmp/anvai-managed-cut/data` (adjust paths to your build
tree):

```
export PGHOST=/tmp/anvai-managed-cut-host PGPORT=5599
export PATH=$REPO/build3/tmp_install/usr/local/pgsql/bin:$PATH
export LD_LIBRARY_PATH=$REPO/build3/src/interfaces/libpq   # psql needs built libpq (PQsendPipelineSync)
# start if down:
pg_ctl -D /tmp/anvai-managed-cut/data -l /tmp/anvai-managed-cut/data/server.log \
    start -o "-p 5599 -k /tmp/anvai-managed-cut-host" -w
```

**The server loads `lakehouse.so` from `build3/tmp_install`** — after any
C edit: `ninja -C build3 src/backend/access/lakehouse/lakehouse.so`,
then `meson test -C build3 --suite postgresql:setup` (installs), then
restart the fixture server. Prove what a backend actually loaded via
`/proc/<backend_pid>/maps` when in doubt.

The publisher/recovery worker binary must be built first:
`cargo build --release -p pgdatalake-core --example bound_publication_fixture`
(pass it to drills as `--worker <path>`).

## Drill index

| Script | Proves |
|---|---|
| `managed-cut-sql-check.py` | The managed-cut executor drill: 0A000 baseline with zero GETs, held-snapshot trio with exact pin/overlay evidence (`managed_read_cut_evidence()`), rescan-retained-cut with zero resolutions, mid-window revocation (revoke between DECLARE and FETCH → 55000, zero rows), nested SPI identity, rotated-scope 55000 before first row |
| `managed-cut-check.py` | Module underneath the SQL drill: gateway purposes (`fixture-send`/`fixture-lookup`/`fixture-read`), reply-loss barrier, ledger snapshot helpers |
| `managed-capture-binding-check.py` | Enrollment/rotation lineage, WAL recovery of ledger + binding state (`--provider --restart-fixture "$PGHOST/data"`) |
| `bound-publication-restart.py` | Worker restart recovery + the Trino helper used by sibling drills |
| `publication-admission-check.py` / `publication-claim-check.py` / `publication-discovery-check.py` / `publication-intent-check.py` / `publication-ledger-load-check.py` | Send-side admission: claims, intents, discovery, bounded ledger loads |
| `native-managed-send-hold-check.py` / `native-managed-dispatch-check.py` / `native-managed-lookup-check.py` / `native-managed-prepare-check.py` | One-shot native dispatch: authorization hold, reply-loss barriers, lookup-only recovery, preparation ABI |
| `catalog-reply-loss-check.py` | The lost-reply barrier around catalog acceptance |
| `scoped-read-check.py` | Login runner semantics (`login()` returns lists of lines; `sqlstate=` kwarg) — imported by the SQL drills |
| `fdw-count-check.py` | Stock-PG19 read-only package: COUNT elision + pinned reads (needs a **separate stock PG19 cluster**, `COUNT_FORK_SMOKE=1` for the fork variant) |
| `pinned-reader-io-check.py` | Pinned reader request counts and bounded reads |
| `stock-fdw-check.py` | Stock FDW package read path |
| `publication_fixture.py` | Shared helpers: `sql()`, gateway/`HDR`/`SEG` constants |

## Gotchas (each cost real debugging time)

- Held REPEATABLE READ sessions must run an immediate `SELECT 1` to pin
  their snapshot.
- Held sessions' AccessShare locks block rotation's ACCESS EXCLUSIVE —
  COMMIT them before rotating.
- Login runners return **lists of lines**; assert against lists, not
  strings. Expected errors use `sqlstate="55P01"`-style codes, and the
  first output line must be the `ERROR:` line (bump `client_min_messages`
  only in sessions you do not assert on).
- Segments are **sealed**: payload bytes never change; scope rotation,
  not reclamation, produces the 55000 consistency shape.
- Savepoint anomalies (`3B001 savepoint does not exist`) mid-drill are
  downstream symptoms of an earlier unexpected error in the same
  session — fix the first error.
