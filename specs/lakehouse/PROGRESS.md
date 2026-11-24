# Lakehouse progress and release gates

## Current dependency order

Public #91 at `8765e97f4ba8015171178ff39067b6b47cdc1ded` qualifies separate
fresh proof and original-snapshot payload access. Public #90 at
`c104bd72f9058215ce5018f90c7a9d188aa8974a` bounds and reuses
the approved scoped catalog observation. Public #89 at
`0dc85911759b8880aa59424289c174d473719e5c` qualifies the
purpose-scoped native session credential adapter. Public #88 at
`32dc215a402312739bbd7b38caf693144f7d2067` qualifies the accepted-child proof. Its
[native CI](https://github.com/anvai-labs/pgdatalake/actions/runs/45187258595)
runs the lost-reply fixture. Public #87
qualifies both one-shot reply-loss cases on one primary. Public #86 adds
the opt-in 1.15 read evidence schema. Public #85 supplies the
[stock and prepare-only CI receipt](https://github.com/anvai-labs/pgdatalake/actions/runs/45139266582).
The #81 foundation has its own [CI receipt](https://github.com/anvai-labs/pgdatalake/actions/runs/45112681173).
The first independently shippable path is a serial, read-only FDW on stock
PG19.0. The managed writer and its visibility proof can advance separately.

| Track | Established evidence | Next dependency and exit |
|---|---|---|
| Stock reader, 01/02/06 | #53 supplies execution-time FDW COUNT and same-pin fallback. #54/#58/#61 cover effective mapping, enrolled root, real SQL logins, rotation and revocation. #70 runs the [TLS smoke](../../src/test/lakehouse/fixtures/stock-scoped-tls.sh) for those reads and reconnect on verified PG-wire with SCRAM. | The scoped reader now bounds catalog and metadata reads and reuses the approved observation for exact-pin planning. The ordinary external pilot needs a deployed client-network, login and grant-generation test. Governed provider identity follows its own gate. Preserve ordinary/pinned count and denied-sibling controls. [External client handoff](modules/10-downstream-handoff.md#external-postgresql-client-handoff) |
| Governed tables, 01/02/06/08 | #67–69 define BI reuse, provider scope and a verifier for 12 positive SQL executions and six refusals. Current governed results come from synthetic evidence. | Implement and run one provider's protected plan on a real enrolled table. Require exact source pin, actual consumption, masks and distinct effective scopes. Only then qualify reuse and multiple readers. [Verifier](modules/06c-governed-published-read.md) |
| Managed send and recovery, 01/04 | #75–79 qualified discovery, scoped credentials, bounded ledger loads, stale-work refusal and native lookup/finalize after WAL restart. #80 qualified a native WAL authorization hold with zero provider requests and a lookup-only receipt. #81 rejects assigned launcher XIDs, bounds ledger-lock waits and qualifies a process-local Rust prepare handle that can be dropped without a catalog POST. #83 adds the [C preparation ABI](modules/04b-scoped-send-admission.md#process-local-native-preparation-abi), which exposes the exact bounded proposal. #85’s opt-in native 1.14 diagnostic joins role-bound static Send credentials to it after closing the ledger transaction, then discards the proposal. Opt-in 1.16 [dispatches once](modules/04b-scoped-send-admission.md#native-one-shot-dispatch) through the live handle after durable authorization and proves both reply-loss barriers with the static mapping fixture. Opt-in 1.17 supplies separate purpose-checked native session credentials and rejects mapping rotation during preparation. | Qualify actual provider issuance, renewal and revocation, hard-disable fencing, bounded polling and primary promotion. Keep recovered attempts lookup-only. [Publisher handoff](modules/04b-scoped-send-admission.md#native-worker-integration) |
| Managed read visibility, 01/03 | #80 qualified the compatible-pin selector under three PostgreSQL snapshots. #81 certifies bounded Iceberg v2 explicit-`main` parent links with real Lakekeeper metadata. #84 rewinds retained cursors and fixes a missing staged rescan result. Opt-in 1.15 persists a pristine enrollment anchor and reads finalized membership separately from the statement-visible segment IDs. At the lost-reply barrier the independent read-only verifier can now prove the unfinalized child from one observed catalog head, exact WAL proposal, bounded manifest digest and sealed membership. Opt-in 1.18 reads pending facts and original-snapshot overlay bytes through separate private scoped ledger accessors. The Rust/C ABI owns one scoped observation with certified pins and a consuming exact-pin plan. PR #93 (open, branch `feat/managed-cut-scan`, extensions 1.18→1.19) joins the cut into ordinary bound TableAM SQL: scan_begin admission, demand-time install with dual scope preflight, fresh-frame grant recheck (direct grant-row query under an explicit fresh snapshot — a VOLATILE SQL function body cannot honor a pushed snapshot), rescan retention, mid-window revocation refusal and the rotated-scope consistency error; all five drill cases green. | Merge #93 after adversarial review, then the managed COUNT red case, the RC latch statement-isolation campaign and the RR held-transaction campaign; retained object lease / provider proof and distinct refusal/timeout telemetry remain gated bands. [Visibility contract](modules/03-managed-visibility.md#certified-retained-ancestry) |
| Cost and expansion, 02/08/09 | #60 defines per-catalog profiles. #62 qualifies pinned benchmark digests. #66 reduces one projected canary from 4,719 to 16 data range GETs. | Diagnose the SF10 q1 timeout before a latency claim. Measure cold/warm dashboard completeness and spend on a qualified profile before selecting an elastic floor or enabling persistent reuse. [Cost evidence](modules/08b-io-cost.md) |

### MVP integration order

1. [Stock published reader](modules/10-downstream-handoff.md#external-postgresql-client-handoff) delivers the independent read-only pilot. For hosted use, verify the deployed backend and complete the external grant-rotation and cancellation drill. Provisioning and hosted admission can proceed alongside managed work.
2. Opt in to the one-shot 1.16 test boundary before joining [read-only discovery](modules/04b-scoped-send-admission.md#discovery-and-dispatch) and its scheduler. The scheduler consumes exact claim and grant inputs without gaining authority to resend a recovered proposal. Qualify separate provider-issued temporary Send and Lookup credentials through the opt-in credential adapter, a retiring writer fenced at the provider, and the source/intent WAL horizon on replacement. Reuse both reply-loss drills with the actual scheduler and deployment profile.
3. Join the bounded catalog observation and the scoped ledger accessors with the read-only pending-head verifier in one native statement cut. Read proof under a fresh committed ledger snapshot and read row visibility and retained payloads under the original executor snapshot. Start with `[10]`, `[10,20]`, `[10,20,20]`, a missing compatible parent before any output, and rescan at the same exact pin. Keep the overlay COUNT shortcut, writer promotion, publication cadence, governed reads and fleet cost qualification gated independently.

### One-shot native dispatch smoke for 1.16

The original worker made zero POSTs after losing the PostgreSQL authorization
reply. Restart recovered source WAL `0/01D8BF88` and exact reserved intent
WAL `0/01D8E6E8`. After five catalog GETs across the drill, lookup remained
unresolved and parent `7762361749703517515` still held `[10]`.

With a lost accepted catalog reply, the proxy saw one POST and matched its
bytes against the already-flushed intent before forwarding. Restart
preserved source WAL `0/023F4E78` and intent WAL `0/023F6388`. Two catalog
GETs verified child `3822450108288747699` with `[10,20,20]` above parent
`5004875050324735321`. Both cases refused resend and kept `[30]` staged.
These are single-primary fixture samples with separate static Send and Lookup
mappings. [Native dispatch contract](modules/04b-scoped-send-admission.md#native-one-shot-dispatch)

### Purpose-bound native session credentials for 1.17

Wrong-purpose Send and Lookup mappings refused before origin access. A
missing synthetic session token received one scoped S3 `403` before an
intent or POST. Replacing a token at the same mapping OID during preparation
returned `40001` with zero intent and POST. A signed purpose-matched Send
then made one accepted catalog POST. A separately signed Lookup recovered
the exact intent across restart and Trino read `[10,20,20]` at
`4744071831784341871`. The later `[30]` stayed staged. Source and intent
WAL were `0/02A60118` and `0/02A62448` in that local sample.
[Credential adapter](modules/04b-scoped-send-admission.md#native-session-credential-adapter)

### Bounded observation for scoped reads

An exact-pin scoped read resolves one approved REST metadata pointer and
loads one size-bounded metadata object. The planner consumes that same
observation. Scoped reads allow up to 8 MiB for each body and four seconds
per origin request. Gzip expansion shares the metadata byte limit. Oversized
or stalled metadata fails before manifest reads. Current-schema mapping
continues to serve the stock path. Managed execution still needs to prove
the selected pin's compatibility with the enrolled source schema.

On the parent, a valid oversized catalog envelope returned
`[10,20,30]`. The candidate returned `08000` after one catalog call and zero
object requests. An oversized gzip expansion returned `08000` after one
catalog call and one metadata object request. Trino and stock PostgreSQL
agreed on `[10,20,20,30]` at `7887953748271609562`, `[10,20,30]` at
`6364289661650475664` and sibling `[90,90,99]` at
`3088841630106092750`, with 7, 9 and 7 object calls respectively and zero
forbidden-root requests. Single-sample SQL latencies were 25.048, 28.032
and 24.333 ms. These are local Lakekeeper/S3 fixture observations.
[Scoped observation interface](modules/01-catalog-storage.md)

### Next managed visibility join

At the accepted-reply-loss barrier, read the unique retained attempt and its
sealed members under a fresh committed ledger snapshot. Verify the exact
binding, body and manifest-list digest against the same bounded catalog
metadata used for ancestry certification. Reuse the read-only verifier.
Keep the original statement snapshot for visible rows and leave generation,
attempt and finalization state untouched. The live verifier now resolves parent `7809961547152552931` for the
old and middle views with overlays `[]` and `[2]` and the accepted child
`4884973408649104829` with an empty overlay for the fresh view. All three
ran before local finalization. One bounded metadata GET and three healthy
manifest ranges supplied their proof. A fourth, corrupt range forced the
safe parent overlay. Missing or mismatched local evidence avoided object
I/O. Zero reader POSTs and unchanged attempt, ownership and member state
held throughout. Keep bound SQL gated until chosen-pin schema and payload
lifetime, whole-statement preflight and stable rescan pass.
[Executor acceptance](modules/03-managed-visibility.md#sql-executor-acceptance)

### Scoped ledger reads for 1.18

The first local run took 13.574 seconds. At the one accepted publication,
a fresh committed ledger read found the pending child
`2688773085668657737`, while the held original data snapshot could see
no intent. The old and middle cuts used parent `7777046454610483096` and
returned `[10]` and `[10,20]`. The fresh cut returned `[10,20,20]` on
the child. One reader metadata GET and three valid manifest ranges
established the cuts, with zero reader POSTs. A separate corrupt range
exercised fallback. After finalization, the pending accessor returned
NULL and overlay member 2 still returned the same sealed bytes under the
middle snapshot.

A tenant without grants received `42501`. Missing or invisible members
and changed binding failed with `55000`. A byte cap failed with `54000`.
A newer generation was refused under the held snapshot and the retired
generation failed the fresh scope check. These accessors enable the
[executor join](modules/03-managed-visibility.md#native-statement-cut-boundary).
Ordinary bound scans still return `0A000`.

### Native managed observation ABI

The loopback fixture certifies exact ancestry IDs above 2^53 from one
catalog GET and one metadata GET. Planning reads only the selected
manifest. A changed, missing or duplicate schema, a pin outside the
certified chain and dropping the unconsumed owner issue zero manifest
requests. Failure consumes the owner slot. A C11 test compiled the
generated header and the stock PG19 smoke matched Trino at the two
position-delete pins and the sibling pin without forbidden-root calls.
The local Rust suite passed 173 tests with one manual ignore.

The next #88 join needs the effective-role managed Read grant and the
native membership, pending-proof and scan lifetime integration. Start
with a direct serial append scan in the local scoped v2 profile. Before
output, verify the full cut and exact copied bytes. Existing bound reads
still return `0A000`. [Admission and native ABI](modules/03-managed-visibility.md#admission-before-the-native-cut)

### Enrollment and membership smoke for 1.15

The local fixture kept child `9132704555144924173` unknown while the catalog
had accepted it and the local receipt was still pending. With certified
parent `1263166121538778487`, the old, middle and fresh PostgreSQL snapshots
produced `[10]`, `[10,20]` and `[10,20,20]` by overlaying exactly the
visible segments. One fixture POST and two authorized catalog GETs later,
lookup proved the child and the ledger returned exact members `[2,3]`.
Removing a receipt member in a rolled-back control returned the child to
unknown. The SQL model verifies a conservative parent cut before local
finalization. Bound TableAM still returns `0A000`.
[Membership contract](modules/03-managed-visibility.md#certified-retained-ancestry)

### Evidence at #81

- **Native hold.** The fork CI receipt records one authorization under two
  concurrent workers, zero provider requests, and a `57014` lost reply. An
  immediate restart recovered source WAL `0/076FF590` and intent WAL
  `0/07704398` with the original proposal bytes and the later member still
  pending. A separate lock test refused after 10,007.369 ms in that run.
- **Managed read model.** The Lakekeeper fixture wrote once and the native
  verifier issued two catalog GETs. Under three PostgreSQL snapshots the
  selector returned exact multisets `[10]`, `[10,20]` and `[10,20,20]` for
  parent `5524602987576199479` and child `6516738430694082503`. Missing and
  expired compatible parents refused the cut. These are model and provider
  fixture receipts. Bound TableAM execution still returns `0A000`.
- **Scope of measurement.** These are individual correctness observations.
  Apply the [cloud and runtime evidence rules](RULES.md#10-cloud-and-runtime-evidence)
  and the [smoke receipt format](10-testing.md#milestone-tdd-and-smoke-receipts)
  when promoting either path.

### Managed recovery smoke at the #71 baseline plus the lookup-only driver

In the local ordinary-INSERT fixture, the lost PostgreSQL reply left Trino at parent `8991556753635846896` with `[10]`. The candidate snapshot `3622891792842048566` never entered table history. The catalog recorder saw zero commit POSTs and five table GETs, including the deliberately unavailable and stale responses. Recovery advanced ownership from generation 2 to 5. Segment 1 remained reserved and segment 2 remained pending. PostgreSQL recovered the source and intent commits at `0/029C0428` and `0/029C2350` and retained identical request bytes. The other two local reply-loss cases observed one catalog POST apiece and verified exactly the first member. Each case is one correctness observation. These runs exercise the test ledger adapter and scoped Rust transport on a single primary.

## Historical findings through PR #52

**Managed COUNT and local writes.** On the PR #47 fork a prepared COUNT returned 1 while the same backend scanned `[10,20]`. The executor now takes the ordinary scan for relations with transaction-local write state. The SQL regression covers savepoint rollback, full rollback and commit with one reused generic plan. Compatible published-base and committed-overlay counts still need a membership proof. [Original reproducer and regression](modules/03-managed-visibility.md#first-red-case-for-managed-count)

### Dependency order at PRs #46–52

| Dependency | Merged evidence or current gate | Next consumer |
|---|---|---|
| Public distribution | #46 specifies the release boundary. #47 builds and exercises the serial stock reader. #48 and #51 make the two builds use one C source list and test named fork dependencies under the stock preprocessor. #49 maps the public evidence. | Preserve each flavor's compile, install and pinned read smoke when extending shared code. |
| Scoped enrollment | #50 fixes the meaning of 22 provider/consumer cases. The ordinary FDW role smoke in this slice exercises owner/invoker mappings and revocation during prepared reuse. | Test independently configured and catalog-resolved roots, cross-scope cache reuse and zero forbidden requests before claiming tenant isolation. [Scoped read handoff](modules/06b-scoped-read-handoff.md) |
| Aggregate correctness | #52 fixes the observed own-write managed COUNT regression by using the ordinary scan when the backend has local write state. The FDW upper-path successor subsequently landed in #53. | Every aggregate must agree with its ordinary scan at the same authorized pin, including actual deletes. Neither shortcut establishes a compatible managed base-plus-overlay view. |
| Managed publication | #39–45 supply claims, intent, request retention, bounded lookup and immutable writer preparation. | Join bound SQL capture to the worker, then require the lost-reply one-POST restart and mixed-visibility scan before offering managed append publication. |

## Stock read foundation following PR #46

Base `8ed4cbab0b0`. On upstream PG19.0 (`5c839a2a08b`), the original planner failed to compile at the fork-only RTE field; clean SQL installation returned `0A000`. The [PGXS source package](../../src/backend/access/lakehouse/stock/README.md) now installs and runs the read-only surface on that release. The fork's assertion build passed 243 upstream subtests and 11 lakehouse SQL groups. One final local stock smoke took 12.18 seconds including Trino DDL and cleanup. Stock PG, schema-only dump/restore and Trino returned exactly `[10,20,20,30]` at snapshot `184327008694008591` and `[10,20,30]` at `1130167751514016848`. This sample measures fixture duration. Query p95/p99 and deployment SLOs are unmeasured.

**Dependency identified at #47.** prove the permission-checking role and approved storage root at planning, execution and cache reuse, including owner/invoker views and a denied sibling tenant with zero forbidden object requests. Signed runtime artifacts, multi-node qualification and managed worker recovery remain open. Downstream services pin the interface specification. A service rollout requires a separate runtime artifact and evidence.

## Original review findings at the historical baseline

| Assessment | Evidence | Next action |
|---|---|---|
| **Beautiful** | Immutable object naming and manifest carry-forward repair the original append-history failure. Normal PostgreSQL heap/WAL durability is a sound foundation for staged inserts. The typed batch ABI, projected reads, and decode-on-demand remove work at the right layers. | Preserve these seams. Test old snapshots with an independent reader, and prove ranged early stopping and memory bounds. |
| **Good** | The fork ported to the CF #5631 AM-options contract, added commit-base validation, improved delete/schema handling, added validators and mappings, split the FDW and publisher, and landed dependency audit checks. Fresh local Rust and assertion-enabled SQL suites passed. | Retain the unit and SQL coverage. Extend the required CI path to live success cases, role boundaries, and fault barriers. |
| **Bad** | M3 isolation and cloud parity are incomplete. The all-columns read remains eager. SF10 q1 timed out in the latest receipt, and q2/q3 baselines are unqualified. The benchmark runner lacks snapshot and result-digest qualification. | Fix the test harness, record one comparable baseline and profile the hot paths before choosing the next optimization. |
| **Ugly** | A generic prepared COUNT kept returning 2 after a third row committed. A transaction inserting into 65 staged relations committed with only 64 ledger entries. Managed COUNT also removes query relation metadata, and publication has a remote/local failure window. | Gate the affected paths. Fix COUNT at execution and use durable, exact publication claims with remote reconciliation. Require negative tests before restoring the fast paths. |

## Milestone status

| Milestone | Implementation | Release evidence still needed |
|---|---|---|
| **M0 Foundation** | Historically accepted at `milestone/m0` | Revalidate clean builds, extension install and supported upgrades at each candidate SHA. |
| **M1 External reads** | FDW, REST/S3, pruning, typed scans, time travel and supported v2/v3 delete reads landed. Time-travel deparse + dump/restore landed 2026-11-21 (ruleutils re-emits the pin; `time_travel.sql` carries the acceptance test) | Independent result equivalence, delete combinations, parallel pinning, cancellation and resource limits. |
| **M2 Managed writes** | Ordinary bound INSERT through opt-in one-shot native dispatch. Both reply-loss cases preserve exact WAL and lookup-only recovery on one primary. | Temporary credentials, scheduled polling, compatible executor scans and promoted-primary recovery. |
| **M3 Semantics and cloud parity** | In progress. Ranged projection, cached bytes, lazy projected decode, and initial COUNT paths landed | Qualify COUNT and lazy reads. Close isolation, ADLS/GCS transport and auth, branch/tag/history semantics and the logical replication decision. |
| **M4 Production hardening** | Design available for maintenance, statistics and execution tiers | Implement and qualify compaction, expiry, orphan safety, ANALYZE and bounded scan/publish resource use. |
| **M5 Engine integrations** | Backlog | Compare independently against the same pinned dataset, permission scope, resource budgets and result digests. |

## Historical milestone evidence and work queue

- **Implementation briefs.** The [module index](modules/README.md) assigns
  ownership and independent tests for the next wave. Land the shared contract
  and fixture vectors first, then qualify each provider with its consumer.
- **M2 publisher, design reviewed 2026-11-21 against `5b31b230680`.** The [staged recovery slice](modules/04a-publisher-recovery.md) defines the first red test and the proof required after a lost catalog reply. The exact-ID update is implemented. Local send intent and receipt finalization are implemented. Catalog lookup on takeover and the full end-to-end worker crash smoke remain pending. The existing successful-append smoke covers a different gate.

- **Local baseline.** 118 Rust tests passed and one manual benchmark was ignored. A clean assertion-enabled native build passed 243 upstream regression subtests and all 11 lakehouse SQL groups. Fork CI passed on PRs #27 and #28. See the [review receipts](16-release-reassessment-and-scale.md#evidence).
- **First mutation and catalog dependencies.** The [Trino and PostgreSQL smoke](../../src/test/lakehouse/fixtures/cow-delete-guard.sh) starts with rows 10, 20, 30. On parent `839fca8c668`, the managed DELETE accepted a rewrite after Trino had removed row 20. Both managed UPDATE and DELETE now refuse that snapshot with `0A000`. In a separate transaction, a concurrent Trino position delete causes PostgreSQL COMMIT to return `40001`, and Trino sees exactly 10 and 30. A Rust catalog fixture applies an append, returns a gateway timeout and verifies one POST plus a subsequent read of snapshot 101.
- **M2 local claim provider (1.4).** On parent `c6884a1df59` the new SQL test stopped at missing bound-segment storage (`42703`). The [implementation and cutover contract](modules/04a-publisher-recovery.md#local-claim-provider-in-extension-14) and [live smoke](../../src/test/lakehouse/fixtures/publication-claim-check.py) cover fresh install and upgrade, competing claimants, rollback, exact replay, late staging, a second target and legacy publisher isolation. An assertion build passed all 243 upstream and 11 lakehouse SQL groups. On the local fixture, a 1024-member claim over 8 MiB of previously sealed input took 40.1 ms on fresh install and 34.3 ms after upgrade, one client-side sample each including psql startup. An immediate PostgreSQL restart took 1.52 s and recovered the exact two-member claim. Trino read row 42 in the one newly introduced snapshot while the bound row remained pending. The foreground TableAM still writes through the existing protocol. Remote recovery and a publication latency SLO remain open.
- **M2 send intent and receipts (1.5).** With the new smoke on parent `9ea7c116f55`, the first generation transition fails with `42883`. On the candidate, one of two sessions receives authorization. Its WAL commit record is present at the local flush position despite session-level `synchronous_commit=off`. The fixture restarts PostgreSQL immediately, observes a lookup-only hold, fences the stale owner and verifies rollback and exact-member finalization. The first local restart sample was 1.42 s. The [1.5 transition contract](modules/04a-publisher-recovery.md#send-intent-and-receipts-in-extension-15) identifies the remaining catalog and worker integration gates.
- **M2 prepared append provider.** The [catalog module](modules/01-catalog-storage.md#prepared-staged-append) pins the exact outgoing bytes to the bound table and `main`. The parent `1f6b8fec333` fails to compile the test at the absent prepared-append API. The HTTP fixture compares emitted bytes with the prepared bytes, inspects the UUID/ref/schema/spec requirements and operation markers, and records one POST after a gateway timeout. The empty-ref and moved-binding cases run without POSTs. At that slice the integrated worker and lookup were open. PR #43 later supplied the lookup provider.
- **M2 proposal retention (1.6).** The smoke failed on parent `4792ed6749f` at missing proposal storage (`42703`). The Rust provider also accepted a request larger than the ledger limit on the parent. The candidate retains exactly 1,044 fixture bytes after a locally flushed authorization and an immediate PostgreSQL restart (1.32 s in the first local sample). A digest-only 1.5 attempt remains reserved through upgrade. The test exercises digest and size rejection, one authorization under contention, two exact completions, and one later segment still pending. [1.6 upgrade and recovery contract](modules/04a-publisher-recovery.md#exact-proposal-retention-in-extension-16)
- **M2 catalog acceptance lookup.** The new test fails to compile on parent `8e5f7f2ecf0` at the missing lookup provider. The HTTP fixture observes an inconclusive lookup before the original request, one accepted POST with a lost reply, and a second lookup that proves snapshot 101 under head 102. Changed manifest bytes, a stale branch, a replaced UUID and contradictory markers remain unresolved. The snapshot writer checks its manifest-list digest against the persisted Avro bytes. [Lookup contract](modules/01-catalog-storage.md#acceptance-lookup-for-a-retained-proposal)
- **M2 bounded manifest verification.** The test fails to compile on parent `9022ea5d1f0` at the missing scoped digest adapter. The candidate reads exactly the allowed size, rejects an oversized or missing object, and uses one ranged GET for a healthy S3 fixture. A stalled fixture read returned in 500.74 ms under a 500 ms deadline in one local sample. The catalog test uses actual object bytes, rejects a different bound root and sees a changed object on a fresh lookup. [Bounded lookup contract](modules/01-catalog-storage.md#acceptance-lookup-for-a-retained-proposal)
- **M2 immutable append preparation.** The tests fail to compile on parent `bc536425614` at the missing writer prepare and create-only entry points. Preparation writes actual Parquet and Avro, preserves the parent manifest, and makes no catalog POST. Stale pins and oversized child lists leave the metadata directory unchanged. A local create collision preserves the original bytes. The SeaweedFS fixture returned 200 on the first create-only PUT and 412 on the collision, with the original body intact. [Writer preparation contract](modules/04a-publisher-recovery.md#prepare-immutable-writer-output)
- **First.** The 65-relation capture regression landed. Managed COUNT executes at runtime and guards SPI fallback against recursion. Its base-and-overlay proof still needs qualification. FDW COUNT landed as a shared, execution-time one-row upper path — live receipts (manifest-count parent, visible-row child, prepared freshness, owner/invoker views) run in BOTH native CI jobs via fdw-count-check.py. The private local segment-to-claim mapping landed in extension 1.4. The 1.5 send fence and exact receipt are implemented. The Rust catalog provider can prepare and submit one immutable append request. Extension 1.6 persists the exact proposal. The read-only catalog lookup provider is available. The Rust lookup verifies the manifest list through scoped, bounded storage reads. The writer can prepare real immutable manifests from a verified pin. Next come verified capture binding, worker integration, end-to-end publication and the compatible-pin visibility model. Keep delete-bearing rewrites gated until the reader and writer agree on physical ordinals and applicable deletes.
- **Second.** Qualify row-group laziness, bounded and cancelable I/O, cache scope, and byte/memory counters. Land the remote positive smoke and result-digest runner in CI.
- **Third.** Profile SF10 and one concurrency workload. Use the measured time split to decide between fewer planning calls, batching an exact aggregate, or improving a join/rescan path. Keep page-index work independent.
- **Milestone completion.** Follow the [TDD and smoke protocol](10-testing.md#milestone-tdd-and-smoke-receipts) and record PASS, FAIL, or NOT RUN against each named gate.

Historical slice notes and receipts are in the [handoff](HANDOFF.md) and [takeover notes](HANDOFF-TAKEOVER.md). Dates, commit IDs, limitations and raw measurements belong with the receipt.
