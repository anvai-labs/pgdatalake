# Governed reads of published Iceberg snapshots

**Proposed v1 conformance contract.** Start with one read-only foreign table,
one explicit pin and one viewer using invoker rights. The existing bound FDW
pilot qualifies exact-pin access to an enrolled table root. A protected table
additionally requires a catalog-authorized view of its rows and columns. A
self-hoster can implement and test this contract with a local provider. Public
core owns the SQL and scan semantics. An operator supplies the authenticated
principal, current grant and provider credential through a trusted adapter.
[Existing scope and mapping contract](06b-scoped-read-handoff.md)

## Interface and ownership

These are logical calls for co-design. The fixture fixes observable behavior
before a C ABI, SQL option or transport is chosen.

| Owner and call | Inputs, result and lifetime | First failing case and smoke |
|---|---|---|
| 06 SQL, `open_governed_read` | At first execution demand, bind installation, statement viewer, effective check-as role, enrollment ID/generation, requested authority, UUID, ref and canonical decimal-string pin. Derive SQL identity afresh at EXECUTE. Pass opaque credential and grant references in trusted execution context. Never serialize bearer tokens into a plan, SQL options, EXPLAIN, cache key or receipt. | Prepare once and change roles. An invoker maps to their current enrollment. An owner view whose remote authority cannot preserve the viewer policy returns unsupported before accessing data. LIMIT 0 and plain EXPLAIN perform zero remote work. |
| 01 catalog, `plan_governed_read` | Verify enrollment and the provider principal/role for the requested authority and table. Obtain the provider's protected scan, bind it to the exact **source** pin, and return an opaque handle for its permitted output objects or batches, validity and reuse rights. A request ID correlates the authorized response to this request. A temporary output snapshot may have a different identity. | A filtered output without trustworthy evidence of its source pin declines the exact-pin profile. Swapping two successful viewers' plan handles fails. Provider returns only ordinary raw paths for a required protected scan → unsupported before object I/O. |
| 02 reader, `read_authorized_batches` | Consume the scoped handle, projected fields and deletes according to the provider's plan semantics. Enforce scope on each object, including temporary output outside the base directory, using separately authorized output roots or capabilities. Carry mask semantics through filters, joins, aggregation and output. Scope every range and byte-cache hit to its authorized handle. | Compare signed SQL bigint COUNT with a full scan of the same governed output. Raw manifest COUNT/min/max and predicate pruning on unmasked values are ineligible unless independently proved equivalent for that plan. No raw fallback after any refusal. |
| 08 qualification, scan evidence | Correlate attempt, profile/version, authority, installation, binding and grant generation, pseudonymous viewer and effective role, source pin, terminal status, exact result digest and multiplicity, expiry and request/byte counters. Report protected planning costs and retries separately. A receipt describes one attempt. Each new execution obtains current authorization. | Run both viewers over one backend and a fresh backend. Revoke during a fill. Independently record requests to the original objects and compare with the provider's governed engine at the same pin. |

Bind issuer-scoped subject and tenant to a stable local viewer identity.
A service principal qualifies only for its own expressly enrolled grant.
An application sharing one database login must supply an independently
authenticated viewer binding to use the per-viewer profile. An arbitrary SQL
setting or BI workbook field supplies no such authority.

Treat the returned plan, policy/entitlement revision (where supplied), grant
generation, resource scope and expiry as distinct. A stable data pin does
not keep a mutable policy current. Default to statement-local reuse.
Cross-statement reuse requires provider permission for the copy and evidence
that covers **every** policy input, including group membership, lookup tables,
and time-dependent decisions. Where there is no authoritative freshness
signal, obtain a fresh protected plan for each execution and keep its results
within that execution. Bound validity by the earliest grant, credential and
plan deadline. Revalidate each cache waiter before delivery. On observed
revocation stop subsequent delivery and report an incomplete stream if rows
already left the executor. Document provider propagation and the stream's
check interval in any advertised revocation bound.

Return existing 06b outcomes for denial, binding change, unavailable pin,
unsupported semantics and unavailable dependencies. A new plan may be acquired
at the same exact pin within the original deadline before delivering rows.
After partial delivery surface failure to the caller. A missing compatible
plan never authorizes a raw credential, a different pin or an older cache
entry. Suppress underlying unrestricted statistics from user-visible EXPLAIN
and metadata shortcuts in this profile.

## Parallel boundaries and acceptance

1. Freeze the fixture, pin encoding, refusal meanings and observable call
   counts. SQL and catalog owners can develop against each other's fakes.
   The reader can use synthetic sanitized batches without a live provider.
   Integrate once at the scan-session boundary. Retain ordinary 06b regression
   cases for existing unrestricted enrollments.
2. Run the public fixture under each package's ordinary scan and COUNT path.
   Authorize before exposing data or source statistics. If a provider omits a
   strong policy version, a persistent tile cannot serve this profile.
   For multiple relations bind every input, including repeated scans of the
   same relation, to the requested vector and viewer. Refuse an unprovable
   exact-pin repeat. Independently pinned tables promise the stated vector.
3. The TableAM can share this reader for a compatible published base. Opening
   a protected managed overlay additionally requires equivalent filtering and
   masking of locally staged rows under the PostgreSQL snapshot. Keep that
   capability closed until its own two-viewer mixed-visibility test passes.
4. Use the [synthetic v1 oracle](fixtures/governed-read-v1.json) and replay
   its SQL rows, masks, NULLs, deletes and COUNT before testing a real adapter.
   The two source pins are opaque strings in chronological fixture order.
   `amount` is the already-masked result column in each expected row.
   All positive executions in this fixture use provider-sanitized output and
   make zero direct base-object requests from the reader. Controls must
   observe a real governed read and distinguish allowed output requests from
   forbidden raw GETs. A fake or algebra test supplies no provider evidence.
   [Downstream qualification](10-downstream-handoff.md) · [BI reuse](02a-bi-reuse.md)
