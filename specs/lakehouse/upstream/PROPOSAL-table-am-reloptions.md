# Upstream proposal: table AMs may own their relation options

*Prepared 2026-11-20. Status: patch ready (0001-table-am-reloptions.patch
in this directory); delivery = pgsql-hackers email + commitfest entry
(the postgres/postgres mirror accepts no issues or PRs).*

## The ask

Add an optional `TableAmRoutine.relation_reloptions` callback:

```c
bytea *(*relation_reloptions) (Datum reloptions, bool validate);
```

Route both the relcache extract path (`extractRelOptions`) and the
CREATE/ALTER validation path (`tablecmds.c`; AM resolved from
`stmt->accessMethod` / `default_table_access_method`, matching
`DefineRelation`) through it when the AM supplies one. NULL = heap
defaults. 62 lines, 4 files, no catalog changes.

## Email (ready to send)

To: pgsql-hackers@lists.postgresql.org
Subject: table AMs may own their relation options (TableAmRoutine.relation_reloptions)

Attachment: 0001-table-am-reloptions.patch (git am / format-patch)

Body: the "WHY REINVOKING THIS, AND WHY THIS SHAPE" section of the
patch commit message (below), plus the motivating-consumer paragraph.

## Why reinvoking this, and why this shape (thread-grounded)

1. Jeff Davis, pgsql-hackers 2020-09, "Reloptions for table access
   methods" (message-id
   429fb58fa3218221bb17c7bf9e70e1aa6cfc6b5d.camel@j-davis.com):
   proposed relaxing fillRelOptions' leftover-option check for heaps.
   Rejected because Simon Riggs objected that heap's option set must
   not silently apply to every extension AM ("options should work the
   same for indexes and tables") and Michael Paquier agreed "bypassing
   a sanity check does not feel like a good solution in the long
   term". Our patch relaxes NO check: AMs without the callback are
   behavior-identical; AMs with it own validation AND parsing — the
   index-AM symmetry Simon asked for.

2. Paquier (2020-12-16, same thread) explicitly named this design:
   "an options callback in TableAmRoutine to do the loading work",
   with AMs defining their own option sets (dummy_index_am-style)
   instead of extending the static relOpts registry. Alvaro Herrera's
   silent-ignored-option concern is covered by the validate path: an
   AM-specific key on a plain heap still errors "unrecognized
   parameter" at CREATE because dispatch follows the effective AM.

3. Korotkov, "Table AM Interface Enhancements" (CF #4866; returned
   with feedback 2024-09, no reviewers; latest patch 2024-04): carried
   this capability inside a large series (ANALYZE generalization,
   index-insert hooks), making the reloptions piece hard to review
   alone. Ours is ONLY that piece.

4. Alternatives discussed in the 2020 thread and why they don't fit:
   - fillRelOptions validate=false (Jeff's one-liner): bypasses a
     check; rejected above.
   - Extensible StdRdOptions + allocateReloptStruct hook: keeps the
     registry in core, needs per-feature TOAST/autovacuum handling
     spread across two mechanisms; Jeff himself called it
     over-engineering.
   - rd_amcache leftover stash: splits validation in two places with
     AM-visible results only at relcache-build time.
   - Per-AM custom relopt kind via default_reloptions(kind): requires
     the registry anyway and special-cases the plain-heap error;
     Paquier's callback suggestion superseded it.

5. Motivating consumer: anvai-labs/postgres (public fork),
   lakehouse-dev — a public Iceberg table AM where the table's remote
   identity and storage endpoints ride in
   CREATE TABLE ... USING lakehouse WITH (table = 'ns.tbl',
   endpoint = '...', ...). One-statement managed DDL that pg_dump/
   restore round-trips; no companion catalog table, no post-create
   attach step. With the callback absent, everything above is
   behavior-neutral for heap and in-core AMs.

## Evidence

- Upstream regress suite green on the patch (local run).
- Consumer AM regression suite exercises CREATE/ALTER/relcache paths:
  with-options identity, unknown-key rejection at CREATE, ALTER
  SET/RESET round-trip via relcache.

## Fair evaluation addendum (2026-11-20, post thread re-read)

CF #4866 verdict on the reloptions piece specifically: mechanism = the
SAME TableAmRoutine callback (named `reloptions` there), "less
debatable", reviewed to ready; objections were mechanical only
(redundant relkind if -> assert; duplicated index_reloptions decl;
missing includes; Mark Dilger: "could use some regression tests").
The series was returned as a whole, not for reloptions design reasons.

Consequences for v2 (before sending):
1. NAME: align the callback with the CF lineage (`reloptions`) so the
   review continues an existing thread rather than starting a rival.
2. TESTS IN THE PATCH: upstream-visible regression tests are a hard
   requirement (Dilger's request) — add a minimal in-tree test AM
   module (dummy_table_am style) exercising the callback through
   CREATE/ALTER/relcache; the consumer suite alone does not count.
3. PRESETS: state honestly that autovacuum/toast option packages are
   NOT included; full ownership without presets is the deliberate
   first step (Davis's extension-learn-by-doing argument); presets
   are follow-up work.
4. ALTER semantics: options must satisfy the (possibly new) AM after
   SET ACCESS METHOD; failures surface at ALTER time — same rule the
   CF series used.
5. NOT claiming: this does not decouple build_reloptions from the
   static relOpts registry (Paquier's fuller vision); it is the
   narrow, reviewable first slice both prior efforts point toward.

## v2 (2026-11-20) — CF #4866 feedback addressed point-by-point

Citing the reviewer feedback on "Custom reloptions for table AM"
(0009/0002 in the series) directly:

- Mark Dilger: "could use some regression tests" -> v2 ships
  src/test/modules/tableam_reloptions: a heap-delegating test AM whose
  regression suite covers CREATE-time AM-owned option acceptance,
  unknown-key rejection (no silent ignore), heap-key delegation,
  plain-heap rejection of AM keys (effective-AM dispatch), ALTER SET
  relcache re-parse, and SET ACCESS METHOD option-ownership transfer.
- Pavel Borisov (redundant relkind if -> assert; duplicated decl) ->
  our dispatch has no redundant relkind block (NULL callback =
  heap_reloptions directly); declaration lives only in reloptions.h.
- Japin Li (missing includes) -> v2's reloptions.c carries explicit
  includes for tableam/pg_am/syscache.
- Naming aligned to the CF lineage: the callback is
  TableAmRoutine.reloptions (CF v5 named it exactly that).
- SGML docs added (doc/src/sgml/tableam.sgml, "Table Access Method
  Relation Options"): Riggs's docs requirement.
- Still scoped out, honestly: shared preset option packages
  (autovacuum/toast) and the fuller relOpts-registry decoupling —
  deliberate first slice per the 2020 thread consensus and Davis's
  extension-learn-by-doing argument.

Branch anvai-labs:table-am-reloptions carries v2; the format-patch in
this directory is regenerated from it. EMAIL STILL HELD until the
patch builds + tests green and the user approves.
