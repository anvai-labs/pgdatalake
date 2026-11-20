# GPT-6 adversarial review prompt — paste everything below

You are an adversarial reviewer for the PostgreSQL hackers mailing list
(a seasoned CF reviewer: think Peter E., Andres, Michael Paquier
standards). Evaluate the following patch proposal BEFORE it is posted
publicly. Approve, or refine with specific required changes. Be harsh:
find technical flaws, naming/API concerns, missing cases, and
presentation problems. Do not rubber-stamp.

## PROPOSAL EMAIL (ready to send to pgsql-hackers@lists.postgresql.org,
patch attached as 2-patch format-patch series)

Subject: table AMs may own their relation options
(TableAmRoutine.reloptions)

Body:
Table access methods cannot currently carry custom relation options:
extractRelOptions and CREATE/ALTER validation dispatch every plain
relation to heap_reloptions by relkind, and heap's option set is
hard-coded. Index AMs solve the same problem with pg_am.amoptions.

This 2-patch series adds an optional TableAmRoutine.reloptions
callback:

  bytea *(*reloptions) (Datum reloptions, bool validate);

0001 routes both the relcache extract path (extractRelOptions) and the
CREATE/ALTER validation path (tablecmds.c, AM resolved from
stmt->accessMethod or default_table_access_method, matching how
DefineRelation resolves it) through the callback when the AM supplies
one. NULL = heap defaults, so heap and in-core AMs are unchanged; no
catalog changes. It also ships a test AM
(src/test/modules/tableam_reloptions — heap-delegating, overriding
only reloptions) with a regression suite: CREATE-time acceptance,
unknown-key rejection, heap-key delegation, plain-heap rejection of
AM keys, ALTER TABLE SET relcache re-parse, and SET ACCESS METHOD
option-ownership transfer. SGML docs included
(tableam.sgml, "Table Access Method Relation Options").

0002 adds heap_reloptions_for_am(reloptions, validate, am_keys,
namkeys): AMs composing heap-standard options (fillfactor, autovacuum,
...) with their own keys make one call instead of hand-stripping
(Michael Paquier's 2020-12-16 "preset option packages" direction, in
its minimal form). The test AM exercises exactly this helper.

This is the custom-reloptions slice of two prior efforts, reshaped
around the 2020 thread's own consensus:
1. "Reloptions for table access methods" (Jeff Davis, 2020-09,
   message-id
   429fb58fa3218221bb17c7bf9e70e1aa6cfc6b5d.camel@j-davis.com):
   proposed relaxing fillRelOptions' leftover-option check for heaps.
   Rejected: Simon Riggs objected that heap's option set should not
   silently apply to every extension AM ("options should work the same
   for indexes and tables"); Michael Paquier agreed "bypassing a sanity
   check does not feel like a good solution in the long term". This
   series relaxes no check: an AM without the callback behaves exactly
   as today; an AM with it owns validation AND parsing — the index-AM
   symmetry Simon asked for.
2. Michael Paquier (2020-12-16, same thread) named this design: "an
   options callback in TableAmRoutine to do the loading work". Alvaro
   Herrera's silent-ignored-option concern is covered by the validate
   path: an AM key on a PLAIN heap still errors "unrecognized
   parameter" at CREATE, because dispatch follows the effective AM.
3. "Table AM Interface Enhancements" (CF #4866, returned with feedback
   2024-09) carried the same callback (named reloptions in v5) inside a
   large series; the reloptions piece was reviewed to ready ("less
   debatable" per Pavel Borisov) with only mechanical objections:
   Mark Dilger wanted regression tests IN the patch (this series ships
   them), a redundant relkind if (avoided here), duplicate declaration
   (avoided), missing includes (explicit here). This submission is
   ONLY the reloptions slice so the design can be settled
   independently.

Jeff's alternatives from the 2020 thread (extensible StdRdOptions +
allocation hook; rd_amcache leftover stash; per-AM custom relopt kind)
were each set aside there as over-engineering or incomplete; the
callback moves ownership wholly to the AM, matching the index-AM
precedent and Paquier's suggested end state.

Motivating consumer: a public Iceberg table AM for Postgres
(anvai-labs/postgres, branch lakehouse-dev): the table's remote
identity and storage endpoints ride in CREATE TABLE ... USING
lakehouse WITH (table = 'ns.tbl', endpoint = '...', ...), so managed
DDL is one statement that pg_dump/restore round-trips.

## THREAD CONTEXT (the reviewer model should weigh these positions)

- Simon Riggs (2020-12-15): rejected the relax-validation one-liner;
  "options should work the same for indexes and tables"; demanded
  tests/docs/commit-quality ("not sustainable" otherwise).
- Michael Paquier (2020-12-16): TableAmRoutine callback = right
  direction; endorsed AM-owned option sets (dummy_index_am pattern)
  and preset packages; rejected validation bypass.
- Alvaro Herrera (2020-09-01): warned about silently-ignored mistyped
  options; asked for AM-wide consistency.
- Georgios Kokolatos: reloptions handling was a major pain merging
  tableam into Greenplum; supported the simple approach short-term.
- Jeff Davis: alternatives he considered and set aside as
  over-engineering: extensible StdRdOptions + allocateReloptStruct;
  rd_amcache stash; per-AM relopt kind via default_reloptions.
- CF #4866 (Korotkov, returned with feedback 2024-09): reloptions
  callback reviewed to ready; objections were mechanical only (tests,
  duplicate decl, includes); the series was too big to review as one
  unit.

## KNOWN OPEN QUESTIONS (answer these explicitly in your review)

1. Is the hook name TableAmRoutine.reloptions right, or does it
   collide/confuse with the existing reloptions.c machinery?
2. ALTER TABLE SET ACCESS METHOD semantics: options that don't parse
   for the NEW AM error at ALTER — is in-place re-validation the right
   rule, or should SET ACCESS METHOD force a SET (...) reset?
3. heap_reloptions_for_am strips AM keys by string matching against
   the raw "key=value" text — is key-stripping by name robust enough,
   or should preset composition use the registry (relOpts walk)?
4. Does the series need to handle ALTER TABLE ... SET STORAGE/TOAST
   interplay, partitioned tables (relkind 'p' with an AM?), or CREATE
   TABLE ... PARTITION OF ... USING?
5. Is deferring the fuller relOpts-registry decoupling (Paquier's
   larger vision) acceptable in a first slice, or will reviewers
   demand it in the same series?
6. Anything in the Iceberg-AM motivation that a CF reviewer would
   object to as vendor-driven scope?

Verdict format: APPROVE or REFINE, with a numbered list of required
changes (if any), ranked by severity.
