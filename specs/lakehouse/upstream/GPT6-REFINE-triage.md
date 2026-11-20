# GPT-6 REFINE verdict (2026-11-20) — triage and decisions

VERDICT: HOLD the posting. Full text preserved in REVIEW-PROMPT thread
(the 8 numbered items below are its findings).

## Immediate findings that affect OUR FORK regardless of upstream

- [SECURITY, from #8] lakehouse WITH-options store token +
  s3_secret_key in pg_class.reloptions — visible in pg_dump, schema
  dumps, and catalog reads. Redesign: credentials must come from
  secure server settings (postgres_fdw-style user mappings or GUC),
  never per-table options. doc 09 amendment + managed-CREATE rework.
- [CORRECTNESS, from #2] our rd_options bytea is NUL-packed strings;
  autovacuum casts extractRelOptions() result to StdRdOptions* and
  copies sizeof(StdRdOptions) -> reads past our buffer. Our AM must
  return a layout with a valid StdRdOptions prefix (or core gets a
  guarded accessor). Fix in tableam.c before next live run.
- [#3] our SET ACCESS METHOD transfer test was hollow (started from a
  failed CREATE); real test must switch populated tables and verify
  rejection/preservation both directions.
- [#5] effective-AM resolution gaps: CREATE MATERIALIZED VIEW USING,
  partitioned-parent inheritance vs session default — reproduce on our
  fork too.

## Upstream strategy decision (from #1)

CF #5631 "Allow table AMs to define their own reloptions" (v14, Needs
review, November PG20 CF) already covers callback + standard options +
lock selection + pg_dump. DECISION: do NOT compete. Strongest
contribution = test the Iceberg consumer against CF #5631 v14, report
what its API cannot express (our findings become review input to
#5631, not a rival patch). Our series is shelved as a scratch
exploration; branch kept for reference only.

## If we ever resurrect the series (GPT-6's required changes, terse)

1. Rebase on stated base; no embedded patch files; reply on #5631.
2. rd_options contract: standard prefix or guarded accessor; NULL
   input + validate=false on old data; flat copyable result; test with
   real consumer layout.
3. Real SET ACCESS METHOD transfer tests on populated tables; validate
   final state incl. NULL-callback destination AM; failed switch
   preserves AM+options+rows.
4. Lock strength: AlterTableGetRelOptionsLockLevel + unregistered
   private keys (ShareUpdateExclusive minimum vs AccessExclusive
   needed); fresh sessions + builtin-name collisions; SET and RESET.
5. Effective AM once: leaf USING, partitioned inheritance w/
   conflicting default, parent-fallback, CTAS, matview; keep toast.*
   path; test toastable table.
6. Drop heap_reloptions_for_am as written (ownership flaw: strips all
   matching keys; shadowing; duplicate test_option accepted; never
   parses). Typed AM definitions w/ collision+duplicate handling, or
   defer public helper. Test must read parsed value from rd_options
   after CREATE/SET/RESET/fresh-backend (not catalog text). Wire the
   module into Make build too.
7. API name: amoptions + amoptions_function (match IndexAmRoutine and
   #5631). Email: correct pg_am.amoptions description; remove
   "consensus"/"reviewed to ready"/"only mechanical objections".
8. Iceberg example: keep, one sentence, no credentials in options;
   narrow restore claim to tested behavior; verify schema dump/restore
   + --no-table-access-method path; note restore ≠ remote object
   recreation.
