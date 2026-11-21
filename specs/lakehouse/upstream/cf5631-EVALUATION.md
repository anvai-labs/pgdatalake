# CF #5631 consumer evaluation (2026-11-20, branch cf5631-port)

We ported anvai-labs/postgres (a public Iceberg table AM) onto the
CF #5631 v13/v14 contract (branch cf5631-port). The port went cleanly;
all our prior findings are already handled by the series (StdRdOptions
prefix after TOAST/fillfactor failures, SET ACCESS METHOD
revalidation, matview dispatch — verified live) with ONE exception and
one residual, below, plus a non-blocking suggestion.

## Gap 1: combined `SET ACCESS METHOD ..., RESET (...)` fails at the
rewrite scan (deferValidation exposes post-RESET options to the OLD AM)

Repro (branch cf5631-port, our consumer AM with per-table remote
identity options):

  CREATE TABLE t (id bigint) USING lakehouse
      WITH (table = 'ns.t', endpoint = '...');   -- succeeds; rows inserted
  ALTER TABLE t SET ACCESS METHOD heap,
      RESET (table, endpoint);                   -- FAILS, rolls back

Failure mode (differs from Ajit Awekar's 2026-09-30 SET-before-RESET
report, which errored at validation): with deferValidation the stored
options are RESET before the table rewrite, and the rewrite's tuple
scan runs under the OLD access method — whose callback no longer sees
the options it was planned against. For a consumer whose options are
load-bearing for scans (remote identity), the rewrite scan loses its
identity mid-ALTER and the ALTER aborts (cleanly — AM, options, and
rows are preserved, which the contract handles well).

Suggested contract rule: a table rewrite triggered by SET ACCESS
METHOD scans with the options AS OF BEFORE the sub-command list
(applied after rewrite), or documents that RESET of old-AM options in
the same statement requires the rewrite to use the new AM's parser for
post-state while giving the old-AM scan a pre-ALTER options snapshot.

## Gap 2 (residual): credentials must never live in options — docs

Our consumer initially accepted s3 keys/tokens as table options; we
removed them (pg_class.reloptions is exposed by pg_dump/schema
dumps). Dunstan's SGML section for amoptions is the right place for a
security note: AM-specific options live in pg_class and are dumped;
credential-style values need server-side settings/user mappings.

## Non-blocking suggestion: preset composition helper

Our AM composes heap-standard options with its own keys via
strip-and-delegate (call default_reloptions on the remainder). That
recipe works under the contract but every AM reimplements it;
Paquier's 2020 "preset packages" suggestion could land later as a
small exported helper. Not needed for this series.

## Confirmed working on v14/v13 + our port

matview dispatch, SET ACCESS METHOD revalidation and
preserve-on-failure (AM/options/rows intact after a rejected
combined ALTER — verified live), StdRdOptions prefix contract,
has_std_options_prefix gating.
