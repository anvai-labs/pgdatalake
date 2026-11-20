# CF #5631 reply draft (HOLD until user approves)

To: pgsql-hackers@lists.postgresql.org
Subject: Re: Add amoptions callback to table access methods
(+#5631: consumer report from an Iceberg table AM)
In-Reply-To: 20250302085641.hmjom5ru3w554t2n@poseidon.home.virt

Hi Andrew,

We maintain a public Iceberg table AM for Postgres
(anvai-labs/postgres, branch lakehouse-dev) and have ported it onto
your amoptions contract from CF #5631 v13/v14 (our port branch:
cf5631-port, CI-green incl. upstream regress). The contract has been
a clean fit: matview dispatch, SET ACCESS METHOD revalidation with
preserve-on-failure, and the StdRdOptions prefix handling all worked
as documented on our consumer.

One contract gap found while testing combined statements:

  ALTER TABLE t SET ACCESS METHOD heap, RESET (<am keys>);

fails at the table-rewrite scan: with deferValidation the options are
RESET before the rewrite, and the rewrite's tuple scan runs under the
OLD access method, which no longer sees the per-table options it was
planned against (for us: the remote table identity). The ALTER aborts
cleanly — AM, options, and rows are preserved — but the statement
cannot succeed without splitting RESET into a separate ALTER that
runs first. Suggested rule: a rewrite scan triggered by SET ACCESS
METHOD uses the options snapshot from before the sub-command list,
with SET/RESET applied after the rewrite. Repro and details:
  (short repro inline; full notes available on request)

Two smaller notes:
1. Docs: the amoptions section could carry a security note — AM
   options live in pg_class and are included in dumps; credential-
   style values (we initially made this mistake with S3 keys) need
   server-side settings or user mappings, not table options.
2. Non-blocking suggestion from the 2020 thread's "preset packages"
   direction: a small exported helper that composes heap-standard
   options with AM-owned keys (we hand-roll strip-and-delegate today;
   happy to contribute the helper if wanted).

Patch series applied for our port testing:
  cf5631/v13-0002 (amoptions callback) + v14-0001 (lock strength).
Happy to run any additional cases you want covered on the consumer.

Thanks,
<user signs>
