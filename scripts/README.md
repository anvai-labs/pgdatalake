# scripts/

Release/ceremony automation for the lakehouse fork.

- `coldroom-smoke-m0.sh` — the M0 cold-room smoke sequence (RULES §9 step a):
  fresh-clone, fixtures up, `meson setup build -Ddatalake=enabled`, ninja,
  upstream regress suite, `make lakehouse-check`. Generalize per-milestone
  headline-feature steps as tags are cut.

- `run_clang_tidy_lakehouse.sh` — curated clang-tidy over the fork-authored
  lakehouse C files (see `.clang-tidy` in that dir for the check policy and
  the deliberately-excluded style checks).
- `repair_pgindent_utf8.py` — MUST run after every pgindent pass over
  `src/backend/access/lakehouse/`: pg_bsd_indent 2.1.3 preserves UTF-8 in
  comments but mangles multi-byte characters inside string literals
  (em-dash → `—<digits>—`) in every locale.
