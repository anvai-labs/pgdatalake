# scripts/

Release/ceremony automation for the lakehouse fork.

- `pgindent_lakehouse.sh` — RULES §4 pgindent enforcement for fork C files.
  Plain pgindent mangles the fork two ways: unknown typedefs make
  pg_bsd_indent split `Type *ptr` into `Type * ptr` (supplied mechanically
  from lakehouse sources + the cbindgen FFI header), and multi-byte UTF-8
  in string literals gets eaten (repaired by `repair_pgindent_utf8.py`,
  run after every pass). `--check` mode exits 1 on any diff — that is what
  the `.githooks/pre-commit` gate runs. Activate the gate:
  `git config core.hooksPath .githooks`.

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
