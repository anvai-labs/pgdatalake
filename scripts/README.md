# scripts/

Release/ceremony automation for the lakehouse fork.

- `coldroom-smoke-m0.sh` — the M0 cold-room smoke sequence (RULES §9 step a):
  fresh-clone, fixtures up, `meson setup build -Ddatalake=enabled`, ninja,
  upstream regress suite, `make lakehouse-check`. Generalize per-milestone
  headline-feature steps as tags are cut.
