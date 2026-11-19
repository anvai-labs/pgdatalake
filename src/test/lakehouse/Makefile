# Apache Iceberg fixtures and test entry points (specs/lakehouse/10-testing.md §6).
#
#   make lakehouse-up           # docker compose: minio + lakekeeper + azurite + fake-gcs
#   make lakehouse-check        # cargo test + upstream regress suite + iceberg regress suite
#   make lakehouse-conformance  # interop suite vs pyiceberg (M1+)
#   make lakehouse-down
#   make perf-lakehouse         # perf harness (M4+, doc 10 §5)
#
# Reached via the root Makefile forward (meson-only checkouts).

COMPOSE = docker compose -f docker-compose.yml -p pgdatalake

.PHONY: lakehouse-up lakehouse-down lakehouse-check lakehouse-conformance perf-lakehouse

lakehouse-up:
	$(COMPOSE) up --wait

lakehouse-down:
	$(COMPOSE) down --volumes --remove-orphans

lakehouse-check:
	@echo "cargo test -p pgdatalake-core"
	cargo test --manifest-path $(CURDIR)/../backend/access/lakehouse/core/Cargo.toml
	@echo "meson test (setup + regress + lakehouse suites)"
	meson test -C $(CURDIR)/../../build --suite setup
	meson test -C $(CURDIR)/../../build --suite regress
	meson test -C $(CURDIR)/../../build --suite lakehouse

lakehouse-conformance:
	@echo "lakehouse-conformance: not implemented until M1 (specs/lakehouse/12-roadmap.md)" && exit 1

perf-lakehouse:
	@echo "perf-lakehouse: not implemented until M4 (specs/lakehouse/10-testing.md §5)" && exit 1
