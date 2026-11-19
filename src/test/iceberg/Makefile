# Apache Iceberg fixtures and test entry points (specs/iceberg/10-testing.md §6).
#
#   make iceberg-up           # docker compose: minio + lakekeeper + azurite + fake-gcs
#   make iceberg-check        # cargo test + upstream regress suite + iceberg regress suite
#   make iceberg-conformance  # interop suite vs pyiceberg (M1+)
#   make iceberg-down
#   make perf-iceberg         # perf harness (M4+, doc 10 §5)
#
# Reached via the root Makefile forward (meson-only checkouts).

COMPOSE = docker compose -f docker-compose.yml -p pgiceberg

.PHONY: iceberg-up iceberg-down iceberg-check iceberg-conformance perf-iceberg

iceberg-up:
	$(COMPOSE) up --wait

iceberg-down:
	$(COMPOSE) down --volumes --remove-orphans

iceberg-check:
	@echo "cargo test -p pgiceberg-core"
	cargo test --manifest-path $(CURDIR)/../backend/access/iceberg/core/Cargo.toml
	@echo "meson test (setup + regress + iceberg suites)"
	meson test -C $(CURDIR)/../../build --suite setup
	meson test -C $(CURDIR)/../../build --suite regress
	meson test -C $(CURDIR)/../../build --suite iceberg

iceberg-conformance:
	@echo "iceberg-conformance: not implemented until M1 (specs/iceberg/12-roadmap.md)" && exit 1

perf-iceberg:
	@echo "perf-iceberg: not implemented until M4 (specs/iceberg/10-testing.md §5)" && exit 1
