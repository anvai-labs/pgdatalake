# Build the read-only extension against the installed stock PostgreSQL headers.
EXTENSION = lakehouse
MODULE_big = anvai_lake_fdw
DATA = lakehouse--1.0-fdw.sql
# The fork's Meson target consumes the same source manifest.
shared_sources := $(shell cat $(dir $(lastword $(MAKEFILE_LIST)))SHARED_SOURCES)
OBJS = $(shared_sources:.c=.o)

PG_CONFIG ?= pg_config
ifeq ($(filter 19.%,$(word 2,$(shell $(PG_CONFIG) --version))),)
$(error The stock lakehouse package requires a PostgreSQL 19 pg_config)
endif

lake_dir := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/..)
rust_dir := $(CURDIR)/.pgdatalake-build
CARGO_TARGET_DIR ?= $(rust_dir)/target
header := $(rust_dir)/pg_datalake_core.h
archive := $(rust_dir)/libpgdatalake_core.a

PG_CPPFLAGS = -DLAKEHOUSE_STOCK_FDW -I$(lake_dir) -I$(rust_dir)
SHLIB_LINK = $(archive) -lpthread -ldl -lm
EXTRA_CLEAN = .pgdatalake-build

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

vpath %.c $(lake_dir)
$(OBJS) $(OBJS:.o=.bc): $(header) $(addprefix $(lake_dir)/,lakehouse.h lakehouse_fdw.h batch_fill.h)
$(shlib): $(archive)
$(header) $(archive) &: $(shell find $(lake_dir)/core/pgdatalake-core/src -name '*.rs') \
    $(lake_dir)/core/pgdatalake-core/Cargo.toml $(lake_dir)/core/Cargo.toml \
    $(lake_dir)/core/Cargo.lock $(lake_dir)/core/cbindgen.toml \
    $(lake_dir)/core/rust-toolchain.toml $(lake_dir)/rust_build.py
	mkdir -p "$(rust_dir)"
	env -u MAKEFLAGS -u MFLAGS python3 "$(lake_dir)/rust_build.py" --manifest-dir "$(lake_dir)/core" \
	    --target-dir "$(abspath $(CARGO_TARGET_DIR))" --out-dir "$(rust_dir)"
	@touch "$(header)" "$(archive)"

.PHONY: check-shared-sources
check-shared-sources: $(header)
	python3 $(lake_dir)/../../../../scripts/check_shared_sources.py --cc $(CC) $(CFLAGS) $(CPPFLAGS)
