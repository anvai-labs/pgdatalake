//! pgdatalake-core — the Rust core of the PostgreSQL × Apache Iceberg fork.
//!
//! Design: specs/lakehouse/01-architecture.md (decision D2). The crate exposes a
//! C ABI (see `ffi`, declared in the cbindgen-generated `pg_datalake_core.h`)
//! and is statically linked into the `lakehouse` and `lakehouse_fdw` modules.
//!
//! M0 scope: build skeleton + version ABI only; catalog clients, metadata
//! model, file IO and Parquet land from M1 on (specs/lakehouse/12-roadmap.md).

// `deny` (not `forbid`) so that the single FFI boundary module may re-enable
// unsafe (it needs the unsafe `#[no_mangle]` attribute) per RULES §4.
#![deny(unsafe_code)]
#![warn(clippy::all)]

pub mod catalog;
pub mod ffi;
pub mod storage;
