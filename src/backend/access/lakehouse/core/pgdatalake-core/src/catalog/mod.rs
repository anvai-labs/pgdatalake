//! Catalog clients (specs/lakehouse/03-catalog.md).
//!
//! `rest` is the primary catalog type (tier 1); Glue/Nessie/Hadoop follow.

pub mod metadata;
pub mod oauth;
pub mod rest;
pub mod types;
