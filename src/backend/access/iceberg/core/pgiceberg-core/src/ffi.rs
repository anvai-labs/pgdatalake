//! The single C ABI boundary of pgiceberg-core (specs/iceberg/RULES.md §4).
//!
//! Contract:
//! - no Rust types cross the boundary other than `#[repr(C)]` layouts and
//!   NUL-terminated string pointers owned by the library;
//! - no panics unwind across the boundary — M0's exported functions are
//!   infallible; M1 introduces Result-shaped C structs with caller-owned
//!   message buffers.

// Only module allowed to relax the crate-level `deny(unsafe_code)`: the
// `#[no_mangle]` export below is an unsafe attribute.
#![allow(unsafe_code)]

use std::ffi::{c_char, CStr};

/// Static, NUL-terminated version string of the core, in the form
/// `pgiceberg-core <major>.<minor>.<patch> (<milestone>)`.
///
/// # C signature (cbindgen-generated `pgiceberg_core.h`)
/// `const char *pgiceberg_core_version(void);`
///
/// Single source of truth for the string is `version_cstr()` below.
#[no_mangle]
pub extern "C" fn pgiceberg_core_version() -> *const c_char {
    version_cstr().as_ptr()
}

/// The version string as a borrowed C string; single source of truth for the
/// exported ABI value and its unit test.
fn version_cstr() -> &'static CStr {
    c"pgiceberg-core 0.1.0 (M0)"
}

#[cfg(test)]
mod tests {
    use super::*;

    /// TDD anchor (M0, specs/iceberg/12-roadmap.md): the exported version
    /// string follows the documented format `pgiceberg-core <semver> (<M#>)`
    /// and is NUL-terminated.
    #[test]
    fn version_string_format() {
        let v = version_cstr().to_str().expect("UTF-8 version");
        assert!(v.starts_with("pgiceberg-core "), "prefix: {v}");
        let rest = v.strip_prefix("pgiceberg-core ").unwrap();
        let (ver, milestone) = rest.split_once(' ').expect("space before milestone");
        let parts: Vec<_> = ver.split('.').collect();
        assert_eq!(parts.len(), 3, "semver in {ver}");
        assert!(
            parts.iter().all(|p| p.parse::<u64>().is_ok()),
            "semver in {ver}"
        );
        assert!(
            milestone.starts_with('(') && milestone.ends_with(')'),
            "milestone: {milestone}"
        );
    }
}
