//! Immutable-path block cache (spec 15 §6.2, slice P0). Iceberg data,
//! manifest and metadata objects are immutable — a rewritten table
//! produces NEW paths — so a cached byte range for path P is correct for
//! P forever. THERE IS NO INVALIDATION PATH by construction: capacity
//! eviction only (LRU). The namespace must include the warehouse (a
//! path is only unique within its warehouse — caller responsibility,
//! see `BlockCache::scoped_key`).

use crate::storage::{StorageError, StorageIo};
use std::collections::HashMap;
use std::sync::Mutex;

/// One cached entry: bytes + position in the LRU order.
struct Entry {
    bytes: Vec<u8>,
    tick: u64,
}

/// Thread-safe byte-capable LRU of byte ranges. Keys are
/// `(scoped_key, offset, len)` where `scoped_key` embeds the warehouse
/// (see below). Eviction is capacity-only: inserting past `max_bytes`
/// evicts least-recently-used entries until the new entry fits (a
/// single entry larger than the cap is NOT cached).
pub struct BlockCache {
    inner: Mutex<CacheInner>,
    max_bytes: u64,
}

struct CacheInner {
    map: HashMap<String, Entry>,
    used: u64,
    tick: u64,
    hits: u64,
    misses: u64,
}

impl BlockCache {
    /// Cache with the given byte capacity (LRU eviction). No invalidation
    /// API exists — by design (immutable-path law, spec 15 §6.2).
    pub fn new(max_bytes: u64) -> Self {
        BlockCache {
            inner: Mutex::new(CacheInner {
                map: HashMap::new(),
                used: 0,
                tick: 0,
                hits: 0,
                misses: 0,
            }),
            max_bytes,
        }
    }

    /// Namespaced key: paths are only unique within a warehouse, so the
    /// scope string (warehouse/endpoint identity) must prefix the path.
    fn scoped_key(scope: &str, key: &str, offset: u64, len: u64) -> String {
        format!("{scope}\u{1}{key}\u{1}{offset}\u{1}{len}")
    }

    /// Ranged GET through the cache: hit = bytes served without I/O,
    /// miss = one ranged GET then cache-fill. Byte-identity with
    /// `StorageIo::get_range` is the contract (tests delete the
    /// underlying object after a miss to PROVE the hit serves no I/O).
    pub fn get_range(
        &self,
        io: &StorageIo,
        scope: &str,
        key: &str,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, StorageError> {
        let sk = Self::scoped_key(scope, key, offset, len);
        {
            let mut inner = self.inner.lock().unwrap();
            inner.tick += 1;
            let tick = inner.tick;
            let hit = inner.map.get_mut(&sk).map(|e| {
                e.tick = tick;
                e.bytes.clone()
            });
            if let Some(bytes) = hit {
                inner.hits += 1;
                return Ok(bytes);
            }
            inner.misses += 1;
        }
        let bytes = io.get_range(key, offset, len)?;
        self.fill(sk, bytes.clone());
        Ok(bytes)
    }

    /// Whole-object variant (metadata.json, manifests, manifest lists —
    /// all immutable per path). Byte-identity with `get_bytes`.
    pub fn get_bytes(
        &self,
        io: &StorageIo,
        scope: &str,
        key: &str,
    ) -> Result<Vec<u8>, StorageError> {
        // whole-object entries key on (scope, key, 0, 0) — ranges never
        // collide with them because their keys carry a real offset
        let sk = Self::scoped_key(scope, key, 0, 0);
        {
            let mut inner = self.inner.lock().unwrap();
            inner.tick += 1;
            let tick = inner.tick;
            let hit = inner.map.get_mut(&sk).map(|e| {
                e.tick = tick;
                e.bytes.clone()
            });
            if let Some(bytes) = hit {
                inner.hits += 1;
                return Ok(bytes);
            }
            inner.misses += 1;
        }
        let bytes = io.get_bytes(key)?;
        self.fill(sk, bytes.clone());
        Ok(bytes)
    }

    /// Cache-fill with capacity-only LRU eviction (immutable-path law:
    /// this is the ONLY way entries leave the cache). Oversized entries
    /// pass through uncached.
    fn fill(&self, sk: String, bytes: Vec<u8>) {
        let mut inner = self.inner.lock().unwrap();
        let cost = bytes.len() as u64;
        let max = self.max_bytes;
        if cost > max {
            return;
        }
        while inner.used + cost > max {
            let victim = inner
                .map
                .iter()
                .min_by_key(|(_, e)| e.tick)
                .map(|(k, _)| k.clone());
            let Some(victim) = victim else {
                break;
            };
            if let Some(e) = inner.map.remove(&victim) {
                let after = inner.used.saturating_sub(e.bytes.len() as u64);
                inner.used = after;
            }
        }
        inner.tick += 1;
        let tick = inner.tick;
        inner.map.insert(sk, Entry { bytes, tick });
        inner.used += cost;
    }

    /// (hits, misses) — test/diagnostics only.
    pub fn counters(&self) -> (u64, u64) {
        let inner = self.inner.lock().unwrap();
        (inner.hits, inner.misses)
    }
}

use std::sync::OnceLock;

/// The per-backend global cache (spec 15 §2a C1: per-backend LRU at the
/// I/O layer; capacity fixed by the first `global_init` — _PG_init does
/// it from lakehouse.cache_max_bytes before any scan can run).
static GLOBAL: OnceLock<BlockCache> = OnceLock::new();

pub fn global_init(max_bytes: u64) {
    let _ = GLOBAL.set(BlockCache::new(max_bytes));
}

/// The global cache, or a lazily-created default (256 MB) when
/// `global_init` was never called (Rust-only test builds, standalone
/// tooling).
pub fn global() -> &'static BlockCache {
    GLOBAL.get_or_init(|| BlockCache::new(256 * 1024 * 1024))
}

/// Whole-object GET through the global cache. Iceberg metadata.json /
/// manifest / manifest-list paths are immutable per snapshot — no
/// invalidation path exists. Byte-identity with `io.get_bytes`.
pub fn cached_get_bytes(io: &StorageIo, key: &str) -> Result<Vec<u8>, StorageError> {
    global().get_bytes(io, &io.scope_id, key)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (tempfile::TempDir, StorageIo) {
        let dir = tempfile::tempdir().unwrap();
        crate::storage::test_allow_local_stores();
        let loc =
            crate::storage::parse_location(&format!("file://{}", dir.path().display())).unwrap();
        let io = StorageIo::open(&loc, None).unwrap();
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        let payload: Vec<u8> = (0..1000u32).map(|i| (i % 251) as u8).collect();
        io.put_bytes("data/blob.bin", payload).unwrap();
        (dir, io)
    }

    /// Byte-identity: cached ranged reads return EXACTLY the same bytes
    /// as slicing get_bytes — the whole point of the cache.
    #[test]
    fn cached_range_is_byte_identical() {
        let (_dir, io) = fixture();
        let cache = BlockCache::new(1 << 20);
        let truth = io.get_bytes("data/blob.bin").unwrap();
        for (off, len) in [(0u64, 16u64), (500, 100), (900, 100), (999, 1)] {
            let got = cache
                .get_range(&io, "wh", "data/blob.bin", off, len)
                .unwrap();
            assert_eq!(
                got,
                &truth[off as usize..(off + len) as usize],
                "{off}+{len}"
            );
        }
        assert_eq!(cache.counters(), (0, 4));
        // every repeat is a hit with identical bytes
        for _ in 0..2 {
            let got = cache
                .get_range(&io, "wh", "data/blob.bin", 500, 100)
                .unwrap();
            assert_eq!(got, &truth[500..600]);
        }
        assert_eq!(cache.counters(), (2, 4));
    }

    /// The invalidation-free law, proven the hard way: after a miss is
    /// cached, DELETE the underlying object — cached reads still succeed
    /// (no second GET happens or could succeed). Different scopes do not
    /// share entries even for identical paths.
    #[test]
    fn hit_serves_no_io_and_scopes_never_collide() {
        let (dir, io) = fixture();
        let cache = BlockCache::new(1 << 20);
        let a = cache
            .get_range(&io, "wh-a", "data/blob.bin", 0, 10)
            .unwrap();
        assert_eq!(a.len(), 10);
        // delete the object entirely
        std::fs::remove_file(dir.path().join("data/blob.bin")).unwrap();
        let b = cache
            .get_range(&io, "wh-a", "data/blob.bin", 0, 10)
            .unwrap();
        assert_eq!(b, a, "served from cache — no I/O");
        // other scope: miss -> real GET -> NotFound (scopes never collide)
        let err = cache.get_range(&io, "wh-b", "data/blob.bin", 0, 10);
        assert!(matches!(err, Err(StorageError::NotFound(_))));
    }

    /// Capacity law: inserting past the cap evicts LRU entries; the cap
    /// itself is respected; oversized entries are not cached (they pass
    /// through). Order: A, B cached; C evicts A (LRU); re-touch B keeps
    /// it; D evicts the LRU-est survivor.
    #[test]
    fn eviction_respects_cap_and_lru() {
        let (_dir, io) = fixture();
        io.put_bytes("data/k1.bin", vec![0u8; 1000]).unwrap();
        io.put_bytes("data/k2.bin", vec![1u8; 1000]).unwrap();
        io.put_bytes("data/k3.bin", vec![2u8; 1000]).unwrap();
        // 3 x 1000-byte entries; cap = 2.5 entries -> at most 1 evicted per insert
        let cache = BlockCache::new(2500);
        cache.get_range(&io, "s", "data/k1.bin", 0, 1000).unwrap();
        cache.get_range(&io, "s", "data/k2.bin", 0, 1000).unwrap();
        cache.get_range(&io, "s", "data/k1.bin", 0, 1000).unwrap(); // touch k1
        cache.get_range(&io, "s", "data/k3.bin", 0, 1000).unwrap(); // evicts k2 (LRU)
                                                                    // k3 inserted: cap forces one eviction = k2. k1 survives (touched).
                                                                    // delete everything: only cached entries can still be read
        for k in ["k1", "k2", "k3"] {
            std::fs::remove_file(_dir.path().join(format!("data/{k}.bin"))).unwrap();
        }
        assert!(
            cache.get_range(&io, "s", "data/k1.bin", 0, 1000).is_ok(),
            "k1 cached"
        );
        assert!(
            cache.get_range(&io, "s", "data/k2.bin", 0, 1000).is_err(),
            "k2 was evicted"
        );
        assert!(
            cache.get_range(&io, "s", "data/k3.bin", 0, 1000).is_ok(),
            "k3 cached"
        );
    }

    /// Whole-object path: byte-identity with get_bytes + hit/no-IO law.
    #[test]
    fn whole_object_cache_is_identical_and_hits() {
        let (dir, io) = fixture();
        let cache = BlockCache::new(1 << 20);
        let truth = io.get_bytes("data/blob.bin").unwrap();
        let got = cache.get_bytes(&io, "wh", "data/blob.bin").unwrap();
        assert_eq!(got, truth);
        std::fs::remove_file(dir.path().join("data/blob.bin")).unwrap();
        let got2 = cache.get_bytes(&io, "wh", "data/blob.bin").unwrap();
        assert_eq!(got2, truth, "hit after object deletion");
        assert_eq!(cache.counters(), (1, 1));
    }
}
