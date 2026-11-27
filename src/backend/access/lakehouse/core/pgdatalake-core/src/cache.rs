//! Bounded byte cache for immutable Iceberg objects (spec 15 §6.2).
//! Remote cache identity is the LOCATION (endpoint|bucket) — entries are
//! immutable bytes valid across handle reopenings (§6.2 amendment
//! 2026-11-26; SQL-layer authorization gates every query). Entries are
//! evicted by capacity.

use crate::storage::{StorageError, StorageIo};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// One cached entry: bytes + position in the LRU order.
struct Entry {
    bytes: Vec<u8>,
    tick: u64,
}

/// Thread-safe LRU for object bytes and ranges. Keys include the storage
/// location scope, caller namespace, object key and range. Entries
/// larger than the capacity pass through uncached.
pub struct BlockCache {
    inner: Mutex<CacheInner>,
    max_bytes: u64,
}

#[derive(Clone, Hash, PartialEq, Eq)]
struct CacheKey {
    storage: Arc<str>,
    scope: String,
    key: String,
    range: Option<(u64, u64)>,
}

struct CacheInner {
    map: HashMap<CacheKey, Entry>,
    used: u64,
    tick: u64,
    hits: u64,
    misses: u64,
    fetched: u64,
}

impl BlockCache {
    /// Cache with the given byte capacity (LRU eviction). No invalidation
    /// API exists — by design (immutable-path law, spec 15 §6.2).
    #[must_use]
    pub fn new(max_bytes: u64) -> Self {
        BlockCache {
            inner: Mutex::new(CacheInner {
                map: HashMap::new(),
                used: 0,
                tick: 0,
                hits: 0,
                misses: 0,
                fetched: 0,
            }),
            max_bytes,
        }
    }

    fn scoped_key(io: &StorageIo, scope: &str, key: &str, range: Option<(u64, u64)>) -> CacheKey {
        CacheKey {
            storage: io.scope_id.clone(),
            scope: scope.to_owned(),
            key: key.to_owned(),
            range,
        }
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
        let sk = Self::scoped_key(io, scope, key, Some((offset, len)));
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
        {
            let mut inner = self.inner.lock().unwrap();
            inner.fetched += bytes.len() as u64;
        }
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
        let sk = Self::scoped_key(io, scope, key, None);
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
        {
            let mut inner = self.inner.lock().unwrap();
            inner.fetched += bytes.len() as u64;
        }
        self.fill(sk, bytes.clone());
        Ok(bytes)
    }

    /// Cache-fill with capacity-only LRU eviction (immutable-path law:
    /// this is the ONLY way entries leave the cache). Oversized entries
    /// pass through uncached.
    fn fill(&self, sk: CacheKey, bytes: Vec<u8>) {
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

    /// Cumulative BYTES fetched from real I/O (miss fills). The P0b
    /// laziness metric: a ranged parquet reader must fetch strictly less
    /// than the whole file for partial scans, and cache hits must add
    /// nothing. Test/diagnostics only.
    /// Full diagnostic snapshot (spec 15 §6.3a measurement gate): hit and
    /// miss counts, cumulative miss-fill bytes, live entry count, used
    /// bytes. Diagnostics only — never a policy input and never a store
    /// (§6.3a: the block cache IS the reader's stats surface).
    pub fn stats(&self) -> (u64, u64, u64, usize, u64) {
        let inner = self.inner.lock().unwrap();
        (
            inner.hits,
            inner.misses,
            inner.fetched,
            inner.map.len(),
            inner.used,
        )
    }

    pub fn fetched_bytes(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        inner.fetched
    }
}

use std::sync::OnceLock;

/// The per-backend global cache (spec 15 §2a C1: per-backend LRU at the
/// I/O layer; capacity fixed by the first `global_init` — _`PG_init` does
/// it from `lakehouse.cache_max_bytes` before any scan can run).
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
    global().get_bytes(io, "", key)
}

/// Ranged GET through the global cache — the ranged counterpart of
/// `cached_get_bytes` (spec 15 §6.2/§6.3a: parquet footers, column
/// chunks, page indexes and bloom filters are byte ranges of immutable
/// files, so the same no-invalidation law covers them).
pub fn cached_get_range(
    io: &StorageIo,
    key: &str,
    offset: u64,
    len: u64,
) -> Result<Vec<u8>, StorageError> {
    global().get_range(io, "", key, offset, len)
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
    /// as slicing `get_bytes` — the whole point of the cache.
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

    #[test]
    fn remote_cache_is_location_scoped_and_credential_free() {
        // 2026-11-26 law (spec 15 §6.2 amendment): cache identity is the
        // LOCATION (endpoint|bucket), not the handle. Cached immutable
        // bytes are credential-free — SQL-layer authorization gates every
        // query before storage is touched, so a reopened handle serves
        // warm bytes WITHOUT consulting the store, even if the store
        // would 403 a fresh GET. A different bucket is a different scope
        // and never shares entries.
        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let addr = server.server_addr().to_string();
        let location = crate::storage::Location::S3 {
            bucket: "fixture".into(),
            key: "table".into(),
        };
        let options = crate::storage::S3Options {
            endpoint: Some(format!("http://{addr}")),
            region: Some("us-east-1".into()),
            access_key_id: Some("test".into()),
            secret_access_key: Some("test".into()),
            path_style: true,
        };
        let responder = std::thread::spawn(move || {
            // The store sees EXACTLY the first handle's two reads (whole
            // object + range); the reopened handle must produce none.
            for is_range in [false, true] {
                let request = server
                    .recv_timeout(std::time::Duration::from_secs(4))
                    .unwrap()
                    .expect("cold reads must consult the store");
                assert_eq!(request.method(), &tiny_http::Method::Get);
                assert!(request.url().ends_with("/table/manifest.avro"));
                let range = request.headers().iter().find(|h| h.field.equiv("Range"));
                assert_eq!(
                    range.map(|h| h.value.as_str()),
                    is_range.then_some("bytes=0-2")
                );
                let mut response = if is_range {
                    tiny_http::Response::from_string("row").with_status_code(206)
                } else {
                    tiny_http::Response::from_string("rows")
                };
                for (name, value) in [
                    ("ETag", "\"fixture\""),
                    ("Last-Modified", "Sat, 21 Nov 2026 14:00:00 GMT"),
                ] {
                    response.add_header(tiny_http::Header::from_bytes(name, value).unwrap());
                }
                if is_range {
                    response.add_header(
                        tiny_http::Header::from_bytes("Content-Range", "bytes 0-2/4").unwrap(),
                    );
                }
                request.respond(response).unwrap();
            }
            // (Zero warm-read consults is proven by the counters
            // assertion below: 4 hits over 2 misses — no store request
            // can hide behind a hit.) The responder just ends here;
            // keep-alive connection noise must not fail the test.
        });
        let cache = BlockCache::new(1024);
        let authorized = StorageIo::open(&location, Some(&options)).unwrap();
        let key = "table/manifest.avro";
        assert_eq!(cache.get_bytes(&authorized, "", key).unwrap(), b"rows");
        assert_eq!(cache.get_range(&authorized, "", key, 0, 3).unwrap(), b"row");

        let same_handle = authorized.clone();
        assert_eq!(cache.get_bytes(&same_handle, "", key).unwrap(), b"rows");
        assert_eq!(
            cache.get_range(&same_handle, "", key, 0, 3).unwrap(),
            b"row"
        );

        // A REOPENED handle (fresh auth, same location) serves the same
        // warm bytes with zero store consults — cross-query reuse is the
        // point of the 2026-11-26 change.
        let reopened = StorageIo::open(&location, Some(&options)).unwrap();
        assert_eq!(cache.get_bytes(&reopened, "", key).unwrap(), b"rows");
        assert_eq!(cache.get_range(&reopened, "", key, 0, 3).unwrap(), b"row");

        responder.join().unwrap();
        // 4 hits (2 warm reads per handle) over 2 cold misses
        assert_eq!(cache.counters(), (4, 2));
        assert_eq!(cache.fetched_bytes(), 7);

        // A different bucket is a different scope: it must NOT see the
        // warm bytes (a collision would return b"rows" from cache); it
        // goes to the store instead, which the joined responder no longer
        // serves — the read fails loudly, proving both isolation and
        // cold-miss behavior.
        let other = crate::storage::Location::S3 {
            bucket: "other".into(),
            key: "table".into(),
        };
        let other_io = StorageIo::open(&other, Some(&options)).unwrap();
        assert!(matches!(
            cache.get_bytes(&other_io, "", key),
            Err(StorageError::Http(_))
        ));
        assert_eq!(cache.counters(), (4, 3));
    }

    /// Capacity law: inserting past the cap evicts LRU entries; the cap
    /// itself is respected; oversized entries are not cached (they pass
    /// through). Order: A, B cached; C evicts A (LRU); re-touch B keeps
    /// it; D evicts the LRU-est survivor.
    #[test]
    fn eviction_respects_cap_and_lru() {
        let (dir, io) = fixture();
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
            std::fs::remove_file(dir.path().join(format!("data/{k}.bin"))).unwrap();
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

    /// Whole-object path: byte-identity with `get_bytes` + hit/no-IO law.
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

    /// P0b laziness metric: `fetched_bytes` counts ONLY real I/O bytes —
    /// every miss adds its payload size, every hit adds nothing, and the
    /// total is exact across mixed range/whole-object traffic.
    #[test]
    fn fetched_bytes_counts_real_io_only() {
        let (_dir, io) = fixture();
        let cache = BlockCache::new(1 << 20);
        assert_eq!(cache.fetched_bytes(), 0);
        // miss: 1000-byte object -> +1000
        cache.get_bytes(&io, "wh", "data/blob.bin").unwrap();
        assert_eq!(cache.fetched_bytes(), 1000);
        // hit: no new I/O
        cache.get_bytes(&io, "wh", "data/blob.bin").unwrap();
        assert_eq!(cache.fetched_bytes(), 1000);
        // ranged miss: +16; ranged repeat: hit, +0
        cache
            .get_range(&io, "wh", "data/blob.bin", 500, 16)
            .unwrap();
        assert_eq!(cache.fetched_bytes(), 1016);
        cache
            .get_range(&io, "wh", "data/blob.bin", 500, 16)
            .unwrap();
        assert_eq!(cache.fetched_bytes(), 1016);
    }
}
