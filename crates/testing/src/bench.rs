//! Harness core: the instrumented chunk store a measurement harness reads its
//! work counts from, and the provenance header a result document opens with.
//!
//! Counters are atomic so an instrumented store stays `Sync`.

use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

use nectar_primitives::ChunkOps;
use nectar_primitives::chunk::{Chunk, ChunkAddress, ChunkRegistry, StandardChunkSet, Verified};
use nectar_primitives::store::{ChunkGet, ChunkPut, MemoryStore};
use serde::Serialize;

/// A point-in-time read of every counter plus the resident chunk count.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counts {
    /// The delta around one operation is that operation's hop count.
    pub gets: u64,
    /// Rewrites of a resident address included.
    pub puts: u64,
    pub put_bytes: u64,
    /// Payload bytes resident; grows only on a not-yet-present address.
    pub live_bytes: u64,
    /// Puts of an address that was not already resident.
    pub distinct_puts: u64,
    /// Distinct resident addresses.
    pub total_chunks: u64,
    /// Probes answered absent.
    pub absent: u64,
}

/// The atomic counter bank an instrumented store records into.
#[derive(Debug, Default)]
pub struct Counters {
    gets: AtomicU64,
    puts: AtomicU64,
    put_bytes: AtomicU64,
    live_bytes: AtomicU64,
    distinct_puts: AtomicU64,
    absent: AtomicU64,
}

impl Counters {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_get(&self) {
        self.gets.fetch_add(1, SeqCst);
    }

    /// `resident` says the address was already stored, so residency does not
    /// grow.
    pub fn record_put(&self, bytes: u64, resident: bool) {
        self.puts.fetch_add(1, SeqCst);
        self.put_bytes.fetch_add(bytes, SeqCst);
        if !resident {
            self.distinct_puts.fetch_add(1, SeqCst);
            self.live_bytes.fetch_add(bytes, SeqCst);
        }
    }

    /// Record a probe answered with a definitively absent report.
    pub fn record_absent(&self) {
        self.absent.fetch_add(1, SeqCst);
    }

    /// Read every counter, `total_chunks` supplied by the caller's store.
    #[must_use]
    pub fn snapshot(&self, total_chunks: u64) -> Counts {
        Counts {
            gets: self.gets.load(SeqCst),
            puts: self.puts.load(SeqCst),
            put_bytes: self.put_bytes.load(SeqCst),
            live_bytes: self.live_bytes.load(SeqCst),
            distinct_puts: self.distinct_puts.load(SeqCst),
            total_chunks,
            absent: self.absent.load(SeqCst),
        }
    }

    #[must_use]
    pub fn gets(&self) -> u64 {
        self.gets.load(SeqCst)
    }

    #[must_use]
    pub fn puts(&self) -> u64 {
        self.puts.load(SeqCst)
    }
}

/// In-memory chunk store that counts gets, puts and byte residency.
#[derive(Debug)]
pub struct CountingStore<R: ChunkRegistry = StandardChunkSet> {
    inner: MemoryStore<R>,
    counters: Counters,
}

impl<R: ChunkRegistry> Default for CountingStore<R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R: ChunkRegistry> CountingStore<R> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: MemoryStore::new(),
            counters: Counters::new(),
        }
    }

    #[must_use]
    pub const fn counters(&self) -> &Counters {
        &self.counters
    }

    #[must_use]
    pub fn snapshot(&self) -> Counts {
        self.counters.snapshot(self.inner.len() as u64)
    }

    #[must_use]
    pub fn gets(&self) -> u64 {
        self.counters.gets()
    }

    #[must_use]
    pub fn puts(&self) -> u64 {
        self.counters.puts()
    }
}

impl<R: ChunkRegistry> ChunkPut<Chunk<Verified, R>> for CountingStore<R> {
    type Error = std::convert::Infallible;

    async fn put(&self, chunk: Chunk<Verified, R>) -> Result<(), Self::Error> {
        let bytes = chunk.envelope().data().len() as u64;
        let address = *chunk.address();
        let resident = self.inner.get(&address).is_some();
        self.counters.record_put(bytes, resident);
        ChunkPut::put(&self.inner, chunk).await
    }
}

impl<R: ChunkRegistry> ChunkGet<R> for CountingStore<R> {
    type Trust = Verified;
    type Error = <MemoryStore<R> as ChunkGet<R>>::Error;

    async fn get(&self, address: &ChunkAddress) -> Result<Chunk<Verified, R>, Self::Error> {
        self.counters.record_get();
        ChunkGet::get(&self.inner, address).await
    }
}

/// The provenance header a result document opens with.
#[derive(Clone, Debug, Serialize)]
pub struct RunMeta {
    /// `SOURCE_DATE_EPOCH` pins it, so two runs are byte-identical.
    pub generated: String,
    pub git_branch: String,
    pub git_commit: String,
    /// The single version authority for the harness and its schema.
    pub harness_version: String,
}

impl RunMeta {
    #[must_use]
    pub fn current(harness_version: &str) -> Self {
        let pinned = std::env::var("SOURCE_DATE_EPOCH")
            .ok()
            .and_then(|v| v.parse().ok());
        Self {
            generated: generated_iso(pinned),
            git_branch: git(&["rev-parse", "--abbrev-ref", "HEAD"]),
            git_commit: git(&["rev-parse", "HEAD"]),
            harness_version: harness_version.to_string(),
        }
    }
}

fn git(args: &[&str]) -> String {
    Command::new("git")
        .args(args)
        .output()
        .ok()
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

/// RFC 3339 UTC seconds for `epoch_secs`, or the current wall clock when
/// `None`.
fn generated_iso(epoch_secs: Option<u64>) -> String {
    let secs = epoch_secs.unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs())
    });
    iso_utc(secs)
}

/// Proleptic-Gregorian UTC render of a Unix timestamp, seconds precision.
fn iso_utc(secs: u64) -> String {
    let (h, m, s) = ((secs / 3600) % 24, (secs / 60) % 60, secs % 60);
    let z = (secs / 86_400) as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(mo <= 2);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}Z")
}

#[cfg(test)]
mod tests {
    use super::{Counters, RunMeta, generated_iso, iso_utc};

    #[test]
    fn iso_render_is_correct_at_known_instants() {
        assert_eq!(iso_utc(0), "1970-01-01T00:00:00Z");
        assert_eq!(iso_utc(951_868_800), "2000-03-01T00:00:00Z");
        assert_eq!(iso_utc(1_767_225_600), "2026-01-01T00:00:00Z");
        assert_eq!(generated_iso(Some(86_399)), "1970-01-01T23:59:59Z");
    }

    #[test]
    fn residency_grows_on_a_first_put_alone() {
        let counters = Counters::new();
        counters.record_put(100, false);
        counters.record_put(100, true);
        counters.record_get();
        counters.record_absent();
        counters.record_get();

        let counts = counters.snapshot(1);
        assert_eq!((counts.puts, counts.distinct_puts), (2, 1));
        assert_eq!((counts.put_bytes, counts.live_bytes), (200, 100));
        assert_eq!((counts.absent, counts.gets), (1, 2));
    }

    #[test]
    fn run_meta_keeps_its_field_order() {
        let json = serde_json::to_string(&RunMeta::current("7")).unwrap();
        let keys: Vec<&str> = json
            .split('"')
            .skip(1)
            .step_by(4)
            .take(4)
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            ["generated", "git_branch", "git_commit", "harness_version"]
        );
        assert!(json.ends_with(r#""harness_version":"7"}"#), "{json}");
    }
}
