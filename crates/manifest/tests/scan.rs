//! Ordered read operations through the public API: a counting store witnesses
//! that iteration fetches trie nodes on the frontier only, never a value chunk,
//! and a std map oracles the key order of iteration, ranges, prefixes and floor.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Result, ensure};
use arbitrary::Unstructured;
use bytes::Bytes;
use nectar_manifest::{Builder, Cursor, Entry, Format, Key, Reader, V1, generators};
use nectar_primitives::store::{ChunkGet, MemoryStore};
use nectar_primitives::{Chunk, ChunkAddress, StandardChunkSet, Verified};
use nectar_testing::run;
use proptest::prelude::*;

/// A trusted store that counts every `get`, so a test can read off how many
/// nodes a walk fetched.
#[derive(Debug, Default)]
struct CountingStore {
    inner: MemoryStore,
    gets: AtomicUsize,
}

impl CountingStore {
    fn gets(&self) -> usize {
        self.gets.load(Ordering::Relaxed)
    }
}

impl ChunkGet<StandardChunkSet> for CountingStore {
    type Trust = Verified;
    type Error = <MemoryStore as ChunkGet>::Error;

    async fn get(
        &self,
        address: &ChunkAddress,
    ) -> Result<Chunk<Verified, StandardChunkSet>, Self::Error> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        ChunkGet::get(&self.inner, address).await
    }
}

/// An ordered map of raw keys to their values.
type Oracle = BTreeMap<Vec<u8>, Entry>;
/// Rows drained from a cursor: raw key and value.
type Rows = Vec<(Vec<u8>, Entry)>;

/// A spread of keys (mapped to raw value bytes) that forces shared-prefix
/// compaction, forks carrying both a value and a continuation, and, once a
/// subtree outgrows the inline bound, referenced children the walk must fetch
/// and cross. Every value is inline, so the store holds trie nodes only: no
/// value chunk exists to fetch.
fn corpus() -> Vec<(Key, Vec<u8>)> {
    let mut pairs: Vec<(Key, Vec<u8>)> = Vec::new();
    for dir in 0u8..16 {
        for file in 0u8..40 {
            let key = format!("dir{dir:02}/file{file:04}.txt");
            pairs.push((Key::from(key.into_bytes()), vec![dir ^ file; 32]));
        }
    }
    // A key that is a strict prefix of another: a fork with a value and a child.
    pairs.push((Key::from(&b"pre"[..]), b"P".to_vec()));
    pairs.push((Key::from(&b"prefix"[..]), b"PX".to_vec()));
    // The empty key: the manifest's own value in the root extension.
    pairs.push((Key::empty(), b"root".to_vec()));
    pairs
}

/// Build the corpus into `store` and return the root and an ordered oracle.
fn build(store: &MemoryStore) -> Result<(ChunkAddress, Oracle)> {
    let mut builder = Builder::new();
    let mut oracle = Oracle::new();
    for (key, bytes) in corpus() {
        let value = Entry::inline(Bytes::from(bytes))?;
        builder.insert(key.clone(), value.clone(), None);
        oracle.insert(key.as_bytes().to_vec(), value);
    }
    let built = run(builder.build(store))?;
    Ok((*built.root(), oracle))
}

/// Collect a whole cursor into an ordered vector.
fn collect(mut cursor: Cursor<'_, &CountingStore>) -> Result<Rows> {
    let mut out = Vec::new();
    while let Some((key, value)) = run(cursor.next())? {
        out.push((key.as_bytes().to_vec(), value));
    }
    Ok(out)
}

#[test]
fn iteration_fetches_nodes_not_values() -> Result<()> {
    let memory = MemoryStore::default();
    let (root, oracle) = build(&memory)?;
    // The builder spilled referenced children, so the walk genuinely crosses
    // fetched hops rather than reading one embedded root.
    let nodes = memory.len();
    ensure!(nodes > 1, "manifest spilled referenced children");
    // Every value is inline, so the store holds trie nodes only. If iteration
    // fetched values it would fetch chunks that do not exist.
    ensure!(oracle.len() > nodes, "more keys than trie nodes");

    let store = CountingStore {
        inner: memory,
        gets: AtomicUsize::new(0),
    };
    let reader: Reader<_> = Reader::new(&store);

    let got = collect(run(reader.iter(&root))?)?;
    let expected: Rows = oracle.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    ensure!(got == expected, "iteration matches the ordered oracle");

    // The whole walk fetched exactly the trie nodes, once each, and far fewer
    // than the key count: values ride in the fork records, so no leaf is pulled.
    ensure!(store.gets() == nodes, "one fetch per trie node");
    ensure!(store.gets() < oracle.len(), "fetches below the key count");
    Ok(())
}

#[test]
fn range_matches_the_oracle() -> Result<()> {
    let store = MemoryStore::default();
    let (root, oracle) = build(&store)?;
    let counting = CountingStore {
        inner: store,
        gets: AtomicUsize::new(0),
    };
    let reader: Reader<_> = Reader::new(&counting);

    for (lo, hi) in [
        (&b"dir03"[..], &b"dir07"[..]),
        (&b"dir05/file0010.txt"[..], &b"dir05/file0020.txt"[..]),
        (&b""[..], &b"dir00/file0005.txt"[..]),
        (&b"pre"[..], &b"zzz"[..]),
        (&b"dir11/file0039.txt"[..], &b"zzz"[..]),
    ] {
        let got = collect(run(reader.range(&root, &Key::from(lo), &Key::from(hi)))?)?;
        let expected: Rows = oracle
            .range(lo.to_vec()..hi.to_vec())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        ensure!(got == expected, "range matches the oracle");
    }
    Ok(())
}

#[test]
fn prefix_matches_the_oracle() -> Result<()> {
    let store = MemoryStore::default();
    let (root, oracle) = build(&store)?;
    let reader: Reader<_> = Reader::new(&store);

    for p in [
        &b"dir05/"[..],
        &b"dir1"[..],
        &b"pre"[..],
        &b"dir05/file000"[..],
        &b""[..],
    ] {
        let got: Rows = {
            let mut cursor = run(reader.prefix(&root, &Key::from(p)))?;
            let mut out = Vec::new();
            while let Some((key, value)) = run(cursor.next())? {
                out.push((key.as_bytes().to_vec(), value));
            }
            out
        };
        let expected: Rows = oracle
            .iter()
            .filter(|(k, _)| k.starts_with(p))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        ensure!(got == expected, "prefix matches the oracle");
    }
    Ok(())
}

/// A future that yields once: `Pending` on the first poll, `Ready` after. It
/// forces the executor to poll the other in-flight fetches before any completes,
/// so the gated store below witnesses their true overlap.
#[derive(Debug)]
struct YieldOnce(bool);

impl Future for YieldOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

/// A trusted store that records the peak number of concurrent `get` calls and
/// the total, so a test reads off both the in-flight bound and the fetch count.
/// Each `get` yields once while counted, so concurrent fetches genuinely overlap
/// under the single-threaded test executor.
#[derive(Debug, Default)]
struct GatedStore {
    inner: MemoryStore,
    inflight: AtomicUsize,
    peak: AtomicUsize,
    gets: AtomicUsize,
}

impl ChunkGet<StandardChunkSet> for GatedStore {
    type Trust = Verified;
    type Error = <MemoryStore as ChunkGet>::Error;

    async fn get(
        &self,
        address: &ChunkAddress,
    ) -> Result<Chunk<Verified, StandardChunkSet>, Self::Error> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        let now = self
            .inflight
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        self.peak.fetch_max(now, Ordering::Relaxed);
        YieldOnce(false).await;
        let chunk = ChunkGet::get(&self.inner, address).await;
        self.inflight.fetch_sub(1, Ordering::Relaxed);
        chunk
    }
}

/// Build a wide manifest whose first-byte subtrees each outgrow the inline bound
/// and become referenced children, so the root fans out into many sibling nodes
/// a scan must fetch. Returns the root and an ordered oracle.
fn wide_build(store: &MemoryStore) -> Result<(ChunkAddress, Oracle)> {
    let mut builder = Builder::new();
    let mut oracle = Oracle::new();
    for a in 0u8..48 {
        for b in 0u8..96 {
            let key = vec![a, b];
            let value = Entry::inline(Bytes::from(vec![a ^ b; 32]))?;
            builder.insert(Key::from(key.clone()), value.clone(), None);
            oracle.insert(key, value);
        }
    }
    let built = run(builder.build(store))?;
    Ok((*built.root(), oracle))
}

#[test]
fn read_ahead_bounds_in_flight_and_matches_the_oracle() -> Result<()> {
    let memory = MemoryStore::default();
    let (root, oracle) = wide_build(&memory)?;
    // The root fans out into referenced children, so the walk crosses many
    // sibling hops rather than reading one embedded root.
    let nodes = memory.len();
    ensure!(nodes > 8, "manifest fans out into many referenced children");

    let store = GatedStore {
        inner: memory,
        ..Default::default()
    };
    let reader: Reader<_> = Reader::new(&store);

    let got: Rows = {
        let mut cursor = run(reader.iter(&root))?;
        let mut out = Vec::new();
        while let Some((key, value)) = run(cursor.next())? {
            out.push((key.as_bytes().to_vec(), value));
        }
        out
    };
    let expected: Rows = oracle.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    ensure!(got == expected, "read-ahead iteration matches the oracle");

    // The concurrent walk fetches exactly the trie nodes a serial walk would,
    // once each: read-ahead cuts round trips, not fetch count.
    ensure!(
        store.gets.load(Ordering::Relaxed) == nodes,
        "one fetch per trie node",
    );

    let peak = store.peak.load(Ordering::Relaxed);
    let cap = V1::READ_AHEAD;
    // The window overlapped fetches: read-ahead is genuinely concurrent.
    ensure!(peak > 1, "read-ahead ran fetches concurrently");
    // Peak in-flight never exceeds the cap: the window is bounded, not O(width).
    ensure!(
        peak <= cap,
        "peak in-flight {peak} exceeded the read-ahead cap {cap}",
    );
    Ok(())
}

proptest! {
    // The concurrent cursor returns byte-identical key/value sequences to the
    // ordered oracle over arbitrary key sets: read-ahead reorders fetches, never
    // results.
    #[test]
    fn read_ahead_iteration_matches_any_ordered_oracle(
        pairs in prop::collection::vec(
            (prop::collection::vec(any::<u8>(), 1..6), any::<u8>()),
            0..300,
        ),
    ) {
        let mut oracle = Oracle::new();
        for (key, fill) in pairs {
            let value = Entry::inline(Bytes::from(vec![fill; 32]))
                .map_err(|e| TestCaseError::fail(e.to_string()))?;
            oracle.insert(key, value);
        }
        let store = MemoryStore::default();
        let mut builder = Builder::new();
        for (key, value) in &oracle {
            builder.insert(Key::from(key.clone()), value.clone(), None);
        }
        let built = run(builder.build(&store))
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        let reader: Reader<_> = Reader::new(&store);
        let got: Rows = {
            let mut cursor = run(reader.iter(built.root()))
                .map_err(|e| TestCaseError::fail(e.to_string()))?;
            let mut out = Vec::new();
            while let Some((key, value)) = run(cursor.next())
                .map_err(|e| TestCaseError::fail(e.to_string()))?
            {
                out.push((key.as_bytes().to_vec(), value));
            }
            out
        };
        let expected: Rows = oracle.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        prop_assert_eq!(got, expected);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    // Ordered iteration at the chain regime: low-entropy keys whose shared
    // runs cross PLEN_MAX chain forks over multiple levels, and the cursor
    // still returns exactly the ordered oracle across the chained edges.
    #[test]
    fn iteration_matches_the_oracle_across_chained_edges(
        seed in prop::collection::vec(any::<u8>(), 0..8192),
    ) {
        let mut u = Unstructured::new(&seed);
        let count = u
            .int_in_range(0..=12usize)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        let mut oracle = Oracle::new();
        for _ in 0..count {
            let key = generators::chain_key::<V1>(&mut u)
                .map_err(|e| TestCaseError::fail(e.to_string()))?;
            let fill = u
                .int_in_range(0..=u8::MAX)
                .map_err(|e| TestCaseError::fail(e.to_string()))?;
            let value = Entry::inline(Bytes::from(vec![fill; 16]))
                .map_err(|e| TestCaseError::fail(e.to_string()))?;
            oracle.insert(key.as_bytes().to_vec(), value);
        }
        let store = MemoryStore::default();
        let mut builder = Builder::new();
        for (key, value) in &oracle {
            builder.insert(Key::from(key.clone()), value.clone(), None);
        }
        let built = run(builder.build(&store))
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        let reader: Reader<_> = Reader::new(&store);
        let got: Rows = {
            let mut cursor = run(reader.iter(built.root()))
                .map_err(|e| TestCaseError::fail(e.to_string()))?;
            let mut out = Vec::new();
            while let Some((key, value)) = run(cursor.next())
                .map_err(|e| TestCaseError::fail(e.to_string()))?
            {
                out.push((key.as_bytes().to_vec(), value));
            }
            out
        };
        let expected: Rows = oracle.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        prop_assert_eq!(got, expected);
    }
}

#[test]
fn read_ahead_never_fetches_past_the_upper_bound() -> Result<()> {
    let memory = MemoryStore::default();
    let (root, oracle) = wide_build(&memory)?;
    // The root fans out into one referenced child per leading byte, so there are
    // more sibling subtrees than the window: an unbounded prefetch would pull
    // whole subtrees the bounded walk never visits.
    let siblings = memory.len().saturating_sub(1);
    ensure!(
        siblings > V1::READ_AHEAD,
        "more sibling subtrees than the window",
    );

    // A gated store is essential here: its fetches park on the first poll, so a
    // future the window launches past the bound genuinely starts (and is
    // counted), the way a real async store would. An immediately-ready store
    // hides the over-fetch, because the awaited fetch completes before the
    // executor ever polls the surplus futures.
    let store = GatedStore {
        inner: memory,
        ..Default::default()
    };
    let reader: Reader<_> = Reader::new(&store);

    // A range over the single leading byte 0x00: its subtree is in range; every
    // sibling at 0x01.. is at or past the exclusive bound. Read-ahead must stop
    // at the bound, so only the root and that one subtree are ever fetched.
    let lo = Key::from(&[0u8][..]);
    let hi = Key::from(&[1u8][..]);
    let got: Rows = {
        let mut cursor = run(reader.range(&root, &lo, &hi))?;
        let mut out = Vec::new();
        while let Some((key, value)) = run(cursor.next())? {
            out.push((key.as_bytes().to_vec(), value));
        }
        out
    };
    let expected: Rows = oracle
        .iter()
        .filter(|(k, _)| k.as_slice() >= lo.as_bytes() && k.as_slice() < hi.as_bytes())
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    ensure!(!expected.is_empty(), "the range covers the first subtree");
    ensure!(
        got == expected,
        "bounded read-ahead matches the range oracle"
    );

    // Exactly the root and the one in-range subtree: no sibling past the bound is
    // prefetched, so the window respects the range at the serial fetch count.
    ensure!(
        store.gets.load(Ordering::Relaxed) == 2,
        "root and the one in-range subtree only",
    );
    Ok(())
}

#[test]
fn floor_matches_the_oracle() -> Result<()> {
    let store = MemoryStore::default();
    let (root, oracle) = build(&store)?;
    let reader: Reader<_> = Reader::new(&store);

    for target in [
        &b""[..],
        &b"a"[..],
        &b"dir05/file0015.txt"[..],
        &b"dir05/file0015.tyt"[..],
        &b"dir05/file00155"[..],
        &b"pre"[..],
        &b"prefix"[..],
        &b"prefiy"[..],
        &b"zzzzz"[..],
    ] {
        let got =
            run(reader.floor(&root, &Key::from(target)))?.map(|(k, v)| (k.as_bytes().to_vec(), v));
        let expected = oracle
            .range(..=target.to_vec())
            .next_back()
            .map(|(k, v)| (k.clone(), v.clone()));
        ensure!(got == expected, "floor matches the oracle");
    }
    Ok(())
}
