//! Ordered read operations through the public API: a counting store witnesses
//! that iteration fetches trie nodes on the frontier only, never a value chunk,
//! and a std map oracles the key order of iteration, ranges, prefixes and floor.

use std::collections::BTreeMap;
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use futures::executor::block_on;
use nectar_manifest::{Builder, Cursor, Entry, Key, Reader};
use nectar_primitives::store::{ChunkGet, MemoryStore};
use nectar_primitives::{Chunk, ChunkAddress, StandardChunkSet, Verified};

type TestResult = Result<(), Box<dyn Error>>;

/// A fallible assertion.
fn ensure(cond: bool, what: &str) -> TestResult {
    if cond { Ok(()) } else { Err(what.into()) }
}

/// A fallible equality assertion.
fn ensure_eq<T: PartialEq + core::fmt::Debug>(left: T, right: T, what: &str) -> TestResult {
    if left == right {
        Ok(())
    } else {
        Err(format!("{what}: {left:?} != {right:?}").into())
    }
}

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
fn build(store: &MemoryStore) -> Result<(ChunkAddress, Oracle), Box<dyn Error>> {
    let mut builder = Builder::new();
    let mut oracle = Oracle::new();
    for (key, bytes) in corpus() {
        let value = Entry::inline(Bytes::from(bytes)).map_err(|e| e.to_string())?;
        builder.insert(key.clone(), value.clone(), None);
        oracle.insert(key.as_bytes().to_vec(), value);
    }
    let built = block_on(builder.build(store)).map_err(|e| e.to_string())?;
    Ok((*built.root(), oracle))
}

/// Collect a whole cursor into an ordered vector.
fn collect(mut cursor: Cursor<'_, &CountingStore>) -> Result<Rows, Box<dyn Error>> {
    let mut out = Vec::new();
    while let Some((key, value)) = block_on(cursor.next()).map_err(|e| e.to_string())? {
        out.push((key.as_bytes().to_vec(), value));
    }
    Ok(out)
}

#[test]
fn iteration_fetches_nodes_not_values() -> TestResult {
    let memory = MemoryStore::default();
    let (root, oracle) = build(&memory)?;
    // The builder spilled referenced children, so the walk genuinely crosses
    // fetched hops rather than reading one embedded root.
    let nodes = memory.len();
    ensure(nodes > 1, "manifest spilled referenced children")?;
    // Every value is inline, so the store holds trie nodes only. If iteration
    // fetched values it would fetch chunks that do not exist.
    ensure(oracle.len() > nodes, "more keys than trie nodes")?;

    let store = CountingStore {
        inner: memory,
        gets: AtomicUsize::new(0),
    };
    let reader: Reader<_> = Reader::new(&store);

    let got = collect(block_on(reader.iter(&root)).map_err(|e| e.to_string())?)?;
    let expected: Rows = oracle.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    ensure_eq(got, expected, "iteration matches the ordered oracle")?;

    // The whole walk fetched exactly the trie nodes, once each, and far fewer
    // than the key count: values ride in the fork records, so no leaf is pulled.
    ensure_eq(store.gets(), nodes, "one fetch per trie node")?;
    ensure(store.gets() < oracle.len(), "fetches below the key count")
}

#[test]
fn range_matches_the_oracle() -> TestResult {
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
        let got = collect(
            block_on(reader.range(&root, &Key::from(lo), &Key::from(hi)))
                .map_err(|e| e.to_string())?,
        )?;
        let expected: Rows = oracle
            .range(lo.to_vec()..hi.to_vec())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        ensure_eq(got, expected, "range matches the oracle")?;
    }
    Ok(())
}

#[test]
fn prefix_matches_the_oracle() -> TestResult {
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
            let mut cursor =
                block_on(reader.prefix(&root, &Key::from(p))).map_err(|e| e.to_string())?;
            let mut out = Vec::new();
            while let Some((key, value)) = block_on(cursor.next()).map_err(|e| e.to_string())? {
                out.push((key.as_bytes().to_vec(), value));
            }
            out
        };
        let expected: Rows = oracle
            .iter()
            .filter(|(k, _)| k.starts_with(p))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        ensure_eq(got, expected, "prefix matches the oracle")?;
    }
    Ok(())
}

#[test]
fn floor_matches_the_oracle() -> TestResult {
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
        let got = block_on(reader.floor(&root, &Key::from(target)))
            .map_err(|e| e.to_string())?
            .map(|(k, v)| (k.as_bytes().to_vec(), v));
        let expected = oracle
            .range(..=target.to_vec())
            .next_back()
            .map(|(k, v)| (k.clone(), v.clone()));
        ensure_eq(got, expected, "floor matches the oracle")?;
    }
    Ok(())
}
