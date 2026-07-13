//! The streaming reader through the public API: descent follows one fork per
//! node, so a lookup down a wide manifest fetches O(depth) nodes and never a
//! whole level. A counting store witnesses the bound directly.

use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::executor::block_on;
use nectar_manifest::{Child, Entry, ForkTable, Key, Node, NodePut, Prefix, Reader, V1};
use nectar_primitives::store::{ChunkGet, MemoryStore};
use nectar_primitives::{Chunk, ChunkAddress, ChunkRef, StandardChunkSet, Verified};

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
/// nodes a lookup fetched.
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

fn entry(byte: u8) -> Entry {
    ChunkRef::new(ChunkAddress::new([byte; 32])).into()
}

/// Build a deliberately wide two-level manifest: a root whose fork table holds
/// one referenced leaf per first byte, each leaf terminating a single key. The
/// second level is `width` chunks wide, but any one key sits two nodes deep.
fn wide_manifest(store: &MemoryStore, width: u16) -> Result<ChunkAddress, Box<dyn Error>> {
    let mut forks = ForkTable::<V1>::new();
    for first in 0..width {
        let first = u8::try_from(first)?;
        let mut leaf = ForkTable::new();
        leaf.insert(Prefix::try_from(&[0xFFu8][..])?, entry(first).into(), None)?;
        let leaf_ref = block_on(store.put_node(&Node::new(None, leaf)))?;
        forks.insert(
            Prefix::try_from(&[first][..])?,
            Child::Ref32(ChunkRef::new(leaf_ref)).into(),
            None,
        )?;
    }
    Ok(block_on(store.put_node(&Node::new(None, forks)))?)
}

#[test]
fn a_lookup_fetches_depth_nodes_not_the_wide_level() -> TestResult {
    // Wide enough to dwarf the depth-2 path, yet inside one root chunk (a
    // radix-full root spills to a directory, which is a later car's concern).
    let width = 100u16;
    let memory = MemoryStore::default();
    let root = wide_manifest(&memory, width)?;

    // The whole manifest is one root plus `width` leaves.
    ensure_eq(memory.len(), usize::from(width) + 1, "stored node count")?;

    let store = CountingStore {
        inner: memory,
        gets: AtomicUsize::new(0),
    };
    let reader: Reader<_> = Reader::new(&store);

    // Look up one key: first byte 0x2A, then the leaf's 0xFF fork.
    let key = Key::from(&[0x2Au8, 0xFF][..]);
    let value = block_on(reader.get(&root, &key))?;
    ensure_eq(value, Some(entry(0x2A)), "looked-up value")?;

    // Two hops: the root and the single leaf on the path. Never the wide
    // sibling level, so fetches track depth, not width.
    ensure_eq(store.gets(), 2, "fetches equal path depth")?;
    ensure(
        store.gets() < usize::from(width),
        "fetches below level width",
    )?;

    // A second lookup on a different branch is again two hops: the frontier is
    // never widened, each key pays only its own path.
    store.gets.store(0, Ordering::Relaxed);
    let value = block_on(reader.get(&root, &Key::from(&[0x50u8, 0xFF][..])))?;
    ensure_eq(value, Some(entry(0x50)), "second value")?;
    ensure_eq(store.gets(), 2, "second lookup is also depth-bounded")
}

#[test]
fn an_absent_key_stops_at_the_first_unmatched_fork() -> TestResult {
    let width = 64u16;
    let memory = MemoryStore::default();
    let root = wide_manifest(&memory, width)?;

    let store = CountingStore {
        inner: memory,
        gets: AtomicUsize::new(0),
    };
    let reader: Reader<_> = Reader::new(&store);

    // A first byte no root fork carries: the walk stops at the root without
    // fetching any leaf.
    let value = block_on(reader.get(&root, &Key::from(&[0xFFu8, 0x00][..])))?;
    ensure_eq(value, None, "absent value")?;
    ensure_eq(store.gets(), 1, "only the root is fetched")
}
