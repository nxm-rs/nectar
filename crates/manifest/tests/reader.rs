//! The streaming reader through the public API: descent follows one fork per
//! node, so a lookup down a wide manifest fetches O(depth) nodes and never a
//! whole level. A counting store witnesses the bound directly.

use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use futures::executor::block_on;
use nectar_manifest::{Builder, Child, Entry, ForkTable, Key, Node, NodePut, Prefix, Reader, V1};
use nectar_primitives::store::{ChunkGet, MemoryStore};
use nectar_primitives::{Chunk, ChunkAddress, ChunkRef, StandardChunkSet, Verified};

mod common;
use common::{TestResult, ensure, ensure_eq};

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
fn every_builder_key_reads_back_through_referenced_hops() -> TestResult {
    // Build a real manifest through the builder so the reader is exercised
    // against the wire structure it exists to read, not a hand-laid table: the
    // shared-prefix grid forces compacted edges, embedded subtrees and, once a
    // subtree outgrows the inline bound, referenced children the reader must
    // fetch and descend.
    let memory = MemoryStore::default();
    let mut builder = Builder::new();
    let mut expected: Vec<(String, Entry)> = Vec::new();
    for dir in 0u8..16 {
        for file in 0u8..40 {
            let key = format!("dir{dir:02}/file{file:04}.txt");
            let value = entry((dir ^ file).wrapping_add(1));
            builder.insert(Key::from(key.clone().into_bytes()), value.clone(), None);
            expected.push((key, value));
        }
    }
    // A key that is a strict prefix of another exercises a fork carrying both a
    // terminal value and a continuation.
    builder.insert(Key::from(&b"pre"[..]), entry(0x11), None);
    expected.push(("pre".to_owned(), entry(0x11)));
    builder.insert(Key::from(&b"prefix"[..]), entry(0x22), None);
    expected.push(("prefix".to_owned(), entry(0x22)));
    // An inline value must read back whole, not as a reference.
    let inline = Entry::inline(Bytes::from_static(b"hello")).map_err(|e| e.to_string())?;
    builder.insert(Key::from(&b"inline.txt"[..]), inline.clone(), None);
    expected.push(("inline.txt".to_owned(), inline));
    // The empty key sets the manifest's own value in the root extension.
    builder.insert(Key::empty(), entry(0x99), None);

    let built = block_on(builder.build(&memory)).map_err(|e| e.to_string())?;
    let root = *built.root();
    // More than one node spilled: the builder emitted referenced children, so
    // reading keys beneath them genuinely drives the fetch-and-descend path.
    ensure(
        built.stats().nodes_written() > 1,
        "builder spilled referenced children",
    )?;

    let store = CountingStore {
        inner: memory,
        gets: AtomicUsize::new(0),
    };
    let reader: Reader<_> = Reader::new(&store);

    // The root extension answers the empty key.
    ensure_eq(
        block_on(reader.get(&root, &Key::empty())).map_err(|e| e.to_string())?,
        Some(entry(0x99)),
        "empty key reads the root value",
    )?;

    // Every key reads back its exact value, and no single lookup ever fetches a
    // whole level: the deepest path stays a small multiple of the tree depth,
    // far below the spilled node count.
    let mut deepest = 0usize;
    for (key, value) in &expected {
        store.gets.store(0, Ordering::Relaxed);
        let got = block_on(reader.get(&root, &Key::from(key.clone().into_bytes())))
            .map_err(|e| e.to_string())?;
        ensure_eq(got.as_ref(), Some(value), key)?;
        deepest = deepest.max(store.gets());
    }
    ensure(deepest >= 2, "at least one key descends a referenced hop")?;
    ensure(
        deepest < built.stats().nodes_written(),
        "no lookup fetches the whole tree",
    )?;

    // Keys that diverge from, fall short of, or overrun a stored key are absent.
    for absent in [
        "nope",
        "dir99/file0000.txt",
        "dir00/file9999.txt",
        "pr",
        "prefixed",
    ] {
        ensure_eq(
            block_on(reader.get(&root, &Key::from(absent.as_bytes())))
                .map_err(|e| e.to_string())?,
            None,
            absent,
        )?;
    }
    Ok(())
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
