//! Memory-bound conformance for the mantaray 1.0 builder and reader.
//!
//! The builder assembles bottom-up over an explicit stack of open nodes, so its
//! peak retained node buffer count is the trie depth, never the key count; the
//! reader follows one fork per node, so a lookup retains one node per level.
//! These tests witness the O(depth) bound directly at a >= 10^6-key scale and
//! pin the single-chunk-node invariant: no node the builder emits exceeds one
//! chunk body.

use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use futures::executor::block_on;
use nectar_manifest::{
    ApplyError, BuildStats, Builder, Changeset, Entry, Key, KeyId, Metadata, Reader, V1, apply,
};
use nectar_primitives::store::{ChunkGet, MemoryStore};
use nectar_primitives::{
    Chunk, ChunkAddress, ChunkOps, ChunkRef, DEFAULT_BODY_SIZE, StandardChunkSet, Verified,
};
use proptest::prelude::*;

type TestResult = Result<(), Box<dyn Error>>;

/// A fallible assertion.
fn ensure(cond: bool, what: String) -> TestResult {
    if cond { Ok(()) } else { Err(what.into()) }
}

/// A reference-valued entry keyed on one byte.
fn entry(byte: u8) -> Entry<V1> {
    Entry::from(ChunkRef::new(ChunkAddress::new([byte; 32])))
}

/// A trusted store that counts every `get`, so a test reads off how many nodes a
/// lookup fetched.
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

/// Stream every `radix`-ary key of the given byte length into the builder: the
/// full odometer over the digit alphabet `0..radix`, `radix.pow(len)` distinct
/// keys of a uniform trie depth `len`, generated one at a time so the test's own
/// working set stays bounded.
fn fill_radix(builder: &mut Builder<V1>, radix: u8, remaining: u32, key: &mut Vec<u8>, fill: u8) {
    if remaining == 0 {
        builder.insert(Key::from(key.clone()), entry(fill), None);
        return;
    }
    for digit in 0..radix {
        key.push(digit);
        fill_radix(builder, radix, remaining.saturating_sub(1), key, fill);
        key.pop();
    }
}

/// Build every `radix`-ary key of length `len` into `store`, returning the build
/// stats.
fn build_radix(store: &MemoryStore, radix: u8, len: u32) -> Result<BuildStats, Box<dyn Error>> {
    let mut builder = Builder::<V1>::new();
    fill_radix(&mut builder, radix, len, &mut Vec::new(), 0x11);
    let built = block_on(builder.build(store)).map_err(|e| e.to_string())?;
    Ok(*built.stats())
}

/// Assert every stored node fits one chunk body: the single-chunk-node
/// invariant, checked over the whole emitted set.
fn assert_single_chunk_nodes(store: &MemoryStore) -> TestResult {
    for chunk in store.clone().into_chunks().into_values() {
        let len = chunk.envelope().data().len();
        ensure(
            len <= DEFAULT_BODY_SIZE,
            format!("emitted node is {len} bytes, over one chunk body"),
        )?;
    }
    Ok(())
}

#[test]
fn peak_open_nodes_tracks_depth_not_width() -> TestResult {
    // radix 10, depth 2 is 100 keys two nodes deep.
    let shallow_narrow = build_radix(&MemoryStore::default(), 10, 2)?;
    // radix 60, depth 2 is 3_600 keys, still two deep but far wider per level.
    let shallow_wide = build_radix(&MemoryStore::default(), 60, 2)?;
    // radix 10, depth 4 is 10_000 keys four nodes deep.
    let deep = build_radix(&MemoryStore::default(), 10, 4)?;

    // Same depth, very different width: identical peak, whatever the fan.
    ensure(
        shallow_narrow.peak_open_nodes() == shallow_wide.peak_open_nodes(),
        format!(
            "peak {} != {} across widths at equal depth",
            shallow_narrow.peak_open_nodes(),
            shallow_wide.peak_open_nodes(),
        ),
    )?;
    // Greater depth raises the peak; width alone never does.
    ensure(
        deep.peak_open_nodes() > shallow_wide.peak_open_nodes(),
        format!(
            "deeper build peak {} did not exceed shallow peak {}",
            deep.peak_open_nodes(),
            shallow_wide.peak_open_nodes(),
        ),
    )?;
    // The wide build writes far more nodes than its peak: work is O(tree), the
    // retained frontier is O(depth).
    ensure(
        shallow_wide.nodes_written() > shallow_wide.peak_open_nodes().saturating_mul(10),
        "wide build node count is not far above its peak".to_owned(),
    )
}

#[test]
fn a_million_key_manifest_is_depth_bounded() -> TestResult {
    // radix 100, depth 3 is exactly 1_000_000 keys, three nodes deep. One build
    // witnesses both bounds: the builder's peak retained buffer count is the
    // depth, independent of the key count, and a lookup fetches one node per
    // level rather than the wide sibling frontier.
    let inner = MemoryStore::default();
    let mut builder = Builder::<V1>::new();
    fill_radix(&mut builder, 100, 3, &mut Vec::new(), 0x22);
    let built = block_on(builder.build(&inner)).map_err(|e| e.to_string())?;
    let stats = *built.stats();
    let root = *built.root();

    ensure(
        stats.peak_open_nodes() <= 8,
        format!(
            "peak {} open nodes is not O(depth) at 10^6 keys",
            stats.peak_open_nodes(),
        ),
    )?;
    // The tree is spilled across many chunks, orders of magnitude above the
    // peak, so the bound is a genuine streaming bound, not a small tree.
    ensure(
        stats.nodes_written() > stats.peak_open_nodes().saturating_mul(100),
        format!(
            "only {} nodes written, no wider than the peak",
            stats.nodes_written(),
        ),
    )?;
    // Every emitted node fits one chunk body, checked at 10^6-key scale.
    assert_single_chunk_nodes(&inner)?;

    // Read a spread of keys through a counting store: each lookup is O(depth).
    let store = CountingStore {
        inner,
        gets: AtomicUsize::new(0),
    };
    let reader: Reader<_> = Reader::new(&store);
    for probe in [[0u8, 0, 0], [50, 50, 50], [99, 99, 99], [7, 63, 12]] {
        store.gets.store(0, Ordering::Relaxed);
        let value =
            block_on(reader.get(&root, &Key::from(&probe[..]))).map_err(|e| e.to_string())?;
        ensure(value == Some(entry(0x22)), format!("missing key {probe:?}"))?;
        // Depth three: at most the root and one node per level.
        ensure(
            store.gets() <= 4,
            format!("lookup fetched {} nodes, not O(depth)", store.gets()),
        )?;
    }
    Ok(())
}

/// Chunk payload lengths of a store, for the budget assertions below.
fn chunk_lengths(store: &MemoryStore) -> Vec<usize> {
    store
        .clone()
        .into_chunks()
        .into_values()
        .map(|chunk| chunk.envelope().data().len())
        .collect()
}

/// A key-value set whose keys spread across many first bytes (wide) and whose
/// values carry large metadata blocks (heavy), so a naive single-chunk node
/// would overrun the body budget.
fn wide_heavy_set() -> impl Strategy<Value = Vec<(Vec<u8>, u8, Option<usize>)>> {
    let key = prop::collection::vec(any::<u8>(), 1..6);
    let meta_len = prop::option::of(0usize..1000);
    prop::collection::vec((key, any::<u8>(), meta_len), 0..400)
}

/// A metadata block of `len` filler bytes within a proptest body.
fn filler_meta(len: usize) -> Result<Metadata<V1>, TestCaseError> {
    Metadata::new(KeyId::ContentType, Bytes::from(vec![b'a'; len]))
        .map_err(|e| TestCaseError::fail(e.to_string()))
}

proptest! {
    // No node the builder emits ever exceeds one chunk body. A key set too wide
    // or heavy to fit surfaces a typed error, never a panic and never an
    // over-budget chunk written to the store.
    #[test]
    fn no_built_node_exceeds_budget(entries in wide_heavy_set()) {
        let store = MemoryStore::default();
        let mut builder = Builder::<V1>::new();
        for (key, fill, meta) in &entries {
            let metadata = match meta {
                Some(len) => Some(filler_meta(*len)?),
                None => None,
            };
            builder.insert(Key::from(key.clone()), entry(*fill), metadata);
        }
        if block_on(builder.build(&store)).is_ok() {
            for len in chunk_lengths(&store) {
                prop_assert!(len <= DEFAULT_BODY_SIZE, "built node over one chunk body");
            }
        }
    }

    // apply upholds the same invariant: folding a fuzzed changeset into a base
    // never writes an over-budget node.
    #[test]
    fn apply_preserves_the_single_chunk_invariant(
        base in wide_heavy_set(),
        delta in wide_heavy_set(),
    ) {
        let store = MemoryStore::default();
        let mut builder = Builder::<V1>::new();
        for (key, fill, _) in &base {
            builder.insert(Key::from(key.clone()), entry(*fill), None);
        }
        let Ok(built) = block_on(builder.build(&store)) else {
            return Ok(());
        };
        let mut changeset = Changeset::<V1>::new();
        for (key, fill, _) in &delta {
            changeset.put(Key::from(key.clone()), entry(*fill), None);
        }
        match block_on(apply(&store, built.root(), &changeset)) {
            Ok(_) => {
                for len in chunk_lengths(&store) {
                    prop_assert!(len <= DEFAULT_BODY_SIZE, "applied node over one chunk body");
                }
            }
            // A wide/heavy merge that cannot fit a node reports a typed error.
            Err(ApplyError::Build(_) | ApplyError::Store(_)) => {}
            Err(other) => prop_assert!(false, "unexpected apply error: {:?}", other),
        }
    }
}
