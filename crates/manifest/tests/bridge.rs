//! Byte pin of the streaming build bridge against the legacy split path.
//!
//! For a battery of boundary sizes, `build_files` must publish the same
//! manifest root as a build over the legacy whole-buffer splitter's
//! references, and the stored chunk address set must match exactly. Chunks
//! are content-addressed, so address equality pins the stored bytes.

// Bench, example, and integration-test code: unwraps, direct indexing,
// casts, and assertions are setup and illustration, not shipped surface.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::panic,
    clippy::panic_in_result_fn,
    clippy::as_conversions,
    clippy::missing_panics_doc
)]
// The legacy whole-buffer splitter is the differential oracle here.
#![allow(deprecated)]

use std::error::Error;

use bytes::Bytes;
use futures::executor::block_on;
use nectar_manifest::{Builder, Entry, Key, Reader, build_files};
use nectar_primitives::{ChunkRef, DEFAULT_BODY_SIZE, MemoryStore, split};

type TestResult = Result<(), Box<dyn Error>>;

/// A fallible assertion: Result-returning tests report failures as errors.
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

const B: usize = DEFAULT_BODY_SIZE;
/// Reference fan-out of one intermediate chunk at the default body size.
const FAN: usize = B / 32;

/// Boundary sizes: empty, single byte, the leaf edges, one full intermediate
/// level and its neighbours, and a two-level interior point.
const SIZES: &[usize] = &[
    0,
    1,
    B - 1,
    B,
    B + 1,
    2 * B + 37,
    FAN * B - 1,
    FAN * B,
    FAN * B + 1,
    2 * FAN * B + 3,
];

/// Non-uniform bytes so leaf boundaries cut through varying content.
fn pattern(size: usize) -> Bytes {
    Bytes::from((0..size).map(|i| (i % 251) as u8).collect::<Vec<u8>>())
}

#[test]
fn streaming_bridge_pins_the_legacy_split_bytes() -> TestResult {
    for &size in SIZES {
        let data = pattern(size);
        let key = Key::from(&b"file"[..]);

        let store = MemoryStore::default();
        let built = block_on(build_files(&store, [(key.clone(), data.clone())]))?;

        // The prior bridge: the same manifest over the legacy splitter's
        // reference, with the legacy chunk set as the file bytes oracle.
        let (legacy_root, legacy_chunks) = split::<DEFAULT_BODY_SIZE>(&data)?;
        let node_store = MemoryStore::default();
        let mut builder: Builder = Builder::new();
        builder.insert(key, Entry::from(ChunkRef::new(legacy_root)), None);
        let legacy_built = block_on(builder.build(&node_store))?;
        ensure_eq(built.root(), legacy_built.root(), "manifest root")?;

        let legacy = legacy_chunks.into_chunks();
        let nodes = node_store.into_chunks();
        for address in legacy.keys() {
            ensure(store.get(address).is_some(), "legacy chunk stored")?;
        }
        // Exact set equality: with the file chunks and manifest nodes both
        // pinned, no other chunk may appear.
        for address in store.into_chunks().keys() {
            ensure(
                legacy.contains_key(address) || nodes.contains_key(address),
                "no chunk beyond the legacy set and the manifest nodes",
            )?;
        }
    }
    Ok(())
}

#[test]
fn bridged_files_round_trip_byte_exact() -> TestResult {
    let store = MemoryStore::default();
    let big = pattern(FAN * B + 5);
    let files = [
        (Key::from(&b"a/big"[..]), big.clone()),
        (Key::from(&b"a/small"[..]), Bytes::from_static(b"x")),
    ];
    let root = *block_on(build_files(&store, files))?.root();

    let reader: Reader<_> = Reader::new(&store);
    ensure_eq(
        block_on(reader.fetch(&root, &Key::from(&b"a/big"[..])))?,
        Some(big),
        "deep file round trip",
    )?;
    ensure_eq(
        block_on(reader.fetch(&root, &Key::from(&b"a/small"[..])))?,
        Some(Bytes::from_static(b"x")),
        "single-leaf round trip",
    )
}
