//! Byte pin of the streaming build bridge against a direct split.
//!
//! For a battery of boundary sizes, `build_files` must publish the same
//! manifest root as a build over a direct whole-buffer split's references,
//! and the stored chunk address set must match exactly. Chunks are
//! content-addressed, so address equality pins the stored bytes.

use std::error::Error;

use bytes::Bytes;
use futures::executor::block_on;
use nectar_file::{Plain, PutWindow, Split};
use nectar_manifest::{Builder, Entry, Key, Reader, build_files};
use nectar_primitives::{ChunkAddress, ChunkRef, DEFAULT_BODY_SIZE, MemoryStore};

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

/// Non-uniform bytes so leaf boundaries cut through varying content. The
/// 251-byte cycle is coprime with the body size, so no two leaves repeat.
fn pattern(size: usize) -> Bytes {
    let cycle = (0u16..251).map(|byte| u8::try_from(byte).unwrap_or_default());
    Bytes::from(cycle.cycle().take(size).collect::<Vec<u8>>())
}

/// Split `data` whole through the streaming engine into a fresh store,
/// returning the root and the split store.
async fn split_whole(data: &[u8]) -> Result<(ChunkAddress, MemoryStore), Box<dyn Error>> {
    let store = std::sync::Arc::new(MemoryStore::default());
    let mut split: Split<std::sync::Arc<MemoryStore>, Plain, DEFAULT_BODY_SIZE> =
        Split::new(std::sync::Arc::clone(&store), PutWindow::DEFAULT);
    let mut rest = data;
    while !rest.is_empty() {
        let taken = core::future::poll_fn(|cx| split.poll_write(cx, rest)).await?;
        rest = rest
            .split_at_checked(taken)
            .map(|(_, tail)| tail)
            .ok_or("write consumed past the buffer")?;
    }
    let root = core::future::poll_fn(|cx| split.poll_finish(cx)).await?;
    drop(split);
    let store = std::sync::Arc::into_inner(store).ok_or("split still holds the store")?;
    Ok((root, store))
}

#[test]
fn streaming_bridge_pins_the_direct_split_bytes() -> TestResult {
    for &size in SIZES {
        let data = pattern(size);
        let key = Key::from(&b"file"[..]);

        let store = MemoryStore::default();
        let built = block_on(build_files(&store, [(key.clone(), data.clone())]))?;

        // The reference bridge: the same manifest over a direct split's
        // reference, with the direct chunk set as the file bytes oracle.
        let (direct_root, direct_store) = block_on(split_whole(&data))?;
        let node_store = MemoryStore::default();
        let mut builder: Builder = Builder::new();
        builder.insert(key, Entry::from(ChunkRef::new(direct_root)), None);
        let direct_built = block_on(builder.build(&node_store))?;
        ensure_eq(built.root(), direct_built.root(), "manifest root")?;

        let direct = direct_store.into_chunks();
        let nodes = node_store.into_chunks();
        for address in direct.keys() {
            ensure(store.get(address).is_some(), "direct-split chunk stored")?;
        }
        // Exact set equality: with the file chunks and manifest nodes both
        // pinned, no other chunk may appear.
        for address in store.into_chunks().keys() {
            ensure(
                direct.contains_key(address) || nodes.contains_key(address),
                "no chunk beyond the direct split set and the manifest nodes",
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

    let reader: Reader<_> = Reader::new(store);
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
