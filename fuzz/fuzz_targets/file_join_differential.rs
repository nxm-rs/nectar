//! Fuzz the streaming read facade against the legacy joiner.
//!
//! One store is built by the legacy splitter over fuzzed bytes; the oracle
//! is byte equality between the legacy join, the streaming collect, and the
//! source bytes. A small body size keeps multi-level trees reachable from
//! short fuzz inputs.

#![no_main]

use futures::executor::block_on;
use libfuzzer_sys::fuzz_target;
use nectar_file::{File, Plain};
use nectar_primitives::chunk::{AnyChunkSet, ChunkAddress};
use nectar_primitives::store::MemoryStore;

/// Tiny body size: fan-out 8, so a few KiB already builds a deep tree.
const BODY: usize = 256;
/// Source-length cap; four tree levels at the tiny body size.
const MAX_LEN: usize = 32 * 1024;

/// Tile `seed` to `copies` repetitions, capped at [`MAX_LEN`] bytes.
fn tile(seed: &[u8], copies: u16) -> Vec<u8> {
    if seed.is_empty() {
        return Vec::new();
    }
    let len = seed
        .len()
        .saturating_mul(usize::from(copies.max(1)))
        .min(MAX_LEN);
    seed.iter().copied().cycle().take(len).collect()
}

/// Legacy split of the whole buffer: the store both joiners read from.
#[allow(deprecated)]
fn legacy_split(data: &[u8]) -> (ChunkAddress, MemoryStore<AnyChunkSet<BODY>>) {
    nectar_primitives::file::split::<BODY>(data).expect("legacy split accepts any buffer")
}

/// Legacy join over the shared store: the byte oracle.
#[allow(deprecated)]
fn legacy_join(store: &MemoryStore<AnyChunkSet<BODY>>, root: ChunkAddress) -> Vec<u8> {
    block_on(nectar_primitives::file::join::<_, _, BODY>(store, root))
        .expect("legacy join reads back its own split")
}

fuzz_target!(|input: (Vec<u8>, u16)| {
    let (seed, copies) = input;
    let data = tile(&seed, copies);

    let (root, store) = legacy_split(&data);
    let legacy = legacy_join(&store, root);

    let streamed = block_on(async {
        let file = File::<_, Plain, BODY>::open(store, root)
            .await
            .expect("open must succeed over a complete store");
        file.collect(u64::MAX)
            .await
            .expect("collect must succeed over a complete store")
    });

    assert_eq!(
        streamed, legacy,
        "streaming join diverged from the legacy joiner"
    );
    assert_eq!(streamed, data, "join output diverged from the source bytes");
});
