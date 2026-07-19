//! Fuzz the walk's rejection of malformed intermediate chunks.
//!
//! The fuzzer authors content-addressed chunks whose spans and bodies need
//! not obey the tree grammar, plus one synthesized intermediate referencing
//! them, and the file is opened at each. The walk must reject every
//! contradiction typed: no panic, no hang, and an `Ok` collect delivers
//! exactly the declared span.

#![no_main]

use libfuzzer_sys::fuzz_target;
use nectar_file::sync::drive;
use nectar_file::{File, Plain};
use nectar_primitives::chunk::{AnyChunkSet, Chunk, ChunkAddress, ContentChunk, Verified};
use nectar_primitives::store::MemoryStore;

/// Tiny body size: fan-out 8, so shallow inputs reach deep shape checks.
const BODY: usize = 256;
/// Synthetic-chunk cap per exec.
const MAX_CHUNKS: usize = 16;
/// Roots probed per exec.
const MAX_PROBES: usize = 4;
/// Collect bound; larger declared spans must fail typed, not allocate.
const COLLECT_BOUND: u64 = 1 << 20;

/// A content chunk from an arbitrary span and body, wire-decoded so the
/// address is honestly derived from the (possibly nonsensical) content.
fn synth(span: u64, payload: &[u8]) -> Option<ContentChunk<BODY>> {
    let payload = payload.get(..payload.len().min(BODY))?;
    let mut wire = Vec::with_capacity(8 + payload.len());
    wire.extend_from_slice(&span.to_le_bytes());
    wire.extend_from_slice(payload);
    ContentChunk::try_from(wire.as_slice()).ok()
}

/// Open and drain the tree at `root`; malformed trees may only fail typed.
fn probe(store: &MemoryStore<AnyChunkSet<BODY>>, root: ChunkAddress) {
    let store = store.clone();
    drive(async move {
        let Ok(file) = File::<_, Plain, BODY>::open(store, root).await else {
            return;
        };
        let span = file.len();
        if let Ok(bytes) = file.collect(COLLECT_BOUND).await {
            assert_eq!(
                bytes.len() as u64,
                span,
                "an accepted tree must deliver exactly its declared span"
            );
        }
        // A mid-file reader probe walks the same tree down a partial range.
        let mut reader = file.read().range(span / 2..span).build();
        let mut buf = [0u8; 64];
        while let Ok(n) = reader.read(&mut buf).await {
            if n == 0 {
                break;
            }
        }
    })
    .expect("a ready store never pends");
}

fuzz_target!(|input: (Vec<(u64, Vec<u8>)>, u64, u8)| {
    let (specs, root_span, arity) = input;

    let mut chunks: Vec<Chunk<Verified, AnyChunkSet<BODY>>> = Vec::new();
    for (span, payload) in specs.iter().take(MAX_CHUNKS) {
        if let Some(chunk) = synth(*span, payload) {
            chunks.push(chunk.seal());
        }
    }
    let addresses: Vec<ChunkAddress> = chunks.iter().map(|chunk| *chunk.address()).collect();
    let store = MemoryStore::from_chunks(chunks);

    // One synthesized intermediate: real child addresses under a fuzzed
    // span, so shape contradictions surface below the root.
    if !addresses.is_empty() {
        let children = usize::from(arity) % (BODY / 32) + 1;
        let mut body = Vec::with_capacity(children * 32);
        for address in addresses.iter().cycle().take(children) {
            body.extend_from_slice(address.as_bytes());
        }
        if let Some(root) = synth(root_span, &body) {
            let sealed: Chunk<Verified, AnyChunkSet<BODY>> = root.seal();
            let root_address = *sealed.address();
            let store =
                MemoryStore::from_chunks(store.clone().into_chunks().into_values().chain([sealed]));
            probe(&store, root_address);
        }
    }

    for address in addresses.iter().take(MAX_PROBES) {
        probe(&store, *address);
    }

    // An absent root must fail the open, not the process.
    probe(&store, ChunkAddress::new([0x5a; 32]));
});
