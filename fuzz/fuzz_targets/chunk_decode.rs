//! Fuzz the chunk wire decoders with raw attacker-controlled bytes.
//!
//! `StandardChunkSet::deserialize` is the polymorphic entry point through
//! which untrusted chunk payloads from the network are parsed (it tries
//! `ContentChunk`, then `SingleOwnerChunk`). The direct `TryFrom<&[u8]>`
//! decoders are also driven so the SOC path is reached even when the CAC
//! decoder accepts the input first. For every successfully decoded chunk the
//! lazy address computation is forced, and for SOCs the owner recovery —
//! ECDSA public-key recovery over the id/signature header (bytes 32..97) —
//! which must be panic-free on arbitrary input. Any returned `Err` is
//! success; the oracle is "no panic, no OOM, no hang".
//!
//! Seeds live in `fuzz/seeds/chunk_decode/` and are replayed on stable by
//! `seed_replay_chunk_decode` in
//! `crates/primitives/src/chunk/chunk_type_set.rs`.

#![no_main]

use libfuzzer_sys::fuzz_target;
use nectar_primitives::{
    Chunk, ChunkTypeSet, ContentChunk, DEFAULT_BODY_SIZE, SingleOwnerChunk, StandardChunkSet,
};

fuzz_target!(|data: &[u8]| {
    // Polymorphic wire decoder (CAC first, then SOC).
    if let Ok(chunk) = <StandardChunkSet as ChunkTypeSet<DEFAULT_BODY_SIZE>>::deserialize(data) {
        // Force the lazy address computation (BMT hash / keccak(id || owner)).
        let _ = chunk.address();
    }

    // Direct decoders, so the SOC branch is exercised even for inputs the
    // CAC decoder accepts.
    let _ = ContentChunk::<DEFAULT_BODY_SIZE>::try_from(data);
    if let Ok(soc) = SingleOwnerChunk::<DEFAULT_BODY_SIZE>::try_from(data) {
        let _ = soc.owner();
        let _ = soc.address();
    }
});
