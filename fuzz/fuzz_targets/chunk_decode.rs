//! Fuzz the chunk wire decoders with raw attacker-controlled bytes.
//!
//! `AnyChunk::from_wire_bytes` is the polymorphic entry point through which
//! untrusted chunk payloads from the network are parsed (it tries
//! `ContentChunk`, then `SingleOwnerChunk`, keyed by the expected address).
//! It is driven twice: with the zero address, exercising the mismatch arm on
//! arbitrary input, and with the address recovered from a direct parse,
//! exercising the `Ok` arm on structurally valid input. The direct
//! `TryFrom<&[u8]>` decoders are also driven so the SOC path is reached even
//! when the CAC decoder accepts the input first. For every successfully
//! decoded chunk the lazy address computation is forced, and for SOCs the
//! owner recovery (ECDSA public-key recovery over the id/signature header,
//! bytes 32..97), which must be panic-free on arbitrary input. Any returned
//! `Err` is success; the oracle is "no panic, no OOM, no hang".
//!
//! Seeds live in `fuzz/seeds/chunk_decode/` and are replayed on stable by
//! `seed_replay_chunk_decode` in
//! `crates/primitives/src/chunk/chunk_type_set.rs`.

#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use nectar_primitives::{
    AnyChunk, Chunk, ChunkAddress, ContentChunk, DEFAULT_BODY_SIZE, SingleOwnerChunk,
};

fuzz_target!(|data: &[u8]| {
    let bytes = Bytes::copy_from_slice(data);

    // Address-mismatch arm: the zero address matches (almost) no input, so
    // both trial parses and their address computations run to `Err`.
    let _ = AnyChunk::<DEFAULT_BODY_SIZE>::from_wire_bytes(&ChunkAddress::default(), bytes.clone());

    // Direct decoders, so the SOC branch is exercised even for inputs the
    // CAC decoder accepts.
    let content = ContentChunk::<DEFAULT_BODY_SIZE>::try_from(data);
    let soc = SingleOwnerChunk::<DEFAULT_BODY_SIZE>::try_from(data);
    if let Ok(soc) = &soc {
        let _ = soc.owner();
        let _ = soc.address();
    }

    // Ok arm: key the wire decoder by the address of whichever direct parse
    // succeeded, CAC first (the same trial order the decoder uses).
    let address = content
        .ok()
        .map(|c| *c.address())
        .or_else(|| soc.ok().map(|s| *s.address()));
    if let Some(address) = address
        && let Ok(chunk) = AnyChunk::<DEFAULT_BODY_SIZE>::from_wire_bytes(&address, bytes)
    {
        // Force the lazy address computation (BMT hash / keccak(id || owner)).
        let _ = chunk.address();
    }
});
