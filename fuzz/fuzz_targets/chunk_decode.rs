//! Fuzz the chunk wire decoders with raw attacker-controlled bytes.
//!
//! The shared `nectar_primitives::oracles::chunk_decode` oracle drives
//! `AnyChunk::from_wire_bytes`, the polymorphic entry point through which
//! untrusted chunk payloads from the network are parsed: once with the zero
//! address, exercising the mismatch arm on arbitrary input, and once keyed
//! by the address recovered from a direct parse, exercising the `Ok` arm on
//! structurally valid input. The direct `TryFrom<&[u8]>` decoders are also
//! driven so the SOC path is reached even when the CAC decoder accepts the
//! input first, and every successful decode forces the lazy address
//! computation and, for SOCs, the owner recovery (ECDSA public-key recovery
//! over the id/signature header, bytes 32..97), which must be panic-free on
//! arbitrary input. Any returned `Err` is success; the oracle is "no panic,
//! no OOM, no hang".
//!
//! Seeds live in `fuzz/seeds/chunk_decode/` and are replayed on stable by
//! `seed_replay_chunk_decode` in `crates/primitives/src/chunk/registry.rs`,
//! through the same oracle.

#![no_main]

use libfuzzer_sys::fuzz_target;
use nectar_primitives::{DEFAULT_BODY_SIZE, oracles};

fuzz_target!(|data: &[u8]| {
    let _ = oracles::chunk_decode::<DEFAULT_BODY_SIZE>(data);
});
