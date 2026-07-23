//! Fuzz the mantaray node wire decoder with raw attacker-controlled bytes.
//!
//! The node wire decoder is the entry point through which untrusted manifest
//! chunks from the network are parsed. The shared
//! `nectar_mantaray::oracles::node_decode` oracle exercises both entry
//! widths on every input: `ChunkRef` (plain manifests, 32-byte entries) and
//! `EncryptedChunkRef` (encrypted manifests, 64-byte entries), each of which
//! drives its own reference-width cursor reads through the shared body
//! decoder. The decoder is the structure recoverer, so the target takes raw
//! bytes; any returned `Err` is success. The oracle is "no panic, no OOM,
//! no hang".
//!
//! Seeds live in `fuzz/seeds/mantaray_node_decode/` and are replayed on
//! stable by `seed_replay_mantaray_node_decode` in
//! `crates/mantaray/src/codec.rs`, through the same oracle.

#![no_main]

use libfuzzer_sys::fuzz_target;
use nectar_mantaray::oracles;

fuzz_target!(|data: &[u8]| {
    let _ = oracles::node_decode(data);
});
