//! Fuzz the mantaray node wire decoder with raw attacker-controlled bytes.
//!
//! `Node::<ChunkAddress>::try_from(&[u8])` is the entry point through which
//! untrusted manifest chunks from the network are parsed (plain manifests,
//! 32-byte entries). The decoder is the structure recoverer, so the target
//! takes raw bytes; any returned `Err` is success. The oracle is "no panic,
//! no OOM, no hang".
//!
//! Seeds live in `fuzz/seeds/mantaray_node_decode/` and are replayed on
//! stable by `seed_replay_mantaray_node_decode` in
//! `crates/mantaray/src/codec.rs`.

#![no_main]

use libfuzzer_sys::fuzz_target;
use nectar_mantaray::Node;
use nectar_primitives::chunk::ChunkAddress;

fuzz_target!(|data: &[u8]| {
    let _ = Node::<ChunkAddress>::try_from(data);
});
