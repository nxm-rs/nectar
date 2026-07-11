//! Fuzz the mantaray node wire decoder with raw attacker-controlled bytes.
//!
//! `Node::<E>::try_from(&[u8])` is the entry point through which untrusted
//! manifest chunks from the network are parsed. Both entry widths are
//! exercised on every input: `ChunkAddress` (plain manifests, 32-byte
//! entries) and `EncryptedChunkRef` (encrypted manifests, 64-byte entries),
//! each of which drives its own `ref_bytes_size` slicing arithmetic in
//! `decode_v01`/`decode_v02`. The decoder is the structure recoverer, so the
//! target takes raw bytes; any returned `Err` is success. The oracle is "no
//! panic, no OOM, no hang".
//!
//! Seeds live in `fuzz/seeds/mantaray_node_decode/` and are replayed on
//! stable by `seed_replay_mantaray_node_decode` in
//! `crates/mantaray/src/codec.rs`.

#![no_main]

use libfuzzer_sys::fuzz_target;
use nectar_mantaray::Node;
use nectar_primitives::EncryptedChunkRef;
use nectar_primitives::chunk::ChunkAddress;

fuzz_target!(|data: &[u8]| {
    let _ = Node::<ChunkAddress>::try_from(data);
    let _ = Node::<EncryptedChunkRef>::try_from(data);
});
