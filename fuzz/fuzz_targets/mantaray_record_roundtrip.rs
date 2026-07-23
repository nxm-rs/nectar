//! Corpus-seeded round-trip fuzz of the mantaray node codec at record
//! granularity, across both reference widths and both wire versions.
//!
//! `mantaray_node_roundtrip` drives the encode-first property from
//! valid-by-construction `Arbitrary` nodes, but the encoder emits v0.2 only,
//! so it never exercises a v0.1 wire image and only the 32-byte plain width.
//! This target closes both gaps with its own seed corpus, shared with
//! `mantaray_node_decode`: the v0.1 and v0.2 plain manifests plus two
//! `ref_size` 64 encrypted cases. The shared
//! `nectar_mantaray::oracles::record_round_trip` oracle decodes a real wire
//! image to recover its header and fork records, then round-trips that
//! recovered node through encode and decode. Both entry widths are attempted
//! on every input, `ChunkRef` (32-byte plain entries) and
//! `EncryptedChunkRef` (64-byte encrypted entries); a width the image does
//! not declare is rejected and skipped.
//!
//! The encoder normalizes to v0.2, so the first re-encode is the canonical
//! image. The oracle is therefore a fixed point rather than equality with the
//! decoded input: re-encoding the re-decoded node must be byte-identical, and
//! decoding it again must be structurally identical. Any drift is a codec bug.
//!
//! The same oracle is pinned on stable by
//! `seed_replay_mantaray_record_roundtrip` in `crates/mantaray/src/codec.rs`.

#![no_main]

use libfuzzer_sys::fuzz_target;
use nectar_mantaray::oracles;
use nectar_primitives::EncryptedChunkRef;
use nectar_primitives::chunk::ChunkRef;

fuzz_target!(|data: &[u8]| {
    let _ = oracles::record_round_trip::<ChunkRef>(data)
        .expect("plain-width records must reach a canonical fixed point");
    let _ = oracles::record_round_trip::<EncryptedChunkRef>(data)
        .expect("encrypted-width records must reach a canonical fixed point");
});
