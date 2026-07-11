//! Corpus-seeded round-trip fuzz of the mantaray node codec at record
//! granularity, across both reference widths and both wire versions.
//!
//! `mantaray_node_roundtrip` drives the encode-first property from
//! valid-by-construction `Arbitrary` nodes, but the encoder emits v0.2 only,
//! so it never exercises a v0.1 wire image and only the 32-byte plain width.
//! This target closes both gaps with its own seed corpus: the v0.1 and v0.2
//! plain manifests shared with `mantaray_node_decode`, plus a `ref_size` 64
//! encrypted case. It decodes a real wire image to recover its header and fork
//! records, then round-trips that recovered node through encode and decode.
//! Both entry widths are attempted on every input,
//! `ChunkRef` (32-byte plain entries) and `EncryptedChunkRef` (64-byte
//! encrypted entries); a width the image does not declare is rejected and
//! skipped.
//!
//! The encoder normalizes to v0.2, so the first re-encode is the canonical
//! image. The oracle is therefore a fixed point rather than equality with the
//! decoded input: re-encoding the re-decoded node must be byte-identical, and
//! decoding it again must be structurally identical. Any drift is a codec bug.
//!
//! The same property is pinned on stable by
//! `seed_replay_mantaray_record_roundtrip` in `crates/mantaray/src/codec.rs`.

#![no_main]

use libfuzzer_sys::fuzz_target;
use nectar_mantaray::hazmat::Node;
use nectar_primitives::EncryptedChunkRef;
use nectar_primitives::chunk::{ChunkRef, Reference};

/// Round-trip a wire image at one reference width. A width the image does not
/// declare decodes to `Err` and is skipped; every decoded node must reach a
/// byte- and structure-canonical fixed point under encode/decode.
fn round_trip<R: Reference>(data: &[u8]) {
    let Ok(node) = Node::<R>::try_from(data) else {
        return;
    };

    // A decoded node carries a saved reference on every fork child, so it is
    // always encodable; this first re-encode is the canonical v0.2 image.
    let encoded = Vec::<u8>::try_from(&node).expect("a decoded node must re-encode");
    let redecoded =
        Node::<R>::try_from(encoded.as_slice()).expect("the canonical image must decode");

    let reencoded = Vec::<u8>::try_from(&redecoded).expect("a re-decoded node must re-encode");
    assert_eq!(reencoded, encoded, "encode/decode must reach a byte-canonical fixed point");

    let redecoded_again =
        Node::<R>::try_from(reencoded.as_slice()).expect("the canonical image must re-decode");
    assert_eq!(
        redecoded_again, redecoded,
        "decode(encode(node)) must be structurally stable"
    );
}

fuzz_target!(|data: &[u8]| {
    round_trip::<ChunkRef>(data);
    round_trip::<EncryptedChunkRef>(data);
});
