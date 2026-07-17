//! Structured round-trip fuzz of the mantaray node codec.
//!
//! The valid-by-construction `Arbitrary` impls for `Node<R>`
//! (crates/mantaray/src/node.rs) generate only encodable, round-trip-stable
//! nodes, so the oracle is stronger than "no panic": every generated node
//! must encode (`hazmat::encode`), the encoding must decode
//! (`hazmat::decode`), the decoded node must equal the original,
//! and re-encoding the decoded node must reproduce the same bytes (canonical
//! form). Any failure is a codec bug.
//!
//! Both reference widths are driven: `ChunkRef` (32-byte plain) and
//! `EncryptedChunkRef` (64-byte encrypted). Fork references are full width,
//! so the encrypted width proves nonzero decryption keys survive the round
//! trip.
//!
//! The same property is pinned on stable by
//! `arbitrary_node_encode_decode_round_trip` and
//! `arbitrary_encrypted_node_encode_decode_round_trip` in
//! `crates/mantaray/src/codec.rs`.

#![no_main]

use libfuzzer_sys::fuzz_target;
use nectar_mantaray::hazmat::{self, Node};
use nectar_primitives::EncryptedChunkRef;
use nectar_primitives::chunk::{ChunkRef, Reference};

/// Round-trip one generated node: encode, decode, compare, re-encode.
fn round_trip<R: Reference>(node: &Node<R>) {
    let encoded = hazmat::encode(node).expect("arbitrary nodes must encode");
    let decoded = hazmat::decode::<R>(encoded.as_slice()).expect("encoded nodes must decode");
    assert_eq!(&decoded, node, "decode(encode(node)) must reproduce the node");

    // Canonical form: re-encoding the decoded node must be byte-identical.
    let reencoded = hazmat::encode(&decoded).expect("decoded nodes must re-encode");
    assert_eq!(reencoded, encoded, "encoding must be canonical");
}

fuzz_target!(|nodes: (Node<ChunkRef>, Node<EncryptedChunkRef>)| {
    round_trip(&nodes.0);
    round_trip(&nodes.1);
});
