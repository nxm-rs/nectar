//! Structured round-trip fuzz of the mantaray node codec.
//!
//! The valid-by-construction `Arbitrary` impl for `Node<ChunkRef>`
//! (crates/mantaray/src/node.rs) generates only encodable, round-trip-stable
//! nodes, so the oracle is stronger than "no panic": every generated node
//! must encode (`hazmat::encode`), the encoding must decode
//! (`hazmat::decode`), the decoded node must equal the original,
//! and re-encoding the decoded node must reproduce the same bytes (canonical
//! form). Any failure is a codec bug.
//!
//! The same property is pinned on stable by
//! `arbitrary_node_encode_decode_round_trip` in `crates/mantaray/src/codec.rs`.

#![no_main]

use libfuzzer_sys::fuzz_target;
use nectar_mantaray::hazmat::{self, Node};
use nectar_primitives::chunk::ChunkRef;

fuzz_target!(|node: Node<ChunkRef>| {
    let encoded = hazmat::encode(&node).expect("arbitrary nodes must encode");
    let decoded =
        hazmat::decode::<ChunkRef>(encoded.as_slice()).expect("encoded nodes must decode");
    assert_eq!(decoded, node, "decode(encode(node)) must reproduce the node");

    // Canonical form: re-encoding the decoded node must be byte-identical.
    let reencoded = hazmat::encode(&decoded).expect("decoded nodes must re-encode");
    assert_eq!(reencoded, encoded, "encoding must be canonical");
});
