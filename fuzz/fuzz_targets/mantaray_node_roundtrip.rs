//! Structured round-trip fuzz of the mantaray node codec.
//!
//! The valid-by-construction `Arbitrary` impls for `Node<R>`
//! (crates/mantaray/src/node.rs) generate only encodable, round-trip-stable
//! nodes, so the oracle is stronger than "no panic": the shared
//! `nectar_mantaray::oracles::node_round_trip` oracle requires every
//! generated node to encode, the encoding to decode to an equal node, and
//! the re-encode to reproduce the same bytes (canonical form). Any failure
//! is a codec bug.
//!
//! Both reference widths are driven: `ChunkRef` (32-byte plain) and
//! `EncryptedChunkRef` (64-byte encrypted). Fork references are full width,
//! so the encrypted width proves nonzero decryption keys survive the round
//! trip.
//!
//! The same oracle is pinned on stable by the
//! `node_encode_decode_round_trip` and
//! `encrypted_node_encode_decode_round_trip` proptests in
//! `crates/mantaray/src/codec.rs`.

#![no_main]

use libfuzzer_sys::fuzz_target;
use nectar_mantaray::hazmat::Node;
use nectar_mantaray::oracles;
use nectar_primitives::EncryptedChunkRef;
use nectar_primitives::chunk::ChunkRef;

fuzz_target!(|nodes: (Node<ChunkRef>, Node<EncryptedChunkRef>)| {
    oracles::node_round_trip(&nodes.0).expect("plain nodes must round-trip");
    oracles::node_round_trip(&nodes.1).expect("encrypted nodes must round-trip");
});
