//! Fuzz the mantaray 1.0 manifest node decoder with raw attacker-controlled
//! bytes.
//!
//! `Node::decode` is the entry point through which untrusted manifest chunks
//! from the network are parsed. The codec's only fallible byte access is the
//! primitives cursor, so the reject-or-accept contract must hold for every
//! input: the oracle is "no panic, no OOM, no hang", never a slice or unwrap
//! out of bounds on a truncated or adversarial image.
//!
//! Whenever an image is accepted, it is canonical by construction, so it must
//! re-encode to the exact bytes decoded and decode again to the same node: the
//! decode -> encode bijection proven here on arbitrary input complements the
//! structured round-trip target.

#![no_main]

use libfuzzer_sys::fuzz_target;
use nectar_manifest::{Node, V1};

fuzz_target!(|data: &[u8]| {
    let Ok(node) = Node::<V1>::decode(data) else {
        return;
    };
    // An accepted image is canonical, so re-encoding reproduces the bytes and
    // decoding the re-encoding reproduces the node. Encode can still reject an
    // over-budget image longer than one chunk body; that is not a bijection
    // break, so only assert when it succeeds.
    if let Ok(encoded) = node.encode() {
        assert_eq!(encoded, data, "accepted image must be canonical");
        let redecoded = Node::<V1>::decode(&encoded).expect("re-encoding must decode");
        assert_eq!(
            redecoded, node,
            "decode(encode(node)) must reproduce the node"
        );
    }
});
