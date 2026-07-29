//! Shared fuzz and test oracle for the manifest node codec.
//!
//! One oracle per invariant: the fuzz target and the stable pins call the
//! same body, so the rungs cannot drift. The oracle returns `Err` instead of
//! panicking; call sites assert. Exposed only to in-crate tests and, under
//! `arbitrary`, to the fuzz workspace; exempt from semver guarantees.

use nectar_primitives::oracles::Violation;

use crate::{Node, V1};

/// Decode one wire image; `Err` from decode is an acceptable outcome for
/// arbitrary bytes and reports as `Ok(None)`. An accepted image is canonical
/// by construction, so when it re-encodes (encode may still reject an
/// over-budget image, which is not a bijection break) the bytes must match
/// exactly and decode back to the same node.
pub fn node_decode_canonical(data: &[u8]) -> Result<Option<Node<V1>>, Violation> {
    let Ok(node) = Node::<V1>::decode(data) else {
        return Ok(None);
    };
    if let Ok(encoded) = node.encode() {
        if encoded != data {
            return Err(Violation::new("an accepted image must be canonical"));
        }
        let Ok(redecoded) = Node::<V1>::decode(&encoded) else {
            return Err(Violation::new("the re-encoding must decode"));
        };
        if redecoded != node {
            return Err(Violation::new(
                "decode(encode(node)) must reproduce the node",
            ));
        }
    }
    Ok(Some(node))
}
