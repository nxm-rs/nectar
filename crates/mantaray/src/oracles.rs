//! Shared fuzz and test oracles for the raw node codec and the node view.
//!
//! One oracle per invariant: the fuzz target and the stable pins call the
//! same body, so the rungs cannot drift. Oracles return `Err` instead of
//! panicking; call sites assert. Exposed only to in-crate tests and, under
//! `hazmat` plus `arbitrary`, to the fuzz workspace; exempt from semver
//! guarantees.

use nectar_primitives::EncryptedChunkRef;
use nectar_primitives::chunk::{ChunkRef, RefKind, Reference};
use nectar_primitives::oracles::Violation;

use crate::node::Node;
use crate::view::NodeView;
use crate::{DecodeResult, NodeType, RefWidth};

/// Decode one wire image at both entry widths, `ChunkRef` (32-byte plain)
/// and `EncryptedChunkRef` (64-byte encrypted). `Err` is an acceptable
/// outcome for arbitrary bytes at either width; the invariant is that no
/// path panics.
pub fn node_decode(
    data: &[u8],
) -> (
    DecodeResult<Node<ChunkRef>>,
    DecodeResult<Node<EncryptedChunkRef>>,
) {
    (Node::decode(data), Node::decode(data))
}

/// Round trip of one in-memory node: encode, decode, compare, and re-encode
/// canonically. For valid-by-construction nodes any failure is a codec bug.
pub fn node_round_trip<R: Reference>(node: &Node<R>) -> Result<(), Violation> {
    let Ok(encoded) = node.encode() else {
        return Err(Violation::new("valid nodes must encode"));
    };
    let Ok(decoded) = Node::<R>::decode(encoded.as_slice()) else {
        return Err(Violation::new("encoded nodes must decode"));
    };
    if decoded != *node {
        return Err(Violation::new(
            "decode(encode(node)) must reproduce the node",
        ));
    }
    let Ok(reencoded) = decoded.encode() else {
        return Err(Violation::new("decoded nodes must re-encode"));
    };
    if reencoded != encoded {
        return Err(Violation::new("encoding must be canonical"));
    }
    Ok(())
}

/// Fixed-point round trip of one wire image at one reference width. A width
/// the image does not declare is rejected and reported as `Ok(false)`. The
/// encoder normalizes to v0.2, so the first re-encode is the canonical image
/// and the oracle is a fixed point, not equality with the decoded input.
pub fn record_round_trip<R: Reference>(data: &[u8]) -> Result<bool, Violation> {
    let Ok(node) = Node::<R>::decode(data) else {
        return Ok(false);
    };
    // A decoded node carries a saved reference on every fork child, so it is
    // always encodable; this first re-encode is the canonical v0.2 image.
    let Ok(encoded) = node.encode() else {
        return Err(Violation::new("a decoded node must re-encode"));
    };
    let Ok(redecoded) = Node::<R>::decode(encoded.as_slice()) else {
        return Err(Violation::new("the canonical image must decode"));
    };
    let Ok(reencoded) = redecoded.encode() else {
        return Err(Violation::new("a re-decoded node must re-encode"));
    };
    if reencoded != encoded {
        return Err(Violation::new(
            "encode/decode must reach a byte-canonical fixed point",
        ));
    }
    let Ok(redecoded_again) = Node::<R>::decode(reencoded.as_slice()) else {
        return Err(Violation::new("the canonical image must re-decode"));
    };
    if redecoded_again != redecoded {
        return Err(Violation::new(
            "decode(encode(node)) must be structurally stable",
        ));
    }
    Ok(true)
}

/// Differential decode of one wire image: the width-pinned node decoder and
/// [`NodeView`] agree on accept/reject, agree structurally on accept, and
/// the view's emit/decode pair is a fixed point. Fork flags are compared by
/// containment of the four named [`NodeType`] bits, so undefined bits in the
/// image cannot diverge the decoders.
pub fn view_differential(data: &[u8]) -> Result<(), Violation> {
    let old_plain = Node::<ChunkRef>::decode(data);
    let old_encrypted = Node::<EncryptedChunkRef>::decode(data);
    let new = NodeView::try_from(data);

    if new.is_ok() != (old_plain.is_ok() || old_encrypted.is_ok()) {
        return Err(Violation::new("decoders must agree on accept/reject"));
    }
    let Ok(view) = new else { return Ok(()) };

    match view.ref_width() {
        RefWidth::Zero => {
            let (Ok(plain), Ok(encrypted)) = (old_plain, old_encrypted) else {
                return Err(Violation::new(
                    "the zero-width shape decodes at every width",
                ));
            };
            if plain.entry().is_some()
                || !plain.forks().is_empty()
                || encrypted.entry().is_some()
                || !encrypted.forks().is_empty()
                || view.entry().is_some()
                || !view.forks().is_empty()
            {
                return Err(Violation::new(
                    "the zero-width shape is entryless and forkless",
                ));
            }
        }
        RefWidth::Kind(RefKind::Plain) => {
            let Ok(node) = old_plain else {
                return Err(Violation::new("the view accepted at the plain width"));
            };
            compare(&node, &view)?;
        }
        RefWidth::Kind(RefKind::Encrypted) => {
            let Ok(node) = old_encrypted else {
                return Err(Violation::new("the view accepted at the encrypted width"));
            };
            compare(&node, &view)?;
        }
    }

    let emitted = Vec::<u8>::from(&view);
    let Ok(redecoded) = NodeView::try_from(emitted.as_slice()) else {
        return Err(Violation::new("re-emitted image must decode"));
    };
    if redecoded != view {
        return Err(Violation::new("emit/decode must be a fixed point"));
    }
    Ok(())
}

/// Field-by-field agreement between a width-pinned decoded node and the view
/// of the same bytes.
fn compare<R: Reference>(node: &Node<R>, view: &NodeView) -> Result<(), Violation> {
    if node.entry().cloned().map(Reference::into_entry_ref) != view.entry().cloned() {
        return Err(Violation::new("entry"));
    }
    if node.obfuscation_key() != view.obfuscation_key() {
        return Err(Violation::new("obfuscation key"));
    }
    if node.forks().len() != view.forks().len() {
        return Err(Violation::new("fork count"));
    }
    for ((key, fork), fork_view) in node.forks().iter().zip(view.forks()) {
        if *key != fork_view.key() {
            return Err(Violation::new("fork key"));
        }
        if fork.prefix() != fork_view.prefix() {
            return Err(Violation::new("fork prefix"));
        }
        let child = fork.node();
        let flags = fork_view.node_type();
        if child.is_value() != flags.contains(NodeType::VALUE)
            || child.is_edge() != flags.contains(NodeType::EDGE)
            || child.is_with_path_separator() != flags.contains(NodeType::PATH_SEPARATOR)
            || child.is_with_metadata() != flags.contains(NodeType::METADATA)
        {
            return Err(Violation::new("fork flags"));
        }
        if child.reference().cloned().map(Reference::into_entry_ref)
            != Some(fork_view.reference().clone())
        {
            return Err(Violation::new("fork reference"));
        }
        let view_metadata = fork_view.metadata().cloned().unwrap_or_default();
        if child.metadata() != &view_metadata {
            return Err(Violation::new("fork metadata"));
        }
    }
    Ok(())
}
