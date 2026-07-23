//! Differential decode fuzz: the width-pinned node decoder vs [`NodeView`].
//!
//! The view reads its reference width from each node's own header, so raw
//! attacker-controlled bytes drive both decoders and the oracle is threefold:
//! the view accepts exactly when either width-pinned decode accepts, an
//! accepted view agrees field-by-field with the accepting width, and the
//! view's emit/decode pair is a fixed point.
//!
//! Seeds live in `fuzz/seeds/mantaray_view_differential/` and are replayed on
//! stable by `seed_replay_mantaray_view_differential` in
//! `crates/mantaray/src/view.rs`.

#![no_main]

use libfuzzer_sys::fuzz_target;
use nectar_mantaray::hazmat::{self, Node};
use nectar_mantaray::{NodeType, NodeView, RefWidth};
use nectar_primitives::EncryptedChunkRef;
use nectar_primitives::chunk::{ChunkRef, RefKind, Reference};

fuzz_target!(|data: &[u8]| {
    let old_plain = hazmat::decode::<ChunkRef>(data);
    let old_encrypted = hazmat::decode::<EncryptedChunkRef>(data);
    let new = NodeView::try_from(data);

    assert_eq!(
        new.is_ok(),
        old_plain.is_ok() || old_encrypted.is_ok(),
        "decoders must agree on accept/reject"
    );
    let Ok(view) = new else { return };

    match view.ref_width() {
        RefWidth::Zero => {
            let plain = old_plain.expect("the zero-width shape decodes at every width");
            let encrypted = old_encrypted.expect("the zero-width shape decodes at every width");
            assert!(plain.entry().is_none() && plain.forks().is_empty());
            assert!(encrypted.entry().is_none() && encrypted.forks().is_empty());
            assert!(view.entry().is_none() && view.forks().is_empty());
        }
        RefWidth::Kind(RefKind::Plain) => compare(
            &old_plain.expect("the view accepted at the plain width"),
            &view,
        ),
        RefWidth::Kind(RefKind::Encrypted) => compare(
            &old_encrypted.expect("the view accepted at the encrypted width"),
            &view,
        ),
    }

    let emitted = Vec::<u8>::from(&view);
    let redecoded = NodeView::try_from(emitted.as_slice()).expect("re-emitted image must decode");
    assert_eq!(redecoded, view, "emit/decode must be a fixed point");
});

/// Field-by-field agreement between a width-pinned decoded node and the view
/// of the same bytes.
fn compare<R: Reference>(node: &Node<R>, view: &NodeView) {
    assert_eq!(
        node.entry().cloned().map(Reference::into_entry_ref),
        view.entry().cloned(),
        "entry"
    );
    assert_eq!(node.obfuscation_key(), view.obfuscation_key());
    assert_eq!(node.forks().len(), view.forks().len(), "fork count");
    for ((key, fork), fork_view) in node.forks().iter().zip(view.forks()) {
        assert_eq!(*key, fork_view.key(), "fork key");
        assert_eq!(fork.prefix(), fork_view.prefix(), "fork prefix");
        let child = fork.node();
        let flags = fork_view.node_type();
        assert_eq!(child.is_value(), flags.contains(NodeType::VALUE));
        assert_eq!(child.is_edge(), flags.contains(NodeType::EDGE));
        assert_eq!(
            child.is_with_path_separator(),
            flags.contains(NodeType::PATH_SEPARATOR)
        );
        assert_eq!(child.is_with_metadata(), flags.contains(NodeType::METADATA));
        assert_eq!(
            child.reference().cloned().map(Reference::into_entry_ref),
            Some(fork_view.reference().clone()),
            "fork reference"
        );
        let view_metadata = fork_view.metadata().cloned().unwrap_or_default();
        assert_eq!(child.metadata(), &view_metadata, "fork metadata");
    }
}
