//! Shared fuzz and test oracles for the raw node codec, the node view and
//! the manifest editor.
//!
//! One oracle per invariant: the fuzz target and the stable pins call the
//! same body, so the rungs cannot drift. Oracles return `Err` instead of
//! panicking; call sites assert. Exposed only to in-crate tests and, under
//! `hazmat` plus `arbitrary`, to the fuzz workspace; exempt from semver
//! guarantees.

use std::collections::BTreeMap;

use nectar_primitives::EncryptedChunkRef;
use nectar_primitives::chunk::{ChunkAddress, ChunkRef, RefKind, Reference};
use nectar_primitives::oracles::Violation;

use crate::node::Node;
use crate::view::NodeView;
use crate::{DecodeResult, DefaultMemoryStore, ManifestEditor, NodeType, Reader, RefWidth};

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

/// Op-sequence cap per differential run.
const MAX_OPS: usize = 24;
/// Path-length cap in alphabet symbols.
const MAX_PATH: usize = 12;

/// One editor mutation for [`editor_differential`], decoded from raw fuzzer
/// bytes.
#[derive(Debug)]
pub enum EditorOp {
    /// Set the entry at the path to a reference filled with one byte.
    Put {
        /// Raw path bytes, mapped onto the dense four-symbol alphabet.
        path: Vec<u8>,
        /// Reference fill byte.
        fill: u8,
    },
    /// Set the entry with one metadata pair.
    PutMeta {
        /// Raw path bytes, mapped onto the dense four-symbol alphabet.
        path: Vec<u8>,
        /// Reference fill byte.
        fill: u8,
        /// Metadata key seed.
        key: u8,
        /// Metadata value seed.
        value: u8,
    },
    /// Remove the value at the path.
    Remove {
        /// Raw path bytes, mapped onto the dense four-symbol alphabet.
        path: Vec<u8>,
    },
    /// Set the website index document.
    SetIndex {
        /// Document name seed.
        name: u8,
    },
    /// Set the website error document.
    SetError {
        /// Document name seed.
        name: u8,
    },
}

impl<'a> arbitrary::Arbitrary<'a> for EditorOp {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        // Multiply-shift variant selection over one u32, the derive layout
        // the committed corpus was recorded under; changing it re-interprets
        // every committed seed.
        Ok(match u64::from(u32::arbitrary(u)?).wrapping_mul(5) >> 32 {
            0 => Self::Put {
                path: u.arbitrary()?,
                fill: u.arbitrary()?,
            },
            1 => Self::PutMeta {
                path: u.arbitrary()?,
                fill: u.arbitrary()?,
                key: u.arbitrary()?,
                value: u.arbitrary()?,
            },
            2 => Self::Remove {
                path: u.arbitrary()?,
            },
            3 => Self::SetIndex {
                name: u.arbitrary()?,
            },
            _ => Self::SetError {
                name: u.arbitrary()?,
            },
        })
    }

    fn size_hint(_depth: usize) -> (usize, Option<usize>) {
        (4, None)
    }
}

/// Map raw bytes onto a dense four-symbol path, so fork splits, nested
/// prefixes and separator edges stay dense in the search space.
fn path(bytes: &[u8]) -> String {
    bytes
        .iter()
        .take(MAX_PATH)
        .map(|byte| match byte % 4 {
            0 => 'a',
            1 => 'b',
            2 => 'c',
            _ => '/',
        })
        .collect()
}

const fn address(fill: u8) -> ChunkAddress {
    ChunkAddress::new([fill; 32])
}

fn document(name: u8) -> String {
    format!("doc{name}")
}

fn metadata(key: u8, value: u8) -> BTreeMap<String, String> {
    BTreeMap::from([(format!("k{key}"), format!("v{value}"))])
}

/// Record the ops into a fresh editor.
fn record(ops: &[EditorOp]) -> ManifestEditor<DefaultMemoryStore> {
    let mut editor = ManifestEditor::new(DefaultMemoryStore::new());
    for op in ops {
        match op {
            EditorOp::Put { path: raw, fill } => {
                editor.put(path(raw), address(*fill));
            }
            EditorOp::PutMeta {
                path: raw,
                fill,
                key,
                value,
            } => {
                editor.put_with_metadata(path(raw), address(*fill), metadata(*key, *value));
            }
            EditorOp::Remove { path: raw } => {
                editor.remove(path(raw));
            }
            EditorOp::SetIndex { name } => {
                editor.set_index_document(&document(*name));
            }
            EditorOp::SetError { name } => {
                editor.set_error_document(&document(*name));
            }
        }
    }
    editor
}

/// The surviving state a successful commit must expose to the reader.
struct Model {
    entries: BTreeMap<String, ChunkAddress>,
    index_document: Option<String>,
    error_document: Option<String>,
}

/// Replay the ops on the path-set model; only meaningful when every op
/// applied, which a successful commit guarantees. Remove mirrors the trie's
/// boundary prune: the named path and everything under it.
fn model(ops: &[EditorOp]) -> Model {
    let mut entries = BTreeMap::new();
    let mut index_document = None;
    let mut error_document = None;
    for op in ops {
        match op {
            EditorOp::Put { path: raw, fill }
            | EditorOp::PutMeta {
                path: raw, fill, ..
            } => {
                entries.insert(path(raw), address(*fill));
            }
            EditorOp::Remove { path: raw } => {
                // Remove prunes the fork whose boundary the path names, so
                // every key under the prefix goes with it.
                let p = path(raw);
                entries.retain(|key, _| !key.starts_with(&p));
            }
            EditorOp::SetIndex { name } => index_document = Some(document(*name)),
            EditorOp::SetError { name } => error_document = Some(document(*name)),
        }
    }
    Model {
        entries,
        index_document,
        error_document,
    }
}

/// Differential of the manifest editor against a reader-based path-set
/// model: a committed op log (capped at [`MAX_OPS`]) must expose exactly
/// the model's surviving paths at their last put references, removed paths
/// must stay absent, the root documents must read back at their last set
/// values, and two commits of one log must agree.
pub async fn editor_differential(ops: &[EditorOp]) -> Result<(), Violation> {
    let ops = ops.get(..ops.len().min(MAX_OPS)).unwrap_or(ops);

    let first = record(ops).commit().await;
    let second = record(ops).commit().await;
    let (root, store) = match (first, second) {
        (Ok((root_a, store)), Ok((root_b, _))) => {
            if root_a != root_b {
                return Err(Violation::new("two commits of one log diverged"));
            }
            (root_a, store)
        }
        (Err(_), Err(_)) => return Ok(()),
        _ => {
            return Err(Violation::new(
                "two commits of one log disagreed on success",
            ));
        }
    };

    let want = model(ops);
    let reader = Reader::new(store);
    for (p, addr) in &want.entries {
        // The empty path is never addressable through the reader.
        if p.is_empty() {
            continue;
        }
        let Ok(got) = reader.get(&root, p.as_bytes()).await else {
            return Err(Violation::new("lookup over a complete store must succeed"));
        };
        let Some(entry) = got else {
            return Err(Violation::new("a committed path must be readable"));
        };
        if entry.reference().map(|r| *r.address()) != Some(*addr) {
            return Err(Violation::new("a reference diverged from the model"));
        }
    }
    // A removed or never-put probe must stay absent; probe removed paths.
    // The "/" node also carries the root documents, so a removed root value
    // may legitimately read back as a metadata-only entry.
    for op in ops {
        if let EditorOp::Remove { path: raw } = op {
            let p = path(raw);
            if p.is_empty() || want.entries.contains_key(&p) {
                continue;
            }
            let Ok(got) = reader.get(&root, p.as_bytes()).await else {
                return Err(Violation::new("lookup over a complete store must succeed"));
            };
            if p == "/" {
                if got.as_ref().is_some_and(|e| e.reference().is_some()) {
                    return Err(Violation::new(
                        "a removed root value still carries a reference",
                    ));
                }
            } else if got.is_some() {
                return Err(Violation::new("a removed path is still readable"));
            }
        }
    }
    // The root documents read back as the last set values; a value op on
    // the "/" path itself may rewrite that node, so the check stands only
    // when no op touched it.
    let slash_touched = ops.iter().any(|op| match op {
        EditorOp::Put { path: raw, .. }
        | EditorOp::PutMeta { path: raw, .. }
        | EditorOp::Remove { path: raw } => path(raw) == "/",
        EditorOp::SetIndex { .. } | EditorOp::SetError { .. } => false,
    });
    if !slash_touched && (want.index_document.is_some() || want.error_document.is_some()) {
        let Ok(Some(root_entry)) = reader.get(&root, b"/").await else {
            return Err(Violation::new("root documents set but no root entry"));
        };
        if root_entry.metadata().get("website-index-document") != want.index_document.as_ref() {
            return Err(Violation::new("the index document diverged"));
        }
        if root_entry.metadata().get("website-error-document") != want.error_document.as_ref() {
            return Err(Violation::new("the error document diverged"));
        }
    }
    Ok(())
}
