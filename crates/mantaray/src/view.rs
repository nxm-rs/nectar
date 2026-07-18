//! Decode-once view of a single mantaray node image.
//!
//! [`NodeView`] parses a node's wire bytes exactly once; fork resolution and
//! re-emission then work from the parsed table without touching bytes again.
//! The reference width is read from each node's own header, so tries mixing
//! plain and encrypted nodes decode without a caller-chosen width.

use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;

use crate::error::{DecodeError, DecodeResult};
use crate::format::{FORK_INDEX_SIZE, MetadataLen, xor_in_place};
use crate::node::{NodeType, Prefix};
use crate::obfuscation::ObfuscationKey;

pub use crate::format::{RefWidth, Version};

use alloy_primitives::U256;
use nectar_primitives::chunk::{ChunkRef, RefKind, Reference};
use nectar_primitives::wire::{Cursor, Underrun, Writer};
use nectar_primitives::{EncryptedChunkRef, EntryRef};

/// A mantaray node decoded from its wire image in one pass.
///
/// Constructed only by [`TryFrom<&[u8]>`], so every view upholds the wire
/// invariants: fork keys are unique and ascending, and every reference slot
/// carries the header-declared width.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeView {
    version: Version,
    obfuscation_key: ObfuscationKey,
    ref_width: RefWidth,
    entry: Option<EntryRef>,
    forks: Vec<ForkView>,
}

impl NodeView {
    /// Wire version this node was decoded from.
    pub const fn version(&self) -> Version {
        self.version
    }

    /// XOR obfuscation key the image was encrypted under.
    pub const fn obfuscation_key(&self) -> &ObfuscationKey {
        &self.obfuscation_key
    }

    /// Reference width declared by this node's own header.
    pub const fn ref_width(&self) -> RefWidth {
        self.ref_width
    }

    /// The entry stored at this node, absent when the slot is all-zero.
    pub const fn entry(&self) -> Option<&EntryRef> {
        self.entry.as_ref()
    }

    /// All forks in ascending key order.
    pub fn forks(&self) -> &[ForkView] {
        &self.forks
    }

    /// Fork whose index byte is `first`, resolved against the table parsed at
    /// decode; no bytes are re-read.
    pub fn fork(&self, first: u8) -> Option<&ForkView> {
        self.forks
            .binary_search_by_key(&first, ForkView::key)
            .ok()
            .and_then(|at| self.forks.get(at))
    }
}

impl TryFrom<&[u8]> for NodeView {
    type Error = DecodeError;

    /// Decode a node image: decrypt under the leading obfuscation key, then
    /// parse the header and body in one pass. Bytes past the last fork are
    /// ignored, matching the node codec.
    fn try_from(bytes: &[u8]) -> DecodeResult<Self> {
        let mut data = bytes.to_vec();
        let (key, body) = data
            .split_first_chunk_mut::<{ ObfuscationKey::SIZE }>()
            .ok_or(DecodeError::TooShort)?;
        let obfuscation_key = ObfuscationKey::from(*key);
        xor_in_place(body, obfuscation_key.as_bytes());

        let mut cur = Cursor::new(body);
        // The whole header is consumed before the version is validated, so a
        // header truncated at the width byte reports TooShort.
        let version_bytes = cur.take::<[u8; Version::SIZE]>()?;
        let width_byte = cur.take::<u8>()?;
        let version = Version::from_bytes(&version_bytes).ok_or(DecodeError::InvalidVersionHash)?;
        let ref_width = RefWidth::try_from(width_byte)?;

        let (entry, forks) = match ref_width {
            RefWidth::Zero => {
                // BEE-WORKAROUND(bee#5483): a zero-width node decodes only as
                // the entry-less terminal shape; declared forks would carry
                // zero-width references, so they are rejected as malformed.
                let index = cur.take::<[u8; FORK_INDEX_SIZE]>()?;
                if index.iter().any(|&b| b != 0) {
                    return Err(DecodeError::ZeroWidthForks);
                }
                (None, Vec::new())
            }
            RefWidth::Kind(kind) => {
                let entry = take_entry(&mut cur, kind)?;
                let index = U256::from_le_slice(&cur.take::<[u8; FORK_INDEX_SIZE]>()?);
                let mut forks = Vec::new();
                for key in 0..=u8::MAX {
                    if index.bit(usize::from(key)) {
                        forks.push(ForkView::take_fork(&mut cur, version, kind, key)?);
                    }
                }
                (entry, forks)
            }
        };

        Ok(Self {
            version,
            obfuscation_key,
            ref_width,
            entry,
            forks,
        })
    }
}

/// Re-emits the wire image the view was decoded from, byte-exact up to the
/// two decode canonicalizations: prefix padding is re-zeroed and ignored
/// trailing bytes are dropped.
impl From<&NodeView> for Vec<u8> {
    fn from(view: &NodeView) -> Self {
        let mut data = Self::new();
        data.extend_from_slice(view.obfuscation_key.as_bytes());
        data.extend_from_slice(view.version.as_bytes());
        {
            let mut w = Writer::new(&mut data);
            w.put(&view.ref_width.as_byte());
            match view.ref_width {
                RefWidth::Zero => w.put_zeros(FORK_INDEX_SIZE),
                RefWidth::Kind(kind) => {
                    // The entry variant matches `kind` by decode construction.
                    match &view.entry {
                        Some(entry) => put_reference(&mut w, entry),
                        None => w.put_zeros(kind.size()),
                    }
                    let mut index = U256::ZERO;
                    for fork in &view.forks {
                        index.set_bit(usize::from(fork.key), true);
                    }
                    w.put(&index.to_le_bytes::<FORK_INDEX_SIZE>());
                    for fork in &view.forks {
                        fork.put_fork(&mut w);
                    }
                }
            }
        }
        let (_, body) = data.split_at_mut(ObfuscationKey::SIZE);
        xor_in_place(body, view.obfuscation_key.as_bytes());
        data
    }
}

/// One fork parsed from a node image: the child's reference and edge data,
/// not the child itself; fetching and decoding the child is the caller's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForkView {
    key: u8,
    node_type: NodeType,
    prefix: Prefix,
    reference: EntryRef,
    metadata: Option<ForkMetadata>,
}

impl ForkView {
    /// Index byte this fork is keyed under in the forks bitfield.
    pub const fn key(&self) -> u8 {
        self.key
    }

    /// Node type flags carried on the fork record.
    pub const fn node_type(&self) -> NodeType {
        self.node_type
    }

    /// The prefix bytes for this fork edge, always 1..=30 bytes.
    pub fn prefix(&self) -> &[u8] {
        &self.prefix
    }

    /// The child's full-width reference.
    pub const fn reference(&self) -> &EntryRef {
        &self.reference
    }

    /// Parsed metadata pairs, present when the wire carried a metadata region.
    pub const fn metadata(&self) -> Option<&BTreeMap<String, String>> {
        match &self.metadata {
            Some(metadata) => Some(&metadata.entries),
            None => None,
        }
    }

    /// Parse one fork record. A metadata region exists only on v0.2 forks
    /// flagged `METADATA`; its raw padded bytes are retained for byte-exact
    /// re-emission.
    fn take_fork(
        cur: &mut Cursor<'_>,
        version: Version,
        kind: RefKind,
        key: u8,
    ) -> DecodeResult<Self> {
        let node_type = NodeType::from_bits_truncate(cur.take::<u8>()?);
        let prefix = cur.take::<Prefix>()?;
        let reference = take_reference(cur, kind)?;
        let metadata = if matches!(version, Version::V02) && node_type.contains(NodeType::METADATA)
        {
            let len = cur.take::<MetadataLen>()?;
            let raw = cur.take_slice(len.get())?.to_vec();
            let entries = if raw.is_empty() {
                BTreeMap::new()
            } else {
                serde_json::from_slice(&raw)?
            };
            Some(ForkMetadata { len, raw, entries })
        } else {
            None
        };
        Ok(Self {
            key,
            node_type,
            prefix,
            reference,
            metadata,
        })
    }

    /// Emit this fork's record: type byte, prefix, full-width reference, then
    /// any metadata region verbatim.
    fn put_fork(&self, w: &mut Writer<'_>) {
        w.put(&self.node_type.bits());
        w.put(&self.prefix);
        put_reference(w, &self.reference);
        if let Some(metadata) = &self.metadata {
            w.put(&metadata.len);
            w.put(metadata.raw.as_slice());
        }
    }
}

/// A fork's metadata region: the parsed pairs plus the raw padded JSON, kept
/// verbatim (`raw.len() == len` by construction) for byte-exact re-emission.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ForkMetadata {
    len: MetadataLen,
    raw: Vec<u8>,
    entries: BTreeMap<String, String>,
}

/// Read the entry slot at the node's declared width; the all-zero slot is
/// `None`.
fn take_entry(cur: &mut Cursor<'_>, kind: RefKind) -> Result<Option<EntryRef>, Underrun> {
    match kind {
        RefKind::Plain => {
            ChunkRef::read_optional(cur).map(|entry| entry.map(Reference::into_entry_ref))
        }
        RefKind::Encrypted => {
            EncryptedChunkRef::read_optional(cur).map(|entry| entry.map(Reference::into_entry_ref))
        }
    }
}

/// Read a fork's full-width reference slot; unlike the entry slot, all-zero
/// is a reference, not absence.
fn take_reference(cur: &mut Cursor<'_>, kind: RefKind) -> Result<EntryRef, Underrun> {
    match kind {
        RefKind::Plain => cur.take::<ChunkRef>().map(Reference::into_entry_ref),
        RefKind::Encrypted => cur
            .take::<EncryptedChunkRef>()
            .map(Reference::into_entry_ref),
    }
}

/// Emit a reference at its full wire width.
fn put_reference(w: &mut Writer<'_>, reference: &EntryRef) {
    match reference {
        EntryRef::Plain(reference) => w.put(reference),
        EntryRef::Encrypted(encrypted) => w.put(encrypted),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::NodeHeader;
    use crate::node::{Fork, Node};
    use alloy_primitives::hex;
    use nectar_primitives::chunk::ChunkAddress;
    use nectar_primitives::{EncryptionKey, EntryRef};

    /// The v0.2 single-fork website manifest fixture from the go test suite.
    const GO_MANIFEST_V02: &str = "00000000000000000000000000000000000000000000000000000000000000005768b3b6a7db56d21d1abff40d41cebfc83448fed8d7e9b06ec0d3b073f28f200000000000000000000000000000000000000000000000000000000000000000000000000080000000000000000000000000000000000000000000000000000016012f0000000000000000000000000000000000000000000000000000000000e87f95c3d081c4fede769b6c69e27b435e525cbd25c6715c607e7c531e329639005d7b22776562736974652d696e6465782d646f63756d656e74223a2233356561656538316262363338303436393965633637316265323736326465626665346662643330636461646139303232393239646131613965366134366436227d0a";

    /// The obfuscated multi-fork fixtures the node codec pins.
    const ENCODED_V01: &str = "52fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64950ac787fbce1061870e8d34e0a638bc7e812c7ca4ebd31d626a572ba47b06f6952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072102654f163f5f0fa0621d729566c74d10037c4d7bbb0407d1e2c64950fcd3072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64950f89d6640e3044f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64850ff9f642182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64b50fc98072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64a50ff99622182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64d";
    const ENCODED_V02: &str = "52fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64905954fb18659339d0b25e0fb9723d3cd5d528fb3c8d495fd157bd7b7a210496952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072102654f163f5f0fa0621d729566c74d10037c4d7bbb0407d1e2c64940fcd3072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952e3872548ec012a6e123b60f9177017fb12e57732621d2c1ada267adbe8cc4350f89d6640e3044f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64850ff9f642182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64b50fc98072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64a50ff99622182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64d";

    /// Assert the differential decode oracle on one input: the width-pinned
    /// node decoder and the view agree on accept/reject, agree structurally on
    /// accept, and the view's emit/decode pair is a fixed point. Mirrors the
    /// `mantaray_view_differential` fuzz target.
    fn assert_differential_agreement(data: &[u8]) {
        let old_plain = Node::<ChunkRef>::decode(data);
        let old_encrypted = Node::<EncryptedChunkRef>::decode(data);
        let new = NodeView::try_from(data);

        assert_eq!(
            new.is_ok(),
            old_plain.is_ok() || old_encrypted.is_ok(),
            "decoders must agree on accept/reject"
        );
        let Ok(view) = new else { return };

        match view.ref_width() {
            RefWidth::Zero => {
                let plain = old_plain.unwrap();
                let encrypted = old_encrypted.unwrap();
                assert!(plain.entry().is_none() && plain.forks().is_empty());
                assert!(encrypted.entry().is_none() && encrypted.forks().is_empty());
                assert!(view.entry().is_none() && view.forks().is_empty());
            }
            RefWidth::Kind(RefKind::Plain) => compare(&old_plain.unwrap(), &view),
            RefWidth::Kind(RefKind::Encrypted) => compare(&old_encrypted.unwrap(), &view),
        }

        let emitted = Vec::<u8>::from(&view);
        let redecoded = NodeView::try_from(emitted.as_slice()).unwrap();
        assert_eq!(redecoded, view, "emit/decode must be a fixed point");
    }

    /// Field-by-field agreement between a width-pinned decoded node and the
    /// view of the same bytes.
    fn compare<R: Reference>(node: &Node<R>, view: &NodeView) {
        assert_eq!(
            node.entry().cloned().map(Reference::into_entry_ref),
            view.entry().cloned()
        );
        assert_eq!(node.obfuscation_key(), view.obfuscation_key());
        assert_eq!(node.forks().len(), view.forks().len());
        for ((key, fork), fork_view) in node.forks().iter().zip(view.forks()) {
            assert_eq!(*key, fork_view.key());
            assert_eq!(fork.prefix(), fork_view.prefix());
            let child = fork.node();
            assert_eq!(child.node_type, fork_view.node_type());
            assert_eq!(
                child.reference().cloned().map(Reference::into_entry_ref),
                Some(fork_view.reference().clone())
            );
            let view_metadata = fork_view.metadata().cloned().unwrap_or_default();
            assert_eq!(child.metadata(), &view_metadata);
        }
    }

    #[test]
    fn go_manifest_decodes_and_re_emits_byte_exactly() {
        let data = hex::decode(GO_MANIFEST_V02).unwrap();
        let view = NodeView::try_from(data.as_slice()).unwrap();

        assert_eq!(view.version(), Version::V02);
        assert_eq!(view.ref_width(), RefWidth::Kind(RefKind::Plain));
        assert!(view.entry().is_none());

        let fork = view.fork(b'/').unwrap();
        assert_eq!(fork.prefix(), b"/");
        let metadata = fork.metadata().unwrap();
        assert_eq!(
            metadata.get("website-index-document").map(String::as_str),
            Some("35eaee81bb63804699ec671be2762debfe4fbd30cdada9022929da1a9e6a46d6"),
        );

        // The fixture carries one stray byte past the declared metadata
        // region; the emitted image is byte-exact over the decoded extent.
        let emitted = Vec::<u8>::from(&view);
        assert_eq!(Some(emitted.as_slice()), data.get(..data.len() - 1));
        assert_eq!(NodeView::try_from(emitted.as_slice()).unwrap(), view);
        assert_differential_agreement(&data);
    }

    #[test]
    fn go_manifest_bad_metadata_lengths_are_rejected() {
        // The valid fixture declares 0x5d (93) metadata bytes; 89 truncates
        // the JSON, 95 and 96 overrun the buffer.
        for declared in ["59", "5f", "60"] {
            let hex_image = GO_MANIFEST_V02.replace("005d7b", &alloc::format!("00{declared}7b"));
            let data = hex::decode(hex_image).unwrap();
            assert!(NodeView::try_from(data.as_slice()).is_err());
            assert_differential_agreement(&data);
        }
    }

    #[test]
    fn obfuscated_fixtures_decode_and_re_emit_byte_exactly() {
        for encoded in [ENCODED_V01, ENCODED_V02] {
            let data = hex::decode(encoded).unwrap();
            let view = NodeView::try_from(data.as_slice()).unwrap();
            assert_eq!(view.forks().len(), 5);
            assert_eq!(view.fork(b'a').unwrap().prefix(), b"aaaaa");
            assert_eq!(view.fork(b'c').unwrap().prefix(), b"cc");
            assert_eq!(Vec::<u8>::from(&view), data);
            assert_differential_agreement(&data);
        }
        let v01 = NodeView::try_from(hex::decode(ENCODED_V01).unwrap().as_slice()).unwrap();
        assert_eq!(v01.version(), Version::V01);
        let v02 = NodeView::try_from(hex::decode(ENCODED_V02).unwrap().as_slice()).unwrap();
        assert_eq!(v02.version(), Version::V02);
        assert!(v02.fork(b'/').unwrap().metadata().is_some());
    }

    #[test]
    fn trailing_bytes_are_ignored_and_dropped_on_re_emit() {
        let data = hex::decode(GO_MANIFEST_V02).unwrap();
        let mut padded = data.clone();
        padded.extend_from_slice(&[0xde, 0xad, 0xbe, 0xef]);
        let view = NodeView::try_from(data.as_slice()).unwrap();
        let padded_view = NodeView::try_from(padded.as_slice()).unwrap();
        assert_eq!(padded_view, view);
        assert_eq!(Vec::<u8>::from(&padded_view), Vec::<u8>::from(&view));
        assert_differential_agreement(&padded);
    }

    /// Header-only prefix of a node: zero obfuscation key (XOR is identity),
    /// the given version, the given `ref_size` byte, zero-padded to `len`.
    fn raw_node_bytes(version: Version, ref_size: u8, len: usize) -> Vec<u8> {
        let mut data = vec![0u8; len];
        data[NodeHeader::VERSION_HASH_OFFSET..NodeHeader::VERSION_HASH_OFFSET + Version::SIZE]
            .copy_from_slice(version.as_bytes());
        data[NodeHeader::REF_SIZE_OFFSET] = ref_size;
        data
    }

    #[test]
    fn zero_width_terminal_decodes_empty_and_re_emits_byte_exactly() {
        for version in [Version::V01, Version::V02] {
            let data = raw_node_bytes(version, 0, NodeHeader::SIZE + FORK_INDEX_SIZE);
            let view = NodeView::try_from(data.as_slice()).unwrap();
            assert_eq!(view.ref_width(), RefWidth::Zero);
            assert!(view.entry().is_none());
            assert!(view.forks().is_empty());
            assert_eq!(Vec::<u8>::from(&view), data);
            assert_differential_agreement(&data);
        }
    }

    #[test]
    fn zero_width_with_forks_is_rejected() {
        let mut data = raw_node_bytes(Version::V02, 0, NodeHeader::SIZE + FORK_INDEX_SIZE);
        data[NodeHeader::SIZE] = 0x01;
        assert!(matches!(
            NodeView::try_from(data.as_slice()),
            Err(DecodeError::ZeroWidthForks)
        ));
        assert_differential_agreement(&data);
    }

    #[test]
    fn unsupported_ref_width_is_rejected() {
        let data = raw_node_bytes(Version::V02, 33, NodeHeader::SIZE + 33 + FORK_INDEX_SIZE);
        assert!(matches!(
            NodeView::try_from(data.as_slice()),
            Err(DecodeError::UnsupportedRefWidth { actual: 33 })
        ));
        assert_differential_agreement(&data);
    }

    #[test]
    fn zero_length_fork_prefix_is_rejected() {
        // 32-byte entry slot, index bit 0 set, then a fork record whose prefix
        // length byte is zero.
        let mut data = raw_node_bytes(
            Version::V02,
            32,
            NodeHeader::SIZE + ChunkRef::SIZE + FORK_INDEX_SIZE + 32 + ChunkRef::SIZE,
        );
        data[NodeHeader::SIZE + ChunkRef::SIZE] = 0x01;
        assert!(matches!(
            NodeView::try_from(data.as_slice()),
            Err(DecodeError::InvalidPrefixLength { actual: 0, .. })
        ));
        assert_differential_agreement(&data);
    }

    #[test]
    fn truncated_inputs_are_rejected() {
        for len in 0..NodeHeader::SIZE + FORK_INDEX_SIZE {
            let mut data = vec![0u8; len];
            if len > NodeHeader::VERSION_HASH_OFFSET + Version::SIZE {
                data[NodeHeader::VERSION_HASH_OFFSET
                    ..NodeHeader::VERSION_HASH_OFFSET + Version::SIZE]
                    .copy_from_slice(Version::V02.as_bytes());
            }
            assert!(NodeView::try_from(data.as_slice()).is_err(), "length {len}");
            assert_differential_agreement(&data);
        }
    }

    /// Two nodes of one trie carrying different widths: the parent declares
    /// plain 32-byte slots, the child it references declares encrypted
    /// 64-byte slots. Each view reads its own header, so both decode.
    #[test]
    fn mixed_width_trie_nodes_decode_independently() {
        let child_ref = EncryptedChunkRef::new(
            ChunkAddress::from([0xaa; 32]),
            EncryptionKey::from([0xbb; 32]),
        );
        let mut grandchild = Node::<EncryptedChunkRef>::from_reference(child_ref.clone());
        grandchild.node_type = NodeType::VALUE;
        let mut child = Node::<EncryptedChunkRef>::new_unencrypted();
        child.forks.insert(
            b'x',
            Fork {
                prefix: Prefix::from_slice(b"x"),
                node: grandchild,
            },
        );
        let child_image = child.encode().unwrap();

        let mut leaf =
            Node::<ChunkRef>::from_reference(ChunkRef::from(ChunkAddress::from([0x11; 32])));
        leaf.node_type = NodeType::VALUE;
        let mut parent = Node::<ChunkRef>::new_unencrypted();
        parent.forks.insert(
            b'a',
            Fork {
                prefix: Prefix::from_slice(b"a"),
                node: leaf,
            },
        );
        let parent_image = parent.encode().unwrap();

        let parent_view = NodeView::try_from(parent_image.as_slice()).unwrap();
        let child_view = NodeView::try_from(child_image.as_slice()).unwrap();
        assert_eq!(parent_view.ref_width(), RefWidth::Kind(RefKind::Plain));
        assert_eq!(child_view.ref_width(), RefWidth::Kind(RefKind::Encrypted));
        assert!(matches!(
            parent_view.fork(b'a').unwrap().reference(),
            EntryRef::Plain(_)
        ));
        assert_eq!(
            child_view.fork(b'x').unwrap().reference(),
            &EntryRef::Encrypted(child_ref)
        );
        assert_differential_agreement(&parent_image);
        assert_differential_agreement(&child_image);
    }

    #[test]
    fn encrypted_entry_and_fork_key_survive_the_view() {
        let entry_ref = EncryptedChunkRef::new(
            ChunkAddress::from([0x22; 32]),
            EncryptionKey::from([0x33; 32]),
        );
        let mut leaf = Node::<EncryptedChunkRef>::from_reference(entry_ref.clone());
        leaf.node_type = NodeType::VALUE;
        let mut node = Node::<EncryptedChunkRef>::new_unencrypted();
        node.entry = Some(entry_ref.clone());
        node.forks.insert(
            b'k',
            Fork {
                prefix: Prefix::from_slice(b"key"),
                node: leaf,
            },
        );
        let image = node.encode().unwrap();

        let view = NodeView::try_from(image.as_slice()).unwrap();
        assert_eq!(view.entry(), Some(&EntryRef::Encrypted(entry_ref.clone())));
        assert_eq!(
            view.fork(b'k').unwrap().reference(),
            &EntryRef::Encrypted(entry_ref)
        );
        assert_eq!(Vec::<u8>::from(&view), image);
        assert_differential_agreement(&image);
    }

    #[test]
    fn fork_resolution_misses_absent_keys() {
        let data = hex::decode(GO_MANIFEST_V02).unwrap();
        let view = NodeView::try_from(data.as_slice()).unwrap();
        assert!(view.fork(b'/').is_some());
        assert!(view.fork(b'a').is_none());
        assert!(view.fork(0).is_none());
    }

    /// Replay the committed differential seed corpus through the exact oracle
    /// the `mantaray_view_differential` fuzz target asserts, keeping the
    /// seeds meaningful on stable without running the fuzzer.
    #[test]
    fn seed_replay_mantaray_view_differential() {
        let seed_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fuzz/seeds/mantaray_view_differential");
        let mut replayed = 0usize;
        for entry in std::fs::read_dir(&seed_dir)
            .unwrap_or_else(|e| panic!("seed dir {} must exist: {e}", seed_dir.display()))
        {
            let data = std::fs::read(entry.unwrap().path()).unwrap();
            assert_differential_agreement(&data);
            replayed += 1;
        }
        assert!(
            replayed >= 3,
            "expected at least the 3 curated seeds, found {replayed}"
        );
    }
}
