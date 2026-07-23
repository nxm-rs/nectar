//! Binary encoding for mantaray nodes (v0.1 and v0.2).
//!
//! Wire behaviour: fork reference slots are carried at their full declared
//! width, so the encrypted width writes and reads the child's address and
//! decryption key (bee-spec node.md stores the full reference). Earlier
//! encoders zero-padded the key half; those images still decode, yielding an
//! all-zero key. Plain manifests are unaffected.

use alloc::collections::BTreeMap;

use crate::error::{DecodeError, DecodeResult, MantarayError, Result};
use crate::format::{FORK_INDEX_SIZE, ForkHeader, MetadataLen, NodeHeader, Version, xor_in_place};
use crate::node::{Fork, Node, NodeType, Prefix};
use crate::obfuscation::ObfuscationKey;

use alloy_primitives::U256;
use nectar_primitives::chunk::Reference;
use nectar_primitives::wire::{Cursor, Underrun, Writer};

impl<R: Reference> Node<R> {
    /// Encode this node into its wire image.
    ///
    /// Crate-internal: the only public path to node bytes is
    /// [`Manifest::save`](crate::Manifest::save), so callers cannot
    /// serialise nodes directly.
    #[inline]
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        encode_node(self)
    }

    /// Decode a wire image into a node.
    ///
    /// Crate-internal: nodes are decoded on load from a chunk store.
    /// The obfuscation key is the header's first field and is stored in the
    /// clear; every later byte is XOR-encrypted under it.
    pub(crate) fn decode(value: &[u8]) -> DecodeResult<Self> {
        let mut data = value.to_vec();

        let (key, body) = data
            .split_first_chunk_mut::<{ ObfuscationKey::SIZE }>()
            .ok_or(DecodeError::TooShort)?;
        let obfuscation_key = ObfuscationKey::from(*key);
        xor_in_place(body, obfuscation_key.as_bytes());

        let mut node = decode_node::<R>(&data)?;
        node.obfuscation_key = obfuscation_key;
        // A decoded node is loaded but unpersisted, i.e. dirty (the default).
        Ok(node)
    }
}

#[allow(clippy::arithmetic_side_effects)] // size arithmetic sums in-memory buffer lengths (<= 256 forks) and cannot overflow usize
fn encode_node<R: Reference>(node: &Node<R>) -> Result<Vec<u8>> {
    let ref_size = R::SIZE;
    // Pre-allocate: header + entry + bitfield + estimated fork data
    let estimated = NodeHeader::SIZE
        + ref_size
        + FORK_INDEX_SIZE
        + node.forks.len() * (ForkHeader::PRE_REFERENCE_SIZE + ref_size);
    let mut data = Vec::with_capacity(estimated);

    // Use the obfuscation key as-is. The key is set at manifest construction:
    // - PlainManifest: ObfuscationKey::ZERO (no obfuscation)
    // - EncryptedManifest: ObfuscationKey::generate() (random key)
    let obfuscation_key = node.obfuscation_key.as_bytes();

    // Header: obfuscation key, version hash, ref_size byte (NodeHeader::SIZE).
    data.extend_from_slice(obfuscation_key);
    data.extend_from_slice(Version::V02.as_bytes());
    #[allow(clippy::as_conversions)] // ref_size = R::SIZE (32 or 64), always fits u8
    let ref_size_byte = ref_size as u8;
    data.push(ref_size_byte);

    // append entry (or R::SIZE zero bytes if empty)
    match &node.entry {
        Some(e) => e.write_to(&mut data),
        None => data.resize(data.len() + ref_size, 0),
    }

    // build the 256-bit index of which fork bytes are present
    let mut index = U256::ZERO;
    for &fork_byte in node.forks.keys() {
        index.set_bit(usize::from(fork_byte), true);
    }
    data.extend_from_slice(&index.to_le_bytes::<FORK_INDEX_SIZE>());

    // append forks in sorted order, each as a total wire record over the buffer
    let mut writer = Writer::new(&mut data);
    for fork in node.forks.values() {
        WireFork::try_from(fork)?.emit(&mut writer);
    }

    // XOR-encrypt everything after the obfuscation key in place.
    let (_, body) = data.split_at_mut(ObfuscationKey::SIZE);
    xor_in_place(body, obfuscation_key);

    Ok(data)
}

// ┌─────────────────────────── HAZMAT ───────────────────────────┐
// │ BEE-WORKAROUND(bee#5483): bee's mantaray writer occasionally  │
// │ emits a node with `ref_size = 0` (the byte at header offset  │
// │ 63) for entry-less terminal nodes. This is not spec-legal:  │
// │ the spec doc (bee/pkg/manifest/mantaray/docs/format/node.md) │
// │ and every reference impl (bee, mantaray-js, nectar) treat    │
// │ `ref_size` as a single uniform width in {32, 64} governing   │
// │ both the entry slot and every fork ref slot. mantaray-js     │
// │ documents the bee artifact with an explicit FIXME: "in Bee,  │
// │ if one uploads a file on the bzz endpoint, the node under    │
// │ `/` gets 0 refsize."                                         │
// │                                                              │
// │ Remove the `RefSize::EmptyTerminal` variant, its zero-width   │
// │ classification in `parse_header`, and `decode_empty_terminal` │
// │ once the upstream bee fix lands and downstream consumers have │
// │ upgraded past the buggy releases.                            │
// └──────────────────────────────────────────────────────────────┘

/// The reference width declared by a node header.
///
/// `EmptyTerminal` is the bee#5483 sentinel (a `ref_size` byte of zero); it is
/// not spec-legal and marks an entry-less terminal node. See the HAZMAT block.
enum RefSize {
    /// bee#5483: `ref_size == 0`, an entry-less terminal node.
    EmptyTerminal,
    /// A declared uniform reference width in bytes.
    Declared(usize),
}

/// Decode a decrypted node buffer: header, then the version-specific body.
fn decode_node<R: Reference>(data: &[u8]) -> DecodeResult<Node<R>> {
    let total = data.len();
    let mut cur = Cursor::new(data);
    let (version, ref_size) = parse_header(&mut cur)?;
    match ref_size {
        RefSize::EmptyTerminal => decode_empty_terminal(&mut cur),
        RefSize::Declared(width) => decode_body::<R>(&mut cur, version, width, total),
    }
}

/// Parse the fixed node header, yielding the wire version and the reference
/// width.
///
/// The version hash is validated only after the ref_size byte is confirmed
/// present, so a header truncated at that byte reports `TooShort` rather
/// than an invalid-version error.
fn parse_header(cur: &mut Cursor<'_>) -> DecodeResult<(Version, RefSize)> {
    // The obfuscation key was already consumed by the caller for decryption.
    cur.take::<[u8; ObfuscationKey::SIZE]>()
        .map_err(|_| DecodeError::TooShort)?;
    let version_bytes = cur
        .take::<[u8; Version::SIZE]>()
        .map_err(|_| DecodeError::TooShort)?;
    let ref_size = cur.take::<u8>().map_err(|_| DecodeError::TooShort)?;
    let version = Version::from_bytes(&version_bytes).ok_or(DecodeError::InvalidVersionHash)?;
    let ref_size = if ref_size == 0 {
        RefSize::EmptyTerminal
    } else {
        RefSize::Declared(usize::from(ref_size))
    };
    Ok((version, ref_size))
}

/// Decode a `ref_size = 0` node as the empty terminal node bee intends.
///
/// Accepts this wire shape only when the forks bitfield is also empty; a
/// `ref_size = 0` node with non-empty forks is unrecoverable (fork refs would
/// have zero width), so it is rejected as malformed rather than silently
/// dropping forks. See the HAZMAT block.
fn decode_empty_terminal<R: Reference>(cur: &mut Cursor<'_>) -> DecodeResult<Node<R>> {
    let index = cur
        .take::<[u8; FORK_INDEX_SIZE]>()
        .map_err(|_| DecodeError::TooShort)?;
    if index.iter().any(|&b| b != 0) {
        return Err(DecodeError::RefSizeMismatch {
            expected: R::SIZE,
            actual: 0,
        });
    }
    Ok(Node {
        entry: None,
        forks: BTreeMap::new(),
        ..Default::default()
    })
}

/// Restate a fork-region [`Underrun`] as an `InsufficientForkBytes` diagnostic
/// carrying absolute byte offsets into the node buffer.
fn insufficient_fork(underrun: Underrun, total: usize, byte_index: u8) -> DecodeError {
    DecodeError::InsufficientForkBytes {
        expected: total
            .saturating_sub(underrun.available)
            .saturating_add(underrun.expected),
        actual: total,
        byte_index: usize::from(byte_index),
    }
}

/// Decode the node body: entry slot, forks bitfield, then each present fork.
///
/// The entry slot and index are both read before the entry is validated, so a
/// truncated index reports `TooShort` rather than an entry-shaped error.
/// v0.2 derives the root EDGE flag from a non-empty index; v0.1 leaves it unset.
fn decode_body<R: Reference>(
    cur: &mut Cursor<'_>,
    version: Version,
    ref_size: usize,
    total: usize,
) -> DecodeResult<Node<R>> {
    if ref_size != R::SIZE {
        return Err(DecodeError::RefSizeMismatch {
            expected: R::SIZE,
            actual: ref_size,
        });
    }

    // The entry slot is a zero-sentinel `Option<R>`: `read_optional` maps the
    // all-zero width to `None`. The index is read next so a truncated index
    // reports `TooShort` rather than an entry-shaped error.
    let entry = R::read_optional(cur).map_err(|_| DecodeError::TooShort)?;
    let index_bytes = cur
        .take::<[u8; FORK_INDEX_SIZE]>()
        .map_err(|_| DecodeError::TooShort)?;

    let mut node_type = NodeType::empty();
    if matches!(version, Version::V02) && index_bytes.iter().any(|&b| b != 0) {
        node_type |= NodeType::EDGE;
    }

    let index = U256::from_le_slice(&index_bytes);
    let mut forks = BTreeMap::new();
    for b in 0..=u8::MAX {
        if index.bit(usize::from(b)) {
            forks.insert(b, parse_fork::<R>(cur, version, b, total)?);
        }
    }

    Ok(Node {
        node_type,
        entry,
        forks,
        ..Default::default()
    })
}

/// Consume one fork from the cursor.
///
/// The fork header is peeked to learn the body width (including any metadata)
/// before the body is consumed, so every availability check precedes prefix
/// and metadata parsing. v0.1 carries no metadata: its fork body is always the
/// pre-reference region plus one reference.
#[allow(clippy::arithmetic_side_effects)] // fork widths sum header constants, R::SIZE (<= 64) and a u16-bounded metadata length; the running cursor never exceeds the buffer
fn parse_fork<R: Reference>(
    cur: &mut Cursor<'_>,
    version: Version,
    byte_index: u8,
    total: usize,
) -> DecodeResult<Fork<R>> {
    let mut peek = cur.clone();
    let node_type = NodeType::from_bits_truncate(
        peek.take::<u8>()
            .map_err(|u| insufficient_fork(u, total, byte_index))?,
    );
    let has_metadata = matches!(version, Version::V02) && node_type.contains(NodeType::METADATA);

    let body_size = if has_metadata {
        // The metadata length field follows the pre-reference region and the
        // reference; skip past them (node_type is already consumed) to read it.
        peek.take_slice(ForkHeader::PRE_REFERENCE_SIZE - size_of::<u8>() + R::SIZE)
            .map_err(|u| insufficient_fork(u, total, byte_index))?;
        let metadata_len = peek
            .take::<MetadataLen>()
            .map_err(|u| insufficient_fork(u, total, byte_index))?
            .get();
        ForkHeader::PRE_REFERENCE_SIZE + R::SIZE + ForkHeader::METADATA_LEN_SIZE + metadata_len
    } else {
        ForkHeader::PRE_REFERENCE_SIZE + R::SIZE
    };

    let body = cur
        .take_slice(body_size)
        .map_err(|u| insufficient_fork(u, total, byte_index))?;
    parse_fork_body::<R>(body, has_metadata)
}

/// Parse a complete, correctly sized fork body: header, full-width reference,
/// and optional metadata.
///
/// The whole reference slot is retained: the encrypted width carries the
/// child's address and decryption key, so nothing is truncated on decode.
fn parse_fork_body<R: Reference>(body: &[u8], has_metadata: bool) -> DecodeResult<Fork<R>> {
    let mut cur = Cursor::new(body);
    let ForkHeader { node_type, prefix } = cur.take::<ForkHeader>()?;

    let ref_region = cur.take_slice(R::SIZE).map_err(|_| DecodeError::TooShort)?;
    // The slice is exactly R::SIZE bytes, so the constructor cannot fail.
    let reference = R::from_wire_bytes(ref_region).ok_or(DecodeError::TooShort)?;

    let mut node = Node::from_reference(reference);
    node.node_type = node_type;

    if has_metadata {
        let metadata_len = cur
            .take::<MetadataLen>()
            .map_err(|_| DecodeError::TooShort)?;
        if metadata_len.get() > 0 {
            node.metadata = serde_json::from_slice(cur.finish())?;
        }
    }

    Ok(Fork { prefix, node })
}

/// A fork in wire-record form: the fields the fork layout emits, with the child
/// reference already resolved so emission is total.
///
/// Construction is the sole fallible step: a fork whose child was never
/// persisted has no reference, and oversized metadata cannot be sized into the
/// `u16` length field. Once built, [`emit`](Self::emit) cannot produce a
/// misaligned image. bee-spec node.md: fork layout is node_type, prefix_len, a
/// 30-byte prefix region, the full-width reference, then optional metadata.
struct WireFork<'a, R: Reference> {
    node_type: NodeType,
    prefix: &'a Prefix,
    /// Child reference; mandatory by construction, emitted at its full wire
    /// width so an encrypted child's decryption key is written, not zeroed.
    reference: &'a R,
    /// Length-prefixed, padded metadata payload, present only when the child
    /// carries metadata.
    metadata: Option<WireMetadata>,
}

/// A fork's metadata payload, serialized and padded to the obfuscation-key
/// stride with its `u16` length precomputed so emission stays total.
struct WireMetadata {
    len: MetadataLen,
    padded_json: Vec<u8>,
}

impl WireMetadata {
    /// Serialize, pad to the `ObfuscationKey::SIZE` stride, and size the length
    /// field. Padding fills with `0x0a`, so the trailing bytes are JSON
    /// whitespace the decoder re-parses transparently.
    #[allow(clippy::arithmetic_side_effects)] // padding math is guarded (`SIZE - x` only when `x < SIZE`, `SIZE - rem` only when `rem != 0`) and `% ObfuscationKey::SIZE` has a nonzero constant divisor
    fn build(metadata: &BTreeMap<String, String>) -> Result<Self> {
        let mut padded_json = serde_json::to_string(metadata)
            .map_err(MantarayError::Metadata)?
            .into_bytes();

        let size_with_header = padded_json.len() + ForkHeader::METADATA_LEN_SIZE;
        let padding = if size_with_header < ObfuscationKey::SIZE {
            ObfuscationKey::SIZE - size_with_header
        } else if size_with_header > ObfuscationKey::SIZE {
            let rem = size_with_header % ObfuscationKey::SIZE;
            if rem == 0 {
                0
            } else {
                ObfuscationKey::SIZE - rem
            }
        } else {
            0
        };
        padded_json.resize(padded_json.len() + padding, 0x0a);

        let len =
            u16::try_from(padded_json.len()).map_err(|_| MantarayError::MetadataTooLarge {
                max: usize::from(u16::MAX),
                actual: padded_json.len(),
            })?;
        Ok(Self {
            len: MetadataLen(len),
            padded_json,
        })
    }
}

impl<'a, R: Reference> TryFrom<&'a Fork<R>> for WireFork<'a, R> {
    type Error = MantarayError;

    /// Resolve a fork into an emittable record. A child without a saved
    /// reference cannot be encoded into a decodable stream, so it is rejected
    /// here, before any bytes are written.
    fn try_from(fork: &'a Fork<R>) -> Result<Self> {
        let reference = fork
            .node
            .reference()
            .ok_or(MantarayError::MissingReference)?;
        let metadata = if fork.node.is_with_metadata() {
            Some(WireMetadata::build(&fork.node.metadata)?)
        } else {
            None
        };
        Ok(Self {
            node_type: fork.node.node_type,
            prefix: &fork.prefix,
            reference,
            metadata,
        })
    }
}

impl<R: Reference> WireFork<'_, R> {
    /// Emit this fork: node_type, the prefix record, the full-width
    /// reference, then any metadata. Total by construction.
    fn emit(&self, w: &mut Writer<'_>) {
        w.put(&self.node_type.bits());
        w.put(self.prefix);

        w.put(self.reference.to_bytes().as_slice());

        if let Some(metadata) = &self.metadata {
            w.put(&metadata.len);
            w.put(metadata.padded_json.as_slice());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::hex;
    use nectar_primitives::chunk::{ChunkAddress, ChunkRef};
    use proptest::prelude::*;
    use proptest_arbitrary_interop::arb;

    const ENCODED_V01: &str = "52fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64950ac787fbce1061870e8d34e0a638bc7e812c7ca4ebd31d626a572ba47b06f6952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072102654f163f5f0fa0621d729566c74d10037c4d7bbb0407d1e2c64950fcd3072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64950f89d6640e3044f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64850ff9f642182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64b50fc98072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64a50ff99622182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64d";
    const ENCODED_V02: &str = "52fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64905954fb18659339d0b25e0fb9723d3cd5d528fb3c8d495fd157bd7b7a210496952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072102654f163f5f0fa0621d729566c74d10037c4d7bbb0407d1e2c64940fcd3072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952e3872548ec012a6e123b60f9177017fb12e57732621d2c1ada267adbe8cc4350f89d6640e3044f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64850ff9f642182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64b50fc98072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64a50ff99622182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64d";

    #[derive(Clone, Default)]
    struct TestEntry {
        path: String,
        metadata: BTreeMap<String, String>,
    }

    fn test_entries() -> [TestEntry; 5] {
        [
            TestEntry {
                path: "/".to_string(),
                metadata: serde_json::from_str(r#"{"index-document": "aaaaa"}"#).unwrap(),
            },
            TestEntry {
                path: "aaaaa".to_string(),
                ..Default::default()
            },
            TestEntry {
                path: "cc".to_string(),
                ..Default::default()
            },
            TestEntry {
                path: "d".to_string(),
                ..Default::default()
            },
            TestEntry {
                path: "ee".to_string(),
                ..Default::default()
            },
        ]
    }

    #[test]
    fn decode_v01() {
        let data = hex::decode(ENCODED_V01).unwrap();
        let n = Node::<ChunkRef>::decode(data.as_slice()).unwrap();

        let mut expect_bytes = hex::decode(&ENCODED_V01[128..192]).unwrap();
        xor_in_place(&mut expect_bytes, n.obfuscation_key().as_bytes());

        // Root entry bytes are all zeros after decryption → None (no entry).
        if expect_bytes.iter().all(|&b| b == 0) {
            assert!(n.entry().is_none());
        } else {
            assert_eq!(n.entry().unwrap().address().as_bytes(), &expect_bytes[..]);
        }
        assert_eq!(test_entries().len(), n.forks().len());

        for entry in test_entries() {
            let key = entry.path.as_bytes()[0];
            assert!(n.forks().contains_key(&key));
            assert_eq!(n.forks()[&key].prefix(), entry.path.as_bytes());
        }
    }

    #[test]
    fn decode_v02() {
        let data = hex::decode(ENCODED_V02).unwrap();
        let n = Node::<ChunkRef>::decode(data.as_slice()).unwrap();

        let mut expect_bytes = hex::decode(&ENCODED_V02[128..192]).unwrap();
        xor_in_place(&mut expect_bytes, n.obfuscation_key().as_bytes());

        // Root entry bytes are all zeros after decryption → None (no entry).
        if expect_bytes.iter().all(|&b| b == 0) {
            assert!(n.entry().is_none());
        } else {
            assert_eq!(n.entry().unwrap().address().as_bytes(), &expect_bytes[..]);
        }
        assert_eq!(test_entries().len(), n.forks().len());

        for entry in test_entries() {
            let key = entry.path.as_bytes()[0];
            assert!(n.forks().contains_key(&key));
            assert_eq!(n.forks()[&key].prefix(), entry.path.as_bytes());

            if !entry.metadata.is_empty() {
                assert_eq!(n.forks()[&key].node().metadata(), &entry.metadata);
            }
        }
    }

    #[test]
    fn decode_nil_input() {
        let result = Node::<ChunkRef>::decode([].as_slice());
        assert!(matches!(result, Err(DecodeError::TooShort)));
    }

    #[test]
    fn decode_too_short_for_header() {
        let data = vec![0u8; NodeHeader::SIZE - 1];
        let result = Node::<ChunkRef>::decode(data.as_slice());
        assert!(matches!(result, Err(DecodeError::TooShort)));
    }

    #[test]
    fn decode_invalid_version_hash() {
        let data = vec![0u8; NodeHeader::SIZE];
        let result = Node::<ChunkRef>::decode(data.as_slice());
        assert!(matches!(result, Err(DecodeError::InvalidVersionHash)));
    }

    /// Test vector: valid manifest with correct metadata size (93 bytes).
    /// This is a v0.2 manifest with zero obfuscation key, a single fork at '/',
    /// and website-index-document metadata.
    #[test]
    fn decode_valid_manifest_from_go() {
        let data = hex::decode(
            "00000000000000000000000000000000000000000000000000000000000000005768b3b6a7db56d21d1abff40d41cebfc83448fed8d7e9b06ec0d3b073f28f200000000000000000000000000000000000000000000000000000000000000000000000000080000000000000000000000000000000000000000000000000000016012f0000000000000000000000000000000000000000000000000000000000e87f95c3d081c4fede769b6c69e27b435e525cbd25c6715c607e7c531e329639005d7b22776562736974652d696e6465782d646f63756d656e74223a2233356561656538316262363338303436393965633637316265323736326465626665346662643330636461646139303232393239646131613965366134366436227d0a"
        ).unwrap();
        assert!(Node::<ChunkRef>::decode(data.as_slice()).is_ok());
    }

    /// Test vector: metadata size field says 89 but actual content needs 93.
    /// Should fail because there aren't enough bytes for the declared metadata.
    #[test]
    fn decode_invalid_manifest_size_89() {
        let data = hex::decode(
            "00000000000000000000000000000000000000000000000000000000000000005768b3b6a7db56d21d1abff40d41cebfc83448fed8d7e9b06ec0d3b073f28f200000000000000000000000000000000000000000000000000000000000000000000000000080000000000000000000000000000000000000000000000000000016012f0000000000000000000000000000000000000000000000000000000000e87f95c3d081c4fede769b6c69e27b435e525cbd25c6715c607e7c531e32963900597b22776562736974652d696e6465782d646f63756d656e74223a2233356561656538316262363338303436393965633637316265323736326465626665346662643330636461646139303232393239646131613965366134366436227d0a"
        ).unwrap();
        assert!(Node::<ChunkRef>::decode(data.as_slice()).is_err());
    }

    /// Test vector: metadata size field says 95 but actual content is 93.
    /// Should fail because the size exceeds available bytes.
    #[test]
    fn decode_invalid_manifest_size_95() {
        let data = hex::decode(
            "00000000000000000000000000000000000000000000000000000000000000005768b3b6a7db56d21d1abff40d41cebfc83448fed8d7e9b06ec0d3b073f28f200000000000000000000000000000000000000000000000000000000000000000000000000080000000000000000000000000000000000000000000000000000016012f0000000000000000000000000000000000000000000000000000000000e87f95c3d081c4fede769b6c69e27b435e525cbd25c6715c607e7c531e329639005f7b22776562736974652d696e6465782d646f63756d656e74223a2233356561656538316262363338303436393965633637316265323736326465626665346662643330636461646139303232393239646131613965366134366436227d0a"
        ).unwrap();
        assert!(Node::<ChunkRef>::decode(data.as_slice()).is_err());
    }

    /// Test vector: metadata size field says 96 but actual content is 93.
    /// Should fail because the size exceeds available bytes.
    #[test]
    fn decode_invalid_manifest_size_96() {
        let data = hex::decode(
            "00000000000000000000000000000000000000000000000000000000000000005768b3b6a7db56d21d1abff40d41cebfc83448fed8d7e9b06ec0d3b073f28f200000000000000000000000000000000000000000000000000000000000000000000000000080000000000000000000000000000000000000000000000000000016012f0000000000000000000000000000000000000000000000000000000000e87f95c3d081c4fede769b6c69e27b435e525cbd25c6715c607e7c531e32963900607b22776562736974652d696e6465782d646f63756d656e74223a2233356561656538316262363338303436393965633637316265323736326465626665346662643330636461646139303232393239646131613965366134366436227d0a"
        ).unwrap();
        assert!(Node::<ChunkRef>::decode(data.as_slice()).is_err());
    }

    /// BEE-WORKAROUND(bee#5483): bee occasionally emits nodes with
    /// `ref_size = 0` for entry-less terminal nodes (mantaray-js FIXME:
    /// "in Bee, if one uploads a file on the bzz endpoint, the node under
    /// `/` gets 0 refsize"). Tolerate this wire shape only when the forks
    /// bitfield is also empty.
    #[test]
    fn decode_bee_legacy_ref_size_zero_empty_node() {
        // v0.2 layout: 32 obfuscation key zeros || 31 version hash || ref_size=0 || 32 index zeros = 96 bytes
        let mut data = vec![0u8; 96];
        data[ObfuscationKey::SIZE..ObfuscationKey::SIZE + Version::SIZE]
            .copy_from_slice(Version::V02.as_bytes());
        // ref_size at offset 63 is left as 0; index (offset 64..96) is all zero.

        let n = Node::<ChunkRef>::decode(data.as_slice())
            .expect("ref_size=0 with empty forks should decode as terminal node");
        assert!(n.entry().is_none());
        assert!(n.forks().is_empty());
    }

    /// BEE-WORKAROUND(bee#5483): a `ref_size = 0` node with a non-empty forks
    /// bitfield is unrecoverable by any reference implementation (fork refs
    /// would have zero width). Reject as malformed rather than silently
    /// dropping forks.
    #[test]
    fn decode_bee_legacy_ref_size_zero_with_forks_is_rejected() {
        let mut data = vec![0u8; 96];
        data[ObfuscationKey::SIZE..ObfuscationKey::SIZE + Version::SIZE]
            .copy_from_slice(Version::V02.as_bytes());
        // ref_size = 0 (offset 63 already zero), but flip one bit in the index.
        data[NodeHeader::SIZE] = 0x01;

        let result = Node::<ChunkRef>::decode(data.as_slice());
        assert!(matches!(
            result,
            Err(DecodeError::RefSizeMismatch {
                expected: 32,
                actual: 0
            })
        ));
    }

    /// BEE-WORKAROUND(bee#5483): same as above but for v0.1; both decoders
    /// must apply the same rule.
    #[test]
    fn decode_bee_legacy_ref_size_zero_v01_empty_node() {
        let mut data = vec![0u8; 96];
        data[ObfuscationKey::SIZE..ObfuscationKey::SIZE + Version::SIZE]
            .copy_from_slice(Version::V01.as_bytes());

        let n = Node::<ChunkRef>::decode(data.as_slice())
            .expect("v0.1 ref_size=0 with empty forks should decode as terminal node");
        assert!(n.entry().is_none());
        assert!(n.forks().is_empty());
    }

    /// Pin nectar's encoder behaviour: even for an entry-less node, it must
    /// emit `ref_size = R::SIZE`, never `0`. Spec-correct, matches bee's
    /// "valid manifest" test fixture, matches mantaray-js. Emitting 0 would
    /// reproduce the bee bug rather than fix it.
    #[test]
    fn encoder_never_emits_ref_size_zero_for_entryless_node() {
        let n = Node::<ChunkRef>::new_unencrypted();
        let encoded = n.encode().unwrap();

        // Decrypt (obfuscation key is all-zero for `new_unencrypted`, so XOR
        // is a no-op, but go through the motions for clarity).
        let mut decoded = encoded;
        let key = decoded[..ObfuscationKey::SIZE].to_vec();
        xor_in_place(&mut decoded[ObfuscationKey::SIZE..], &key);

        assert_eq!(
            usize::from(decoded[NodeHeader::REF_SIZE_OFFSET]),
            <ChunkRef as Reference>::SIZE,
            "encoder must emit ref_size = R::SIZE, not 0; spec requires uniform reference width"
        );
    }

    /// Build a header-only prefix of a node: zero obfuscation key (XOR is
    /// identity), the given raw version hash, `ref_size = 32`, then zero
    /// padding up to `len` bytes.
    fn truncated_node_bytes(version: &Version, len: usize) -> Vec<u8> {
        assert!(len >= NodeHeader::SIZE);
        let mut data = vec![0u8; len];
        data[NodeHeader::VERSION_HASH_OFFSET..NodeHeader::VERSION_HASH_OFFSET + Version::SIZE]
            .copy_from_slice(version.as_bytes());
        data[NodeHeader::REF_SIZE_OFFSET] = 32;
        data
    }

    /// Regression: a 64-byte input (header only, `ref_size = 32`) used to
    /// panic slicing the absent entry slot (`data[64..96]`). It must return
    /// `Err`, never panic. Exact minimal crash case.
    #[test]
    fn decode_v01_header_only_64_bytes_returns_err() {
        let data = truncated_node_bytes(&Version::V01, NodeHeader::SIZE);
        let result = Node::<ChunkRef>::decode(data.as_slice());
        assert!(matches!(result, Err(DecodeError::TooShort)));
    }

    /// Regression: same minimal 64-byte crash case for the v0.2 decoder.
    #[test]
    fn decode_v02_header_only_64_bytes_returns_err() {
        let data = truncated_node_bytes(&Version::V02, NodeHeader::SIZE);
        let result = Node::<ChunkRef>::decode(data.as_slice());
        assert!(matches!(result, Err(DecodeError::TooShort)));
    }

    /// Regression: every length in 64..128 used to panic in `decode_v01`,
    /// either slicing the entry slot (`data[64..96]`, lengths 64..96) or the
    /// forks index (`data[96..128]`, lengths 96..128). All must return `Err`.
    #[test]
    fn decode_v01_truncated_lengths_return_err() {
        for len in NodeHeader::SIZE..NodeHeader::SIZE + 32 + 32 {
            let data = truncated_node_bytes(&Version::V01, len);
            let result = Node::<ChunkRef>::decode(data.as_slice());
            assert!(
                matches!(result, Err(DecodeError::TooShort)),
                "length {len} must yield TooShort"
            );
        }
    }

    /// Regression: same truncated-length sweep for the v0.2 decoder, which
    /// additionally read the forks index twice (edge-type deduction).
    #[test]
    fn decode_v02_truncated_lengths_return_err() {
        for len in NodeHeader::SIZE..NodeHeader::SIZE + 32 + 32 {
            let data = truncated_node_bytes(&Version::V02, len);
            let result = Node::<ChunkRef>::decode(data.as_slice());
            assert!(
                matches!(result, Err(DecodeError::TooShort)),
                "length {len} must yield TooShort"
            );
        }
    }

    /// A 128-byte input (header + entry + index) whose index demands a fork
    /// ref that is not present must hit the existing guarded error path.
    #[test]
    fn decode_v01_index_demands_missing_fork_returns_err() {
        let mut data = truncated_node_bytes(&Version::V01, NodeHeader::SIZE + 32 + 32);
        // Set bit 0 of the forks index (LE bitfield at offset 96).
        data[NodeHeader::SIZE + 32] = 0x01;
        let result = Node::<ChunkRef>::decode(data.as_slice());
        assert!(matches!(
            result,
            Err(DecodeError::InsufficientForkBytes { .. })
        ));
    }

    /// Same as above for the v0.2 decoder.
    #[test]
    fn decode_v02_index_demands_missing_fork_returns_err() {
        let mut data = truncated_node_bytes(&Version::V02, NodeHeader::SIZE + 32 + 32);
        data[NodeHeader::SIZE + 32] = 0x01;
        let result = Node::<ChunkRef>::decode(data.as_slice());
        assert!(matches!(
            result,
            Err(DecodeError::InsufficientForkBytes { .. })
        ));
    }

    /// Same as `truncated_node_bytes` but with `ref_size = 64`, the width of
    /// the `EncryptedChunkRef` entry, so the 64-byte slicing arithmetic in
    /// `decode_v01`/`decode_v02` is exercised on stable (the fuzz target
    /// drives this width too, but only under libfuzzer mutation on nightly).
    fn truncated_encref_node_bytes(version: &Version, len: usize) -> Vec<u8> {
        assert!(len >= NodeHeader::SIZE);
        let mut data = vec![0u8; len];
        data[NodeHeader::VERSION_HASH_OFFSET..NodeHeader::VERSION_HASH_OFFSET + Version::SIZE]
            .copy_from_slice(version.as_bytes());
        data[NodeHeader::REF_SIZE_OFFSET] = 64;
        data
    }

    /// Regression for the 64-byte entry width: with `ref_size = 64` the entry
    /// slot is `data[64..128]` and the forks index `data[128..160]`, so every
    /// length below 160 must return `Err` (the PR bounds check, exercised for
    /// the encrypted-ref path), never panic.
    #[test]
    fn decode_encref_truncated_lengths_return_err() {
        use nectar_primitives::EncryptedChunkRef;
        for (label, version) in [("v01", Version::V01), ("v02", Version::V02)] {
            for len in NodeHeader::SIZE..NodeHeader::SIZE + 64 + 32 {
                let data = truncated_encref_node_bytes(&version, len);
                let result = Node::<EncryptedChunkRef>::decode(data.as_slice());
                assert!(
                    matches!(result, Err(DecodeError::TooShort)),
                    "encref {label} length {len} must yield TooShort"
                );
            }
        }
    }

    /// A full 160-byte encrypted-ref node whose index demands a fork ref that
    /// is not present must hit the guarded error path, not panic.
    #[test]
    fn decode_encref_index_demands_missing_fork_returns_err() {
        use nectar_primitives::EncryptedChunkRef;
        for (label, version) in [("v01", Version::V01), ("v02", Version::V02)] {
            let mut data = truncated_encref_node_bytes(&version, NodeHeader::SIZE + 64 + 32);
            // Set bit 0 of the forks index (LE bitfield at offset 128).
            data[NodeHeader::SIZE + 64] = 0x01;
            let result = Node::<EncryptedChunkRef>::decode(data.as_slice());
            assert!(
                matches!(result, Err(DecodeError::InsufficientForkBytes { .. })),
                "encref {label} missing fork body must yield InsufficientForkBytes"
            );
        }
    }

    /// Replay the committed seed corpus of the `mantaray_node_decode` fuzz
    /// target through the shared `node_decode` oracle the fuzzer drives
    /// (`Node::<ChunkRef>` for 32-byte plain entries and
    /// `Node::<EncryptedChunkRef>` for 64-byte entries).
    /// The oracle is "no panic";
    /// `Err` is an acceptable outcome for any seed. Seed intent is pinned by
    /// name: `crash-*` must stay `Err` at both widths, `valid-encrypted-*`
    /// must decode at the `EncryptedChunkRef` width and every other `valid-*`
    /// at the `ChunkRef` width. The per-width decode counts are what stop a
    /// width from passing while every seed silently skips it.
    ///
    /// This keeps the fuzz seeds meaningful on stable without running the
    /// fuzzer itself.
    #[test]
    fn seed_replay_mantaray_node_decode() {
        let plain_decoded = core::cell::Cell::new(0usize);
        let wide_decoded = core::cell::Cell::new(0usize);
        nectar_testing::SeedReplay::corpus(env!("CARGO_MANIFEST_DIR"), "mantaray_node_decode")
            .each(|_, data| {
                // The fuzz oracle: must not panic. The shared oracle drives
                // both entry widths, exactly as the fuzz target does.
                let (plain, wide) = crate::oracles::node_decode(data);
                if plain.is_ok() {
                    plain_decoded.set(plain_decoded.get() + 1);
                }
                if wide.is_ok() {
                    wide_decoded.set(wide_decoded.get() + 1);
                }
            })
            .on("crash-", |name, data| {
                let (plain, wide) = crate::oracles::node_decode(data);
                assert!(
                    plain.is_err() && wide.is_err(),
                    "seed {name} must remain an Err reproducer at both widths"
                );
            })
            .on("valid-encrypted-", |name, data| {
                assert!(
                    crate::oracles::node_decode(data).1.is_ok(),
                    "seed {name} must decode at the EncryptedChunkRef width"
                );
            })
            .on("valid-", |name, data| {
                assert!(
                    crate::oracles::node_decode(data).0.is_ok(),
                    "seed {name} must decode at the ChunkRef width"
                );
            })
            .floor(5)
            .run();
        assert!(
            plain_decoded.get() >= 2,
            "expected the v0.1 and v0.2 manifests to decode at the ChunkRef width, decoded {}",
            plain_decoded.get()
        );
        assert!(
            wide_decoded.get() >= 2,
            "expected the ref_size=64 seeds (empty and keyed fork) to decode at the EncryptedChunkRef width, decoded {}",
            wide_decoded.get()
        );
    }

    /// Replay the `mantaray_record_roundtrip` seed corpus through the shared
    /// `record_round_trip` oracle the fuzz target runs, at both widths.
    /// The corpus carries a v0.1 and a v0.2 plain manifest plus two `ref_size`
    /// 64 encrypted cases (an empty node and a keyed fork), so this pins
    /// record round-tripping across both wire versions and both widths on
    /// stable, without running the fuzzer. Each width must actually decode at
    /// least the seeds it claims, not merely be skipped: the two plain
    /// manifests at the `ChunkRef` width and the encrypted cases at the
    /// `EncryptedChunkRef` width. Counting decodes is what keeps the
    /// fixed-point assertions from passing vacuously.
    #[test]
    fn seed_replay_mantaray_record_roundtrip() {
        let plain_decoded = core::cell::Cell::new(0usize);
        let wide_decoded = core::cell::Cell::new(0usize);
        nectar_testing::SeedReplay::corpus(env!("CARGO_MANIFEST_DIR"), "mantaray_record_roundtrip")
            .each(|name, data| {
                let plain = crate::oracles::record_round_trip::<ChunkRef>(data)
                    .unwrap_or_else(|v| panic!("seed {name}: {v}"));
                if plain {
                    plain_decoded.set(plain_decoded.get() + 1);
                }
                let wide =
                    crate::oracles::record_round_trip::<nectar_primitives::EncryptedChunkRef>(data)
                        .unwrap_or_else(|v| panic!("seed {name}: {v}"));
                if wide {
                    wide_decoded.set(wide_decoded.get() + 1);
                }
            })
            .covers("valid-")
            .floor(4)
            .run();
        assert!(
            plain_decoded.get() >= 2,
            "expected the v0.1 and v0.2 manifests to round-trip at the ChunkRef width, decoded {}",
            plain_decoded.get()
        );
        assert!(
            wide_decoded.get() >= 2,
            "expected the ref_size=64 seeds (empty and keyed fork) to decode at the EncryptedChunkRef width, decoded {}",
            wide_decoded.get()
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        /// Valid-by-construction nodes survive the shared `node_round_trip`
        /// oracle at the plain width; the property the
        /// `mantaray_node_roundtrip` fuzz target drives.
        #[test]
        fn node_encode_decode_round_trip(node in arb::<Node<ChunkRef>>()) {
            prop_assert_eq!(crate::oracles::node_round_trip(&node), Ok(()));
        }

        /// The encrypted width round-trips arbitrary full-width fork
        /// references, so nonzero decryption keys survive encode and decode.
        #[test]
        fn encrypted_node_encode_decode_round_trip(
            node in arb::<Node<nectar_primitives::EncryptedChunkRef>>(),
        ) {
            prop_assert_eq!(crate::oracles::node_round_trip(&node), Ok(()));
        }
    }

    /// Encoding a fork whose child has no saved reference must error rather
    /// than emit a truncated, undecodable stream.
    #[test]
    fn encode_fork_with_unsaved_child_errors() {
        let mut n = Node::<ChunkRef>::new_unencrypted();

        let path = b"aaaaa";
        let e = {
            let mut buf = [0u8; 32];
            buf[32 - path.len()..].copy_from_slice(path);
            ChunkRef::from(ChunkAddress::from(buf))
        };
        nectar_testing::run(n.add::<
            nectar_primitives::store::NullLoader,
            { nectar_primitives::bmt::DEFAULT_BODY_SIZE },
        >(
            path, Some(e), BTreeMap::new(), &nectar_primitives::store::NullLoader,
        ))
        .unwrap();

        // The fork's child node has no reference assigned (never persisted).
        let result = n.encode();
        assert!(matches!(result, Err(MantarayError::MissingReference)));
    }

    /// Build a leaf fork whose child is exactly what the single body decoder
    /// reconstructs: a referenced node with a node_type and optional metadata,
    /// no entry or children.
    fn leaf_fork(
        prefix: &[u8],
        addr_byte: u8,
        node_type: NodeType,
        metadata: BTreeMap<String, String>,
    ) -> Fork<ChunkRef> {
        let mut addr = [0u8; 32];
        addr[31] = addr_byte;
        let mut node = Node::from_reference(ChunkRef::from(ChunkAddress::from(addr)));
        node.node_type = node_type;
        node.metadata = metadata;
        Fork {
            prefix: Prefix::from_slice(prefix),
            node,
        }
    }

    /// Per-record property: parsing an emitted `WireFork` reproduces the fork.
    /// Misalignment is unrepresentable, so the only variation is the presence
    /// of metadata, exercised here alongside plain and value-typed forks.
    #[test]
    fn wire_fork_record_round_trips() {
        let mut website: BTreeMap<String, String> = BTreeMap::new();
        website.insert("index-document".to_string(), "aaaaa".to_string());

        let cases = [
            leaf_fork(b"aaaaa", 1, NodeType::VALUE, BTreeMap::new()),
            leaf_fork(b"cc", 2, NodeType::empty(), BTreeMap::new()),
            leaf_fork(b"/", 3, NodeType::VALUE | NodeType::METADATA, website),
        ];

        for fork in cases {
            let has_metadata = fork.node.is_with_metadata();
            let mut buf = Vec::new();
            WireFork::try_from(&fork)
                .unwrap()
                .emit(&mut Writer::new(&mut buf));

            let parsed = parse_fork_body::<ChunkRef>(&buf, has_metadata).unwrap();
            assert_eq!(parsed, fork, "parse(emit(fork)) must reproduce the fork");
        }
    }

    /// The fix this codec pins: an encrypted fork child's decryption key is
    /// written at its wire position and read back, not zero-padded away.
    #[test]
    fn encrypted_fork_reference_round_trips_key() {
        use nectar_primitives::{EncryptedChunkRef, EncryptionKey};

        let child_ref = EncryptedChunkRef::new(
            ChunkAddress::from([0xaa; 32]),
            EncryptionKey::from([0xbb; 32]),
        );
        let mut child = Node::<EncryptedChunkRef>::from_reference(child_ref.clone());
        child.node_type = NodeType::VALUE;

        let mut n = Node::<EncryptedChunkRef>::new_unencrypted();
        n.forks.insert(
            b'a',
            Fork {
                prefix: Prefix::from_slice(b"a"),
                node: child,
            },
        );

        let encoded = n.encode().unwrap();
        let decoded = Node::<EncryptedChunkRef>::decode(encoded.as_slice()).unwrap();
        assert_eq!(
            decoded.forks()[&b'a'].node().reference(),
            Some(&child_ref),
            "fork decryption key must survive encode and decode"
        );
    }

    /// Zero-key images (what earlier encoders emitted by zero-padding the key
    /// half) must keep decoding, the key read back as all-zero, and a zero-key
    /// reference must re-encode to the same zero-padded slot.
    #[test]
    fn zero_padded_fork_key_decodes_as_zero_key() {
        use nectar_primitives::{EncryptedChunkRef, EncryptionKey};

        let child_ref = EncryptedChunkRef::new(
            ChunkAddress::from([0xaa; 32]),
            EncryptionKey::from([0u8; 32]),
        );
        let mut child = Node::<EncryptedChunkRef>::from_reference(child_ref.clone());
        child.node_type = NodeType::VALUE;

        let mut n = Node::<EncryptedChunkRef>::new_unencrypted();
        n.forks.insert(
            b'a',
            Fork {
                prefix: Prefix::from_slice(b"a"),
                node: child,
            },
        );

        // The zero obfuscation key makes the XOR a no-op, so the image is the
        // cleartext layout: the fork record starts after the node header, the
        // 64-byte entry slot and the 32-byte forks index, and its reference
        // slot follows the 32-byte pre-reference region.
        let encoded = n.encode().unwrap();
        let ref_slot_offset = NodeHeader::SIZE
            + EncryptedChunkRef::SIZE
            + FORK_INDEX_SIZE
            + ForkHeader::PRE_REFERENCE_SIZE;
        let slot = encoded
            .get(ref_slot_offset..ref_slot_offset + EncryptedChunkRef::SIZE)
            .unwrap();
        assert_eq!(&slot[..32], &[0xaa; 32], "address half of the slot");
        assert_eq!(&slot[32..], &[0u8; 32], "key half stays zero-padded");

        let decoded = Node::<EncryptedChunkRef>::decode(encoded.as_slice()).unwrap();
        assert_eq!(
            decoded.forks()[&b'a'].node().reference(),
            Some(&child_ref),
            "a zero-padded key half must decode as an all-zero key"
        );
    }

    /// Encode-decode round-trip preserves entries and metadata.
    #[test]
    fn encode_decode_round_trip() {
        let mut n = Node::<ChunkRef>::new_unencrypted();

        for entry in test_entries() {
            let path = entry.path.as_bytes();
            let e = {
                let mut buf = [0u8; 32];
                let len = path.len().min(32);
                buf[32 - len..].copy_from_slice(&path[..len]);
                ChunkRef::from(ChunkAddress::from(buf))
            };
            nectar_testing::run(n.add::<
                nectar_primitives::store::NullLoader,
                { nectar_primitives::bmt::DEFAULT_BODY_SIZE },
            >(
                path, Some(e), entry.metadata, &nectar_primitives::store::NullLoader,
            ))
            .unwrap();
        }

        // assign deterministic references to forks so encoding works
        for (counter, fork) in n.forks.values_mut().enumerate() {
            let mut addr = [0u8; 32];
            #[allow(clippy::as_conversions)] // forks are keyed by u8, so counter <= 255
            let counter_byte = counter as u8;
            addr[31] = counter_byte;
            fork.node
                .mark_persisted(ChunkRef::from(ChunkAddress::from(addr)));
        }

        let encoded = n.encode().unwrap();
        let n2 = Node::<ChunkRef>::decode(encoded.as_slice()).unwrap();

        // Root has no entry; encoding writes zero bytes, decoding reads them back as None
        assert!(n2.entry().is_none());
        assert_eq!(n.forks().len(), n2.forks().len());

        for entry in test_entries() {
            let key = entry.path.as_bytes()[0];
            assert!(n2.forks().contains_key(&key));
            assert_eq!(n2.forks()[&key].prefix(), entry.path.as_bytes());
            if !entry.metadata.is_empty() {
                assert_eq!(n2.forks()[&key].node().metadata(), &entry.metadata);
            }
        }
    }
}
