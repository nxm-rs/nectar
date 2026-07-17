//! Binary encoding for mantaray nodes (v0.1 and v0.2).

use alloc::collections::BTreeMap;

use crate::error::{DecodeError, DecodeResult, MantarayError, Result};
use crate::node::{Fork, Node, NodeType, Prefix};
use crate::obfuscation::ObfuscationKey;

use alloy_primitives::{U256, hex};
use nectar_primitives::chunk::{ChunkAddress, Reference};
use nectar_primitives::wire::{Cursor, FromCursor, ToWriter, Underrun, Writer};

/// Mantaray wire format version (truncated keccak256, 31 bytes).
#[derive(Clone, Copy)]
enum VersionHash {
    V01,
    V02,
}

impl VersionHash {
    /// Wire size of a truncated version hash.
    const SIZE: usize = 31;

    const V01_BYTES: [u8; Self::SIZE] =
        hex!("025184789d63635766d78c41900196b57d7400875ebe4d9b5d1e76bd9652a9");
    const V02_BYTES: [u8; Self::SIZE] =
        hex!("5768b3b6a7db56d21d1abff40d41cebfc83448fed8d7e9b06ec0d3b073f28f");

    const fn as_bytes(&self) -> &[u8; Self::SIZE] {
        match self {
            Self::V01 => &Self::V01_BYTES,
            Self::V02 => &Self::V02_BYTES,
        }
    }

    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes == Self::V01_BYTES {
            Some(Self::V01)
        } else if bytes == Self::V02_BYTES {
            Some(Self::V02)
        } else {
            None
        }
    }
}

/// Wire layout descriptor for a serialised node header.
struct NodeHeader;

impl NodeHeader {
    const SIZE: usize = ObfuscationKey::SIZE + VersionHash::SIZE + size_of::<u8>();
    const VERSION_HASH_OFFSET: usize = ObfuscationKey::SIZE;
    const REF_SIZE_OFFSET: usize = ObfuscationKey::SIZE + VersionHash::SIZE;
}

/// A fork's fixed wire header: the node type byte, then the prefix record.
struct ForkHeader {
    node_type: NodeType,
    prefix: Prefix,
}

impl ForkHeader {
    /// Protocol anchor: total pre-reference bytes in a fork.
    const PRE_REFERENCE_SIZE: usize = 32;
    /// Size of the metadata length field.
    const METADATA_LEN_SIZE: usize = size_of::<u16>();
}

impl FromCursor for ForkHeader {
    type Error = DecodeError;

    fn take_from(cur: &mut Cursor<'_>) -> core::result::Result<Self, Self::Error> {
        let node_type = NodeType::from_bits_truncate(cur.take::<u8>()?);
        let prefix = cur.take::<Prefix>()?;
        Ok(Self { node_type, prefix })
    }
}

/// Length of a fork's metadata region, stored big-endian on the wire.
#[derive(Clone, Copy)]
struct MetadataLen(u16);

impl MetadataLen {
    /// The length in bytes.
    fn get(self) -> usize {
        usize::from(self.0)
    }
}

impl FromCursor for MetadataLen {
    type Error = Underrun;

    fn take_from(cur: &mut Cursor<'_>) -> core::result::Result<Self, Underrun> {
        cur.take::<[u8; ForkHeader::METADATA_LEN_SIZE]>()
            .map(|bytes| Self(u16::from_be_bytes(bytes)))
    }
}

/// Writes the length big-endian, the mirror of the `FromCursor` impl above;
/// the byte order never leaves this type.
impl ToWriter for MetadataLen {
    fn put_into(&self, w: &mut Writer<'_>) {
        w.put(&self.0.to_be_bytes());
    }
}

/// Size of the 256-bit forks presence bitfield following the entry slot.
const FORK_INDEX_SIZE: usize = size_of::<U256>();

// Compile-time layout assertions.
const _: () = assert!(NodeHeader::SIZE == 64);
const _: () = assert!(ForkHeader::PRE_REFERENCE_SIZE == 32);
// node_type byte + prefix length byte + padded prefix block fill the
// pre-reference region exactly.
const _: () = assert!(2 * size_of::<u8>() + Prefix::MAX_LEN == ForkHeader::PRE_REFERENCE_SIZE);
const _: () = assert!(ObfuscationKey::SIZE == 32);
const _: () = assert!(NodeHeader::VERSION_HASH_OFFSET == ObfuscationKey::SIZE);
const _: () = assert!(NodeHeader::REF_SIZE_OFFSET == NodeHeader::SIZE - size_of::<u8>());
const _: () = assert!(FORK_INDEX_SIZE == 32);

#[cfg(test)]
const VERSION_HASH_01_BYTES: [u8; 32] =
    hex!("025184789d63635766d78c41900196b57d7400875ebe4d9b5d1e76bd9652a9b7");
#[cfg(test)]
const VERSION_HASH_02_BYTES: [u8; 32] =
    hex!("5768b3b6a7db56d21d1abff40d41cebfc83448fed8d7e9b06ec0d3b073f28f7b");

#[cfg(test)]
const VERSION_STRING_01: &str = "mantaray:0.1";
#[cfg(test)]
const VERSION_STRING_02: &str = "mantaray:0.2";

/// XOR `data` in place with a repeating `key`.
fn xor_in_place(data: &mut [u8], key: &[u8]) {
    for (byte, mask) in data.iter_mut().zip(key.iter().cycle()) {
        *byte ^= *mask;
    }
}

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
    data.extend_from_slice(VersionHash::V02.as_bytes());
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
    data.extend_from_slice(&index.to_le_bytes::<32>());

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
fn parse_header(cur: &mut Cursor<'_>) -> DecodeResult<(VersionHash, RefSize)> {
    // The obfuscation key was already consumed by the caller for decryption.
    cur.take::<[u8; ObfuscationKey::SIZE]>()
        .map_err(|_| DecodeError::TooShort)?;
    let version_bytes = cur
        .take::<[u8; VersionHash::SIZE]>()
        .map_err(|_| DecodeError::TooShort)?;
    let ref_size = cur.take::<u8>().map_err(|_| DecodeError::TooShort)?;
    let version = VersionHash::from_bytes(&version_bytes).ok_or(DecodeError::InvalidVersionHash)?;
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
/// dropping forks the way bee's v0.2 decoder does
/// (`bee/pkg/manifest/mantaray/marshal.go:285-287`). See the HAZMAT block.
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
    version: VersionHash,
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
    // reports `DataTooShort` rather than an entry-shaped error.
    let entry = R::read_optional(cur).map_err(|_| DecodeError::TooShort)?;
    let index_bytes = cur
        .take::<[u8; FORK_INDEX_SIZE]>()
        .map_err(|_| DecodeError::TooShort)?;

    let mut node_type = NodeType::empty();
    if matches!(version, VersionHash::V02) && index_bytes.iter().any(|&b| b != 0) {
        node_type |= NodeType::EDGE;
    }

    let index = U256::from_le_slice(&index_bytes);
    let mut forks = BTreeMap::new();
    for b in 0..=u8::MAX {
        if index.bit(usize::from(b)) {
            forks.insert(b, parse_fork::<R>(cur, version, ref_size, b, total)?);
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
    version: VersionHash,
    ref_size: usize,
    byte_index: u8,
    total: usize,
) -> DecodeResult<Fork<R>> {
    let mut peek = cur.clone();
    let node_type = NodeType::from_bits_truncate(
        peek.take::<u8>()
            .map_err(|u| insufficient_fork(u, total, byte_index))?,
    );
    let has_metadata =
        matches!(version, VersionHash::V02) && node_type.contains(NodeType::METADATA);

    let body_size = if has_metadata {
        // The metadata length field follows the pre-reference region and the
        // reference; skip past them (node_type is already consumed) to read it.
        peek.take_slice(ForkHeader::PRE_REFERENCE_SIZE - size_of::<u8>() + ref_size)
            .map_err(|u| insufficient_fork(u, total, byte_index))?;
        let metadata_len = peek
            .take::<MetadataLen>()
            .map_err(|u| insufficient_fork(u, total, byte_index))?
            .get();
        ForkHeader::PRE_REFERENCE_SIZE + ref_size + ForkHeader::METADATA_LEN_SIZE + metadata_len
    } else {
        ForkHeader::PRE_REFERENCE_SIZE + ref_size
    };

    let body = cur
        .take_slice(body_size)
        .map_err(|u| insufficient_fork(u, total, byte_index))?;
    parse_fork_body::<R>(body, ref_size, has_metadata)
}

/// Parse a complete, correctly sized fork body: header, reference (address
/// only), and optional metadata.
///
/// Only the first 32 bytes of the reference slot are retained: a fork child is
/// addressed by its chunk address, so the encryption-key half of a 64-byte
/// reference is dropped.
fn parse_fork_body<R: Reference>(
    body: &[u8],
    ref_size: usize,
    has_metadata: bool,
) -> DecodeResult<Fork<R>> {
    let mut cur = Cursor::new(body);
    let ForkHeader { node_type, prefix } = cur.take::<ForkHeader>()?;

    let ref_region = cur
        .take_slice(ref_size)
        .map_err(|_| DecodeError::TooShort)?;
    let mut ref_cur = Cursor::new(ref_region);
    let addr = ref_cur
        .take::<[u8; 32]>()
        .map_err(|_| DecodeError::TooShort)?;

    let mut node = Node::from_reference(ChunkAddress::from(addr));
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
/// 30-byte prefix region, the reference, then optional metadata.
struct WireFork<'a> {
    node_type: NodeType,
    prefix: &'a Prefix,
    /// Child chunk address; mandatory by construction, filling the reference
    /// slot within the fork's pre-reference region.
    address: &'a ChunkAddress,
    /// Uniform reference width; the 32-byte address is right-padded with zeros
    /// to it (encrypted mode carries a 64-byte reference slot).
    ref_size: usize,
    /// Length-prefixed, padded metadata payload, present only when the child
    /// carries metadata.
    metadata: Option<WireMetadata>,
}

/// A fork's metadata payload, serialised and padded to the obfuscation-key
/// stride with its `u16` length precomputed so emission stays total.
struct WireMetadata {
    len: MetadataLen,
    padded_json: Vec<u8>,
}

impl WireMetadata {
    /// Serialise, pad to the `ObfuscationKey::SIZE` stride, and size the length
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

impl<'a, R: Reference> TryFrom<&'a Fork<R>> for WireFork<'a> {
    type Error = MantarayError;

    /// Resolve a fork into an emittable record. A child without a saved
    /// reference cannot be encoded into a decodable stream, so it is rejected
    /// here, before any bytes are written.
    fn try_from(fork: &'a Fork<R>) -> Result<Self> {
        let address = fork
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
            address,
            ref_size: R::SIZE,
            metadata,
        })
    }
}

impl WireFork<'_> {
    /// Emit this fork: node_type, the prefix record, the reference padded to
    /// `ref_size`, then any metadata. Total by construction.
    fn emit(&self, w: &mut Writer<'_>) {
        w.put(&self.node_type.bits());
        w.put(self.prefix);

        w.put(self.address.as_bytes());
        w.put_zeros(self.ref_size.saturating_sub(32));

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
    use alloy_primitives::utils::keccak256;
    use nectar_primitives::chunk::ChunkRef;

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
    fn version_hash_01() {
        assert_eq!(
            keccak256(VERSION_STRING_01.as_bytes()),
            VERSION_HASH_01_BYTES,
        );
    }

    #[test]
    fn version_hash_02() {
        assert_eq!(
            keccak256(VERSION_STRING_02.as_bytes()),
            VERSION_HASH_02_BYTES,
        );
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
    fn fork_header_take_consumes_the_pre_reference_region() {
        let mut wire = vec![NodeType::VALUE.bits(), 2];
        wire.extend_from_slice(b"ab");
        wire.resize(ForkHeader::PRE_REFERENCE_SIZE, 0);
        let mut cur = Cursor::new(&wire);
        let header = cur.take::<ForkHeader>().unwrap();
        assert_eq!(header.node_type, NodeType::VALUE);
        assert_eq!(&*header.prefix, b"ab");
        assert!(cur.is_empty());
    }

    #[test]
    fn fork_header_take_underrun_is_too_short() {
        let wire = [NodeType::VALUE.bits()];
        let mut cur = Cursor::new(&wire);
        assert!(matches!(
            cur.take::<ForkHeader>(),
            Err(DecodeError::TooShort)
        ));
    }

    #[test]
    fn metadata_len_wire_round_trips_through_put_and_take() {
        let mut buf = Vec::new();
        Writer::new(&mut buf).put(&MetadataLen(0x1234));
        assert_eq!(buf, [0x12, 0x34]);

        let mut cur = Cursor::new(&buf);
        assert_eq!(cur.take::<MetadataLen>().unwrap().get(), 0x1234);
        assert!(cur.is_empty());
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
        data[ObfuscationKey::SIZE..ObfuscationKey::SIZE + VersionHash::SIZE]
            .copy_from_slice(VersionHash::V02.as_bytes());
        // ref_size at offset 63 is left as 0; index (offset 64..96) is all zero.

        let n = Node::<ChunkRef>::decode(data.as_slice())
            .expect("ref_size=0 with empty forks should decode as terminal node");
        assert!(n.entry().is_none());
        assert!(n.forks().is_empty());
    }

    /// BEE-WORKAROUND(bee#5483): a `ref_size = 0` node with a non-empty forks
    /// bitfield is unrecoverable by any reference implementation (fork refs
    /// would have zero width). Reject as malformed rather than silently
    /// dropping forks the way bee's v0.2 decoder does.
    #[test]
    fn decode_bee_legacy_ref_size_zero_with_forks_is_rejected() {
        let mut data = vec![0u8; 96];
        data[ObfuscationKey::SIZE..ObfuscationKey::SIZE + VersionHash::SIZE]
            .copy_from_slice(VersionHash::V02.as_bytes());
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
        data[ObfuscationKey::SIZE..ObfuscationKey::SIZE + VersionHash::SIZE]
            .copy_from_slice(VersionHash::V01.as_bytes());

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
    fn truncated_node_bytes(version: &VersionHash, len: usize) -> Vec<u8> {
        assert!(len >= NodeHeader::SIZE);
        let mut data = vec![0u8; len];
        data[NodeHeader::VERSION_HASH_OFFSET..NodeHeader::VERSION_HASH_OFFSET + VersionHash::SIZE]
            .copy_from_slice(version.as_bytes());
        data[NodeHeader::REF_SIZE_OFFSET] = 32;
        data
    }

    /// Regression: a 64-byte input (header only, `ref_size = 32`) used to
    /// panic slicing the absent entry slot (`data[64..96]`). It must return
    /// `Err`, never panic. Exact minimal crash case.
    #[test]
    fn decode_v01_header_only_64_bytes_returns_err() {
        let data = truncated_node_bytes(&VersionHash::V01, NodeHeader::SIZE);
        let result = Node::<ChunkRef>::decode(data.as_slice());
        assert!(matches!(result, Err(DecodeError::TooShort)));
    }

    /// Regression: same minimal 64-byte crash case for the v0.2 decoder.
    #[test]
    fn decode_v02_header_only_64_bytes_returns_err() {
        let data = truncated_node_bytes(&VersionHash::V02, NodeHeader::SIZE);
        let result = Node::<ChunkRef>::decode(data.as_slice());
        assert!(matches!(result, Err(DecodeError::TooShort)));
    }

    /// Regression: every length in 64..128 used to panic in `decode_v01`,
    /// either slicing the entry slot (`data[64..96]`, lengths 64..96) or the
    /// forks index (`data[96..128]`, lengths 96..128). All must return `Err`.
    #[test]
    fn decode_v01_truncated_lengths_return_err() {
        for len in NodeHeader::SIZE..NodeHeader::SIZE + 32 + 32 {
            let data = truncated_node_bytes(&VersionHash::V01, len);
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
            let data = truncated_node_bytes(&VersionHash::V02, len);
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
        let mut data = truncated_node_bytes(&VersionHash::V01, NodeHeader::SIZE + 32 + 32);
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
        let mut data = truncated_node_bytes(&VersionHash::V02, NodeHeader::SIZE + 32 + 32);
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
    #[cfg(feature = "encryption")]
    fn truncated_encref_node_bytes(version: &VersionHash, len: usize) -> Vec<u8> {
        assert!(len >= NodeHeader::SIZE);
        let mut data = vec![0u8; len];
        data[NodeHeader::VERSION_HASH_OFFSET..NodeHeader::VERSION_HASH_OFFSET + VersionHash::SIZE]
            .copy_from_slice(version.as_bytes());
        data[NodeHeader::REF_SIZE_OFFSET] = 64;
        data
    }

    /// Regression for the 64-byte entry width: with `ref_size = 64` the entry
    /// slot is `data[64..128]` and the forks index `data[128..160]`, so every
    /// length below 160 must return `Err` (the PR bounds check, exercised for
    /// the encrypted-ref path), never panic.
    #[cfg(feature = "encryption")]
    #[test]
    fn decode_encref_truncated_lengths_return_err() {
        use nectar_primitives::EncryptedChunkRef;
        for (label, version) in [("v01", VersionHash::V01), ("v02", VersionHash::V02)] {
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
    #[cfg(feature = "encryption")]
    #[test]
    fn decode_encref_index_demands_missing_fork_returns_err() {
        use nectar_primitives::EncryptedChunkRef;
        for (label, version) in [("v01", VersionHash::V01), ("v02", VersionHash::V02)] {
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
    /// target through the exact decode entry points the fuzzer exercises
    /// (`Node::<ChunkRef>` for 32-byte plain entries and, under the
    /// `encryption` feature, `Node::<EncryptedChunkRef>` for 64-byte
    /// entries). The oracle is "no panic";
    /// `Err` is an acceptable outcome for any seed. Additionally pin the
    /// intent of each seed by name: `crash-*` seeds must stay `Err` (they
    /// are fixed panic reproducers), `valid-*` seeds must decode `Ok`.
    ///
    /// This keeps the fuzz seeds meaningful on stable without running the
    /// fuzzer itself.
    #[test]
    fn seed_replay_mantaray_node_decode() {
        let seed_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fuzz/seeds/mantaray_node_decode");
        let mut replayed = 0usize;
        for entry in std::fs::read_dir(&seed_dir)
            .unwrap_or_else(|e| panic!("seed dir {} must exist: {e}", seed_dir.display()))
        {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            let data = std::fs::read(&path).unwrap();

            // The fuzz oracle: must not panic. Drive both entry widths the
            // fuzz target drives; the 64-byte `EncryptedChunkRef` path only
            // exists under the `encryption` feature.
            let result = Node::<ChunkRef>::decode(data.as_slice());
            #[cfg(feature = "encryption")]
            let _ = Node::<nectar_primitives::EncryptedChunkRef>::decode(data.as_slice());

            if name.starts_with("crash-") {
                assert!(result.is_err(), "seed {name} must remain an Err reproducer");
            } else if name.starts_with("valid-") {
                assert!(result.is_ok(), "seed {name} must decode successfully");
            }
            replayed += 1;
        }
        assert!(
            replayed >= 3,
            "expected at least the 3 curated seeds, found {replayed}"
        );
    }

    /// Round-trip one wire image at a single reference width: a width the
    /// image does not declare decodes to `Err` and is skipped. The encoder
    /// normalizes to v0.2, so the first re-encode is the canonical image and
    /// the oracle is a fixed point, not equality with the decoded input.
    /// Returns whether the width decoded, so callers can assert the corpus
    /// actually exercises the width it claims.
    fn record_round_trip<R: Reference>(data: &[u8]) -> bool {
        let Ok(node) = Node::<R>::decode(data) else {
            return false;
        };
        let encoded = node.encode().expect("a decoded node must re-encode");
        let redecoded =
            Node::<R>::decode(encoded.as_slice()).expect("the canonical image must decode");
        let reencoded = redecoded
            .encode()
            .expect("a re-decoded node must re-encode");
        assert_eq!(
            reencoded, encoded,
            "encode/decode must reach a byte-canonical fixed point"
        );
        let redecoded_again =
            Node::<R>::decode(reencoded.as_slice()).expect("the canonical image must re-decode");
        assert_eq!(
            redecoded_again, redecoded,
            "decode(encode(node)) must be structurally stable"
        );
        true
    }

    /// Replay the `mantaray_record_roundtrip` seed corpus through the exact
    /// fixed-point round trip the fuzz target runs, at both reference widths.
    /// The corpus carries a v0.1 and a v0.2 plain manifest plus a `ref_size`
    /// 64 encrypted case, so this pins record round-tripping across both wire
    /// versions and both widths on stable, without running the fuzzer. Each
    /// width must actually decode at least the seeds it claims, not merely be
    /// skipped: the two plain manifests at the `ChunkRef` width and the
    /// encrypted case at the `EncryptedChunkRef` width. Counting decodes is
    /// what keeps the fixed-point assertions from passing vacuously.
    #[test]
    fn seed_replay_mantaray_record_roundtrip() {
        let seed_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fuzz/seeds/mantaray_record_roundtrip");
        let mut replayed = 0usize;
        let mut plain_decoded = 0usize;
        #[cfg(feature = "encryption")]
        let mut wide_decoded = 0usize;
        for entry in std::fs::read_dir(&seed_dir)
            .unwrap_or_else(|e| panic!("seed dir {} must exist: {e}", seed_dir.display()))
        {
            let path = entry.unwrap().path();
            let data = std::fs::read(&path).unwrap();

            if record_round_trip::<ChunkRef>(&data) {
                plain_decoded += 1;
            }
            #[cfg(feature = "encryption")]
            if record_round_trip::<nectar_primitives::EncryptedChunkRef>(&data) {
                wide_decoded += 1;
            }
            replayed += 1;
        }
        assert!(
            replayed >= 3,
            "expected at least the 3 curated seeds, found {replayed}"
        );
        assert!(
            plain_decoded >= 2,
            "expected the v0.1 and v0.2 manifests to round-trip at the ChunkRef width, decoded {plain_decoded}"
        );
        #[cfg(feature = "encryption")]
        assert!(
            wide_decoded >= 1,
            "expected the ref_size=64 seed to decode at the EncryptedChunkRef width"
        );
    }

    /// Build arbitrary (valid-by-construction) nodes from a fixed byte buffer
    /// and prove `decode(encode(node)) == node` for each: the `Arbitrary`
    /// impls generate only encodable, round-trip-stable nodes, which is the
    /// property the structured round-trip fuzz target relies on. The buffer
    /// is deterministic, so this pins the impls on stable without running the
    /// fuzzer.
    #[test]
    fn arbitrary_node_encode_decode_round_trip() {
        use arbitrary::{Arbitrary, Unstructured};

        // Deterministic pseudo-random bytes (Knuth multiplicative hash).
        #[allow(clippy::as_conversions)] // u32 >> 24 is always <= 0xFF, fits u8
        let raw: Vec<u8> = (0u32..8192)
            .map(|i| (i.wrapping_mul(2654435761) >> 24) as u8)
            .collect();
        let mut u = Unstructured::new(&raw);

        let mut checked = 0usize;
        while !u.is_empty() && checked < 16 {
            let node = Node::<ChunkRef>::arbitrary(&mut u).unwrap();
            let encoded = node.encode().unwrap();
            let decoded = Node::<ChunkRef>::decode(encoded.as_slice()).unwrap();
            assert_eq!(
                decoded, node,
                "decode(encode(node)) must reproduce the node"
            );
            checked += 1;
        }
        assert!(
            checked >= 8,
            "expected at least 8 arbitrary nodes, got {checked}"
        );
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
        futures::executor::block_on(n.add::<
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
        let mut node = Node::from_reference(ChunkAddress::from(addr));
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

            let parsed =
                parse_fork_body::<ChunkRef>(&buf, <ChunkRef as Reference>::SIZE, has_metadata)
                    .unwrap();
            assert_eq!(parsed, fork, "parse(emit(fork)) must reproduce the fork");
        }
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
            futures::executor::block_on(n.add::<
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
                .mark_persisted(nectar_primitives::chunk::ChunkAddress::from(addr));
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
