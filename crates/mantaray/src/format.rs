//! Mantaray wire grammar: node header layout, fork framing, and the shared
//! field codecs used by both the node codec and [`NodeView`](crate::view::NodeView).

use crate::error::DecodeError;
use crate::node::{NodeType, Prefix};
use crate::obfuscation::ObfuscationKey;

use alloy_primitives::{U256, hex};
use nectar_primitives::EncryptedChunkRef;
use nectar_primitives::chunk::{ChunkRef, RefKind};
use nectar_primitives::wire::{Cursor, FromCursor, ToWriter, Underrun, Writer};

/// Mantaray wire format version.
///
/// Wire form: the first 31 bytes of the keccak-256 hash of the version string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Version {
    /// `mantaray:0.1`; forks carry no metadata.
    V01,
    /// `mantaray:0.2`; forks flagged `METADATA` carry a length-prefixed
    /// metadata region.
    V02,
}

impl Version {
    /// Wire size of a truncated version hash.
    pub(crate) const SIZE: usize = 31;

    const V01_BYTES: [u8; Self::SIZE] =
        hex!("025184789d63635766d78c41900196b57d7400875ebe4d9b5d1e76bd9652a9");
    const V02_BYTES: [u8; Self::SIZE] =
        hex!("5768b3b6a7db56d21d1abff40d41cebfc83448fed8d7e9b06ec0d3b073f28f");

    pub(crate) const fn as_bytes(&self) -> &[u8; Self::SIZE] {
        match self {
            Self::V01 => &Self::V01_BYTES,
            Self::V02 => &Self::V02_BYTES,
        }
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes == Self::V01_BYTES {
            Some(Self::V01)
        } else if bytes == Self::V02_BYTES {
            Some(Self::V02)
        } else {
            None
        }
    }
}

/// Reference width declared by a node's own header byte, governing the entry
/// slot and every fork reference slot of that node only; each node in a trie
/// declares its own, so mixed-width tries decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefWidth {
    /// BEE-WORKAROUND(bee#5483): a zero `ref_size` byte marking an entry-less
    /// terminal node. Not spec-legal; see the codec's workaround note.
    Zero,
    /// A declared width naming the reference kind of every slot.
    Kind(RefKind),
}

// Every reference width fits the single header `ref_size` byte.
const _: () = assert!(ChunkRef::SIZE < 256 && EncryptedChunkRef::SIZE < 256);

impl RefWidth {
    /// Width in bytes of each reference slot.
    pub const fn size(self) -> usize {
        match self {
            Self::Zero => 0,
            Self::Kind(kind) => kind.size(),
        }
    }

    /// The header `ref_size` byte: the width itself, whose low byte is exact
    /// by the assertion above.
    pub(crate) const fn as_byte(self) -> u8 {
        let [byte, ..] = self.size().to_le_bytes();
        byte
    }
}

impl TryFrom<u8> for RefWidth {
    type Error = DecodeError;

    fn try_from(byte: u8) -> Result<Self, DecodeError> {
        let width = usize::from(byte);
        if width == 0 {
            Ok(Self::Zero)
        } else if width == RefKind::Plain.size() {
            Ok(Self::Kind(RefKind::Plain))
        } else if width == RefKind::Encrypted.size() {
            Ok(Self::Kind(RefKind::Encrypted))
        } else {
            Err(DecodeError::UnsupportedRefWidth { actual: byte })
        }
    }
}

/// Wire layout descriptor for a serialized node header.
pub(crate) struct NodeHeader;

impl NodeHeader {
    pub(crate) const SIZE: usize = ObfuscationKey::SIZE + Version::SIZE + size_of::<u8>();
    pub(crate) const VERSION_HASH_OFFSET: usize = ObfuscationKey::SIZE;
    pub(crate) const REF_SIZE_OFFSET: usize = ObfuscationKey::SIZE + Version::SIZE;
}

/// A fork's fixed wire header: the node type byte, then the prefix record.
pub(crate) struct ForkHeader {
    pub(crate) node_type: NodeType,
    pub(crate) prefix: Prefix,
}

impl ForkHeader {
    /// Protocol anchor: total pre-reference bytes in a fork.
    pub(crate) const PRE_REFERENCE_SIZE: usize = 32;
    /// Size of the metadata length field.
    pub(crate) const METADATA_LEN_SIZE: usize = size_of::<u16>();
}

impl FromCursor for ForkHeader {
    type Error = DecodeError;

    fn take_from(cur: &mut Cursor<'_>) -> Result<Self, Self::Error> {
        let node_type = NodeType::from_bits_truncate(cur.take::<u8>()?);
        let prefix = cur.take::<Prefix>()?;
        Ok(Self { node_type, prefix })
    }
}

/// Length of a fork's metadata region, stored big-endian on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MetadataLen(pub(crate) u16);

impl MetadataLen {
    /// The length in bytes.
    pub(crate) fn get(self) -> usize {
        usize::from(self.0)
    }
}

impl FromCursor for MetadataLen {
    type Error = Underrun;

    fn take_from(cur: &mut Cursor<'_>) -> Result<Self, Underrun> {
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
pub(crate) const FORK_INDEX_SIZE: usize = size_of::<U256>();

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

/// XOR `data` in place with a repeating `key`.
pub(crate) fn xor_in_place(data: &mut [u8], key: &[u8]) {
    for (byte, mask) in data.iter_mut().zip(key.iter().cycle()) {
        *byte ^= *mask;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::utils::keccak256;

    const VERSION_HASH_01_BYTES: [u8; 32] =
        hex!("025184789d63635766d78c41900196b57d7400875ebe4d9b5d1e76bd9652a9b7");
    const VERSION_HASH_02_BYTES: [u8; 32] =
        hex!("5768b3b6a7db56d21d1abff40d41cebfc83448fed8d7e9b06ec0d3b073f28f7b");

    const VERSION_STRING_01: &str = "mantaray:0.1";
    const VERSION_STRING_02: &str = "mantaray:0.2";

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
    fn ref_width_classifies_the_header_byte() {
        assert_eq!(RefWidth::try_from(0).unwrap(), RefWidth::Zero);
        let plain = RefWidth::try_from(RefWidth::Kind(RefKind::Plain).as_byte()).unwrap();
        assert_eq!(plain, RefWidth::Kind(RefKind::Plain));
        assert_eq!(plain.size(), ChunkRef::SIZE);
        let enc = RefWidth::try_from(RefWidth::Kind(RefKind::Encrypted).as_byte()).unwrap();
        assert_eq!(enc, RefWidth::Kind(RefKind::Encrypted));
        assert_eq!(enc.size(), EncryptedChunkRef::SIZE);
    }

    #[test]
    fn ref_width_rejects_unknown_widths() {
        for byte in [1u8, 31, 33, 63, 65, 255] {
            assert!(matches!(
                RefWidth::try_from(byte),
                Err(DecodeError::UnsupportedRefWidth { actual }) if actual == byte
            ));
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
}
