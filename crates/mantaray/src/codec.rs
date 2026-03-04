//! Binary encoding for mantaray nodes (v0.1 and v0.2).

use std::collections::BTreeMap;

use crate::error::{MantarayError, Result};
use crate::mode::NodeEntry;
use crate::node::{Fork, Node, NodeType, Prefix};
use crate::obfuscation::ObfuscationKey;

use alloy_primitives::{U256, hex};
use nectar_primitives::chunk::ChunkAddress;

/// Mantaray wire format version (truncated keccak256, 31 bytes).
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
        if bytes == &Self::V01_BYTES {
            Some(Self::V01)
        } else if bytes == &Self::V02_BYTES {
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

/// Wire layout descriptor for a serialised fork header.
struct ForkHeader;

impl ForkHeader {
    /// Protocol anchor: total pre-reference bytes in a fork.
    const PRE_REFERENCE_SIZE: usize = 32;
    /// Offset to the prefix data (past node_type u8 + prefix_len u8).
    const PREFIX_OFFSET: usize = size_of::<u8>() + size_of::<u8>();
    /// Maximum prefix length that fits in a fork header.
    const MAX_PREFIX_LEN: usize = Self::PRE_REFERENCE_SIZE - Self::PREFIX_OFFSET;
    /// Size of the metadata length field.
    const METADATA_LEN_SIZE: usize = size_of::<u16>();
}

// Compile-time layout assertions.
const _: () = assert!(NodeHeader::SIZE == 64);
const _: () = assert!(ForkHeader::PRE_REFERENCE_SIZE == 32);
const _: () = assert!(ForkHeader::MAX_PREFIX_LEN == Prefix::MAX_LEN);
const _: () = assert!(ObfuscationKey::SIZE == 32);

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

/// XOR `data` in-place with a repeating `key`.
fn xor_in_place(data: &mut [u8], key: &[u8]) {
    let key_len = key.len();
    for (i, byte) in data.iter_mut().enumerate() {
        *byte ^= key[i % key_len];
    }
}

impl<E: NodeEntry> TryFrom<&Node<E>> for Vec<u8> {
    type Error = MantarayError;

    #[inline]
    fn try_from(node: &Node<E>) -> Result<Self> {
        encode_node(node)
    }
}

fn encode_node<E: NodeEntry>(node: &Node<E>) -> Result<Vec<u8>> {
    let ref_size = E::SIZE;
    // Pre-allocate: header + entry + bitfield(32) + estimated fork data
    let estimated = NodeHeader::SIZE
        + ref_size
        + 32
        + node.forks.len() * (ForkHeader::PRE_REFERENCE_SIZE + ref_size);
    let mut data = Vec::with_capacity(estimated);
    data.resize(NodeHeader::SIZE, 0);

    // Use the obfuscation key as-is. The key is set at manifest construction:
    // - PlainManifest: ObfuscationKey::ZERO (no obfuscation)
    // - EncryptedManifest: ObfuscationKey::generate() (random key)
    let obfuscation_key = node.obfuscation_key.as_bytes();

    data[..ObfuscationKey::SIZE].copy_from_slice(obfuscation_key);

    data[NodeHeader::VERSION_HASH_OFFSET..NodeHeader::VERSION_HASH_OFFSET + VersionHash::SIZE]
        .copy_from_slice(VersionHash::V02.as_bytes());

    data[NodeHeader::REF_SIZE_OFFSET] = ref_size as u8;

    // append entry (or E::SIZE zero bytes if empty)
    match &node.entry {
        Some(e) => e.write_to(&mut data),
        None => data.resize(data.len() + ref_size, 0),
    }

    // build the 256-bit index of which fork bytes are present
    let mut index = U256::ZERO;
    for &fork_byte in node.forks.keys() {
        index.set_bit(fork_byte as usize, true);
    }
    data.extend_from_slice(&index.to_le_bytes::<32>());

    // append forks in sorted order
    for fork in node.forks.values() {
        fork.encode_into(&mut data)?;
    }

    // XOR-encrypt everything after the obfuscation key in-place
    xor_in_place(&mut data[ObfuscationKey::SIZE..], obfuscation_key);

    Ok(data)
}

impl<E: NodeEntry> TryFrom<&[u8]> for Node<E> {
    type Error = MantarayError;

    fn try_from(value: &[u8]) -> Result<Self> {
        if value.len() < NodeHeader::SIZE {
            return Err(MantarayError::DataTooShort);
        }

        let mut data = value.to_vec();

        let key_bytes: [u8; ObfuscationKey::SIZE] = data[..ObfuscationKey::SIZE]
            .try_into()
            .map_err(|_| MantarayError::DataTooShort)?;
        let obfuscation_key = ObfuscationKey::from(key_bytes);

        // decrypt in-place
        xor_in_place(
            &mut data[ObfuscationKey::SIZE..],
            obfuscation_key.as_bytes(),
        );

        let version_hash = &data
            [NodeHeader::VERSION_HASH_OFFSET..NodeHeader::VERSION_HASH_OFFSET + VersionHash::SIZE];

        let mut node = match VersionHash::from_bytes(version_hash) {
            Some(VersionHash::V01) => decode_v01::<E>(&data)?,
            Some(VersionHash::V02) => decode_v02::<E>(&data)?,
            None => return Err(MantarayError::InvalidVersionHash),
        };

        node.obfuscation_key = obfuscation_key;
        node.loaded = true;
        Ok(node)
    }
}

fn decode_v01<E: NodeEntry>(data: &[u8]) -> Result<Node<E>> {
    let ref_bytes_size = data[NodeHeader::REF_SIZE_OFFSET] as usize;
    if ref_bytes_size != E::SIZE {
        return Err(MantarayError::EntrySizeMismatch {
            expected: E::SIZE,
            actual: ref_bytes_size,
        });
    }

    let entry_bytes = &data[NodeHeader::SIZE..NodeHeader::SIZE + ref_bytes_size];
    let entry = if entry_bytes.iter().all(|&b| b == 0) {
        None
    } else {
        Some(E::try_from_bytes(entry_bytes)?)
    };

    let mut offset = NodeHeader::SIZE + ref_bytes_size;
    let index = U256::from_le_slice(&data[offset..offset + 32]);
    offset += 32;

    let mut forks = BTreeMap::new();
    for b in 0..=u8::MAX {
        if index.bit(b as usize) {
            let end = offset + ForkHeader::PRE_REFERENCE_SIZE + ref_bytes_size;
            if data.len() < end {
                return Err(MantarayError::InsufficientForkBytes {
                    expected: end,
                    actual: data.len(),
                    byte_index: b as usize,
                });
            }

            let mut fork = Fork::default();
            fork.decode_v01(&data[offset..end])?;
            forks.insert(b, fork);
            offset = end;
        }
    }

    Ok(Node {
        entry,
        forks,
        ..Default::default()
    })
}

fn decode_v02<E: NodeEntry>(data: &[u8]) -> Result<Node<E>> {
    let ref_bytes_size = data[NodeHeader::REF_SIZE_OFFSET] as usize;
    if ref_bytes_size != E::SIZE {
        return Err(MantarayError::EntrySizeMismatch {
            expected: E::SIZE,
            actual: ref_bytes_size,
        });
    }

    let entry_bytes = &data[NodeHeader::SIZE..NodeHeader::SIZE + ref_bytes_size];
    let entry = if entry_bytes.iter().all(|&b| b == 0) {
        None
    } else {
        Some(E::try_from_bytes(entry_bytes)?)
    };

    let mut offset = NodeHeader::SIZE + ref_bytes_size;
    let mut node_type = NodeType::empty();

    // deduce edge type from index
    if data[offset..offset + 32].iter().any(|&b| b != 0) {
        node_type |= NodeType::EDGE;
    }

    let index = U256::from_le_slice(&data[offset..offset + 32]);
    offset += 32;

    let mut forks = BTreeMap::new();
    for b in 0..=u8::MAX {
        if index.bit(b as usize) {
            let mut fork = Fork::default();

            if data.len() < offset + 1 {
                return Err(MantarayError::InsufficientForkBytes {
                    expected: offset + 1,
                    actual: data.len(),
                    byte_index: b as usize,
                });
            }

            let fork_node_type = NodeType::from_bits_truncate(data[offset]);
            let mut node_fork_size = ForkHeader::PRE_REFERENCE_SIZE + ref_bytes_size;

            if fork_node_type.contains(NodeType::METADATA) {
                if data.len()
                    < offset
                        + ForkHeader::PRE_REFERENCE_SIZE
                        + ref_bytes_size
                        + ForkHeader::METADATA_LEN_SIZE
                {
                    return Err(MantarayError::InsufficientForkBytes {
                        expected: offset
                            + ForkHeader::PRE_REFERENCE_SIZE
                            + ref_bytes_size
                            + ForkHeader::METADATA_LEN_SIZE,
                        actual: data.len(),
                        byte_index: b as usize,
                    });
                }

                let metadata_bytes_size = u16::from_be_bytes(
                    data[offset + node_fork_size
                        ..offset + node_fork_size + ForkHeader::METADATA_LEN_SIZE]
                        .try_into()
                        .map_err(|_| MantarayError::DataTooShort)?,
                ) as usize;

                node_fork_size += ForkHeader::METADATA_LEN_SIZE;
                node_fork_size += metadata_bytes_size;

                if offset + node_fork_size > data.len() {
                    return Err(MantarayError::InsufficientForkBytes {
                        expected: offset + node_fork_size,
                        actual: data.len(),
                        byte_index: b as usize,
                    });
                }

                fork.decode_v02(
                    &data[offset..offset + node_fork_size],
                    ref_bytes_size,
                    metadata_bytes_size,
                )?;
            } else {
                if data.len() < offset + ForkHeader::PRE_REFERENCE_SIZE + ref_bytes_size {
                    return Err(MantarayError::InsufficientForkBytes {
                        expected: offset + ForkHeader::PRE_REFERENCE_SIZE + ref_bytes_size,
                        actual: data.len(),
                        byte_index: b as usize,
                    });
                }

                fork.decode_v01(&data[offset..offset + node_fork_size])?;
            }

            forks.insert(b, fork);
            offset += node_fork_size;
        }
    }

    Ok(Node {
        node_type,
        entry,
        forks,
        ..Default::default()
    })
}

/// Parse and validate fork header. Returns (node_type, prefix).
fn parse_fork_header(data: &[u8]) -> Result<(NodeType, Prefix)> {
    let node_type = NodeType::from_bits_truncate(data[0]);
    let prefix_length = data[1] as usize;
    if prefix_length == 0 || prefix_length > Prefix::MAX_LEN {
        return Err(MantarayError::InvalidPrefixLength {
            max: Prefix::MAX_LEN,
            actual: prefix_length,
        });
    }
    let prefix = Prefix::from_slice(
        &data[ForkHeader::PREFIX_OFFSET..ForkHeader::PREFIX_OFFSET + prefix_length],
    );
    Ok((node_type, prefix))
}

impl<E: NodeEntry> Fork<E> {
    /// Create a node from reference bytes (first 32 bytes used as chunk address).
    fn node_from_ref_bytes(ref_data: &[u8]) -> Result<Node<E>> {
        if ref_data.len() < 32 {
            return Err(MantarayError::DataTooShort);
        }
        let addr_bytes: [u8; 32] = ref_data[..32]
            .try_into()
            .map_err(|_| MantarayError::DataTooShort)?;
        Ok(Node::from_reference(ChunkAddress::from(addr_bytes)))
    }

    /// Encode this fork, appending to `buf`.
    fn encode_into(&self, data: &mut Vec<u8>) -> Result<()> {
        data.push(self.node.node_type.bits());
        data.push(self.prefix.len() as u8);

        // write prefix padded to Prefix::MAX_LEN — Prefix is already zero-padded
        data.extend_from_slice(self.prefix.padded_bytes());

        // Write E::SIZE bytes for the reference (chunk address + zero padding)
        if let Some(addr) = &self.node.reference {
            data.extend_from_slice(addr.as_bytes());
            // Pad to E::SIZE if needed (encrypted mode has 64-byte refs)
            let padding = E::SIZE.saturating_sub(32);
            if padding > 0 {
                data.resize(data.len() + padding, 0);
            }
        }

        if self.node.is_with_metadata() {
            let mut metadata_json = serde_json::to_string(&self.node.metadata)
                .map_err(|e| MantarayError::InvalidMetadata {
                    message: e.to_string(),
                })?
                .into_bytes();

            let metadata_bytes_size_with_header =
                metadata_json.len() + ForkHeader::METADATA_LEN_SIZE;

            let padding = if metadata_bytes_size_with_header < ObfuscationKey::SIZE {
                ObfuscationKey::SIZE - metadata_bytes_size_with_header
            } else if metadata_bytes_size_with_header > ObfuscationKey::SIZE {
                let rem = metadata_bytes_size_with_header % ObfuscationKey::SIZE;
                if rem == 0 {
                    0
                } else {
                    ObfuscationKey::SIZE - rem
                }
            } else {
                0
            };

            metadata_json.resize(metadata_json.len() + padding, 0x0a);

            let metadata_size = metadata_json.len();
            if metadata_size > u16::MAX as usize {
                return Err(MantarayError::MetadataTooLarge {
                    max: u16::MAX as usize,
                    actual: metadata_size,
                });
            }

            data.extend_from_slice(&(metadata_size as u16).to_be_bytes());
            data.extend_from_slice(&metadata_json);
        }

        Ok(())
    }

    /// Decode a fork from v0.1 binary data.
    pub(crate) fn decode_v01(&mut self, data: &[u8]) -> Result<()> {
        let (node_type, prefix) = parse_fork_header(data)?;

        self.prefix = prefix;
        let ref_data = &data[ForkHeader::PRE_REFERENCE_SIZE..];
        self.node = Self::node_from_ref_bytes(ref_data)?;
        self.node.node_type = node_type;

        Ok(())
    }

    /// Decode a fork from v0.2 binary data (with metadata).
    pub(crate) fn decode_v02(
        &mut self,
        data: &[u8],
        ref_bytes_size: usize,
        metadata_bytes_size: usize,
    ) -> Result<()> {
        let (node_type, prefix) = parse_fork_header(data)?;

        self.prefix = prefix;
        let ref_data =
            &data[ForkHeader::PRE_REFERENCE_SIZE..ForkHeader::PRE_REFERENCE_SIZE + ref_bytes_size];
        self.node = Self::node_from_ref_bytes(ref_data)?;
        self.node.node_type = node_type;

        if metadata_bytes_size > 0 {
            let metadata_start =
                ForkHeader::PRE_REFERENCE_SIZE + ref_bytes_size + ForkHeader::METADATA_LEN_SIZE;
            let metadata_bytes = &data[metadata_start..];
            self.node.metadata = serde_json::from_slice(metadata_bytes).map_err(|e| {
                MantarayError::InvalidMetadata {
                    message: e.to_string(),
                }
            })?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::hex;
    use alloy_primitives::utils::keccak256;

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
        let n = Node::<ChunkAddress>::try_from(data.as_slice()).unwrap();

        let mut expect_bytes = hex::decode(&ENCODED_V01[128..192]).unwrap();
        xor_in_place(&mut expect_bytes, n.obfuscation_key().as_bytes());

        // Root entry bytes are all zeros after decryption → None (no entry).
        if expect_bytes.iter().all(|&b| b == 0) {
            assert!(n.entry().is_none());
        } else {
            assert_eq!(n.entry().unwrap().as_bytes(), &expect_bytes[..]);
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
        let n = Node::<ChunkAddress>::try_from(data.as_slice()).unwrap();

        let mut expect_bytes = hex::decode(&ENCODED_V02[128..192]).unwrap();
        xor_in_place(&mut expect_bytes, n.obfuscation_key().as_bytes());

        // Root entry bytes are all zeros after decryption → None (no entry).
        if expect_bytes.iter().all(|&b| b == 0) {
            assert!(n.entry().is_none());
        } else {
            assert_eq!(n.entry().unwrap().as_bytes(), &expect_bytes[..]);
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
        let result = Node::<ChunkAddress>::try_from([].as_slice());
        assert!(matches!(result, Err(MantarayError::DataTooShort)));
    }

    #[test]
    fn decode_too_short_for_header() {
        let data = vec![0u8; NodeHeader::SIZE - 1];
        let result = Node::<ChunkAddress>::try_from(data.as_slice());
        assert!(matches!(result, Err(MantarayError::DataTooShort)));
    }

    #[test]
    fn decode_invalid_version_hash() {
        let data = vec![0u8; NodeHeader::SIZE];
        let result = Node::<ChunkAddress>::try_from(data.as_slice());
        assert!(matches!(result, Err(MantarayError::InvalidVersionHash)));
    }

    /// Test vector: valid manifest with correct metadata size (93 bytes).
    /// This is a v0.2 manifest with zero obfuscation key, a single fork at '/',
    /// and website-index-document metadata.
    #[test]
    fn decode_valid_manifest_from_go() {
        let data = hex::decode(
            "00000000000000000000000000000000000000000000000000000000000000005768b3b6a7db56d21d1abff40d41cebfc83448fed8d7e9b06ec0d3b073f28f200000000000000000000000000000000000000000000000000000000000000000000000000080000000000000000000000000000000000000000000000000000016012f0000000000000000000000000000000000000000000000000000000000e87f95c3d081c4fede769b6c69e27b435e525cbd25c6715c607e7c531e329639005d7b22776562736974652d696e6465782d646f63756d656e74223a2233356561656538316262363338303436393965633637316265323736326465626665346662643330636461646139303232393239646131613965366134366436227d0a"
        ).unwrap();
        assert!(Node::<ChunkAddress>::try_from(data.as_slice()).is_ok());
    }

    /// Test vector: metadata size field says 89 but actual content needs 93.
    /// Should fail because there aren't enough bytes for the declared metadata.
    #[test]
    fn decode_invalid_manifest_size_89() {
        let data = hex::decode(
            "00000000000000000000000000000000000000000000000000000000000000005768b3b6a7db56d21d1abff40d41cebfc83448fed8d7e9b06ec0d3b073f28f200000000000000000000000000000000000000000000000000000000000000000000000000080000000000000000000000000000000000000000000000000000016012f0000000000000000000000000000000000000000000000000000000000e87f95c3d081c4fede769b6c69e27b435e525cbd25c6715c607e7c531e32963900597b22776562736974652d696e6465782d646f63756d656e74223a2233356561656538316262363338303436393965633637316265323736326465626665346662643330636461646139303232393239646131613965366134366436227d0a"
        ).unwrap();
        assert!(Node::<ChunkAddress>::try_from(data.as_slice()).is_err());
    }

    /// Test vector: metadata size field says 95 but actual content is 93.
    /// Should fail because the size exceeds available bytes.
    #[test]
    fn decode_invalid_manifest_size_95() {
        let data = hex::decode(
            "00000000000000000000000000000000000000000000000000000000000000005768b3b6a7db56d21d1abff40d41cebfc83448fed8d7e9b06ec0d3b073f28f200000000000000000000000000000000000000000000000000000000000000000000000000080000000000000000000000000000000000000000000000000000016012f0000000000000000000000000000000000000000000000000000000000e87f95c3d081c4fede769b6c69e27b435e525cbd25c6715c607e7c531e329639005f7b22776562736974652d696e6465782d646f63756d656e74223a2233356561656538316262363338303436393965633637316265323736326465626665346662643330636461646139303232393239646131613965366134366436227d0a"
        ).unwrap();
        assert!(Node::<ChunkAddress>::try_from(data.as_slice()).is_err());
    }

    /// Test vector: metadata size field says 96 but actual content is 93.
    /// Should fail because the size exceeds available bytes.
    #[test]
    fn decode_invalid_manifest_size_96() {
        let data = hex::decode(
            "00000000000000000000000000000000000000000000000000000000000000005768b3b6a7db56d21d1abff40d41cebfc83448fed8d7e9b06ec0d3b073f28f200000000000000000000000000000000000000000000000000000000000000000000000000080000000000000000000000000000000000000000000000000000016012f0000000000000000000000000000000000000000000000000000000000e87f95c3d081c4fede769b6c69e27b435e525cbd25c6715c607e7c531e32963900607b22776562736974652d696e6465782d646f63756d656e74223a2233356561656538316262363338303436393965633637316265323736326465626665346662643330636461646139303232393239646131613965366134366436227d0a"
        ).unwrap();
        assert!(Node::<ChunkAddress>::try_from(data.as_slice()).is_err());
    }

    /// Encode-decode round-trip preserves entries and metadata.
    #[test]
    fn encode_decode_round_trip() {
        let mut n = Node::<ChunkAddress>::new_unencrypted();

        for entry in test_entries() {
            let path = entry.path.as_bytes();
            let e = {
                let mut buf = [0u8; 32];
                let len = path.len().min(32);
                buf[32 - len..].copy_from_slice(&path[..len]);
                ChunkAddress::from(buf)
            };
            n.add::<nectar_primitives::store::NullLoader, { nectar_primitives::bmt::DEFAULT_BODY_SIZE }>(
                path, Some(e), entry.metadata, &nectar_primitives::store::NullLoader,
            )
            .unwrap();
        }

        // assign deterministic references to forks so encoding works
        for (counter, fork) in n.forks.values_mut().enumerate() {
            let mut addr = [0u8; 32];
            addr[31] = counter as u8;
            fork.node.reference = Some(nectar_primitives::chunk::ChunkAddress::from(addr));
        }

        let encoded = Vec::<u8>::try_from(&n).unwrap();
        let n2 = Node::<ChunkAddress>::try_from(encoded.as_slice()).unwrap();

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
