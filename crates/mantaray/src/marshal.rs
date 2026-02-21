//! Binary serialisation for mantaray nodes (v0.1 and v0.2).

use std::collections::BTreeMap;

use crate::error::{MantarayError, Result};
use crate::node::{Fork, Node};
use crate::{
    NODE_FORK_HEADER_SIZE, NODE_FORK_METADATA_BYTES_SIZE, NODE_FORK_PRE_REFERENCE_SIZE,
    NODE_FORK_TYPE_BYTES_SIZE, NODE_HEADER_SIZE, NODE_OBFUSCATION_KEY_SIZE, NODE_PREFIX_MAX_SIZE,
    NT_WITH_METADATA, VERSION_HASH_SIZE,
};

// pre-calculated keccak256 hashes as const byte arrays (no runtime hex::decode)
const VERSION_HASH_01_BYTES: [u8; 32] = [
    0x02, 0x51, 0x84, 0x78, 0x9d, 0x63, 0x63, 0x57, 0x66, 0xd7, 0x8c, 0x41, 0x90, 0x01, 0x96,
    0xb5, 0x7d, 0x74, 0x00, 0x87, 0x5e, 0xbe, 0x4d, 0x9b, 0x5d, 0x1e, 0x76, 0xbd, 0x96, 0x52,
    0xa9, 0xb7,
];
const VERSION_HASH_02_BYTES: [u8; 32] = [
    0x57, 0x68, 0xb3, 0xb6, 0xa7, 0xdb, 0x56, 0xd2, 0x1d, 0x1a, 0xbf, 0xf4, 0x0d, 0x41, 0xce,
    0xbf, 0xc8, 0x34, 0x48, 0xfe, 0xd8, 0xd7, 0xe9, 0xb0, 0x6e, 0xc0, 0xd3, 0xb0, 0x73, 0xf2,
    0x8f, 0x7b,
];

#[cfg(test)]
const VERSION_STRING_01: &str = "mantaray:0.1";
#[cfg(test)]
const VERSION_STRING_02: &str = "mantaray:0.2";

/// 256-bit field used as a compact index over the 256 possible fork byte values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BitField {
    bits: [u8; 32],
}

impl BitField {
    pub(crate) const fn new() -> Self {
        Self { bits: [0; 32] }
    }

    pub(crate) fn from_slice(slice: &[u8]) -> Self {
        let mut bf = Self::new();
        bf.bits.copy_from_slice(&slice[..32]);
        bf
    }

    pub(crate) const fn as_bytes(&self) -> &[u8; 32] {
        &self.bits
    }

    pub(crate) const fn set(&mut self, i: u8) {
        self.bits[i as usize / 8] |= 1 << (i % 8);
    }

    pub(crate) const fn get(&self, i: u8) -> bool {
        self.bits[i as usize / 8] & (1 << (i % 8)) != 0
    }
}

/// XOR encrypt/decrypt data in-place with a repeating key.
pub(crate) fn encrypt_decrypt_in_place(data: &mut [u8], key: &[u8]) {
    let key_len = key.len();
    for (i, byte) in data.iter_mut().enumerate() {
        *byte ^= key[i % key_len];
    }
}

/// XOR encrypt/decrypt data with a repeating key, returning a new Vec.
#[cfg(test)]
pub(crate) fn encrypt_decrypt(data: &[u8], key: &[u8]) -> Vec<u8> {
    let key_len = key.len();
    data.iter()
        .enumerate()
        .map(|(i, byte)| byte ^ key[i % key_len])
        .collect()
}

impl Node {
    /// Serialise this node to its binary representation (v0.2 format).
    pub fn marshal_binary(&self) -> Result<Vec<u8>> {
        // Pre-allocate: header + entry(32) + bitfield(32) + estimated fork data
        let estimated =
            NODE_HEADER_SIZE + 32 + 32 + self.forks.len() * (NODE_FORK_PRE_REFERENCE_SIZE + 32);
        let mut data = Vec::with_capacity(estimated);
        data.resize(NODE_HEADER_SIZE, 0);

        // generate or use existing obfuscation key
        let obfuscation_key = if self.obfuscation_key.is_empty() {
            #[cfg(feature = "rand")]
            {
                let mut key = [0u8; NODE_OBFUSCATION_KEY_SIZE];
                rand::fill(&mut key[..]);
                key.to_vec()
            }
            #[cfg(not(feature = "rand"))]
            {
                vec![0u8; NODE_OBFUSCATION_KEY_SIZE]
            }
        } else {
            self.obfuscation_key.clone()
        };

        data[..NODE_OBFUSCATION_KEY_SIZE].copy_from_slice(&obfuscation_key);

        // use pre-computed const version hash (no hex::decode allocation)
        data[NODE_OBFUSCATION_KEY_SIZE..NODE_OBFUSCATION_KEY_SIZE + VERSION_HASH_SIZE]
            .copy_from_slice(&VERSION_HASH_02_BYTES[..VERSION_HASH_SIZE]);

        data[NODE_OBFUSCATION_KEY_SIZE + VERSION_HASH_SIZE] = self.ref_bytes_size as u8;

        // append entry (or 32 zero bytes if empty)
        if self.entry.is_empty() {
            data.extend_from_slice(&[0; 32]);
        } else {
            data.extend_from_slice(&self.entry);
        }

        // build the 256-bit index of which fork bytes are present
        let mut index = BitField::new();
        for &fork_byte in self.forks.keys() {
            index.set(fork_byte);
        }
        data.extend_from_slice(index.as_bytes());

        // append forks in sorted order
        for fork in self.forks.values() {
            fork.marshal_binary_into(&mut data)?;
        }

        // XOR-encrypt everything after the obfuscation key in-place
        encrypt_decrypt_in_place(&mut data[NODE_OBFUSCATION_KEY_SIZE..], &obfuscation_key);

        Ok(data)
    }

    /// Deserialise a node from its binary representation (v0.1 or v0.2).
    pub fn unmarshal_binary(&mut self, data: &mut [u8]) -> Result<()> {
        if data.len() < NODE_HEADER_SIZE {
            return Err(MantarayError::DataTooShort);
        }

        self.obfuscation_key = data[..NODE_OBFUSCATION_KEY_SIZE].to_vec();

        // decrypt in-place (no allocation)
        encrypt_decrypt_in_place(&mut data[NODE_OBFUSCATION_KEY_SIZE..], &self.obfuscation_key);

        let version_hash =
            &data[NODE_OBFUSCATION_KEY_SIZE..NODE_OBFUSCATION_KEY_SIZE + VERSION_HASH_SIZE];

        // compare against pre-computed const byte arrays (no hex::decode)
        if version_hash == &VERSION_HASH_01_BYTES[..VERSION_HASH_SIZE] {
            self.unmarshal_v01(data)
        } else if version_hash == &VERSION_HASH_02_BYTES[..VERSION_HASH_SIZE] {
            self.unmarshal_v02(data)
        } else {
            Err(MantarayError::InvalidVersionHash)
        }
    }

    fn unmarshal_v01(&mut self, data: &[u8]) -> Result<()> {
        let ref_bytes_size = data[NODE_HEADER_SIZE - 1] as usize;

        self.entry = data[NODE_HEADER_SIZE..NODE_HEADER_SIZE + ref_bytes_size].to_vec();

        let mut offset = NODE_HEADER_SIZE + ref_bytes_size;
        let index = BitField::from_slice(&data[offset..offset + 32]);
        offset += 32;

        for b in 0..=u8::MAX {
            if index.get(b) {
                let end = offset + NODE_FORK_PRE_REFERENCE_SIZE + ref_bytes_size;
                if data.len() < end {
                    return Err(MantarayError::InsufficientForkBytes {
                        expected: end,
                        actual: data.len(),
                        byte_index: b as usize,
                    });
                }

                let mut fork = Fork::default();
                fork.unmarshal_binary(&data[offset..end])?;
                self.forks.insert(b, fork);
                offset = end;
            }
        }

        Ok(())
    }

    fn unmarshal_v02(&mut self, data: &[u8]) -> Result<()> {
        let ref_bytes_size = data[NODE_HEADER_SIZE - 1] as usize;

        self.entry = data[NODE_HEADER_SIZE..NODE_HEADER_SIZE + ref_bytes_size].to_vec();

        let mut offset = NODE_HEADER_SIZE + ref_bytes_size;

        // deduce edge type from index
        if data[offset..offset + 32].iter().any(|&b| b != 0) && !self.is_edge() {
            self.make_edge();
        }

        self.forks = BTreeMap::new();

        let index = BitField::from_slice(&data[offset..offset + 32]);
        offset += 32;

        for b in 0..=u8::MAX {
            if index.get(b) {
                let mut fork = Fork::default();

                if data.len() < offset + NODE_FORK_TYPE_BYTES_SIZE {
                    return Err(MantarayError::InsufficientForkBytes {
                        expected: offset + NODE_FORK_TYPE_BYTES_SIZE,
                        actual: data.len(),
                        byte_index: b as usize,
                    });
                }

                let node_type = data[offset];
                let mut node_fork_size = NODE_FORK_PRE_REFERENCE_SIZE + ref_bytes_size;

                if (node_type & NT_WITH_METADATA) == NT_WITH_METADATA {
                    if data.len()
                        < offset + NODE_FORK_PRE_REFERENCE_SIZE + ref_bytes_size
                            + NODE_FORK_METADATA_BYTES_SIZE
                    {
                        return Err(MantarayError::InsufficientForkBytes {
                            expected: offset + NODE_FORK_PRE_REFERENCE_SIZE + ref_bytes_size
                                + NODE_FORK_METADATA_BYTES_SIZE,
                            actual: data.len(),
                            byte_index: b as usize,
                        });
                    }

                    let metadata_bytes_size = u16::from_be_bytes(
                        data[offset + node_fork_size
                            ..offset + node_fork_size + NODE_FORK_METADATA_BYTES_SIZE]
                            .try_into()
                            .expect("slice is 2 bytes"),
                    ) as usize;

                    node_fork_size += NODE_FORK_METADATA_BYTES_SIZE;
                    node_fork_size += metadata_bytes_size;

                    if offset + node_fork_size > data.len() {
                        return Err(MantarayError::InsufficientForkBytes {
                            expected: offset + node_fork_size,
                            actual: data.len(),
                            byte_index: b as usize,
                        });
                    }

                    fork.unmarshal_binary_v02(
                        &data[offset..offset + node_fork_size],
                        ref_bytes_size,
                        metadata_bytes_size,
                    )?;
                } else {
                    if data.len() < offset + NODE_FORK_PRE_REFERENCE_SIZE + ref_bytes_size {
                        return Err(MantarayError::InsufficientForkBytes {
                            expected: offset + NODE_FORK_PRE_REFERENCE_SIZE + ref_bytes_size,
                            actual: data.len(),
                            byte_index: b as usize,
                        });
                    }

                    fork.unmarshal_binary(&data[offset..offset + node_fork_size])?;
                }

                self.forks.insert(b, fork);
                offset += node_fork_size;
            }
        }

        Ok(())
    }
}

impl Fork {
    /// Serialise this fork to its binary representation.
    pub fn marshal_binary(&self) -> Result<Vec<u8>> {
        let mut data = Vec::with_capacity(NODE_FORK_PRE_REFERENCE_SIZE + self.node.reference.len());
        self.marshal_binary_into(&mut data)?;
        Ok(data)
    }

    /// Serialise this fork, appending to an existing buffer (avoids intermediate allocation).
    fn marshal_binary_into(&self, data: &mut Vec<u8>) -> Result<()> {
        let r = &self.node.reference;
        if r.len() > 256 {
            return Err(MantarayError::RefTooLong {
                max: 256,
                actual: r.len(),
            });
        }

        data.push(self.node.node_type);
        data.push(self.prefix.len() as u8);

        // write prefix padded to NODE_PREFIX_MAX_SIZE without allocating
        let mut prefix_buf = [0u8; NODE_PREFIX_MAX_SIZE];
        prefix_buf[..self.prefix.len()].copy_from_slice(&self.prefix);
        data.extend_from_slice(&prefix_buf);

        data.extend_from_slice(r);

        if self.node.is_with_metadata() {
            let mut metadata_json =
                serde_json::to_string(&self.node.metadata)
                    .map_err(|e| MantarayError::InvalidMetadata {
                        message: e.to_string(),
                    })?
                    .into_bytes();

            let metadata_bytes_size_with_header =
                metadata_json.len() + NODE_FORK_METADATA_BYTES_SIZE;

            let padding = if metadata_bytes_size_with_header < NODE_OBFUSCATION_KEY_SIZE {
                NODE_OBFUSCATION_KEY_SIZE - metadata_bytes_size_with_header
            } else if metadata_bytes_size_with_header > NODE_OBFUSCATION_KEY_SIZE {
                let rem = metadata_bytes_size_with_header % NODE_OBFUSCATION_KEY_SIZE;
                if rem == 0 { 0 } else { NODE_OBFUSCATION_KEY_SIZE - rem }
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

    /// Deserialise a fork from v0.1 format binary data.
    pub fn unmarshal_binary(&mut self, data: &[u8]) -> Result<()> {
        let node_type = data[0];
        let prefix_length = data[1] as usize;

        if prefix_length == 0 || prefix_length > NODE_PREFIX_MAX_SIZE {
            return Err(MantarayError::InvalidPrefixLength {
                max: NODE_PREFIX_MAX_SIZE,
                actual: prefix_length,
            });
        }

        self.prefix = data[NODE_FORK_HEADER_SIZE..NODE_FORK_HEADER_SIZE + prefix_length].to_vec();
        self.node = Node::from_reference(&data[NODE_FORK_PRE_REFERENCE_SIZE..]);
        self.node.node_type = node_type;

        Ok(())
    }

    /// Deserialise a fork from v0.2 format binary data (with metadata support).
    pub fn unmarshal_binary_v02(
        &mut self,
        data: &[u8],
        ref_bytes_size: usize,
        metadata_bytes_size: usize,
    ) -> Result<()> {
        let node_type = data[0];
        let prefix_length = data[1] as usize;

        if prefix_length == 0 || prefix_length > NODE_PREFIX_MAX_SIZE {
            return Err(MantarayError::InvalidPrefixLength {
                max: NODE_PREFIX_MAX_SIZE,
                actual: prefix_length,
            });
        }

        self.prefix = data[NODE_FORK_HEADER_SIZE..NODE_FORK_HEADER_SIZE + prefix_length].to_vec();
        self.node = Node::from_reference(
            &data[NODE_FORK_PRE_REFERENCE_SIZE..NODE_FORK_PRE_REFERENCE_SIZE + ref_bytes_size],
        );
        self.node.node_type = node_type;

        if metadata_bytes_size > 0 {
            let metadata_start =
                NODE_FORK_PRE_REFERENCE_SIZE + ref_bytes_size + NODE_FORK_METADATA_BYTES_SIZE;
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
    use nectar_primitives::bmt::DEFAULT_BODY_SIZE;
    use nectar_primitives::store::MemorySink;
    use crate::keccak256;

    type Store = MemorySink<DEFAULT_BODY_SIZE>;

    const TEST_MARSHAL_OUTPUT_01: &str = "52fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64950ac787fbce1061870e8d34e0a638bc7e812c7ca4ebd31d626a572ba47b06f6952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072102654f163f5f0fa0621d729566c74d10037c4d7bbb0407d1e2c64950fcd3072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64950f89d6640e3044f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64850ff9f642182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64b50fc98072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64a50ff99622182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64d";
    const TEST_MARSHAL_OUTPUT_02: &str = "52fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64905954fb18659339d0b25e0fb9723d3cd5d528fb3c8d495fd157bd7b7a210496952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072102654f163f5f0fa0621d729566c74d10037c4d7bbb0407d1e2c64940fcd3072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952e3872548ec012a6e123b60f9177017fb12e57732621d2c1ada267adbe8cc4350f89d6640e3044f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64850ff9f642182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64b50fc98072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64a50ff99622182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64952fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c64d";

    #[derive(Clone, Default)]
    struct MarshalNodeEntry {
        path: String,
        metadata: BTreeMap<String, String>,
    }

    fn test_entries() -> [MarshalNodeEntry; 5] {
        [
            MarshalNodeEntry {
                path: "/".to_string(),
                metadata: serde_json::from_str(r#"{"index-document": "aaaaa"}"#).unwrap(),
            },
            MarshalNodeEntry {
                path: "aaaaa".to_string(),
                ..Default::default()
            },
            MarshalNodeEntry {
                path: "cc".to_string(),
                ..Default::default()
            },
            MarshalNodeEntry {
                path: "d".to_string(),
                ..Default::default()
            },
            MarshalNodeEntry {
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
    fn unmarshal_01() {
        let mut data = hex::decode(TEST_MARSHAL_OUTPUT_01).unwrap();
        let mut n = Node::default();

        n.unmarshal_binary(&mut data).unwrap();

        let expect_encrypted_bytes =
            hex::decode(&TEST_MARSHAL_OUTPUT_01[128..192]).unwrap();
        let expect_bytes = encrypt_decrypt(&expect_encrypted_bytes, n.obfuscation_key());

        assert_eq!(n.entry(), expect_bytes);
        assert_eq!(test_entries().len(), n.forks().len());

        for entry in test_entries() {
            let key = entry.path.as_bytes()[0];
            assert!(n.forks().contains_key(&key));
            assert_eq!(n.forks()[&key].prefix(), entry.path.as_bytes());
        }
    }

    #[test]
    fn unmarshal_02() {
        let mut data = hex::decode(TEST_MARSHAL_OUTPUT_02).unwrap();
        let mut n = Node::default();

        n.unmarshal_binary(&mut data).unwrap();

        let expect_encrypted_bytes =
            hex::decode(&TEST_MARSHAL_OUTPUT_02[128..192]).unwrap();
        let expect_bytes = encrypt_decrypt(&expect_encrypted_bytes, n.obfuscation_key());

        assert_eq!(n.entry(), expect_bytes);
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

    // --- Go bee compatibility: Test_UnmarshalBinary edge cases ---

    #[test]
    fn unmarshal_nil_input() {
        let mut n = Node::default();
        let result = n.unmarshal_binary(&mut []);
        assert_eq!(result, Err(MantarayError::DataTooShort));
    }

    #[test]
    fn unmarshal_too_short_for_header() {
        let mut n = Node::default();
        let mut data = vec![0u8; crate::NODE_HEADER_SIZE - 1];
        let result = n.unmarshal_binary(&mut data);
        assert_eq!(result, Err(MantarayError::DataTooShort));
    }

    #[test]
    fn unmarshal_invalid_version_hash() {
        let mut n = Node::default();
        let mut data = vec![0u8; crate::NODE_HEADER_SIZE];
        let result = n.unmarshal_binary(&mut data);
        assert_eq!(result, Err(MantarayError::InvalidVersionHash));
    }

    /// Go bee test vector: valid manifest with correct metadata size (93 bytes).
    /// This is a v0.2 manifest with zero obfuscation key, a single fork at '/',
    /// and website-index-document metadata.
    #[test]
    fn unmarshal_valid_manifest_from_go() {
        let mut data = hex::decode(
            "00000000000000000000000000000000000000000000000000000000000000005768b3b6a7db56d21d1abff40d41cebfc83448fed8d7e9b06ec0d3b073f28f200000000000000000000000000000000000000000000000000000000000000000000000000080000000000000000000000000000000000000000000000000000016012f0000000000000000000000000000000000000000000000000000000000e87f95c3d081c4fede769b6c69e27b435e525cbd25c6715c607e7c531e329639005d7b22776562736974652d696e6465782d646f63756d656e74223a2233356561656538316262363338303436393965633637316265323736326465626665346662643330636461646139303232393239646131613965366134366436227d0a"
        ).unwrap();
        let mut n = Node::default();
        assert!(n.unmarshal_binary(&mut data).is_ok());
    }

    /// Go bee test vector: metadata size field says 89 but actual content needs 93.
    /// Should fail because there aren't enough bytes for the declared metadata.
    #[test]
    fn unmarshal_invalid_manifest_size_89() {
        let mut data = hex::decode(
            "00000000000000000000000000000000000000000000000000000000000000005768b3b6a7db56d21d1abff40d41cebfc83448fed8d7e9b06ec0d3b073f28f200000000000000000000000000000000000000000000000000000000000000000000000000080000000000000000000000000000000000000000000000000000016012f0000000000000000000000000000000000000000000000000000000000e87f95c3d081c4fede769b6c69e27b435e525cbd25c6715c607e7c531e32963900597b22776562736974652d696e6465782d646f63756d656e74223a2233356561656538316262363338303436393965633637316265323736326465626665346662643330636461646139303232393239646131613965366134366436227d0a"
        ).unwrap();
        let mut n = Node::default();
        assert!(n.unmarshal_binary(&mut data).is_err());
    }

    /// Go bee test vector: metadata size field says 95 but actual content is 93.
    /// Should fail because the size exceeds available bytes.
    #[test]
    fn unmarshal_invalid_manifest_size_95() {
        let mut data = hex::decode(
            "00000000000000000000000000000000000000000000000000000000000000005768b3b6a7db56d21d1abff40d41cebfc83448fed8d7e9b06ec0d3b073f28f200000000000000000000000000000000000000000000000000000000000000000000000000080000000000000000000000000000000000000000000000000000016012f0000000000000000000000000000000000000000000000000000000000e87f95c3d081c4fede769b6c69e27b435e525cbd25c6715c607e7c531e329639005f7b22776562736974652d696e6465782d646f63756d656e74223a2233356561656538316262363338303436393965633637316265323736326465626665346662643330636461646139303232393239646131613965366134366436227d0a"
        ).unwrap();
        let mut n = Node::default();
        assert!(n.unmarshal_binary(&mut data).is_err());
    }

    /// Go bee test vector: metadata size field says 96 but actual content is 93.
    /// Should fail because the size exceeds available bytes.
    #[test]
    fn unmarshal_invalid_manifest_size_96() {
        let mut data = hex::decode(
            "00000000000000000000000000000000000000000000000000000000000000005768b3b6a7db56d21d1abff40d41cebfc83448fed8d7e9b06ec0d3b073f28f200000000000000000000000000000000000000000000000000000000000000000000000000080000000000000000000000000000000000000000000000000000016012f0000000000000000000000000000000000000000000000000000000000e87f95c3d081c4fede769b6c69e27b435e525cbd25c6715c607e7c531e32963900607b22776562736974652d696e6465782d646f63756d656e74223a2233356561656538316262363338303436393965633637316265323736326465626665346662643330636461646139303232393239646131613965366134366436227d0a"
        ).unwrap();
        let mut n = Node::default();
        assert!(n.unmarshal_binary(&mut data).is_err());
    }

    /// Marshal then unmarshal round-trip preserves entries and metadata.
    #[test]
    fn marshal_unmarshal_round_trip() {
        let mut n = Node::new_unencrypted();

        for entry in test_entries() {
            let path = entry.path.as_bytes();
            let e: Vec<u8> = if path.len() <= 32 {
                let mut v = vec![0u8; 32 - path.len()];
                v.extend_from_slice(path);
                v
            } else {
                path.to_vec()
            };
            n.add::<Store, { DEFAULT_BODY_SIZE }>(path, &e, entry.metadata, None).unwrap();
        }

        // assign deterministic references to forks so marshal works
        let mut counter = 0u8;
        for fork in n.forks.values_mut() {
            let mut ref_ = vec![0u8; 32];
            ref_[31] = counter;
            fork.node.reference = ref_;
            counter += 1;
        }

        let marshalled = n.marshal_binary().unwrap();
        let mut n2 = Node::default();
        let mut data = marshalled;
        n2.unmarshal_binary(&mut data).unwrap();

        // Root has no entry; marshal writes zero bytes, unmarshal reads them back
        assert!(n2.entry().iter().all(|&b| b == 0));
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
