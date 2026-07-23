//! Shared fixtures for the file-pipeline and manifest fuzz targets.

use arbitrary::Arbitrary;
use bytes::Bytes;
use nectar_manifest::{Entry, Format, V1};
use nectar_primitives::ChunkRef;
use nectar_primitives::chunk::ChunkAddress;

/// Upper bound on a tiled input length.
pub const MAX_LEN: usize = 32 * 1024;

/// Tile `seed` to `copies` repetitions, capped at [`MAX_LEN`] bytes.
pub fn tile(seed: &[u8], copies: u16) -> Vec<u8> {
    if seed.is_empty() {
        return Vec::new();
    }
    let len = seed
        .len()
        .saturating_mul(usize::from(copies.max(1)))
        .min(MAX_LEN);
    seed.iter().copied().cycle().take(len).collect()
}

/// One fuzzed manifest value: a plain reference or an inline byte string.
///
/// Compact by design: a reference costs one input byte, so mutation explores
/// trie structure rather than address space. The general value model is the
/// `Entry` impl and `nectar_manifest::generators`.
#[derive(Arbitrary, Clone, Debug)]
pub enum Val {
    /// A 32-byte reference synthesized from one byte.
    Ref(u8),
    /// An inline value; capped to the format bound before insertion.
    Inline(Vec<u8>),
}

/// Turn a fuzzed value into an entry, capping an inline value at the format
/// bound.
pub fn entry(val: Val) -> Entry<V1> {
    match val {
        Val::Ref(byte) => Entry::from(ChunkRef::new(ChunkAddress::new([byte; 32]))),
        Val::Inline(mut bytes) => {
            bytes.truncate(V1::VINLINE_MAX);
            Entry::inline(Bytes::from(bytes))
                .unwrap_or_else(|_| Entry::from(ChunkRef::new(ChunkAddress::new([0; 32]))))
        }
    }
}
