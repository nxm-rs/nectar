//! Split-side encrypted sealing: keccak counter-mode bodies keyed through a
//! [`KeySource`]. Joining encrypted references stays unconditional; only
//! this encoding side sits behind the `encryption` feature.

use alloc::vec::Vec;

use bytes::Bytes;
use nectar_primitives::chunk::encryption::{ChunkEncrypt, EncryptedChunkRef, EncryptionKey};
use nectar_primitives::chunk::{AnyChunkSet, ContentChunk};
use nectar_primitives::store::{MaybeSend, MaybeSync};

use super::error::SealError;
use super::mode::{Sealed, SplitMode};
use crate::geometry::Mode;
use crate::walk::Encrypted;

/// Key supply for the encrypted split; one fresh key seals one chunk.
///
/// Sources are shared handles: a clone draws from the same stream, so keys
/// stay unique across the streaming ascent and pool workers.
pub trait KeySource: MaybeSend + MaybeSync + 'static {
    /// The key sealing the next chunk.
    fn next_key(&self) -> Result<EncryptionKey, KeyError>;
}

/// Typed key-supply failure.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum KeyError {
    /// A finite source ran out of keys.
    #[error("key source exhausted after {issued} keys")]
    Exhausted {
        /// Keys issued before the supply ran out.
        issued: u64,
    },
}

/// Default source: an independent random key per chunk.
///
/// ```
/// use nectar_file::{Encrypted, RandomKeys, Split};
/// use nectar_primitives::chunk::AnyChunkSet;
/// use nectar_primitives::store::MemoryStore;
///
/// # nectar_testing::run(async {
/// let store = MemoryStore::<AnyChunkSet<4096>>::new();
/// let root = Split::<_, Encrypted<RandomKeys>, 4096>::collect(store, b"secret")
///     .await
///     .unwrap();
/// assert_eq!(root.to_bytes().len(), 64);
/// # });
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RandomKeys;

impl KeySource for RandomKeys {
    fn next_key(&self) -> Result<EncryptionKey, KeyError> {
        Ok(EncryptionKey::generate())
    }
}

/// Encrypted split: each payload is sealed under a fresh key from the
/// source, and a reference carries the ciphertext address plus that key.
impl<K: KeySource> SplitMode for Encrypted<K> {
    const MODE: Mode = Mode::Encrypted;

    type Ref = EncryptedChunkRef;
    type Root = EncryptedChunkRef;

    fn data_slots(branches: u64) -> u64 {
        branches
    }

    fn seal<const B: usize>(&self, payload: Bytes) -> Result<Sealed<Self, B>, SealError> {
        let key = self.source().next_key()?;
        let (chunk, reference) = ContentChunk::<B>::try_from(payload)?
            .encrypt_with(&key)?
            .into_parts();
        Ok((chunk.seal::<AnyChunkSet<B>>(), reference))
    }

    fn write_ref(reference: &EncryptedChunkRef, out: &mut Vec<u8>) {
        out.extend_from_slice(&reference.to_bytes());
    }

    fn into_root(reference: EncryptedChunkRef) -> EncryptedChunkRef {
        reference
    }
}
