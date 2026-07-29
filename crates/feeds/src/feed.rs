//! Feed identity, topic and address derivation.

use alloy_primitives::{Address, B256, Keccak256, keccak256};
use derive_more::{AsRef, Display, From, Into};
use nectar_primitives::DEFAULT_BODY_SIZE;
use nectar_primitives::chunk::{ChunkAddress, SocId};

use crate::index::Index;

/// 32-byte feed topic, mixed raw (never hashed again) into every update id.
///
/// Nominally distinct from the hash it wraps: a bare `B256` is rejected
/// where a `Topic` is expected. Arbitrary-length labels go through
/// [`from_label`](Self::from_label); [`new`](Self::new) wraps 32 bytes
/// verbatim.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into, AsRef)]
#[display("{_0}")]
#[from(B256, [u8; 32])]
#[into(B256, [u8; 32])]
#[as_ref([u8])]
#[repr(transparent)]
pub struct Topic(B256);

impl Topic {
    /// Zero topic, useful for tests and deterministic vectors.
    pub const ZERO: Self = Self(B256::ZERO);

    /// Construct from raw 32 bytes. `const` for static contexts; for runtime
    /// conversions prefer the `From` impls.
    #[inline]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(B256::new(bytes))
    }

    /// Derive a topic from an arbitrary-length label: `keccak256(label)`.
    pub fn from_label(label: impl AsRef<[u8]>) -> Self {
        Self(keccak256(label))
    }

    /// Borrow the underlying 32 bytes.
    #[inline]
    pub const fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }
}

#[cfg(any(test, feature = "arbitrary"))]
impl<'a> arbitrary::Arbitrary<'a> for Topic {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self::new(u.arbitrary()?))
    }
}

/// Feed identity: the `(topic, owner)` pair.
///
/// `BODY_SIZE` fixes the chunk geometry the reader and publisher operate at;
/// the address derivation itself is body-independent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Feed<const BODY_SIZE: usize = DEFAULT_BODY_SIZE> {
    topic: Topic,
    owner: Address,
}

impl<const BODY_SIZE: usize> Feed<BODY_SIZE> {
    /// Create a feed from its topic and owner.
    pub const fn new(topic: Topic, owner: Address) -> Self {
        Self { topic, owner }
    }

    /// The feed topic.
    pub const fn topic(&self) -> Topic {
        self.topic
    }

    /// The feed owner.
    pub const fn owner(&self) -> Address {
        self.owner
    }

    /// The single-owner chunk id for the update at `index`:
    /// `keccak256(topic || index.marshal())`.
    pub fn update_id<I: Index>(&self, index: &I) -> SocId {
        let mut hasher = Keccak256::new();
        hasher.update(self.topic.as_slice());
        hasher.update(index.marshal().as_ref());
        SocId::from(hasher.finalize())
    }

    /// The plain single-owner chunk address for the update at `index`:
    /// `keccak256(update_id || owner)`.
    pub fn update_address<I: Index>(&self, index: &I) -> ChunkAddress {
        let mut hasher = Keccak256::new();
        hasher.update(self.update_id(index).as_slice());
        hasher.update(self.owner.as_slice());
        ChunkAddress::from(hasher.finalize())
    }
}

#[cfg(test)]
mod tests {
    use nectar_primitives::DEFAULT_BODY_SIZE;
    use proptest::prelude::*;
    use proptest_arbitrary_interop::arb;

    use crate::Sequence;

    use super::*;

    #[test]
    fn from_label_is_keccak_of_label() {
        assert_eq!(Topic::from_label("abc"), Topic::from(keccak256("abc")));
    }

    #[test]
    fn new_wraps_verbatim() {
        let raw = [0x5au8; 32];
        assert_eq!(Topic::new(raw).as_slice(), &raw);
        assert_eq!(Topic::ZERO.as_slice(), &[0u8; 32]);
    }

    proptest! {
        /// Derivation oracle: both hashes recomputed by hand over the raw
        /// preimages.
        #[test]
        fn derivation_matches_manual_keccak(
            topic in arb::<Topic>(),
            owner in arb::<Address>(),
            n in any::<u64>(),
        ) {
            let feed = Feed::<DEFAULT_BODY_SIZE>::new(topic, owner);
            let index = Sequence::new(n);

            let mut preimage = Vec::new();
            preimage.extend_from_slice(topic.as_slice());
            preimage.extend_from_slice(&n.to_be_bytes());
            let id = keccak256(&preimage);
            prop_assert_eq!(feed.update_id(&index), SocId::from(id));

            let mut preimage = Vec::new();
            preimage.extend_from_slice(id.as_slice());
            preimage.extend_from_slice(owner.as_slice());
            let address = keccak256(&preimage);
            prop_assert_eq!(feed.update_address(&index), ChunkAddress::from(address));
        }

        /// Distinct indices land on distinct slots.
        #[test]
        fn adjacent_indices_diverge(topic in arb::<Topic>(), owner in arb::<Address>(), n in 0..u64::MAX) {
            let feed = Feed::<DEFAULT_BODY_SIZE>::new(topic, owner);
            prop_assert_ne!(
                feed.update_address(&Sequence::new(n)),
                feed.update_address(&Sequence::new(n + 1))
            );
        }
    }
}
