//! Feed identity and address derivation.

use alloy_primitives::{Address, Keccak256};
use nectar_primitives::DEFAULT_BODY_SIZE;
use nectar_primitives::chunk::{ChunkAddress, SocId};

use crate::index::Index;
use crate::topic::Topic;

/// Feed identity: the `(topic, owner)` pair.
///
/// `BODY_SIZE` fixes the chunk geometry the getter and updater operate at;
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
    use alloy_primitives::keccak256;
    use nectar_primitives::DEFAULT_BODY_SIZE;
    use proptest::prelude::*;
    use proptest_arbitrary_interop::arb;

    use crate::Sequence;

    use super::*;

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
