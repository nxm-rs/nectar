//! Feed identity and address derivation.

use alloy_primitives::{Address, B256, Keccak256};
use nectar_primitives::DEFAULT_BODY_SIZE;
use nectar_primitives::chunk::ChunkAddress;

use crate::index::Index;
use crate::topic::Topic;

/// A feed: the `{ topic, owner }` pair that names a mutable sequence of
/// owner-signed updates.
///
/// Every update lives at a deterministic single-owner chunk address derived
/// from the feed identity and an [`Index`]:
///
/// - `id = keccak256(topic || index.marshal())`
/// - `address = keccak256(id || owner)`
///
/// The body size `BS` is carried so address derivation and the reader/writer
/// agree on the chunk geometry, though feed addressing itself does not depend
/// on the body bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Feed<const BS: usize = DEFAULT_BODY_SIZE> {
    topic: Topic,
    owner: Address,
}

impl<const BS: usize> Feed<BS> {
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

    /// The single-owner chunk id for an update at `index`:
    /// `keccak256(topic || index.marshal())`.
    pub fn update_id<I: Index>(&self, index: &I) -> B256 {
        let mut hasher = Keccak256::new();
        hasher.update(self.topic.as_ref());
        hasher.update(index.marshal().as_ref());
        hasher.finalize()
    }

    /// The single-owner chunk address for an update at `index`:
    /// `keccak256(update_id || owner)`.
    pub fn update_address<I: Index>(&self, index: &I) -> ChunkAddress {
        let mut hasher = Keccak256::new();
        hasher.update(self.update_id(index));
        hasher.update(self.owner.as_slice());
        ChunkAddress::from(hasher.finalize())
    }
}
