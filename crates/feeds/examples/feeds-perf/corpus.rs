//! Deterministic feed corpus: one fixed identity and a dense slot-address
//! table the presence store answers from.

use std::collections::HashMap;

use alloy_signer_local::PrivateKeySigner;
use nectar_feeds::{Feed, Sequence, Topic};
use nectar_primitives::chunk::ChunkAddress;

/// Fixed owner key: the corpus is fully determined by this key, the topic
/// label and the length.
const OWNER_KEY: [u8; 32] = [0x2a; 32];

/// Topic label of the measured feed.
pub const TOPIC_LABEL: &str = "finder-cost";

/// One feed identity plus the update addresses of slots `0..len`.
#[derive(Debug)]
pub struct Corpus {
    feed: Feed,
    signer: PrivateKeySigner,
    index_of: HashMap<ChunkAddress, u64>,
    len: u64,
}

impl Corpus {
    /// Build the address table for slots `0..len`.
    #[must_use]
    pub fn new(len: u64) -> Self {
        let signer = PrivateKeySigner::from_slice(&OWNER_KEY).expect("fixed key is a valid scalar");
        let feed = Feed::new(Topic::from_label(TOPIC_LABEL), signer.address());
        let mut index_of = HashMap::with_capacity(usize::try_from(len).unwrap_or_default());
        for i in 0..len {
            index_of.insert(feed.update_address(&Sequence::new(i)), i);
        }
        Self {
            feed,
            signer,
            index_of,
            len,
        }
    }

    /// The measured feed.
    #[must_use]
    pub const fn feed(&self) -> Feed {
        self.feed
    }

    /// The owner's signer, for building boundary updates.
    #[must_use]
    pub const fn signer(&self) -> &PrivateKeySigner {
        &self.signer
    }

    /// The slot index a probed address derives from, when it is in the table.
    #[must_use]
    pub fn slot(&self, address: &ChunkAddress) -> Option<u64> {
        self.index_of.get(address).copied()
    }

    /// Highest feed length this corpus can answer for.
    #[must_use]
    pub const fn len(&self) -> u64 {
        self.len
    }
}
