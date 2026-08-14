//! Utility functions for postage operations.

use nectar_primitives::{ChunkAddress, SwarmSpec};

use crate::BucketDepth;

/// Returns the collision bucket of `address`: its leading `bucket_depth` bits
/// read big-endian.
///
/// # Example
///
/// ```
/// use nectar_postage::{BucketDepth, calculate_bucket};
/// use nectar_primitives::{ChunkAddress, Mainnet};
///
/// let address = ChunkAddress::new([0xCB, 0xE5, 0x00, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
/// let bucket_depth = BucketDepth::<Mainnet>::new(16).unwrap();
/// assert_eq!(calculate_bucket(&address, bucket_depth), 0xCBE5);
/// ```
///
/// An unvalidated depth does not compile:
///
/// ```compile_fail
/// use nectar_postage::calculate_bucket;
/// use nectar_primitives::ChunkAddress;
///
/// calculate_bucket(&ChunkAddress::ZERO, 0u8);
/// ```
#[inline]
pub fn calculate_bucket<S: SwarmSpec>(address: &ChunkAddress, bucket_depth: BucketDepth<S>) -> u32 {
    // A `ChunkAddress` is 32 bytes, so the split always yields a head.
    let leading = address
        .as_bytes()
        .split_first_chunk::<4>()
        .map_or(0, |(head, _)| u32::from_be_bytes(*head));
    // A `BucketDepth` is 1..=32, so the shift is at most 31 and never wraps.
    leading.wrapping_shr(u32::from(
        BucketDepth::<S>::MAX.saturating_sub(bucket_depth.get()),
    ))
}

/// Context for postage validation.
///
/// Contains the current state needed to determine whether batches are expired
/// or usable. This data may come from a blockchain, database, or any other source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PostageContext {
    /// The current block number (or equivalent time reference).
    block: u64,
    /// The cumulative payout per chunk.
    ///
    /// This represents the total amount that has been distributed to storage providers
    /// per chunk up to this point. A batch expires when its value (balance per chunk)
    /// is less than or equal to this amount.
    total_amount: u128,
}

impl PostageContext {
    /// Creates a new postage context.
    #[inline]
    pub const fn new(block: u64, total_amount: u128) -> Self {
        Self {
            block,
            total_amount,
        }
    }

    /// Returns the current block number.
    #[inline]
    pub const fn block(&self) -> u64 {
        self.block
    }

    /// Returns the cumulative payout per chunk.
    #[inline]
    pub const fn total_amount(&self) -> u128 {
        self.total_amount
    }

    /// Updates the block number.
    #[inline]
    pub const fn set_block(&mut self, block: u64) {
        self.block = block;
    }

    /// Updates the total amount.
    #[inline]
    pub const fn set_total_amount(&mut self, total_amount: u128) {
        self.total_amount = total_amount;
    }
}

#[cfg(test)]
mod tests {
    use nectar_primitives::Mainnet;
    use nectar_testing::LowFloor;

    use super::*;

    fn address_cbe5() -> ChunkAddress {
        ChunkAddress::new([
            0xCB, 0xE5, 0x00, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0,
        ])
    }

    #[test]
    fn test_calculate_bucket() {
        let address = address_cbe5();

        assert_eq!(
            calculate_bucket(&address, BucketDepth::<Mainnet>::new(16).unwrap()),
            0xCBE5
        );
        assert_eq!(
            calculate_bucket(&address, BucketDepth::<LowFloor>::new(8).unwrap()),
            0xCB
        );
        assert_eq!(
            calculate_bucket(&address, BucketDepth::<LowFloor>::new(4).unwrap()),
            0xC
        );
    }

    #[test]
    fn calculate_bucket_spans_the_whole_depth_range() {
        let address = address_cbe5();

        // Depth 1 is the shallowest a `BucketDepth` can hold and shifts by 31;
        // depth 32 shifts by 0 and keeps the whole leading word.
        assert_eq!(
            calculate_bucket(&address, BucketDepth::<LowFloor>::new(1).unwrap()),
            1
        );
        assert_eq!(
            calculate_bucket(&address, BucketDepth::<LowFloor>::new(32).unwrap()),
            0xCBE5_0000
        );
    }

    #[test]
    fn test_chain_state() {
        let mut state = PostageContext::new(100, 5000);

        assert_eq!(state.block(), 100);
        assert_eq!(state.total_amount(), 5000);

        state.set_block(200);
        state.set_total_amount(10000);

        assert_eq!(state.block(), 200);
        assert_eq!(state.total_amount(), 10000);
    }

    #[test]
    fn test_chain_state_default() {
        let state = PostageContext::default();
        assert_eq!(state.block(), 0);
        assert_eq!(state.total_amount(), 0);
    }
}
