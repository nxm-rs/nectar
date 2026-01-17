//! Utility functions for postage operations.

use nectar_primitives::SwarmAddress;

/// Calculates which collision bucket a chunk belongs to based on its address.
///
/// The bucket is determined by taking the first `bucket_depth` bits of the
/// chunk address, interpreted as a big-endian unsigned integer.
///
/// # Arguments
///
/// * `address` - The chunk's Swarm address
/// * `bucket_depth` - The number of leading bits to use (from the batch configuration)
///
/// # Returns
///
/// The bucket number (0 to 2^bucket_depth - 1)
///
/// # Example
///
/// ```
/// use nectar_postage::calculate_bucket;
/// use nectar_primitives::SwarmAddress;
///
/// let address = SwarmAddress::new([0xCB, 0xE5, 0x00, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
/// let bucket = calculate_bucket(&address, 16);
/// assert_eq!(bucket, 0xCBE5);
/// ```
#[inline]
pub fn calculate_bucket(address: &SwarmAddress, bucket_depth: u8) -> u32 {
    // Take the first 4 bytes as a big-endian u32
    let leading = u32::from_be_bytes(address.as_bytes()[0..4].try_into().unwrap());
    // Shift right to get only the top `bucket_depth` bits
    leading >> (32 - bucket_depth)
}

/// Represents the current blockchain state relevant to batch expiry calculations.
///
/// The chain state is used to determine whether batches are expired or usable
/// based on the current cumulative payout and block number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ChainState {
    /// The current block number.
    block: u64,
    /// The cumulative payout per chunk (total_amount in Bee terminology).
    ///
    /// This represents the total amount that has been distributed to storage providers
    /// per chunk up to this point. A batch expires when its value (balance per chunk)
    /// is less than or equal to this amount.
    total_amount: u128,
}

impl ChainState {
    /// Creates a new chain state.
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
    pub fn set_block(&mut self, block: u64) {
        self.block = block;
    }

    /// Updates the total amount.
    #[inline]
    pub fn set_total_amount(&mut self, total_amount: u128) {
        self.total_amount = total_amount;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_bucket() {
        // Address starting with 0xCBE5...
        let address = SwarmAddress::new([
            0xCB, 0xE5, 0x00, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0,
        ]);

        // With bucket_depth=16, we should get 0xCBE5
        assert_eq!(calculate_bucket(&address, 16), 0xCBE5);

        // With bucket_depth=8, we should get 0xCB
        assert_eq!(calculate_bucket(&address, 8), 0xCB);

        // With bucket_depth=4, we should get 0xC
        assert_eq!(calculate_bucket(&address, 4), 0xC);
    }

    #[test]
    fn test_chain_state() {
        let mut state = ChainState::new(100, 5000);

        assert_eq!(state.block(), 100);
        assert_eq!(state.total_amount(), 5000);

        state.set_block(200);
        state.set_total_amount(10000);

        assert_eq!(state.block(), 200);
        assert_eq!(state.total_amount(), 10000);
    }

    #[test]
    fn test_chain_state_default() {
        let state = ChainState::default();
        assert_eq!(state.block(), 0);
        assert_eq!(state.total_amount(), 0);
    }
}
