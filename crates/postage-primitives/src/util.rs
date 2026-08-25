//! Chain state shared by the postage validation paths.

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
    use super::*;

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
