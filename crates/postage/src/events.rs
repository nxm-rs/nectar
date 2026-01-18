//! Batch event types for monitoring blockchain events.
//!
//! This module provides types for handling postage batch events from the blockchain.
//! Any node that maintains a batch store (for stamp validation) needs to handle
//! these events to keep their batch state synchronized with on-chain state.

use crate::{Batch, BatchId};

/// Events emitted by the postage stamp contract.
///
/// These events represent state changes to batches on-chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchEvent {
    /// A new batch was created.
    Created {
        /// The batch that was created.
        batch: Batch,
    },

    /// A batch was topped up with additional funds.
    TopUp {
        /// The batch ID.
        batch_id: BatchId,
        /// The new normalized balance.
        new_value: u128,
    },

    /// A batch was diluted (depth increased).
    DepthIncrease {
        /// The batch ID.
        batch_id: BatchId,
        /// The new depth.
        new_depth: u8,
    },

    /// A batch expired.
    Expired {
        /// The batch ID.
        batch_id: BatchId,
    },
}

impl BatchEvent {
    /// Returns the batch ID associated with this event.
    pub const fn batch_id(&self) -> BatchId {
        match self {
            Self::Created { batch } => batch.id(),
            Self::TopUp { batch_id, .. } => *batch_id,
            Self::DepthIncrease { batch_id, .. } => *batch_id,
            Self::Expired { batch_id } => *batch_id,
        }
    }
}

/// A handler for batch events.
///
/// Implementations receive events as they are processed from the blockchain
/// and update internal state accordingly.
pub trait BatchEventHandler {
    /// The error type returned when handling fails.
    type Error;

    /// Handles a batch event.
    ///
    /// Implementations should update any internal state (e.g., batch store)
    /// based on the event.
    fn handle_event(&mut self, event: BatchEvent) -> Result<(), Self::Error>;

    /// Handles a batch of events atomically.
    ///
    /// The default implementation calls `handle_event` for each event,
    /// but implementations may override this for better performance
    /// or transactional semantics.
    fn handle_events(&mut self, events: Vec<BatchEvent>) -> Result<(), Self::Error> {
        for event in events {
            self.handle_event(event)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256};

    #[test]
    fn test_batch_event_batch_id() {
        let batch = Batch::new(
            B256::repeat_byte(1),
            1000,
            100,
            Address::ZERO,
            20,
            16,
            false,
        );
        let batch_id = batch.id();

        let created = BatchEvent::Created { batch };
        assert_eq!(created.batch_id(), batch_id);

        let topup = BatchEvent::TopUp {
            batch_id,
            new_value: 2000,
        };
        assert_eq!(topup.batch_id(), batch_id);

        let depth = BatchEvent::DepthIncrease {
            batch_id,
            new_depth: 21,
        };
        assert_eq!(depth.batch_id(), batch_id);

        let expired = BatchEvent::Expired { batch_id };
        assert_eq!(expired.batch_id(), batch_id);
    }
}
