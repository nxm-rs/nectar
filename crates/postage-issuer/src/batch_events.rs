//! Decoded `IPostageStamp` batch logs to the domain [`BatchEvent`].
//!
//! The block number comes from the log envelope, not the event body, so the
//! constructors take it. `Expired` has no wire shape: the contract emits no
//! expiry event.

use alloy_primitives::U256;
use nectar_contracts::IPostageStamp;
use nectar_postage::{Batch, BatchEvent, BatchId, BucketDepth, StampError};

/// The decoded log does not fit the domain shape.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum EventError {
    /// The event's `bucketDepth` is outside the spec range.
    #[error(transparent)]
    BucketDepth(#[from] StampError),

    /// The event's `normalisedBalance` does not fit a `u128`.
    #[error("normalised balance exceeds u128")]
    BalanceOverflow {
        /// The balance as the event carried it.
        balance: U256,
    },
}

impl EventError {
    /// Whether a retry over the same log can succeed: both variants are
    /// permanent log-validation failures.
    pub const fn is_permanent(&self) -> bool {
        match self {
            Self::BucketDepth(_) | Self::BalanceOverflow { .. } => true,
        }
    }
}

fn balance_to_u128(balance: U256) -> Result<u128, EventError> {
    u128::try_from(balance).map_err(|_| EventError::BalanceOverflow { balance })
}

/// Translates a decoded `BatchCreated` log. `block` is the envelope's block
/// and becomes the batch `start`. The event `totalAmount` is not carried:
/// the domain keys the batch by its normalised balance.
///
/// # Errors
///
/// [`EventError::BucketDepth`] for an out-of-spec `bucketDepth`;
/// [`EventError::BalanceOverflow`] when the balance does not fit a `u128`.
pub fn created(log: &IPostageStamp::BatchCreated, block: u64) -> Result<BatchEvent, EventError> {
    Ok(BatchEvent::Created {
        batch: Batch::new(
            log.batchId.into(),
            balance_to_u128(log.normalisedBalance)?,
            block,
            log.owner,
            log.depth,
            BucketDepth::new(log.bucketDepth)?,
            log.immutableFlag,
        ),
    })
}

/// Translates a decoded `BatchTopUp` log. The event `topupAmount` is not
/// carried: the store updates the normalised balance from `new_value`.
///
/// # Errors
///
/// [`EventError::BalanceOverflow`] when the balance does not fit a `u128`.
pub fn top_up(log: &IPostageStamp::BatchTopUp) -> Result<BatchEvent, EventError> {
    Ok(BatchEvent::TopUp {
        batch_id: log.batchId.into(),
        new_value: balance_to_u128(log.normalisedBalance)?,
    })
}

/// Translates a decoded `BatchDepthIncrease` log. `block` is the envelope's
/// block, which the issuance gate counts confirmations from.
///
/// # Errors
///
/// [`EventError::BalanceOverflow`] when the balance does not fit a `u128`.
pub fn depth_increase(
    log: &IPostageStamp::BatchDepthIncrease,
    block: u64,
) -> Result<BatchEvent, EventError> {
    Ok(BatchEvent::DepthIncrease {
        batch_id: log.batchId.into(),
        new_depth: log.newDepth,
        new_value: balance_to_u128(log.normalisedBalance)?,
        block,
    })
}

/// Builds the node-derived [`BatchEvent::Expired`]: the contract emits no
/// expiry event.
#[must_use]
pub const fn expired(batch_id: BatchId) -> BatchEvent {
    BatchEvent::Expired { batch_id }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256, U256};

    fn created_log(balance: U256, bucket_depth: u8) -> IPostageStamp::BatchCreated {
        IPostageStamp::BatchCreated {
            batchId: B256::ZERO,
            totalAmount: U256::from(1_000),
            normalisedBalance: balance,
            owner: Address::ZERO,
            depth: 20,
            bucketDepth: bucket_depth,
            immutableFlag: false,
        }
    }

    #[test]
    fn created_translates_the_event_body_and_takes_block_from_the_envelope() {
        let event = created(&created_log(U256::from(500), 16), 7_000).unwrap();
        let BatchEvent::Created { batch } = event else {
            panic!("expected a created event");
        };
        assert_eq!(batch.id(), BatchId::ZERO);
        assert_eq!(batch.value(), 500);
        assert_eq!(batch.start(), 7_000);
        assert_eq!(batch.depth(), 20);
        assert!(!batch.immutable());
    }

    #[test]
    fn top_up_carries_the_normalised_balance() {
        let event = top_up(&IPostageStamp::BatchTopUp {
            batchId: B256::ZERO,
            topupAmount: U256::from(42),
            normalisedBalance: U256::from(900),
        })
        .unwrap();
        let BatchEvent::TopUp {
            batch_id,
            new_value,
        } = event
        else {
            panic!("expected a top up event");
        };
        assert_eq!(batch_id, BatchId::ZERO);
        assert_eq!(new_value, 900);
    }

    #[test]
    fn depth_increase_carries_the_rescaled_balance_and_envelope_block() {
        let event = depth_increase(
            &IPostageStamp::BatchDepthIncrease {
                batchId: B256::ZERO,
                newDepth: 21,
                normalisedBalance: U256::from(250),
            },
            8_000,
        )
        .unwrap();
        let BatchEvent::DepthIncrease {
            batch_id,
            new_depth,
            new_value,
            block,
        } = event
        else {
            panic!("expected a depth increase event");
        };
        assert_eq!(batch_id, BatchId::ZERO);
        assert_eq!(new_depth, 21);
        assert_eq!(new_value, 250);
        assert_eq!(block, 8_000);
    }

    #[test]
    fn expired_is_node_derived() {
        let event = expired(BatchId::ZERO);
        assert_eq!(
            event,
            BatchEvent::Expired {
                batch_id: BatchId::ZERO
            }
        );
    }

    #[test]
    fn balance_overflow_is_permanent() {
        let balance = U256::from(u128::MAX) + U256::from(1);
        let err = top_up(&IPostageStamp::BatchTopUp {
            batchId: B256::ZERO,
            topupAmount: U256::ONE,
            normalisedBalance: balance,
        })
        .unwrap_err();
        assert!(matches!(err, EventError::BalanceOverflow { .. }));
        assert!(err.is_permanent());
    }

    #[test]
    fn bucket_depth_out_of_spec_is_permanent() {
        let err = created(&created_log(U256::from(1), 0), 0).unwrap_err();
        assert!(matches!(err, EventError::BucketDepth(_)));
        // Guards the exhaustive is_permanent match.
        assert!(err.is_permanent());
    }
}
