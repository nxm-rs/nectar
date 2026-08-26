//! The stamped chunk as a unit of transfer.
//!
//! The metadata half of the put seam: a [`StampedChunk`] names its storage
//! address, so it is a valid put unit. The store-facing bridge
//! (`StampIndifferent`) lives in `nectar-postage`.

use nectar_primitives::{ChunkAddress, DEFAULT_BODY_SIZE, PutUnit, Verified};

use crate::{Stamp, StampedChunk, Unvalidated, ValidationState};

impl<V: ValidationState, const BODY_SIZE: usize> PutUnit for StampedChunk<Verified, V, BODY_SIZE> {
    type Validation = Stamp;

    #[inline]
    fn address(&self) -> &ChunkAddress {
        Self::address(self)
    }
}

const _VALIDATION_IS_STAMP: fn(
    <StampedChunk<Verified, Unvalidated, DEFAULT_BODY_SIZE> as PutUnit>::Validation,
) -> Stamp = {
    const fn unit(validation: Stamp) -> Stamp {
        validation
    }
    unit
};
