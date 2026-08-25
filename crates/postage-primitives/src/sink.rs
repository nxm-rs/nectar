//! The stamped chunk as a unit of transfer.
//!
//! The metadata half of the put seam: a [`StampedChunk`] names its storage
//! address, so it is a valid put unit. The store-facing bridge
//! (`StampIndifferent`) lives in `nectar-postage`.

use nectar_primitives::{ChunkAddress, PutUnit, Verified};

use crate::{StampedChunk, ValidationState};

impl<V: ValidationState, const BODY_SIZE: usize> PutUnit for StampedChunk<Verified, V, BODY_SIZE> {
    #[inline]
    fn address(&self) -> &ChunkAddress {
        Self::address(self)
    }
}
