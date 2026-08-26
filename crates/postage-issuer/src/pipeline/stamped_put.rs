//! The stamped-put error and the issued-map bound, shared by the staged put.

use core::num::NonZeroUsize;

use nectar_postage::StampError;
use nectar_primitives::StoreError;

use crate::error::SigningError;

/// Memory switch for the issued map (~145 B per unique address).
///
/// Anything below full tracking reintroduces duplicate allocation: an
/// untracked duplicate burns a fresh index, and a repetitive region can
/// refuse with `BucketFull`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IssuedBound {
    /// Track every unique address.
    #[default]
    Unbounded,
    /// Track at most this many addresses; later addresses go untracked.
    AtMost(NonZeroUsize),
    /// Track nothing.
    Off,
}

impl IssuedBound {
    /// Whether a new address enters a tracking set already holding `tracked`.
    pub const fn tracks(self, tracked: usize) -> bool {
        match self {
            Self::Off => false,
            Self::Unbounded => true,
            Self::AtMost(bound) => tracked < bound.get(),
        }
    }
}

/// A stamped put failure.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum StampedPutError<E> {
    /// Index allocation refused; no index consumed, a retry is free.
    #[error(transparent)]
    Stamp(#[from] StampError),
    /// Signing failed; the allocated index is burnt.
    #[error("stamp signing failed")]
    Sign(#[source] SigningError),
    /// The sink refused the pair; the signed stamp is retained for reuse.
    #[error("stamped sink refused the pair")]
    Put(#[source] E),
    /// An earlier failure has already surfaced; the decorator is shut.
    #[error("stamping is poisoned by an earlier failure")]
    Poisoned,
}

impl<E: StoreError> StoreError for StampedPutError<E> {
    /// Only the put leg names a medium condition; a refused allocation, a
    /// failed signature or a poisoned decorator is terminal.
    fn is_definitely_absent(&self) -> bool {
        match self {
            Self::Put(error) => error.is_definitely_absent(),
            Self::Stamp(_) | Self::Sign(_) | Self::Poisoned => false,
        }
    }

    fn is_transient(&self) -> bool {
        match self {
            Self::Put(error) => error.is_transient(),
            Self::Stamp(_) | Self::Sign(_) | Self::Poisoned => false,
        }
    }
}
