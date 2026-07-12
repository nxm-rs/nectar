//! Typed Kademlia bin index in the range `0..=MAX_PO`.
//!
//! A [`Bin`] is a routing-table slot keyed by the proximity order between an
//! anchor address (e.g. our own overlay) and a peer's overlay. Distinguished
//! from [`ProximityOrder`] at the type level even
//! though they share a representation. PO is the metric, Bin is the slot.

use crate::{MAX_PO, ProximityOrder};
use derive_more::{Display, Into};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Typed Kademlia bin index, `0..=MAX_PO` (= 0..=31).
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, Into)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[display("bin={_0}")]
#[into(u8, usize)]
pub struct Bin(u8);

/// Errors from constructing a [`Bin`].
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BinError {
    /// Value exceeded [`MAX_PO`].
    #[error("bin index {raw} exceeds MAX_PO ({max})")]
    OutOfRange {
        /// The rejected value.
        raw: u8,
        /// The maximum permitted value.
        max: u8,
    },
}

impl Bin {
    /// The first (shallowest) bin.
    pub const ZERO: Self = Self(0);

    /// The deepest bin ([`MAX_PO`] = 31).
    pub const MAX: Self = Self(MAX_PO);

    /// The number of bins in the routing table (`MAX_PO + 1` = 32).
    // `u8 -> usize` widening is infallible; `usize::from` is not
    // const-callable.
    #[allow(clippy::as_conversions)]
    pub const COUNT: usize = MAX_PO as usize + 1;

    /// Construct without bounds checking. Caller must ensure `raw <= MAX_PO`.
    #[inline]
    pub(crate) const fn new_unchecked(raw: u8) -> Self {
        debug_assert!(raw <= MAX_PO);
        Self(raw)
    }

    /// Construct from a raw byte, validating the range.
    #[inline]
    pub const fn new(raw: u8) -> Result<Self, BinError> {
        if raw <= MAX_PO {
            Ok(Self(raw))
        } else {
            Err(BinError::OutOfRange { raw, max: MAX_PO })
        }
    }

    /// Underlying byte value.
    #[inline]
    pub const fn get(self) -> u8 {
        self.0
    }

    /// Index into a `[T; Bin::COUNT]` array.
    // `u8 -> usize` widening is infallible; `usize::from` is not
    // const-callable.
    #[allow(clippy::as_conversions)]
    #[inline]
    pub const fn as_index(self) -> usize {
        self.0 as usize
    }
}

impl TryFrom<u8> for Bin {
    type Error = BinError;

    fn try_from(raw: u8) -> Result<Self, Self::Error> {
        Self::new(raw)
    }
}

impl From<ProximityOrder> for Bin {
    /// Every valid [`ProximityOrder`] is a valid [`Bin`] (they share a range).
    /// Use this when crossing the metric/slot semantic boundary.
    fn from(po: ProximityOrder) -> Self {
        // PO is range-validated, so this is sound.
        Self(po.get())
    }
}

#[cfg(any(test, feature = "arbitrary"))]
impl<'a> arbitrary::Arbitrary<'a> for Bin {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        // Uniform over the representable range; every Bin is valid.
        Ok(Self(u.int_in_range(0..=MAX_PO)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn min_max_count() {
        assert_eq!(Bin::ZERO.get(), 0);
        assert_eq!(Bin::MAX.get(), MAX_PO);
        assert_eq!(Bin::COUNT, MAX_PO as usize + 1);
    }

    #[test]
    fn new_in_range_ok() {
        assert!(Bin::new(0).is_ok());
        assert!(Bin::new(MAX_PO).is_ok());
    }

    #[test]
    fn new_out_of_range_errs() {
        let err = Bin::new(MAX_PO + 1).unwrap_err();
        assert!(matches!(err, BinError::OutOfRange { raw: 32, max: 31 }));
    }

    #[test]
    fn from_proximity_order() {
        let po = ProximityOrder::new(7).unwrap();
        let bin = Bin::from(po);
        assert_eq!(bin.get(), 7);
    }

    #[test]
    fn display_shows_bin() {
        assert_eq!(format!("{}", Bin::new(3).unwrap()), "bin=3");
    }
}
