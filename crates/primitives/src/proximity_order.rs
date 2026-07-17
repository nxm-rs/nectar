//! Typed Kademlia proximity order (PO) in the range `0..=MAX_PO`.
//!
//! See [`MAX_PO`] for the standard cap (31) and
//! [`xor_metric`](crate::xor_metric) for the derivation from two address-space points.

use crate::MAX_PO;
use derive_more::{Display, Into};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Typed proximity order, `0..=MAX_PO` (= 0..=31).
///
/// Distinguished from [`Bin`](crate::Bin) at the type level even though they
/// share a representation. PO is the metric, Bin is the routing-table slot.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, Into)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[display("po={_0}")]
#[into(u8, usize)]
pub struct ProximityOrder(u8);

/// Errors from constructing a [`ProximityOrder`].
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ProximityOrderError {
    /// Value exceeded [`MAX_PO`].
    #[error("proximity order {raw} exceeds MAX_PO ({max})")]
    OutOfRange {
        /// The rejected value.
        raw: u8,
        /// The maximum permitted value ([`MAX_PO`]).
        max: u8,
    },
}

impl ProximityOrder {
    /// The smallest proximity order.
    pub const MIN: Self = Self(0);

    /// The largest proximity order ([`MAX_PO`] = 31).
    pub const MAX: Self = Self(MAX_PO);

    /// Construct without bounds checking. Caller must ensure `raw <= MAX_PO`.
    ///
    /// Used by internal code that already validated the range (e.g.
    /// `XorMetric::proximity`, which can never exceed `MAX_PO`).
    #[inline]
    pub(crate) const fn new_unchecked(raw: u8) -> Self {
        debug_assert!(raw <= MAX_PO);
        Self(raw)
    }

    /// Construct from a raw byte, validating the range.
    #[inline]
    pub const fn new(raw: u8) -> Result<Self, ProximityOrderError> {
        if raw <= MAX_PO {
            Ok(Self(raw))
        } else {
            Err(ProximityOrderError::OutOfRange { raw, max: MAX_PO })
        }
    }

    /// Underlying byte value.
    #[inline]
    pub const fn get(self) -> u8 {
        self.0
    }
}

impl TryFrom<u8> for ProximityOrder {
    type Error = ProximityOrderError;

    fn try_from(raw: u8) -> Result<Self, Self::Error> {
        Self::new(raw)
    }
}

#[cfg(any(test, feature = "arbitrary"))]
impl<'a> arbitrary::Arbitrary<'a> for ProximityOrder {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        // Uniform over the representable range; every ProximityOrder is valid.
        Ok(Self(u.int_in_range(0..=MAX_PO)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn min_max_consts() {
        assert_eq!(ProximityOrder::MIN.get(), 0);
        assert_eq!(ProximityOrder::MAX.get(), MAX_PO);
    }

    #[test]
    fn new_in_range_ok() {
        assert!(ProximityOrder::new(0).is_ok());
        assert!(ProximityOrder::new(MAX_PO).is_ok());
    }

    #[test]
    fn new_out_of_range_errs() {
        let err = ProximityOrder::new(MAX_PO + 1).unwrap_err();
        assert!(matches!(
            err,
            ProximityOrderError::OutOfRange { raw: 32, max: 31 }
        ));
    }

    #[test]
    fn display_shows_po() {
        assert_eq!(format!("{}", ProximityOrder::new(7).unwrap()), "po=7");
    }

    #[test]
    fn try_from_byte() {
        let po: ProximityOrder = 5u8.try_into().unwrap();
        assert_eq!(po.get(), 5);
    }
}
