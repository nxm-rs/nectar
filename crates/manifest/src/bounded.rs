//! Bounded newtypes: each checks a [`Format`] bound once at construction and
//! carries it as a type invariant, so use sites need no runtime check.

use core::marker::PhantomData;

use bytes::Bytes;

use crate::error::{MetadataTooLong, PrefixTooLong, WeightOverBudget};
use crate::format::{Format, V1};

/// A fork prefix: at most `F::PLEN_MAX` bytes, checked once here.
///
/// Cheap to clone (shared [`Bytes`]). The empty prefix is representable;
/// requiring non-empty prefixes on wire forks is the codec's job.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Prefix<F: Format = V1> {
    bytes: Bytes,
    _format: PhantomData<F>,
}

impl<F: Format> Prefix<F> {
    /// The empty prefix.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            bytes: Bytes::new(),
            _format: PhantomData,
        }
    }

    /// Wrap `bytes` as a prefix, rejecting lengths above `F::PLEN_MAX`.
    pub fn new(bytes: Bytes) -> Result<Self, PrefixTooLong> {
        check_plen::<F>(bytes.len())?;
        Ok(Self {
            bytes,
            _format: PhantomData,
        })
    }

    /// Prefix length in bytes; always at most `F::PLEN_MAX`.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Returns `true` when the prefix holds no bytes.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// The prefix bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// The prefix bytes as shared [`Bytes`].
    #[must_use]
    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }

    /// Split off the first byte, sharing the tail; `None` when empty.
    ///
    /// The tail is strictly shorter, so its bound needs no re-check.
    #[must_use]
    pub fn split_first(self) -> Option<(u8, Self)> {
        let first = *self.bytes.first()?;
        Some((
            first,
            Self {
                bytes: self.bytes.slice(1..),
                _format: PhantomData,
            },
        ))
    }
}

/// Length gate shared by the owned and copying constructors, checked before
/// any copy so an over-long slice never allocates.
const fn check_plen<F: Format>(actual: usize) -> Result<(), PrefixTooLong> {
    if actual > F::PLEN_MAX {
        return Err(PrefixTooLong {
            actual,
            max: F::PLEN_MAX,
        });
    }
    Ok(())
}

impl<F: Format> Default for Prefix<F> {
    fn default() -> Self {
        Self::empty()
    }
}

impl<F: Format> AsRef<[u8]> for Prefix<F> {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl<F: Format> TryFrom<Bytes> for Prefix<F> {
    type Error = PrefixTooLong;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        Self::new(bytes)
    }
}

impl<F: Format> TryFrom<&[u8]> for Prefix<F> {
    type Error = PrefixTooLong;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        check_plen::<F>(bytes.len())?;
        Ok(Self {
            bytes: Bytes::copy_from_slice(bytes),
            _format: PhantomData,
        })
    }
}

/// Encoded length of one metadata block: at most `F::META_MAX` bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MetadataLen<F: Format = V1> {
    len: usize,
    _format: PhantomData<F>,
}

impl<F: Format> MetadataLen<F> {
    /// The empty metadata block's length.
    pub const ZERO: Self = Self {
        len: 0,
        _format: PhantomData,
    };

    /// Accept `len`, rejecting values above `F::META_MAX`.
    pub const fn new(len: usize) -> Result<Self, MetadataTooLong> {
        if len > F::META_MAX {
            return Err(MetadataTooLong {
                actual: len,
                max: F::META_MAX,
            });
        }
        Ok(Self {
            len,
            _format: PhantomData,
        })
    }

    /// The length in bytes; always at most `F::META_MAX`.
    #[must_use]
    pub const fn get(self) -> usize {
        self.len
    }
}

impl<F: Format> TryFrom<usize> for MetadataLen<F> {
    type Error = MetadataTooLong;

    fn try_from(len: usize) -> Result<Self, Self::Error> {
        Self::new(len)
    }
}

/// Accumulated packing weight of one segment: at most `F::BUDGET` bytes.
///
/// The per-kind capacities (`CAP_FORK`, `CAP_DIR`) sit below `BUDGET`; this
/// type carries the universal bound, the partitioner enforces the cap.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SegmentWeight<F: Format = V1> {
    weight: usize,
    _format: PhantomData<F>,
}

impl<F: Format> SegmentWeight<F> {
    /// The empty segment's weight.
    pub const ZERO: Self = Self {
        weight: 0,
        _format: PhantomData,
    };

    /// Accept `weight`, rejecting values above `F::BUDGET`.
    pub const fn new(weight: usize) -> Result<Self, WeightOverBudget> {
        if weight > F::BUDGET {
            return Err(WeightOverBudget {
                actual: weight,
                max: F::BUDGET,
            });
        }
        Ok(Self {
            weight,
            _format: PhantomData,
        })
    }

    /// The weight in bytes; always at most `F::BUDGET`.
    #[must_use]
    pub const fn get(self) -> usize {
        self.weight
    }

    /// Sum of two weights; `None` when the sum would exceed `F::BUDGET`.
    #[must_use]
    pub const fn checked_add(self, other: Self) -> Option<Self> {
        match self.weight.checked_add(other.weight) {
            Some(sum) if sum <= F::BUDGET => Some(Self {
                weight: sum,
                _format: PhantomData,
            }),
            _ => None,
        }
    }
}

impl<F: Format> Default for SegmentWeight<F> {
    fn default() -> Self {
        Self::ZERO
    }
}

impl<F: Format> TryFrom<usize> for SegmentWeight<F> {
    type Error = WeightOverBudget;

    fn try_from(weight: usize) -> Result<Self, Self::Error> {
        Self::new(weight)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_accepts_up_to_plen_max() {
        let bytes = vec![0xAB; V1::PLEN_MAX];
        let prefix = Prefix::<V1>::try_from(bytes.as_slice()).unwrap();
        assert_eq!(prefix.len(), V1::PLEN_MAX);
        assert_eq!(prefix.as_bytes(), bytes.as_slice());
    }

    #[test]
    fn prefix_rejects_over_plen_max() {
        let bytes = vec![0xAB; V1::PLEN_MAX + 1];
        let err = Prefix::<V1>::new(Bytes::from(bytes)).unwrap_err();
        assert_eq!(
            err,
            PrefixTooLong {
                actual: V1::PLEN_MAX + 1,
                max: V1::PLEN_MAX
            }
        );
    }

    #[test]
    fn prefix_empty_is_default() {
        let prefix = Prefix::<V1>::default();
        assert!(prefix.is_empty());
        assert_eq!(prefix, Prefix::empty());
    }

    #[test]
    fn metadata_len_boundary() {
        assert!(MetadataLen::<V1>::new(V1::META_MAX).is_ok());
        assert_eq!(
            MetadataLen::<V1>::new(V1::META_MAX + 1).unwrap_err(),
            MetadataTooLong {
                actual: V1::META_MAX + 1,
                max: V1::META_MAX
            }
        );
    }

    #[test]
    fn segment_weight_boundary() {
        assert_eq!(SegmentWeight::<V1>::ZERO.get(), 0);
        assert!(SegmentWeight::<V1>::new(V1::BUDGET).is_ok());
        assert_eq!(
            SegmentWeight::<V1>::new(V1::BUDGET + 1).unwrap_err(),
            WeightOverBudget {
                actual: V1::BUDGET + 1,
                max: V1::BUDGET
            }
        );
    }

    #[test]
    fn segment_weight_checked_add_respects_budget() {
        let half = SegmentWeight::<V1>::new(V1::BUDGET / 2).unwrap();
        let full = half.checked_add(half).unwrap();
        assert_eq!(full.get(), V1::BUDGET / 2 * 2);
        assert!(full.checked_add(SegmentWeight::new(1).unwrap()).is_none());
        assert_eq!(full.checked_add(SegmentWeight::ZERO), Some(full));
    }
}
