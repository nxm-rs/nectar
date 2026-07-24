//! Sequence indexing: a monotonic `u64` counter.

use derive_more::{Display, From, Into};

use crate::index::Index;

/// Sequence index: marshals as 8 big-endian bytes, never hashed.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Display, From, Into,
)]
#[display("{_0}")]
#[repr(transparent)]
pub struct Sequence(u64);

impl Sequence {
    /// First position of every sequence feed.
    pub const ZERO: Self = Self(0);

    /// Last representable position; its [`next`](Index::next) is `None`.
    pub const MAX: Self = Self(u64::MAX);

    /// Construct from a sequence number.
    #[inline]
    pub const fn new(n: u64) -> Self {
        Self(n)
    }

    /// The sequence number.
    #[inline]
    pub const fn get(&self) -> u64 {
        self.0
    }
}

impl Index for Sequence {
    fn marshal(&self) -> impl AsRef<[u8]> {
        self.0.to_be_bytes()
    }

    fn next(&self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

#[cfg(any(test, feature = "arbitrary"))]
impl<'a> arbitrary::Arbitrary<'a> for Sequence {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self::new(u.arbitrary()?))
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use proptest_arbitrary_interop::arb;

    use super::*;

    proptest! {
        #[test]
        fn marshal_is_eight_big_endian_bytes(seq in arb::<Sequence>()) {
            let bytes = seq.marshal();
            let expected = seq.get().to_be_bytes();
            prop_assert_eq!(bytes.as_ref().len(), 8);
            prop_assert_eq!(bytes.as_ref(), expected.as_slice());
        }

        #[test]
        fn next_increments_until_max(n in 0..u64::MAX) {
            prop_assert_eq!(Sequence::new(n).next(), Some(Sequence::new(n + 1)));
        }
    }

    #[test]
    fn next_at_max_is_spent() {
        assert_eq!(Sequence::MAX.next(), None);
    }
}
