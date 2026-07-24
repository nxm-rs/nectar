//! Feed topic.

use alloy_primitives::{B256, keccak256};
use derive_more::{AsRef, Display, From, Into};

/// 32-byte feed topic, mixed raw (never hashed again) into every update id.
///
/// Nominally distinct from the hash it wraps: a bare `B256` is rejected
/// where a `Topic` is expected. Arbitrary-length labels go through
/// [`from_label`](Self::from_label); [`new`](Self::new) wraps 32 bytes
/// verbatim.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into, AsRef)]
#[display("{_0}")]
#[from(B256, [u8; 32])]
#[into(B256, [u8; 32])]
#[as_ref([u8])]
#[repr(transparent)]
pub struct Topic(B256);

impl Topic {
    /// Zero topic, useful for tests and deterministic vectors.
    pub const ZERO: Self = Self(B256::ZERO);

    /// Construct from raw 32 bytes. `const` for static contexts; for runtime
    /// conversions prefer the `From` impls.
    #[inline]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(B256::new(bytes))
    }

    /// Derive a topic from an arbitrary-length label: `keccak256(label)`.
    pub fn from_label(label: impl AsRef<[u8]>) -> Self {
        Self(keccak256(label))
    }

    /// Borrow the underlying 32 bytes.
    #[inline]
    pub const fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }
}

#[cfg(any(test, feature = "arbitrary"))]
impl<'a> arbitrary::Arbitrary<'a> for Topic {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self::new(u.arbitrary()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_label_is_keccak_of_label() {
        assert_eq!(Topic::from_label("abc"), Topic::from(keccak256("abc")));
    }

    #[test]
    fn new_wraps_verbatim() {
        let raw = [0x5au8; 32];
        assert_eq!(Topic::new(raw).as_slice(), &raw);
        assert_eq!(Topic::ZERO.as_slice(), &[0u8; 32]);
    }
}
