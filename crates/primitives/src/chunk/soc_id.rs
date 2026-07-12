//! Typed identifier for single-owner chunks.
//!
//! A [`SocId`] is the 32-byte identifier a single-owner chunk is signed
//! under; the SOC address is `keccak256(id || owner)`. See
//! [`SingleOwnerChunk`](super::single_owner::SingleOwnerChunk) and bee
//! `pkg/soc/soc.go` for the reference semantics.

use alloy_primitives::B256;
use derive_more::{AsRef, Display, From, Into};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// 32-byte single-owner chunk identifier.
///
/// Transparent over the same 32 wire bytes as the raw field it types: a SOC
/// still serializes as `id || signature || body`. Dispersed replicas
/// constrain `id[1..]` to the wrapped body hash, leaving only the first byte
/// mined.
///
/// Nominally distinct from the raw hash it wraps: a bare `B256` is rejected
/// where a `SocId` is expected.
///
/// ```compile_fail
/// use alloy_primitives::B256;
/// use nectar_primitives::SocId;
///
/// fn sign_under(_id: SocId) {}
/// sign_under(B256::ZERO);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into, AsRef)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[display("{_0}")]
#[from(B256, [u8; 32])]
#[into(B256, [u8; 32])]
#[as_ref([u8])]
#[repr(transparent)]
pub struct SocId(B256);

impl SocId {
    /// Zero id, useful for tests and deterministic vectors.
    pub const ZERO: Self = Self(B256::ZERO);

    /// Construct from raw 32 bytes. `const` for static contexts; for runtime
    /// conversions prefer the `From` impls.
    #[inline]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(B256::new(bytes))
    }

    /// Borrow the underlying 32 bytes.
    #[inline]
    pub const fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }

    /// Sample a cryptographically random id via `alloy_primitives::B256::random`.
    pub fn random() -> Self {
        Self(B256::random())
    }
}

#[cfg(any(test, feature = "arbitrary"))]
impl<'a> arbitrary::Arbitrary<'a> for SocId {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self::new(u.arbitrary()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_is_all_zero_bytes() {
        assert_eq!(SocId::ZERO.as_slice(), &[0u8; 32]);
    }

    #[test]
    fn roundtrips_via_from_impls() {
        let bytes = [7u8; 32];
        let id = SocId::new(bytes);
        assert_eq!(B256::from(id), B256::new(bytes));
        assert_eq!(SocId::from(B256::new(bytes)), id);
        assert_eq!(<[u8; 32]>::from(id), bytes);
        assert_eq!(SocId::from(bytes), id);
    }

    #[test]
    fn display_matches_b256_lowercase_hex() {
        let id = SocId::new([0xab; 32]);
        let rendered = format!("{id}");
        assert!(rendered.starts_with("0x"));
        assert_eq!(rendered.len(), 66);
        assert!(rendered.chars().skip(2).all(|c| c.is_ascii_hexdigit()));
    }
}
