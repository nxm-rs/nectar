//! Valid-by-construction test-value generators.
//!
//! Two tiers exist for test values. The raw tier is the `arbitrary::Arbitrary`
//! impls on the types themselves: deterministic in the input bytes but free to
//! produce values that fail validation (an unconstrained single-owner chunk
//! signature, say), which is what fuzzing rejection paths needs. This module
//! is the valid tier: every value it returns passes the corresponding
//! validation (a content chunk hashes to its own address, a single-owner
//! chunk's signature recovers its owner). Generators stay deterministic in
//! `u`, so shrinking and replay work.
//!
//! Bridge to proptest with `proptest-arbitrary-interop` for the raw tier, or
//! by mapping a byte-vector strategy through [`arbitrary::Unstructured`] for
//! these generators.

use alloy_primitives::keccak256;
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use arbitrary::{Arbitrary, Unstructured};

use crate::{AnyChunk, ContentChunk, SingleOwnerChunk};

/// A deterministic signer drawn from `u`.
///
/// The key is the keccak-256 of 32 drawn bytes, so it is uniformly
/// distributed and valid except with negligible probability.
pub fn signer(u: &mut Unstructured<'_>) -> arbitrary::Result<PrivateKeySigner> {
    let seed: [u8; 32] = u.arbitrary()?;
    PrivateKeySigner::from_bytes(&keccak256(seed)).map_err(|_| arbitrary::Error::IncorrectFormat)
}

/// A valid content-addressed chunk.
///
/// Every representable content chunk is valid (the address is derived from
/// span and payload), so this delegates to the `Arbitrary` impl; it exists so
/// valid-tier call sites read uniformly.
pub fn content_chunk<const BODY_SIZE: usize>(
    u: &mut Unstructured<'_>,
) -> arbitrary::Result<ContentChunk<BODY_SIZE>> {
    ContentChunk::arbitrary(u)
}

/// A valid single-owner chunk signed by a signer drawn from `u`.
///
/// Recover the owner from the chunk itself via
/// [`SingleOwnerChunk::owner`]. To pin the owner, use
/// [`single_owner_chunk_signed_by`].
pub fn single_owner_chunk<const BODY_SIZE: usize>(
    u: &mut Unstructured<'_>,
) -> arbitrary::Result<SingleOwnerChunk<BODY_SIZE>> {
    let signer = signer(u)?;
    SingleOwnerChunk::arbitrary_signed(u, &signer)
}

/// A valid single-owner chunk signed by the given signer.
pub fn single_owner_chunk_signed_by<const BODY_SIZE: usize>(
    u: &mut Unstructured<'_>,
    signer: &impl SignerSync,
) -> arbitrary::Result<SingleOwnerChunk<BODY_SIZE>> {
    SingleOwnerChunk::arbitrary_signed(u, signer)
}

/// A valid chunk of either kind: content-addressed, or single-owner signed by
/// a signer drawn from `u`.
pub fn any_chunk<const BODY_SIZE: usize>(
    u: &mut Unstructured<'_>,
) -> arbitrary::Result<AnyChunk<BODY_SIZE>> {
    if u.arbitrary()? {
        Ok(content_chunk::<BODY_SIZE>(u)?.into())
    } else {
        Ok(single_owner_chunk::<BODY_SIZE>(u)?.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DEFAULT_BODY_SIZE;
    use crate::chunk::ChunkOps;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn generated_chunks_verify(seed in proptest::collection::vec(any::<u8>(), 64..2048)) {
            let mut u = Unstructured::new(&seed);
            let chunk = any_chunk::<DEFAULT_BODY_SIZE>(&mut u).unwrap();
            let address = *chunk.address();
            prop_assert!(chunk.verify(&address).is_ok());
        }

        #[test]
        fn signer_is_deterministic(seed in any::<[u8; 32]>()) {
            let a = signer(&mut Unstructured::new(&seed)).unwrap();
            let b = signer(&mut Unstructured::new(&seed)).unwrap();
            prop_assert_eq!(a.address(), b.address());
        }
    }
}
