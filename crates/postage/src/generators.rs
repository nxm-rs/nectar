//! Valid-by-construction test-value generators.
//!
//! Two tiers exist for test values. The raw tier is the `arbitrary::Arbitrary`
//! impls on the types themselves (a [`Stamp`] whose signature is well-formed
//! but signs nothing, say), which is what fuzzing rejection paths needs. This
//! module is the valid tier: stamps are really signed over the chunk address
//! with a bucket and index coherent with their batch, so
//! [`Stamp::verify`] passes against the batch owner. Generators stay
//! deterministic in `u`, so shrinking and replay work.
//!
//! See `nectar_primitives::generators` for the chunk-side generators and the
//! deterministic signer.

use alloy_primitives::Address;
use alloy_signer::SignerSync;
use arbitrary::Unstructured;
use nectar_primitives::{ChunkAddress, ChunkOps};

use crate::{Batch, BatchId, Stamp, StampDigest, StampIndex, StampedChunk};

/// A batch with valid depth invariants and the given owner.
///
/// `bucket_depth` is drawn in `1..=min(depth, 31)`, so bucket count and
/// per-bucket capacity both stay within `u32`.
pub fn batch(u: &mut Unstructured<'_>, owner: Address) -> arbitrary::Result<Batch> {
    let depth: u8 = u.int_in_range(1..=32)?;
    let bucket_depth: u8 = u.int_in_range(1..=depth.min(31))?;
    Ok(Batch::new(
        BatchId::from(u.arbitrary::<[u8; 32]>()?),
        u.arbitrary()?,
        u.arbitrary()?,
        owner,
        depth,
        bucket_depth,
        u.arbitrary()?,
    ))
}

/// A stamp for `address`, signed by `signer` and coherent with `batch`.
///
/// The bucket is the one `batch` assigns to `address`; the position is drawn
/// within the bucket capacity, so [`Batch::validate_index`] and
/// [`Batch::validate_bucket`] both pass. [`Stamp::verify`] passes when
/// `signer` controls the batch owner address.
pub fn signed_stamp(
    u: &mut Unstructured<'_>,
    signer: &impl SignerSync,
    batch: &Batch,
    address: &ChunkAddress,
) -> arbitrary::Result<Stamp> {
    let bucket = batch.bucket_for_address(address);
    let position = u.int_in_range(0..=batch.bucket_upper_bound().saturating_sub(1))?;
    let index = StampIndex::new(bucket, position);
    let timestamp = u.arbitrary()?;

    let digest = StampDigest::new(*address, batch.id(), index, timestamp);
    let signature = signer
        .sign_message_sync(digest.to_prehash().as_slice())
        .map_err(|_| arbitrary::Error::IncorrectFormat)?;

    Ok(Stamp::with_index(batch.id(), index, timestamp, signature))
}

/// A fully coherent stamped chunk: a valid chunk paired with a stamp that
/// verifies against the returned batch's owner.
///
/// The signer is drawn from `u` (see `nectar_primitives::generators::signer`);
/// its address is the batch owner, so
/// `stamped.stamp().verify(stamped.address(), batch.owner())` passes.
pub fn signed_stamped_chunk<const BODY_SIZE: usize>(
    u: &mut Unstructured<'_>,
) -> arbitrary::Result<(StampedChunk<BODY_SIZE>, Batch)> {
    let signer = nectar_primitives::generators::signer(u)?;
    let batch = batch(u, signer.address())?;
    let chunk = nectar_primitives::generators::any_chunk::<BODY_SIZE>(u)?;
    let stamp = signed_stamp(u, &signer, &batch, chunk.address())?;
    Ok((StampedChunk::new(chunk, stamp), batch))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn generated_stamp_verifies_and_is_coherent(
            seed in proptest::collection::vec(any::<u8>(), 128..2048),
        ) {
            let mut u = Unstructured::new(&seed);
            let (stamped, batch): (StampedChunk, Batch) =
                signed_stamped_chunk(&mut u).unwrap();

            let address = *stamped.address();
            let stamp = stamped.stamp();

            // Signature really covers the chunk address and stamp fields.
            prop_assert!(stamp.verify(&address, batch.owner()).is_ok());

            // Bucket and position are coherent with the batch geometry.
            prop_assert!(batch.validate_index(&stamp.stamp_index()).is_ok());
            prop_assert!(batch.validate_bucket(&stamp.stamp_index(), &address).is_ok());

            // The stamp round-trips its fixed-size wire encoding.
            let decoded = Stamp::from_bytes(&stamp.to_bytes()).unwrap();
            prop_assert_eq!(stamp, &decoded);
        }
    }
}
