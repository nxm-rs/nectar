//! A chunk paired with the postage stamp that authorizes its storage.
//!
//! [`StampedChunk`] is the pairing of a verified [`Chunk`] (a
//! `nectar-primitives` type) with a [`Stamp`] (a `nectar-postage` type). A
//! retrieval, a pushsync delivery, and an upload all move a *chunk plus its
//! proof of payment* as one unit, so this pairing is the cohesive value that
//! flows across those boundaries instead of two loose fields.
//!
//! Both halves are nectar types, so the pairing and its serialization belong
//! here. The wire codec is pure serialization layered on top of the
//! registry's type-tagged chunk codec and the fixed-size [`Stamp`] codec.

use alloc::vec::Vec;

use nectar_primitives::{
    AnyChunkSet, Chunk, ChunkAddress, DEFAULT_BODY_SIZE, Unverified, Verified, bytes::Bytes,
    wire::Cursor,
};

use crate::{BatchId, Stamp, StampError};

/// A verified chunk together with its postage stamp.
///
/// The chunk half is `Chunk<Verified>`, so a stamped chunk can only be built
/// around a certified address: every wire ingress runs parse then verify
/// before this type exists. [`address`](Self::address) reads the certified
/// fact; it never recomputes or recovers anything.
///
/// # Equality
///
/// Structural: the chunk half compares its address and decoded envelope, the
/// stamp half all stamp fields.
#[derive(Debug, Clone)]
pub struct StampedChunk<const BODY_SIZE: usize = DEFAULT_BODY_SIZE> {
    chunk: Chunk<Verified, AnyChunkSet<BODY_SIZE>>,
    stamp: Stamp,
}

impl<const BODY_SIZE: usize> StampedChunk<BODY_SIZE> {
    /// Pair a verified chunk with its stamp.
    #[inline]
    #[must_use]
    pub const fn new(chunk: Chunk<Verified, AnyChunkSet<BODY_SIZE>>, stamp: Stamp) -> Self {
        Self { chunk, stamp }
    }

    /// The verified chunk.
    #[inline]
    #[must_use]
    pub const fn chunk(&self) -> &Chunk<Verified, AnyChunkSet<BODY_SIZE>> {
        &self.chunk
    }

    /// The postage stamp.
    #[inline]
    #[must_use]
    pub const fn stamp(&self) -> &Stamp {
        &self.stamp
    }

    /// The chunk's certified address: a stored fact, free to read.
    #[inline]
    #[must_use]
    pub const fn address(&self) -> &ChunkAddress {
        self.chunk.address()
    }

    /// Split into the verified chunk and its stamp.
    #[inline]
    #[must_use]
    pub fn into_parts(self) -> (Chunk<Verified, AnyChunkSet<BODY_SIZE>>, Stamp) {
        (self.chunk, self.stamp)
    }

    /// Encode to a self-describing byte string: the stamp followed by the
    /// type-tagged chunk.
    ///
    /// The layout is `[stamp: STAMP_SIZE][id: 1][version: 1][chunk wire
    /// bytes]`, the stamp's fixed-size encoding ([`Stamp::to_bytes`],
    /// `STAMP_SIZE = 113` bytes) followed by the chunk's registry typed
    /// encoding. Decode with [`from_typed_bytes`](Self::from_typed_bytes).
    #[must_use]
    pub fn to_typed_bytes(&self) -> Vec<u8> {
        let stamp = self.stamp.to_bytes();
        let chunk = self.chunk.typed_bytes();
        let mut out = Vec::with_capacity(stamp.len().saturating_add(chunk.len()));
        out.extend_from_slice(&stamp);
        out.extend_from_slice(&chunk);
        out
    }

    /// Decode a stamped chunk produced by [`to_typed_bytes`](Self::to_typed_bytes).
    ///
    /// The first `STAMP_SIZE` bytes are the stamp ([`Stamp::from_bytes`]); the
    /// remainder is the type-tagged chunk, parsed under `address` as a claim
    /// and then verified, so the result holds a certified address.
    ///
    /// # Errors
    ///
    /// Returns an error (and never panics) when the input is shorter than a
    /// stamp, the stamp bytes are invalid, the chunk payload cannot be parsed,
    /// or the chunk fails verification against `address`.
    pub fn from_typed_bytes(address: &ChunkAddress, bytes: &[u8]) -> Result<Self, StampError> {
        let mut cur = Cursor::new(bytes);
        let stamp = cur.take::<Stamp>()?;
        let chunk = Chunk::<Unverified, AnyChunkSet<BODY_SIZE>>::parse(*address, cur.finish())
            .map_err(|_| StampError::Chunk("failed to parse typed chunk"))?
            .verify()
            .map_err(|_| StampError::Chunk("chunk does not verify at the claimed address"))?;
        Ok(Self::new(chunk, stamp))
    }

    /// Rebuild a stamped chunk from the bare chunk wire bytes, its expected
    /// address, and a separately-carried stamp.
    ///
    /// `data` is the bare chunk wire bytes (no type tag), as carried in a
    /// `Delivery { data, stamp }` wire message. Bare wire bytes carry no tag,
    /// so the address routes the decode and certification is inseparable from
    /// it: the result holds a verified chunk at `expected`.
    ///
    /// # Errors
    ///
    /// Returns an error (and never panics) when `data` does not verify at
    /// `expected` as any registry member.
    pub fn reconstruct(
        expected: ChunkAddress,
        data: Bytes,
        stamp: Stamp,
    ) -> Result<Self, StampError> {
        let chunk = Chunk::<Verified, AnyChunkSet<BODY_SIZE>>::decode_wire(expected, data)
            .map_err(|_| StampError::Chunk("chunk bytes do not match expected address"))?;
        Ok(Self::new(chunk, stamp))
    }

    /// Read the batch id from a [`to_typed_bytes`](Self::to_typed_bytes) value
    /// without a full decode.
    ///
    /// The stamp leads the encoding and the batch id is the stamp's first wire
    /// field, so a cursor over the typed bytes yields it directly. This lets a
    /// store index a stamped chunk by batch without decoding the chunk.
    ///
    /// # Errors
    ///
    /// Returns an error (and never panics) when `typed_bytes` is shorter than a
    /// batch id.
    pub fn batch_id(typed_bytes: &[u8]) -> Result<BatchId, StampError> {
        Ok(Cursor::new(typed_bytes).take::<BatchId>()?)
    }
}

impl<const BODY_SIZE: usize> PartialEq for StampedChunk<BODY_SIZE> {
    fn eq(&self, other: &Self) -> bool {
        self.chunk == other.chunk && self.stamp == other.stamp
    }
}

impl<const BODY_SIZE: usize> Eq for StampedChunk<BODY_SIZE> {}

#[cfg(any(test, feature = "arbitrary"))]
impl<'a, const BODY_SIZE: usize> arbitrary::Arbitrary<'a> for StampedChunk<BODY_SIZE> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        // The chunk half is valid by construction: the sealed verified type
        // admits nothing less. The stamp is structurally valid but not signed
        // over the chunk's address; use `crate::generators::signed_stamped_chunk`
        // for a pairing whose stamp verifies.
        let envelope = nectar_primitives::generators::any_chunk::<BODY_SIZE>(u)?;
        let chunk =
            Chunk::from_envelope(envelope).map_err(|_| arbitrary::Error::IncorrectFormat)?;
        Ok(Self::new(chunk, Stamp::arbitrary(u)?))
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{B256, Signature};
    use alloy_signer_local::PrivateKeySigner;
    use nectar_primitives::{
        AnyChunk, ChunkOps, ContentChunk, SingleOwnerChunk, SocId, bytes::Bytes,
    };

    use super::*;
    use crate::STAMP_SIZE;

    type DefaultStampedChunk = StampedChunk<DEFAULT_BODY_SIZE>;

    fn test_stamp() -> Stamp {
        let sig = Signature::from_raw(&[1u8; 65]).expect("valid signature");
        Stamp::new(BatchId::new([0xaa; 32]), 3, 7, 42, sig)
    }

    fn content_chunk() -> ContentChunk<DEFAULT_BODY_SIZE> {
        ContentChunk::new(&b"hello swarm"[..]).expect("valid content chunk")
    }

    fn single_owner_chunk() -> SingleOwnerChunk<DEFAULT_BODY_SIZE> {
        let signer = PrivateKeySigner::from_bytes(&B256::repeat_byte(0x11)).expect("valid signer");
        SingleOwnerChunk::new(
            SocId::from(B256::repeat_byte(0x22)),
            &b"soc payload"[..],
            &signer,
        )
        .expect("valid soc")
    }

    fn verified(chunk: impl Into<AnyChunk<DEFAULT_BODY_SIZE>>) -> Chunk<Verified> {
        Chunk::from_envelope(chunk.into()).expect("locally built chunk certifies")
    }

    #[test]
    fn into_parts_round_trips_the_fields() {
        let chunk = verified(content_chunk());
        let address = *chunk.address();
        let stamp = test_stamp();
        let stamped = DefaultStampedChunk::new(chunk, stamp.clone());
        assert_eq!(stamped.address(), &address);
        let (got_chunk, got_stamp) = stamped.into_parts();
        assert_eq!(got_chunk.address(), &address);
        assert_eq!(got_stamp, stamp);
    }

    #[test]
    fn typed_content_round_trip() {
        let chunk = content_chunk();
        let address = *chunk.address();
        let stamp = test_stamp();
        let stamped = DefaultStampedChunk::new(verified(chunk), stamp.clone());

        let bytes = stamped.to_typed_bytes();
        let decoded = DefaultStampedChunk::from_typed_bytes(&address, &bytes).expect("decode");

        assert!(decoded.chunk().envelope().is_content());
        assert_eq!(*decoded.address(), address);
        assert_eq!(decoded.stamp().batch(), stamp.batch());
        assert_eq!(decoded.stamp().timestamp(), stamp.timestamp());
        assert_eq!(decoded, stamped);
    }

    #[test]
    fn typed_single_owner_round_trip() {
        let chunk = single_owner_chunk();
        let address = *chunk.address();
        let stamp = test_stamp();
        let stamped = DefaultStampedChunk::new(verified(chunk), stamp.clone());

        let bytes = stamped.to_typed_bytes();
        let decoded = DefaultStampedChunk::from_typed_bytes(&address, &bytes).expect("decode");

        assert!(decoded.chunk().envelope().is_single_owner());
        assert_eq!(*decoded.address(), address);
        assert_eq!(decoded.stamp().batch(), stamp.batch());
        assert_eq!(decoded.stamp().timestamp(), stamp.timestamp());
        assert_eq!(decoded, stamped);
    }

    #[test]
    fn reconstruct_round_trips_from_wire() {
        let chunk = content_chunk();
        let address = *chunk.address();
        let data = Bytes::from(chunk);
        let stamp = test_stamp();

        let rebuilt = DefaultStampedChunk::reconstruct(address, data.clone(), stamp.clone())
            .expect("rebuild");
        assert!(rebuilt.chunk().envelope().is_content());
        assert_eq!(*rebuilt.address(), address);
        assert_eq!(rebuilt.stamp(), &stamp);
        assert_eq!(rebuilt.into_parts().0.into_envelope().into_bytes(), data);
    }

    #[test]
    fn reconstruct_single_owner_from_wire() {
        let chunk = single_owner_chunk();
        let address = *chunk.address();
        let data = Bytes::from(chunk);
        let stamp = test_stamp();

        let rebuilt =
            DefaultStampedChunk::reconstruct(address, data, stamp.clone()).expect("rebuild");
        assert!(rebuilt.chunk().envelope().is_single_owner());
        assert_eq!(*rebuilt.address(), address);
        assert_eq!(rebuilt.stamp(), &stamp);
    }

    #[test]
    fn equality_compares_address_and_stamp() {
        let stamp = test_stamp();
        let a = DefaultStampedChunk::new(verified(content_chunk()), stamp.clone());
        let b = DefaultStampedChunk::new(verified(content_chunk()), stamp.clone());
        assert_eq!(a, b);

        let sig = Signature::from_raw(&[1u8; 65]).expect("valid signature");
        let other_stamp = Stamp::new(BatchId::new([0xbb; 32]), 3, 7, 42, sig);
        let c = DefaultStampedChunk::new(verified(content_chunk()), other_stamp);
        assert_ne!(a, c);

        let d = DefaultStampedChunk::new(verified(single_owner_chunk()), stamp);
        assert_ne!(a, d);
    }

    #[test]
    fn batch_id_matches_stamp_and_leading_bytes() {
        let chunk = content_chunk();
        let stamp = test_stamp();
        let stamped = DefaultStampedChunk::new(verified(chunk), stamp.clone());
        let bytes = stamped.to_typed_bytes();

        let id = DefaultStampedChunk::batch_id(&bytes).expect("batch id");
        assert_eq!(id, stamp.batch());
        assert_eq!(id.as_slice(), &bytes[0..32]);
    }

    #[test]
    fn from_typed_bytes_empty_errors() {
        let address: ChunkAddress = [0u8; 32].into();
        assert!(DefaultStampedChunk::from_typed_bytes(&address, &[]).is_err());
    }

    #[test]
    fn from_typed_bytes_shorter_than_stamp_errors() {
        let address: ChunkAddress = [0u8; 32].into();
        let short = [0u8; STAMP_SIZE - 1];
        let err = DefaultStampedChunk::from_typed_bytes(&address, &short)
            .expect_err("short input must error");
        assert!(matches!(err, StampError::Underrun { .. }));
    }

    #[test]
    fn from_typed_bytes_address_mismatch_errors() {
        let chunk = content_chunk();
        let stamped = DefaultStampedChunk::new(verified(chunk), test_stamp());
        let bytes = stamped.to_typed_bytes();

        let wrong: ChunkAddress = [0xFFu8; 32].into();
        let err = DefaultStampedChunk::from_typed_bytes(&wrong, &bytes)
            .expect_err("address mismatch must error");
        assert!(matches!(err, StampError::Chunk(_)));
    }

    #[test]
    fn reconstruct_rejects_wrong_address() {
        let chunk = content_chunk();
        let data = Bytes::from(chunk);
        let wrong: ChunkAddress = [0xFFu8; 32].into();
        let err = DefaultStampedChunk::reconstruct(wrong, data, test_stamp())
            .expect_err("wrong address must error");
        assert!(matches!(err, StampError::Chunk(_)));
    }

    #[test]
    fn batch_id_short_errors() {
        let short = [0u8; 31];
        assert!(DefaultStampedChunk::batch_id(&short).is_err());
    }
}
