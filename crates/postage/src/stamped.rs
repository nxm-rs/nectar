//! A chunk paired with the postage stamp that authorises its storage.
//!
//! [`StampedChunk`] is the pairing of an [`AnyChunk`] (a `nectar-primitives`
//! type) with a [`Stamp`] (a `nectar-postage` type). A retrieval, a pushsync
//! delivery, and an upload all move a *chunk plus its proof of payment* as one
//! unit, so this pairing is the cohesive value that flows across those
//! boundaries instead of two loose fields.
//!
//! Both halves are nectar types, so the pairing and its serialisation belong
//! here. The wire codec is pure serialisation layered on top of the
//! type-tagged [`AnyChunk`] codec and the fixed-size [`Stamp`] codec.

use alloc::vec::Vec;

use nectar_primitives::{AnyChunk, ChunkAddress, DEFAULT_BODY_SIZE, bytes::Bytes};

use crate::{BatchId, STAMP_SIZE, Stamp, StampError};

/// A chunk together with its postage stamp.
///
/// [`AnyChunk`] holds the chunk bytes but carries no stamp, so this pairing is
/// the always-stamped currency on the network paths (pushsync, upload, the
/// stamped reserve). The address is the chunk's own address;
/// [`address`](Self::address) delegates to it.
///
/// # Equality
///
/// `PartialEq`/`Eq` are derived. The chunk component therefore inherits
/// [`AnyChunk`]'s own equality, which compares chunks *by address only* (a
/// chunk's address is the cryptographic commitment to its bytes, so equal
/// addresses mean equal chunks), and the stamp component compares all stamp
/// fields. Two stamped chunks are equal when their chunks share an address and
/// their stamps are field-for-field identical. This matches the semantics of
/// the original vertex `StampedChunk`, which also derived equality over the
/// same two fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StampedChunk<const BODY_SIZE: usize = DEFAULT_BODY_SIZE> {
    chunk: AnyChunk<BODY_SIZE>,
    stamp: Stamp,
}

impl<const BODY_SIZE: usize> StampedChunk<BODY_SIZE> {
    /// Pair a chunk with its stamp.
    #[inline]
    #[must_use]
    pub const fn new(chunk: AnyChunk<BODY_SIZE>, stamp: Stamp) -> Self {
        Self { chunk, stamp }
    }

    /// The chunk.
    #[inline]
    #[must_use]
    pub const fn chunk(&self) -> &AnyChunk<BODY_SIZE> {
        &self.chunk
    }

    /// The postage stamp.
    #[inline]
    #[must_use]
    pub const fn stamp(&self) -> &Stamp {
        &self.stamp
    }

    /// The chunk's address (delegates to the chunk).
    #[inline]
    #[must_use]
    pub fn address(&self) -> &ChunkAddress {
        self.chunk.address()
    }

    /// Split into the chunk and its stamp.
    #[inline]
    #[must_use]
    pub fn into_parts(self) -> (AnyChunk<BODY_SIZE>, Stamp) {
        (self.chunk, self.stamp)
    }

    /// Encode to a self-describing byte string: the stamp followed by the
    /// type-tagged chunk.
    ///
    /// The layout is `[stamp: STAMP_SIZE][type_id: 1][chunk wire bytes]`, the
    /// stamp's fixed-size encoding ([`Stamp::to_bytes`], `STAMP_SIZE = 113`
    /// bytes) followed by the chunk's type-tagged encoding
    /// ([`AnyChunk::to_typed_bytes`]). Decode with
    /// [`from_typed_bytes`](Self::from_typed_bytes).
    #[must_use]
    pub fn to_typed_bytes(&self) -> Vec<u8> {
        let stamp = self.stamp.to_bytes();
        let chunk = self.chunk.to_typed_bytes();
        #[allow(clippy::arithmetic_side_effects)]
        // capacity hint: sum of two in-memory buffer lengths cannot overflow usize
        let mut out = Vec::with_capacity(stamp.len() + chunk.len());
        out.extend_from_slice(&stamp);
        out.extend_from_slice(&chunk);
        out
    }

    /// Decode a stamped chunk produced by [`to_typed_bytes`](Self::to_typed_bytes).
    ///
    /// The first `STAMP_SIZE` bytes are the stamp ([`Stamp::from_bytes`]); the
    /// remainder is the type-tagged chunk ([`AnyChunk::from_typed_bytes`]),
    /// verified against `address`.
    ///
    /// # Errors
    ///
    /// Returns an error (and never panics) when the input is shorter than a
    /// stamp, the stamp bytes are invalid, the chunk payload cannot be decoded,
    /// or the chunk's computed address does not match `address`.
    pub fn from_typed_bytes(address: &ChunkAddress, bytes: &[u8]) -> Result<Self, StampError> {
        if bytes.len() < STAMP_SIZE {
            return Err(StampError::InvalidData(
                "stamped chunk shorter than a stamp",
            ));
        }
        let (stamp_bytes, chunk_bytes) = bytes.split_at(STAMP_SIZE);
        let stamp = Stamp::try_from_slice(stamp_bytes)?;
        let chunk = AnyChunk::<BODY_SIZE>::from_typed_bytes(address, chunk_bytes)
            .map_err(|_| StampError::Chunk("failed to decode typed chunk"))?;
        Ok(Self::new(chunk, stamp))
    }

    /// Rebuild a stamped chunk from the bare chunk wire bytes, its expected
    /// address, and a separately-carried stamp.
    ///
    /// `data` is the bare chunk wire bytes (no type tag), as carried in a
    /// `Delivery { data, stamp }` wire message; the address disambiguates the
    /// chunk variant (see [`AnyChunk::from_wire_bytes`]).
    ///
    /// # Errors
    ///
    /// Returns an error (and never panics) when `data` does not hash to
    /// `expected` as either a content or a single-owner chunk.
    pub fn reconstruct(
        expected: ChunkAddress,
        data: Bytes,
        stamp: Stamp,
    ) -> Result<Self, StampError> {
        let chunk = AnyChunk::<BODY_SIZE>::from_wire_bytes(&expected, data)
            .map_err(|_| StampError::Chunk("chunk bytes do not match expected address"))?;
        Ok(Self::new(chunk, stamp))
    }

    /// Read the batch id from a [`to_typed_bytes`](Self::to_typed_bytes) value
    /// without a full decode.
    ///
    /// The stamp leads the encoding and [`Stamp::to_bytes`] places the batch id
    /// in its first 32 bytes, so the batch id is `typed_bytes[0..32]`. This lets
    /// a store index a stamped chunk by batch without decoding the chunk.
    ///
    /// # Errors
    ///
    /// Returns an error (and never panics) when `typed_bytes` is shorter than 32
    /// bytes.
    pub fn batch_id(typed_bytes: &[u8]) -> Result<BatchId, StampError> {
        let id = typed_bytes.get(..32).ok_or(StampError::InvalidData(
            "typed bytes shorter than a batch id",
        ))?;
        Ok(BatchId::from_slice(id))
    }
}

#[cfg(any(test, feature = "arbitrary"))]
impl<'a, const BODY_SIZE: usize> arbitrary::Arbitrary<'a> for StampedChunk<BODY_SIZE> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        // Raw tier: the stamp is structurally valid but not signed over the
        // chunk's address. Use `crate::generators::signed_stamped_chunk` for a
        // pairing whose stamp verifies.
        Ok(Self::new(
            nectar_primitives::AnyChunk::arbitrary(u)?,
            Stamp::arbitrary(u)?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{B256, Signature};
    use alloy_signer_local::PrivateKeySigner;
    use nectar_primitives::{Chunk, ContentChunk, SingleOwnerChunk, SocId, bytes::Bytes};

    use super::*;

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

    #[test]
    fn into_parts_round_trips_the_fields() {
        let chunk: AnyChunk = content_chunk().into();
        let stamp = test_stamp();
        let stamped = DefaultStampedChunk::new(chunk.clone(), stamp.clone());
        assert_eq!(stamped.address(), chunk.address());
        let (got_chunk, got_stamp) = stamped.into_parts();
        assert_eq!(got_chunk, chunk);
        assert_eq!(got_stamp, stamp);
    }

    #[test]
    fn typed_content_round_trip() {
        let chunk = content_chunk();
        let address = *chunk.address();
        let stamp = test_stamp();
        let stamped = DefaultStampedChunk::new(chunk.into(), stamp.clone());

        let bytes = stamped.to_typed_bytes();
        let decoded = DefaultStampedChunk::from_typed_bytes(&address, &bytes).expect("decode");

        assert!(decoded.chunk().is_content());
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
        let stamped = DefaultStampedChunk::new(chunk.into(), stamp.clone());

        let bytes = stamped.to_typed_bytes();
        let decoded = DefaultStampedChunk::from_typed_bytes(&address, &bytes).expect("decode");

        assert!(decoded.chunk().is_single_owner());
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
        assert!(rebuilt.chunk().is_content());
        assert_eq!(*rebuilt.address(), address);
        assert_eq!(rebuilt.stamp(), &stamp);
        assert_eq!(rebuilt.into_parts().0.into_bytes(), data);
    }

    #[test]
    fn reconstruct_single_owner_from_wire() {
        let chunk = single_owner_chunk();
        let address = *chunk.address();
        let data = Bytes::from(chunk);
        let stamp = test_stamp();

        let rebuilt =
            DefaultStampedChunk::reconstruct(address, data, stamp.clone()).expect("rebuild");
        assert!(rebuilt.chunk().is_single_owner());
        assert_eq!(*rebuilt.address(), address);
        assert_eq!(rebuilt.stamp(), &stamp);
    }

    #[test]
    fn batch_id_matches_stamp_and_leading_bytes() {
        let chunk = content_chunk();
        let stamp = test_stamp();
        let stamped = DefaultStampedChunk::new(chunk.into(), stamp.clone());
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
        assert!(matches!(err, StampError::InvalidData(_)));
    }

    #[test]
    fn from_typed_bytes_address_mismatch_errors() {
        let chunk = content_chunk();
        let stamped = DefaultStampedChunk::new(chunk.into(), test_stamp());
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
