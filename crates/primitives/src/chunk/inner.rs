//! The single carrier for header-committed chunks.
//!
//! Every chunk is one [`ChunkHeader`] plus one [`BmtBody`]; the concrete chunk
//! types are aliases of [`ChunkInner`] with their header filled in. Keeping one
//! carrier means there is nowhere to hand-write a divergent address or verify
//! path per chunk type.

use bytes::{Bytes, BytesMut};

use crate::bmt::DEFAULT_BODY_SIZE;
use crate::cache::OnceCache;
use crate::error::{PrimitivesError, Result};
use crate::wire;

use super::address::ChunkAddress;
use super::bmt_body::BmtBody;
use super::chunk_type::ChunkType;
use super::traits::{BmtChunk, Chunk, ChunkHeader};

/// A chunk: a wire header committing to a BMT body.
///
/// The address is always derived as `header.commit(body.hash())` and cached on
/// first use; no constructor accepts one, so a carrier can never hold an
/// internally consistent lie about its own address. Verification routes
/// through [`ChunkHeader::validate`], never a bare address compare.
#[derive(Debug, Clone)]
pub struct ChunkInner<H: ChunkHeader, const BODY_SIZE: usize = DEFAULT_BODY_SIZE> {
    /// Everything preceding the BMT body on the wire.
    header: H,
    /// The `span || payload` tail.
    body: BmtBody<BODY_SIZE>,
    /// Lazily derived `header.commit(body.hash())`; never caller-supplied.
    address: OnceCache<ChunkAddress>,
}

impl<H: ChunkHeader, const BODY_SIZE: usize> ChunkInner<H, BODY_SIZE> {
    /// Assemble a chunk from its header and body.
    ///
    /// The address is derived from the parts on first use; it cannot be
    /// supplied.
    #[must_use]
    pub const fn from_header_and_body(header: H, body: BmtBody<BODY_SIZE>) -> Self {
        Self {
            header,
            body,
            address: OnceCache::new(),
        }
    }

    /// Borrow the wire header of this chunk.
    #[must_use]
    pub const fn header(&self) -> &H {
        &self.header
    }

    /// Borrow the BMT body (`span || payload`) of this chunk.
    ///
    /// The body carries the chunk's `span`, `payload`, and the `BODY_SIZE`
    /// const, so this is the zero-copy accessor callers use to feed the body
    /// into BMT operations (e.g. [`BmtBody::transformed_root`]) without
    /// re-slicing the span/payload back out of the wire form.
    #[must_use]
    pub const fn body(&self) -> &BmtBody<BODY_SIZE> {
        &self.body
    }
}

impl<H: ChunkHeader, const BODY_SIZE: usize> Chunk for ChunkInner<H, BODY_SIZE> {
    type Header = H;

    fn address(&self) -> &ChunkAddress {
        self.address
            .get_or_compute(|| self.header.commit(self.body.hash().into()))
    }

    fn header(&self) -> &H {
        &self.header
    }

    fn data(&self) -> &Bytes {
        self.body.data()
    }

    fn size(&self) -> usize {
        // Header and a body bounded by BODY_SIZE cannot overflow usize.
        H::SIZE.saturating_add(self.body.size())
    }

    /// Certify through [`ChunkHeader::validate`]: the header's full acceptance
    /// rule runs, not an address compare against the cached address.
    fn verify(&self, expected: &ChunkAddress) -> Result<()> {
        Ok(self.header.validate(self.body.hash().into(), expected)?)
    }
}

impl<H: ChunkHeader, const BODY_SIZE: usize> BmtChunk for ChunkInner<H, BODY_SIZE> {
    fn span(&self) -> u64 {
        self.body.span()
    }
}

impl<H: ChunkHeader, const BODY_SIZE: usize> ChunkType for ChunkInner<H, BODY_SIZE> {
    const TYPE_ID: super::type_id::ChunkTypeId = H::TYPE_ID;
    const TYPE_NAME: &'static str = H::NAME;
}

/// Structural equality over header and body. A SOC address does not commit
/// to the body, so address equality is slot identity, not chunk equality;
/// compare `address()` where slot identity is meant.
impl<H: ChunkHeader + PartialEq, const BODY_SIZE: usize> PartialEq for ChunkInner<H, BODY_SIZE> {
    fn eq(&self, other: &Self) -> bool {
        self.header == other.header && self.body == other.body
    }
}

impl<H: ChunkHeader + Eq, const BODY_SIZE: usize> Eq for ChunkInner<H, BODY_SIZE> {}

impl<H: ChunkHeader, const BODY_SIZE: usize> From<ChunkInner<H, BODY_SIZE>> for Bytes {
    fn from(chunk: ChunkInner<H, BODY_SIZE>) -> Self {
        let mut bytes = BytesMut::with_capacity(chunk.size());
        chunk.header.encode(&mut bytes);
        bytes.extend_from_slice(&Self::from(chunk.body));
        bytes.freeze()
    }
}

impl<H: ChunkHeader, const BODY_SIZE: usize> TryFrom<Bytes> for ChunkInner<H, BODY_SIZE> {
    type Error = PrimitivesError;

    fn try_from(bytes: Bytes) -> Result<Self> {
        let mut cursor = wire::Cursor::new(&bytes);
        let header = H::decode(&mut cursor)?;

        // decode consumed exactly H::SIZE bytes, so the slice holds.
        let body = BmtBody::try_from(bytes.slice(H::SIZE..))?;

        Ok(Self::from_header_and_body(header, body))
    }
}

impl<H: ChunkHeader, const BODY_SIZE: usize> TryFrom<&[u8]> for ChunkInner<H, BODY_SIZE> {
    type Error = PrimitivesError;

    fn try_from(bytes: &[u8]) -> Result<Self> {
        Self::try_from(Bytes::copy_from_slice(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::super::content::ContentChunk;
    use super::super::single_owner::{SingleOwnerChunk, SocHeader};
    use super::*;
    use crate::DEFAULT_BODY_SIZE;
    use alloy_primitives::hex;

    type DefaultContentChunk = ContentChunk<DEFAULT_BODY_SIZE>;
    type DefaultSingleOwnerChunk = SingleOwnerChunk<DEFAULT_BODY_SIZE>;

    /// Go-interop single-owner vector: id(32) || signature(65) || span(8) || "foo".
    fn soc_test_vector() -> Vec<u8> {
        hex!(
            "000000000000000000000000000000000000000000000000000000000000000\
            05acd384febc133b7b245e5ddc62d82d2cded9182d2716126cd8844509af65a05\
            3deb418208027f548e3e88343af6f84a8772fb3cebc0a1833a0ea7ec0c134831\
            1b0300000000000000666f6f"
        )
        .to_vec()
    }

    /// The address is derived from the parts, for both aliases.
    #[test]
    fn address_is_header_commit_over_body_hash() {
        let cac = DefaultContentChunk::new(b"carrier".to_vec()).unwrap();
        assert_eq!(
            *cac.address(),
            cac.header().commit(cac.body().hash().into())
        );

        let soc = DefaultSingleOwnerChunk::try_from(soc_test_vector().as_slice()).unwrap();
        assert_eq!(
            *soc.address(),
            soc.header().commit(soc.body().hash().into())
        );
    }

    /// Key regression: verify runs the header's acceptance rule, not an
    /// address compare. A single-owner chunk with a garbage signature commits
    /// to *some* address (the zero-owner one); asking verify about exactly
    /// that address must still fail, where a compare against the cached
    /// address would lie its way through.
    #[test]
    fn verify_routes_through_validate_not_address_compare() {
        let mut wire = soc_test_vector();
        // Clobber the 65 signature bytes after the 32-byte id.
        for byte in wire.iter_mut().skip(32).take(65) {
            *byte = 0xff;
        }

        let chunk = DefaultSingleOwnerChunk::try_from(wire.as_slice()).unwrap();
        let committed = *chunk.address();
        assert!(chunk.verify(&committed).is_err());
    }

    /// Both aliases round-trip through the one carrier codec.
    #[test]
    fn wire_round_trip_via_carrier_codec() {
        let cac = DefaultContentChunk::new(b"round trip".to_vec()).unwrap();
        let wire: Bytes = cac.clone().into();
        let decoded = DefaultContentChunk::try_from(wire).unwrap();
        assert_eq!(cac, decoded);
        assert!(decoded.verify(cac.address()).is_ok());

        let soc_wire = soc_test_vector();
        let soc = DefaultSingleOwnerChunk::try_from(soc_test_vector().as_slice()).unwrap();
        let encoded: Bytes = soc.clone().into();
        assert_eq!(encoded.as_ref(), soc_wire.as_slice());
        assert!(soc.verify(soc.address()).is_ok());
    }

    /// The carrier derives type metadata from the header predicate.
    #[test]
    fn type_metadata_comes_from_the_header() {
        use super::super::type_id::ChunkTypeId;

        assert_eq!(DefaultContentChunk::TYPE_ID, ChunkTypeId::CONTENT);
        assert_eq!(DefaultContentChunk::TYPE_NAME, "content");
        assert_eq!(DefaultSingleOwnerChunk::TYPE_ID, ChunkTypeId::SINGLE_OWNER);
        assert_eq!(DefaultSingleOwnerChunk::TYPE_NAME, "single_owner");
        assert_eq!(DefaultSingleOwnerChunk::TYPE_NAME, SocHeader::NAME);
    }
}
