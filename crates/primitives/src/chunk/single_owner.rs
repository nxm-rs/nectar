//! Single-owner chunk implementation
//!
//! This module provides the single-owner chunk type: the [`ChunkInner`]
//! carrier under a [`SocHeader`], which binds the body to an owner via an
//! id and a signature.

use alloy_primitives::{Address, B256, Keccak256, Signature, address, b256, hex};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use bytes::{Bytes, BytesMut};
use std::fmt;
use std::marker::PhantomData;

use crate::bmt::DEFAULT_BODY_SIZE;
use crate::chunk::error::{self, ChunkError};
use crate::error::Result;
use crate::wire;

use super::address::ChunkAddress;
use super::bmt_body::BmtBody;
use super::content::ContentChunk;
use super::inner::ChunkInner;
use super::soc_id::SocId;
use super::traits::ChunkHeader;
use super::type_id::ChunkTypeId;
use super::type_tag::ChunkVersion;

// Constants for field sizes
const ID_SIZE: usize = std::mem::size_of::<B256>();
const SIGNATURE_SIZE: usize = 65;

/// The address of the owner of the SOC for dispersed replicas.
const DISPERSED_REPLICA_OWNER: Address = address!("0xdc5b20847f43d67928f49cd4f85d696b5a7617b5");
/// Generated from the private key `0x0100000000000000000000000000000000000000000000000000000000000000`.
const DISPERSED_REPLICA_OWNER_PK: B256 =
    b256!("0x0100000000000000000000000000000000000000000000000000000000000000");

/// A single-owner chunk (SOC) with configurable body size.
///
/// The [`ChunkInner`] carrier under a [`SocHeader`]: the address is
/// `keccak256(id || owner)`, derived by the carrier and never
/// caller-supplied.
pub type SingleOwnerChunk<const BODY_SIZE: usize = DEFAULT_BODY_SIZE> =
    ChunkInner<SocHeader, BODY_SIZE>;

/// Header of a single-owner chunk (SOC): `id || signature`, 97 wire bytes.
///
/// The address is `keccak256(id || owner)`, with the owner recovered from
/// the signature over `keccak256(id || body_hash)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SocHeader {
    /// Unique identifier the chunk is signed under
    id: SocId,
    /// Digital signature over the chunk's ID and body hash
    signature: Signature,
}

impl SocHeader {
    /// Create a new header with the given ID and signature
    pub const fn new(id: SocId, signature: Signature) -> Self {
        Self { id, signature }
    }

    /// Get the unique ID of this header
    pub const fn id(&self) -> SocId {
        self.id
    }

    /// Get the signature of this header
    pub const fn signature(&self) -> &Signature {
        &self.signature
    }

    /// EIP-191 message the owner signs: `keccak256(id || body_hash)`.
    pub fn owner_message(id: SocId, body_hash: B256) -> B256 {
        let mut hasher = Keccak256::new();
        hasher.update(id.as_slice());
        hasher.update(body_hash);
        hasher.finalize()
    }

    /// Recover the owner's address from the signature over `body_hash`.
    ///
    /// # Errors
    ///
    /// Returns `ChunkError::Signature` if the signature recovery fails.
    pub fn owner(&self, body_hash: B256) -> error::Result<Address> {
        self.signature
            .recover_address_from_msg(Self::owner_message(self.id, body_hash))
            .map_err(Into::into)
    }

    /// The SOC address derivation: `keccak256(id || owner)`.
    fn address_for(id: SocId, owner: Address) -> ChunkAddress {
        let mut hasher = Keccak256::new();
        hasher.update(id.as_slice());
        hasher.update(owner);
        ChunkAddress::from(hasher.finalize())
    }

    /// Dispersed-replica rule: `id[1..]` must equal `body_hash[1..]`; only the
    /// first id byte is mined.
    fn is_valid_replica(&self, body_hash: B256) -> bool {
        // Both slices are fixed 32-byte values, so split_first is always Some.
        let id_tail = self.id.as_slice().split_first().map(|(_, tail)| tail);
        let hash_tail = body_hash.as_slice().split_first().map(|(_, tail)| tail);
        id_tail == hash_tail
    }
}

impl ChunkHeader for SocHeader {
    const TYPE_ID: ChunkTypeId = ChunkTypeId::SINGLE_OWNER;
    const VERSION: ChunkVersion = ChunkVersion::new(0);
    const NAME: &'static str = "single_owner";
    const SIZE: usize = ID_SIZE + SIGNATURE_SIZE;

    /// Total commitment: an unrecoverable signature commits under the zero
    /// owner, an address [`validate`](ChunkHeader::validate) then rejects.
    fn commit(&self, body_hash: B256) -> ChunkAddress {
        let owner = self.owner(body_hash).unwrap_or(Address::ZERO);
        Self::address_for(self.id, owner)
    }

    fn validate(
        &self,
        body_hash: B256,
        expected: &ChunkAddress,
    ) -> std::result::Result<(), ChunkError> {
        let owner = self.owner(body_hash)?;

        // If the owner is the replica chunk owner, the ID must adhere to the
        // dispersed-replica semantics.
        if owner == DISPERSED_REPLICA_OWNER && !self.is_valid_replica(body_hash) {
            return Err(ChunkError::invalid_format("invalid dispersed replica"));
        }

        let actual = Self::address_for(self.id, owner);
        if actual != *expected {
            return Err(ChunkError::verification_failed(*expected, actual));
        }
        Ok(())
    }

    /// Plain (unprefixed) `keccak256(soc_address || transformed_root)`.
    fn seal_transformed(&self, address: &ChunkAddress, transformed_root: B256) -> ChunkAddress {
        let mut hasher = Keccak256::new();
        hasher.update(address);
        hasher.update(transformed_root);
        ChunkAddress::from(hasher.finalize())
    }

    fn encode(&self, out: &mut BytesMut) {
        out.extend_from_slice(self.id.as_slice());
        out.extend_from_slice(&self.signature.as_bytes());
    }

    fn decode(cursor: &mut wire::Cursor<'_>) -> std::result::Result<Self, ChunkError> {
        let id = SocId::new(cursor.take::<[u8; ID_SIZE]>()?);
        let signature = Signature::from_raw(&cursor.take::<[u8; SIGNATURE_SIZE]>()?)?;
        Ok(Self::new(id, signature))
    }
}

impl<const BODY_SIZE: usize> SingleOwnerChunk<BODY_SIZE> {
    /// Create a new single-owner chunk with the given ID, data, and signer.
    ///
    /// This function automatically calculates the span based on the data length
    /// and signs the chunk using the provided signer.
    ///
    /// # Arguments
    ///
    /// * `id` - The unique identifier for this chunk.
    /// * `data` - The raw data content to encapsulate in the chunk.
    /// * `signer` - The signer to use for signing the chunk.
    ///
    /// # Returns
    ///
    /// A Result containing the new SingleOwnerChunk, or an error if creation fails.
    #[must_use = "this returns a new chunk without modifying the input"]
    pub fn new(id: SocId, data: impl Into<Bytes>, signer: &impl SignerSync) -> Result<Self> {
        SingleOwnerChunkBuilderImpl::<BODY_SIZE, Initial>::default()
            .auto_from_data(data)?
            .with_id(id)
            .with_signer(signer)?
            .build()
    }

    /// Create a new SingleOwnerChunk with a pre-signed signature.
    ///
    /// This function is useful when the signature is already known, for example
    /// when retrieving a chunk from a database or when reconstructing after verification.
    ///
    /// # Arguments
    ///
    /// * `id` - The unique identifier for this chunk.
    /// * `signature` - The pre-computed signature.
    /// * `data` - The raw data content to encapsulate in the chunk.
    ///
    /// # Returns
    ///
    /// A Result containing the new SingleOwnerChunk, or an error if creation fails.
    #[must_use = "this returns a new chunk without modifying the input"]
    pub fn with_signature(id: SocId, signature: Signature, data: impl Into<Bytes>) -> Result<Self> {
        SingleOwnerChunkBuilderImpl::<BODY_SIZE, Initial>::default()
            .auto_from_data(data)?
            .with_id(id)
            .with_signature(signature)?
            .build()
    }

    /// Create a new `SingleOwnerChunk` as a dispersed replica.
    ///
    /// # Arguments
    /// * `mined_byte` - The first byte of the chunk's ID.
    /// * `body` - The underlying BMT body containing the data and metadata.
    #[must_use = "this returns a new chunk without modifying the input"]
    pub fn new_dispersed_replica(mined_byte: u8, body: BmtBody<BODY_SIZE>) -> Result<Self> {
        SingleOwnerChunkBuilderImpl::<BODY_SIZE, Initial>::default()
            .with_body(body)
            .dispersed_replica(mined_byte)?
            .build()
    }

    /// Create a SingleOwnerChunk from pre-computed parts.
    ///
    /// This is an advanced method for reconstructing chunks from storage
    /// when you have all the individual components. The address is derived
    /// from the parts on first use.
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk's unique identifier.
    /// * `signature` - The digital signature.
    /// * `body` - The BMT body containing the data.
    #[must_use]
    pub const fn from_parts(id: SocId, signature: Signature, body: BmtBody<BODY_SIZE>) -> Self {
        Self::from_header_and_body(SocHeader::new(id, signature), body)
    }

    /// Get the owner's address, derived from the signature.
    ///
    /// This recovers the owner's address from the signature over the chunk's
    /// ID and body hash. The body hash is cached; the recovery itself runs on
    /// every call.
    ///
    /// # Errors
    ///
    /// Returns `ChunkError::Signature` if the signature recovery fails.
    pub fn owner(&self) -> error::Result<Address> {
        self.header().owner(self.body().hash().into())
    }

    // Checks if the chunk is a valid dispersed replica
    #[cfg(test)]
    fn is_valid_replica(&self) -> bool {
        self.header().is_valid_replica(self.body().hash().into())
    }

    /// Get the ID of this chunk.
    pub const fn id(&self) -> SocId {
        self.header().id()
    }

    /// Get the signature of this chunk.
    pub const fn signature(&self) -> &Signature {
        self.header().signature()
    }

    /// Extract the content-addressed chunk (CAC) wrapped inside this SOC.
    ///
    /// The SOC body *is* a CAC body (`span || payload`), so this rebuilds the
    /// [`ContentChunk`] from it without any manual cursor arithmetic. The
    /// returned chunk's address is the wrapped content address, not the SOC
    /// address.
    #[must_use]
    pub fn unwrap_cac(&self) -> ContentChunk<BODY_SIZE> {
        ContentChunk::from_body(self.body().clone())
    }
}

impl<const BODY_SIZE: usize> fmt::Display for SingleOwnerChunk<BODY_SIZE> {
    #[allow(clippy::indexing_slicing)] // id is a fixed 32-byte value, so [..8] holds
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let owner_str = self.owner().map_or_else(
            |_| "invalid".to_string(),
            |addr| hex::encode(addr.as_slice()),
        );
        write!(
            f,
            "SingleOwnerChunk[id={}, owner={}]",
            hex::encode(&self.id().as_slice()[..8]),
            owner_str
        )
    }
}

// Internal builder state marker traits
trait BuilderState {}

#[derive(Debug, Default)]
struct Initial;
impl BuilderState for Initial {}

#[derive(Debug)]
struct WithData;
impl BuilderState for WithData {}

#[derive(Debug)]
struct WithId;
impl BuilderState for WithId {}

#[derive(Debug)]
struct ReadyToBuild;
impl BuilderState for ReadyToBuild {}

/// Builder for SingleOwnerChunk with type state pattern
#[derive(Debug)]
struct SingleOwnerChunkBuilderImpl<const BODY_SIZE: usize, S: BuilderState = Initial> {
    /// The body to use for the chunk
    body: Option<BmtBody<BODY_SIZE>>,
    /// The ID to use for the chunk
    id: Option<SocId>,
    /// The signature to use for the chunk
    signature: Option<Signature>,
    /// Marker for the builder state
    _state: PhantomData<S>,
}

impl<const BODY_SIZE: usize> Default for SingleOwnerChunkBuilderImpl<BODY_SIZE, Initial> {
    fn default() -> Self {
        Self {
            body: None,
            id: None,
            signature: None,
            _state: PhantomData,
        }
    }
}

impl<const BODY_SIZE: usize> SingleOwnerChunkBuilderImpl<BODY_SIZE, Initial> {
    /// Initialize from data with automatically calculated span
    fn auto_from_data(
        mut self,
        data: impl Into<Bytes>,
    ) -> Result<SingleOwnerChunkBuilderImpl<BODY_SIZE, WithData>> {
        let body = BmtBody::<BODY_SIZE>::builder()
            .auto_from_data(data)?
            .build()?;
        self.body = Some(body);

        Ok(SingleOwnerChunkBuilderImpl {
            body: self.body,
            id: self.id,
            signature: self.signature,
            _state: PhantomData,
        })
    }

    /// Initialize with a specific body
    fn with_body(
        mut self,
        body: BmtBody<BODY_SIZE>,
    ) -> SingleOwnerChunkBuilderImpl<BODY_SIZE, WithData> {
        self.body = Some(body);

        SingleOwnerChunkBuilderImpl {
            body: self.body,
            id: self.id,
            signature: self.signature,
            _state: PhantomData,
        }
    }
}

impl<const BODY_SIZE: usize> SingleOwnerChunkBuilderImpl<BODY_SIZE, WithData> {
    /// Set the ID for this chunk
    fn with_id(mut self, id: SocId) -> SingleOwnerChunkBuilderImpl<BODY_SIZE, WithId> {
        self.id = Some(id);

        SingleOwnerChunkBuilderImpl {
            body: self.body,
            id: self.id,
            signature: self.signature,
            _state: PhantomData,
        }
    }

    /// Creates a new dispersed replica chunk with the given first byte and transitions to ReadyToBuild
    #[allow(clippy::unwrap_used, clippy::indexing_slicing)] // the WithData typestate guarantees body is Some; id and body_hash are fixed 32-byte values; DISPERSED_REPLICA_OWNER_PK is a known-valid constant key
    fn dispersed_replica(
        self,
        first_byte: u8,
    ) -> Result<SingleOwnerChunkBuilderImpl<BODY_SIZE, ReadyToBuild>> {
        let body_hash = self.body.as_ref().unwrap().hash();
        let mut id = B256::default();
        id[0] = first_byte;
        id[1..].copy_from_slice(&body_hash.as_bytes()[1..]);

        let signer = PrivateKeySigner::from_slice(DISPERSED_REPLICA_OWNER_PK.as_slice()).unwrap();

        self.with_id(SocId::from(id)).with_signer(&signer)
    }
}

impl<const BODY_SIZE: usize> SingleOwnerChunkBuilderImpl<BODY_SIZE, WithId> {
    /// Sign the chunk with the given signer
    #[allow(clippy::unwrap_used)] // the WithId typestate guarantees body and id are Some
    fn with_signer(
        self,
        signer: &impl SignerSync,
    ) -> Result<SingleOwnerChunkBuilderImpl<BODY_SIZE, ReadyToBuild>> {
        // Get body and ID - these are guaranteed to be Some by the state
        let body = self.body.as_ref().unwrap();
        let id = *self.id.as_ref().unwrap();

        // Compute hash to sign
        let hash = SocHeader::owner_message(id, body.hash().into());

        // Sign the hash
        let signature = signer
            .sign_message_sync(hash.as_ref())
            .map_err(ChunkError::from)?;

        self.with_signature(signature)
    }

    /// Set a pre-computed signature
    fn with_signature(
        mut self,
        signature: Signature,
    ) -> Result<SingleOwnerChunkBuilderImpl<BODY_SIZE, ReadyToBuild>> {
        self.signature = Some(signature);

        Ok(SingleOwnerChunkBuilderImpl {
            body: self.body,
            id: self.id,
            signature: self.signature,
            _state: PhantomData,
        })
    }
}

impl<const BODY_SIZE: usize> SingleOwnerChunkBuilderImpl<BODY_SIZE, ReadyToBuild> {
    /// Build the final SingleOwnerChunk
    #[allow(clippy::unwrap_used)] // the ReadyToBuild typestate guarantees body, id and signature are Some
    fn build(self) -> Result<SingleOwnerChunk<BODY_SIZE>> {
        let body = self.body.unwrap();
        let id = self.id.unwrap();
        let signature = self.signature.unwrap();

        Ok(SingleOwnerChunk::from_parts(id, signature, body))
    }
}

#[cfg(any(test, feature = "arbitrary"))]
impl<const BODY_SIZE: usize> SingleOwnerChunk<BODY_SIZE> {
    /// Valid-by-construction generator: a chunk with a `u`-drawn id and body,
    /// signed by `signer` so ownership recovery and `verify` succeed.
    ///
    /// Deterministic in `u` for a deterministic signer; contrast with the raw
    /// `Arbitrary` impl, whose signature is unconstrained.
    pub fn arbitrary_signed(
        u: &mut arbitrary::Unstructured<'_>,
        signer: &impl SignerSync,
    ) -> arbitrary::Result<Self> {
        use arbitrary::Arbitrary;

        let id = SocId::arbitrary(u)?;
        let body = BmtBody::<BODY_SIZE>::arbitrary(u)?;

        SingleOwnerChunkBuilderImpl::<BODY_SIZE, Initial>::default()
            .with_body(body)
            .with_id(id)
            .with_signer(signer)
            .and_then(SingleOwnerChunkBuilderImpl::build)
            .map_err(|_| arbitrary::Error::IncorrectFormat)
    }
}

#[cfg(any(test, feature = "arbitrary"))]
impl<'a, const BODY_SIZE: usize> arbitrary::Arbitrary<'a> for SingleOwnerChunk<BODY_SIZE> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        // Raw tier: the signature is an unconstrained well-formed signature,
        // not a signature over the id and body, so ownership recovery and
        // address verification may fail. Use [`Self::arbitrary_signed`] or
        // `crate::generators` for a valid-by-construction chunk.
        let id = SocId::arbitrary(u)?;
        let signature = Signature::arbitrary(u)?;
        let body = BmtBody::<BODY_SIZE>::arbitrary(u)?;
        Ok(Self::from_parts(id, signature, body))
    }
}

#[cfg(test)]
mod tests {
    use crate::{DEFAULT_BODY_SIZE, PrimitivesError, chunk::Chunk};

    use super::*;
    use alloy_primitives::hex;
    use proptest::prelude::*;
    use proptest_arbitrary_interop::arb;

    type DefaultSingleOwnerChunk = SingleOwnerChunk<DEFAULT_BODY_SIZE>;

    fn get_test_wallet() -> PrivateKeySigner {
        // Test private key corresponding to address 0x8d3766440f0d7b949a5e32995d09619a7f86e632
        let pk = hex!("2c7536e3605d9c16a7a3d7b1898e529396a65c23a3bcbd4012a11cf2731b0fbc");
        PrivateKeySigner::from_slice(&pk).unwrap()
    }

    // Strategy for generating SingleOwnerChunk using the raw Arbitrary
    // implementation (signature unconstrained, so no verify assertions).
    fn chunk_strategy() -> impl Strategy<Value = DefaultSingleOwnerChunk> {
        arb::<DefaultSingleOwnerChunk>()
    }

    // Strategy for valid-by-construction chunks via `arbitrary_signed`.
    fn signed_chunk_strategy() -> impl Strategy<Value = DefaultSingleOwnerChunk> {
        proptest::collection::vec(any::<u8>(), 64..1024).prop_filter_map(
            "arbitrary_signed needs a signable draw",
            |bytes| {
                let mut u = arbitrary::Unstructured::new(&bytes);
                let signer = crate::generators::signer(&mut u).ok()?;
                DefaultSingleOwnerChunk::arbitrary_signed(&mut u, &signer).ok()
            },
        )
    }

    proptest! {
        #[test]
        fn test_chunk_properties(chunk in chunk_strategy()) {
            prop_assert!(chunk.size() >= SocHeader::SIZE);

            // Test round-trip conversion of the raw parts
            let bytes: Bytes = chunk.clone().into();
            let decoded = DefaultSingleOwnerChunk::try_from(bytes.as_ref()).unwrap();
            prop_assert_eq!(chunk.id(), decoded.id());
            prop_assert_eq!(chunk.signature(), decoded.signature());
            prop_assert_eq!(chunk.data(), decoded.data());
        }

        #[test]
        fn test_signed_chunk_verifies(chunk in signed_chunk_strategy()) {
            // Owner recovery succeeds and survives the round-trip
            let bytes: Bytes = chunk.clone().into();
            let decoded = DefaultSingleOwnerChunk::try_from(bytes.as_ref()).unwrap();
            prop_assert_eq!(chunk.owner().unwrap(), decoded.owner().unwrap());

            // Address verification succeeds
            let address = chunk.address();
            prop_assert!(chunk.verify(address).is_ok());
        }

        #[test]
        fn test_dispersed_replica_properties(first_byte in any::<u8>(), data in proptest::collection::vec(any::<u8>(), 1..DEFAULT_BODY_SIZE)) {
            let chunk = DefaultSingleOwnerChunk::new_dispersed_replica(first_byte, BmtBody::<DEFAULT_BODY_SIZE>::builder().auto_from_data(data).unwrap().build().unwrap()).unwrap();

            // Verify it's recognised as a dispersed replica
            prop_assert!(chunk.is_valid_replica());
            prop_assert_eq!(chunk.id().as_slice()[0], first_byte);
            prop_assert_eq!(chunk.owner().unwrap(), DISPERSED_REPLICA_OWNER);

            // Verify chunk address
            prop_assert!(chunk.verify(chunk.address()).is_ok());
        }

        #[test]
        fn test_chunk_creation(id in arb::<SocId>(), data in proptest::collection::vec(any::<u8>(), 1..DEFAULT_BODY_SIZE)) {
            let wallet = get_test_wallet();

            // Test creation through builder
            let chunk = SingleOwnerChunkBuilderImpl::<DEFAULT_BODY_SIZE, Initial>::default()
                .with_body(
                    BmtBody::<DEFAULT_BODY_SIZE>::builder()
                        .auto_from_data(data.clone())
                        .unwrap()
                        .build()
                        .unwrap(),
                )
                .with_id(id)
                .with_signer(&wallet)
                .unwrap()
                .build()
                .unwrap();

            prop_assert_eq!(chunk.id(), id);
            prop_assert_eq!(chunk.data(), &data);
            prop_assert!(!chunk.owner().unwrap().is_zero());
        }

        #[test]
        fn test_dispersed_replica_mismatched_address(first_byte in any::<u8>(), data in proptest::collection::vec(any::<u8>(), 1..DEFAULT_BODY_SIZE)) {
            let chunk = SingleOwnerChunkBuilderImpl::<DEFAULT_BODY_SIZE, Initial>::default().with_body(
                BmtBody::<DEFAULT_BODY_SIZE>::builder()
                    .auto_from_data(data)
                    .unwrap()
                    .build()
                    .unwrap(),
            ).dispersed_replica(first_byte).unwrap().build().unwrap();
            let replica_address = *chunk.address();
            // Serialise the chunk
            let bytes: Bytes = chunk.into();

            // Modify the ID (31 bytes), except the first byte to be random.
            // This should make the chunk not recognised as a dispersed replica
            let mut modified_bytes = bytes.to_vec();
            modified_bytes[1..ID_SIZE].copy_from_slice(&[0x01; 31]);

            let modified_chunk = DefaultSingleOwnerChunk::try_from(modified_bytes.as_slice()).unwrap();
            prop_assert!(!modified_chunk.is_valid_replica());
            prop_assert!(modified_chunk.verify(&replica_address).is_err());
        }

        #[test]
        fn test_chunk_invalid_signature(id in arb::<SocId>(), data in proptest::collection::vec(any::<u8>(), 1..DEFAULT_BODY_SIZE)) {
            let wallet = get_test_wallet();

            // Test creation through builder
            let chunk = DefaultSingleOwnerChunk::new(id, data, &wallet).unwrap();
            let original_address = *chunk.address();

            // Serialise the chunk
            let bytes: Bytes = chunk.into();

            // Modify the signature (65 bytes), except the first byte to be random.
            // This should make the chunk not recognised as a dispersed replica
            let mut modified_bytes = bytes.to_vec();
            modified_bytes[ID_SIZE..ID_SIZE + 65].copy_from_slice(&[0xff; 65]);

            let modified_chunk = DefaultSingleOwnerChunk::try_from(modified_bytes.as_slice()).unwrap();
            prop_assert!(modified_chunk.verify(&original_address).is_err());
            // Owner recovery should fail for invalid signature
            prop_assert!(modified_chunk.owner().is_err());
        }

        #[test]
        fn test_chunk_too_small(data in proptest::collection::vec(any::<u8>(), 1..SocHeader::SIZE)) {
            // Test insufficient data size
            let chunk = DefaultSingleOwnerChunk::try_from(data.as_slice());
            prop_assert!(chunk.is_err());
        }
    }

    #[test]
    fn test_new() {
        let id = SocId::ZERO;
        let data = b"foo".to_vec();
        let wallet = get_test_wallet();

        let chunk = DefaultSingleOwnerChunk::new(id, data.clone(), &wallet).unwrap();

        assert_eq!(chunk.id(), id);
        assert_eq!(chunk.data(), &data);
    }

    #[test]
    fn test_new_signed() {
        let id = SocId::ZERO;
        let data = b"foo".to_vec();

        // Known good signature from Go tests
        let sig = hex!(
            "5acd384febc133b7b245e5ddc62d82d2cded9182d2716126cd8844509af65a053deb418208027f548e3e88343af6f84a8772fb3cebc0a1833a0ea7ec0c1348311b"
        );
        let signature = Signature::try_from(sig.as_slice()).unwrap();

        let chunk = SingleOwnerChunkBuilderImpl::<DEFAULT_BODY_SIZE, Initial>::default()
            .auto_from_data(data.clone())
            .unwrap()
            .with_id(id)
            .with_signature(signature)
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(chunk.id(), id);
        assert_eq!(chunk.data(), &data);
        assert_eq!(chunk.signature().as_bytes(), sig);

        // Verify owner address matches expected
        let expected_owner = address!("8d3766440f0d7b949a5e32995d09619a7f86e632");
        assert_eq!(chunk.owner().unwrap(), expected_owner);
    }

    fn get_test_chunk_data() -> Vec<u8> {
        hex!(
            "000000000000000000000000000000000000000000000000000000000000000\
            05acd384febc133b7b245e5ddc62d82d2cded9182d2716126cd8844509af65a05\
            3deb418208027f548e3e88343af6f84a8772fb3cebc0a1833a0ea7ec0c134831\
            1b0300000000000000666f6f"
        )
        .to_vec()
    }

    #[test]
    fn test_chunk_address() {
        // Should parse successfully
        let chunk = DefaultSingleOwnerChunk::try_from(get_test_chunk_data().as_slice()).unwrap();

        // Verify expected owner
        let expected_owner = address!("8d3766440f0d7b949a5e32995d09619a7f86e632");
        assert_eq!(chunk.owner().unwrap(), expected_owner);

        // Verify expected address
        let expected_address =
            b256!("9d453ebb73b2fedaaf44ceddcf7a0aa37f3e3d6453fea5841c31f0ea6d61dc85");
        assert_eq!(chunk.address().as_ref(), expected_address);
    }

    #[test]
    fn test_invalid_dispersed_replica() -> Result<()> {
        let test_data = b"test data".to_vec();
        let dispersed_replica_wallet =
            PrivateKeySigner::from_slice(DISPERSED_REPLICA_OWNER_PK.as_slice()).unwrap();

        let chunk = SingleOwnerChunkBuilderImpl::<DEFAULT_BODY_SIZE, Initial>::default()
            .with_body(
                BmtBody::<DEFAULT_BODY_SIZE>::builder()
                    .auto_from_data(test_data)?
                    .build()?,
            )
            .with_id(SocId::ZERO)
            .with_signer(&dispersed_replica_wallet)?
            .build()?;
        let replica_address = chunk.address();

        assert!(!chunk.is_valid_replica());
        assert!(matches!(
            chunk.verify(replica_address),
            Err(PrimitivesError::Chunk(ChunkError::InvalidFormat { .. }))
        ));

        Ok(())
    }

    /// Decode the go-interop test vector's header and pin its wire shape:
    /// `id || signature`, 97 bytes, no type or version prefix.
    #[test]
    fn soc_header_decode_encode_round_trips_go_vector() {
        let wire = get_test_chunk_data();
        let mut cursor = wire::Cursor::new(&wire);
        let header = SocHeader::decode(&mut cursor).unwrap();

        assert_eq!(header.id(), SocId::ZERO);
        assert_eq!(cursor.remaining(), &wire[SocHeader::SIZE..]);

        let mut out = BytesMut::new();
        header.encode(&mut out);
        assert_eq!(out.len(), SocHeader::SIZE);
        assert_eq!(out.as_ref(), &wire[..SocHeader::SIZE]);
    }

    #[test]
    fn soc_header_decode_underrun_errors() {
        for len in [0, ID_SIZE, SocHeader::SIZE - 1] {
            let short = vec![0u8; len];
            let mut cursor = wire::Cursor::new(&short);
            assert!(matches!(
                SocHeader::decode(&mut cursor),
                Err(ChunkError::Underrun(_))
            ));
        }
    }

    /// The commit rule on the go vector: `keccak256(id || owner)` with the
    /// owner recovered over `keccak256(id || body_hash)`.
    #[test]
    fn soc_header_commit_reproduces_go_vector_address() {
        let chunk = DefaultSingleOwnerChunk::try_from(get_test_chunk_data().as_slice()).unwrap();
        let body_hash: B256 = chunk.body().hash().into();

        let expected = b256!("9d453ebb73b2fedaaf44ceddcf7a0aa37f3e3d6453fea5841c31f0ea6d61dc85");
        assert_eq!(
            chunk.header().commit(body_hash),
            ChunkAddress::from(expected)
        );
        assert!(chunk.header().validate(body_hash, &expected.into()).is_ok());
    }

    /// An unrecoverable signature commits under the zero owner and validate
    /// rejects it with a signature error, never an address compare.
    #[test]
    fn soc_header_bad_signature_commits_zero_owner_and_fails_validate() {
        let mut wire = get_test_chunk_data();
        wire[ID_SIZE..ID_SIZE + SIGNATURE_SIZE].copy_from_slice(&[0xff; SIGNATURE_SIZE]);

        let chunk = DefaultSingleOwnerChunk::try_from(wire.as_slice()).unwrap();
        let body_hash: B256 = chunk.body().hash().into();
        let header = chunk.header();

        // Commit is total: the zero owner stands in.
        let zero_owner_address = SocHeader::address_for(header.id(), Address::ZERO);
        assert_eq!(header.commit(body_hash), zero_owner_address);

        // Validate is not: even the committed (lying) address is rejected.
        assert!(matches!(
            header.validate(body_hash, &zero_owner_address),
            Err(ChunkError::Signature(_))
        ));
    }

    /// The dispersed-replica rule lives inside validate: a replica-owner
    /// signature over a non-replica id fails even at its committed address.
    #[test]
    fn soc_header_validate_enforces_replica_rule() {
        let signer = PrivateKeySigner::from_slice(DISPERSED_REPLICA_OWNER_PK.as_slice()).unwrap();
        let chunk = DefaultSingleOwnerChunk::new(SocId::ZERO, b"data".to_vec(), &signer).unwrap();
        let body_hash: B256 = chunk.body().hash().into();
        let committed = chunk.header().commit(body_hash);

        assert!(matches!(
            chunk.header().validate(body_hash, &committed),
            Err(ChunkError::InvalidFormat(_))
        ));

        // A well-formed replica id passes.
        let replica =
            DefaultSingleOwnerChunk::new_dispersed_replica(0x2a, chunk.body().clone()).unwrap();
        let replica_body_hash: B256 = replica.body().hash().into();
        assert!(
            replica
                .header()
                .validate(replica_body_hash, replica.address())
                .is_ok()
        );
    }

    /// SOC transformed sealing is the plain `keccak256(address || root)`.
    #[test]
    fn soc_header_seal_transformed_is_plain_keccak() {
        let chunk = DefaultSingleOwnerChunk::try_from(get_test_chunk_data().as_slice()).unwrap();
        let address = *chunk.address();
        let root = B256::repeat_byte(0x5a);

        let mut hasher = Keccak256::new();
        hasher.update(address);
        hasher.update(root);
        let expected = ChunkAddress::from(hasher.finalize());

        assert_eq!(chunk.header().seal_transformed(&address, root), expected);
    }

    #[test]
    fn soc_header_constants() {
        assert_eq!(SocHeader::SIZE, 97);
        assert_eq!(SocHeader::TYPE_ID, ChunkTypeId::SINGLE_OWNER);
        assert_eq!(SocHeader::VERSION, ChunkVersion::new(0));
        assert_eq!(SocHeader::NAME, "single_owner");
    }
}
