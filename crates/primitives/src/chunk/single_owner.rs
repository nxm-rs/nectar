//! Single-owner chunk implementation
//!
//! This module provides the implementation of single-owner chunks,
//! which are chunks that include an owner identifier and signature.

use alloy_primitives::{Address, B256, FixedBytes, Keccak256, Signature, address, b256, hex};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use bytes::{Bytes, BytesMut};
use std::fmt;
use std::marker::PhantomData;

use crate::PrimitivesError;
use crate::cache::OnceCache;
use crate::chunk::error::{self, ChunkError};
use crate::error::Result;

use super::bmt_body::BmtBody;
use super::traits::{BmtChunk, Chunk, ChunkAddress, ChunkHeader, ChunkMetadata};

// Constants for field sizes
const ID_SIZE: usize = std::mem::size_of::<B256>();
const SIGNATURE_SIZE: usize = 65;
const MIN_SOC_FIELDS_SIZE: usize = ID_SIZE + SIGNATURE_SIZE;

/// The address of the owner of the SOC for dispersed replicas.
const DISPERSED_REPLICA_OWNER: Address = address!("0xdc5b20847f43d67928f49cd4f85d696b5a7617b5");
/// Generated from the private key `0x0100000000000000000000000000000000000000000000000000000000000000`.
const DISPERSED_REPLICA_OWNER_PK: B256 =
    b256!("0x0100000000000000000000000000000000000000000000000000000000000000");

/// A single-owner chunk.
///
/// This type represents a chunk of data that belongs to a specific owner
/// and includes a digital signature proving ownership.
#[derive(Debug, Clone)]
pub struct SingleOwnerChunk {
    /// The header containing type ID, version, and metadata (ID and signature)
    header: SingleOwnerChunkHeader,
    /// The body of the chunk, containing the actual data
    body: BmtBody,
    /// Cache for the chunk's address
    chunk_address_cache: OnceCache<ChunkAddress>,
    /// Cache for the chunk's owner address (derived from signature)
    owner_cache: OnceCache<Address>,
}

/// Metadata for a single-owner chunk
#[derive(Debug, Clone)]
pub struct SingleOwnerChunkMetadata {
    /// Unique identifier for this chunk
    id: B256,
    /// Digital signature of the chunk's ID and body hash
    signature: Signature,
}

impl SingleOwnerChunkMetadata {
    /// Create a new metadata instance with the given ID and signature
    pub fn new(id: B256, signature: Signature) -> Self {
        Self { id, signature }
    }

    /// Get the unique ID of this chunk
    pub fn id(&self) -> B256 {
        self.id
    }

    /// Get the signature of this chunk
    pub fn signature(&self) -> &Signature {
        &self.signature
    }
}

impl ChunkMetadata for SingleOwnerChunkMetadata {
    fn bytes(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(ID_SIZE + SIGNATURE_SIZE);
        bytes.extend_from_slice(self.id.as_ref());
        bytes.extend_from_slice(&self.signature.as_bytes());
        bytes.freeze()
    }
}

/// Header for a single-owner chunk
#[derive(Debug, Clone)]
pub struct SingleOwnerChunkHeader {
    metadata: SingleOwnerChunkMetadata,
}

impl SingleOwnerChunkHeader {
    /// Create a new header with the given metadata
    pub fn new(metadata: SingleOwnerChunkMetadata) -> Self {
        Self { metadata }
    }
}

impl ChunkHeader for SingleOwnerChunkHeader {
    type Metadata = SingleOwnerChunkMetadata;

    fn id(&self) -> u8 {
        1
    }

    fn version(&self) -> u8 {
        1
    }

    fn metadata(&self) -> &Self::Metadata {
        &self.metadata
    }

    fn bytes(&self) -> Bytes {
        self.metadata.bytes()
    }
}

impl SingleOwnerChunk {
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
    pub fn new(id: B256, data: impl Into<Bytes>, signer: &impl SignerSync) -> Result<Self> {
        SingleOwnerChunkBuilderImpl::default()
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
    pub fn with_signature(id: B256, signature: Signature, data: impl Into<Bytes>) -> Result<Self> {
        SingleOwnerChunkBuilderImpl::default()
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
    pub fn new_dispersed_replica(mined_byte: u8, body: BmtBody) -> Result<Self> {
        SingleOwnerChunkBuilderImpl::default()
            .with_body(body)
            .dispersed_replica(mined_byte)?
            .build()
    }

    /// Get the owner's address, derived from the signature.
    ///
    /// This computes the owner's address by recovering it from the signature
    /// and the signed data (the chunk's ID and body hash).
    ///
    /// # Returns
    ///
    /// The owner's address as a 20-byte fixed array.
    pub fn owner(&self) -> Address {
        *self
            .owner_cache
            .get_or_compute(|| self.calculate_owner().unwrap_or(Address::ZERO))
    }

    /// Calculate the owner's address from the signature.
    fn calculate_owner(&self) -> error::Result<Address> {
        // Generate the hash to verify
        let hash = Self::to_sign(&self.header.metadata.id, &self.body);

        // Recover the address from the signature
        self.signature()
            .recover_address_from_msg(hash)
            .map_err(Into::into)
    }

    /// Compute the data to be signed for this chunk.
    ///
    /// This combines the chunk's ID and body hash to create the data
    /// that is signed to prove ownership.
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk's ID.
    /// * `body` - The chunk's body.
    ///
    /// # Returns
    ///
    /// A 32-byte hash representing the data to sign.
    fn to_sign(id: &B256, body: &BmtBody) -> B256 {
        let mut hasher = Keccak256::new();
        hasher.update(id);
        hasher.update(body.hash());
        hasher.finalize()
    }

    // Checks if the chunk is a valid dispersed replica
    fn is_valid_replica(&self) -> bool {
        self.id()[1..] == self.body.hash().as_slice()[1..]
    }

    /// Get the ID of this chunk.
    pub fn id(&self) -> B256 {
        self.header.metadata.id
    }

    /// Get the signature of this chunk.
    pub fn signature(&self) -> &Signature {
        &self.header.metadata.signature
    }
}

impl Chunk for SingleOwnerChunk {
    type Header = SingleOwnerChunkHeader;

    fn address(&self) -> &ChunkAddress {
        self.chunk_address_cache.get_or_compute(|| {
            // Compute address from id and owner
            let mut hasher = Keccak256::new();
            hasher.update(self.id());
            hasher.update(self.owner());

            hasher.finalize().into()
        })
    }

    fn data(&self) -> &Bytes {
        self.body.data()
    }

    fn size(&self) -> usize {
        self.header().bytes().len() + self.body.size()
    }

    fn header(&self) -> &Self::Header {
        &self.header
    }

    fn verify(&self, expected: &ChunkAddress) -> Result<()> {
        let actual = self.address();

        // At this point, the owner has been recovered. Now check if the owner
        // is the replica chunk owner, the ID must adhere to specific semantics.
        if self.owner() == DISPERSED_REPLICA_OWNER && !self.is_valid_replica() {
            return Err(error::ChunkError::invalid_format("invalid dispersed replica").into());
        }

        if actual != expected {
            return Err(error::ChunkError::verification_failed(*expected, *actual).into());
        }
        Ok(())
    }
}

impl BmtChunk for SingleOwnerChunk {
    fn span(&self) -> u64 {
        self.body.span()
    }
}

impl From<SingleOwnerChunk> for Bytes {
    fn from(chunk: SingleOwnerChunk) -> Self {
        let mut bytes = BytesMut::with_capacity(chunk.size());
        bytes.extend_from_slice(chunk.header().bytes().as_ref());
        bytes.extend_from_slice(&Bytes::from(chunk.body));
        bytes.freeze()
    }
}

impl TryFrom<Bytes> for SingleOwnerChunk {
    type Error = PrimitivesError;

    fn try_from(bytes: Bytes) -> Result<Self> {
        if bytes.len() < MIN_SOC_FIELDS_SIZE {
            return Err(ChunkError::invalid_size(
                "insufficient data for single-owner chunk",
                MIN_SOC_FIELDS_SIZE,
                bytes.len(),
            )
            .into());
        }

        // Extract ID
        let id_slice = &bytes.slice(0..ID_SIZE);
        let mut id = FixedBytes::<32>::default();
        id.copy_from_slice(id_slice);

        // Extract signature
        let sig_slice = &bytes.slice(ID_SIZE..ID_SIZE + SIGNATURE_SIZE);
        let signature = Signature::from_raw(sig_slice).map_err(ChunkError::from)?;

        // Extract body
        let body_bytes = bytes.slice(ID_SIZE + SIGNATURE_SIZE..);
        let body = BmtBody::try_from(body_bytes)?;

        // Create metadata and header
        let metadata = SingleOwnerChunkMetadata::new(id, signature);
        let header = SingleOwnerChunkHeader::new(metadata);

        Ok(Self {
            header,
            body,
            chunk_address_cache: OnceCache::new(),
            owner_cache: OnceCache::new(),
        })
    }
}

impl TryFrom<&[u8]> for SingleOwnerChunk {
    type Error = PrimitivesError;

    fn try_from(bytes: &[u8]) -> Result<Self> {
        Self::try_from(Bytes::copy_from_slice(bytes))
    }
}

impl fmt::Display for SingleOwnerChunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SingleOwnerChunk[id={}, owner={}]",
            hex::encode(&self.id()[..8]),
            hex::encode(&self.owner()[..])
        )
    }
}

impl PartialEq for SingleOwnerChunk {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id() && self.owner() == other.owner()
    }
}

impl Eq for SingleOwnerChunk {}

/// Builder for creating SingleOwnerChunk instances.
///
/// This builder provides a fluent interface for constructing single-owner chunks
/// with various configuration options.
#[derive(Debug)]
pub struct SingleOwnerChunkBuilder(SingleOwnerChunkBuilderImpl<Initial>);

// Public builder facade - with data state
/// Builder for SingleOwnerChunk with data set.
#[derive(Debug)]
pub struct SingleOwnerChunkBuilderWithData(SingleOwnerChunkBuilderImpl<WithData>);

// Public builder facade - with ID state
/// Builder for SingleOwnerChunk with data and ID set.
#[derive(Debug)]
pub struct SingleOwnerChunkBuilderWithId(SingleOwnerChunkBuilderImpl<WithId>);

// Public builder facade - ready to build state
/// Final stage of the SingleOwnerChunk builder, ready to build the chunk.
#[derive(Debug)]
pub struct SingleOwnerChunkBuilderReady(SingleOwnerChunkBuilderImpl<ReadyToBuild>);

// Implement the public facades with simplified API
impl Default for SingleOwnerChunkBuilder {
    fn default() -> Self {
        Self(SingleOwnerChunkBuilderImpl::default())
    }
}

impl SingleOwnerChunkBuilder {
    /// Initialize the builder with data using an automatically calculated span.
    pub fn auto_from_data(self, data: impl Into<Bytes>) -> Result<SingleOwnerChunkBuilderWithData> {
        Ok(SingleOwnerChunkBuilderWithData(
            self.0.auto_from_data(data)?,
        ))
    }

    /// Initialize the builder with a specific BMT body.
    pub fn with_body(self, body: BmtBody) -> SingleOwnerChunkBuilderWithData {
        SingleOwnerChunkBuilderWithData(self.0.with_body(body))
    }
}

impl SingleOwnerChunkBuilderWithData {
    /// Set the ID for this chunk.
    pub fn with_id(self, id: B256) -> SingleOwnerChunkBuilderWithId {
        SingleOwnerChunkBuilderWithId(self.0.with_id(id))
    }
}

impl SingleOwnerChunkBuilderWithId {
    /// Sign the chunk with the given signer.
    pub fn with_signer(self, signer: &impl SignerSync) -> Result<SingleOwnerChunkBuilderReady> {
        Ok(SingleOwnerChunkBuilderReady(self.0.with_signer(signer)?))
    }

    /// Set a pre-computed signature.
    pub fn with_signature(self, signature: Signature) -> Result<SingleOwnerChunkBuilderReady> {
        Ok(SingleOwnerChunkBuilderReady(
            self.0.with_signature(signature)?,
        ))
    }
}

impl SingleOwnerChunkBuilderReady {
    /// Set a pre-computed address for the chunk.
    pub fn with_address(self, address: ChunkAddress) -> Self {
        SingleOwnerChunkBuilderReady(self.0.with_address(address))
    }

    /// Set a pre-computed owner for the chunk.
    pub fn with_owner(self, owner: Address) -> Self {
        SingleOwnerChunkBuilderReady(self.0.with_owner(owner))
    }

    /// Build the final SingleOwnerChunk.
    pub fn build(self) -> Result<SingleOwnerChunk> {
        self.0.build()
    }
}

/// Builder state marker traits
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
struct SingleOwnerChunkBuilderImpl<S: BuilderState = Initial> {
    /// The body to use for the chunk
    body: Option<BmtBody>,
    /// The ID to use for the chunk
    id: Option<B256>,
    /// The signature to use for the chunk
    signature: Option<Signature>,
    /// Pre-computed address for the chunk
    address: Option<ChunkAddress>,
    /// Pre-computed owner for the chunk
    owner: Option<Address>,
    /// Marker for the builder state
    _state: PhantomData<S>,
}

impl Default for SingleOwnerChunkBuilderImpl<Initial> {
    fn default() -> Self {
        Self {
            body: None,
            id: None,
            signature: None,
            address: None,
            owner: None,
            _state: PhantomData,
        }
    }
}

impl SingleOwnerChunkBuilderImpl<Initial> {
    /// Initialize from data with automatically calculated span
    fn auto_from_data(
        mut self,
        data: impl Into<Bytes>,
    ) -> Result<SingleOwnerChunkBuilderImpl<WithData>> {
        let body = BmtBody::builder().auto_from_data(data)?.build()?;
        self.body = Some(body);

        Ok(SingleOwnerChunkBuilderImpl {
            body: self.body,
            id: self.id,
            signature: self.signature,
            address: self.address,
            owner: self.owner,
            _state: PhantomData,
        })
    }

    /// Initialize with a specific body
    fn with_body(mut self, body: BmtBody) -> SingleOwnerChunkBuilderImpl<WithData> {
        self.body = Some(body);

        SingleOwnerChunkBuilderImpl {
            body: self.body,
            id: self.id,
            signature: self.signature,
            address: self.address,
            owner: self.owner,
            _state: PhantomData,
        }
    }
}

impl SingleOwnerChunkBuilderImpl<WithData> {
    /// Set the ID for this chunk
    fn with_id(mut self, id: B256) -> SingleOwnerChunkBuilderImpl<WithId> {
        self.id = Some(id);

        SingleOwnerChunkBuilderImpl {
            body: self.body,
            id: self.id,
            signature: self.signature,
            address: self.address,
            owner: self.owner,
            _state: PhantomData,
        }
    }

    /// Creates a new dispersed replica chunk with the given first byte and transitions to ReadyToBuild
    fn dispersed_replica(
        self,
        first_byte: u8,
    ) -> Result<SingleOwnerChunkBuilderImpl<ReadyToBuild>> {
        let body_hash = self.body.as_ref().unwrap().hash();
        let mut id = B256::default();
        id[0] = first_byte;
        id[1..].copy_from_slice(&body_hash.as_slice()[1..]);

        let signer = PrivateKeySigner::from_slice(DISPERSED_REPLICA_OWNER_PK.as_slice()).unwrap();

        self.with_id(id).with_signer(&signer)
    }
}

impl SingleOwnerChunkBuilderImpl<WithId> {
    /// Sign the chunk with the given signer
    fn with_signer(
        self,
        signer: &impl SignerSync,
    ) -> Result<SingleOwnerChunkBuilderImpl<ReadyToBuild>> {
        // Get body and ID - these are guaranteed to be Some by the state
        let body = self.body.as_ref().unwrap();
        let id = self.id.as_ref().unwrap();

        // Compute hash to sign
        let hash = SingleOwnerChunk::to_sign(id, body);

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
    ) -> Result<SingleOwnerChunkBuilderImpl<ReadyToBuild>> {
        self.signature = Some(signature);

        Ok(SingleOwnerChunkBuilderImpl {
            body: self.body,
            id: self.id,
            signature: self.signature,
            address: self.address,
            owner: self.owner,
            _state: PhantomData,
        })
    }
}

impl SingleOwnerChunkBuilderImpl<ReadyToBuild> {
    /// Set a pre-computed address for the chunk
    fn with_address(mut self, address: ChunkAddress) -> Self {
        self.address = Some(address);
        self
    }

    /// Set a pre-computed owner for the chunk
    fn with_owner(mut self, owner: Address) -> Self {
        self.owner = Some(owner);
        self
    }

    /// Build the final SingleOwnerChunk
    fn build(self) -> Result<SingleOwnerChunk> {
        let body = self.body.unwrap();
        let id = self.id.unwrap();
        let signature = self.signature.unwrap();

        // Create metadata and header
        let metadata = SingleOwnerChunkMetadata::new(id, signature);
        let header = SingleOwnerChunkHeader::new(metadata);

        let chunk_address_cache = match self.address {
            Some(addr) => OnceCache::with_value(addr),
            None => OnceCache::new(),
        };

        let owner_cache = match self.owner {
            Some(addr) => OnceCache::with_value(addr),
            None => OnceCache::new(),
        };

        Ok(SingleOwnerChunk {
            header,
            body,
            chunk_address_cache,
            owner_cache,
        })
    }
}

#[cfg(any(test, feature = "arbitrary"))]
impl<'a> arbitrary::Arbitrary<'a> for SingleOwnerChunk {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let id = B256::arbitrary(u)?;
        let body = BmtBody::arbitrary(u)?;
        let signer = alloy_signer_local::PrivateKeySigner::random();

        Ok(SingleOwnerChunkBuilder::default()
            .with_body(body)
            .with_id(id)
            .with_signer(&signer)
            .unwrap()
            .build()
            .unwrap())
    }
}

#[cfg(test)]
mod tests {
    use crate::MAX_CHUNK_SIZE;

    use super::*;
    use alloy_primitives::hex;
    use proptest::prelude::*;
    use proptest_arbitrary_interop::arb;

    fn get_test_wallet() -> PrivateKeySigner {
        // Test private key corresponding to address 0x8d3766440f0d7b949a5e32995d09619a7f86e632
        let pk = hex!("2c7536e3605d9c16a7a3d7b1898e529396a65c23a3bcbd4012a11cf2731b0fbc");
        PrivateKeySigner::from_slice(&pk).unwrap()
    }

    // Strategy for generating SingleOwnerChunk using the Arbitrary implementation
    fn chunk_strategy() -> impl Strategy<Value = SingleOwnerChunk> {
        arb::<SingleOwnerChunk>()
    }

    proptest! {
        #[test]
        fn test_chunk_properties(chunk in chunk_strategy()) {
            // Test basic properties
            prop_assert!(!chunk.id().is_zero());
            prop_assert!(!chunk.data().is_empty());
            prop_assert!(chunk.size() >= MIN_SOC_FIELDS_SIZE);

            // Test round-trip conversion
            let bytes: Bytes = chunk.clone().into();
            let decoded = SingleOwnerChunk::try_from(bytes.as_ref()).unwrap();
            prop_assert_eq!(chunk.id(), decoded.id());
            prop_assert_eq!(chunk.signature(), decoded.signature());
            prop_assert_eq!(chunk.data(), decoded.data());
            prop_assert_eq!(chunk.owner(), decoded.owner());

            // Test address verification
            let address = chunk.address();
            prop_assert!(chunk.verify(address).is_ok());
        }

        #[test]
        fn test_dispersed_replica_properties(first_byte in any::<u8>(), data in proptest::collection::vec(any::<u8>(), 1..MAX_CHUNK_SIZE)) {
            let chunk = SingleOwnerChunk::new_dispersed_replica(first_byte, BmtBody::builder().auto_from_data(data).unwrap().build().unwrap()).unwrap();

            // Verify it's recognised as a dispersed replica
            prop_assert!(chunk.is_valid_replica());
            prop_assert_eq!(chunk.id()[0], first_byte);
            prop_assert_eq!(chunk.owner(), DISPERSED_REPLICA_OWNER);

            // Verify chunk address
            prop_assert!(chunk.verify(chunk.address()).is_ok());
        }

        #[test]
        fn test_chunk_creation(id in arb::<B256>(), data in proptest::collection::vec(any::<u8>(), 1..MAX_CHUNK_SIZE)) {
            let wallet = get_test_wallet();

            // Test creation through builder
            let chunk = SingleOwnerChunkBuilderImpl::default()
                .with_body(
                    BmtBody::builder()
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
            prop_assert!(!chunk.owner().is_zero());
        }

        #[test]
        fn test_dispersed_replica_mismatched_address(first_byte in any::<u8>(), data in proptest::collection::vec(any::<u8>(), 1..MAX_CHUNK_SIZE)) {
            let chunk = SingleOwnerChunkBuilderImpl::default().with_body(
                BmtBody::builder()
                    .auto_from_data(data.clone())
                    .unwrap()
                    .build()
                    .unwrap(),
            ).dispersed_replica(first_byte).unwrap().build().unwrap();
            let replica_address = chunk.address().clone();
            // Serialise the chunk
            let bytes: Bytes = chunk.into();

            // Modify the ID (31 bytes), except the first byte to be random.
            // This should make the chunk not recognised as a dispersed replica
            let mut modified_bytes = bytes.to_vec();
            modified_bytes[1..ID_SIZE].copy_from_slice(&[0x01; 31]);

            let modified_chunk = SingleOwnerChunk::try_from(modified_bytes.as_slice()).unwrap();
            prop_assert!(!modified_chunk.is_valid_replica());
            prop_assert!(modified_chunk.verify(&replica_address).is_err());
        }

        #[test]
        fn test_chunk_invalid_signature(id in arb::<B256>(), data in proptest::collection::vec(any::<u8>(), 1..MAX_CHUNK_SIZE)) {
            let wallet = get_test_wallet();

            // Test creation through builder
            let chunk = SingleOwnerChunk::new(id, data.clone(), &wallet).unwrap();
            let original_address = chunk.address().clone();

            // Serialise the chunk
            let bytes: Bytes = chunk.into();

            // Modify the signature (65 bytes), except the first byte to be random.
            // This should make the chunk not recognised as a dispersed replica
            let mut modified_bytes = bytes.to_vec();
            modified_bytes[ID_SIZE..ID_SIZE + 65].copy_from_slice(&[0xff; 65]);

            let modified_chunk = SingleOwnerChunk::try_from(modified_bytes.as_slice()).unwrap();
            prop_assert!(modified_chunk.verify(&original_address).is_err());
            prop_assert!(modified_chunk.owner() == Address::ZERO);
        }

        #[test]
        fn test_chunk_too_small(data in proptest::collection::vec(any::<u8>(), 1..MIN_SOC_FIELDS_SIZE)) {
            // Test insufficient data size
            let chunk = SingleOwnerChunk::try_from(data.as_slice());
            prop_assert!(chunk.is_err());
        }
    }

    #[test]
    fn test_new() {
        let id = B256::ZERO;
        let data = b"foo".to_vec();
        let wallet = get_test_wallet();

        let chunk = SingleOwnerChunk::new(id, data.clone(), &wallet).unwrap();

        assert_eq!(chunk.id(), id);
        assert_eq!(chunk.data(), &data);
    }

    #[test]
    fn test_new_signed() {
        let id = B256::ZERO;
        let data = b"foo".to_vec();

        // Known good signature from Go tests
        let sig = hex!(
            "5acd384febc133b7b245e5ddc62d82d2cded9182d2716126cd8844509af65a053deb418208027f548e3e88343af6f84a8772fb3cebc0a1833a0ea7ec0c1348311b"
        );
        let signature = Signature::try_from(sig.as_slice()).unwrap();

        let chunk = SingleOwnerChunkBuilderImpl::default()
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
        assert_eq!(chunk.owner(), expected_owner);
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
        let chunk = SingleOwnerChunk::try_from(get_test_chunk_data().as_slice()).unwrap();

        // Verify expected owner
        let expected_owner = address!("8d3766440f0d7b949a5e32995d09619a7f86e632");
        assert_eq!(chunk.owner(), expected_owner);

        // Verify expected address
        let expected_address =
            b256!("9d453ebb73b2fedaaf44ceddcf7a0aa37f3e3d6453fea5841c31f0ea6d61dc85");
        assert_eq!(chunk.address().as_ref(), expected_address);
    }

    #[test]
    fn test_invalid_dispersed_replica() -> Result<()> {
        let test_data = b"test data".to_vec();
        let dispersed_replica_wallet =
            PrivateKeySigner::from_slice(&DISPERSED_REPLICA_OWNER_PK.as_slice()).unwrap();

        let chunk = SingleOwnerChunkBuilderImpl::default()
            .with_body(
                BmtBody::builder()
                    .auto_from_data(test_data.clone())?
                    .build()?,
            )
            .with_id(B256::ZERO)
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
}
