//! BMT body implementation for chunks
//!
//! This module provides the implementation of BMT (Binary Merkle Tree) bodies,
//! which form the basis for content-addressed chunks in the storage system.

use bytes::{Bytes, BytesMut};
use std::marker::PhantomData;
use std::sync::OnceLock;

use crate::SwarmAddress;
use crate::bmt::{Hasher, MAX_DATA_LENGTH};
use crate::chunk::error::{self, ChunkError};
use crate::error::{PrimitivesError, Result};

const SPAN_SIZE: usize = std::mem::size_of::<u64>();

/// A BMT body, which represents the data and metadata for a chunk.
///
/// This includes the span (size) of the data and the raw data itself.
/// It forms the basis for both content-addressed and single-owner chunks.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BmtBody {
    /// The span of the BMT body (size of data in bytes)
    span: u64,
    /// The raw data content
    data: Bytes,
    /// Cache for the BMT hash
    cached_hash: OnceLock<SwarmAddress>,
}

impl BmtBody {
    // Private constructor for internal use
    fn new_unchecked(span: u64, data: Bytes) -> Self {
        Self {
            span,
            data,
            cached_hash: OnceLock::new(),
        }
    }

    /// Create a new builder for BMTBody (crate-internal)
    pub(crate) fn builder() -> BmtBodyBuilder<Initial> {
        BmtBodyBuilder::default()
    }

    /// Get the span of this body
    pub fn span(&self) -> u64 {
        self.span
    }

    /// Get the data of this body
    pub fn data(&self) -> &Bytes {
        &self.data
    }

    /// Get the size of this body in bytes
    pub fn size(&self) -> usize {
        SPAN_SIZE + self.data.len()
    }

    /// Compute the BMT hash of this body
    pub fn hash(&self) -> SwarmAddress {
        *self.cached_hash.get_or_init(|| self.calculate_hash())
    }

    // Calculate the hash using the BMT hasher
    fn calculate_hash(&self) -> SwarmAddress {
        let mut hasher = Hasher::new();
        hasher.set_span(self.span);
        hasher.update(self.data.as_ref());
        hasher.sum().into()
    }
}

/// Validates the data size and returns the data as Bytes.
fn validate_data(data: impl Into<Bytes>) -> error::Result<Bytes> {
    let data = data.into();
    if data.len() > MAX_DATA_LENGTH {
        return Err(ChunkError::invalid_size(
            "data exceeds maximum chunk size",
            MAX_DATA_LENGTH,
            data.len(),
        ));
    }
    Ok(data)
}

impl From<BmtBody> for Bytes {
    fn from(body: BmtBody) -> Self {
        let mut bytes = BytesMut::with_capacity(body.size());
        bytes.extend(&body.span.to_le_bytes());
        bytes.extend(body.data());
        bytes.freeze()
    }
}

impl TryFrom<Bytes> for BmtBody {
    type Error = PrimitivesError;

    fn try_from(mut buf: Bytes) -> Result<Self> {
        if buf.len() < SPAN_SIZE {
            return Err(ChunkError::invalid_size(
                "insufficient data for span",
                SPAN_SIZE,
                buf.len(),
            )
            .into());
        }

        // Extract span bytes
        let span_bytes = buf.split_to(SPAN_SIZE);
        // This is safe because we checked that the buffer has at least SPAN_SIZE bytes
        let span = u64::from_le_bytes(span_bytes.as_ref().try_into().unwrap());

        // Remaining bytes are the data
        let data = buf;

        BmtBody::builder().with_span(span).with_data(data)?.build()
    }
}

impl TryFrom<&[u8]> for BmtBody {
    type Error = PrimitivesError;

    fn try_from(buf: &[u8]) -> Result<Self> {
        Self::try_from(Bytes::copy_from_slice(buf))
    }
}

/// Builder state marker traits (crate-internal)
pub(crate) trait BuilderState {}

#[derive(Default, Debug)]
pub(crate) struct Initial;
impl BuilderState for Initial {}

#[derive(Debug)]
pub(crate) struct WithSpan;
impl BuilderState for WithSpan {}

#[derive(Debug)]
pub(crate) struct ReadyToBuild;
impl BuilderState for ReadyToBuild {}

/// Builder for BMTBody with type state pattern (crate-internal)
#[derive(Debug)]
pub(crate) struct BmtBodyBuilder<S: BuilderState = Initial> {
    /// The span to use for the body
    span: Option<u64>,
    /// The data to use for the body
    data: Option<Bytes>,
    /// Marker for the builder state
    _state: PhantomData<S>,
}

impl Default for BmtBodyBuilder<Initial> {
    fn default() -> Self {
        Self {
            span: None,
            data: None,
            _state: PhantomData,
        }
    }
}

impl BmtBodyBuilder<Initial> {
    /// Set the span for this body and transition to WithSpan state
    pub(crate) fn with_span(mut self, span: u64) -> BmtBodyBuilder<WithSpan> {
        self.span = Some(span);
        BmtBodyBuilder {
            span: self.span,
            data: self.data,
            _state: PhantomData,
        }
    }

    /// Initialize from data with automatically calculated span
    pub(crate) fn auto_from_data(
        mut self,
        data: impl Into<Bytes>,
    ) -> Result<BmtBodyBuilder<ReadyToBuild>> {
        let data = validate_data(data)?;
        let len = data.len();
        self.data = Some(data);
        self.span = Some(len as u64);

        Ok(BmtBodyBuilder {
            span: self.span,
            data: self.data,
            _state: PhantomData,
        })
    }
}

impl BmtBodyBuilder<WithSpan> {
    /// Set the data for this body and transition to ReadyToBuild state
    pub(crate) fn with_data(
        mut self,
        data: impl Into<Bytes>,
    ) -> Result<BmtBodyBuilder<ReadyToBuild>> {
        let data = validate_data(data)?;
        let data_len = data.len();
        self.data = Some(data);

        let span = self.span.unwrap();
        if span <= MAX_DATA_LENGTH as u64 && data_len != span as usize {
            return Err(ChunkError::invalid_size(
                "span does not match data size",
                span as usize,
                data_len,
            )
            .into());
        }

        Ok(BmtBodyBuilder {
            span: self.span,
            data: self.data,
            _state: PhantomData,
        })
    }
}

impl BmtBodyBuilder<ReadyToBuild> {
    /// Build the final BMTBody
    pub(crate) fn build(self) -> Result<BmtBody> {
        // This is safe because it is only possible to get here with valid data and span
        Ok(BmtBody::new_unchecked(
            self.span.unwrap(),
            self.data.unwrap(),
        ))
    }
}

#[cfg(any(test, feature = "arbitrary"))]
impl<'a> arbitrary::Arbitrary<'a> for BmtBody {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        // Generate a random span value
        let span = u64::arbitrary(u)?;

        // Ensure data size does not exceed MAX_DATA_LENGTH
        let data_len: usize = u.int_in_range(0..=MAX_DATA_LENGTH)?;
        let mut buf = vec![0; data_len];
        u.fill_buffer(&mut buf)?;

        Ok(BmtBodyBuilder::default()
            .with_span(span)
            .with_data(buf)
            .unwrap()
            .build()
            .unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use proptest_arbitrary_interop::arb;

    // Define strategies for generating BMTBody using the Arbitrary implementation
    fn bmt_body_strategy() -> impl Strategy<Value = BmtBody> {
        arb::<BmtBody>()
    }

    fn create_bmt_body(span: u64, data: Vec<u8>) -> Result<BmtBody> {
        BmtBody::builder().with_span(span).with_data(data)?.build()
    }

    proptest! {
        #[test]
        fn test_bmt_body_properties(body in bmt_body_strategy()) {
            // Test that span is within valid range
            prop_assert!(body.span() <= u64::MAX);

            // Test that data size is within valid range
            prop_assert!(body.data().len() <= MAX_DATA_LENGTH);

            // Test that total size is correct
            prop_assert_eq!(body.size(), SPAN_SIZE + body.data().len());

            // Test serialisation / deserialisation
            let bytes: Bytes = body.clone().into();
            let decoded = BmtBody::try_from(bytes).unwrap();
            prop_assert_eq!(body, decoded);
        }

        #[test]
        fn test_bmt_body_size_validation(span in 0..=u64::MAX, data_len in MAX_DATA_LENGTH + 1..=MAX_DATA_LENGTH * 2) {
            let data = vec![0; data_len];
            let result = create_bmt_body(span, data);
            assert!(matches!(result, Err(PrimitivesError::Chunk(ChunkError::InvalidSize { .. }))));
        }

        #[test]
        fn test_bmt_body_builder_properties(
            span in 0..=u64::MAX,
            data_len in 0..=MAX_DATA_LENGTH,
        ) {
            let data = vec![0; data_len];
            let builder = BmtBodyBuilder::default()
                .with_span(span)
                .with_data(data.clone())?;

            let body: BmtBody = builder.build().unwrap();
            assert_eq!(body.span(), span);
            assert_eq!(body.data(), &data);
            prop_assert_eq!(body.size(), SPAN_SIZE + data.len());
        }

        #[test]
        fn test_span_data_length_mismatch(
            span in 0..=MAX_DATA_LENGTH as u64,
            data_len in 0..=MAX_DATA_LENGTH,
        ) {
            let data = vec![0; data_len];
            let result = BmtBody::builder()
                .with_span(span)
                .with_data(data.clone());

            if span <= MAX_DATA_LENGTH as u64 && data.len() != span as usize {
                assert!(matches!(result, Err(PrimitivesError::Chunk(ChunkError::InvalidSize { .. }))));
            } else {
                assert!(matches!(result, Ok(_)));
            }
        }
    }

    #[test]
    fn test_bmt_body_creation() {
        let span = 5;
        let data = vec![1, 2, 3, 4, 5];
        let body = create_bmt_body(span, data.clone()).unwrap();

        assert_eq!(body.span(), span);
        assert_eq!(body.data(), &data);
        assert_eq!(body.size(), SPAN_SIZE + data.len());
    }

    #[test]
    fn test_bmt_body_from_bytes() {
        let mut input = Vec::new();
        input.extend_from_slice(&5u64.to_le_bytes()); // Span
        input.extend_from_slice(&[1, 2, 3, 4, 5]); // Data

        let body = BmtBody::try_from(Bytes::from(input)).unwrap();
        assert_eq!(body.span(), 5);
        assert_eq!(body.data(), &[1, 2, 3, 4, 5].as_slice());
    }

    #[test]
    fn test_hash_caching() {
        let body = create_bmt_body(3, vec![1, 2, 3]).unwrap();

        let hash1 = body.hash();
        let hash2 = body.hash();
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_size_validation() {
        let result = BmtBody::builder()
            .with_span(42)
            .with_data(vec![0; MAX_DATA_LENGTH + 1]);

        assert!(matches!(
            result,
            Err(PrimitivesError::Chunk(ChunkError::InvalidSize { .. }))
        ));

        let result = BmtBody::try_from(vec![0; MAX_DATA_LENGTH + 9].as_slice());
        assert!(matches!(
            result,
            Err(PrimitivesError::Chunk(ChunkError::InvalidSize { .. }))
        ));
    }
}
