//! BMT body implementation for chunks
//!
//! This module provides the implementation of BMT (Binary Merkle Tree) bodies,
//! which form the basis for content-addressed chunks in the storage system.

use bytes::{Bytes, BytesMut};
use std::marker::PhantomData;
use std::sync::OnceLock;

use crate::SwarmAddress;
use crate::bmt::{DEFAULT_BODY_SIZE, Hasher};
use crate::chunk::error::{self, ChunkError};
use crate::error::{PrimitivesError, Result};

const SPAN_SIZE: usize = std::mem::size_of::<u64>();

/// A BMT body with configurable maximum size.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BmtBody<const BODY_SIZE: usize = DEFAULT_BODY_SIZE> {
    span: u64,
    data: Bytes,
    cached_hash: OnceLock<SwarmAddress>,
}

impl<const BODY_SIZE: usize> BmtBody<BODY_SIZE> {
    fn new_unchecked(span: u64, data: Bytes) -> Self {
        Self {
            span,
            data,
            cached_hash: OnceLock::new(),
        }
    }

    /// Create a new builder for BMTBody (crate-internal)
    pub(crate) fn builder() -> BmtBodyBuilder<BODY_SIZE, Initial> {
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

    fn calculate_hash(&self) -> SwarmAddress {
        let mut hasher: Hasher<BODY_SIZE> = Hasher::new();
        hasher.set_span(self.span);
        hasher.update(self.data.as_ref());
        hasher.sum().into()
    }
}

fn validate_data<const BODY_SIZE: usize>(data: impl Into<Bytes>) -> error::Result<Bytes> {
    let data = data.into();
    if data.len() > BODY_SIZE {
        return Err(ChunkError::invalid_size(
            "data exceeds maximum chunk size",
            BODY_SIZE,
            data.len(),
        ));
    }
    Ok(data)
}

impl<const BODY_SIZE: usize> From<BmtBody<BODY_SIZE>> for Bytes {
    fn from(body: BmtBody<BODY_SIZE>) -> Self {
        let mut bytes = BytesMut::with_capacity(body.size());
        bytes.extend(&body.span.to_le_bytes());
        bytes.extend(body.data());
        bytes.freeze()
    }
}

impl<const BODY_SIZE: usize> TryFrom<Bytes> for BmtBody<BODY_SIZE> {
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

        let span_bytes = buf.split_to(SPAN_SIZE);
        let span = u64::from_le_bytes(span_bytes.as_ref().try_into().unwrap());
        let data = buf;

        BmtBody::builder().with_span(span).with_data(data)?.build()
    }
}

impl<const BODY_SIZE: usize> TryFrom<&[u8]> for BmtBody<BODY_SIZE> {
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
pub(crate) struct BmtBodyBuilder<const BODY_SIZE: usize, S: BuilderState = Initial> {
    span: Option<u64>,
    data: Option<Bytes>,
    _state: PhantomData<S>,
}

impl<const BODY_SIZE: usize> Default for BmtBodyBuilder<BODY_SIZE, Initial> {
    fn default() -> Self {
        Self {
            span: None,
            data: None,
            _state: PhantomData,
        }
    }
}

impl<const BODY_SIZE: usize> BmtBodyBuilder<BODY_SIZE, Initial> {
    pub(crate) fn with_span(mut self, span: u64) -> BmtBodyBuilder<BODY_SIZE, WithSpan> {
        self.span = Some(span);
        BmtBodyBuilder {
            span: self.span,
            data: self.data,
            _state: PhantomData,
        }
    }

    pub(crate) fn auto_from_data(
        mut self,
        data: impl Into<Bytes>,
    ) -> Result<BmtBodyBuilder<BODY_SIZE, ReadyToBuild>> {
        let data = validate_data::<BODY_SIZE>(data)?;
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

impl<const BODY_SIZE: usize> BmtBodyBuilder<BODY_SIZE, WithSpan> {
    pub(crate) fn with_data(
        mut self,
        data: impl Into<Bytes>,
    ) -> Result<BmtBodyBuilder<BODY_SIZE, ReadyToBuild>> {
        let data = validate_data::<BODY_SIZE>(data)?;
        let data_len = data.len();
        self.data = Some(data);

        let span = self.span.unwrap();
        if span <= BODY_SIZE as u64 && data_len != span as usize {
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

impl<const BODY_SIZE: usize> BmtBodyBuilder<BODY_SIZE, ReadyToBuild> {
    pub(crate) fn build(self) -> Result<BmtBody<BODY_SIZE>> {
        Ok(BmtBody::new_unchecked(
            self.span.unwrap(),
            self.data.unwrap(),
        ))
    }
}

#[cfg(any(test, feature = "arbitrary"))]
impl<'a, const BODY_SIZE: usize> arbitrary::Arbitrary<'a> for BmtBody<BODY_SIZE> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let span = u64::arbitrary(u)?;
        let data_len: usize = u.int_in_range(0..=BODY_SIZE)?;
        let mut buf = vec![0; data_len];
        u.fill_buffer(&mut buf)?;

        Ok(BmtBodyBuilder::<BODY_SIZE, _>::default()
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

    type DefaultBmtBody = BmtBody<DEFAULT_BODY_SIZE>;

    fn bmt_body_strategy() -> impl Strategy<Value = DefaultBmtBody> {
        arb::<DefaultBmtBody>()
    }

    fn create_bmt_body(span: u64, data: Vec<u8>) -> Result<DefaultBmtBody> {
        DefaultBmtBody::builder()
            .with_span(span)
            .with_data(data)?
            .build()
    }

    proptest! {
        #[test]
        fn test_bmt_body_properties(body in bmt_body_strategy()) {
            prop_assert!(body.span() <= u64::MAX);
            prop_assert!(body.data().len() <= DEFAULT_BODY_SIZE);
            prop_assert_eq!(body.size(), SPAN_SIZE + body.data().len());

            let bytes: Bytes = body.clone().into();
            let decoded = DefaultBmtBody::try_from(bytes).unwrap();
            prop_assert_eq!(body, decoded);
        }

        #[test]
        fn test_bmt_body_size_validation(span in 0..=u64::MAX, data_len in DEFAULT_BODY_SIZE + 1..=DEFAULT_BODY_SIZE * 2) {
            let data = vec![0; data_len];
            let result = create_bmt_body(span, data);
            assert!(matches!(result, Err(PrimitivesError::Chunk(ChunkError::InvalidSize { .. }))));
        }

        #[test]
        fn test_bmt_body_builder_properties(
            span in 0..=u64::MAX,
            data_len in 0..=DEFAULT_BODY_SIZE,
        ) {
            let data = vec![0; data_len];
            let builder = BmtBodyBuilder::<DEFAULT_BODY_SIZE, _>::default()
                .with_span(span)
                .with_data(data.clone())?;

            let body = builder.build().unwrap();
            assert_eq!(body.span(), span);
            assert_eq!(body.data(), &data);
            prop_assert_eq!(body.size(), SPAN_SIZE + data.len());
        }

        #[test]
        fn test_span_data_length_mismatch(
            span in 0..=DEFAULT_BODY_SIZE as u64,
            data_len in 0..=DEFAULT_BODY_SIZE,
        ) {
            let data = vec![0; data_len];
            let result = DefaultBmtBody::builder()
                .with_span(span)
                .with_data(data.clone());

            if span <= DEFAULT_BODY_SIZE as u64 && data.len() != span as usize {
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
        input.extend_from_slice(&5u64.to_le_bytes());
        input.extend_from_slice(&[1, 2, 3, 4, 5]);

        let body = DefaultBmtBody::try_from(Bytes::from(input)).unwrap();
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
        let result = DefaultBmtBody::builder()
            .with_span(42)
            .with_data(vec![0; DEFAULT_BODY_SIZE + 1]);

        assert!(matches!(
            result,
            Err(PrimitivesError::Chunk(ChunkError::InvalidSize { .. }))
        ));

        let result = DefaultBmtBody::try_from(vec![0; DEFAULT_BODY_SIZE + 9].as_slice());
        assert!(matches!(
            result,
            Err(PrimitivesError::Chunk(ChunkError::InvalidSize { .. }))
        ));
    }
}
