//! Postage batch types.

use alloy_primitives::{Address, B256};
use derive_more::{AsRef, Display, From, Into};
use nectar_primitives::{
    ChunkAddress, SwarmSpec,
    wire::{Cursor, FromCursor, ToWriter, Underrun, Writer},
};

use crate::{StampError, StampIndex, calculate_bucket};

/// A 32-byte batch identifier.
///
/// Nominal wrapper over [`B256`]: other 32-byte values (chunk addresses,
/// hashes) do not type-check as batch ids. The `From`/`Into` conversions
/// cover the contracts `bytes32` boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into, AsRef)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[display("{_0}")]
#[from(B256, [u8; 32])]
#[into(B256, [u8; 32])]
#[as_ref([u8])]
#[repr(transparent)]
pub struct BatchId(B256);

impl BatchId {
    /// Width in bytes of an id.
    pub const SIZE: usize = size_of::<B256>();

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

    /// Copy an id out of a 32-byte slice.
    ///
    /// # Panics
    ///
    /// Panics when `slice` is not exactly 32 bytes.
    #[inline]
    pub fn from_slice(slice: &[u8]) -> Self {
        Self(B256::from_slice(slice))
    }
}

/// Reads the id as its raw 32 bytes.
impl FromCursor for BatchId {
    type Error = Underrun;

    fn take_from(cur: &mut Cursor<'_>) -> Result<Self, Underrun> {
        cur.take::<[u8; Self::SIZE]>().map(Self::new)
    }
}

/// Writes the raw 32 bytes, the mirror of the `FromCursor` impl above.
impl ToWriter for BatchId {
    fn put_into(&self, w: &mut Writer<'_>) {
        w.put(&<[u8; Self::SIZE]>::from(*self));
    }
}

#[cfg(any(test, feature = "arbitrary"))]
impl<'a> arbitrary::Arbitrary<'a> for BatchId {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self::new(u.arbitrary()?))
    }
}

/// The number of leading chunk-address bits that select a collision bucket.
///
/// Bounded to `1..=32` at construction: bucket selection shifts a `u32` right by
/// `32 - depth`, so a depth outside that range names no bucket. Holding the
/// bound in the type keeps the shift total wherever a depth reaches it.
///
/// The bound is what the shift can represent, not what a network accepts. A
/// spec sets a minimum operative depth, so a batch is only well-formed when its
/// bucket depth reaches that minimum and its batch depth leaves room above it;
/// [`validate_geometry`] applies both.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, Into)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "u8", into = "u8"))]
#[display("{_0}")]
#[into(u8)]
#[repr(transparent)]
pub struct BucketDepth(u8);

impl BucketDepth {
    /// The smallest representable depth, two collision buckets. A network sets
    /// a far higher minimum; this is only the point below which the bucket
    /// shift stops naming anything.
    pub const MIN: Self = Self(1);

    /// The largest representable depth, the bit width of the bucket key.
    pub const MAX: Self = Self(32);

    /// Validates a raw depth against the `1..=32` bound.
    ///
    /// # Errors
    ///
    /// [`StampError::InvalidBucketDepth`] when `depth` is zero or above 32.
    #[inline]
    pub const fn new(depth: u8) -> Result<Self, StampError> {
        if depth < Self::MIN.0 || depth > Self::MAX.0 {
            return Err(StampError::InvalidBucketDepth {
                bucket_depth: depth,
            });
        }
        Ok(Self(depth))
    }

    /// Returns the depth as a bit count.
    #[inline]
    pub const fn get(self) -> u8 {
        self.0
    }

    /// Returns the number of collision buckets, `2^depth`.
    ///
    /// Widened to `u64` because depth 32 overflows a `u32` count by one.
    #[inline]
    pub const fn bucket_count(self) -> u64 {
        1u64 << self.0
    }

    /// Returns whether a bucket index is one this depth addresses.
    #[inline]
    pub const fn contains_bucket(self, bucket: u32) -> bool {
        // At the maximum depth every `u32` is a bucket, and the count no longer
        // fits the `u32` shift used below.
        self.0 == Self::MAX.0 || bucket < (1u32 << self.0)
    }
}

impl TryFrom<u8> for BucketDepth {
    type Error = StampError;

    #[inline]
    fn try_from(depth: u8) -> Result<Self, StampError> {
        Self::new(depth)
    }
}

#[cfg(any(test, feature = "arbitrary"))]
impl<'a> arbitrary::Arbitrary<'a> for BucketDepth {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        Self::new(u.int_in_range(Self::MIN.0..=Self::MAX.0)?)
            .map_err(|_| arbitrary::Error::IncorrectFormat)
    }
}

/// Validates a batch geometry against the network `spec`.
///
/// Covers the two protocol bounds [`BucketDepth::new`] cannot: the bucket depth
/// reaches [`SwarmSpec::min_bucket_depth`], and the batch depth leaves room
/// above it. Takes the spec instead of binding one into every constructor, so
/// building a depth stays free where no spec is at hand and the protocol check
/// lands at the edge that has one.
///
/// # Errors
///
/// [`StampError::BucketDepthBelowMinimum`] when `bucket_depth` is under the
/// spec minimum, [`StampError::DepthBelowBucketDepth`] when `depth` is under
/// `bucket_depth`.
pub fn validate_geometry<S>(
    spec: &S,
    depth: u8,
    bucket_depth: BucketDepth,
) -> Result<(), StampError>
where
    S: SwarmSpec + ?Sized,
{
    let minimum = spec.min_bucket_depth();
    if bucket_depth.get() < minimum {
        return Err(StampError::BucketDepthBelowMinimum {
            bucket_depth: bucket_depth.get(),
            minimum,
        });
    }
    if depth < bucket_depth.get() {
        return Err(StampError::DepthBelowBucketDepth {
            depth,
            bucket_depth: bucket_depth.get(),
        });
    }
    Ok(())
}

/// Parameters for creating a new batch.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BatchParams {
    /// The owner's Ethereum address.
    pub owner: Address,
    /// The depth of the batch (total capacity = 2^depth chunks).
    pub depth: u8,
    /// The bucket depth for collision bucket uniformity.
    pub bucket_depth: BucketDepth,
    /// Whether the batch is immutable.
    ///
    /// Immutable batches cannot be diluted (depth increased) and chunks cannot
    /// be overwritten. Mutable batches allow writing new chunks to the same
    /// bucket index with a later timestamp, replacing the previous chunk.
    pub immutable: bool,
    /// Initial amount to fund the batch.
    pub amount: u128,
}

impl BatchParams {
    /// Creates new batch parameters.
    pub const fn new(owner: Address, depth: u8, bucket_depth: BucketDepth, amount: u128) -> Self {
        Self {
            owner,
            depth,
            bucket_depth,
            immutable: false,
            amount,
        }
    }

    /// Sets the immutable flag.
    #[must_use]
    pub const fn immutable(mut self, immutable: bool) -> Self {
        self.immutable = immutable;
        self
    }

    /// Validates the declared geometry against `spec`, per [`validate_geometry`].
    ///
    /// # Errors
    ///
    /// As [`validate_geometry`].
    #[inline]
    pub fn validate_geometry<S>(&self, spec: &S) -> Result<(), StampError>
    where
        S: SwarmSpec + ?Sized,
    {
        validate_geometry(spec, self.depth, self.bucket_depth)
    }
}

/// A postage batch represents a prepaid storage allocation in the Swarm network.
///
/// Batches are created by sending BZZ tokens to the postage stamp contract.
/// Each batch has a depth that determines the maximum number of chunks it can stamp,
/// and a bucket depth that controls the uniformity of chunk distribution.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Batch {
    /// The unique identifier for this batch.
    id: BatchId,
    /// The normalized balance of the batch (value per chunk).
    value: u128,
    /// The block number when this batch was created.
    start: u64,
    /// The Ethereum address of the batch owner.
    owner: Address,
    /// The depth of the batch, determining total capacity (2^depth chunks).
    depth: u8,
    /// The bucket depth for collision bucket uniformity.
    bucket_depth: BucketDepth,
    /// Whether the batch is immutable.
    ///
    /// Immutable batches cannot be diluted (depth increased) and chunks cannot
    /// be overwritten. Mutable batches allow writing new chunks to the same
    /// bucket index with a later timestamp, replacing the previous chunk.
    immutable: bool,
}

impl Batch {
    /// Creates a new batch with the given parameters.
    #[inline]
    pub const fn new(
        id: BatchId,
        value: u128,
        start: u64,
        owner: Address,
        depth: u8,
        bucket_depth: BucketDepth,
        immutable: bool,
    ) -> Self {
        Self {
            id,
            value,
            start,
            owner,
            depth,
            bucket_depth,
            immutable,
        }
    }

    /// Returns the batch ID.
    #[inline]
    pub const fn id(&self) -> BatchId {
        self.id
    }

    /// Returns the normalized value (balance per chunk).
    #[inline]
    pub const fn value(&self) -> u128 {
        self.value
    }

    /// Returns the block number when this batch was created.
    #[inline]
    pub const fn start(&self) -> u64 {
        self.start
    }

    /// Returns the owner's Ethereum address.
    #[inline]
    pub const fn owner(&self) -> Address {
        self.owner
    }

    /// Returns the batch depth.
    ///
    /// The total capacity is 2^depth chunks.
    #[inline]
    pub const fn depth(&self) -> u8 {
        self.depth
    }

    /// Returns the bucket depth.
    ///
    /// This controls the uniformity of chunk distribution across collision buckets.
    #[inline]
    pub const fn bucket_depth(&self) -> BucketDepth {
        self.bucket_depth
    }

    /// Returns whether this batch is immutable.
    ///
    /// Immutable batches cannot be diluted (depth increased) and chunks cannot
    /// be overwritten. Mutable batches allow writing new chunks to the same
    /// bucket index with a later timestamp, replacing the previous chunk.
    #[inline]
    pub const fn immutable(&self) -> bool {
        self.immutable
    }

    /// Returns the maximum number of chunks per bucket, `2^(depth - bucket_depth)`.
    ///
    /// Yields a single slot for a batch shallower than its bucket depth, and
    /// saturates at [`u32::MAX`] for a slot count wider than a `u32`.
    #[inline]
    pub const fn bucket_upper_bound(&self) -> u32 {
        let slots = self.depth.saturating_sub(self.bucket_depth.get());
        // `BucketDepth::MAX` is the bit width of the count, so a wider slot
        // count has no `u32` to land in.
        if slots >= BucketDepth::MAX.get() {
            return u32::MAX;
        }
        1u32 << slots
    }

    /// Returns the number of collision buckets, `2^bucket_depth`.
    #[inline]
    pub const fn bucket_count(&self) -> u64 {
        self.bucket_depth.bucket_count()
    }

    /// Updates the batch value (for top-up operations).
    #[inline]
    pub const fn set_value(&mut self, value: u128) {
        self.value = value;
    }

    /// Updates the batch depth (for dilution operations).
    #[inline]
    pub const fn set_depth(&mut self, depth: u8) {
        self.depth = depth;
    }

    /// Checks if the batch has expired given the current chain state.
    #[inline]
    pub const fn is_expired(&self, total_amount: u128) -> bool {
        self.value <= total_amount
    }

    /// Checks if the batch is usable (has enough confirmations).
    #[inline]
    pub const fn is_usable(&self, current_block: u64, threshold: u64) -> bool {
        current_block >= self.start.saturating_add(threshold)
    }

    // =========================================================================
    // Validation methods
    // =========================================================================

    /// Validates this batch's geometry against `spec`, per [`validate_geometry`].
    ///
    /// Dilution raises the batch depth, so a batch stays valid across a
    /// [`set_depth`](Self::set_depth) only while the new depth clears the
    /// bucket depth.
    ///
    /// # Errors
    ///
    /// As [`validate_geometry`].
    #[inline]
    pub fn validate_geometry<S>(&self, spec: &S) -> Result<(), StampError>
    where
        S: SwarmSpec + ?Sized,
    {
        validate_geometry(spec, self.depth, self.bucket_depth)
    }

    /// Validates that an index is within the valid range for this batch.
    ///
    /// Checks that:
    /// - The bucket is within the valid range (< bucket_count)
    /// - The position within the bucket is within capacity (< bucket_upper_bound)
    ///
    /// # Returns
    ///
    /// `Ok(())` if the index is valid, or `Err(StampError::InvalidIndex)` otherwise.
    pub const fn validate_index(&self, index: &StampIndex) -> Result<(), StampError> {
        // Check bucket is within range
        if !self.bucket_depth.contains_bucket(index.bucket()) {
            return Err(StampError::InvalidIndex);
        }

        // Check index is within bucket capacity
        if index.index() >= self.bucket_upper_bound() {
            return Err(StampError::InvalidIndex);
        }

        Ok(())
    }

    /// Calculates which bucket a chunk address belongs to.
    ///
    /// The bucket is determined by taking the first `bucket_depth` bits of the
    /// chunk address, interpreted as a big-endian unsigned integer.
    #[inline]
    pub fn bucket_for_address(&self, address: &ChunkAddress) -> u32 {
        calculate_bucket(address, self.bucket_depth.get())
    }

    /// Checks if a chunk address matches the expected bucket for a stamp index.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the bucket matches, or `Err(StampError::BucketMismatch)` otherwise.
    pub fn validate_bucket(
        &self,
        index: &StampIndex,
        address: &ChunkAddress,
    ) -> Result<(), StampError> {
        let expected_bucket = self.bucket_for_address(address);
        if index.bucket() != expected_bucket {
            return Err(StampError::BucketMismatch);
        }
        Ok(())
    }
}

// Arbitrary implementations for property-based testing

#[cfg(any(test, feature = "arbitrary"))]
impl<'a> arbitrary::Arbitrary<'a> for BatchParams {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        // Generate valid depth values (bucket_depth must be <= depth)
        let depth: u8 = u.int_in_range(1..=32)?;
        let bucket_depth = BucketDepth::new(u.int_in_range(1..=depth)?)
            .map_err(|_| arbitrary::Error::IncorrectFormat)?;

        Ok(Self {
            owner: Address::arbitrary(u)?,
            depth,
            bucket_depth,
            immutable: u.arbitrary()?,
            amount: u.arbitrary()?,
        })
    }
}

#[cfg(any(test, feature = "arbitrary"))]
impl<'a> arbitrary::Arbitrary<'a> for Batch {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        // Generate valid depth values (bucket_depth must be <= depth)
        let depth: u8 = u.int_in_range(1..=32)?;
        let bucket_depth = BucketDepth::new(u.int_in_range(1..=depth)?)
            .map_err(|_| arbitrary::Error::IncorrectFormat)?;

        Ok(Self::new(
            BatchId::arbitrary(u)?,
            u.arbitrary()?,
            u.arbitrary()?,
            Address::arbitrary(u)?,
            depth,
            bucket_depth,
            u.arbitrary()?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_id_roundtrips_via_from_impls() {
        let bytes = [7u8; 32];
        let id = BatchId::new(bytes);
        assert_eq!(B256::from(id), B256::new(bytes));
        assert_eq!(BatchId::from(B256::new(bytes)), id);
        assert_eq!(<[u8; 32]>::from(id), bytes);
        assert_eq!(BatchId::from(bytes), id);
    }

    #[test]
    fn bucket_depth_accepts_only_one_to_thirty_two() {
        assert!(matches!(
            BucketDepth::new(0),
            Err(StampError::InvalidBucketDepth { bucket_depth: 0 })
        ));
        assert_eq!(BucketDepth::new(1).unwrap(), BucketDepth::MIN);
        assert_eq!(BucketDepth::new(32).unwrap(), BucketDepth::MAX);
        assert!(matches!(
            BucketDepth::new(33),
            Err(StampError::InvalidBucketDepth { bucket_depth: 33 })
        ));
        assert!(matches!(
            BucketDepth::try_from(u8::MAX),
            Err(StampError::InvalidBucketDepth {
                bucket_depth: u8::MAX
            })
        ));
    }

    #[test]
    fn geometry_takes_the_bucket_depth_minimum_from_the_spec() {
        let spec = nectar_primitives::MAINNET;
        assert_eq!(spec.min_bucket_depth(), 16);

        let depth = |d: u8| BucketDepth::new(d).unwrap();

        // Below the minimum, at it, and deeper than it.
        assert!(matches!(
            validate_geometry(&spec, 24, depth(15)),
            Err(StampError::BucketDepthBelowMinimum {
                bucket_depth: 15,
                minimum: 16
            })
        ));
        assert!(validate_geometry(&spec, 24, depth(16)).is_ok());
        assert!(validate_geometry(&spec, 24, depth(20)).is_ok());
        // A batch exactly as deep as its buckets holds one slot each.
        assert!(validate_geometry(&spec, 16, depth(16)).is_ok());

        assert!(matches!(
            validate_geometry(&spec, 15, depth(16)),
            Err(StampError::DepthBelowBucketDepth {
                depth: 15,
                bucket_depth: 16
            })
        ));
    }

    #[test]
    fn geometry_follows_a_deployment_that_raises_the_minimum() {
        struct Deep;
        impl SwarmSpec for Deep {
            fn network_id(&self) -> nectar_primitives::NetworkId {
                nectar_primitives::NetworkId::TESTNET
            }
            fn min_bucket_depth(&self) -> u8 {
                20
            }
        }

        let bucket_depth = BucketDepth::new(16).unwrap();
        assert!(validate_geometry(&nectar_primitives::MAINNET, 24, bucket_depth).is_ok());
        assert!(matches!(
            validate_geometry(&Deep, 24, bucket_depth),
            Err(StampError::BucketDepthBelowMinimum {
                bucket_depth: 16,
                minimum: 20
            })
        ));
    }

    #[test]
    fn geometry_validates_through_batch_and_params() {
        let spec = nectar_primitives::MAINNET;
        let bucket_depth = BucketDepth::new(16).unwrap();

        let params = BatchParams::new(Address::ZERO, 20, bucket_depth, 1000);
        assert!(params.validate_geometry(&spec).is_ok());

        let batch = Batch::new(BatchId::ZERO, 0, 0, Address::ZERO, 20, bucket_depth, false);
        assert!(batch.validate_geometry(&spec).is_ok());

        let shallow = Batch::new(BatchId::ZERO, 0, 0, Address::ZERO, 8, bucket_depth, false);
        assert!(matches!(
            shallow.validate_geometry(&spec),
            Err(StampError::DepthBelowBucketDepth {
                depth: 8,
                bucket_depth: 16
            })
        ));
    }

    #[test]
    fn bucket_geometry_holds_at_the_bounds() {
        let min = Batch::new(
            BatchId::ZERO,
            0,
            0,
            Address::ZERO,
            1,
            BucketDepth::MIN,
            false,
        );
        assert_eq!(min.bucket_count(), 2);
        assert_eq!(min.bucket_for_address(&ChunkAddress::new([0xFF; 32])), 1);

        let max = Batch::new(
            BatchId::ZERO,
            0,
            0,
            Address::ZERO,
            u8::MAX,
            BucketDepth::MAX,
            false,
        );
        assert_eq!(max.bucket_count(), 1 << 32);
        assert_eq!(
            max.bucket_for_address(&ChunkAddress::new([0xFF; 32])),
            u32::MAX
        );
        // Every `u32` is a bucket at the maximum depth, and the per-bucket slot
        // count saturates rather than overflowing its shift.
        assert!(max.validate_index(&StampIndex::new(u32::MAX, 0)).is_ok());
        assert_eq!(max.bucket_upper_bound(), u32::MAX);
    }

    #[test]
    fn bucket_upper_bound_holds_for_a_batch_shallower_than_its_buckets() {
        let batch = Batch::new(
            BatchId::ZERO,
            0,
            0,
            Address::ZERO,
            8,
            BucketDepth::MAX,
            false,
        );
        assert_eq!(batch.bucket_upper_bound(), 1);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn serde_rejects_an_out_of_range_bucket_depth() {
        use serde::{Deserialize, de::IntoDeserializer, de::value::Error};

        let decode =
            |raw: u8| BucketDepth::deserialize(IntoDeserializer::<Error>::into_deserializer(raw));

        assert_eq!(decode(16).unwrap(), BucketDepth::new(16).unwrap());
        assert!(decode(0).is_err());
        assert!(decode(33).is_err());
    }

    #[test]
    fn test_batch_creation() {
        let id = BatchId::ZERO;
        let batch = Batch::new(
            id,
            1000,
            100,
            Address::ZERO,
            18,
            BucketDepth::new(16).unwrap(),
            false,
        );

        assert_eq!(batch.id(), id);
        assert_eq!(batch.value(), 1000);
        assert_eq!(batch.start(), 100);
        assert_eq!(batch.owner(), Address::ZERO);
        assert_eq!(batch.depth(), 18);
        assert_eq!(batch.bucket_depth().get(), 16);
        assert!(!batch.immutable());
    }

    #[test]
    fn test_bucket_calculations() {
        let batch = Batch::new(
            BatchId::ZERO,
            0,
            0,
            Address::ZERO,
            18,
            BucketDepth::new(16).unwrap(),
            false,
        );

        // 2^(18-16) = 2^2 = 4 chunks per bucket
        assert_eq!(batch.bucket_upper_bound(), 4);
        // 2^16 = 65536 buckets
        assert_eq!(batch.bucket_count(), 65536);
    }

    #[test]
    fn test_batch_expiry() {
        let batch = Batch::new(
            BatchId::ZERO,
            1000,
            0,
            Address::ZERO,
            18,
            BucketDepth::new(16).unwrap(),
            false,
        );

        assert!(!batch.is_expired(999));
        assert!(batch.is_expired(1000));
        assert!(batch.is_expired(1001));
    }

    #[test]
    fn test_batch_usability() {
        let batch = Batch::new(
            BatchId::ZERO,
            1000,
            100,
            Address::ZERO,
            18,
            BucketDepth::new(16).unwrap(),
            false,
        );

        assert!(!batch.is_usable(100, 10)); // Same block
        assert!(!batch.is_usable(109, 10)); // Not enough confirmations
        assert!(batch.is_usable(110, 10)); // Exactly threshold
        assert!(batch.is_usable(111, 10)); // Past threshold
    }

    #[test]
    fn test_batch_params_builder() {
        let params = BatchParams::new(Address::ZERO, 20, BucketDepth::new(16).unwrap(), 1000)
            .immutable(true);

        assert_eq!(params.owner, Address::ZERO);
        assert_eq!(params.depth, 20);
        assert_eq!(params.bucket_depth.get(), 16);
        assert_eq!(params.amount, 1000);
        assert!(params.immutable);
    }
}
