//! Postage batch types.

use core::{fmt, marker::PhantomData};

use alloy_primitives::{Address, B256};
use derive_more::{AsRef, Display, From, Into};
use nectar_primitives::{
    ChunkAddress, Mainnet, SwarmSpec,
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

/// The number of leading chunk-address bits that select a collision bucket, as
/// the network `S` accepts it.
///
/// Two bounds hold at construction: [`SwarmSpec::MIN_BUCKET_DEPTH`], the floor
/// the PostageStamp contract publishes as `minimumBucketDepth()`, and
/// [`MAX`](Self::MAX), the width of the bucket key. Bucket selection shifts a
/// `u32` right by `32 - depth`; holding both bounds in the type keeps that
/// shift total wherever a depth reaches it.
///
/// The floor is a compile-time property: a `BucketDepth<Mainnet>` below 16 does
/// not exist, and one network's depth does not type-check where another's is
/// wanted. Every constructor funnels through [`new`](Self::new), including the
/// serde and `Arbitrary` paths.
///
/// A depth carries its network, so this does not compile:
///
/// ```compile_fail
/// use nectar_postage::{Batch, BatchId, BucketDepth};
/// use nectar_primitives::Testnet;
///
/// let bucket_depth = BucketDepth::<Testnet>::new(16).unwrap();
/// // `Batch` without a spec argument is a mainnet batch.
/// let batch: Batch = Batch::new(
///     BatchId::ZERO, 0, 0, Default::default(), 20, bucket_depth, false,
/// );
/// ```
#[repr(transparent)]
pub struct BucketDepth<S: SwarmSpec = Mainnet> {
    depth: u8,
    // `fn() -> S` rather than `S`: the tag carries no data, so the depth (and
    // everything holding one) is `Send`/`Sync` whatever the spec marker is.
    spec: PhantomData<fn() -> S>,
}

impl<S: SwarmSpec> BucketDepth<S> {
    /// Largest representable depth, the bit width of the bucket key.
    pub const MAX: u8 = 32;

    /// Validates a raw depth against the spec floor and [`MAX`](Self::MAX).
    ///
    /// # Errors
    ///
    /// [`StampError::BucketDepthBelowMinimum`] when `depth` is under
    /// [`SwarmSpec::MIN_BUCKET_DEPTH`], [`StampError::InvalidBucketDepth`] when
    /// it is above [`MAX`](Self::MAX).
    #[inline]
    pub const fn new(depth: u8) -> Result<Self, StampError> {
        if depth < S::MIN_BUCKET_DEPTH.get() {
            return Err(StampError::BucketDepthBelowMinimum {
                bucket_depth: depth,
                minimum: S::MIN_BUCKET_DEPTH.get(),
            });
        }
        if depth > Self::MAX {
            return Err(StampError::InvalidBucketDepth {
                bucket_depth: depth,
            });
        }
        Ok(Self {
            depth,
            spec: PhantomData,
        })
    }

    /// Returns the depth as a bit count.
    #[inline]
    pub const fn get(self) -> u8 {
        self.depth
    }

    /// Returns the number of collision buckets, `2^depth`.
    ///
    /// Widened to `u64` because depth 32 overflows a `u32` count by one.
    #[inline]
    pub const fn bucket_count(self) -> u64 {
        1u64 << self.depth
    }

    /// Returns whether a bucket index is one this depth addresses.
    #[inline]
    pub const fn contains_bucket(self, bucket: u32) -> bool {
        // At the maximum depth every `u32` is a bucket, and the count no longer
        // fits the `u32` shift used below.
        self.depth == Self::MAX || bucket < (1u32 << self.depth)
    }
}

// The spec is a type-level tag, so the manual impls below carry no bound on
// `S` beyond `SwarmSpec`; deriving would demand `S: Clone`, `S: Eq` and the
// rest of a marker type that holds no data.

impl<S: SwarmSpec> Clone for BucketDepth<S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<S: SwarmSpec> Copy for BucketDepth<S> {}

impl<S: SwarmSpec> fmt::Debug for BucketDepth<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("BucketDepth").field(&self.depth).finish()
    }
}

impl<S: SwarmSpec> fmt::Display for BucketDepth<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.depth, f)
    }
}

impl<S: SwarmSpec> PartialEq for BucketDepth<S> {
    fn eq(&self, other: &Self) -> bool {
        self.depth == other.depth
    }
}

impl<S: SwarmSpec> Eq for BucketDepth<S> {}

impl<S: SwarmSpec> PartialOrd for BucketDepth<S> {
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<S: SwarmSpec> Ord for BucketDepth<S> {
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        self.depth.cmp(&other.depth)
    }
}

impl<S: SwarmSpec> core::hash::Hash for BucketDepth<S> {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        self.depth.hash(state);
    }
}

impl<S: SwarmSpec> From<BucketDepth<S>> for u8 {
    #[inline]
    fn from(depth: BucketDepth<S>) -> Self {
        depth.depth
    }
}

impl<S: SwarmSpec> TryFrom<u8> for BucketDepth<S> {
    type Error = StampError;

    #[inline]
    fn try_from(depth: u8) -> Result<Self, StampError> {
        Self::new(depth)
    }
}

/// Serializes as the bare depth byte.
#[cfg(feature = "serde")]
impl<S: SwarmSpec> serde::Serialize for BucketDepth<S> {
    fn serialize<Z: serde::Serializer>(&self, serializer: Z) -> Result<Z::Ok, Z::Error> {
        serializer.serialize_u8(self.depth)
    }
}

/// Deserializes through [`BucketDepth::new`], so a stored depth below the spec
/// floor is refused rather than reconstructed.
#[cfg(feature = "serde")]
impl<'de, S: SwarmSpec> serde::Deserialize<'de> for BucketDepth<S> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let depth = u8::deserialize(deserializer)?;
        Self::new(depth).map_err(serde::de::Error::custom)
    }
}

/// Draws from the spec's accepted window, so every generated depth is one the
/// network accepts.
#[cfg(any(test, feature = "arbitrary"))]
impl<'a, S: SwarmSpec> arbitrary::Arbitrary<'a> for BucketDepth<S> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let low = S::MIN_BUCKET_DEPTH.get();
        if low > Self::MAX {
            // A spec whose floor is past the bucket-key width admits no depth.
            return Err(arbitrary::Error::IncorrectFormat);
        }
        Self::new(u.int_in_range(low..=Self::MAX)?).map_err(|_| arbitrary::Error::IncorrectFormat)
    }
}

/// Parameters for creating a new batch on the network `S`.
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(bound(serialize = "", deserialize = "")))]
pub struct BatchParams<S: SwarmSpec = Mainnet> {
    /// The owner's Ethereum address.
    pub owner: Address,
    /// The depth of the batch (total capacity = 2^depth chunks).
    pub depth: u8,
    /// The bucket depth for collision bucket uniformity.
    pub bucket_depth: BucketDepth<S>,
    /// Whether the batch is immutable.
    ///
    /// Immutable batches cannot be diluted (depth increased) and chunks cannot
    /// be overwritten. Mutable batches allow writing new chunks to the same
    /// bucket index with a later timestamp, replacing the previous chunk.
    pub immutable: bool,
    /// Initial amount to fund the batch.
    pub amount: u128,
}

// As for [`BucketDepth`] above: the spec is a type-level tag, so `Clone` and
// equality carry no bound on `S` beyond `SwarmSpec`. Only `Debug` is derived,
// following the marker's own.

impl<S: SwarmSpec> Clone for BatchParams<S> {
    fn clone(&self) -> Self {
        Self {
            owner: self.owner,
            depth: self.depth,
            bucket_depth: self.bucket_depth,
            immutable: self.immutable,
            amount: self.amount,
        }
    }
}

impl<S: SwarmSpec> PartialEq for BatchParams<S> {
    fn eq(&self, other: &Self) -> bool {
        self.owner == other.owner
            && self.depth == other.depth
            && self.bucket_depth == other.bucket_depth
            && self.immutable == other.immutable
            && self.amount == other.amount
    }
}

impl<S: SwarmSpec> Eq for BatchParams<S> {}

impl<S: SwarmSpec> BatchParams<S> {
    /// Creates new batch parameters.
    pub const fn new(
        owner: Address,
        depth: u8,
        bucket_depth: BucketDepth<S>,
        amount: u128,
    ) -> Self {
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

    /// Validates that the batch depth leaves room above the bucket depth.
    ///
    /// The bucket depth clears the network floor by construction; this is the
    /// one geometry bound left to check, because `depth` is a plain `u8` the
    /// type system cannot relate to it.
    ///
    /// # Errors
    ///
    /// [`StampError::DepthBelowBucketDepth`] when `depth` is under the bucket
    /// depth.
    #[inline]
    pub const fn validate_depth(&self) -> Result<(), StampError> {
        validate_depth(self.depth, self.bucket_depth)
    }
}

/// Validates that a batch depth leaves room above its bucket depth.
const fn validate_depth<S: SwarmSpec>(
    depth: u8,
    bucket_depth: BucketDepth<S>,
) -> Result<(), StampError> {
    if depth < bucket_depth.get() {
        return Err(StampError::DepthBelowBucketDepth {
            depth,
            bucket_depth: bucket_depth.get(),
        });
    }
    Ok(())
}

/// A postage batch represents a prepaid storage allocation in the Swarm network.
///
/// Batches are created by sending BZZ tokens to the postage stamp contract.
/// Each batch has a depth that determines the maximum number of chunks it can stamp,
/// and a bucket depth that controls the uniformity of chunk distribution.
///
/// The network is a type parameter, defaulting to [`Mainnet`], and reaches the
/// batch through its [`BucketDepth`].
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(bound(serialize = "", deserialize = "")))]
pub struct Batch<S: SwarmSpec = Mainnet> {
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
    bucket_depth: BucketDepth<S>,
    /// Whether the batch is immutable.
    ///
    /// Immutable batches cannot be diluted (depth increased) and chunks cannot
    /// be overwritten. Mutable batches allow writing new chunks to the same
    /// bucket index with a later timestamp, replacing the previous chunk.
    immutable: bool,
}

impl<S: SwarmSpec> Clone for Batch<S> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            value: self.value,
            start: self.start,
            owner: self.owner,
            depth: self.depth,
            bucket_depth: self.bucket_depth,
            immutable: self.immutable,
        }
    }
}

impl<S: SwarmSpec> PartialEq for Batch<S> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.value == other.value
            && self.start == other.start
            && self.owner == other.owner
            && self.depth == other.depth
            && self.bucket_depth == other.bucket_depth
            && self.immutable == other.immutable
    }
}

impl<S: SwarmSpec> Eq for Batch<S> {}

impl<S: SwarmSpec> Batch<S> {
    /// Creates a new batch with the given parameters.
    #[inline]
    pub const fn new(
        id: BatchId,
        value: u128,
        start: u64,
        owner: Address,
        depth: u8,
        bucket_depth: BucketDepth<S>,
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
    pub const fn bucket_depth(&self) -> BucketDepth<S> {
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
        if slots >= BucketDepth::<S>::MAX {
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

    /// Validates that the batch depth leaves room above the bucket depth.
    ///
    /// The bucket depth clears the network floor by construction; this is the
    /// one geometry bound left to check, because `depth` is a plain `u8` the
    /// type system cannot relate to it. [`set_depth`](Self::set_depth) takes a
    /// bare depth for a dilution, so a batch stays well-formed across one only
    /// while the new depth clears the bucket depth.
    ///
    /// # Errors
    ///
    /// [`StampError::DepthBelowBucketDepth`] when `depth` is under the bucket
    /// depth.
    #[inline]
    pub const fn validate_depth(&self) -> Result<(), StampError> {
        validate_depth(self.depth, self.bucket_depth)
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

/// Draws a bucket depth the network accepts, then a batch depth at or above
/// it, so the generated geometry satisfies both bounds.
#[cfg(any(test, feature = "arbitrary"))]
fn arbitrary_geometry<S: SwarmSpec>(
    u: &mut arbitrary::Unstructured<'_>,
) -> arbitrary::Result<(u8, BucketDepth<S>)> {
    let bucket_depth = <BucketDepth<S> as arbitrary::Arbitrary>::arbitrary(u)?;
    let depth = u.int_in_range(bucket_depth.get()..=BucketDepth::<S>::MAX)?;
    Ok((depth, bucket_depth))
}

#[cfg(any(test, feature = "arbitrary"))]
impl<'a, S: SwarmSpec> arbitrary::Arbitrary<'a> for BatchParams<S> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let (depth, bucket_depth) = arbitrary_geometry::<S>(u)?;

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
impl<'a, S: SwarmSpec> arbitrary::Arbitrary<'a> for Batch<S> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let (depth, bucket_depth) = arbitrary_geometry::<S>(u)?;

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
    use core::num::NonZeroU8;

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

    /// A deployment that raises the bucket-depth floor above mainnet's.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct Deep;

    impl SwarmSpec for Deep {
        const NETWORK_ID: nectar_primitives::NetworkId = nectar_primitives::NetworkId::TESTNET;
        const MIN_BUCKET_DEPTH: NonZeroU8 = NonZeroU8::new(20).unwrap();
    }

    /// A deployment whose floor is the lowest a spec can declare.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct Shallow;

    impl SwarmSpec for Shallow {
        const NETWORK_ID: nectar_primitives::NetworkId = nectar_primitives::NetworkId::TESTNET;
        const MIN_BUCKET_DEPTH: NonZeroU8 = NonZeroU8::new(1).unwrap();
    }

    #[test]
    fn bucket_depth_takes_its_floor_from_the_spec() {
        // Below the mainnet floor, at it, and deeper than it.
        assert!(matches!(
            BucketDepth::<Mainnet>::new(15),
            Err(StampError::BucketDepthBelowMinimum {
                bucket_depth: 15,
                minimum: 16
            })
        ));
        assert_eq!(BucketDepth::<Mainnet>::new(16).unwrap().get(), 16);
        assert_eq!(BucketDepth::<Mainnet>::new(20).unwrap().get(), 20);
        assert_eq!(
            BucketDepth::<Mainnet>::new(BucketDepth::<Mainnet>::MAX)
                .unwrap()
                .get(),
            32
        );
    }

    #[test]
    fn bucket_depth_rejects_an_unrepresentable_depth() {
        assert!(matches!(
            BucketDepth::<Mainnet>::new(33),
            Err(StampError::InvalidBucketDepth { bucket_depth: 33 })
        ));
        assert!(matches!(
            BucketDepth::<Mainnet>::try_from(u8::MAX),
            Err(StampError::InvalidBucketDepth {
                bucket_depth: u8::MAX
            })
        ));
    }

    #[test]
    fn the_lowest_floor_admits_a_one_bit_bucket() {
        assert_eq!(BucketDepth::<Shallow>::new(1).unwrap().get(), 1);
        assert!(matches!(
            BucketDepth::<Shallow>::new(0),
            Err(StampError::BucketDepthBelowMinimum {
                bucket_depth: 0,
                minimum: 1
            })
        ));
    }

    #[test]
    fn a_raised_floor_refuses_a_depth_mainnet_accepts() {
        assert!(BucketDepth::<Mainnet>::new(16).is_ok());
        assert!(matches!(
            BucketDepth::<Deep>::new(16),
            Err(StampError::BucketDepthBelowMinimum {
                bucket_depth: 16,
                minimum: 20
            })
        ));
        assert!(BucketDepth::<Deep>::new(20).is_ok());
    }

    #[test]
    fn depth_below_bucket_depth_is_rejected_through_batch_and_params() {
        let bucket_depth = BucketDepth::<Mainnet>::new(16).unwrap();

        let params = BatchParams::new(Address::ZERO, 20, bucket_depth, 1000);
        assert!(params.validate_depth().is_ok());

        let batch = Batch::new(BatchId::ZERO, 0, 0, Address::ZERO, 20, bucket_depth, false);
        assert!(batch.validate_depth().is_ok());

        // A batch exactly as deep as its buckets holds one slot each.
        let flat = Batch::new(BatchId::ZERO, 0, 0, Address::ZERO, 16, bucket_depth, false);
        assert!(flat.validate_depth().is_ok());

        let shallow = Batch::new(BatchId::ZERO, 0, 0, Address::ZERO, 8, bucket_depth, false);
        assert!(matches!(
            shallow.validate_depth(),
            Err(StampError::DepthBelowBucketDepth {
                depth: 8,
                bucket_depth: 16
            })
        ));
        assert!(matches!(
            BatchParams::new(Address::ZERO, 8, bucket_depth, 1000).validate_depth(),
            Err(StampError::DepthBelowBucketDepth {
                depth: 8,
                bucket_depth: 16
            })
        ));

        // Dilution moves the depth, so the check survives a `set_depth`.
        let mut diluted = batch;
        diluted.set_depth(15);
        assert!(diluted.validate_depth().is_err());
    }

    #[test]
    fn bucket_geometry_holds_at_the_bounds() {
        let min: Batch = Batch::new(
            BatchId::ZERO,
            0,
            0,
            Address::ZERO,
            16,
            BucketDepth::new(16).unwrap(),
            false,
        );
        assert_eq!(min.bucket_count(), 65536);
        assert_eq!(
            min.bucket_for_address(&ChunkAddress::new([0xFF; 32])),
            65535
        );

        let max: Batch = Batch::new(
            BatchId::ZERO,
            0,
            0,
            Address::ZERO,
            u8::MAX,
            BucketDepth::new(BucketDepth::<Mainnet>::MAX).unwrap(),
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
        let batch: Batch = Batch::new(
            BatchId::ZERO,
            0,
            0,
            Address::ZERO,
            8,
            BucketDepth::new(BucketDepth::<Mainnet>::MAX).unwrap(),
            false,
        );
        assert_eq!(batch.bucket_upper_bound(), 1);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn serde_decodes_a_depth_and_enforces_the_floor() {
        use serde::{Deserialize, de::IntoDeserializer, de::value::Error};

        fn decode<S: SwarmSpec>(raw: u8) -> Result<BucketDepth<S>, Error> {
            BucketDepth::deserialize(IntoDeserializer::<Error>::into_deserializer(raw))
        }

        assert_eq!(
            decode::<Mainnet>(16).unwrap(),
            BucketDepth::<Mainnet>::new(16).unwrap()
        );

        // The floor and the representable bound both survive the wire.
        assert!(decode::<Mainnet>(15).is_err());
        assert!(decode::<Mainnet>(33).is_err());
        // And the floor is the spec's, not a constant: 16 decodes on mainnet
        // and is refused on a deployment that asks for 20.
        assert!(decode::<Deep>(16).is_err());
        assert!(decode::<Deep>(20).is_ok());
    }

    #[test]
    fn test_batch_creation() {
        let id = BatchId::ZERO;
        let batch: Batch = Batch::new(
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
        let batch: Batch = Batch::new(
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
        let batch: Batch = Batch::new(
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
        let batch: Batch = Batch::new(
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
        let params: BatchParams =
            BatchParams::new(Address::ZERO, 20, BucketDepth::new(16).unwrap(), 1000)
                .immutable(true);

        assert_eq!(params.owner, Address::ZERO);
        assert_eq!(params.depth, 20);
        assert_eq!(params.bucket_depth.get(), 16);
        assert_eq!(params.amount, 1000);
        assert!(params.immutable);
    }
}
