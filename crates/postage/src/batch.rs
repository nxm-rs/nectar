//! Postage batch types.

use alloy_primitives::{Address, B256};
use derive_more::{AsRef, Display, From, Into};
use nectar_primitives::{
    ChunkAddress, Mainnet, SwarmSpec, WrongLength,
    wire::{Cursor, FromCursor, ToWriter, Underrun, Writer},
};

use crate::{BatchDepth, Bucket, BucketDepth, StampError, StampIndex, calculate_bucket};

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
    #[inline]
    pub fn from_slice(slice: &[u8]) -> Result<Self, WrongLength> {
        Self::try_from(slice)
    }
}

impl TryFrom<&[u8]> for BatchId {
    type Error = WrongLength;

    fn try_from(slice: &[u8]) -> Result<Self, WrongLength> {
        <[u8; Self::SIZE]>::try_from(slice)
            .map(Self::new)
            .map_err(|_| WrongLength {
                expected: Self::SIZE,
                got: slice.len(),
            })
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

/// Parameters for creating a new batch on the network `S`.
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(bound(serialize = "", deserialize = "")))]
#[non_exhaustive]
pub struct BatchParams<S: SwarmSpec = Mainnet> {
    owner: Address,
    depth: u8,
    bucket_depth: BucketDepth<S>,
    immutable: bool,
    amount: u128,
}

// As for [`BucketDepth`]: the spec is a type-level tag, so `Clone` and
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
    pub const fn with_immutable(mut self, immutable: bool) -> Self {
        self.immutable = immutable;
        self
    }

    /// Returns the owner's Ethereum address.
    #[inline]
    pub const fn owner(&self) -> Address {
        self.owner
    }

    /// Returns the batch depth.
    #[inline]
    pub const fn depth(&self) -> u8 {
        self.depth
    }

    /// Returns the bucket depth.
    #[inline]
    pub const fn bucket_depth(&self) -> BucketDepth<S> {
        self.bucket_depth
    }

    /// Governs slot reuse, not dilution: an immutable batch refuses a full
    /// bucket, a mutable one wraps and reissues the slot on a later timestamp.
    #[inline]
    pub const fn immutable(&self) -> bool {
        self.immutable
    }

    /// Returns the initial funding amount.
    #[inline]
    pub const fn amount(&self) -> u128 {
        self.amount
    }

    /// Validates the raw depth against the bucket depth.
    ///
    /// # Errors
    ///
    /// See [`BatchDepth::new`].
    #[inline]
    pub const fn geometry(&self) -> Result<BatchDepth<S>, StampError> {
        BatchDepth::new(self.depth, self.bucket_depth)
    }
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
    /// Governs slot reuse, not dilution: an immutable batch refuses a full
    /// bucket, a mutable one wraps and reissues the slot on a later timestamp.
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

    /// Governs slot reuse, not dilution: an immutable batch refuses a full
    /// bucket, a mutable one wraps and reissues the slot on a later timestamp.
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

    /// Validates the depth decoded from chain against the bucket depth, and
    /// yields the geometry every allocation path is cut against.
    ///
    /// Chain decode keeps [`depth`](Self::depth) a raw byte, so this is the one
    /// place the validated geometry is born. [`set_depth`](Self::set_depth)
    /// takes a bare depth for a dilution, so a diluted batch has to be asked
    /// again.
    ///
    /// # Errors
    ///
    /// See [`BatchDepth::new`].
    #[inline]
    pub const fn geometry(&self) -> Result<BatchDepth<S>, StampError> {
        BatchDepth::new(self.depth, self.bucket_depth)
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

    /// Cuts the collision bucket of a chunk address at this batch's bucket
    /// depth.
    #[inline]
    pub fn bucket_for_address(&self, address: &ChunkAddress) -> Bucket<S> {
        calculate_bucket(address, self.bucket_depth)
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
        if index.bucket() != self.bucket_for_address(address).value() {
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

        let owner = Address::arbitrary(u)?;
        let immutable = u.arbitrary()?;
        let amount = u.arbitrary()?;

        Ok(Self::new(owner, depth, bucket_depth, amount).with_immutable(immutable))
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
    fn depth_below_bucket_depth_is_rejected_through_batch_and_params() {
        let bucket_depth = BucketDepth::<Mainnet>::new(16).unwrap();

        let params = BatchParams::new(Address::ZERO, 20, bucket_depth, 1000);
        assert_eq!(params.geometry().unwrap().slots_per_bucket(), 16);

        let batch = Batch::new(BatchId::ZERO, 0, 0, Address::ZERO, 20, bucket_depth, false);
        assert_eq!(batch.geometry().unwrap().get(), 20);

        // A batch exactly as deep as its buckets holds one slot each.
        let flat = Batch::new(BatchId::ZERO, 0, 0, Address::ZERO, 16, bucket_depth, false);
        assert_eq!(flat.geometry().unwrap().slots_per_bucket(), 1);

        let shallow = Batch::new(BatchId::ZERO, 0, 0, Address::ZERO, 8, bucket_depth, false);
        assert!(matches!(
            shallow.geometry(),
            Err(StampError::DepthBelowBucketDepth {
                depth: 8,
                bucket_depth: 16
            })
        ));
        assert!(matches!(
            BatchParams::new(Address::ZERO, 8, bucket_depth, 1000).geometry(),
            Err(StampError::DepthBelowBucketDepth {
                depth: 8,
                bucket_depth: 16
            })
        ));

        // Dilution moves the depth, so the check survives a `set_depth`.
        let mut diluted = batch;
        diluted.set_depth(15);
        assert!(diluted.geometry().is_err());
    }

    #[test]
    fn chain_decode_stays_total_where_the_geometry_is_unrepresentable() {
        // A depth this far above the bucket depth has no `u32` slot count, yet
        // decoding it must not fail: only `geometry` refuses it.
        let batch: Batch = Batch::new(
            BatchId::ZERO,
            0,
            0,
            Address::ZERO,
            u8::MAX,
            BucketDepth::new(16).unwrap(),
            false,
        );
        assert_eq!(batch.depth(), u8::MAX);
        assert!(batch.validate_index(&StampIndex::new(0, 0)).is_ok());
        assert!(matches!(
            batch.geometry(),
            Err(StampError::SlotsTooWide {
                depth: 255,
                bucket_depth: 16,
                max: 31
            })
        ));
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
            min.bucket_for_address(&ChunkAddress::new([0xFF; 32]))
                .value(),
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
            max.bucket_for_address(&ChunkAddress::new([0xFF; 32]))
                .value(),
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
                .with_immutable(true);

        assert_eq!(params.owner(), Address::ZERO);
        assert_eq!(params.depth(), 20);
        assert_eq!(params.bucket_depth().get(), 16);
        assert_eq!(params.amount(), 1000);
        assert!(params.immutable());
    }

    #[test]
    fn test_batch_id_from_slice_length() {
        let bytes = [7u8; 32];
        assert_eq!(BatchId::from_slice(&bytes).unwrap(), BatchId::new(bytes));

        assert_eq!(
            BatchId::from_slice(&bytes[..31]).unwrap_err(),
            WrongLength {
                expected: 32,
                got: 31
            }
        );
        assert_eq!(
            BatchId::from_slice(&[0u8; 33]).unwrap_err(),
            WrongLength {
                expected: 32,
                got: 33
            }
        );
        assert!(BatchId::from_slice(&[]).is_err());
    }

    #[test]
    fn test_batch_params_accessors_round_trip() {
        let params: BatchParams =
            BatchParams::new(Address::ZERO, 20, BucketDepth::new(16).unwrap(), 1000)
                .with_immutable(true);

        assert_eq!(params.owner(), Address::ZERO);
        assert_eq!(params.depth(), 20);
        assert_eq!(params.bucket_depth(), BucketDepth::new(16).unwrap());
        assert!(params.immutable());
        assert_eq!(params.amount(), 1000);
    }
}
