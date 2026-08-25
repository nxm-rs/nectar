//! Batch geometry: the bucket depth, a bucket derived at one, and the batch
//! depth above one.

use core::{fmt, marker::PhantomData};

use nectar_primitives::{ChunkAddress, Mainnet, SwarmSpec};

use crate::{StampError, StampIndex};

/// The number of leading chunk-address bits that select a collision bucket, as
/// the network `S` accepts it.
///
/// Construction holds it between [`SwarmSpec::MIN_BUCKET_DEPTH`], the floor the
/// PostageStamp contract publishes as `minimumBucketDepth()`, and
/// [`MAX`](Self::MAX), the bucket-key width, so the `32 - depth` selection shift
/// is total wherever a depth reaches it.
///
/// A depth carries its network, so this does not compile:
///
/// ```compile_fail
/// use nectar_postage_primitives::{Batch, BatchId, BucketDepth};
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

/// A collision bucket together with the [`BucketDepth`] that cut it.
///
/// A bucket cut at depth 16 is in range for a depth-20 table, so a bare `u32`
/// lands in the wrong bucket undetected.
pub struct Bucket<S: SwarmSpec = Mainnet> {
    value: u32,
    depth: BucketDepth<S>,
}

impl<S: SwarmSpec> Bucket<S> {
    /// Cuts the bucket of `address`: its leading `depth` bits, read big-endian.
    #[inline]
    pub fn of(address: &ChunkAddress, depth: BucketDepth<S>) -> Self {
        let &[a, b, c, d, ..] = address.as_array();
        // Depth is 1..=32, so the shift is 0..=31 and never wraps.
        let value = u32::from_be_bytes([a, b, c, d])
            .wrapping_shr(u32::from(BucketDepth::<S>::MAX.saturating_sub(depth.get())));
        Self { value, depth }
    }

    /// Rejoins a bucket read from a stamp or a snapshot with the depth it must
    /// belong to.
    ///
    /// # Errors
    ///
    /// [`StampError::InvalidIndex`] when `value` is outside the range `depth`
    /// addresses.
    #[inline]
    pub const fn checked(value: u32, depth: BucketDepth<S>) -> Result<Self, StampError> {
        if !depth.contains_bucket(value) {
            return Err(StampError::InvalidIndex);
        }
        Ok(Self { value, depth })
    }

    /// Returns the bucket index.
    #[inline]
    pub const fn value(self) -> u32 {
        self.value
    }

    /// Returns the depth the bucket was cut at.
    #[inline]
    pub const fn depth(self) -> BucketDepth<S> {
        self.depth
    }
}

impl<S: SwarmSpec> Clone for Bucket<S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<S: SwarmSpec> Copy for Bucket<S> {}

impl<S: SwarmSpec> fmt::Debug for Bucket<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Bucket")
            .field("value", &self.value)
            .field("depth", &self.depth.get())
            .finish()
    }
}

impl<S: SwarmSpec> fmt::Display for Bucket<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.value, f)
    }
}

impl<S: SwarmSpec> PartialEq for Bucket<S> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value && self.depth == other.depth
    }
}

impl<S: SwarmSpec> Eq for Bucket<S> {}

impl<S: SwarmSpec> core::hash::Hash for Bucket<S> {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        self.value.hash(state);
        self.depth.hash(state);
    }
}

impl<S: SwarmSpec> From<Bucket<S>> for u32 {
    #[inline]
    fn from(bucket: Bucket<S>) -> Self {
        bucket.value
    }
}

/// A validated batch depth together with the [`BucketDepth`] beneath it.
///
/// Construction settles both bounds, so `depth - bucket_depth` cannot underflow
/// and `2^(depth - bucket_depth)` fits a `u32`.
pub struct BatchDepth<S: SwarmSpec = Mainnet> {
    depth: u8,
    bucket_depth: BucketDepth<S>,
}

impl<S: SwarmSpec> BatchDepth<S> {
    /// Widest `depth - bucket_depth` whose slot count fits a `u32`.
    pub const MAX_SLOT_BITS: u8 = 31;

    /// Validates a raw batch depth against `bucket_depth`.
    ///
    /// # Errors
    ///
    /// [`StampError::DepthBelowBucketDepth`] when `depth` is under the bucket
    /// depth, [`StampError::SlotsTooWide`] when it exceeds the bucket depth by
    /// more than [`MAX_SLOT_BITS`](Self::MAX_SLOT_BITS) bits.
    #[inline]
    pub const fn new(depth: u8, bucket_depth: BucketDepth<S>) -> Result<Self, StampError> {
        if depth < bucket_depth.get() {
            return Err(StampError::DepthBelowBucketDepth {
                depth,
                bucket_depth: bucket_depth.get(),
            });
        }
        // Guarded by the disjunct above.
        #[allow(clippy::arithmetic_side_effects)]
        let slot_bits = depth - bucket_depth.get();
        if slot_bits > Self::MAX_SLOT_BITS {
            return Err(StampError::SlotsTooWide {
                depth,
                bucket_depth: bucket_depth.get(),
                max: Self::MAX_SLOT_BITS,
            });
        }
        Ok(Self {
            depth,
            bucket_depth,
        })
    }

    /// Returns the batch depth as a bit count.
    #[inline]
    pub const fn get(self) -> u8 {
        self.depth
    }

    /// Returns the bucket depth beneath it.
    #[inline]
    pub const fn bucket_depth(self) -> BucketDepth<S> {
        self.bucket_depth
    }

    /// Returns `depth - bucket_depth`, the bit width of a slot position.
    #[inline]
    // Settled by `new`.
    #[allow(clippy::arithmetic_side_effects)]
    pub const fn slot_bits(self) -> u8 {
        self.depth - self.bucket_depth.get()
    }

    /// Returns the number of slots in each bucket, `2^(depth - bucket_depth)`.
    #[inline]
    pub const fn slots_per_bucket(self) -> u32 {
        1u32 << self.slot_bits()
    }

    /// Returns the number of collision buckets, `2^bucket_depth`.
    #[inline]
    pub const fn bucket_count(self) -> u64 {
        self.bucket_depth.bucket_count()
    }

    /// Returns the total number of slots, `2^depth`.
    #[inline]
    pub const fn total_slots(self) -> u64 {
        1u64 << self.depth
    }

    /// Returns whether both coordinates of `index` are inside this geometry.
    #[inline]
    pub const fn contains(self, index: &StampIndex) -> bool {
        self.bucket_depth.contains_bucket(index.bucket()) && index.index() < self.slots_per_bucket()
    }

    /// Adopts an on-chain dilution; a depth at or below the current one is a
    /// no-op, so a redelivered or reordered event cannot shrink the batch.
    ///
    /// # Errors
    ///
    /// [`StampError::SlotsTooWide`] when the new depth leaves more slots per
    /// bucket than a `u32` count holds.
    #[inline]
    pub const fn diluted(self, new_depth: u8) -> Result<Self, StampError> {
        if new_depth <= self.depth {
            return Ok(self);
        }
        Self::new(new_depth, self.bucket_depth)
    }
}

impl<S: SwarmSpec> Clone for BatchDepth<S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<S: SwarmSpec> Copy for BatchDepth<S> {}

impl<S: SwarmSpec> fmt::Debug for BatchDepth<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchDepth")
            .field("depth", &self.depth)
            .field("bucket_depth", &self.bucket_depth.get())
            .finish()
    }
}

impl<S: SwarmSpec> fmt::Display for BatchDepth<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.depth, f)
    }
}

impl<S: SwarmSpec> PartialEq for BatchDepth<S> {
    fn eq(&self, other: &Self) -> bool {
        self.depth == other.depth && self.bucket_depth == other.bucket_depth
    }
}

impl<S: SwarmSpec> Eq for BatchDepth<S> {}

impl<S: SwarmSpec> core::hash::Hash for BatchDepth<S> {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        self.depth.hash(state);
        self.bucket_depth.hash(state);
    }
}

/// Returns the collision bucket of `address` at `bucket_depth`.
///
/// # Example
///
/// ```
/// use nectar_postage_primitives::{BucketDepth, calculate_bucket};
/// use nectar_primitives::{ChunkAddress, Mainnet};
///
/// let address = ChunkAddress::new([0xCB, 0xE5, 0x00, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
/// let bucket_depth = BucketDepth::<Mainnet>::new(16).unwrap();
/// assert_eq!(calculate_bucket(&address, bucket_depth).value(), 0xCBE5);
/// ```
#[inline]
pub fn calculate_bucket<S: SwarmSpec>(
    address: &ChunkAddress,
    bucket_depth: BucketDepth<S>,
) -> Bucket<S> {
    Bucket::of(address, bucket_depth)
}

#[cfg(test)]
mod tests {
    use nectar_primitives::Mainnet;
    use nectar_testing::{HighFloor, LowFloor};

    use super::*;

    // `nectar_testing::low_floor` returns the `BucketDepth` of the
    // `nectar-postage` instance it links, which is not this one.
    fn low_floor(depth: u8) -> BucketDepth<LowFloor> {
        BucketDepth::new(depth).unwrap()
    }

    fn address_cbe5() -> ChunkAddress {
        ChunkAddress::new([
            0xCB, 0xE5, 0x00, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0,
        ])
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
        assert_eq!(BucketDepth::<LowFloor>::new(1).unwrap().get(), 1);
        assert!(matches!(
            BucketDepth::<LowFloor>::new(0),
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
            BucketDepth::<HighFloor>::new(16),
            Err(StampError::BucketDepthBelowMinimum {
                bucket_depth: 16,
                minimum: 20
            })
        ));
        assert!(BucketDepth::<HighFloor>::new(20).is_ok());
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
        assert!(decode::<HighFloor>(16).is_err());
        assert!(decode::<HighFloor>(20).is_ok());
    }

    #[test]
    fn test_calculate_bucket() {
        let address = address_cbe5();

        assert_eq!(
            calculate_bucket(&address, BucketDepth::<Mainnet>::new(16).unwrap()).value(),
            0xCBE5
        );
        assert_eq!(calculate_bucket(&address, low_floor(8)).value(), 0xCB);
        assert_eq!(calculate_bucket(&address, low_floor(4)).value(), 0xC);
    }

    #[test]
    fn calculate_bucket_spans_the_whole_depth_range() {
        let address = address_cbe5();

        assert_eq!(calculate_bucket(&address, low_floor(1)).value(), 1);
        assert_eq!(
            calculate_bucket(&address, low_floor(32)).value(),
            0xCBE5_0000
        );
    }

    #[test]
    fn a_bucket_carries_the_depth_that_cut_it() {
        let address = address_cbe5();
        let shallow = Bucket::of(&address, low_floor(8));
        let deep = Bucket::of(&address, low_floor(16));

        assert_eq!(shallow.depth(), low_floor(8));
        assert_eq!(deep.depth(), low_floor(16));
        assert_eq!(Bucket::of(&address, low_floor(16)), deep);
        assert_ne!(shallow, Bucket::checked(0xCB, low_floor(16)).unwrap());
    }

    #[test]
    fn checked_refuses_a_bucket_outside_the_depth() {
        assert_eq!(Bucket::checked(0xFF, low_floor(8)).unwrap().value(), 0xFF);
        assert!(matches!(
            Bucket::checked(0x100, low_floor(8)),
            Err(StampError::InvalidIndex)
        ));
        // Every `u32` is a bucket at the widest depth.
        assert!(Bucket::checked(u32::MAX, low_floor(32)).is_ok());
    }

    #[test]
    fn batch_depth_folds_both_geometry_bounds() {
        let bucket_depth = BucketDepth::<Mainnet>::new(16).unwrap();

        let geometry = BatchDepth::new(20, bucket_depth).unwrap();
        assert_eq!(geometry.get(), 20);
        assert_eq!(geometry.bucket_depth(), bucket_depth);
        assert_eq!(geometry.slot_bits(), 4);
        assert_eq!(geometry.slots_per_bucket(), 16);
        assert_eq!(geometry.bucket_count(), 1 << 16);
        assert_eq!(geometry.total_slots(), 1 << 20);

        assert_eq!(
            BatchDepth::new(16, bucket_depth)
                .unwrap()
                .slots_per_bucket(),
            1
        );

        assert!(matches!(
            BatchDepth::new(15, bucket_depth),
            Err(StampError::DepthBelowBucketDepth {
                depth: 15,
                bucket_depth: 16
            })
        ));
    }

    #[test]
    fn batch_depth_refuses_a_slot_count_wider_than_a_u32() {
        let bucket_depth = BucketDepth::<Mainnet>::new(16).unwrap();

        assert_eq!(
            BatchDepth::new(47, bucket_depth)
                .unwrap()
                .slots_per_bucket(),
            1 << 31
        );
        assert!(matches!(
            BatchDepth::new(48, bucket_depth),
            Err(StampError::SlotsTooWide {
                depth: 48,
                bucket_depth: 16,
                max: 31
            })
        ));
    }

    #[test]
    fn contains_bounds_both_coordinates() {
        let geometry = BatchDepth::new(18, BucketDepth::<Mainnet>::new(16).unwrap()).unwrap();

        assert!(geometry.contains(&StampIndex::new(0xFFFF, 3)));
        assert!(!geometry.contains(&StampIndex::new(0x1_0000, 0)));
        assert!(!geometry.contains(&StampIndex::new(0, 4)));
    }

    #[test]
    fn dilution_is_monotone_and_stays_representable() {
        let bucket_depth = BucketDepth::<Mainnet>::new(16).unwrap();
        let geometry = BatchDepth::new(18, bucket_depth).unwrap();

        assert_eq!(geometry.diluted(20).unwrap().get(), 20);
        assert_eq!(geometry.diluted(17).unwrap(), geometry);
        assert_eq!(geometry.diluted(18).unwrap(), geometry);
        assert_eq!(geometry.diluted(0).unwrap(), geometry);

        assert!(matches!(
            geometry.diluted(48),
            Err(StampError::SlotsTooWide { .. })
        ));
    }
}
