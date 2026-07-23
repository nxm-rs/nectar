//! Wire format encoder and decoder for usage snapshots.
//!
//! See the crate documentation and `README.md` for the format specification.

use alloc::vec;
use alloc::vec::Vec;

use alloy_primitives::{B256, keccak256};
use bytes::Bytes;
use nectar_postage::{BatchId, BucketDepth};
use nectar_primitives::wire::{Cursor, FromCursor, ToWriter, Underrun, Writer};
use nectar_primitives::{Mainnet, SwarmSpec};

use crate::snapshot::SnapshotFor;
use crate::table::{Mutability, UsageTableFor, checked_bucket_depth};
use crate::{
    MAGIC, MAX_EXCEPTIONS, MAX_PAYLOAD_SIZE, MAX_WIDTH, ROOT_HEADER_SIZE, Result, UsageError,
};

/// The serialized form of a snapshot: one root payload and zero or more leaf
/// payloads. Payload `n` of the snapshot (root is `n = 0`, leaf `i` is
/// `n = i + 1`) belongs in the single-owner chunk with id
/// [`usage_chunk_id`](crate::usage_chunk_id)`(batch_id, n)`.
///
/// Crate-internal: the only encoder is [`encode`], and [`Validated::plan_persist`]
/// turns its output into the public [`PersistPlan`], so no external consumer ever
/// holds an `Encoded`.
///
/// [`Validated::plan_persist`]: crate::Validated::plan_persist
/// [`PersistPlan`]: crate::PersistPlan
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Encoded {
    /// The root chunk payload.
    pub root: Bytes,
    /// The leaf chunk payloads, in chunk-index order.
    pub leaves: Vec<Bytes>,
}

/// The leaf section of a parsed root payload.
#[derive(Debug, Clone, PartialEq, Eq)]
enum LeafSection {
    /// The packed delta bitstream is stored inline in the root.
    Inline(Vec<u8>),
    /// The packed delta bitstream is split across leaf chunks bound by
    /// these payload digests.
    Digests(Vec<B256>),
}

/// A parsed root payload, ready to be assembled with its leaf payloads.
///
/// The network is a type parameter: the geometry on the wire is a bare depth
/// byte, and parsing it as a snapshot of `S` is what re-establishes the
/// [`BucketDepth`] proof. [`RootInfo`] is the mainnet root.
#[derive(Debug)]
pub struct RootInfoFor<S: SwarmSpec = Mainnet> {
    batch_id: BatchId,
    depth: u8,
    bucket_depth: BucketDepth<S>,
    mutable: bool,
    width: u8,
    sequence: u64,
    total_issued: u64,
    base: u32,
    exceptions: Vec<(u32, u32)>,
    slots: Vec<u32>,
    leaves: LeafSection,
}

/// The [`RootInfoFor`] of the mainnet spec.
pub type RootInfo = RootInfoFor<Mainnet>;

// The spec is a type-level tag, so the impls below carry no bound on `S` beyond
// `SwarmSpec`; deriving would demand `S: Clone` and `S: Eq` of a marker type
// that holds no data.

impl<S: SwarmSpec> Clone for RootInfoFor<S> {
    fn clone(&self) -> Self {
        Self {
            batch_id: self.batch_id,
            depth: self.depth,
            bucket_depth: self.bucket_depth,
            mutable: self.mutable,
            width: self.width,
            sequence: self.sequence,
            total_issued: self.total_issued,
            base: self.base,
            exceptions: self.exceptions.clone(),
            slots: self.slots.clone(),
            leaves: self.leaves.clone(),
        }
    }
}

impl<S: SwarmSpec> PartialEq for RootInfoFor<S> {
    fn eq(&self, other: &Self) -> bool {
        self.batch_id == other.batch_id
            && self.depth == other.depth
            && self.bucket_depth == other.bucket_depth
            && self.mutable == other.mutable
            && self.width == other.width
            && self.sequence == other.sequence
            && self.total_issued == other.total_issued
            && self.base == other.base
            && self.exceptions == other.exceptions
            && self.slots == other.slots
            && self.leaves == other.leaves
    }
}

impl<S: SwarmSpec> Eq for RootInfoFor<S> {}

/// The fixed root header, stated once in wire order. Multi-byte fields are
/// big-endian; `README.md` gives the full layout.
struct RootHeader {
    magic: [u8; 4],
    batch_id: BatchId,
    depth: u8,
    bucket_depth: u8,
    /// Bit 0 marks a mutable (ring-cursor) batch; other bits are reserved.
    flags: u8,
    width: u8,
    sequence: u64,
    total_issued: u64,
    base: u32,
    allocated: u16,
    leaves: u16,
    exceptions: u16,
}

impl RootHeader {
    /// Total wire width, summed from the field types in wire order.
    const SIZE: usize = size_of::<[u8; 4]>()  // magic
        + BatchId::SIZE                       // batch_id
        + 4 * size_of::<u8>()                 // depth, bucket_depth, flags, width
        + 2 * size_of::<u64>()                // sequence, total_issued
        + size_of::<u32>()                    // base
        + 3 * size_of::<u16>(); // allocated, leaves, exceptions
}

// The layout is stated once, by the paired impls below; this pins their total
// width to the crate constant.
const _: () = assert!(RootHeader::SIZE == ROOT_HEADER_SIZE);

impl FromCursor for RootHeader {
    type Error = Underrun;

    fn take_from(cur: &mut Cursor<'_>) -> core::result::Result<Self, Underrun> {
        Ok(Self {
            magic: cur.take()?,
            batch_id: BatchId::new(cur.take()?),
            depth: cur.take()?,
            bucket_depth: cur.take()?,
            flags: cur.take()?,
            width: cur.take()?,
            sequence: u64::from_be_bytes(cur.take()?),
            total_issued: u64::from_be_bytes(cur.take()?),
            base: u32::from_be_bytes(cur.take()?),
            allocated: u16::from_be_bytes(cur.take()?),
            leaves: u16::from_be_bytes(cur.take()?),
            exceptions: u16::from_be_bytes(cur.take()?),
        })
    }
}

/// The mirror of the `FromCursor` impl above; the field order and byte order
/// never leave this type.
impl ToWriter for RootHeader {
    fn put_into(&self, w: &mut Writer<'_>) {
        w.put(&self.magic);
        w.put(&<[u8; 32]>::from(self.batch_id));
        w.put(&self.depth);
        w.put(&self.bucket_depth);
        w.put(&self.flags);
        w.put(&self.width);
        w.put(&self.sequence.to_be_bytes());
        w.put(&self.total_issued.to_be_bytes());
        w.put(&self.base.to_be_bytes());
        w.put(&self.allocated.to_be_bytes());
        w.put(&self.leaves.to_be_bytes());
        w.put(&self.exceptions.to_be_bytes());
    }
}

/// One exception record: the bucket index then its absolute count, both
/// big-endian u32.
struct ExceptionEntry {
    bucket: u32,
    count: u32,
}

impl FromCursor for ExceptionEntry {
    type Error = Underrun;

    fn take_from(cur: &mut Cursor<'_>) -> core::result::Result<Self, Underrun> {
        Ok(Self {
            bucket: u32::from_be_bytes(cur.take()?),
            count: u32::from_be_bytes(cur.take()?),
        })
    }
}

impl ToWriter for ExceptionEntry {
    fn put_into(&self, w: &mut Writer<'_>) {
        w.put(&self.bucket.to_be_bytes());
        w.put(&self.count.to_be_bytes());
    }
}

/// One allocated within-bucket slot index, big-endian u32.
struct SlotEntry(u32);

impl FromCursor for SlotEntry {
    type Error = Underrun;

    fn take_from(cur: &mut Cursor<'_>) -> core::result::Result<Self, Underrun> {
        cur.take().map(|bytes| Self(u32::from_be_bytes(bytes)))
    }
}

impl ToWriter for SlotEntry {
    fn put_into(&self, w: &mut Writer<'_>) {
        w.put(&self.0.to_be_bytes());
    }
}

/// Narrows a section count to its u16 wire field. The validated geometry
/// keeps every section count within u16, so a failure signals an internal
/// inconsistency rather than an expected condition.
fn section_count(n: usize) -> Result<u16> {
    u16::try_from(n).map_err(|_| UsageError::Malformed("section count exceeds u16"))
}

/// Returns the maximum delta representable in `width` bits.
// In the else branch `width < 32`, so the u64 shift is in range and
// `1 << width >= 1` makes the decrement safe. The `u32::MAX -> u64`
// widening is infallible; `u64::from` is not const-callable.
#[allow(clippy::arithmetic_side_effects, clippy::as_conversions)]
const fn delta_limit(width: u8) -> u64 {
    if width >= 32 {
        u32::MAX as u64
    } else {
        (1u64 << width) - 1
    }
}

/// Returns the byte length of `buckets` deltas packed at `width` bits.
// `buckets <= 2^16` (validated geometry) and `width <= 32`, so the product
// is at most 2^21 and cannot overflow usize. The `u8 -> usize` widening is
// infallible; `usize::from` is not const-callable.
#[allow(clippy::arithmetic_side_effects, clippy::as_conversions)]
const fn packed_len(buckets: usize, width: u8) -> usize {
    (buckets * width as usize).div_ceil(8)
}

/// Returns the number of buckets per leaf for a given width (`width > 0`).
// Callers uphold `width > 0` (leaves exist only for a nonzero delta width:
// the encoder inlines width 0 and the parser rejects `leaves > 0` with
// width 0), so the division cannot be by zero; the dividend is a constant.
// The `u8 -> usize` widening is infallible; `usize::from` is not
// const-callable.
#[allow(clippy::arithmetic_side_effects, clippy::as_conversions)]
const fn buckets_per_leaf(width: u8) -> usize {
    (MAX_PAYLOAD_SIZE * 8) / width as usize
}

/// Returns the number of leaves needed for `buckets` deltas at `width` bits.
const fn leaf_count(buckets: usize, width: u8) -> usize {
    if width == 0 {
        0
    } else {
        buckets.div_ceil(buckets_per_leaf(width))
    }
}

/// Writes the low `width` bits of `value` at `bit_offset`, MSB first.
// Callers size `buf` to `packed_len(..)` with `bit_offset + width` within
// its bit length, and `i < width` keeps the shift exponent in range, so the
// offset math and byte indexing cannot overflow or go out of bounds.
#[allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)]
fn write_bits(buf: &mut [u8], bit_offset: usize, width: u8, value: u64) {
    for i in 0..usize::from(width) {
        let bit = (value >> (usize::from(width) - 1 - i)) & 1;
        if bit != 0 {
            let pos = bit_offset + i;
            buf[pos / 8] |= 1 << (7 - pos % 8);
        }
    }
}

/// Reads `width` bits at `bit_offset`, MSB first.
// Callers validate `buf` to `packed_len(..)` bytes with `bit_offset + width`
// within its bit length before reading, and `width <= 32` keeps the value
// accumulation within u64, so the offset math and indexing cannot fail.
#[allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)]
fn read_bits(buf: &[u8], bit_offset: usize, width: u8) -> u64 {
    let mut value = 0u64;
    for i in 0..usize::from(width) {
        let pos = bit_offset + i;
        let bit = (buf[pos / 8] >> (7 - pos % 8)) & 1;
        value = (value << 1) | u64::from(bit);
    }
    value
}

/// Returns `bucket` as a table index.
// `u32` always fits `usize` on the >=32-bit targets this crate supports;
// `usize::from` takes at most `u16`.
#[allow(clippy::as_conversions)]
const fn bucket_index(bucket: u32) -> usize {
    bucket as usize
}

/// Packs the deltas of buckets `range` into a fresh zero-padded buffer.
///
/// Exception buckets are filled with all one bits; `exceptions` must be
/// sorted ascending by bucket.
// `start <= end <= counts.len()` (callers pass leaf ranges clamped to the
// bucket count), every bucket index is below `end`, and `base` is the
// minimum count, so `count - base` cannot underflow; `i * width` is bounded
// by the buffer sized from `packed_len`.
#[allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)]
fn pack_range(
    counts: &[u32],
    base: u32,
    width: u8,
    exceptions: &[(u32, u32)],
    start: usize,
    end: usize,
) -> Vec<u8> {
    let limit = delta_limit(width);
    let mut out = vec![0u8; packed_len(end - start, width)];
    let mut except = exceptions
        .iter()
        .map(|&(bucket, _)| bucket_index(bucket))
        .skip_while(|&b| b < start)
        .peekable();
    for (i, bucket) in (start..end).enumerate() {
        let delta = if except.peek() == Some(&bucket) {
            except.next();
            limit
        } else {
            u64::from(counts[bucket] - base)
        };
        write_bits(&mut out, i * usize::from(width), width, delta);
    }
    out
}

/// Checks that all bits past `bit_len` in `buf` are zero.
// Callers validate `buf.len() == packed_len(..) == ceil(bit_len / 8)`
// before the call, so when `bit_len` is not byte-aligned the final byte
// `buf[bit_len / 8]` exists.
#[allow(clippy::indexing_slicing)]
const fn padding_is_zero(buf: &[u8], bit_len: usize) -> bool {
    if !bit_len.is_multiple_of(8) {
        let last = buf[bit_len / 8];
        let mask = 0xffu8 >> (bit_len % 8);
        if last & mask != 0 {
            return false;
        }
    }
    true
}

/// Picks the encoding width minimizing the encoded byte size: packed bits
/// plus 8 bytes per exception, plus 32 bytes per leaf digest when the table
/// does not inline. Ties break toward the smaller width. Returns `None` when
/// no width fits, which cannot happen within the supported geometry.
// `base` is the minimum count so `count - base` cannot underflow;
// `leading_zeros() <= 32` keeps the histogram index within its 33 entries
// and `width <= MAX_WIDTH = 32` keeps the tail slice in bounds; the size
// arithmetic is over values bounded by the geometry (buckets <= 2^16,
// exceptions <= buckets, allocated <= the snapshot's chunk count), all far
// below usize overflow.
#[allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)]
fn select_width(counts: &[u32], base: u32, buckets: usize, allocated: usize) -> Option<u8> {
    // histogram[n] counts deltas whose minimal representation is n bits, so
    // the exception count at width w is the histogram tail above w.
    let mut histogram = [0usize; 33];
    for &count in counts {
        let delta = count - base;
        // `leading_zeros() <= 32`, so the histogram index fits usize.
        #[allow(clippy::as_conversions)]
        let bits = (32 - delta.leading_zeros()) as usize;
        histogram[bits] += 1;
    }

    let mut best: Option<(u8, usize)> = None;
    for width in 0..=MAX_WIDTH {
        let exceptions: usize = histogram[usize::from(width) + 1..].iter().sum();
        if exceptions > MAX_EXCEPTIONS {
            continue;
        }
        let fixed = ROOT_HEADER_SIZE + 8 * exceptions + 4 * allocated;
        let packed = packed_len(buckets, width);
        let mut cost = 8 * exceptions + packed;
        if fixed + packed > MAX_PAYLOAD_SIZE {
            let leaves = leaf_count(buckets, width);
            if fixed + 32 * leaves > MAX_PAYLOAD_SIZE {
                continue;
            }
            cost += 32 * leaves;
        }
        if best.is_none_or(|(_, c)| cost < c) {
            best = Some((width, cost));
        }
    }
    best.map(|(width, _)| width)
}

/// Encodes a snapshot into its root and leaf payloads.
// Sizes are bounded by the validated geometry (`buckets <= 2^16`,
// `width <= 32`, `exceptions <= MAX_EXCEPTIONS`, `allocated` at most the
// chunk count), so every saturating operation below is exact.
#[must_use = "the encoded payloads are the snapshot to publish; dropping them discards the encode"]
pub(crate) fn encode<S: SwarmSpec>(
    table: &UsageTableFor<S>,
    sequence: u64,
    slots: &[u32],
) -> Result<Encoded> {
    if slots.is_empty() {
        return Err(UsageError::Malformed("root slot not allocated"));
    }
    // The bucket count is a table index bound, `<= 2^16`.
    let buckets = bucket_index(table.bucket_count());
    let counts = table.counts();
    let base = table.min_count();
    let allocated = slots.len();

    // Unreachable for supported geometry: at width 32 there are no
    // exceptions and the leaf table always fits in the root.
    let width = select_width(counts, base, buckets, allocated)
        .ok_or(UsageError::Malformed("no encoding fits the root chunk"))?;

    let limit = delta_limit(width);
    // `base` is the minimum count, so the saturating subtraction is exact;
    // the u32 bucket range covers `buckets <= 2^16`.
    let exceptions: Vec<(u32, u32)> = (0u32..)
        .zip(counts)
        .filter(|&(_, &count)| u64::from(count.saturating_sub(base)) > limit)
        .map(|(bucket, &count)| (bucket, count))
        .collect();

    let fixed = ROOT_HEADER_SIZE
        .saturating_add(exceptions.len().saturating_mul(8))
        .saturating_add(allocated.saturating_mul(4));
    let inline_len = packed_len(buckets, width);
    let (leaves, inline) = if fixed.saturating_add(inline_len) <= MAX_PAYLOAD_SIZE {
        (0usize, true)
    } else {
        (leaf_count(buckets, width), false)
    };

    let mut leaf_payloads = Vec::with_capacity(leaves);
    if !inline {
        let per_leaf = buckets_per_leaf(width);
        for i in 0..leaves {
            let start = i.saturating_mul(per_leaf);
            let end = start.saturating_add(per_leaf).min(buckets);
            leaf_payloads.push(Bytes::from(pack_range(
                counts,
                base,
                width,
                &exceptions,
                start,
                end,
            )));
        }
    }

    let tail = if inline {
        inline_len
    } else {
        leaves.saturating_mul(32)
    };
    let mut root = Vec::with_capacity(fixed.saturating_add(tail));
    let mut w = Writer::new(&mut root);
    w.put(&RootHeader {
        magic: MAGIC,
        batch_id: table.batch_id(),
        depth: table.depth(),
        bucket_depth: table.bucket_depth().get(),
        flags: if table.is_mutable() { 1 } else { 0 },
        width,
        sequence,
        total_issued: table.total_issued(),
        base,
        allocated: section_count(allocated)?,
        leaves: section_count(leaves)?,
        exceptions: section_count(exceptions.len())?,
    });
    for &(bucket, count) in &exceptions {
        w.put(&ExceptionEntry { bucket, count });
    }
    for &slot in slots {
        w.put(&SlotEntry(slot));
    }
    if inline {
        w.put(pack_range(counts, base, width, &exceptions, 0, buckets).as_slice());
    } else {
        for payload in &leaf_payloads {
            w.put(&keccak256(payload).0);
        }
    }

    Ok(Encoded {
        root: Bytes::from(root),
        leaves: leaf_payloads,
    })
}

impl<S: SwarmSpec> RootInfoFor<S> {
    /// Parses and structurally validates a root payload.
    ///
    /// Untrusted input: every byte access goes through [`Cursor`], and the
    /// declared section counts must explain the payload length exactly before
    /// any section is read.
    // The size arithmetic is over u16-derived section counts and validated
    // geometry (`bucket_depth in 1..=16`, `bucket_depth <= depth`), so every
    // saturating operation is exact for an accepted payload; a saturated sum
    // fails the chunk-size check.
    pub fn parse(payload: &[u8]) -> Result<Self> {
        let mut cur = Cursor::new(payload);
        let header = cur
            .take::<RootHeader>()
            .map_err(|_| UsageError::PayloadLength {
                expected: ROOT_HEADER_SIZE,
                got: payload.len(),
            })?;
        if header.magic != MAGIC {
            return Err(UsageError::BadMagic);
        }
        let bucket_depth = checked_bucket_depth::<S>(header.depth, header.bucket_depth)
            .map_err(UsageError::into_corruption)?;
        // Only bit 0 (mutable) is defined; any other bit set is rejected so a
        // future flag is never silently ignored by an older reader.
        if header.flags & !0x01 != 0 {
            return Err(UsageError::Malformed("unsupported flags"));
        }
        let mutable = header.flags & 0x01 == 1;
        let width = header.width;
        if width > MAX_WIDTH {
            return Err(UsageError::Malformed("delta width exceeds 32"));
        }
        let allocated = usize::from(header.allocated);
        let leaves = usize::from(header.leaves);
        let exception_count = usize::from(header.exceptions);

        let buckets = 1usize << header.bucket_depth;
        let capacity = 1u32 << header.depth.saturating_sub(header.bucket_depth);

        if exception_count > MAX_EXCEPTIONS {
            return Err(UsageError::Malformed("too many exceptions"));
        }
        if allocated == 0 {
            return Err(UsageError::Malformed("root slot missing"));
        }
        if leaves > 0 {
            if width == 0 {
                return Err(UsageError::Malformed("leaves with zero delta width"));
            }
            if leaves != leaf_count(buckets, width) {
                return Err(UsageError::Malformed("leaf count does not match width"));
            }
            // Every leaf needs a slot, plus one for the root.
            if allocated <= leaves {
                return Err(UsageError::Malformed("missing slots for leaves"));
            }
        }

        let tail = if leaves > 0 {
            leaves.saturating_mul(32)
        } else {
            packed_len(buckets, width)
        };
        let expected = ROOT_HEADER_SIZE
            .saturating_add(exception_count.saturating_mul(8))
            .saturating_add(allocated.saturating_mul(4))
            .saturating_add(tail);
        if expected > MAX_PAYLOAD_SIZE {
            return Err(UsageError::Malformed("root larger than a chunk"));
        }
        if payload.len() != expected {
            return Err(UsageError::PayloadLength {
                expected,
                got: payload.len(),
            });
        }

        let mut exceptions = Vec::with_capacity(exception_count);
        let mut previous: Option<u32> = None;
        for _ in 0..exception_count {
            let ExceptionEntry { bucket, count } = cur.take()?;
            if bucket_index(bucket) >= buckets {
                return Err(UsageError::CorruptBucket { bucket });
            }
            if previous.is_some_and(|p| p >= bucket) {
                return Err(UsageError::Malformed("exceptions not strictly ascending"));
            }
            if count > capacity {
                return Err(UsageError::CorruptCounter {
                    bucket,
                    count,
                    capacity,
                });
            }
            previous = Some(bucket);
            exceptions.push((bucket, count));
        }

        let mut slots = Vec::with_capacity(allocated);
        for _ in 0..allocated {
            let SlotEntry(slot) = cur.take()?;
            if slot >= capacity {
                return Err(UsageError::CorruptSlot { slot, capacity });
            }
            slots.push(slot);
        }

        let leaves = if leaves > 0 {
            let mut digests = Vec::with_capacity(leaves);
            for _ in 0..leaves {
                digests.push(B256::from(cur.take::<[u8; 32]>()?));
            }
            LeafSection::Digests(digests)
        } else {
            let packed = cur.finish().to_vec();
            if !padding_is_zero(&packed, buckets.saturating_mul(usize::from(width))) {
                return Err(UsageError::Malformed("nonzero padding"));
            }
            LeafSection::Inline(packed)
        };

        Ok(Self {
            batch_id: header.batch_id,
            depth: header.depth,
            bucket_depth,
            mutable,
            width,
            sequence: header.sequence,
            total_issued: header.total_issued,
            base: header.base,
            exceptions,
            slots,
            leaves,
        })
    }

    /// Returns the batch id.
    pub const fn batch_id(&self) -> BatchId {
        self.batch_id
    }

    /// Returns whether the snapshot describes a mutable (ring-cursor) batch.
    pub const fn is_mutable(&self) -> bool {
        self.mutable
    }

    /// Returns the batch depth.
    pub const fn depth(&self) -> u8 {
        self.depth
    }

    /// Returns the bucket depth.
    pub const fn bucket_depth(&self) -> BucketDepth<S> {
        self.bucket_depth
    }

    /// Returns the snapshot sequence number.
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the total number of issued slots declared by the header.
    pub const fn total_issued(&self) -> u64 {
        self.total_issued
    }

    /// Returns the within-bucket slots allocated to snapshot chunks, in
    /// chunk-index order (entry 0 is the root's own slot).
    pub fn allocated_slots(&self) -> &[u32] {
        &self.slots
    }

    /// Returns the number of leaf chunks that must be fetched to assemble
    /// this snapshot.
    // The digest list length round-trips a `leaves` header field parsed
    // from a u16, so it always fits back into u16; `u16::try_from` is not
    // const-callable.
    #[allow(clippy::as_conversions)]
    pub const fn leaf_count(&self) -> u16 {
        match &self.leaves {
            LeafSection::Inline(_) => 0,
            LeafSection::Digests(digests) => digests.len() as u16,
        }
    }

    /// Returns the expected byte length of the zero-based `leaf`, or `None`
    /// if the index is out of range.
    // `leaf < leaf_count <= 2^16` and `per_leaf <= MAX_PAYLOAD_SIZE * 8`,
    // so `leaf * per_leaf` stays far below usize overflow, and
    // `end = min(start + per_leaf, buckets) >= start` keeps `end - start`
    // from underflowing.
    #[allow(clippy::arithmetic_side_effects)]
    pub fn expected_leaf_len(&self, leaf: u16) -> Option<usize> {
        if usize::from(leaf) >= usize::from(self.leaf_count()) {
            return None;
        }
        let buckets = 1usize << self.bucket_depth.get();
        let per_leaf = buckets_per_leaf(self.width);
        let start = usize::from(leaf) * per_leaf;
        let end = (start + per_leaf).min(buckets);
        Some(packed_len(end - start, self.width))
    }

    /// Verifies the leaf payloads against the root and reconstructs the
    /// snapshot.
    ///
    /// `leaves` must contain exactly [`leaf_count`](Self::leaf_count)
    /// payloads in chunk-index order.
    // `parse` already validated the geometry (`depth - bucket_depth <= 31`),
    // the exception buckets (`bucket < buckets`, indexed into a `counts`
    // vector of exactly `buckets` entries), and the inline packed length;
    // each untrusted leaf payload is length checked against `packed_len`
    // before it is unpacked. The remaining index math (`i * per_leaf`,
    // `start + per_leaf`, `end - start` with `end >= start`) is bounded by
    // `buckets <= 2^16` and `width <= 32`.
    #[allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)]
    #[must_use = "the reassembled snapshot is the recovered state; dropping it discards the assemble"]
    pub fn assemble<L: AsRef<[u8]>>(self, leaves: &[L]) -> Result<SnapshotFor<S>> {
        let buckets = 1usize << self.bucket_depth.get();
        let capacity = 1u32 << (self.depth - self.bucket_depth.get());
        let width = self.width;

        let mut counts = vec![0u32; buckets];
        // Unpacks the deltas of buckets `start..end`. Exception buckets carry
        // a meaningless filler value; they are skipped here and overlaid with
        // their absolute counts afterwards.
        let unpack_range = |counts: &mut [u32], packed: &[u8], start: usize, end: usize| {
            if !padding_is_zero(packed, (end - start) * usize::from(width)) {
                return Err(UsageError::Malformed("nonzero padding"));
            }
            let mut except = self
                .exceptions
                .iter()
                .map(|&(bucket, _)| bucket_index(bucket))
                .skip_while(|&b| b < start)
                .peekable();
            for (i, count) in counts[start..end].iter_mut().enumerate() {
                let bucket = start + i;
                if except.peek() == Some(&bucket) {
                    except.next();
                    continue;
                }
                let value = u64::from(self.base) + read_bits(packed, i * usize::from(width), width);
                if value > u64::from(capacity) {
                    // `bucket < buckets <= 2^16` fits u32, and the reported
                    // count is min-clamped to u32::MAX before the cast.
                    #[allow(clippy::as_conversions)]
                    let bucket = bucket as u32;
                    #[allow(clippy::as_conversions)]
                    let count = value.min(u64::from(u32::MAX)) as u32;
                    return Err(UsageError::CorruptCounter {
                        bucket,
                        count,
                        capacity,
                    });
                }
                // `value <= capacity` (checked above) keeps it within u32.
                #[allow(clippy::as_conversions)]
                let value = value as u32;
                *count = value;
            }
            Ok(())
        };

        match &self.leaves {
            LeafSection::Inline(packed) => {
                if !leaves.is_empty() {
                    return Err(UsageError::LeafCount {
                        expected: 0,
                        got: leaves.len(),
                    });
                }
                unpack_range(&mut counts, packed, 0, buckets)?;
            }
            LeafSection::Digests(digests) => {
                if leaves.len() != digests.len() {
                    return Err(UsageError::LeafCount {
                        expected: digests.len(),
                        got: leaves.len(),
                    });
                }
                let per_leaf = buckets_per_leaf(width);
                for (i, (payload, digest)) in leaves.iter().zip(digests.iter()).enumerate() {
                    // `i < digests.len() <= u16::MAX` (the digest list length
                    // round-trips a u16 header field).
                    #[allow(clippy::as_conversions)]
                    let index = i as u16;
                    let payload = payload.as_ref();
                    let start = i * per_leaf;
                    let end = (start + per_leaf).min(buckets);
                    let expected = packed_len(end - start, width);
                    if payload.len() != expected {
                        return Err(UsageError::LeafLength {
                            index,
                            expected,
                            got: payload.len(),
                        });
                    }
                    if keccak256(payload) != *digest {
                        return Err(UsageError::LeafDigestMismatch { index });
                    }
                    unpack_range(&mut counts, payload, start, end)?;
                }
            }
        }

        for &(bucket, count) in &self.exceptions {
            counts[bucket_index(bucket)] = count;
        }

        let sum: u64 = counts.iter().map(|&c| u64::from(c)).sum();
        if sum != self.total_issued {
            return Err(UsageError::IssuedMismatch {
                header: self.total_issued,
                sum,
            });
        }

        // Reconstruct the table in its original mode. The sum check above
        // validates the counters in both modes (a checksum for mutable).
        //
        // A mutable table recovered here carries no reserved state: the decoder
        // has the batch id and the allocated slots but not the owner address, so
        // it cannot map a reserved slot to its bucket (that needs the
        // single-owner chunk address, which depends on the owner). The reserved
        // set is installed from the owner when the holder obtains an
        // [`Issuer`](crate::Issuer) through [`Snapshot::issuer`](crate::Snapshot::issuer),
        // which is the only counter-advance path, so issuance on a recovered
        // mutable snapshot is reserved-aware by construction.
        let mutability = if self.mutable {
            Mutability::Mutable
        } else {
            Mutability::Immutable
        };
        // These reconstruct the table and validate the recovered slots through
        // the shared caller-input checks, so a corrupt decoded counter or slot
        // would otherwise surface as the caller-input range variant. Map those to
        // their corruption counterparts: reached from the decode path, they mean
        // the fetched bytes are bad, not a caller input that can be adjusted. A
        // direct `Snapshot::from_parts` caller still gets the caller-input variant.
        let table = UsageTableFor::from_counts(
            self.batch_id,
            self.depth,
            self.bucket_depth,
            counts,
            mutability,
        )
        .map_err(UsageError::into_corruption)?;
        let parts = SnapshotFor::recovered_parts(table, self.sequence, self.slots)
            .map_err(UsageError::into_corruption)?;
        SnapshotFor::from_parts(parts).map_err(UsageError::into_corruption)
    }
}

#[cfg(test)]
mod tests {
    use core::num::NonZeroU8;

    use nectar_primitives::NetworkId;
    use proptest::prelude::*;
    use proptest_arbitrary_interop::arb;

    use super::*;
    use crate::UsageTable;

    /// A deployment whose bucket-depth floor is the format minimum. Mainnet
    /// sets its floor at 16, which the format also caps, so a mainnet snapshot
    /// has exactly 2^16 buckets; the fixtures below exercise the smaller
    /// geometries the format supports for a network that allows them.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct Shallow;

    impl SwarmSpec for Shallow {
        const NETWORK_ID: NetworkId = NetworkId::TESTNET;
        const MIN_BUCKET_DEPTH: NonZeroU8 = NonZeroU8::new(1).unwrap();
    }

    /// A bucket depth `Shallow` accepts but mainnet does not.
    fn shallow(depth: u8) -> BucketDepth<Shallow> {
        BucketDepth::new(depth).unwrap()
    }

    /// The mainnet bucket depth, the only one mainnet and the format agree on.
    fn mainnet() -> BucketDepth {
        BucketDepth::new(16).unwrap()
    }

    /// Reads a big-endian u32 at `offset`, for fixture verification.
    fn read_u32(buf: &[u8], offset: usize) -> u32 {
        u32::from_be_bytes(buf[offset..offset + 4].try_into().unwrap())
    }

    #[test]
    fn bit_packing_round_trips() {
        for width in 1..=32u8 {
            let limit = delta_limit(width);
            let values = [0, 1, limit / 2, limit.saturating_sub(1), limit];
            let mut buf = vec![0u8; packed_len(values.len(), width)];
            for (i, &v) in values.iter().enumerate() {
                write_bits(&mut buf, i * usize::from(width), width, v);
            }
            for (i, &v) in values.iter().enumerate() {
                assert_eq!(
                    read_bits(&buf, i * usize::from(width), width),
                    v,
                    "width {width}"
                );
            }
        }
    }

    #[test]
    fn layout_math() {
        assert_eq!(packed_len(65536, 4), 32768);
        assert_eq!(buckets_per_leaf(4), 8192);
        assert_eq!(leaf_count(65536, 4), 8);
        assert_eq!(leaf_count(65536, 1), 2);
        assert_eq!(leaf_count(65536, 32), 64);
        assert_eq!(leaf_count(65536, 9), 19);
        assert_eq!(leaf_count(1024, 0), 0);
    }

    #[test]
    fn padding_check() {
        assert!(padding_is_zero(&[0b1010_0000], 3));
        assert!(!padding_is_zero(&[0b1011_0000], 3));
        assert!(padding_is_zero(&[0xff], 8));
    }

    #[test]
    fn encode_requires_an_allocated_root() {
        // Encoding with no allocated slot has no root slot to serialize, so the
        // codec refuses it. The public path reaches encoding only through
        // `plan_persist`, which always allocates the root first.
        let table = UsageTable::new(
            BatchId::new([0x42; 32]),
            20,
            mainnet(),
            Mutability::Immutable,
        )
        .unwrap();
        assert_eq!(
            encode(&table, 0, &[]),
            Err(UsageError::Malformed("root slot not allocated")),
        );
    }

    #[test]
    fn flags_bit0_is_mutable_other_bits_rejected() {
        // Build a minimal valid mutable root: width 0, one slot, no exceptions.
        let table =
            UsageTable::new(BatchId::new([0x42; 32]), 20, mainnet(), Mutability::Mutable).unwrap();
        let encoded = encode(&table, 1, &[0]).unwrap();
        let mut root = encoded.root.to_vec();
        assert_eq!(root[38], 0x01, "mutable flag must be set");

        let info = RootInfo::parse(&root).unwrap();
        assert!(info.is_mutable());

        // Any reserved flag bit is rejected.
        for bit in 1..8u8 {
            let mut bad = root.clone();
            bad[38] = 1 << bit;
            assert_eq!(
                RootInfo::parse(&bad),
                Err(UsageError::Malformed("unsupported flags"))
            );
        }

        // Clearing bit 0 yields an immutable snapshot.
        root[38] = 0x00;
        let info = RootInfo::parse(&root).unwrap();
        assert!(!info.is_mutable());
    }

    /// Builds a valid immutable root with depth 12, bucket depth 8 (256 buckets
    /// of capacity 16), a single exception bucket, and one allocated slot. The
    /// encoder selects width 0 with one exception, so the layout is the 66-byte
    /// header, one 8-byte exception (bucket then count), and one 4-byte slot.
    fn root_with_one_exception() -> Vec<u8> {
        let mut counts = vec![0u32; 256];
        // A single full bucket becomes the lone exception; the rest stay at the
        // base so the encoder packs nothing inline.
        counts[5] = 16;
        let table = UsageTableFor::from_counts(
            BatchId::new([0x42; 32]),
            12,
            shallow(8),
            counts,
            Mutability::Immutable,
        )
        .unwrap();
        let root = encode(&table, 1, &[4]).unwrap().root.to_vec();
        // Header(66) + one exception(8) + one slot(4), width 0 so no packed tail.
        assert_eq!(root.len(), ROOT_HEADER_SIZE + 8 + 4);
        // The exception is bucket 5; confirm the offsets the tests corrupt below.
        assert_eq!(read_u32(&root, ROOT_HEADER_SIZE), 5);
        assert_eq!(read_u32(&root, ROOT_HEADER_SIZE + 4), 16);
        assert_eq!(read_u32(&root, ROOT_HEADER_SIZE + 8), 4);
        root
    }

    #[test]
    fn parse_rejects_out_of_range_exception_bucket_as_corruption() {
        let mut root = root_with_one_exception();
        // Push the exception bucket index past the 256-bucket range.
        root[ROOT_HEADER_SIZE..ROOT_HEADER_SIZE + 4].copy_from_slice(&300u32.to_be_bytes());
        let err = RootInfoFor::<Shallow>::parse(&root).unwrap_err();
        assert_eq!(err, UsageError::CorruptBucket { bucket: 300 });
        assert!(err.is_corruption());
        assert!(!err.is_recoverable());
    }

    #[test]
    fn parse_rejects_over_capacity_exception_counter_as_corruption() {
        let mut root = root_with_one_exception();
        // Push the exception counter past the per-bucket capacity of 16.
        root[ROOT_HEADER_SIZE + 4..ROOT_HEADER_SIZE + 8].copy_from_slice(&17u32.to_be_bytes());
        let err = RootInfoFor::<Shallow>::parse(&root).unwrap_err();
        assert_eq!(
            err,
            UsageError::CorruptCounter {
                bucket: 5,
                count: 17,
                capacity: 16,
            }
        );
        assert!(err.is_corruption());
        assert!(!err.is_recoverable());
    }

    #[test]
    fn parse_rejects_invalid_geometry_as_corruption() {
        let mut root = root_with_one_exception();
        // The geometry is read straight from the fetched bytes, so a corrupt
        // bucket-depth byte (here past the supported maximum) is decode
        // corruption, not a caller-input error.
        root[37] = 17;
        let err = RootInfoFor::<Shallow>::parse(&root).unwrap_err();
        assert_eq!(
            err,
            UsageError::CorruptGeometry {
                depth: 12,
                bucket_depth: 17,
            }
        );
        assert!(err.is_corruption());
        assert!(!err.is_recoverable());
    }

    #[test]
    fn parse_rejects_zero_bucket_depth_as_corruption() {
        // A handcrafted root that is valid in every other respect: depth 5,
        // bucket depth 0 (one zero-width bucket), width 0, sequence 1, one
        // allocated slot, no exceptions, no leaves. Before bucket depth 0 was
        // rejected by `validate_geometry`, this payload parsed `Ok` and the
        // recovered snapshot panicked downstream in `calculate_bucket`
        // (`leading >> (32 - 0)` is a shift overflow) on the persist and
        // issue paths.
        let mut root = vec![0u8; ROOT_HEADER_SIZE + 4];
        root[..4].copy_from_slice(&MAGIC);
        root[4..36].copy_from_slice(B256::repeat_byte(0x42).as_slice());
        root[36] = 5; // depth
        root[37] = 0; // bucket_depth: the zero-width bucket under test
        root[40..48].copy_from_slice(&1u64.to_be_bytes()); // sequence
        root[60..62].copy_from_slice(&1u16.to_be_bytes()); // allocated = 1
        // exceptions = 0, leaves = 0, base = 0, slot 0 already zeroed.

        let err = RootInfoFor::<Shallow>::parse(&root).unwrap_err();
        assert_eq!(
            err,
            UsageError::CorruptGeometry {
                depth: 5,
                bucket_depth: 0,
            }
        );
        assert!(err.is_corruption());
        assert!(!err.is_recoverable());
    }

    #[test]
    fn parse_rejects_out_of_range_slot_as_corruption() {
        let mut root = root_with_one_exception();
        // Push the allocated slot to the capacity bound (valid slots are < 16).
        root[ROOT_HEADER_SIZE + 8..ROOT_HEADER_SIZE + 12].copy_from_slice(&16u32.to_be_bytes());
        let err = RootInfoFor::<Shallow>::parse(&root).unwrap_err();
        assert_eq!(
            err,
            UsageError::CorruptSlot {
                slot: 16,
                capacity: 16,
            }
        );
        assert!(err.is_corruption());
        assert!(!err.is_recoverable());
    }

    #[test]
    fn assemble_rejects_over_capacity_decoded_counter_as_corruption() {
        // A table whose deltas span 0..16 packs inline at width 5 (no
        // exceptions): every count is within the capacity of 16.
        let counts: Vec<u32> = (0..256u32).map(|b| b % 17).collect();
        let table = UsageTableFor::from_counts(
            BatchId::new([0x42; 32]),
            12,
            shallow(8),
            counts,
            Mutability::Immutable,
        )
        .unwrap();
        let mut corrupt = encode(&table, 1, &[4]).unwrap().root.to_vec();
        assert_eq!(
            RootInfoFor::<Shallow>::parse(&corrupt)
                .unwrap()
                .leaf_count(),
            0,
            "this geometry inlines the deltas"
        );

        // The packed deltas follow the header(66), the slot(4), and no
        // exceptions, so they begin at offset 70. Force the first delta's high
        // bits so it decodes to 31, above the capacity of 16.
        let packed_start = ROOT_HEADER_SIZE + 4;
        corrupt[packed_start] |= 0b1111_1000;

        let info = RootInfoFor::<Shallow>::parse(&corrupt).unwrap();
        let err = info.assemble::<&[u8]>(&[]).unwrap_err();
        assert_eq!(
            err,
            UsageError::CorruptCounter {
                bucket: 0,
                count: 31,
                capacity: 16,
            }
        );
        assert!(err.is_corruption());
        assert!(!err.is_recoverable());
    }

    #[test]
    fn from_counts_over_capacity_is_caller_input_recoverable() {
        // The same over-capacity condition supplied directly as caller input
        // (not decoded from fetched bytes) stays the recoverable variant: the
        // caller can fix the counts it passed.
        let mut counts = vec![0u32; 256];
        counts[5] = 17; // capacity is 16
        let err = UsageTableFor::from_counts(
            BatchId::new([0x42; 32]),
            12,
            shallow(8),
            counts,
            Mutability::Immutable,
        )
        .unwrap_err();
        assert_eq!(
            err,
            UsageError::CounterOverflow {
                bucket: 5,
                count: 17,
                capacity: 16,
            }
        );
        assert!(err.is_recoverable());
        assert!(!err.is_corruption());
    }

    /// Replay crafted edge inputs through `RootInfo::parse`, the exact entry
    /// point the `usage_snapshot_decode` fuzz target exercises: length
    /// boundaries around the 66-byte header, all-zero/all-0xff payloads, and
    /// magic-prefixed headers probing the geometry (`capacity =
    /// 1 << (depth - bucket_depth)`) and delta-width guards. The fuzz oracle
    /// is "no panic"; `Err` is an acceptable outcome for arbitrary bytes.
    #[test]
    fn usage_snapshot_decode_edge_inputs_do_not_panic() {
        let mut edge_inputs: Vec<Vec<u8>> = vec![
            Vec::new(),
            vec![0x00],
            vec![0xff; ROOT_HEADER_SIZE - 1],
            vec![0x00; ROOT_HEADER_SIZE],
            vec![0xff; ROOT_HEADER_SIZE],
            vec![0xff; MAX_PAYLOAD_SIZE],
        ];
        // Magic-prefixed headers probing the geometry and width guards:
        // (depth, bucket_depth, width) triples around the validation
        // boundaries, including the depth < bucket_depth underflow shape and
        // the depth - bucket_depth = 32 shift-overflow shape.
        for (depth, bucket_depth, width) in [
            (0u8, 0u8, 0u8),
            (15, 16, 0),  // depth < bucket_depth
            (16, 17, 0),  // bucket_depth over MAX_BUCKET_DEPTH
            (47, 16, 0),  // depth - bucket_depth = 31 (max counter bits)
            (48, 16, 0),  // depth - bucket_depth = 32 (must be rejected)
            (20, 16, 32), // width at MAX_WIDTH
            (20, 16, 33), // width over MAX_WIDTH
            (255, 255, 255),
        ] {
            let mut header = vec![0u8; ROOT_HEADER_SIZE];
            header[..4].copy_from_slice(&MAGIC);
            header[36] = depth;
            header[37] = bucket_depth;
            header[39] = width;
            edge_inputs.push(header);
        }
        for data in &edge_inputs {
            let _ = RootInfo::parse(data);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// Valid-by-construction snapshots survive the shared
        /// `snapshot_persist_round_trip` oracle; the property the
        /// `usage_snapshot_roundtrip` fuzz target drives. The generator runs
        /// at `Shallow` so it spans the format's whole bucket-depth range
        /// rather than the single geometry mainnet's floor admits. A refused
        /// snapshot slot (a full immutable bucket, an exhausted capacity-1
        /// ring) is a legitimate skip, bounded by proptest's reject limit.
        #[test]
        fn snapshot_persist_parse_assemble_round_trip(
            snapshot in arb::<crate::SnapshotFor<Shallow>>(),
        ) {
            let outcome = crate::oracles::snapshot_persist_round_trip(snapshot);
            prop_assume!(outcome != Ok(false));
            prop_assert_eq!(outcome, Ok(true));
        }
    }

    /// Replay the committed seed corpus of the `usage_snapshot_decode` fuzz
    /// target (`fuzz/seeds/usage_snapshot_decode/`). Seed intent is pinned by
    /// name: `valid-*` must parse `Ok`, `invalid-*` must stay `Err`. This
    /// keeps the fuzz seeds meaningful on stable without running the fuzzer
    /// itself.
    #[test]
    fn seed_replay_usage_snapshot_decode() {
        let seed_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fuzz/seeds/usage_snapshot_decode");
        let mut replayed = 0usize;
        for entry in std::fs::read_dir(&seed_dir)
            .unwrap_or_else(|e| panic!("seed dir {} must exist: {e}", seed_dir.display()))
        {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            let data = std::fs::read(&path).unwrap();

            let result = RootInfo::parse(&data);

            if name.starts_with("valid-") {
                assert!(
                    result.is_ok(),
                    "seed {name} must parse successfully: {result:?}"
                );
            } else if name.starts_with("invalid-") {
                assert!(result.is_err(), "seed {name} must remain an Err input");
            }
            replayed += 1;
        }
        assert!(
            replayed >= 3,
            "expected at least the 3 curated seeds, found {replayed}"
        );
    }
}
