//! Wire format encoder and decoder for usage snapshots.
//!
//! See the crate documentation and `README.md` for the format specification.

use alloc::vec;
use alloc::vec::Vec;

use alloy_primitives::{B256, keccak256};
use bytes::Bytes;
use nectar_postage::BatchId;

use crate::snapshot::Snapshot;
use crate::table::{UsageTable, validate_geometry};
use crate::{
    MAGIC, MAX_EXCEPTIONS, MAX_PAYLOAD_SIZE, MAX_WIDTH, ROOT_HEADER_SIZE, Result, UsageError,
};

/// The serialized form of a snapshot: one root payload and zero or more leaf
/// payloads. Payload `n` of the snapshot (root is `n = 0`, leaf `i` is
/// `n = i + 1`) belongs in the single-owner chunk with id
/// [`usage_chunk_id`](crate::usage_chunk_id)`(batch_id, n)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Encoded {
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootInfo {
    batch_id: BatchId,
    depth: u8,
    bucket_depth: u8,
    mutable: bool,
    width: u8,
    sequence: u64,
    total_issued: u64,
    base: u32,
    exceptions: Vec<(u32, u32)>,
    slots: Vec<u32>,
    leaves: LeafSection,
}

/// Returns the maximum delta representable in `width` bits.
const fn delta_limit(width: u8) -> u64 {
    if width >= 32 {
        u32::MAX as u64
    } else {
        (1u64 << width) - 1
    }
}

/// Returns the byte length of `buckets` deltas packed at `width` bits.
const fn packed_len(buckets: usize, width: u8) -> usize {
    (buckets * width as usize).div_ceil(8)
}

/// Returns the number of buckets per leaf for a given width (`width > 0`).
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
fn write_bits(buf: &mut [u8], bit_offset: usize, width: u8, value: u64) {
    for i in 0..width as usize {
        let bit = (value >> (width as usize - 1 - i)) & 1;
        if bit != 0 {
            let pos = bit_offset + i;
            buf[pos / 8] |= 1 << (7 - pos % 8);
        }
    }
}

/// Reads `width` bits at `bit_offset`, MSB first.
fn read_bits(buf: &[u8], bit_offset: usize, width: u8) -> u64 {
    let mut value = 0u64;
    for i in 0..width as usize {
        let pos = bit_offset + i;
        let bit = (buf[pos / 8] >> (7 - pos % 8)) & 1;
        value = (value << 1) | u64::from(bit);
    }
    value
}

/// Packs the deltas of buckets `range` into a fresh zero-padded buffer.
///
/// Exception buckets are filled with all one bits; `exceptions` must be
/// sorted ascending by bucket.
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
        .map(|&(bucket, _)| bucket as usize)
        .skip_while(|&b| b < start)
        .peekable();
    for (i, bucket) in (start..end).enumerate() {
        let delta = if except.peek() == Some(&bucket) {
            except.next();
            limit
        } else {
            u64::from(counts[bucket] - base)
        };
        write_bits(&mut out, i * width as usize, width, delta);
    }
    out
}

/// Checks that all bits past `bit_len` in `buf` are zero.
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
fn select_width(counts: &[u32], base: u32, buckets: usize, allocated: usize) -> Option<u8> {
    // histogram[n] counts deltas whose minimal representation is n bits, so
    // the exception count at width w is the histogram tail above w.
    let mut histogram = [0usize; 33];
    for &count in counts {
        let delta = count - base;
        histogram[(32 - delta.leading_zeros()) as usize] += 1;
    }

    let mut best: Option<(u8, usize)> = None;
    for width in 0..=MAX_WIDTH {
        let exceptions: usize = histogram[width as usize + 1..].iter().sum();
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
pub(crate) fn encode(table: &UsageTable, sequence: u64, slots: &[u32]) -> Result<Encoded> {
    if slots.is_empty() {
        return Err(UsageError::Malformed("root slot not allocated"));
    }
    let buckets = table.bucket_count() as usize;
    let counts = table.counts();
    let base = table.min_count();
    let allocated = slots.len();

    // Unreachable for supported geometry: at width 32 there are no
    // exceptions and the leaf table always fits in the root.
    let width = select_width(counts, base, buckets, allocated)
        .ok_or(UsageError::Malformed("no encoding fits the root chunk"))?;

    {
        let limit = delta_limit(width);
        let exceptions: Vec<(u32, u32)> = counts
            .iter()
            .enumerate()
            .filter(|&(_, &count)| u64::from(count - base) > limit)
            .map(|(bucket, &count)| (bucket as u32, count))
            .collect();

        let fixed = ROOT_HEADER_SIZE + 8 * exceptions.len() + 4 * allocated;
        let inline_len = packed_len(buckets, width);
        let (leaves, inline) = if fixed + inline_len <= MAX_PAYLOAD_SIZE {
            (0usize, true)
        } else {
            (leaf_count(buckets, width), false)
        };

        let mut leaf_payloads = Vec::with_capacity(leaves);
        if !inline {
            let per_leaf = buckets_per_leaf(width);
            for i in 0..leaves {
                let start = i * per_leaf;
                let end = (start + per_leaf).min(buckets);
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

        let mut root = Vec::with_capacity(fixed + if inline { inline_len } else { 32 * leaves });
        root.extend_from_slice(&MAGIC);
        root.extend_from_slice(table.batch_id().as_slice());
        root.push(table.depth());
        root.push(table.bucket_depth());
        // flags: bit 0 marks a mutable (ring-cursor) batch.
        root.push(if table.is_mutable() { 1 } else { 0 });
        root.push(width);
        root.extend_from_slice(&sequence.to_be_bytes());
        root.extend_from_slice(&table.total_issued().to_be_bytes());
        root.extend_from_slice(&base.to_be_bytes());
        root.extend_from_slice(&(allocated as u16).to_be_bytes());
        root.extend_from_slice(&(leaves as u16).to_be_bytes());
        root.extend_from_slice(&(exceptions.len() as u16).to_be_bytes());
        for &(bucket, count) in &exceptions {
            root.extend_from_slice(&bucket.to_be_bytes());
            root.extend_from_slice(&count.to_be_bytes());
        }
        for &slot in slots {
            root.extend_from_slice(&slot.to_be_bytes());
        }
        if inline {
            root.extend_from_slice(&pack_range(counts, base, width, &exceptions, 0, buckets));
        } else {
            for payload in &leaf_payloads {
                root.extend_from_slice(keccak256(payload).as_slice());
            }
        }

        Ok(Encoded {
            root: Bytes::from(root),
            leaves: leaf_payloads,
        })
    }
}

fn read_u16(buf: &[u8], offset: usize) -> u16 {
    u16::from_be_bytes(buf[offset..offset + 2].try_into().unwrap())
}

fn read_u32(buf: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(buf[offset..offset + 4].try_into().unwrap())
}

fn read_u64(buf: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes(buf[offset..offset + 8].try_into().unwrap())
}

impl RootInfo {
    /// Parses and structurally validates a root payload.
    pub fn parse(payload: &[u8]) -> Result<Self> {
        if payload.len() < ROOT_HEADER_SIZE {
            return Err(UsageError::PayloadLength {
                expected: ROOT_HEADER_SIZE,
                got: payload.len(),
            });
        }
        if payload[0..4] != MAGIC {
            return Err(UsageError::BadMagic);
        }
        let batch_id = B256::from_slice(&payload[4..36]);
        let depth = payload[36];
        let bucket_depth = payload[37];
        validate_geometry(depth, bucket_depth)?;
        let flags = payload[38];
        // Only bit 0 (mutable) is defined; any other bit set is rejected so a
        // future flag is never silently ignored by an older reader.
        if flags & !0x01 != 0 {
            return Err(UsageError::Malformed("unsupported flags"));
        }
        let mutable = flags & 0x01 == 1;
        let width = payload[39];
        if width > MAX_WIDTH {
            return Err(UsageError::Malformed("delta width exceeds 32"));
        }
        let sequence = read_u64(payload, 40);
        let total_issued = read_u64(payload, 48);
        let base = read_u32(payload, 56);
        let allocated = read_u16(payload, 60) as usize;
        let leaves = read_u16(payload, 62) as usize;
        let exception_count = read_u16(payload, 64) as usize;

        let buckets = 1usize << bucket_depth;
        let capacity = 1u32 << (depth - bucket_depth);

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
            if allocated < leaves + 1 {
                return Err(UsageError::Malformed("missing slots for leaves"));
            }
        }

        let tail = if leaves > 0 {
            32 * leaves
        } else {
            packed_len(buckets, width)
        };
        let expected = ROOT_HEADER_SIZE + 8 * exception_count + 4 * allocated + tail;
        if expected > MAX_PAYLOAD_SIZE {
            return Err(UsageError::Malformed("root larger than a chunk"));
        }
        if payload.len() != expected {
            return Err(UsageError::PayloadLength {
                expected,
                got: payload.len(),
            });
        }

        let mut offset = ROOT_HEADER_SIZE;
        let mut exceptions = Vec::with_capacity(exception_count);
        let mut previous: Option<u32> = None;
        for _ in 0..exception_count {
            let bucket = read_u32(payload, offset);
            let count = read_u32(payload, offset + 4);
            offset += 8;
            if bucket as usize >= buckets {
                return Err(UsageError::InvalidBucket { bucket });
            }
            if previous.is_some_and(|p| p >= bucket) {
                return Err(UsageError::Malformed("exceptions not strictly ascending"));
            }
            if count > capacity {
                return Err(UsageError::CounterOverflow {
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
            let slot = read_u32(payload, offset);
            offset += 4;
            if slot >= capacity {
                return Err(UsageError::InvalidSlot { slot, capacity });
            }
            slots.push(slot);
        }

        let leaves = if leaves > 0 {
            let mut digests = Vec::with_capacity(leaves);
            for _ in 0..leaves {
                digests.push(B256::from_slice(&payload[offset..offset + 32]));
                offset += 32;
            }
            LeafSection::Digests(digests)
        } else {
            let packed = payload[offset..].to_vec();
            if !padding_is_zero(&packed, buckets * width as usize) {
                return Err(UsageError::Malformed("nonzero padding"));
            }
            LeafSection::Inline(packed)
        };

        Ok(Self {
            batch_id,
            depth,
            bucket_depth,
            mutable,
            width,
            sequence,
            total_issued,
            base,
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
    pub const fn bucket_depth(&self) -> u8 {
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
    pub const fn leaf_count(&self) -> u16 {
        match &self.leaves {
            LeafSection::Inline(_) => 0,
            LeafSection::Digests(digests) => digests.len() as u16,
        }
    }

    /// Returns the expected byte length of the zero-based `leaf`, or `None`
    /// if the index is out of range.
    pub fn expected_leaf_len(&self, leaf: u16) -> Option<usize> {
        if (leaf as usize) >= self.leaf_count() as usize {
            return None;
        }
        let buckets = 1usize << self.bucket_depth;
        let per_leaf = buckets_per_leaf(self.width);
        let start = leaf as usize * per_leaf;
        let end = (start + per_leaf).min(buckets);
        Some(packed_len(end - start, self.width))
    }

    /// Verifies the leaf payloads against the root and reconstructs the
    /// snapshot.
    ///
    /// `leaves` must contain exactly [`leaf_count`](Self::leaf_count)
    /// payloads in chunk-index order.
    pub fn assemble<L: AsRef<[u8]>>(self, leaves: &[L]) -> Result<Snapshot> {
        let buckets = 1usize << self.bucket_depth;
        let capacity = 1u32 << (self.depth - self.bucket_depth);
        let width = self.width;

        let mut counts = vec![0u32; buckets];
        // Unpacks the deltas of buckets `start..end`. Exception buckets carry
        // a meaningless filler value; they are skipped here and overlaid with
        // their absolute counts afterwards.
        let unpack_range = |counts: &mut [u32], packed: &[u8], start: usize, end: usize| {
            if !padding_is_zero(packed, (end - start) * width as usize) {
                return Err(UsageError::Malformed("nonzero padding"));
            }
            let mut except = self
                .exceptions
                .iter()
                .map(|&(bucket, _)| bucket as usize)
                .skip_while(|&b| b < start)
                .peekable();
            for (i, count) in counts[start..end].iter_mut().enumerate() {
                let bucket = start + i;
                if except.peek() == Some(&bucket) {
                    except.next();
                    continue;
                }
                let value = u64::from(self.base) + read_bits(packed, i * width as usize, width);
                if value > u64::from(capacity) {
                    return Err(UsageError::CounterOverflow {
                        bucket: bucket as u32,
                        count: value.min(u64::from(u32::MAX)) as u32,
                        capacity,
                    });
                }
                *count = value as u32;
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
                    let payload = payload.as_ref();
                    let start = i * per_leaf;
                    let end = (start + per_leaf).min(buckets);
                    let expected = packed_len(end - start, width);
                    if payload.len() != expected {
                        return Err(UsageError::LeafLength {
                            index: i as u16,
                            expected,
                            got: payload.len(),
                        });
                    }
                    if keccak256(payload) != *digest {
                        return Err(UsageError::LeafDigestMismatch { index: i as u16 });
                    }
                    unpack_range(&mut counts, payload, start, end)?;
                }
            }
        }

        for &(bucket, count) in &self.exceptions {
            counts[bucket as usize] = count;
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
        let table = if self.mutable {
            UsageTable::from_counts_mutable(self.batch_id, self.depth, self.bucket_depth, counts)?
        } else {
            UsageTable::from_counts(self.batch_id, self.depth, self.bucket_depth, counts)?
        };
        Snapshot::from_parts(Snapshot::recovered_parts(table, self.sequence, self.slots)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bit_packing_round_trips() {
        for width in 1..=32u8 {
            let limit = delta_limit(width);
            let values = [0, 1, limit / 2, limit.saturating_sub(1), limit];
            let mut buf = vec![0u8; packed_len(values.len(), width)];
            for (i, &v) in values.iter().enumerate() {
                write_bits(&mut buf, i * width as usize, width, v);
            }
            for (i, &v) in values.iter().enumerate() {
                assert_eq!(
                    read_bits(&buf, i * width as usize, width),
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
    fn flags_bit0_is_mutable_other_bits_rejected() {
        use alloy_primitives::B256;

        // Build a minimal valid mutable root: width 0, one slot, no exceptions.
        let table = UsageTable::new_mutable(B256::repeat_byte(0x42), 20, 16).unwrap();
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
}
