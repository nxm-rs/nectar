//! Content-defined packing: the deterministic, history-independent shape of
//! the trie.
//!
//! Every parameter comes from `F`. A boundary is a pure function of content,
//! keyed on the fork-relative prefix, never the value bytes or the
//! accumulated root path, so an insert disturbs `O(1)` boundaries and
//! re-rooting a subtree does not churn its cuts. The forced caps and the
//! single-chunk-node invariant keep every produced node within one chunk.

use core::mem::size_of;
use core::ops::Range;

use alloy_primitives::keccak256;
use nectar_primitives::{ChunkRef, EncryptedChunkRef};

use crate::bounded::{Prefix, SegmentWeight};
use crate::count::MAX_WIRE_BYTES;
use crate::fork::Child;
use crate::format::Format;
use crate::value::Entry;

/// Bytes an edge starting at `consumed` may span before the next forced cut.
///
/// The forced cap is anchored to the absolute key offset, not to the edge's
/// own start, so splitting or merging an edge never moves a boundary below it:
/// a re-rooted run keeps every cut a build would place, and the subtree under
/// the first shifted segment stays byte-identical. Always `1..=F::PLEN_MAX`.
///
/// Both the builder and `apply` cut through here, so the two cannot drift.
pub(crate) const fn cut_allowance<F: Format>(consumed: usize) -> usize {
    match consumed.checked_rem(F::PLEN_MAX) {
        Some(spent) => F::PLEN_MAX.saturating_sub(spent),
        // A zero cap admits no edge byte at all; the format forbids it.
        None => F::PLEN_MAX,
    }
}

/// The encryption regime of a subtree: plaintext 32-byte references, or
/// encrypted 64-byte references carrying in-band keys.
///
/// Embedding never crosses the domain, so an encrypted child never inlines
/// into a plaintext parent.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Domain {
    /// Plaintext subtree: 32-byte references.
    Plain,
    /// Encrypted subtree: 64-byte references carrying in-band keys.
    Encrypted,
}

impl Domain {
    /// The domain a resolved entry reference belongs to: plaintext for a
    /// 32-byte reference, encrypted for a key-carrying 64-byte reference.
    /// `None` for inline bytes, which are not a reference and so have no
    /// independent domain.
    #[must_use]
    pub const fn of_entry<F: Format>(entry: &Entry<F>) -> Option<Self> {
        match entry {
            Entry::Ref32(_) => Some(Self::Plain),
            Entry::Ref64(_) => Some(Self::Encrypted),
            Entry::Inline(_) => None,
        }
    }

    /// The domain a resolved child reference belongs to: plaintext for a
    /// 32-byte reference, encrypted for a key-carrying 64-byte reference.
    /// `None` for an embedded subtree, which carries no reference and inherits
    /// its parent's domain.
    #[must_use]
    pub const fn of_child<F: Format>(child: &Child<F>) -> Option<Self> {
        match child {
            Child::Ref32(_) => Some(Self::Plain),
            Child::Ref64(_) => Some(Self::Encrypted),
            Child::Embedded(_) => None,
        }
    }
}

/// Which capacity a segment run is held to: a leaf run of fork records, or a
/// directory run of segment references.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SegmentKind {
    /// A leaf segment of fork records, capped at `F::CAP_FORK`.
    Leaf,
    /// A directory segment of segment references, capped at `F::CAP_DIR`.
    Directory,
}

impl SegmentKind {
    /// The forced-cut capacity this run is held to, taken from `F`.
    const fn cap<F: Format>(self) -> usize {
        match self {
            Self::Leaf => F::CAP_FORK,
            Self::Directory => F::CAP_DIR,
        }
    }
}

/// The boundary hash: the first eight bytes of `keccak256(prefix)`, read
/// little-endian.
///
/// Keyed on the fork-relative prefix alone, so a cut is a pure function of
/// content, independent of insertion order and of the path the subtree hangs
/// under.
#[must_use]
pub fn h64(prefix: &[u8]) -> u64 {
    let [b0, b1, b2, b3, b4, b5, b6, b7, ..] = keccak256(prefix).0;
    u64::from_le_bytes([b0, b1, b2, b3, b4, b5, b6, b7])
}

/// The child-local embedding predicate: a child inlines into its parent iff
/// its flat body fits `F::INLINE_MAX` and shares the parent's encryption
/// domain.
///
/// The size test reads nothing but the child, so the decision is stable under
/// re-rooting; the domain test keeps an encrypted child out of a plaintext
/// parent.
#[must_use]
pub fn embed<F: Format>(flat_body_len: usize, parent: Domain, child: Domain) -> bool {
    flat_body_len <= F::INLINE_MAX && parent == child
}

/// The content-cut predicate for one fork: a boundary falls after it when its
/// weight alone reaches `F::SEG_TARGET`, or its prefix hash lands in the
/// target-scaled window `[0, weight * F::CUT_SCALE)`.
///
/// The product never overflows: the window is consulted only below the
/// target, where `weight * F::CUT_SCALE < F::SEG_TARGET * F::CUT_SCALE ==
/// 2^64`.
#[must_use]
pub fn cut<F: Format>(prefix: &[u8], weight: usize) -> bool {
    if weight >= F::SEG_TARGET {
        return true;
    }
    u64::try_from(weight).map_or(true, |w| h64(prefix) < w.saturating_mul(F::CUT_SCALE))
}

/// Partition a weighted, fork-order sequence into segments by the running
/// [`cut`] predicate, suppressing cuts below `F::SEG_MIN` and forcing one
/// before any item that would overrun `cap`.
///
/// Returns index ranges into the input, contiguous and covering. Each item is
/// its fork-relative prefix (the hash key) and its packing weight.
fn partition<'a, F: Format>(
    items: impl Iterator<Item = (&'a [u8], usize)>,
    cap: usize,
) -> Vec<Range<usize>> {
    let mut segments = Vec::new();
    let mut start = 0usize;
    let mut end = 0usize;
    let mut curw = 0usize;
    for (prefix, weight) in items {
        // Forced cut: close the open run before an item that would overrun the
        // capacity, so the item opens a fresh run.
        if curw > 0 && curw.saturating_add(weight) > cap {
            segments.push(start..end);
            start = end;
            curw = 0;
        }
        curw = curw.saturating_add(weight);
        end = end.saturating_add(1);
        // Content cut, suppressed until the run reaches SEG_MIN.
        if curw >= F::SEG_MIN && cut::<F>(prefix, weight) {
            segments.push(start..end);
            start = end;
            curw = 0;
        }
    }
    if start < end {
        segments.push(start..end);
    }
    segments
}

/// Segment a fork run into index ranges over `forks`, held to `kind`'s
/// capacity.
///
/// The partition is a pure function of the fork-relative prefixes and their
/// weights, so it is independent of insertion history.
#[must_use]
pub fn segment<F: Format>(
    forks: &[(Prefix<F>, SegmentWeight<F>)],
    kind: SegmentKind,
) -> Vec<Range<usize>> {
    partition::<F>(
        forks
            .iter()
            .map(|(prefix, weight)| (prefix.as_bytes(), weight.get())),
        kind.cap::<F>(),
    )
}

/// A <= depth-2 segment directory for a spilled fork table: the leaf segments,
/// and the directory nodes that route to them.
///
/// `leaves` partitions the input forks; each leaf is one sub-node chunk.
/// `dirs` partitions the leaves; each group is one directory node. A single
/// group routes straight to its leaves (depth one); several groups sit under
/// a top directory (depth two). Depth never exceeds two: a directory entry is
/// a few bytes, so one directory node under `F::CAP_DIR` reaches every leaf a
/// `F::FORKS_MAX`-wide table can hold once split across two levels.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Directory {
    leaves: Vec<Range<usize>>,
    dirs: Vec<Range<usize>>,
}

impl Directory {
    /// The leaf segments in fork order, each a sub-node chunk.
    #[must_use]
    pub fn leaves(&self) -> &[Range<usize>] {
        &self.leaves
    }

    /// The directory groups over the leaves, each a directory node.
    #[must_use]
    pub fn dirs(&self) -> &[Range<usize>] {
        &self.dirs
    }

    /// Directory depth: one when a single directory node reaches every leaf,
    /// two when the leaves span several directory nodes under a top directory.
    #[must_use]
    pub const fn depth(&self) -> usize {
        if self.dirs.len() > 1 { 2 } else { 1 }
    }
}

/// The packing weight of one directory fork: it routes a single first byte,
/// with no tail, to one segment child, so its record is the flags and
/// prefix-length bytes plus one reference of the domain's width, behind its
/// fork-table index slot. A descriptor also trails a `seg_count`; its
/// worst-case width is charged so a directory stays within one chunk by
/// construction, as the leaf path charges the count on its records.
const fn dir_entry_weight(domain: Domain) -> usize {
    let reference = match domain {
        Domain::Plain => ChunkRef::SIZE,
        Domain::Encrypted => EncryptedChunkRef::SIZE,
    };
    let count = MAX_WIRE_BYTES;
    let slot = size_of::<u8>().saturating_add(size_of::<u16>());
    slot.saturating_add(size_of::<u8>()) // fflags
        .saturating_add(size_of::<u8>()) // plen (routes by the first byte)
        .saturating_add(reference)
        .saturating_add(count)
}

/// Spill an oversized fork table into a <= depth-2 segment directory.
///
/// The forks are cut into leaf segments; the leaves are then cut again, each
/// keyed on its first fork's prefix, into directory nodes. Both levels use
/// the same content-defined boundary, so the whole structure stays a pure
/// function of content.
#[must_use]
pub fn spill<F: Format>(forks: &[(Prefix<F>, SegmentWeight<F>)], domain: Domain) -> Directory {
    let leaves = segment::<F>(forks, SegmentKind::Leaf);
    let weight = dir_entry_weight(domain);
    let dirs = partition::<F>(
        leaves.iter().filter_map(|leaf| {
            forks
                .get(leaf.start)
                .map(|(prefix, _)| (prefix.as_bytes(), weight))
        }),
        SegmentKind::Directory.cap::<F>(),
    );
    Directory { leaves, dirs }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{V1, V1Read};

    // The directory packing weight must cover the widest descriptor its segment
    // chunk can actually carry, including the worst-case trailing seg_count,
    // so a spilled directory stays within one chunk by construction.
    #[test]
    fn the_directory_weight_covers_the_widest_descriptor() {
        // The on-wire descriptor: its first-key byte, the routed reference and
        // a seg_count of up to MAX_WIRE_BYTES.
        let widest = size_of::<u8>()
            .saturating_add(ChunkRef::SIZE)
            .saturating_add(MAX_WIRE_BYTES);
        assert!(widest <= dir_entry_weight(Domain::Plain));
    }

    // The eight worked forks a..h with the spec's weights and hash-cut bits.
    const ROWS: [(u8, u64, bool); 8] = [
        (b'a', 207, true),
        (b'b', 707, false),
        (b'c', 307, true),
        (b'd', 1007, false),
        (b'e', 1031, false),
        (b'f', 807, false),
        (b'g', 104, false),
        (b'h', 1150, false),
    ];

    fn worked_forks() -> Vec<(Prefix, SegmentWeight)> {
        ROWS.into_iter()
            .map(|(key, w, _)| {
                (
                    Prefix::try_from(&[key][..]).unwrap(),
                    SegmentWeight::new(usize::try_from(w).unwrap()).unwrap(),
                )
            })
            .collect()
    }

    #[test]
    fn h64_reads_the_first_eight_digest_bytes_little_endian() {
        assert_eq!(h64(b""), 0x3c23_f786_0146_d2c5);
        assert_eq!(h64(b"index.html"), 0xb4c3_763c_9ea4_174d);
    }

    #[test]
    fn cut_is_the_hash_window_below_target_and_forced_at_it() {
        // Below SEG_TARGET the window decides; g cuts at w = 105 but not 104.
        assert!(!cut::<V1>(b"g", 104));
        assert!(cut::<V1>(b"g", 105));
        // At or above SEG_TARGET the weight alone forces a cut.
        assert!(cut::<V1>(b"g", V1::SEG_TARGET));
        // The per-row bits match the pure hash comparison below the target.
        for (key, w, hash_cut) in ROWS {
            assert_eq!(cut::<V1>(&[key], usize::try_from(w).unwrap()), hash_cut);
        }
    }

    #[test]
    fn segment_reproduces_the_worked_leaf_partition() {
        let forks = worked_forks();
        let ranges = segment::<V1>(&forks, SegmentKind::Leaf);
        assert_eq!(ranges, vec![0..3, 3..7, 7..8]);
    }

    #[test]
    fn segmentation_is_history_independent_after_a_boundary() {
        // A cut is keyed on each fork's own prefix and weight, so a run that
        // opens on a fresh boundary segments the same wherever it sits.
        // Prefixing a self-contained segment (its weight alone forces a cut,
        // resetting the accumulator) leaves the worked run's internal
        // boundaries untouched, only shifted by one.
        let forks = worked_forks();
        let base = segment::<V1>(&forks, SegmentKind::Leaf);

        let lead = (
            Prefix::try_from(&[0x00u8][..]).unwrap(),
            SegmentWeight::new(V1::SEG_TARGET).unwrap(),
        );
        let mut prefixed = vec![lead];
        prefixed.extend(forks);
        let shifted = segment::<V1>(&prefixed, SegmentKind::Leaf);

        let expected: Vec<_> = core::iter::once(0..1)
            .chain(base.iter().map(|r| r.start + 1..r.end + 1))
            .collect();
        assert_eq!(shifted, expected);
    }

    #[test]
    fn reference_domains_gate_cross_boundary_embedding() {
        use nectar_primitives::{ChunkAddress, ChunkRef, EncryptedChunkRef, EncryptionKey};

        let addr = ChunkAddress::new([7; 32]);
        let plain = Entry::<V1>::from(ChunkRef::new(addr));
        let encrypted =
            Entry::<V1>::from(EncryptedChunkRef::new(addr, EncryptionKey::from([9; 32])));

        let plain_dom = Domain::of_entry(&plain).unwrap();
        let enc_dom = Domain::of_entry(&encrypted).unwrap();
        assert_eq!(plain_dom, Domain::Plain);
        assert_eq!(enc_dom, Domain::Encrypted);
        // Inline bytes carry no reference, so no domain.
        let inline = Entry::<V1>::inline(bytes::Bytes::from_static(b"v")).unwrap();
        assert_eq!(Domain::of_entry(&inline), None);

        let plain_child = Child::<V1>::from(ChunkRef::new(addr));
        let enc_child =
            Child::<V1>::from(EncryptedChunkRef::new(addr, EncryptionKey::from([9; 32])));
        assert_eq!(Domain::of_child(&plain_child), Some(Domain::Plain));
        assert_eq!(Domain::of_child(&enc_child), Some(Domain::Encrypted));

        // An encrypted child never inlines into a plaintext parent, however
        // small: the boundary is structural, not a size decision.
        assert!(!embed::<V1>(1, plain_dom, enc_dom));
        assert!(embed::<V1>(1, enc_dom, enc_dom));
    }

    #[test]
    fn embed_gates_on_inline_max_and_domain() {
        assert!(embed::<V1>(V1::INLINE_MAX, Domain::Plain, Domain::Plain));
        assert!(!embed::<V1>(
            V1::INLINE_MAX + 1,
            Domain::Plain,
            Domain::Plain
        ));
        assert!(!embed::<V1>(1, Domain::Plain, Domain::Encrypted));
        assert!(embed::<V1>(1, Domain::Encrypted, Domain::Encrypted));
    }

    // A subtree sized in the window between the two budgets is referenced under
    // V1 but embedded under the read profile: the direct source of the read
    // profile's fewer-fetches trade.
    #[test]
    fn the_read_profile_embeds_where_v1_references() {
        for len in [V1::INLINE_MAX + 1, V1Read::INLINE_MAX] {
            assert!(!embed::<V1>(len, Domain::Plain, Domain::Plain));
            assert!(embed::<V1Read>(len, Domain::Plain, Domain::Plain));
        }
        // The read profile does not embed across the domain boundary either.
        assert!(!embed::<V1Read>(1, Domain::Plain, Domain::Encrypted));
        // Nor beyond its own, larger budget.
        assert!(!embed::<V1Read>(
            V1Read::INLINE_MAX + 1,
            Domain::Plain,
            Domain::Plain
        ));
    }

    #[test]
    fn spill_of_a_small_table_is_a_single_directory() {
        let forks = worked_forks();
        let dir = spill::<V1>(&forks, Domain::Plain);
        assert_eq!(dir.leaves(), &[0..3, 3..7, 7..8]);
        assert_eq!(dir.depth(), 1);
        assert_eq!(dir.dirs(), core::slice::from_ref(&(0..3)));
    }

    #[test]
    fn spill_escalates_to_depth_two_when_the_leaves_overflow_one_directory() {
        // Many single-fork leaves overrun one directory node, forcing a top
        // directory over sub-directories.
        let forks: Vec<(Prefix, SegmentWeight)> = (0u8..200)
            .map(|first| {
                (
                    Prefix::try_from(&[first][..]).unwrap(),
                    SegmentWeight::new(V1::SEG_TARGET).unwrap(),
                )
            })
            .collect();
        let dir = spill::<V1>(&forks, Domain::Plain);
        assert_eq!(dir.leaves().len(), 200);
        assert_eq!(dir.depth(), 2);
    }
}
