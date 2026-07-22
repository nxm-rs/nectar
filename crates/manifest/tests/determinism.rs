//! History-independence and packing determinism, through the public API: the
//! packed tree is a pure function of its key set, boundaries follow the frozen
//! `CUT_SCALE = 2^53` window, and the worst-case record and directory depth
//! stay within the format's termination bounds.
//!
//! Content addressing and cross-update dedup rest on invariant I6: two nodes
//! with the same keys encode to the same bytes regardless of build order.

use core::ops::Range;
use std::collections::BTreeMap;

use nectar_manifest::{
    Domain, Entry, ForkTable, Format, Node, Prefix, SegmentKind, SegmentWeight, V1, cut, h64,
    segment, spill,
};
use nectar_primitives::{ChunkAddress, ChunkRef};
use proptest::prelude::*;

mod common;
use common::{TestResult, ensure, ensure_eq};

const fn ref32(byte: u8) -> ChunkRef {
    ChunkRef::new(ChunkAddress::new([byte; 32]))
}

/// A prefix within a proptest body, mapping the length bound to a test failure.
fn to_prefix(bytes: &[u8]) -> Result<Prefix, TestCaseError> {
    Prefix::try_from(bytes).map_err(|e| TestCaseError::fail(e.to_string()))
}

/// A segment weight within a proptest body, mapping the budget bound to a test
/// failure.
fn to_weight(weight: usize) -> Result<SegmentWeight, TestCaseError> {
    SegmentWeight::new(weight).map_err(|e| TestCaseError::fail(e.to_string()))
}

/// Collapse a raw fork list to one entry per first byte, in ascending key
/// order: the canonical fork order a radix-256 table imposes.
fn by_first_byte<V: Clone>(raw: Vec<(Vec<u8>, V)>) -> Vec<(Vec<u8>, V)> {
    let mut map: BTreeMap<u8, (Vec<u8>, V)> = BTreeMap::new();
    for (prefix, value) in raw {
        if let Some(&first) = prefix.first() {
            map.entry(first).or_insert((prefix, value));
        }
    }
    map.into_values().collect()
}

/// The forks of a weighted set in fork order, ready for [`segment`].
fn to_forks(forks: &[(Vec<u8>, usize)]) -> Result<Vec<(Prefix, SegmentWeight)>, TestCaseError> {
    let ordered = by_first_byte(forks.to_vec());
    let mut out = Vec::with_capacity(ordered.len());
    for (prefix, weight) in &ordered {
        out.push((to_prefix(prefix)?, to_weight(*weight)?));
    }
    Ok(out)
}

/// The forks of a weighted set in fork order, canonicalized through a real
/// [`ForkTable`] built in the given insert order rather than a private sort:
/// the wire order is then the library's radix order, so a table that leaked
/// its insert order would show up as a segmentation difference.
fn forks_through_table(
    order: &[(Vec<u8>, usize)],
) -> Result<Vec<(Prefix, SegmentWeight)>, TestCaseError> {
    let mut table = ForkTable::new();
    let mut weights = BTreeMap::new();
    for (prefix, weight) in order {
        let Some(&first) = prefix.first() else {
            continue;
        };
        weights.insert(first, *weight);
        table
            .insert(to_prefix(prefix)?, Entry::from(ref32(0)).into(), None)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
    }
    let mut out = Vec::with_capacity(table.len());
    for (first, record) in table.iter() {
        let tail = record.tail().as_bytes();
        let mut full = Vec::with_capacity(tail.len().saturating_add(1));
        full.push(first);
        full.extend_from_slice(tail);
        let weight = weights.get(&first).copied().unwrap_or_default();
        out.push((to_prefix(&full)?, to_weight(weight)?));
    }
    Ok(out)
}

/// Encode a node whose fork table is built by inserting `forks` in the given
/// order; distinct first bytes mean no insert ever replaces another.
fn encode_in_order(forks: &[(Vec<u8>, u8)]) -> Result<Vec<u8>, TestCaseError> {
    let mut table = ForkTable::new();
    for (prefix, fill) in forks {
        table
            .insert(to_prefix(prefix)?, Entry::from(ref32(*fill)).into(), None)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
    }
    Node::new(None, table)
        .encode()
        .map_err(|e| TestCaseError::fail(e.to_string()))
}

/// A set of forks keyed on distinct first bytes, small enough that the encoded
/// node stays within `F::BUDGET`.
fn small_fork_set() -> impl Strategy<Value = Vec<(Vec<u8>, u8)>> {
    prop::collection::vec(
        (prop::collection::vec(any::<u8>(), 1..=4), any::<u8>()),
        0..=32,
    )
    .prop_map(by_first_byte)
}

/// A wide weighted fork set: up to a full radix-256 table of distinct first
/// bytes, each with a sub-budget weight.
fn weighted_fork_set() -> impl Strategy<Value = Vec<(Vec<u8>, usize)>> {
    prop::collection::vec(
        (
            prop::collection::vec(any::<u8>(), 1..=4),
            1usize..=V1::CAP_FORK,
        ),
        0..=256,
    )
    .prop_map(by_first_byte)
}

/// Pair a fork set with a shuffled copy, so the two carry the same keys in
/// different build orders.
fn set_and_shuffle<T: Clone + core::fmt::Debug>(
    base: impl Strategy<Value = Vec<T>>,
) -> impl Strategy<Value = (Vec<T>, Vec<T>)> {
    base.prop_flat_map(|forks| (Just(forks.clone()), Just(forks).prop_shuffle()))
}

proptest! {
    // Invariant I6: the packed bytes are a pure function of the key set. Two
    // fork tables built from the same keys in different insert orders encode to
    // byte-identical chunks, so content addresses agree and updates dedup.
    #[test]
    fn packed_bytes_are_history_independent(
        (base, shuffled) in set_and_shuffle(small_fork_set()),
    ) {
        let first = encode_in_order(&base)?;
        let second = encode_in_order(&shuffled)?;
        prop_assert_eq!(first, second);
    }

    // A leaf boundary is a pure function of the fork-relative key set. The two
    // build orders reach the partition through a real fork table, so the order
    // is the library's, not the test's; the ranges must also tile the run,
    // contiguous, non-empty and covering, so an off-by-one in the partitioner
    // surfaces here rather than as a silent gap or overlap.
    #[test]
    fn segmentation_is_a_pure_function_of_the_key_set(
        (base, shuffled) in set_and_shuffle(weighted_fork_set()),
    ) {
        let ordered = forks_through_table(&base)?;
        let from_base = segment::<V1>(&ordered, SegmentKind::Leaf);
        let from_shuffled = segment::<V1>(&forks_through_table(&shuffled)?, SegmentKind::Leaf);
        prop_assert_eq!(&from_base, &from_shuffled);

        let mut next = 0usize;
        for range in &from_base {
            prop_assert_eq!(range.start, next);
            prop_assert!(range.start < range.end);
            next = range.end;
        }
        prop_assert_eq!(next, ordered.len());
    }

    // Below SEG_TARGET a cut is exactly the scaled-window comparison against
    // 2^53; at or above it the weight alone forces one. The predicate reads
    // nothing but the fork's own prefix and weight, so it is deterministic.
    #[test]
    fn boundary_matches_the_scaled_window(
        prefix in prop::collection::vec(any::<u8>(), 0..=8),
        weight in 0usize..=V1::SEG_TARGET,
    ) {
        let cut_now = cut::<V1>(&prefix, weight);
        prop_assert_eq!(cut_now, cut::<V1>(&prefix, weight));
        if weight >= V1::SEG_TARGET {
            prop_assert!(cut_now);
        } else {
            let scaled = u64::try_from(weight)
                .ok()
                .and_then(|w| w.checked_mul(V1::CUT_SCALE))
                .ok_or_else(|| TestCaseError::fail("scaled window overflowed"))?;
            prop_assert_eq!(cut_now, h64(&prefix) < scaled);
        }
    }

    // Re-rooting stability: because each cut is keyed on the fork alone, a run
    // that reopens on a fresh boundary segments identically wherever it sits.
    // Prefixing a self-contained leader (its weight alone forces a cut) shifts
    // the run's boundaries by exactly one and disturbs none of them.
    #[test]
    fn content_cuts_are_position_independent(run in weighted_fork_set()) {
        let forks = to_forks(&run)?;
        let base = segment::<V1>(&forks, SegmentKind::Leaf);

        let leader = (to_prefix(&[0x00])?, to_weight(V1::SEG_TARGET)?);
        let mut prefixed = vec![leader];
        prefixed.extend(forks);
        let shifted = segment::<V1>(&prefixed, SegmentKind::Leaf);

        let mut expected: Vec<Range<usize>> = Vec::with_capacity(base.len().saturating_add(1));
        expected.push(0..1);
        for range in &base {
            expected.push(range.start.saturating_add(1)..range.end.saturating_add(1));
        }
        prop_assert_eq!(shifted, expected);
    }

    // A spilled fork table is never deeper than two directory levels, and one
    // top directory node routes every sub-directory it produces.
    #[test]
    fn spill_directory_depth_never_exceeds_two(forks in weighted_fork_set()) {
        let dir = spill::<V1>(&to_forks(&forks)?, Domain::Plain);
        prop_assert!(dir.depth() <= 2);
        // One directory record is its index slot, flags, prefix-length byte and
        // a single plain reference.
        let route = 5usize.saturating_add(ChunkRef::SIZE);
        let top = dir.dirs().len().saturating_mul(route);
        prop_assert!(top <= V1::CAP_DIR);
    }
}

// The frozen boundary anchors: CUT_SCALE is 2^53 and divides the 64-bit hash
// space by SEG_TARGET exactly, so the window product never overflows a u64.
#[test]
fn cut_scale_is_frozen() -> TestResult {
    ensure_eq(V1::CUT_SCALE, 1u64 << 53, "CUT_SCALE")?;
    let seg_target = u128::try_from(V1::SEG_TARGET)?;
    let product = u128::from(V1::CUT_SCALE)
        .checked_mul(seg_target)
        .ok_or("window product overflowed")?;
    ensure_eq(product, 1u128 << 64, "CUT_SCALE * SEG_TARGET")?;
    Ok(())
}

// Termination at V1: the widest fork record fits a leaf segment with a margin
// wider than the cut-suppression window, so packing always makes progress.
#[test]
fn termination_bounds_hold_at_v1() -> TestResult {
    // The worst record by the codec's layout: index slot (key byte + u16
    // offset), fork flags, prefix-length byte, the longest tail, the largest
    // inline entry, the largest embedded child, and the largest metadata block.
    let slot = 3usize;
    let header = slot.saturating_add(2);
    let tail = V1::PLEN_MAX.saturating_sub(1);
    let entry = V1::VINLINE_MAX.saturating_add(1);
    let child = V1::INLINE_MAX.saturating_add(2);
    let metadata = V1::META_MAX.saturating_add(2);
    let worst = header
        .saturating_add(tail)
        .saturating_add(entry)
        .saturating_add(child)
        .saturating_add(metadata);
    ensure_eq(worst, 2952, "worst fork record")?;
    ensure(
        worst <= V1::CAP_FORK,
        "the worst record fits a leaf segment",
    )?;

    let margin = V1::CAP_FORK.saturating_sub(worst);
    ensure_eq(margin, 1139, "forced-cut margin")?;
    ensure(
        margin >= V1::SEG_MIN,
        "the margin covers the suppression window",
    )?;
    Ok(())
}

// The pathological table: a full radix-256 node of single-byte forks, each
// heavy enough to force its own leaf. It splits across exactly two levels, and
// one top directory still routes every group.
#[test]
fn spill_of_a_full_radix_table_stays_depth_two() -> TestResult {
    let mut forks = Vec::with_capacity(V1::FORKS_MAX);
    for first in 0u8..=u8::MAX {
        forks.push((
            Prefix::try_from(&[first][..])?,
            SegmentWeight::new(V1::SEG_TARGET)?,
        ));
    }
    let dir = spill::<V1>(&forks, Domain::Plain);
    ensure_eq(dir.leaves().len(), V1::FORKS_MAX, "one leaf per fork")?;
    ensure_eq(dir.depth(), 2, "a full table needs two levels")?;

    let route = 5usize.saturating_add(ChunkRef::SIZE);
    let top = dir.dirs().len().saturating_mul(route);
    ensure(top <= V1::CAP_DIR, "one top directory routes every group")?;
    Ok(())
}
