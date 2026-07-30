//! Sealed format-version carrier for the manifest wire format.

use core::fmt::Debug;
use core::hash::Hash;

use nectar_primitives::DEFAULT_BODY_SIZE;

use crate::count::MAX_WIRE_BYTES;

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::V1 {}
    impl Sealed for super::V1Read {}
}

/// Frozen layout parameters of one manifest wire format version, carried as
/// associated consts on a zero-sized marker type.
///
/// Sealed: retuning any parameter is a new version implemented here, never a
/// runtime knob. Generic code takes `F: Format`; public types default
/// `F = V1`. The supertraits make format markers inert tokens, so containers
/// derive standard impls without manual bounds.
pub trait Format:
    sealed::Sealed + Copy + Debug + Ord + Hash + Send + Sync + Unpin + 'static
{
    /// In-payload format marker (ASCII `m`), the first payload byte. Shared
    /// by every version; readers dispatch on the `(MAGIC, VERSION)` pair.
    const MAGIC: u8 = b'm';

    /// Format version, the second payload byte. Pins every constant below;
    /// readers reject unknown versions rather than forward-parse.
    const VERSION: u8;

    /// The two payload bytes preceding the node body: `MAGIC || VERSION`.
    const PREAMBLE: [u8; 2] = [Self::MAGIC, Self::VERSION];

    /// Folder-view path separator (ASCII `/`). The byte the folder and website
    /// views split keys into path segments on; a view-layer interpretation,
    /// never stored in the trie.
    const SEPARATOR: u8 = b'/';

    /// Max node body bytes: the chunk body minus [`Self::PREAMBLE`].
    const BUDGET: usize;

    /// Max fork prefix length in bytes.
    const PLEN_MAX: usize;

    /// Max inline value length in bytes.
    const VINLINE_MAX: usize;

    /// Max encoded metadata length per meta block, in bytes.
    const META_MAX: usize;

    /// Max custom metadata key length in bytes.
    const CKEY_MAX: usize;

    /// Max forks per node: radix-256, one fork per distinct first byte.
    const FORKS_MAX: usize;

    /// Max child body size, in bytes, eligible for embedding in its parent's
    /// chunk. Child-local: the predicate reads nothing but the child.
    const INLINE_MAX: usize;

    /// Expected segment weight in bytes: content-defined cuts arrive at this
    /// rate in expectation regardless of the record-size mix.
    const SEG_TARGET: usize;

    /// Accumulated segment weight below which content cuts are suppressed.
    const SEG_MIN: usize;

    /// Leaf-segment weight capacity.
    const CAP_FORK: usize;

    /// Directory-segment weight capacity.
    const CAP_DIR: usize;

    /// Content-cut threshold scale: a cut falls where the prefix hash is
    /// below `weight * CUT_SCALE`. Equals `2^64 / SEG_TARGET`, so the
    /// product fits u64 for every sub-target weight. Typed `u64`, not
    /// `usize`: the value exceeds a 32-bit `usize` on wasm32.
    const CUT_SCALE: u64;

    /// Ordered-scan read-ahead window: the most trie-node fetches a cursor
    /// keeps in flight at once while prefetching the covering frontier. A
    /// reader tuning bounded by memory, not a wire parameter; the sliding
    /// window keeps peak retained state O(depth) at this fetch count.
    const READ_AHEAD: usize = 16;

    /// Domain-separation tag for the deterministic per-reference key
    /// derivation `keccak256(DERIVE_TAG || secret || plaintext)`. Frozen per
    /// version so encrypting the same plaintext under the same secret always
    /// yields the same key, ciphertext and address, keeping canonical bytes
    /// and cross-build dedup intact for encrypted trees.
    const DERIVE_TAG: &'static [u8];
}

/// The frozen `tag_version 0x01` parameter set.
///
/// The node grammar carries order-statistic subtree counts: every
/// referenced-child fork ends with a trailing `child_count` and every
/// segment-directory descriptor carries a `seg_count`, so navigation by rank
/// costs O(depth) instead of O(window). Each count is a pure function of the
/// key set, so canonical bytes and bit-exact `apply` hold unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct V1;

impl Format for V1 {
    const VERSION: u8 = 0x01;
    const BUDGET: usize = 4094;
    const PLEN_MAX: usize = 255;
    const VINLINE_MAX: usize = 128;
    const META_MAX: usize = 1024;
    const CKEY_MAX: usize = 64;
    const FORKS_MAX: usize = 256;
    const INLINE_MAX: usize = 1536;
    const SEG_TARGET: usize = 2048;
    const SEG_MIN: usize = 512;
    const CAP_FORK: usize = 4091;
    const CAP_DIR: usize = 4090;
    const CUT_SCALE: u64 = 9_007_199_254_740_992;
    const DERIVE_TAG: &'static [u8] = b"mantaray/1.0/key";
}

/// The read-optimized `tag_version 0x02` parameter set: the frozen `V1` layout
/// with a heavier embedding budget.
///
/// A larger `INLINE_MAX` inlines heavier subtrees into their parent, so a
/// range or listing window resolves through fewer referenced hops. The honest
/// cost is single-update write-amplification: editing any key beneath an
/// embedded subtree rewrites the larger parent chunk. A distinct wire version,
/// so a manifest built here is byte-distinct from a `V1` one and only a
/// `V1Read` reader accepts its heavier embeds; frozen `V1` is untouched. Every
/// termination bound `V1` carries holds here, asserted below.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct V1Read;

impl Format for V1Read {
    const VERSION: u8 = 0x02;
    const BUDGET: usize = V1::BUDGET;
    const PLEN_MAX: usize = V1::PLEN_MAX;
    const VINLINE_MAX: usize = V1::VINLINE_MAX;
    const META_MAX: usize = V1::META_MAX;
    const CKEY_MAX: usize = V1::CKEY_MAX;
    const FORKS_MAX: usize = V1::FORKS_MAX;
    // The one retuned parameter: a heavier embedding budget, still leaving the
    // forced-cut margin a minimum-weight segment (asserted below).
    const INLINE_MAX: usize = 2048;
    const SEG_TARGET: usize = V1::SEG_TARGET;
    const SEG_MIN: usize = V1::SEG_MIN;
    const CAP_FORK: usize = V1::CAP_FORK;
    const CAP_DIR: usize = V1::CAP_DIR;
    const CUT_SCALE: u64 = V1::CUT_SCALE;
    const DERIVE_TAG: &'static [u8] = V1::DERIVE_TAG;
}

// Frozen cross-parameter facts, kept honest at compile time. The bounds hold
// for every profile: the heaviest fork record still fits a leaf segment alone,
// so partitioning terminates and the OverBudget guard stays unreachable (spec
// 5.4). Emitted as a const block per format so the checks evaluate at compile
// time.
macro_rules! assert_layout {
    ($f:ty) => {
        const _: () = {
            assert!(
                <$f>::BUDGET + <$f>::PREAMBLE.len() == DEFAULT_BODY_SIZE,
                "BUDGET must be the chunk body minus the preamble"
            );
            assert!(
                <$f>::CAP_DIR < <$f>::CAP_FORK && <$f>::CAP_FORK < <$f>::BUDGET,
                "segment capacities must sit below the body budget"
            );
            assert!(
                <$f>::SEG_MIN <= <$f>::SEG_TARGET && <$f>::SEG_TARGET <= <$f>::CAP_DIR,
                "cut suppression and target must fit the tightest capacity"
            );
            assert!(
                <$f>::VINLINE_MAX <= <$f>::INLINE_MAX && <$f>::INLINE_MAX < <$f>::BUDGET,
                "inline caps must nest below the body budget"
            );
            assert!(
                <$f>::VINLINE_MAX <= 0xFF && <$f>::CKEY_MAX <= 0xFF && <$f>::META_MAX <= 0xFFFF,
                "bounded lengths must fit their one- or two-byte wire length fields"
            );
            // The worst-case Both fork record: an index slot, flags, plen, the
            // longest tail, an inline value, an embedded child and two full
            // metadata blocks (spec 5.4). A referenced child adds a trailing
            // count; charge its worst-case width to the record even though the
            // embedded worst case never carries one.
            let count = MAX_WIRE_BYTES;
            let worst = 3
                + 1
                + 1
                + (<$f>::PLEN_MAX - 1)
                + (1 + <$f>::VINLINE_MAX)
                + (2 + <$f>::INLINE_MAX)
                + (2 + <$f>::META_MAX)
                + count;
            // Any single record fits a leaf segment, so partitioning
            // terminates, and the forced-cut margin still leaves room for a
            // minimum-weight segment.
            assert!(
                worst <= <$f>::CAP_FORK,
                "the worst fork record must fit a leaf segment alone"
            );
            assert!(
                <$f>::CAP_FORK - worst >= <$f>::SEG_MIN,
                "the forced-cut margin must leave a minimum-weight segment"
            );
        };
    };
}

assert_layout!(V1);
assert_layout!(V1Read);

// Wire-version registry: 0x01 V1, 0x02 V1Read. Readers dispatch on the
// version byte, so every profile must own a distinct one; pinned at compile
// time so a collision is a build error rather than a silent wire ambiguity.
const _: () = {
    assert!(V1::VERSION == 0x01);
    assert!(V1Read::VERSION == 0x02);
    assert!(V1::VERSION != V1Read::VERSION);
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preamble_is_magic_then_version() {
        assert_eq!(V1::PREAMBLE, [0x6D, 0x01]);
    }

    // Frozen tag_version 0x01 parameters: pin every value so a silent edit to
    // a wire-format constant fails here rather than drifting the format.
    #[test]
    fn v1_parameters_are_frozen() {
        assert_eq!(V1::MAGIC, 0x6D);
        assert_eq!(V1::VERSION, 0x01);
        assert_eq!(V1::SEPARATOR, b'/');
        assert_eq!(V1::BUDGET, 4094);
        assert_eq!(V1::PLEN_MAX, 255);
        assert_eq!(V1::VINLINE_MAX, 128);
        assert_eq!(V1::META_MAX, 1024);
        assert_eq!(V1::CKEY_MAX, 64);
        assert_eq!(V1::FORKS_MAX, 256);
        assert_eq!(V1::INLINE_MAX, 1536);
        assert_eq!(V1::SEG_TARGET, 2048);
        assert_eq!(V1::SEG_MIN, 512);
        assert_eq!(V1::CAP_FORK, 4091);
        assert_eq!(V1::CAP_DIR, 4090);
        assert_eq!(V1::CUT_SCALE, 9_007_199_254_740_992);
        assert_eq!(V1::DERIVE_TAG, b"mantaray/1.0/key");
        // The spec fixes the KDF domain tag at 16 ASCII bytes (spec 12.2).
        assert_eq!(V1::DERIVE_TAG.len(), 16);
    }

    #[test]
    fn cut_scale_divides_the_hash_space_by_seg_target() {
        let product = u128::from(V1::CUT_SCALE) * u128::try_from(V1::SEG_TARGET).unwrap();
        assert_eq!(product, 1u128 << 64);
    }

    // The read profile is a distinct wire version carrying V1's layout with a
    // heavier embedding budget; only INLINE_MAX and the version byte move.
    #[test]
    fn v1read_is_v1_with_a_heavier_embedding_budget() {
        assert_eq!(V1Read::PREAMBLE, [0x6D, 0x02]);
        assert_eq!(V1Read::VERSION, 0x02);
        assert_ne!(V1Read::VERSION, V1::VERSION);
        // A strictly larger embedding budget: the whole point of the profile.
        const { assert!(V1Read::INLINE_MAX > V1::INLINE_MAX) };
        assert_eq!(V1Read::INLINE_MAX, 2048);
        // Every other layout parameter is inherited from V1 unchanged.
        assert_eq!(V1Read::BUDGET, V1::BUDGET);
        assert_eq!(V1Read::PLEN_MAX, V1::PLEN_MAX);
        assert_eq!(V1Read::VINLINE_MAX, V1::VINLINE_MAX);
        assert_eq!(V1Read::META_MAX, V1::META_MAX);
        assert_eq!(V1Read::CKEY_MAX, V1::CKEY_MAX);
        assert_eq!(V1Read::FORKS_MAX, V1::FORKS_MAX);
        assert_eq!(V1Read::SEG_TARGET, V1::SEG_TARGET);
        assert_eq!(V1Read::SEG_MIN, V1::SEG_MIN);
        assert_eq!(V1Read::CAP_FORK, V1::CAP_FORK);
        assert_eq!(V1Read::CAP_DIR, V1::CAP_DIR);
        assert_eq!(V1Read::CUT_SCALE, V1::CUT_SCALE);
        assert_eq!(V1Read::DERIVE_TAG, V1::DERIVE_TAG);
    }

    // The heavier budget keeps the termination bounds: the worst fork record,
    // with the worst-case trailing count charged, still fits a leaf segment
    // alone, and the forced-cut margin still leaves a minimum-weight segment
    // (spec 5.4).
    #[test]
    fn the_read_profile_worst_fork_record_fits_a_segment_alone() {
        let worst = 3
            + 1
            + 1
            + (V1Read::PLEN_MAX - 1)
            + (1 + V1Read::VINLINE_MAX)
            + (2 + V1Read::INLINE_MAX)
            + (2 + V1Read::META_MAX)
            + MAX_WIRE_BYTES;
        assert_eq!(worst, 3474);
        assert!(worst <= V1Read::CAP_FORK);
        assert!(V1Read::CAP_FORK - worst >= V1Read::SEG_MIN);
    }

    // The frozen bounds that make spill terminate and keep the OverBudget guard
    // unreachable: the heaviest fork record still fits a leaf segment alone, so
    // every node partitions into one-chunk segments (spec 5.4).
    #[test]
    fn the_worst_fork_record_fits_a_segment_alone() {
        // A Both fork at the worst case: index slot, flags, plen, the longest
        // tail, an inline value, an embedded child, two full metadata blocks
        // and the worst-case trailing count.
        let worst = 3
            + 1
            + 1
            + (V1::PLEN_MAX - 1)
            + (1 + V1::VINLINE_MAX)
            + (2 + V1::INLINE_MAX)
            + (2 + V1::META_MAX)
            + MAX_WIRE_BYTES;
        assert_eq!(worst, 2962);
        // Any single record fits a leaf segment, so partitioning terminates.
        assert!(worst <= V1::CAP_FORK);
        // The forced-cut margin leaves room for a minimum-weight segment, so
        // every segment but the last reaches SEG_MIN.
        assert!(V1::CAP_FORK - worst >= V1::SEG_MIN);
    }
}
