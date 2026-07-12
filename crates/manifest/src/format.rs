//! Sealed format-version carrier for the manifest wire format.

use core::fmt::Debug;
use core::hash::Hash;

use nectar_primitives::DEFAULT_BODY_SIZE;

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::V1 {}
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

    /// Max node body bytes: the chunk body minus [`Self::PREAMBLE`].
    const BUDGET: usize;

    /// Max fork prefix length in bytes.
    const PLEN_MAX: usize;

    /// Max inline value length in bytes.
    const VINLINE_MAX: usize;

    /// Max encoded metadata length per meta block, in bytes.
    const META_MAX: usize;

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
}

/// The frozen `tag_version 0x01` parameter set.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct V1;

impl Format for V1 {
    const VERSION: u8 = 0x01;
    const BUDGET: usize = 4094;
    const PLEN_MAX: usize = 255;
    const VINLINE_MAX: usize = 128;
    const META_MAX: usize = 1024;
    const FORKS_MAX: usize = 256;
    const INLINE_MAX: usize = 1536;
    const SEG_TARGET: usize = 2048;
    const SEG_MIN: usize = 512;
    const CAP_FORK: usize = 4091;
    const CAP_DIR: usize = 4090;
    const CUT_SCALE: u64 = 9_007_199_254_740_992;
}

// Frozen cross-parameter facts, kept honest at compile time.
const _: () = {
    assert!(
        V1::BUDGET + V1::PREAMBLE.len() == DEFAULT_BODY_SIZE,
        "BUDGET must be the chunk body minus the preamble"
    );
    assert!(
        V1::CAP_DIR < V1::CAP_FORK && V1::CAP_FORK < V1::BUDGET,
        "segment capacities must sit below the body budget"
    );
    assert!(
        V1::SEG_MIN <= V1::SEG_TARGET && V1::SEG_TARGET <= V1::CAP_DIR,
        "cut suppression and target must fit the tightest capacity"
    );
    assert!(
        V1::VINLINE_MAX <= V1::INLINE_MAX && V1::INLINE_MAX < V1::BUDGET,
        "inline caps must nest below the body budget"
    );
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preamble_is_magic_then_version() {
        assert_eq!(V1::PREAMBLE, [0x6D, 0x01]);
    }

    #[test]
    fn cut_scale_divides_the_hash_space_by_seg_target() {
        let product = u128::from(V1::CUT_SCALE) * u128::try_from(V1::SEG_TARGET).unwrap();
        assert_eq!(product, 1u128 << 64);
    }
}
