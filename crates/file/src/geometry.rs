//! Chunk-tree geometry: every fan-out fact derives from (body size, mode).
//!
//! Const-assert policy: each concrete profile is pinned by
//! [`assert_tree_geometry`], evaluated at compile time with `u128` arithmetic
//! so coverage of the full `u64` length range is provable without overflow. A
//! violated invariant is a build error, never a runtime panic; `assert!` in
//! const-evaluated items is the one sanctioned panicking form here.

/// How a tree references its children.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Mode {
    /// References carry a 32-byte address.
    Plain,
    /// References carry a 32-byte address plus a 32-byte decryption key.
    Encrypted,
}

/// Address width of a reference in bytes.
const ADDRESS_SIZE: u32 = 32;
/// Decryption-key width of an encrypted reference in bytes.
const KEY_SIZE: u32 = 32;

/// Plain reference width in bytes.
const PLAIN_REF_SIZE: u32 = ADDRESS_SIZE;
/// Encrypted reference width in bytes.
const ENCRYPTED_REF_SIZE: u32 = ADDRESS_SIZE + KEY_SIZE;

impl Mode {
    /// Reference width in bytes.
    pub const fn ref_size(self) -> u32 {
        match self {
            Self::Plain => PLAIN_REF_SIZE,
            Self::Encrypted => ENCRYPTED_REF_SIZE,
        }
    }
}

/// References per intermediate chunk body: 128 plain, 64 encrypted at the
/// default 4096-byte body. Valid profiles are pinned by
/// [`assert_tree_geometry`].
pub const fn branches(body_size: u32, mode: Mode) -> u32 {
    match mode {
        Mode::Plain => body_size / PLAIN_REF_SIZE,
        Mode::Encrypted => body_size / ENCRYPTED_REF_SIZE,
    }
}

/// Upper bound of the [`max_depth`] search; even a two-branch profile covers
/// `u64` within it. Degenerate profiles saturate here and are rejected by
/// [`assert_tree_geometry`].
const DEPTH_SEARCH_LIMIT: u32 = 64;

/// Least depth whose leaf capacity spans every `u64` byte length: 9 plain,
/// 10 encrypted at the default 4096-byte body.
pub const fn max_depth(body_size: u32, mode: Mode) -> u32 {
    let mut depth = 1;
    while depth < DEPTH_SEARCH_LIMIT && !covers_u64(body_size, mode, depth) {
        depth = depth.saturating_add(1);
    }
    depth
}

/// One more than `u64::MAX`: capacity at or past this covers every length.
const EVERY_U64_LENGTH: u128 = 1u128 << 64;

/// Whether a tree of `depth` levels spans every `u64` byte length; the
/// capacity `branches^(depth - 1) * body_size` is computed in `u128`.
const fn covers_u64(body_size: u32, mode: Mode, depth: u32) -> bool {
    let fan_out = u128_from_u32(branches(body_size, mode));
    let mut capacity = u128_from_u32(body_size);
    let mut hops = depth.saturating_sub(1);
    while hops > 0 {
        capacity = match capacity.checked_mul(fan_out) {
            Some(grown) => grown,
            // Overflowing u128 already exceeds the bound.
            None => return true,
        };
        hops = hops.saturating_sub(1);
    }
    capacity >= EVERY_U64_LENGTH
}

/// Lossless const widening; `From` is not const-callable.
const fn u128_from_u32(value: u32) -> u128 {
    let [a, b, c, d] = value.to_le_bytes();
    u128::from_le_bytes([a, b, c, d, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
}

/// Const-evaluated profile check behind [`assert_tree_geometry`]; call it
/// only from const context.
///
/// # Panics
/// During const evaluation (a build error) when the profile is invalid.
#[doc(hidden)]
pub const fn assert_profile(body_size: u32, mode: Mode) {
    assert!(
        body_size.is_power_of_two(),
        "body size must be a power of two"
    );
    let fan_out = branches(body_size, mode);
    assert!(fan_out >= 2, "fan-out must be at least two");
    let covered = match fan_out.checked_mul(mode.ref_size()) {
        Some(bytes) => bytes,
        None => 0,
    };
    assert!(
        covered == body_size,
        "reference width must divide the body exactly"
    );
    let depth = max_depth(body_size, mode);
    assert!(
        covers_u64(body_size, mode, depth),
        "max depth must span every u64 length"
    );
    assert!(
        !covers_u64(body_size, mode, depth.saturating_sub(1)),
        "max depth must be minimal"
    );
}

/// Pins the tree geometry of one body-size profile at compile time, for both
/// modes: exact reference-width division, a fan-out of at least two, and a
/// minimal [`max_depth`] spanning every `u64` length.
#[macro_export]
macro_rules! assert_tree_geometry {
    ($body_size:expr) => {
        const _: () = {
            $crate::geometry::assert_profile($body_size, $crate::geometry::Mode::Plain);
            $crate::geometry::assert_profile($body_size, $crate::geometry::Mode::Encrypted);
        };
    };
}

/// Body size of a standard Swarm content chunk in bytes.
pub const DEFAULT_BODY_SIZE: u32 = 4096;

assert_tree_geometry!(DEFAULT_BODY_SIZE);

// Default-profile pins: the values the engines are designed around.
const _: () = {
    assert!(branches(DEFAULT_BODY_SIZE, Mode::Plain) == 128);
    assert!(branches(DEFAULT_BODY_SIZE, Mode::Encrypted) == 64);
    assert!(max_depth(DEFAULT_BODY_SIZE, Mode::Plain) == 9);
    assert!(max_depth(DEFAULT_BODY_SIZE, Mode::Encrypted) == 10);
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ref_widths() {
        assert_eq!(Mode::Plain.ref_size(), 32);
        assert_eq!(Mode::Encrypted.ref_size(), 64);
    }

    #[test]
    fn default_profile_fan_out() {
        assert_eq!(branches(DEFAULT_BODY_SIZE, Mode::Plain), 128);
        assert_eq!(branches(DEFAULT_BODY_SIZE, Mode::Encrypted), 64);
    }

    #[test]
    fn default_profile_depth_is_minimal() {
        assert_eq!(max_depth(DEFAULT_BODY_SIZE, Mode::Plain), 9);
        assert_eq!(max_depth(DEFAULT_BODY_SIZE, Mode::Encrypted), 10);
        assert!(!covers_u64(DEFAULT_BODY_SIZE, Mode::Plain, 8));
        assert!(!covers_u64(DEFAULT_BODY_SIZE, Mode::Encrypted, 9));
    }

    #[test]
    fn one_level_covers_nothing_beyond_its_body() {
        assert!(!covers_u64(DEFAULT_BODY_SIZE, Mode::Plain, 1));
    }

    #[test]
    fn smallest_plain_profile_reaches_u64() {
        // A 64-byte body forks two ways: 6 + (depth - 1) bits must reach 64.
        assert_eq!(branches(64, Mode::Plain), 2);
        assert_eq!(max_depth(64, Mode::Plain), 59);
    }
}
