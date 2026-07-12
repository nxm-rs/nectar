//! Crate-internal explicit-cast helpers.
//!
//! Single justified home for the `usize`/`u64` conversions the chunk-tree
//! math needs, replacing scattered silent `as` casts
//! (`clippy::as_conversions` is denied workspace-wide).

/// Returns `n` as `u64`.
// `usize` is at most 64 bits wide on every Rust target, so this widening
// never loses information; `u64: From<usize>` does not exist in std, and
// `try_from` is not const-callable.
#[allow(clippy::as_conversions)]
pub(crate) const fn u64_from_usize(n: usize) -> u64 {
    n as u64
}

/// Returns `n` as `usize`, with the same semantics as the `as` cast it
/// replaces: lossless on 64-bit targets, truncating on 32-bit targets
/// (wasm32).
// Callers pass in-memory byte counts and offsets that are bounded by an
// existing allocation or by the chunk body size, or values whose pre-existing
// truncation-on-32-bit behavior must be preserved verbatim. Do NOT use this
// for values that must be range-checked: use `usize::try_from` there.
#[allow(clippy::as_conversions)]
pub(crate) const fn usize_from_u64(n: u64) -> usize {
    n as usize
}
