//! Widening casts and the fan-out quotient shared by both engines.

/// Lossless widening; `From` is not const-callable here.
#[cfg(target_pointer_width = "64")]
pub(crate) const fn u64_from_usize(value: usize) -> u64 {
    u64::from_le_bytes(value.to_le_bytes())
}

/// Lossless widening; `From` is not const-callable here.
#[cfg(target_pointer_width = "32")]
pub(crate) const fn u64_from_usize(value: usize) -> u64 {
    let [a, b, c, d] = value.to_le_bytes();
    u64::from_le_bytes([a, b, c, d, 0, 0, 0, 0])
}

/// Lossless widening; `From` is not const-callable.
pub(crate) const fn u64_from_u32(value: u32) -> u64 {
    let [a, b, c, d] = value.to_le_bytes();
    u64::from_le_bytes([a, b, c, d, 0, 0, 0, 0])
}

/// References per intermediate body; zero only for a degenerate profile the
/// compile-time guard rejects.
pub(crate) const fn fan_out(body: u64, ref_size: u64) -> u64 {
    match body.checked_div(ref_size) {
        Some(fan) => fan,
        None => 0,
    }
}
