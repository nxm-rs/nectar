//! Allocation witness over the counting global allocator.

pub use allocation_counter::AllocationInfo;

/// Runs `f` and returns its output with the allocator traffic it caused.
///
/// Counting is thread-local: it composes with [`run`](crate::run)'s
/// single-thread `block_on`, and anything allocated on a pool thread is
/// invisible to the witness. `bytes_max` is the peak net bytes over the
/// call's own baseline, so no reset is needed between measurements.
pub fn measure_allocations<T>(f: impl FnOnce() -> T) -> (T, AllocationInfo) {
    let mut out = None;
    let info = allocation_counter::measure(|| out = Some(f()));
    (out.expect("measured closure ran"), info)
}
