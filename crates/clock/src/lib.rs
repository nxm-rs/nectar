//! Injectable time source: one [`Clock`] trait plus a system clock (`std`)
//! and a deterministic [`ManualClock`] for tests.
//!
//! All readings are signed nanoseconds since the unix epoch; negative values
//! are pre-epoch.
//!
//! # Features
//!
//! - `std` (default): [`SystemClock`], the platform wall clock (browser clock
//!   on wasm32)
//! - `unsync`: relaxes the [`Clock`] thread-safety bounds on non-wasm
//!   single-threaded targets (via `nectar-marker`)

#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
// Test code may freely unwrap/index/panic; the runtime-safety restriction
// lints target production code paths.
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::get_unwrap,
        clippy::indexing_slicing,
        clippy::string_slice,
        clippy::arithmetic_side_effects,
        clippy::panic,
        clippy::unreachable,
        clippy::panic_in_result_fn,
        clippy::as_conversions
    )
)]

mod manual;
#[cfg(feature = "std")]
mod system;

pub use manual::ManualClock;
#[cfg(feature = "std")]
pub use system::SystemClock;

use nectar_marker::{MaybeSend, MaybeSync};

const NANOS_PER_SEC: i64 = 1_000_000_000;
const NANOS_PER_MILLI: i64 = 1_000_000;

/// Injectable wall-clock source.
///
/// Readings are signed nanoseconds since the unix epoch; negative values are
/// pre-epoch. Helpers truncate toward zero.
pub trait Clock: MaybeSend + MaybeSync {
    /// Nanoseconds since the unix epoch.
    fn now_ns(&self) -> i64;

    /// Whole seconds since the unix epoch.
    fn now_secs(&self) -> i64 {
        // Truncating division by a positive constant cannot panic.
        self.now_ns().wrapping_div(NANOS_PER_SEC)
    }

    /// Whole milliseconds since the unix epoch.
    fn now_millis(&self) -> i64 {
        // Truncating division by a positive constant cannot panic.
        self.now_ns().wrapping_div(NANOS_PER_MILLI)
    }
}

impl<C: Clock + ?Sized> Clock for &C {
    fn now_ns(&self) -> i64 {
        (**self).now_ns()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn helpers_truncate_toward_zero() {
        let clock = ManualClock::new(1_999_999_999);
        assert_eq!(clock.now_secs(), 1);
        assert_eq!(clock.now_millis(), 1_999);

        clock.set_ns(-1_999_999_999);
        assert_eq!(clock.now_secs(), -1);
        assert_eq!(clock.now_millis(), -1_999);
    }

    #[test]
    fn reference_delegates() {
        let clock = ManualClock::new(42);
        let by_ref = &clock;
        assert_eq!(Clock::now_ns(&by_ref), 42);
        clock.set_ns(43);
        assert_eq!(Clock::now_ns(&by_ref), 43);
    }
}
