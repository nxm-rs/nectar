//! The boxed-future alias every bounded set holds.

use alloc::boxed::Box;
use core::future::Future;
use core::pin::Pin;

/// Boxed future: `Send` on multi-threaded targets, unbounded on wasm32,
/// bare metal, and under the `unsync` feature, mirroring the marker traits.
///
/// Feature unification: `unsync` enabled by any crate in a build relaxes
/// the alias for every consumer in that build.
#[cfg(multi_thread)]
pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
/// Boxed future: `Send` on multi-threaded targets, unbounded on wasm32,
/// bare metal, and under the `unsync` feature, mirroring the marker traits.
///
/// Feature unification: `unsync` enabled by any crate in a build relaxes
/// the alias for every consumer in that build.
#[cfg(not(multi_thread))]
pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T>>>;

// The default build keeps the alias `Send`; only `unsync` or a
// single-threaded target may relax it.
#[cfg(multi_thread)]
const _: () = {
    const fn require_send<T: Send>() {}
    require_send::<BoxFuture<()>>();
};

#[cfg(all(test, multi_thread))]
mod tests {
    use super::*;

    const fn require_send<T: Send>() {}

    #[test]
    fn box_future_is_send_on_the_default_build() {
        require_send::<BoxFuture<u32>>();
    }
}
