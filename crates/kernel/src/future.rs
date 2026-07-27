//! The boxed-future alias every bounded set holds.

/// Boxed future capturing no more than `'a`, `Send` on multi-threaded
/// targets: the futures-core alias, re-exported for `.boxed()` interop.
///
/// Feature unification: `unsync`, wasm32, or a bare-metal target relaxes
/// the alias off `Send` for every consumer in that build.
#[cfg(multi_thread)]
pub use futures_core::future::BoxFuture;
/// Boxed future capturing no more than `'a`, unbounded on wasm32, bare
/// metal, and under `unsync`: the futures-core alias, re-exported for
/// `.boxed_local()` interop.
#[cfg(not(multi_thread))]
pub use futures_core::future::LocalBoxFuture as BoxFuture;

// The default build keeps the alias `Send`; only `unsync` or a
// single-threaded target may relax it.
#[cfg(multi_thread)]
const _: () = {
    const fn require_send<T: Send>() {}
    require_send::<BoxFuture<'static, ()>>();
};

#[cfg(all(test, multi_thread))]
mod tests {
    use super::*;

    const fn require_send<T: Send>() {}

    #[test]
    fn box_future_is_send_on_the_default_build() {
        require_send::<BoxFuture<'static, u32>>();
    }
}
