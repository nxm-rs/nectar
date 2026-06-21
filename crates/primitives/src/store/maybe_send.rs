//! Target-conditional `Send`/`Sync` marker aliases for the async store traits.
//!
//! On native targets the async store traits and their returned futures must be
//! `Send + Sync` so they can be driven by a multi-threaded work-stealing
//! executor. On `wasm32-unknown-unknown` the runtime is single-threaded and the
//! underlying primitives (browser timers, libp2p swarm and stream futures) are
//! `!Send`. Bounding the traits on these aliases lets a single crate compile on
//! both: `MaybeSend`/`MaybeSync` carry the real `Send`/`Sync` requirement off
//! wasm and collapse to no-op markers on wasm, so wasm consumers (feed finders,
//! mantaray traversal) need no `SendWrapper` to satisfy the bounds.

/// `Send` on native targets, a no-op marker on `wasm32`.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSend: Send {}

#[cfg(not(target_arch = "wasm32"))]
impl<T: Send> MaybeSend for T {}

/// `Send` on native targets, a no-op marker on `wasm32`.
#[cfg(target_arch = "wasm32")]
pub trait MaybeSend {}

#[cfg(target_arch = "wasm32")]
impl<T> MaybeSend for T {}

/// `Sync` on native targets, a no-op marker on `wasm32`.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSync: Sync {}

#[cfg(not(target_arch = "wasm32"))]
impl<T: Sync> MaybeSync for T {}

/// `Sync` on native targets, a no-op marker on `wasm32`.
#[cfg(target_arch = "wasm32")]
pub trait MaybeSync {}

#[cfg(target_arch = "wasm32")]
impl<T> MaybeSync for T {}
