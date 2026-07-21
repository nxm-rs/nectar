//! `Send`/`Sync` on multi-threaded targets, unbounded on wasm32 and under the
//! `unsync` feature (the single-thread escape for non-wasm targets such as
//! zkVM guests).

/// `Send` on multi-threaded targets, no bound on wasm32 or with the `unsync`
/// feature. A single-threaded executor runs futures that may hold `!Send`
/// state (a JS handle in the browser) across an await point, and such a
/// store must still satisfy the store traits.
#[cfg(not(any(target_arch = "wasm32", feature = "unsync")))]
pub trait MaybeSend: Send {}
#[cfg(not(any(target_arch = "wasm32", feature = "unsync")))]
impl<T: ?Sized + Send> MaybeSend for T {}

/// `Send` on multi-threaded targets, no bound on wasm32 or with the `unsync`
/// feature. A single-threaded executor runs futures that may hold `!Send`
/// state (a JS handle in the browser) across an await point, and such a
/// store must still satisfy the store traits.
#[cfg(any(target_arch = "wasm32", feature = "unsync"))]
pub trait MaybeSend {}
#[cfg(any(target_arch = "wasm32", feature = "unsync"))]
impl<T: ?Sized> MaybeSend for T {}

/// `Sync` on multi-threaded targets, no bound on wasm32 or with the `unsync`
/// feature. A store holding single-thread state (a JS handle) is `!Sync`;
/// on a single-threaded executor that is sound.
#[cfg(not(any(target_arch = "wasm32", feature = "unsync")))]
pub trait MaybeSync: Sync {}
#[cfg(not(any(target_arch = "wasm32", feature = "unsync")))]
impl<T: ?Sized + Sync> MaybeSync for T {}

/// `Sync` on multi-threaded targets, no bound on wasm32 or with the `unsync`
/// feature. A store holding single-thread state (a JS handle) is `!Sync`;
/// on a single-threaded executor that is sound.
#[cfg(any(target_arch = "wasm32", feature = "unsync"))]
pub trait MaybeSync {}
#[cfg(any(target_arch = "wasm32", feature = "unsync"))]
impl<T: ?Sized> MaybeSync for T {}
