//! `Send`/`Sync` on native, unbounded on wasm32.

/// `Send` on native targets, no bound on wasm32. The browser executor is
/// single-threaded, so a getter future holding a JS handle across an await
/// point is `!Send` there and must still satisfy the store traits.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSend: Send {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: ?Sized + Send> MaybeSend for T {}

/// `Send` on native targets, no bound on wasm32. The browser executor is
/// single-threaded, so a getter future holding a JS handle across an await
/// point is `!Send` there and must still satisfy the store traits.
#[cfg(target_arch = "wasm32")]
pub trait MaybeSend {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeSend for T {}

/// `Sync` on native targets, no bound on wasm32. A browser store holding
/// JS handles is `!Sync`; on the single-threaded wasm executor that is sound.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSync: Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: ?Sized + Sync> MaybeSync for T {}

/// `Sync` on native targets, no bound on wasm32. A browser store holding
/// JS handles is `!Sync`; on the single-threaded wasm executor that is sound.
#[cfg(target_arch = "wasm32")]
pub trait MaybeSync {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeSync for T {}
