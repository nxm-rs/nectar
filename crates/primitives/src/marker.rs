//! Thread-safety markers: `Send`/`Sync` on multi-threaded targets, unbounded
//! on wasm32 and under the `unsync` feature (the single-thread escape for
//! non-wasm targets such as zkVM guests).

use alloc::boxed::Box;
use core::future::Future;
use core::pin::Pin;

use futures_core::Stream;

/// `Send` on multi-threaded targets, no bound on wasm32 or with the `unsync`
/// feature. A single-threaded executor may hold `!Send` state (a JS handle
/// in the browser) across an await point.
#[cfg(multi_thread)]
pub trait MaybeSend: Send {}
#[cfg(multi_thread)]
impl<T: ?Sized + Send> MaybeSend for T {}

/// `Send` on multi-threaded targets, no bound on wasm32 or with the `unsync`
/// feature. A single-threaded executor may hold `!Send` state (a JS handle
/// in the browser) across an await point.
#[cfg(not(multi_thread))]
pub trait MaybeSend {}
#[cfg(not(multi_thread))]
impl<T: ?Sized> MaybeSend for T {}

/// `Sync` on multi-threaded targets, no bound on wasm32 or with the `unsync`
/// feature. Single-thread state (a JS handle) is `!Sync`; on a
/// single-threaded executor that is sound.
#[cfg(multi_thread)]
pub trait MaybeSync: Sync {}
#[cfg(multi_thread)]
impl<T: ?Sized + Sync> MaybeSync for T {}

/// `Sync` on multi-threaded targets, no bound on wasm32 or with the `unsync`
/// feature. Single-thread state (a JS handle) is `!Sync`; on a
/// single-threaded executor that is sound.
#[cfg(not(multi_thread))]
pub trait MaybeSync {}
#[cfg(not(multi_thread))]
impl<T: ?Sized> MaybeSync for T {}

/// Boxed future: `Send` on multi-threaded targets, unbounded on wasm32 and
/// under the `unsync` feature.
#[cfg(multi_thread)]
pub type MaybeSendBoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
/// Boxed future: `Send` on multi-threaded targets, unbounded on wasm32 and
/// under the `unsync` feature.
#[cfg(not(multi_thread))]
pub type MaybeSendBoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Boxed stream: `Send` on multi-threaded targets, unbounded on wasm32 and
/// under the `unsync` feature.
#[cfg(multi_thread)]
pub type MaybeSendStream<'a, T> = Pin<Box<dyn Stream<Item = T> + Send + 'a>>;
/// Boxed stream: `Send` on multi-threaded targets, unbounded on wasm32 and
/// under the `unsync` feature.
#[cfg(not(multi_thread))]
pub type MaybeSendStream<'a, T> = Pin<Box<dyn Stream<Item = T> + 'a>>;

/// Boxed iterator: `Send` on multi-threaded targets, unbounded on wasm32 and
/// under the `unsync` feature.
#[cfg(multi_thread)]
pub type MaybeSendIter<'a, T> = Box<dyn Iterator<Item = T> + Send + 'a>;
/// Boxed iterator: `Send` on multi-threaded targets, unbounded on wasm32 and
/// under the `unsync` feature.
#[cfg(not(multi_thread))]
pub type MaybeSendIter<'a, T> = Box<dyn Iterator<Item = T> + 'a>;
