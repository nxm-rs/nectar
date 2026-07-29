//! Thread-safety markers: `Send`/`Sync` on multi-threaded targets, unbounded
//! on wasm32 and under the `unsync` feature (the single-thread escape for
//! non-wasm targets such as zkVM guests).

use alloc::boxed::Box;
use core::future::Future;
use core::pin::Pin;

use futures_core::Stream;

pub use nectar_marker::{MaybeSend, MaybeSync};

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
