//! Object-safe spawn seam: one [`Spawn`] trait over boxed unit futures,
//! held as a trait object so an embedding application can route
//! library-spawned tasks through its own executor.
//!
//! # Features
//!
//! - `tokio`: `TokioSpawner`, spawns onto the ambient tokio runtime
//! - `wasm` (wasm32 only): `WasmSpawner`, spawns onto the browser event
//!   loop
//! - `unsync`: relaxes the thread-safety bounds on non-wasm single-threaded
//!   targets (via `nectar-marker`)

#![no_std]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
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

extern crate alloc;

#[cfg(all(feature = "tokio", multi_thread))]
mod tokio;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
mod wasm;

#[cfg(all(feature = "tokio", multi_thread))]
pub use self::tokio::TokioSpawner;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub use self::wasm::WasmSpawner;

use alloc::boxed::Box;
use alloc::sync::Arc;
use core::fmt;
use core::future::Future;
use core::pin::Pin;

use nectar_marker::{MaybeSend, MaybeSync};

/// Boxed future: `Send` on multi-threaded targets, unbounded on wasm32 and
/// under the `unsync` feature.
#[cfg(multi_thread)]
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
/// Boxed future: `Send` on multi-threaded targets, unbounded on wasm32 and
/// under the `unsync` feature.
#[cfg(not(multi_thread))]
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Abort callback: `Send` on multi-threaded targets, unbounded on wasm32
/// and under the `unsync` feature.
#[cfg(multi_thread)]
type BoxAbort = Box<dyn FnOnce() + Send>;
/// Abort callback: `Send` on multi-threaded targets, unbounded on wasm32
/// and under the `unsync` feature.
#[cfg(not(multi_thread))]
type BoxAbort = Box<dyn FnOnce()>;

/// Object-safe task spawner.
///
/// An implementation polls the task to completion unless the returned
/// handle aborts it.
pub trait Spawn: MaybeSend + MaybeSync {
    /// Spawn `task` onto the executor; the handle aborts it on drop.
    fn spawn(&self, task: BoxFuture<'static, ()>) -> TaskHandle;
}

impl<S: Spawn + ?Sized> Spawn for &S {
    fn spawn(&self, task: BoxFuture<'static, ()>) -> TaskHandle {
        (**self).spawn(task)
    }
}

impl<S: Spawn + ?Sized> Spawn for Box<S> {
    fn spawn(&self, task: BoxFuture<'static, ()>) -> TaskHandle {
        (**self).spawn(task)
    }
}

impl<S: Spawn + ?Sized> Spawn for Arc<S> {
    fn spawn(&self, task: BoxFuture<'static, ()>) -> TaskHandle {
        (**self).spawn(task)
    }
}

/// Owned handle to a spawned task; aborts the task on drop.
pub struct TaskHandle {
    abort: Option<BoxAbort>,
}

impl TaskHandle {
    /// Handle that runs `abort` at most once, on drop or [`abort`](Self::abort).
    #[cfg(multi_thread)]
    pub fn new(abort: impl FnOnce() + Send + 'static) -> Self {
        Self {
            abort: Some(Box::new(abort)),
        }
    }

    /// Handle that runs `abort` at most once, on drop or [`abort`](Self::abort).
    #[cfg(not(multi_thread))]
    pub fn new(abort: impl FnOnce() + 'static) -> Self {
        Self {
            abort: Some(Box::new(abort)),
        }
    }

    /// Abort the task now; drop becomes a no-op.
    pub fn abort(&mut self) {
        if let Some(abort) = self.abort.take() {
            abort();
        }
    }

    /// Let the task run to completion; drop never aborts it.
    pub fn detach(mut self) {
        self.abort = None;
    }
}

impl Drop for TaskHandle {
    fn drop(&mut self) {
        self.abort();
    }
}

impl fmt::Debug for TaskHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskHandle")
            .field("armed", &self.abort.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use alloc::sync::Arc;
    use core::sync::atomic::{AtomicU32, Ordering};

    use super::TaskHandle;

    fn armed_handle() -> (TaskHandle, Arc<AtomicU32>) {
        let aborts = Arc::new(AtomicU32::new(0));
        let counter = Arc::clone(&aborts);
        (
            TaskHandle::new(move || {
                counter.fetch_add(1, Ordering::Relaxed);
            }),
            aborts,
        )
    }

    #[test]
    fn drop_aborts_once() {
        let (handle, aborts) = armed_handle();
        drop(handle);
        assert_eq!(aborts.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn abort_disarms_drop() {
        let (mut handle, aborts) = armed_handle();
        handle.abort();
        handle.abort();
        drop(handle);
        assert_eq!(aborts.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn detach_never_aborts() {
        let (handle, aborts) = armed_handle();
        handle.detach();
        assert_eq!(aborts.load(Ordering::Relaxed), 0);
    }
}
