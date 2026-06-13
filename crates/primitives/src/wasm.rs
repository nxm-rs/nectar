//! WebAssembly bindings for nectar-primitives.
//!
//! This module provides JavaScript-friendly wrappers around core types.
//! It re-exports all WASM bindings from submodules in a flat namespace.
//!
//! # Thread Pool Initialization
//!
//! For parallel operations using Web Workers, call `initThreadPool(numThreads)`
//! from JavaScript after loading the WASM module:
//!
//! ```javascript
//! import init, { initThreadPool } from './nectar_primitives.js';
//!
//! await init();
//! await initThreadPool(navigator.hardwareConcurrency);
//! // Now parallel operations will use Web Workers
//! ```
//!
//! This uses [wasm-bindgen-rayon](https://github.com/RReverser/wasm-bindgen-rayon)
//! for rayon-based parallelism via SharedArrayBuffer and Web Workers.

// Re-export thread pool initialization from wasm-bindgen-rayon.
//
// Gated behind the `wasm-threads` feature (default-off). The mere PRESENCE of
// this re-export in the linked artifact makes wasm-bindgen run its threads
// transform and require a SharedArrayBuffer memory (hence COOP/COEP on the
// host) — independent of whether JS ever calls it. Without `wasm-threads`, the
// wasm needs no shared memory; the plain `rayon` code paths run inline.
#[cfg(feature = "wasm-threads")]
pub use wasm_bindgen_rayon::init_thread_pool;

// Re-export WASM bindings from each submodule
pub use crate::bmt::wasm::*;
pub use crate::chunk::wasm::*;
