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

// Re-export thread pool initialization from wasm-bindgen-rayon
pub use wasm_bindgen_rayon::init_thread_pool;

// Re-export WASM bindings from each submodule
pub use crate::bmt::wasm::*;
pub use crate::chunk::wasm::*;
