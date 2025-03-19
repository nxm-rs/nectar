//! WebAssembly bindings for nectar-primitives.
//!
//! This module provides JavaScript-friendly wrappers around core types.
//! It re-exports all WASM bindings from submodules in a flat namespace.

// Re-export WASM bindings from each submodule
pub use crate::bmt::wasm::*;
pub use crate::chunk::wasm::*;
