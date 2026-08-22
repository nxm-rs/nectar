//! Data sinks: positional byte targets a download writes into.
//!
//! The vocabulary lives in `nectar_primitives::sink` and is re-exported here;
//! [`FsSink`] is this crate's filesystem impl of the contract.

mod fs;
#[cfg(test)]
mod tests;

pub use fs::FsSink;

pub use nectar_primitives::sink::{DataSink, MemSink, MemSinkError};
