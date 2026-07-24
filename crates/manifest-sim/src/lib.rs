//! Empirical performance harness: mantaray 1.0 (`nectar-manifest`) vs mantaray
//! 0.2 (the registry-pinned legacy crate), both run as plain (ref32)
//! readers+writers over instrumented in-memory chunk stores.
//!
//! The public surface is the corpus generators, the counting stores, the
//! per-cell measurement functions and the result schema; the bins drive them
//! across every `(format, corpus, scale)` and write one JSON document each.

pub mod corpus;
pub mod criterion_fold;
pub mod measure;
pub mod perf_v3;
pub mod results;
pub mod results_v3;
pub mod store;
pub mod store02;
