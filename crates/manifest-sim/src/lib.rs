//! Empirical performance harness: mantaray 1.0 (`nectar-manifest`) vs mantaray
//! 0.2 (`nectar-mantaray`), both run as plain (ref32) readers+writers over one
//! shared instrumented in-memory chunk store.
//!
//! The public surface is the corpus generators, the `CountingStore`, the
//! per-cell measurement functions and the result schema; the bin drives them
//! across every `(format, corpus, scale)` and writes one JSON document.

pub mod corpus;
pub mod criterion_fold;
pub mod measure;
pub mod perf_v3;
pub mod results;
pub mod results_v3;
pub mod store;
