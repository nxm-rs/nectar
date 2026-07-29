//! Range-query and pagination measurement harness for the on-swarm key-value
//! database, run as a plain (ref32) reader+writer over instrumented in-memory
//! chunk stores.
//!
//! The public surface is the corpus generators, the counting stores, the
//! per-cell measurement functions and the result schema; the bin drives them
//! across every `(corpus, scale)` and writes one JSON document.

pub mod corpus;
pub mod perf;
pub mod results;
pub mod store;
