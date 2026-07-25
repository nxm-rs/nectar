//! Lookup work-count measurement harness for the sequence feed finders, run
//! as deterministic probe and round counts over a counting presence store.
//!
//! The public surface is the corpus, the counting store, the per-cell
//! measurement functions and the result schema; the bin drives them across
//! every `(finder, length, width)` and writes one JSON document.

pub mod corpus;
pub mod measure;
pub mod results;
pub mod store;
