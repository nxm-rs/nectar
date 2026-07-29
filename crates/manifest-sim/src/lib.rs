//! Two-arm manifest measurement harness: mantaray 0.2 (`nectar-mantaray`
//! through the production `nectar-loadsave` seam) against mantaray 1.0
//! (`nectar-ldb`), run as a plain (ref32) reader-writer over instrumented
//! in-memory chunk stores.
//!
//! Store fetches and puts are the currency; a capability gap is a
//! null-with-reason, never an estimate. The [`arm::Arm`] trait is the seam
//! both formats implement over their own [`store::CountingStore`]; the
//! per-metric modules drive that seam and land serde cells in [`results`]; the
//! bin drives every `(corpus, scale)` and writes one JSON document split into
//! a bit-reproducible deterministic section and a non-deterministic build
//! wall-time section.

pub mod arm;
pub mod arm_ldb;
pub mod arm_mantaray;
pub mod build_time;
pub mod corpus;
pub mod matrix;
pub mod ordered_prefix;
pub mod perf;
pub mod results;
pub mod storage_hops;
pub mod store;
pub mod writeamp_build;

#[cfg(test)]
mod seam_smoke;
