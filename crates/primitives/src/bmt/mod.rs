//! Binary Merkle Tree (BMT) implementation for content addressing.
//!
//! This module provides an optimized implementation of a Binary Merkle Tree
//! for hashing data in parallel and generating proofs of inclusion.

pub mod constants;
pub mod error;
pub mod hasher;
pub mod proof;

pub use constants::*;
pub use error::{DigestError, Result};
pub use hasher::{BMTHasher, BMTHasherFactory};
pub use proof::{BMT_PROOF_LENGTH, BMTProof, BmtProver};

#[cfg(test)]
mod tests;
