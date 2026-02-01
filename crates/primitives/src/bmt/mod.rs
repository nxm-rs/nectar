//! Binary Merkle Tree (BMT) implementation
//!
//! This module provides an optimized implementation of a Binary Merkle Tree
//! for hashing data in parallel and generating proofs of inclusion.
//!
//! ## Key Components
//!
//! - **Hasher**: Core BMT hashing functionality with span support
//! - **Proof**: Inclusion proofs for efficient verification
//! - **Prover**: Interface for generating and verifying proofs
//!
//! ## Example Usage
//!
//! ```
//! use nectar_primitives::bmt::{Hasher, Prover};
//!
//! // Create a hasher and update with data
//! let data = b"hello world";
//! let mut hasher = Hasher::new();
//! hasher.set_span(data.len() as u64);
//! hasher.update(data);
//!
//! // Get the hash
//! let hash = hasher.sum();
//!
//! // Generate a proof for the first segment
//! let proof = hasher.generate_proof(data, 0).unwrap();
//!
//! // Verify the proof
//! assert!(Hasher::verify_proof(&proof, hash.as_slice()).unwrap());
//! ```

mod constants;
pub(crate) mod error;
mod hasher;
mod proof;

pub use constants::DEFAULT_BODY_SIZE;
pub use hasher::{Hasher, HasherFactory};
pub use proof::{Proof, Prover};

// Re-export for convenience
pub use crate::error::{PrimitivesError, Result};

#[cfg(target_arch = "wasm32")]
mod wasm;

#[cfg(test)]
mod tests;
