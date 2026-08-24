//! Binary Merkle Tree (BMT) implementation.
//!
//! The hasher, the inclusion proofs and their constants live in
//! [`nectar_primitives_core::bmt`] and are re-exported here unchanged; this
//! module adds only the wasm bindings.
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
//! assert!(Hasher::verify_proof(&proof, &hash).unwrap());
//! ```

pub use nectar_primitives_core::bmt::*;
