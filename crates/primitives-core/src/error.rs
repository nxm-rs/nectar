//! Error types for the nectar-primitives crates
//!
//! This module provides error types and helper functions for handling
//! errors that occur in various components of the crates.
//!
//! ## Error Structure
//!
//! The crates use a two-level error hierarchy:
//!
//! - `PrimitivesError`: The top-level error type that wraps all other errors
//! - Component-specific errors: More detailed errors from specific subsystems
//!   (like `BmtError` and `ChunkError`)
//!
//! [`EncryptionError`] and [`ChunkStoreError`] name failures of
//! `nectar-primitives` subsystems that live outside this crate. They are
//! defined here because [`PrimitivesError`] wraps them, and are re-exported at
//! their own module paths there.
//!
//! ## Example Usage
//!
//! ```
//! use nectar_primitives_core::error::{PrimitivesError, Result};
//!
//! fn fallible_operation() -> Result<()> {
//!     // Something that might fail
//!     Ok(())
//! }
//!
//! fn handle_errors() {
//!     match fallible_operation() {
//!         Ok(_) => println!("Operation succeeded"),
//!         Err(e) => match e {
//!             PrimitivesError::Bmt(bmt_err) => println!("BMT error: {}", bmt_err),
//!             PrimitivesError::Chunk(chunk_err) => println!("Chunk error: {}", chunk_err),
//!             _ => println!("Other error: {}", e),
//!         }
//!     }
//! }
//! ```
//!
//! This design allows for detailed error reporting while maintaining a consistent
//! interface across the crate.

use alloc::boxed::Box;
use thiserror::Error;

use crate::chunk::ChunkAddress;

/// Result type for operations in the primitives crate
pub type Result<T> = core::result::Result<T, PrimitivesError>;

/// A byte slice did not carry the width its target type requires.
///
/// Returned by the fallible byte constructors on the fixed-width types so
/// wire codecs can propagate the observed length instead of pre-checking it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("wrong length: expected {expected} bytes, got {got}")]
pub struct WrongLength {
    /// The byte width the target type requires.
    pub expected: usize,
    /// The byte width the slice actually carried.
    pub got: usize,
}

/// Main error type for the primitives crate
///
/// This enum represents all the possible errors that can occur when using
/// the nectar-primitives crate. It wraps component-specific errors like
/// `BmtError` and `ChunkError` to provide a unified error interface.
#[non_exhaustive]
#[derive(Error, Debug)]
pub enum PrimitivesError {
    /// Errors from BMT operations
    #[error(transparent)]
    Bmt(#[from] crate::bmt::error::BmtError),

    /// Errors from chunk operations
    #[error(transparent)]
    Chunk(#[from] crate::chunk::error::ChunkError),

    /// Errors from chunk store operations
    #[error(transparent)]
    Store(#[from] ChunkStoreError),

    /// Errors from encryption operations
    #[error(transparent)]
    Encryption(#[from] EncryptionError),

    /// Input/output errors
    #[cfg(feature = "std")]
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Array conversion errors
    #[error("Array conversion error: {0}")]
    ArrayConversion(#[from] core::array::TryFromSliceError),

    /// A byte slice had the wrong width for a fixed-width type
    #[error(transparent)]
    WrongLength(#[from] WrongLength),
}

/// Errors from encryption operations.
///
/// The cipher itself lives in `nectar_primitives::chunk::encryption`; the type
/// is here because [`PrimitivesError`] wraps it.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum EncryptionError {
    /// Input data is shorter than the required minimum.
    #[error("data too short: {len} bytes, minimum {min}")]
    DataTooShort {
        /// Actual length.
        len: usize,
        /// Minimum required length.
        min: usize,
    },

    /// Input data exceeds the maximum allowed length.
    #[error("data too long: {len} bytes, maximum {max}")]
    DataTooLong {
        /// Actual length.
        len: usize,
        /// Maximum allowed length.
        max: usize,
    },

    /// Output buffer is too small for decryption.
    #[error("output buffer too small: {len} bytes, need {required}")]
    OutputBufferTooSmall {
        /// Actual buffer length.
        len: usize,
        /// Required buffer length.
        required: usize,
    },
}

/// Boxed store error: `Send + Sync` on multi-threaded targets, unbounded on
/// wasm32 and under the `unsync` feature where a backend error may hold
/// single-thread state (a JS handle).
#[cfg(multi_thread)]
pub type BoxedError = Box<dyn core::error::Error + Send + Sync>;
/// Boxed store error: `Send + Sync` on multi-threaded targets, unbounded on
/// wasm32 and under the `unsync` feature where a backend error may hold
/// single-thread state (a JS handle).
#[cfg(not(multi_thread))]
pub type BoxedError = Box<dyn core::error::Error>;

/// Shared store error: `Send + Sync` on multi-threaded targets, unbounded on
/// wasm32 and under the `unsync` feature where a backend error may hold
/// single-thread state (a JS handle).
#[cfg(multi_thread)]
pub type SharedError = alloc::sync::Arc<dyn core::error::Error + Send + Sync>;
/// Shared store error: `Send + Sync` on multi-threaded targets, unbounded on
/// wasm32 and under the `unsync` feature where a backend error may hold
/// single-thread state (a JS handle).
#[cfg(not(multi_thread))]
pub type SharedError = alloc::sync::Arc<dyn core::error::Error>;

/// Errors from chunk storage operations.
///
/// The stores themselves live in `nectar_primitives::store`; the type is here
/// because [`PrimitivesError`] wraps it.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum ChunkStoreError {
    /// Chunk not found at the given address.
    #[error("chunk not found: {0}")]
    NotFound(ChunkAddress),
    /// Catch-all for backend-specific errors.
    #[error("{0}")]
    Other(#[source] BoxedError),
}

/// Classification of a chunk-store seam error.
///
/// Every error the [`ChunkGet`](nectar_primitives::store::ChunkGet) and
/// [`ChunkPut`](nectar_primitives::store::ChunkPut) seams hand back answers
/// whether an absence is definite, so a generic consumer can separate a miss
/// from a failure the medium could not classify. Swarm has no wire presence
/// verb, so a networked negative always arrives as an error: a definite
/// absence is the medium's own not-found answer, and nothing else may be
/// read as one.
pub trait StoreError:
    core::error::Error + nectar_marker::MaybeSend + nectar_marker::MaybeSync + 'static
{
    /// The medium definitively answered that the addressed data is not
    /// there: a miss.
    fn is_definitely_absent(&self) -> bool;

    /// The failure is a medium condition that may clear on retry, so a
    /// bounded retrier should retry.
    ///
    /// Mutually exclusive with [`StoreError::is_definitely_absent`]; an
    /// error that is neither is a terminal failure.
    fn is_transient(&self) -> bool;
}

impl StoreError for core::convert::Infallible {
    fn is_definitely_absent(&self) -> bool {
        false
    }

    fn is_transient(&self) -> bool {
        false
    }
}

impl ChunkStoreError {
    /// Create a `NotFound` error for the given address.
    pub const fn not_found(address: &ChunkAddress) -> Self {
        Self::NotFound(*address)
    }

    /// A `NotFound` is the medium's own absence answer; a boxed backend
    /// error cannot be classified at this boundary.
    pub const fn is_definitely_absent(&self) -> bool {
        matches!(self, Self::NotFound(..))
    }

    /// A miss is terminal and a boxed backend error is unclassifiable here,
    /// so nothing this type carries is worth retrying.
    pub const fn is_transient(&self) -> bool {
        false
    }
}

impl StoreError for ChunkStoreError {
    fn is_definitely_absent(&self) -> bool {
        Self::is_definitely_absent(self)
    }

    fn is_transient(&self) -> bool {
        Self::is_transient(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::boxed::Box;

    #[derive(Debug, thiserror::Error)]
    #[error("backend refusal")]
    struct BackendRefusal;

    /// New variants must be classified, so the match stays exhaustive; the
    /// predicates stay mutually exclusive per variant.
    #[test]
    fn every_variant_is_classified_into_exactly_one_group() {
        let not_found = ChunkStoreError::not_found(&ChunkAddress::default());
        let other = ChunkStoreError::Other(Box::new(BackendRefusal));
        for error in [not_found, other] {
            let absent = error.is_definitely_absent();
            let transient = error.is_transient();
            match &error {
                ChunkStoreError::NotFound(..) => {
                    assert!(absent, "a miss is definitely absent");
                    assert!(!transient, "a miss is not retryable");
                }
                ChunkStoreError::Other(..) => {
                    assert!(!absent, "a backend error is not an absence answer");
                    assert!(!transient, "an unclassifiable error is not retryable");
                }
            }
        }
    }
}
