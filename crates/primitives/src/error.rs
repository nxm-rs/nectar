//! Error types for the nectar-primitives crate
//!
//! This module provides error types and helper functions for handling
//! errors that occur in various components of the crate.
//!
//! ## Error Structure
//!
//! The crate uses a two-level error hierarchy:
//!
//! - `PrimitivesError`: The top-level error type that wraps all other errors
//! - Component-specific errors: More detailed errors from specific subsystems
//!   (like `BmtError` and `ChunkError`)
//!
//! ## Example Usage
//!
//! ```
//! use nectar_primitives::error::{PrimitivesError, Result};
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

use thiserror::Error;

/// Result type for operations in the primitives crate
pub type Result<T> = std::result::Result<T, PrimitivesError>;

/// Main error type for the primitives crate
///
/// This enum represents all the possible errors that can occur when using
/// the nectar-primitives crate. It wraps component-specific errors like
/// `BmtError` and `ChunkError` to provide a unified error interface.
#[derive(Error, Debug)]
pub enum PrimitivesError {
    /// Errors from BMT operations
    #[error(transparent)]
    Bmt(#[from] crate::bmt::error::BmtError),

    /// Errors from chunk operations
    #[error(transparent)]
    Chunk(#[from] crate::chunk::error::ChunkError),

    /// Input/output errors
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Array conversion errors
    #[error("Array conversion error: {0}")]
    ArrayConversion(#[from] std::array::TryFromSliceError),
}
