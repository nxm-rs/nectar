use thiserror::Error;

/// Errors specific to BMT operations
#[derive(Error, Debug)]
pub enum BmtError {
    /// Input size is invalid for the operation
    #[error("Invalid input size: {0}")]
    InvalidInputSize(String),

    /// Proof has invalid length
    #[error("Invalid proof length: expected {expected}, got {actual}")]
    InvalidProofLength { expected: usize, actual: usize },

    /// Verification of a proof failed
    #[error("Proof verification failed: {0}")]
    VerificationFailed(String),

    /// Computation error during BMT operations
    #[error("BMT computation failed: {0}")]
    ComputationFailed(String),
}

impl BmtError {
    pub fn invalid_input_size<S: Into<String>>(msg: S) -> Self {
        Self::InvalidInputSize(msg.into())
    }

    pub fn invalid_proof_length(expected: usize, actual: usize) -> Self {
        Self::InvalidProofLength { expected, actual }
    }

    pub fn verification_failed<S: Into<String>>(msg: S) -> Self {
        Self::VerificationFailed(msg.into())
    }

    pub fn computation_failed<S: Into<String>>(msg: S) -> Self {
        Self::ComputationFailed(msg.into())
    }
}
