//! The authentication seam for descent proofs.

use alloy_primitives::B256;

/// The authentication layer a descent proof verifies against.
///
/// A descent proof walks one path of steps, each binding the bytes it
/// carries to the address that step owns, and lands on a trusted address.
/// The replay borrows its inputs and allocates nothing, which keeps it
/// allocation-free in a guest.
///
/// The layer carries no key vocabulary, no content verdict and no span
/// arithmetic: a step that does not bind makes the replay report false.
pub trait Authenticate {
    /// The proof the replay walks.
    type Proof: ?Sized;

    /// Replay the proof against the trusted address.
    ///
    /// Returns true when every step binds and the walk lands on the trusted
    /// address.
    fn verify(trusted: &B256, proof: &Self::Proof) -> bool;
}
