//! Parallel verification utilities.
//!
//! This module provides high-throughput parallel implementations for stamp verification
//! using rayon parallel iterators.
//!
//! Verification is embarrassingly parallel - each stamp can be verified independently.
//! We use rayon's parallel iterators to distribute verification across all available cores.
//!
//! # Performance Optimization
//!
//! For batches where you've already recovered the owner's public key, use
//! [`verify_stamps_parallel_with_pubkey`] for approximately 2x faster verification
//! compared to full ECDSA recovery.

use alloy_primitives::Address;
use alloy_signer::k256::ecdsa::VerifyingKey;
use alloy_signer::utils::public_key_to_address;
use rayon::prelude::*;

use crate::{Stamp, StampDigest, StampError};
use nectar_primitives::SwarmAddress;

// =============================================================================
// Parallel Verification
// =============================================================================

/// Result of a stamp verification.
#[derive(Debug, Clone)]
pub struct VerifyResult {
    /// The index in the original input array.
    pub index: usize,
    /// The recovered signer address, or an error.
    pub result: Result<Address, StampError>,
}

/// Verifies multiple stamps in parallel.
///
/// This function uses rayon to distribute verification across all available cores.
/// Each stamp is verified by recovering the signer address from the signature.
///
/// # Arguments
///
/// * `stamps` - Slice of `(stamp, address)` tuples to verify
///
/// # Returns
///
/// A vector of verification results in the same order as the input.
///
/// # Example
///
/// ```ignore
/// use nectar_postage::parallel::verify_stamps_parallel;
///
/// let stamps: Vec<Stamp> = /* ... */;
/// let addresses: Vec<SwarmAddress> = /* ... */;
///
/// // Create tuples of references
/// let items: Vec<_> = stamps.iter().zip(addresses.iter()).collect();
/// let results = verify_stamps_parallel(&items);
///
/// for result in results {
///     if let Ok(signer) = result.result {
///         println!("Stamp {} signed by {}", result.index, signer);
///     }
/// }
/// ```
pub fn verify_stamps_parallel(stamps: &[(&Stamp, &SwarmAddress)]) -> Vec<VerifyResult> {
    stamps
        .par_iter()
        .enumerate()
        .map(|(index, (stamp, address))| {
            let result = recover_stamp_signer(stamp, address);
            VerifyResult { index, result }
        })
        .collect()
}

/// Verifies multiple stamps in parallel against an expected owner.
///
/// This is a convenience function that checks if all stamps were signed
/// by the expected batch owner.
///
/// # Arguments
///
/// * `stamps` - Slice of `(stamp, address)` tuples to verify
/// * `expected_owner` - The expected batch owner address
///
/// # Returns
///
/// A vector of verification results. Each result contains either the recovered
/// address if the stamp is valid and signed by the expected owner, or an error.
pub fn verify_stamps_parallel_with_owner(
    stamps: &[(&Stamp, &SwarmAddress)],
    expected_owner: Address,
) -> Vec<VerifyResult> {
    stamps
        .par_iter()
        .enumerate()
        .map(|(index, (stamp, address))| {
            let result = verify_stamp_owner(stamp, address, expected_owner);
            VerifyResult { index, result }
        })
        .collect()
}

/// Verifies multiple stamps in parallel using a cached public key.
///
/// This is approximately 10x faster than [`verify_stamps_parallel`] because it
/// avoids the expensive ECDSA public key recovery operation. Use this when you've
/// already recovered the owner's public key from a previous stamp in the same batch.
///
/// # Arguments
///
/// * `stamps` - Slice of `(stamp, address)` tuples to verify
/// * `owner_pubkey` - The cached owner public key (from a previous recovery)
///
/// # Returns
///
/// A vector of verification results. Each result contains either the owner address
/// (derived from the public key) if verification succeeded, or an error.
///
/// # Example
///
/// ```ignore
/// use nectar_postage::parallel::verify_stamps_parallel_with_pubkey;
///
/// // First, recover the public key from any stamp in the batch
/// let pubkey = first_stamp.recover_pubkey(&first_address)?;
///
/// // Then verify all remaining stamps with the cached pubkey (~10x faster)
/// let items: Vec<_> = stamps.iter().zip(addresses.iter()).collect();
/// let results = verify_stamps_parallel_with_pubkey(&items, &pubkey);
/// ```
pub fn verify_stamps_parallel_with_pubkey(
    stamps: &[(&Stamp, &SwarmAddress)],
    owner_pubkey: &VerifyingKey,
) -> Vec<VerifyResult> {
    let owner_address = public_key_to_address(owner_pubkey);

    stamps
        .par_iter()
        .enumerate()
        .map(|(index, (stamp, address))| {
            let result = match stamp.verify_with_pubkey(address, owner_pubkey) {
                Ok(()) => Ok(owner_address),
                Err(e) => Err(e),
            };
            VerifyResult { index, result }
        })
        .collect()
}

/// Recovers the signer address from a stamp.
///
/// Uses EIP-191 message recovery for interoperability.
/// The prehash (keccak256 of stamp data) is treated as the message.
fn recover_stamp_signer(stamp: &Stamp, address: &SwarmAddress) -> Result<Address, StampError> {
    let digest = StampDigest::new(
        *address,
        stamp.batch(),
        stamp.stamp_index(),
        stamp.timestamp(),
    );
    let prehash = digest.to_prehash();

    // Use recover_address_from_msg for EIP-191 compatibility
    stamp
        .signature()
        .recover_address_from_msg(prehash.as_slice())
        .map_err(|_| StampError::InvalidSignature)
}

/// Verifies a stamp was signed by the expected owner.
fn verify_stamp_owner(
    stamp: &Stamp,
    address: &SwarmAddress,
    expected_owner: Address,
) -> Result<Address, StampError> {
    let recovered = recover_stamp_signer(stamp, address)?;
    if recovered != expected_owner {
        return Err(StampError::OwnerMismatch {
            expected: expected_owner,
            actual: recovered,
        });
    }
    Ok(recovered)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;

    use crate::{Stamp, StampIndex, current_timestamp};

    fn random_address() -> SwarmAddress {
        let mut bytes = [0u8; 32];
        for b in &mut bytes {
            *b = rand::random();
        }
        SwarmAddress::new(bytes)
    }

    /// Creates a stamp for testing verification.
    fn create_test_stamp(
        signer: &PrivateKeySigner,
        chunk_address: &SwarmAddress,
        batch_id: B256,
    ) -> Stamp {
        let index = StampIndex::new(0, 0);
        let timestamp = current_timestamp();
        let digest = StampDigest::new(*chunk_address, batch_id, index, timestamp);
        let prehash = digest.to_prehash();

        // sign_message_sync returns alloy_primitives::Signature directly
        let sig = signer.sign_message_sync(prehash.as_slice()).unwrap();
        Stamp::with_index(batch_id, index, timestamp, sig)
    }

    #[test]
    fn test_parallel_verification() {
        let signer = PrivateKeySigner::random();
        let expected_owner = signer.address();
        let batch_id = B256::ZERO;

        // Create stamps
        let addresses: Vec<_> = (0..50).map(|_| random_address()).collect();
        let stamps: Vec<_> = addresses
            .iter()
            .map(|addr| create_test_stamp(&signer, addr, batch_id))
            .collect();

        // Verify stamps using tuple syntax
        let verify_input: Vec<_> = stamps.iter().zip(addresses.iter()).collect();
        let verify_results = verify_stamps_parallel_with_owner(&verify_input, expected_owner);

        assert_eq!(verify_results.len(), 50);
        for result in &verify_results {
            assert!(result.result.is_ok());
            assert_eq!(result.result.as_ref().unwrap(), &expected_owner);
        }
    }

    #[test]
    fn test_verify_wrong_signer() {
        let signer = PrivateKeySigner::random();
        let wrong_owner = Address::repeat_byte(0xFF);
        let batch_id = B256::ZERO;

        let address = random_address();
        let stamp = create_test_stamp(&signer, &address, batch_id);

        // Use tuple syntax
        let verify_input = [(&stamp, &address)];

        let verify_results = verify_stamps_parallel_with_owner(&verify_input, wrong_owner);
        assert!(matches!(
            verify_results[0].result,
            Err(StampError::OwnerMismatch { .. })
        ));
    }

    #[test]
    fn test_verify_stamps_parallel_basic() {
        let signer = PrivateKeySigner::random();
        let expected_owner = signer.address();
        let batch_id = B256::ZERO;

        let address = random_address();
        let stamp = create_test_stamp(&signer, &address, batch_id);

        let verify_input = [(&stamp, &address)];
        let results = verify_stamps_parallel(&verify_input);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].result.as_ref().unwrap(), &expected_owner);
    }

    #[test]
    fn test_verify_stamps_parallel_with_pubkey() {
        let signer = PrivateKeySigner::random();
        let expected_owner = signer.address();
        let batch_id = B256::ZERO;

        // Create stamps
        let addresses: Vec<_> = (0..50).map(|_| random_address()).collect();
        let stamps: Vec<_> = addresses
            .iter()
            .map(|addr| create_test_stamp(&signer, addr, batch_id))
            .collect();

        // Recover pubkey from first stamp
        let pubkey = stamps[0].recover_pubkey(&addresses[0]).unwrap();

        // Verify all stamps using cached pubkey
        let verify_input: Vec<_> = stamps.iter().zip(addresses.iter()).collect();
        let verify_results = verify_stamps_parallel_with_pubkey(&verify_input, &pubkey);

        assert_eq!(verify_results.len(), 50);
        for result in &verify_results {
            assert!(result.result.is_ok());
            assert_eq!(result.result.as_ref().unwrap(), &expected_owner);
        }
    }
}
