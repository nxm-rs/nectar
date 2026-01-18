//! Streaming parallel operations using tokio channels with rayon CPU parallelism.
//!
//! This module provides memory-efficient streaming for large datasets by combining:
//! - **Tokio channels** for async ingestion with backpressure
//! - **Rayon** for CPU-parallel processing of cryptographic operations
//! - **Oneshot responses** for direct request-response without output channel contention
//!
//! # Architecture
//!
//! ```text
//! [Async Input] → [Batch Collector] → [Rayon Parallel] → [Oneshot Responses]
//!      ↓                ↓                   ↓                    ↓
//!   bounded        accumulate           par_iter            direct reply
//!   channel        up to N              CPU work            per request
//! ```
//!
//! The collector batches incoming requests and processes them in parallel via rayon.
//! This provides:
//! - **Memory efficiency**: Bounded channel limits in-flight requests
//! - **CPU efficiency**: Rayon uses all cores for crypto operations
//! - **No deadlocks**: Oneshot responses eliminate output channel contention
//!
//! # Optimizations
//!
//! - **Timeout-based batching**: Waits up to 5ms for batch to fill before processing
//! - **Sequential fallback**: Small batches (< 4 items) process sequentially to avoid rayon overhead
//! - **Vector reuse**: Batch vector is reused via `drain()` to avoid allocations
//! - **Zero Arc cloning**: Issuer/signer Arcs moved into rayon closure, not cloned per batch
//!
//! # Example
//!
//! ```ignore
//! use nectar_postage::streaming::{SignRequest, streaming_signer};
//! use nectar_postage::parallel::ShardedIssuer;
//! use tokio::sync::oneshot;
//!
//! let issuer = Arc::new(ShardedIssuer::new(batch_id, depth, bucket_depth));
//! let signer = Arc::new(|prehash: &B256| wallet.sign_hash(prehash));
//!
//! // Create the streaming signer (returns input channel)
//! let tx = streaming_signer(issuer, signer, 1000, 256);
//!
//! // Send requests with oneshot for response
//! let (resp_tx, resp_rx) = oneshot::channel();
//! tx.send(SignRequest { address, response: resp_tx }).await?;
//!
//! // Get response when ready
//! let stamp = resp_rx.await??;
//! ```

use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::{Address, B256};
use alloy_signer::Signature;
use rayon::prelude::*;
use tokio::sync::{mpsc, oneshot};

use crate::parallel::ShardedIssuer;
use crate::{current_timestamp, Stamp, StampDigest, StampError};
use nectar_primitives::SwarmAddress;

/// Threshold below which we process sequentially instead of using rayon.
/// Rayon has setup overhead that isn't worth it for tiny batches.
const PARALLEL_THRESHOLD: usize = 4;

/// Maximum time to wait for batch to fill before processing.
const BATCH_TIMEOUT: Duration = Duration::from_millis(5);

// =============================================================================
// Signing
// =============================================================================

/// Request to sign a stamp for a chunk address.
///
/// Each request contains a oneshot channel for the response.
#[derive(Debug)]
pub struct SignRequest {
    /// The chunk address to stamp.
    pub address: SwarmAddress,
    /// Oneshot channel to send the result back.
    pub response: oneshot::Sender<Result<Stamp, StampError>>,
}

/// Creates a streaming signer that processes requests via async channel with rayon parallelism.
///
/// # Arguments
///
/// * `issuer` - The sharded issuer for bucket allocation (shared across requests)
/// * `signer` - The signing function (should use EIP-191 message signing)
/// * `channel_size` - Bounded channel capacity (controls memory/backpressure)
/// * `batch_size` - Max requests to batch before processing (tune for latency vs throughput)
///
/// # Returns
///
/// A sender for submitting sign requests. Drop the sender to signal completion.
pub fn streaming_signer<S>(
    issuer: Arc<ShardedIssuer>,
    signer: Arc<S>,
    channel_size: usize,
    batch_size: usize,
) -> mpsc::Sender<SignRequest>
where
    S: Fn(&B256) -> Result<Signature, alloy_signer::Error> + Send + Sync + 'static,
{
    let (tx, rx) = mpsc::channel(channel_size);

    tokio::spawn(async move {
        sign_processor(rx, issuer, signer, batch_size).await;
    });

    tx
}

/// Internal processor that batches requests and processes with rayon.
async fn sign_processor<S>(
    mut input: mpsc::Receiver<SignRequest>,
    issuer: Arc<ShardedIssuer>,
    signer: Arc<S>,
    batch_size: usize,
) where
    S: Fn(&B256) -> Result<Signature, alloy_signer::Error> + Send + Sync + 'static,
{
    // Reusable batch vector - avoid allocation per batch
    let mut batch: Vec<SignRequest> = Vec::with_capacity(batch_size);

    loop {
        // Wait for at least one request
        let Some(first) = input.recv().await else {
            break; // Channel closed
        };
        batch.push(first);

        // Try to fill the batch with timeout
        let deadline = tokio::time::Instant::now() + BATCH_TIMEOUT;
        while batch.len() < batch_size {
            let timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
            if timeout.is_zero() {
                break;
            }

            tokio::select! {
                biased;

                result = input.recv() => {
                    match result {
                        Some(req) => batch.push(req),
                        None => break, // Channel closed
                    }
                }
                _ = tokio::time::sleep(timeout) => {
                    break; // Timeout reached
                }
            }
        }

        if batch.is_empty() {
            continue;
        }

        // Process batch - use spawn_blocking to not block tokio runtime
        let batch_to_process: Vec<_> = batch.drain(..).collect();
        let issuer = Arc::clone(&issuer);
        let signer = Arc::clone(&signer);

        // Use spawn_blocking to free tokio worker thread during CPU work
        let _ = tokio::task::spawn_blocking(move || {
            process_sign_batch(batch_to_process, &issuer, &*signer);
        })
        .await;
    }
}

/// Process a batch of sign requests, choosing sequential or parallel based on size.
fn process_sign_batch<S>(batch: Vec<SignRequest>, issuer: &ShardedIssuer, signer: &S)
where
    S: Fn(&B256) -> Result<Signature, alloy_signer::Error> + Sync,
{
    if batch.len() < PARALLEL_THRESHOLD {
        // Sequential for tiny batches - avoid rayon overhead
        for req in batch {
            let result = sign_stamp_internal(issuer, signer, &req.address);
            let _ = req.response.send(result);
        }
    } else {
        // Parallel for larger batches
        batch.into_par_iter().for_each(|req| {
            let result = sign_stamp_internal(issuer, signer, &req.address);
            let _ = req.response.send(result);
        });
    }
}

/// Internal function to sign a single stamp.
#[inline]
fn sign_stamp_internal<S>(
    issuer: &ShardedIssuer,
    signer: &S,
    address: &SwarmAddress,
) -> Result<Stamp, StampError>
where
    S: Fn(&B256) -> Result<Signature, alloy_signer::Error>,
{
    let timestamp = current_timestamp();
    let digest = issuer.prepare_stamp(address, timestamp)?;
    let prehash = digest.to_prehash();
    let sig = signer(&prehash)?;
    Ok(stamp_from_signature(&digest, sig))
}

/// Creates a stamp from a digest and signature.
#[inline]
fn stamp_from_signature(digest: &StampDigest, sig: Signature) -> Stamp {
    let sig_bytes: [u8; 65] = {
        let mut bytes = [0u8; 65];
        bytes[..32].copy_from_slice(&sig.r().to_be_bytes::<32>());
        bytes[32..64].copy_from_slice(&sig.s().to_be_bytes::<32>());
        bytes[64] = sig.v() as u8;
        bytes
    };
    Stamp::with_index(digest.batch_id, digest.index, digest.timestamp, sig_bytes)
}

// =============================================================================
// Verification
// =============================================================================

/// Error from stamp verification.
#[derive(Debug, Clone, thiserror::Error)]
pub enum StreamVerifyError {
    /// The recovered signer doesn't match the expected owner.
    #[error("wrong signer: expected {expected}, got {actual}")]
    WrongSigner {
        /// Expected owner address.
        expected: Address,
        /// Actual recovered signer.
        actual: Address,
    },
    /// Signature recovery failed.
    #[error("invalid signature")]
    InvalidSignature,
}

/// Request to verify a stamp.
///
/// Each request contains a oneshot channel for the response.
#[derive(Debug)]
pub struct VerifyRequest {
    /// The stamp to verify.
    pub stamp: Stamp,
    /// The chunk address the stamp was created for.
    pub address: SwarmAddress,
    /// Oneshot channel to send the result back.
    pub response: oneshot::Sender<Result<Address, StreamVerifyError>>,
}

/// Creates a streaming verifier that processes requests via async channel with rayon parallelism.
///
/// # Arguments
///
/// * `channel_size` - Bounded channel capacity (controls memory/backpressure)
/// * `batch_size` - Max requests to batch before processing
///
/// # Returns
///
/// A sender for submitting verify requests.
pub fn streaming_verifier(channel_size: usize, batch_size: usize) -> mpsc::Sender<VerifyRequest> {
    let (tx, rx) = mpsc::channel(channel_size);

    tokio::spawn(async move {
        verify_processor(rx, batch_size, None).await;
    });

    tx
}

/// Creates a streaming verifier that also checks against an expected owner.
///
/// # Arguments
///
/// * `channel_size` - Bounded channel capacity
/// * `batch_size` - Max requests to batch before processing
/// * `expected_owner` - The expected signer address
///
/// # Returns
///
/// A sender for submitting verify requests.
pub fn streaming_verifier_with_owner(
    channel_size: usize,
    batch_size: usize,
    expected_owner: Address,
) -> mpsc::Sender<VerifyRequest> {
    let (tx, rx) = mpsc::channel(channel_size);

    tokio::spawn(async move {
        verify_processor(rx, batch_size, Some(expected_owner)).await;
    });

    tx
}

/// Internal processor that batches verify requests and processes with rayon.
async fn verify_processor(
    mut input: mpsc::Receiver<VerifyRequest>,
    batch_size: usize,
    expected_owner: Option<Address>,
) {
    // Reusable batch vector
    let mut batch: Vec<VerifyRequest> = Vec::with_capacity(batch_size);

    loop {
        // Wait for at least one request
        let Some(first) = input.recv().await else {
            break;
        };
        batch.push(first);

        // Try to fill the batch with timeout
        let deadline = tokio::time::Instant::now() + BATCH_TIMEOUT;
        while batch.len() < batch_size {
            let timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
            if timeout.is_zero() {
                break;
            }

            tokio::select! {
                biased;

                result = input.recv() => {
                    match result {
                        Some(req) => batch.push(req),
                        None => break,
                    }
                }
                _ = tokio::time::sleep(timeout) => {
                    break;
                }
            }
        }

        if batch.is_empty() {
            continue;
        }

        // Process batch
        let batch_to_process: Vec<_> = batch.drain(..).collect();

        // Use spawn_blocking to free tokio worker thread during CPU work
        let _ = tokio::task::spawn_blocking(move || {
            process_verify_batch(batch_to_process, expected_owner);
        })
        .await;
    }
}

/// Process a batch of verify requests, choosing sequential or parallel based on size.
fn process_verify_batch(batch: Vec<VerifyRequest>, expected_owner: Option<Address>) {
    if batch.len() < PARALLEL_THRESHOLD {
        // Sequential for tiny batches
        for req in batch {
            let result = match expected_owner {
                Some(owner) => verify_with_owner_internal(&req.stamp, &req.address, owner),
                None => verify_internal(&req.stamp, &req.address),
            };
            let _ = req.response.send(result);
        }
    } else {
        // Parallel for larger batches
        batch.into_par_iter().for_each(|req| {
            let result = match expected_owner {
                Some(owner) => verify_with_owner_internal(&req.stamp, &req.address, owner),
                None => verify_internal(&req.stamp, &req.address),
            };
            let _ = req.response.send(result);
        });
    }
}

/// Internal function to verify a single stamp.
#[inline]
fn verify_internal(stamp: &Stamp, address: &SwarmAddress) -> Result<Address, StreamVerifyError> {
    stamp
        .recover_signer(address)
        .map_err(|_| StreamVerifyError::InvalidSignature)
}

/// Internal function to verify a stamp and check ownership.
#[inline]
fn verify_with_owner_internal(
    stamp: &Stamp,
    address: &SwarmAddress,
    expected_owner: Address,
) -> Result<Address, StreamVerifyError> {
    let recovered = stamp
        .recover_signer(address)
        .map_err(|_| StreamVerifyError::InvalidSignature)?;

    if recovered != expected_owner {
        return Err(StreamVerifyError::WrongSigner {
            expected: expected_owner,
            actual: recovered,
        });
    }

    Ok(recovered)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parallel::ShardedIssuer;
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;

    fn random_address() -> SwarmAddress {
        let mut bytes = [0u8; 32];
        for b in &mut bytes {
            *b = rand::random();
        }
        SwarmAddress::new(bytes)
    }

    #[tokio::test]
    async fn test_streaming_signer_basic() {
        let issuer = Arc::new(ShardedIssuer::new(B256::ZERO, 24, 16));
        let signer = PrivateKeySigner::random();
        let signer = Arc::new(move |prehash: &B256| signer.sign_message_sync(prehash.as_slice()));

        let tx = streaming_signer(issuer, signer, 100, 64);

        // Send requests and collect response receivers
        let mut receivers = Vec::new();
        for _ in 0..5 {
            let address = random_address();
            let (resp_tx, resp_rx) = oneshot::channel();
            tx.send(SignRequest {
                address,
                response: resp_tx,
            })
            .await
            .unwrap();
            receivers.push(resp_rx);
        }

        // Drop sender to signal completion
        drop(tx);

        // Collect responses
        let mut results = Vec::new();
        for rx in receivers {
            results.push(rx.await.unwrap());
        }

        assert_eq!(results.len(), 5);
        for result in &results {
            assert!(result.is_ok());
        }
    }

    #[tokio::test]
    async fn test_streaming_signer_sequential_path() {
        // Test with < PARALLEL_THRESHOLD items to exercise sequential path
        let issuer = Arc::new(ShardedIssuer::new(B256::ZERO, 24, 16));
        let signer = PrivateKeySigner::random();
        let signer = Arc::new(move |prehash: &B256| signer.sign_message_sync(prehash.as_slice()));

        let tx = streaming_signer(issuer, signer, 100, 64);

        // Send only 2 requests (below PARALLEL_THRESHOLD)
        let mut receivers = Vec::new();
        for _ in 0..2 {
            let address = random_address();
            let (resp_tx, resp_rx) = oneshot::channel();
            tx.send(SignRequest {
                address,
                response: resp_tx,
            })
            .await
            .unwrap();
            receivers.push(resp_rx);
        }

        drop(tx);

        for rx in receivers {
            assert!(rx.await.unwrap().is_ok());
        }
    }

    #[tokio::test]
    async fn test_streaming_verifier_basic() {
        let issuer = ShardedIssuer::new(B256::ZERO, 24, 16);
        let signer = PrivateKeySigner::random();
        let expected_owner = signer.address();

        // Create some stamps first
        let sign_fn = |prehash: &B256| signer.sign_message_sync(prehash.as_slice());

        let addresses: Vec<_> = (0..5).map(|_| random_address()).collect();
        let stamps: Vec<_> = addresses
            .iter()
            .map(|addr| {
                let timestamp = current_timestamp();
                let digest = issuer.prepare_stamp(addr, timestamp).unwrap();
                let prehash = digest.to_prehash();
                let sig = sign_fn(&prehash).unwrap();
                stamp_from_signature(&digest, sig)
            })
            .collect();

        // Verify using streaming verifier
        let tx = streaming_verifier_with_owner(100, 64, expected_owner);

        let mut receivers = Vec::new();
        for (stamp, address) in stamps.into_iter().zip(addresses.iter()) {
            let (resp_tx, resp_rx) = oneshot::channel();
            tx.send(VerifyRequest {
                stamp,
                address: *address,
                response: resp_tx,
            })
            .await
            .unwrap();
            receivers.push(resp_rx);
        }

        drop(tx);

        let mut results = Vec::new();
        for rx in receivers {
            results.push(rx.await.unwrap());
        }

        assert_eq!(results.len(), 5);
        for result in &results {
            assert!(result.is_ok());
            assert_eq!(result.as_ref().unwrap(), &expected_owner);
        }
    }

    #[tokio::test]
    async fn test_streaming_verifier_wrong_owner() {
        let issuer = ShardedIssuer::new(B256::ZERO, 24, 16);
        let signer = PrivateKeySigner::random();
        let wrong_owner = Address::repeat_byte(0xFF);

        let sign_fn = |prehash: &B256| signer.sign_message_sync(prehash.as_slice());

        let address = random_address();
        let timestamp = current_timestamp();
        let digest = issuer.prepare_stamp(&address, timestamp).unwrap();
        let prehash = digest.to_prehash();
        let sig = sign_fn(&prehash).unwrap();
        let stamp = stamp_from_signature(&digest, sig);

        let tx = streaming_verifier_with_owner(100, 64, wrong_owner);

        let (resp_tx, resp_rx) = oneshot::channel();
        tx.send(VerifyRequest {
            stamp,
            address,
            response: resp_tx,
        })
        .await
        .unwrap();

        drop(tx);

        let result = resp_rx.await.unwrap();
        assert!(matches!(result, Err(StreamVerifyError::WrongSigner { .. })));
    }

    #[tokio::test]
    async fn test_streaming_signer_large_batch() {
        // Test with 1000 items to ensure good parallelism
        let issuer = Arc::new(ShardedIssuer::new(B256::ZERO, 24, 16));
        let signer = PrivateKeySigner::random();
        let signer = Arc::new(move |prehash: &B256| signer.sign_message_sync(prehash.as_slice()));

        let tx = streaming_signer(issuer, signer, 100, 256);

        // Send 1000 requests
        let mut receivers = Vec::with_capacity(1000);
        for _ in 0..1000 {
            let address = random_address();
            let (resp_tx, resp_rx) = oneshot::channel();
            tx.send(SignRequest {
                address,
                response: resp_tx,
            })
            .await
            .unwrap();
            receivers.push(resp_rx);
        }

        drop(tx);

        // Collect all responses
        let mut success_count = 0;
        for rx in receivers {
            if rx.await.unwrap().is_ok() {
                success_count += 1;
            }
        }

        assert_eq!(success_count, 1000);
    }

    #[tokio::test]
    async fn test_batch_timeout() {
        // Test that batching doesn't wait forever when items trickle in slowly
        let issuer = Arc::new(ShardedIssuer::new(B256::ZERO, 24, 16));
        let signer = PrivateKeySigner::random();
        let signer = Arc::new(move |prehash: &B256| signer.sign_message_sync(prehash.as_slice()));

        let tx = streaming_signer(issuer, signer, 100, 1000); // Large batch size

        // Send just one request - should process after timeout, not wait for 1000
        let address = random_address();
        let (resp_tx, resp_rx) = oneshot::channel();
        tx.send(SignRequest {
            address,
            response: resp_tx,
        })
        .await
        .unwrap();

        // Should complete within reasonable time (timeout + processing)
        let result = tokio::time::timeout(Duration::from_secs(1), resp_rx).await;
        assert!(result.is_ok(), "Should not timeout waiting for response");
        assert!(result.unwrap().unwrap().is_ok());
    }
}
