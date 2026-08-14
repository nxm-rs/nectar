//! Stream-stamp many chunk addresses through the unordered pipeline.
//!
//! Results yield as signatures complete, tagged by address; the retry set is
//! the set of addresses with no Ok result.
#![allow(clippy::arithmetic_side_effects)]

use alloy_primitives::B256;
use alloy_signer_local::PrivateKeySigner;
use nectar_postage_issuer::{
    BatchId, BucketDepth, MemoryIssuer, StampPipeline, StampResult, Window,
};
use nectar_primitives::ChunkAddress;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let issuer: MemoryIssuer = MemoryIssuer::new(BatchId::ZERO, 24, BucketDepth::new(16)?);
    let pipeline = StampPipeline::from_signer(PrivateKeySigner::random())
        .with_window(Window::new(256).ok_or("window must be nonzero")?);

    // Any address iterator works; it must not depend on consuming the
    // pipeline's output.
    let addresses: Vec<ChunkAddress> = (0..1_000).map(|_| B256::random().into()).collect();

    let mut stamped = 0usize;
    let mut retry: Vec<ChunkAddress> = Vec::new();
    for StampResult { address, result } in pipeline.stamp(&issuer, addresses) {
        match result {
            Ok(_) => stamped += 1,
            Err(error) => {
                eprintln!("{address}: {error}");
                retry.push(address);
            }
        }
    }

    println!("stamped {stamped} chunks, {} to retry", retry.len());
    Ok(())
}
