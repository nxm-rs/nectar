//! Builder pattern examples for nectar-primitives
//!
//! This example demonstrates usage of the chunk creation APIs,
//! since our API doesn't use the builder pattern extensively.

// Bench and example code: unwraps, direct indexing, casts, and assertions
// are setup and illustration, not shipped surface.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::panic,
    clippy::panic_in_result_fn,
    clippy::as_conversions,
    clippy::missing_panics_doc
)]
use alloy_signer::SignerSync;
use alloy_signer_local::LocalSigner;

use nectar_primitives::chunk::{BmtChunk, Chunk};
use nectar_primitives::{DefaultContentChunk, DefaultSingleOwnerChunk, SocId};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Nectar Primitives - Creation Pattern Examples");
    println!("===========================================\n");

    // Content chunk creation
    content_chunk_creation_methods()?;

    // Single owner chunk creation
    let wallet = LocalSigner::random();
    single_owner_creation_methods(&wallet)?;

    // Specialized use cases
    special_use_cases(&wallet)?;

    Ok(())
}

fn content_chunk_creation_methods() -> Result<(), Box<dyn std::error::Error>> {
    println!("Content Chunk Creation Methods");
    println!("-----------------------------");

    // Basic creation - automatic span
    println!("\n1. Simple creation");
    let data = b"Basic content chunk with auto-calculated span".to_vec();
    let chunk = DefaultContentChunk::new(data)?;

    println!("  - Created chunk with address: {}", chunk.address());
    println!("  - Span: {} bytes", chunk.span());
    println!("  - Data size: {} bytes", chunk.data().len());

    // Certifying against a known address
    println!("\n2. Certifying against a known address");
    let known_address = *chunk.address(); // Simulating a known address
    let data_copy = chunk.data().clone();

    let chunk2 = DefaultContentChunk::new(data_copy)?;
    chunk2.verify(&known_address)?;

    println!("  - Rebuilt chunk with address: {}", chunk2.address());
    assert_eq!(chunk.address(), chunk2.address());
    println!("  - Chunk certifies against the known address ✅");

    Ok(())
}

fn single_owner_creation_methods(
    wallet: &impl SignerSync,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\nSingle Owner Chunk Creation Methods");
    println!("---------------------------------");

    // Basic creation
    println!("\n1. Basic creation with signer");
    let id = SocId::random();
    let data = b"Single owner chunk".to_vec();

    let chunk = DefaultSingleOwnerChunk::new(id, data, wallet)?;

    println!("  - Created chunk with address: {}", chunk.address());
    println!(
        "  - ID: {}",
        alloy_primitives::hex::encode(&id.as_slice()[..8])
    );
    println!(
        "  - Owner: {}",
        alloy_primitives::hex::encode(chunk.owner()?.as_slice())
    );

    // With precomputed signature
    println!("\n2. Creation with precomputed signature");
    let signature = *chunk.signature();
    let data_copy = chunk.data().clone();

    let chunk2 = DefaultSingleOwnerChunk::with_signature(id, signature, data_copy)?;

    println!("  - Created chunk with precomputed signature");
    println!("  - Address: {}", chunk2.address());
    assert_eq!(chunk.address(), chunk2.address());
    println!("  - Addresses match ✅");

    Ok(())
}

fn special_use_cases(wallet: &impl SignerSync) -> Result<(), Box<dyn std::error::Error>> {
    println!("\nSpecialized Use Cases");
    println!("-------------------");

    // Creating chunks for reconstructing from storage
    println!("\n1. Reconstructing chunks from storage");

    // Simulate stored chunk data
    let original_id = SocId::random();
    let original_data = b"Original chunk data".to_vec();
    let original_chunk = DefaultSingleOwnerChunk::new(original_id, original_data, wallet)?;
    let stored_data = original_chunk.data().clone();
    let stored_id = original_chunk.id();
    let stored_signature = *original_chunk.signature();

    // Later, reconstruct the chunk from stored components
    let reconstructed =
        DefaultSingleOwnerChunk::with_signature(stored_id, stored_signature, stored_data)?;

    println!("  - Reconstructed chunk from stored components");
    println!("  - Original address: {}", original_chunk.address());
    println!("  - Reconstructed address: {}", reconstructed.address());
    assert_eq!(original_chunk.address(), reconstructed.address());
    println!("  - Addresses match ✅");

    // Creating content chunks with different hash functions
    println!("\n2. Creating content chunks with specific data");

    // First, create a normal content chunk
    let data1 = b"Test data for a content chunk".to_vec();
    let content_chunk1 = DefaultContentChunk::new(data1)?;
    println!(
        "  - Normal content chunk address: {}",
        content_chunk1.address()
    );

    // Then, create one with the same content but different size
    let data2 = b"Test data for a content chunk with more content".to_vec();
    let content_chunk2 = DefaultContentChunk::new(data2)?;
    println!(
        "  - Longer content chunk address: {}",
        content_chunk2.address()
    );
    assert_ne!(content_chunk1.address(), content_chunk2.address());
    println!("  - Different addresses for different content ✅");

    // Demonstrate verification
    println!("\n3. Chunk verification");

    // Verify a content chunk's address
    let valid_address = *content_chunk1.address();
    println!("  - Verifying content chunk against its own address");
    content_chunk1.verify(&valid_address)?;
    println!("  - Verification successful ✅");

    // Verify a single-owner chunk's signature - verify against its own address
    let owner_chunk =
        DefaultSingleOwnerChunk::new(SocId::random(), b"Signed data".to_vec(), wallet)?;
    println!("  - Verifying signature on single-owner chunk");
    owner_chunk.verify(owner_chunk.address())?;
    println!("  - Signature verification successful ✅");

    Ok(())
}
