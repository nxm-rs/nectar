//! Builder pattern examples for nectar-primitives
//!
//! This example demonstrates usage of the chunk creation APIs,
//! since our API doesn't use the builder pattern extensively.

use alloy_primitives::B256;
use alloy_signer::SignerSync;
use alloy_signer_local::LocalSigner;

use nectar_primitives::chunk::{BmtChunk, Chunk, ContentChunk, SingleOwnerChunk};

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
    let chunk = ContentChunk::new(data)?;

    println!("  - Created chunk with address: {}", chunk.address());
    println!("  - Span: {} bytes", chunk.span());
    println!("  - Data size: {} bytes", chunk.data().len());

    // With precomputed address
    println!("\n2. Creation with precomputed address");
    let precomputed_address = *chunk.address(); // Simulating a known address
    let data_copy = chunk.data().clone();

    let chunk2 = ContentChunk::with_address(data_copy, precomputed_address)?;

    println!("  - Created chunk with address: {}", chunk2.address());
    assert_eq!(chunk.address(), chunk2.address());
    println!("  - Address matches precomputed value ✅");

    Ok(())
}

fn single_owner_creation_methods(
    wallet: &impl SignerSync,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\nSingle Owner Chunk Creation Methods");
    println!("---------------------------------");

    // Basic creation
    println!("\n1. Basic creation with signer");
    let id = B256::random();
    let data = b"Single owner chunk".to_vec();

    let chunk = SingleOwnerChunk::new(id, data, wallet)?;

    println!("  - Created chunk with address: {}", chunk.address());
    println!("  - ID: {}", alloy_primitives::hex::encode(&id[..8]));
    println!(
        "  - Owner: {}",
        alloy_primitives::hex::encode(chunk.owner()?.as_slice())
    );

    // With precomputed signature
    println!("\n2. Creation with precomputed signature");
    let signature = *chunk.signature();
    let data_copy = chunk.data().clone();

    let chunk2 = SingleOwnerChunk::with_signature(id, signature, data_copy)?;

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
    let original_id = B256::random();
    let original_data = b"Original chunk data".to_vec();
    let original_chunk = SingleOwnerChunk::new(original_id, original_data, wallet)?;
    let stored_data = original_chunk.data().clone();
    let stored_id = original_chunk.id();
    let stored_signature = *original_chunk.signature();

    // Later, reconstruct the chunk from stored components
    let reconstructed = SingleOwnerChunk::with_signature(stored_id, stored_signature, stored_data)?;

    println!("  - Reconstructed chunk from stored components");
    println!("  - Original address: {}", original_chunk.address());
    println!("  - Reconstructed address: {}", reconstructed.address());
    assert_eq!(original_chunk.address(), reconstructed.address());
    println!("  - Addresses match ✅");

    // Creating content chunks with different hash functions
    println!("\n2. Creating content chunks with specific data");

    // First, create a normal content chunk
    let data1 = b"Test data for a content chunk".to_vec();
    let content_chunk1 = ContentChunk::new(data1)?;
    println!(
        "  - Normal content chunk address: {}",
        content_chunk1.address()
    );

    // Then, create one with the same content but different size
    let data2 = b"Test data for a content chunk with more content".to_vec();
    let content_chunk2 = ContentChunk::new(data2)?;
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
    let owner_chunk = SingleOwnerChunk::new(B256::random(), b"Signed data".to_vec(), wallet)?;
    println!("  - Verifying signature on single-owner chunk");
    owner_chunk.verify(owner_chunk.address())?;
    println!("  - Signature verification successful ✅");

    Ok(())
}
