//! Basic usage example for nectar-primitives
//!
//! This example demonstrates the creation and verification of
//! both content-addressed and single-owner chunks.

use alloy_primitives::B256;
use alloy_signer::SignerSync;
use alloy_signer_local::LocalSigner;
use bytes::Bytes;

use nectar_primitives::bmt::{Hasher, Prover, DEFAULT_BODY_SIZE};
use nectar_primitives::chunk::{BmtChunk, Chunk, ContentChunk, SingleOwnerChunk};

// Type aliases for default body size
type DefaultContentChunk = ContentChunk<DEFAULT_BODY_SIZE>;
type DefaultSingleOwnerChunk = SingleOwnerChunk<DEFAULT_BODY_SIZE>;
type DefaultHasher = Hasher<DEFAULT_BODY_SIZE>;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Nectar Primitives Example");
    println!("=========================\n");

    // Create a test wallet for signing
    let wallet = LocalSigner::random();
    println!(
        "Created wallet with address: {}",
        alloy_primitives::hex::encode(wallet.address())
    );

    // Content-addressed chunk example
    println!("\n1. Content-Addressed Chunk Example");
    println!("----------------------------------");
    content_chunk_example()?;

    // Single-owner chunk example
    println!("\n2. Single-Owner Chunk Example");
    println!("---------------------------");
    single_owner_chunk_example(&wallet)?;

    // Deserialization example
    println!("\n3. Deserialization Example");
    println!("------------------------");
    deserialization_example()?;

    // BMT hashing and proof example
    println!("\n4. BMT Hashing and Proof Example");
    println!("------------------------------");
    bmt_proof_example()?;

    Ok(())
}

fn content_chunk_example() -> Result<(), Box<dyn std::error::Error>> {
    // Create a content-addressed chunk directly
    let data = b"This is a test of the content-addressed chunk system.".to_vec();
    let chunk = DefaultContentChunk::new(data)?;

    println!("Created content chunk:");
    println!("  - Data: \"{}\"", String::from_utf8_lossy(chunk.data()));
    println!("  - Size: {} bytes", chunk.size());
    println!("  - Span: {}", chunk.span());
    println!("  - Address: {}", chunk.address());

    // Convert to bytes and back
    let bytes: Bytes = chunk.clone().into();
    println!("\nSerialized to {} bytes", bytes.len());

    // Deserialize
    let parsed = DefaultContentChunk::try_from(bytes)?;
    println!("Deserialized successfully:");
    println!("  - Address: {}", parsed.address());

    // Verify addresses match
    assert_eq!(chunk.address(), parsed.address());
    println!("Addresses match \u{2705}");

    // Create with a pre-computed address
    let address = *chunk.address(); // Pretend we know this in advance
    let data_copy = chunk.data().clone();
    let chunk_with_address = DefaultContentChunk::with_address(data_copy, address)?;
    println!("\nCreated chunk with pre-computed address:");
    println!("  - Address: {}", chunk_with_address.address());

    // Verify it matches our expected address
    assert_eq!(chunk.address(), chunk_with_address.address());
    println!("Addresses match \u{2705}");

    Ok(())
}

fn single_owner_chunk_example(wallet: &impl SignerSync) -> Result<(), Box<dyn std::error::Error>> {
    // Create a unique ID for the chunk
    let id = B256::random();
    println!(
        "Generated random chunk ID: {}",
        alloy_primitives::hex::encode(&id[..8])
    );

    // Create a single-owner chunk
    let data = b"This chunk is owned by a specific account.".to_vec();
    let chunk = DefaultSingleOwnerChunk::new(id, data, wallet)?;

    println!("\nCreated single-owner chunk:");
    println!("  - Data: \"{}\"", String::from_utf8_lossy(chunk.data()));
    println!("  - Size: {} bytes", chunk.size());
    println!("  - Span: {}", chunk.span());
    println!(
        "  - Owner: {}",
        alloy_primitives::hex::encode(chunk.owner()?.as_slice())
    );
    println!("  - Address: {}", chunk.address());

    // Convert to bytes and back
    let bytes: Bytes = chunk.clone().into();
    println!("\nSerialized to {} bytes", bytes.len());

    // Deserialize
    let parsed = DefaultSingleOwnerChunk::try_from(bytes)?;
    println!("Deserialized successfully:");
    println!("  - Address: {}", parsed.address());
    println!(
        "  - Owner: {}",
        alloy_primitives::hex::encode(parsed.owner()?.as_slice())
    );

    // There is no verify_signature method directly, but we can test verification indirectly
    // by calling the Chunk trait's verify method with its own address
    parsed.verify(parsed.address())?;
    println!("Signature verification successful \u{2705}");

    // Create with signature
    let signature = *chunk.signature();
    let data_copy = chunk.data().clone();
    let chunk_with_sig = DefaultSingleOwnerChunk::with_signature(id, signature, data_copy)?;
    println!("\nCreated chunk with pre-computed signature:");
    println!("  - Address: {}", chunk_with_sig.address());
    println!(
        "  - Owner: {}",
        alloy_primitives::hex::encode(chunk_with_sig.owner()?.as_slice())
    );

    // Verify that both chunks have the same properties
    assert_eq!(chunk.address(), chunk_with_sig.address());
    assert_eq!(chunk.owner()?, chunk_with_sig.owner()?);
    println!("Both chunks have identical properties \u{2705}");

    Ok(())
}

fn deserialization_example() -> Result<(), Box<dyn std::error::Error>> {
    // Create chunks of both types
    let data1 = b"Example for deserialization".to_vec();
    let content_chunk = DefaultContentChunk::new(data1)?;

    let wallet = LocalSigner::random();
    let id = B256::random();
    let data2 = b"Example owner chunk".to_vec();
    let single_owner_chunk = DefaultSingleOwnerChunk::new(id, data2, &wallet)?;

    // Serialize
    let content_bytes = Bytes::from(content_chunk.clone());
    let owner_bytes = Bytes::from(single_owner_chunk.clone());

    println!("Serialized chunks:");
    println!("  - Content chunk: {} bytes", content_bytes.len());
    println!("  - Single-owner chunk: {} bytes", owner_bytes.len());

    // Deserialize manually by type
    let deserialized1 = DefaultContentChunk::try_from(content_bytes)?;
    let deserialized2 = DefaultSingleOwnerChunk::try_from(owner_bytes)?;

    println!("\nDeserialized successfully:");
    println!("  - First chunk address: {}", deserialized1.address());
    println!("  - Second chunk address: {}", deserialized2.address());

    // Verify addresses match
    assert_eq!(deserialized1.address(), content_chunk.address());
    assert_eq!(deserialized2.address(), single_owner_chunk.address());
    println!("All addresses match \u{2705}");

    Ok(())
}

fn bmt_proof_example() -> Result<(), Box<dyn std::error::Error>> {
    // Create some test data
    let data = b"This is an example of BMT hashing and proof generation.";

    // Create a hasher and calculate the hash
    let mut hasher = DefaultHasher::new();
    hasher.set_span(data.len() as u64);
    hasher.update(data);
    let hash = hasher.sum();

    println!("Created BMT hash for data:");
    println!("  - Data length: {} bytes", data.len());
    println!(
        "  - Hash: {}",
        alloy_primitives::hex::encode(hash.as_slice())
    );

    // Generate a proof for segment 0
    let proof = hasher.generate_proof(data, 0)?;
    println!("\nGenerated proof for segment 0:");
    println!("  - Proof length: {} segments", proof.proof_segments.len());

    // Verify the proof
    let is_valid = DefaultHasher::verify_proof(&proof, hash.as_slice())?;
    println!(
        "  - Proof verification: {}",
        if is_valid {
            "Success ✅"
        } else {
            "Failed ❌"
        }
    );

    // Show how proofs are useful
    println!("\nProofs allow verification without the full data:");
    println!(
        "  - Only need: proof ({} bytes) + root hash (32 bytes)",
        proof.proof_segments.len() * 32
    );
    println!("  - Instead of: full data ({} bytes)", data.len());

    Ok(())
}
