//! Basic usage example for the primitives crate

use bytes::BytesMut;
use nectar_primitives::{bmt::BMTHasher, chunk::ChunkData, error::Result};

fn main() -> Result<()> {
    // Create some test data
    let mut data = BytesMut::with_capacity(1024);
    // Add 8-byte span header (the span value in little-endian bytes)
    let span: u64 = 1016; // length of payload
    data.extend_from_slice(&span.to_le_bytes());
    // Add some payload data
    data.extend_from_slice(&[0u8; 1016]);
    let bytes = data.freeze();

    // Create a BMT hasher and compute the chunk address
    println!("Creating a new chunk...");
    let mut hasher = BMTHasher::new();
    hasher.set_span(span);
    let address = hasher.chunk_address(&bytes)?;
    println!("Calculated chunk address: {:?}", address);

    // Create a chunk directly using ChunkData and content addressed type
    let chunk = ChunkData::deserialize(bytes.clone(), false)?;
    println!("Created chunk with address: {:?}", chunk.address());

    // Verify the chunk
    println!("Verifying chunk integrity...");
    chunk.verify_integrity()?;
    println!("Chunk integrity verified successfully");

    Ok(())
}
