# Chunk Module

The chunk module provides implementations of different chunk types used in the storage system.

## Architecture

The chunk module is built around a trait-based architecture that allows for different chunk types to be handled uniformly while still maintaining their type-specific functionality.

### Core Traits

- `Chunk`: The main trait that all chunk types implement, defining common operations
- `DeserializableChunk`: A super trait that provides deserialization capabilities
- `ChunkSerialization`: A trait for serializing chunks with type prefixes
- `BMTChunk`: A trait for chunks that contain a BMT body (most chunk types)

### Chunk Types

- `ContentChunk`: A chunk whose address is derived from its content hash
- `SingleOwnerChunk`: A chunk owned by a specific account, with signature verification

## Content-Addressed Chunks

Content-addressed chunks are the simplest form of chunks, where the address is directly derived from the content hash. These are used for general data storage where ownership is not a concern.

Key features:
- Fast address calculation
- Content verification
- Immutable by design

## Single-Owner Chunks

Single-owner chunks include owner information and signatures to prove ownership. These are used when data needs to be associated with a specific owner who has control over it.

Key features:
- Owner identification
- Signature verification
- Protected updates (only the owner can create valid versions)

## Serialization and Deserialization

Chunks support two forms of serialization:

1. **Raw serialization**: Just the chunk data without type information
2. **Prefixed serialization**: Includes type ID and version prefix

For deserialization, you can either:

1. Use `deserialize_chunk` with prefixed data
2. Use type-specific parsing with `try_from` for raw data
3. Use `infer_and_deserialize` to attempt automatic type detection

## Extending with New Chunk Types

To create a new chunk type, you need to:

1. Create a struct that implements the `Chunk` trait
2. Define the `ID`, `VERSION`, and `TYPE_NAME` constants
3. Implement parsing and serialization logic
4. Update the `deserialize_chunk` function to handle your new type

## Usage Examples

```rust
use nectar_primitives::chunk::{Chunk, ContentChunk, SingleOwnerChunk};
use nectar_primitives::SwarmAddress;

// Create a content chunk
let content_chunk = ContentChunk::new(b"Hello, world!").unwrap();
let address = content_chunk.address();

// Create a single-owner chunk
let wallet = LocalWallet::random();
let id = FixedBytes::random();
let owner_chunk = SingleOwnerChunk::new(id, b"My data", &wallet).unwrap();

// Verify ownership
owner_chunk.verify_signature().unwrap();
```
