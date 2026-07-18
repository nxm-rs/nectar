# Chunk Module

The chunk module provides implementations of the chunk types used in the storage system.

## Architecture

The chunk module is built around a trait-based architecture that allows for different chunk types to be handled uniformly while still maintaining their type-specific functionality.

### Core Traits

- `ChunkOps`: The header-free behaviour every chunk value offers (address, data, span, verify, transformed address, wire encode); implemented by the concrete chunk types and by `AnyChunk`
- `Chunk`: Ties a carrier to its header type
- `ChunkHeader`: The unsealed predicate a chunk type is: address derivation (`commit`), self-certification (`validate`), transformed-address sealing, and the wire header codec (`CacHeader` is empty, `SocHeader` is `id || signature`)
- `ChunkType`: Compile-time type identification (type ID and name)
- `ChunkRegistry`: Compile-time registry of the chunk types a network accepts; its closed envelope type is the type-level set

### Chunk Types

- `ContentChunk`: A chunk whose address is derived from its content hash
- `SingleOwnerChunk`: A chunk owned by a specific account, with signature verification

`AnyChunk` is the type-erased enum over these for runtime polymorphism.

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

Serializing a chunk (`Into<Bytes>`) produces its bare wire bytes: `span || payload` for a content chunk, `id || signature || span || payload` for a single-owner chunk.

For deserialization, you can:

1. Use type-specific parsing with `try_from` when the chunk type is known
2. Use `AnyChunk::from_wire_bytes` when only the expected address is known: the address disambiguates content vs single-owner
3. Use `AnyChunk::from_typed_bytes` for the self-describing `[type_id][wire bytes]` storage form produced by `AnyChunk::to_typed_bytes`

## Usage Examples

```rust
use nectar_primitives::{AnyChunk, Chunk, ContentChunk};

// Create a content chunk
let chunk = ContentChunk::new(&b"Hello, world!"[..]).unwrap();
let address = *chunk.address();

// Serialize, then recover it from the wire bytes and its address
let wire: bytes::Bytes = chunk.into();
let recovered = AnyChunk::from_wire_bytes(&address, wire).unwrap();
assert_eq!(*recovered.address(), address);
```
