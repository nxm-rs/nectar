# nectar

[![CI Status](https://github.com/nullisxyz/nectar/actions/workflows/unit.yml/badge.svg)](https://github.com/nullisxyz/nectar/actions/workflows/unit.yml)
[![codecov](https://codecov.io/gh/nullisxyz/nectar/graph/badge.svg?token=wfOmWGcYv2)](https://codecov.io/gh/nullisxyz/nectar)

**The sweet essential primitives for building Ethereum Swarm applications**

## What is nectar?

Just as nectar is essential for bees to produce honey, `nectar` provides the essential primitives for building applications on Ethereum Swarm. This crate contains the core data structures, cryptographic tools, and protocol implementations needed to interact with the Swarm network.

Whether you're building a full node like [Vertex](https://github.com/nullisxyz/vertex), crafting developer tools, or creating your own Swarm-powered applications, `nectar` provides the building blocks you need to communicate with the hive.

## Features

- 🍯 **Binary Merkle Tree (BMT)** - High-performance content addressing with parallel processing
- 🐝 **Chunk Management** - Core primitives for working with Swarm chunks
- 🐝 **Proof Generation** - Create and verify inclusion proofs for chunk segments
- 🍯 **WASM Support** - Run core functionality in browsers and other WASM environments

## Usage

Add nectar to your `Cargo.toml`:

```toml
[dependencies]
nectar-primitives = "0.1.0"
```

### Basic Example

```rust
use nectar_primitives::{bmt::BMTHasher, chunk::ChunkData};
use bytes::Bytes;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create some data
    let mut data = vec![0u8; 4096];
    data[0..4].copy_from_slice(&1016u32.to_le_bytes()); // Prefix with span
    let bytes = Bytes::from(data);

    // Create a BMT hasher and compute the chunk address
    let mut hasher = BMTHasher::new();
    hasher.set_span(1016);
    let address = hasher.chunk_address(&bytes)?;
    println!("Calculated chunk address: {:?}", address);

    // Create a chunk directly using ChunkData
    let chunk = ChunkData::deserialize(bytes, false)?;
    chunk.verify_integrity()?;

    Ok(())
}
```

## Architecture

`nectar` is structured around several core components:

- **BMT (Binary Merkle Tree)** - Optimized implementation of the Binary Merkle Tree for content addressing
- **Chunk** - Core types for working with Swarm chunks
- **Error Handling** - Comprehensive error types for robust error management

### Binary Merkle Tree (BMT)

The BMT implementation provides an optimized way to compute Swarm's content addressing function:

```rust
let mut hasher = BMTHasher::new();
hasher.set_span(data.len() as u64);
hasher.update(&data);
let root_hash = hasher.sum();
```

It also supports proof generation and verification:

```rust
// Generate proof for segment at index 0
let proof = hasher.generate_proof(&data, 0)?;

// Verify the proof
let is_valid = BMTHasher::verify_proof(&proof, root_hash.as_slice())?;
```

## WebAssembly Support

`nectar` includes WebAssembly bindings for use in browsers and other WASM environments. This allows you to use key functionality directly from JavaScript:

```javascript
import init, { BMTHasher } from 'nectar-wasm';

await init();

// Create a hasher
const hasher = new BMTHasher();
hasher.set_span(data.length);
hasher.update(new Uint8Array(data));
const hash = hasher.sum();
```

See the [WASM demo](./crates/primitives/examples/wasm-demo) for a complete example.

## Building and Testing

To build the library:

```sh
cargo build
```

To run tests:

```sh
cargo test
```

To run benchmarks:

```sh
cargo bench
```

## License

`nectar` is licensed under the AGPL License. See [LICENSE](./LICENSE) for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Warning

This software is currently in development. While we strive to make this code as sweet as honey, bugs may still buzz around. Use at your own risk.
