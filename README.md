# nectar

[![CI Status](https://github.com/nxm-rs/nectar/actions/workflows/unit.yml/badge.svg)](https://github.com/nxm-rs/nectar/actions/workflows/unit.yml)
[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL--3.0-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)

**Low-level Swarm primitives in Rust. The tedious bits that make the magic happen.**

## What is nectar?

The sweet stuff that makes the hive run. `nectar` provides the essential primitives for building applications on Ethereum Swarm: content addressing, chunk management, postage stamps, and all the cryptographic goodness you need to talk to the network.

Used by [Vertex](https://github.com/nxm-rs/vertex) (the Rust Swarm node) and available for anyone bold enough to build their own Swarm-powered applications.

## Crates

| Crate | Description |
|-------|-------------|
| `nectar-primitives` | Binary Merkle Tree, chunks, proofs. The foundation. |
| `nectar-contracts` | Contract bindings for on-chain Swarm interactions |
| `nectar-postage` | Postage stamp handling and verification |
| `nectar-postage-issuer` | High-performance stamp issuance with parallel signing |
| `nectar-swarms` | Network identifiers (mainnet, testnet, etc.) |

## Quick Start

```toml
[dependencies]
nectar-primitives = "0.1"
```

```rust
use nectar_primitives::{DefaultHasher, DefaultContentChunk};

// Hash some data with the Binary Merkle Tree
let mut hasher = DefaultHasher::new();
hasher.set_span(data.len() as u64);
hasher.update(&data);
let root_hash = hasher.sum();

// Create a content-addressed chunk
let chunk = DefaultContentChunk::new(data)?;
let address = chunk.address();
```

## Features

- **Binary Merkle Tree (BMT)**: High-performance content addressing with parallel Keccak256 hashing. Zero-tree optimisations for when your data is mostly nothing.
- **Chunk Types**: Content chunks, single-owner chunks, and all the serialisation you need.
- **Proof Generation**: Create and verify inclusion proofs for chunk segments.
- **Postage Stamps**: Create, verify, and manage postage stamps for network storage.
- **WASM Support**: Runs in browsers because why not.

## Performance

BMT hashing is optimised for real-world workloads:

| Data Size | Time |
|-----------|------|
| 64 bytes | ~1.7 µs |
| 4096 bytes (full chunk) | ~23 µs |
| All zeros (any size) | ~230 ns |

Sequential processing for small data, parallel for full chunks. No rayon overhead where it does not help.

## Building

```bash
cargo build           # Build everything
cargo test            # Run tests
cargo bench           # Run benchmarks (grab a coffee)
```

## WASM

```bash
cd crates/primitives/examples/wasm-demo
wasm-pack build --target web
```

Then use it from JavaScript:

```javascript
import init, { BMTHasher } from 'nectar-wasm';

await init();
const hasher = new BMTHasher();
hasher.set_span(data.length);
hasher.update(new Uint8Array(data));
const hash = hasher.sum();
```

## Contributing

We welcome contributions. Please read the [CLA](./CLA.md) before submitting PRs.

- Open an [issue](https://github.com/nxm-rs/nectar/issues) if something is broken
- Join the [Matrix space](https://matrix.to/#/#nexum:nxm.rs) to discuss development

## Licence

[AGPL-3.0-or-later](./LICENSE): because we believe in sharing.

## Warning

This software is under active development. It works, but so did my first attempt at sourdough. Use accordingly.
