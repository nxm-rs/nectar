# nectar-primitives

Core primitives for Ethereum Swarm: content-addressed and single-owner chunks, Swarm addresses, and the Binary Merkle Tree (BMT) hasher with proof generation.

Part of the [nectar](https://github.com/nxm-rs/nectar) workspace, a collection of low-level Ethereum Swarm primitives in Rust. See the [workspace README](https://github.com/nxm-rs/nectar) for the full crate list and project context.

## Usage

```toml
[dependencies]
nectar-primitives = "0.1"
```

```rust
use nectar_primitives::DefaultContentChunk;

let chunk = DefaultContentChunk::new(b"Hello, world!".as_slice()).unwrap();
let address = chunk.address();
```

This crate is `no_std` compatible (default features enable `std`).

The chunk-verification subset (BMT, chunk carriers, single-owner recovery) lives in [`nectar-primitives-core`](../primitives-core) and is re-exported here at the same paths.
Depend on the core crate directly when you only need to verify chunks, such as inside a zkVM guest.

## License

AGPL-3.0-or-later. See [LICENSE](https://github.com/nxm-rs/nectar/blob/main/LICENSE).
