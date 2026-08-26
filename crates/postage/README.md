# nectar-postage

The behaviour half of the Swarm postage domain: the stamped put seam (the `ChunkPut` target for a paid chunk, with `StampIndifferent` bridging a plain chunk store into it) and the batch event surface.

The data half (stamp and batch types, bucket math and stamp verification) lives in [nectar-postage-primitives](https://docs.rs/nectar-postage-primitives), which this crate re-exports at the original paths.

Part of the [nectar](https://github.com/nxm-rs/nectar) workspace, a collection of low-level Ethereum Swarm primitives in Rust. See the [workspace README](https://github.com/nxm-rs/nectar) for the full crate list and project context.

## Usage

```toml
[dependencies]
nectar-postage = "0.1"
```

This crate is `no_std` compatible (default features enable `std` and the events module).

## License

AGPL-3.0-or-later. See [LICENSE](https://github.com/nxm-rs/nectar/blob/main/LICENSE).
