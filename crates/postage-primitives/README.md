# nectar-postage-primitives

The data half of the postage domain: stamps, batches, bucket geometry, the stamped-address typestate and the signature recovery that binds a stamp to an address.

This is the half a guest links. The store-backed put seam and the event surface live in [nectar-postage](https://docs.rs/nectar-postage), which re-exports every item at its original path.

Part of the [nectar](https://github.com/nxm-rs/nectar) workspace, a collection of low-level Ethereum Swarm primitives in Rust. See the [workspace README](https://github.com/nxm-rs/nectar) for the full crate list and project context.

## Usage

```toml
[dependencies]
nectar-postage-primitives = "0.4"
```

This crate is `no_std` (default features enable `std`).

## License

AGPL-3.0-or-later. See [LICENSE](https://github.com/nxm-rs/nectar/blob/main/LICENSE).
