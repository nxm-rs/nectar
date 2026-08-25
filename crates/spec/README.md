# nectar-spec

The Swarm network identity: the `SwarmSpec` trait and its per-network knobs (network id, kademlia tuning, postage floors), the canonical `Mainnet` and `Testnet` markers, `NetworkId`, the named-swarm table and the typed proximity-order and bin kinds the spec constants are built from.

The overlay derivation and the chunk machinery that consume these values live in `nectar-primitives-core`, which re-exports every item at its original path.

Part of the [nectar](https://github.com/nxm-rs/nectar) workspace, a collection of low-level Ethereum Swarm primitives in Rust. See the [workspace README](https://github.com/nxm-rs/nectar) for the full crate list and project context.

## Usage

```toml
[dependencies]
nectar-spec = "0.4"
```

This crate is `no_std` (default features enable `std`).

## License

AGPL-3.0-or-later. See [LICENSE](https://github.com/nxm-rs/nectar/blob/main/LICENSE).
