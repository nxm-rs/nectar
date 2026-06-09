<p align="center">
  <img src=".github/banner.svg" alt="Nexum · nectar — low-level Swarm primitives in Rust" width="100%" />
</p>

**Low-level Ethereum Swarm primitives in Rust** — the tedious bits that make the magic happen. Content addressing, chunk management, postage stamps, manifest tries, contract bindings.

Used by [`nxm-rs/vertex`](https://github.com/nxm-rs/vertex) (the Rust Swarm node) and available for anyone building Swarm-powered Rust applications who'd rather import a vetted primitives crate than re-implement the wire format.

> **Pre-release.** Published on crates.io; APIs may shift between minor versions until `1.0`.

Looking for the org overview? See **[github.com/nxm-rs](https://github.com/nxm-rs)**.

---

## Crates

| Crate | What it is | crates.io | docs.rs |
|---|---|---|---|
| **[`nectar-primitives`](./crates/primitives)** | Binary Merkle Tree, chunks, proofs · the foundation | [![crates.io](https://img.shields.io/crates/v/nectar-primitives.svg)](https://crates.io/crates/nectar-primitives) | [![docs.rs](https://docs.rs/nectar-primitives/badge.svg)](https://docs.rs/nectar-primitives) |
| **[`nectar-mantaray`](./crates/mantaray)** | Mantaray manifest trie · path-to-reference mapping | [![crates.io](https://img.shields.io/crates/v/nectar-mantaray.svg)](https://crates.io/crates/nectar-mantaray) | [![docs.rs](https://docs.rs/nectar-mantaray/badge.svg)](https://docs.rs/nectar-mantaray) |
| **[`nectar-postage`](./crates/postage)** | Postage stamp handling + verification | [![crates.io](https://img.shields.io/crates/v/nectar-postage.svg)](https://crates.io/crates/nectar-postage) | [![docs.rs](https://docs.rs/nectar-postage/badge.svg)](https://docs.rs/nectar-postage) |
| **[`nectar-postage-issuer`](./crates/postage-issuer)** | High-performance stamp issuance with parallel signing | [![crates.io](https://img.shields.io/crates/v/nectar-postage-issuer.svg)](https://crates.io/crates/nectar-postage-issuer) | [![docs.rs](https://docs.rs/nectar-postage-issuer/badge.svg)](https://docs.rs/nectar-postage-issuer) |
| **[`nectar-contracts`](./crates/contracts)** | Contract bindings for on-chain Swarm interactions | [![crates.io](https://img.shields.io/crates/v/nectar-contracts.svg)](https://crates.io/crates/nectar-contracts) | [![docs.rs](https://docs.rs/nectar-contracts/badge.svg)](https://docs.rs/nectar-contracts) |
| **[`nectar-swarms`](./crates/swarms)** | Network identifiers (mainnet, testnet, etc.) | [![crates.io](https://img.shields.io/crates/v/nectar-swarms.svg)](https://crates.io/crates/nectar-swarms) | [![docs.rs](https://docs.rs/nectar-swarms/badge.svg)](https://docs.rs/nectar-swarms) |

---

## Quick start

```sh
cargo add nectar-primitives
```

```rust
use nectar_primitives::{DefaultHasher, DefaultContentChunk};

let mut hasher = DefaultHasher::new();
hasher.set_span(data.len() as u64);
hasher.update(&data);
let root_hash = hasher.sum();
```

All crates share one workspace version and are released together. To track unreleased changes, depend by git rev instead:

```toml
nectar-primitives = { git = "https://github.com/nxm-rs/nectar", rev = "..." }
```

---

## Contributing

Open an issue before non-trivial PRs. Conventional Commits, `cargo fmt`, `cargo clippy -- -D warnings`. Tests for protocol-touching changes are non-optional — wire-format regressions are expensive to debug after the fact. CLA in [`CLA.md`](./CLA.md).

## Security

See [SECURITY.md](https://github.com/nxm-rs/.github/blob/main/SECURITY.md). Chunk hashing, postage-stamp verification, and manifest resolution findings via GitHub Security Advisories on this repo.

## License

AGPL-3.0-or-later. See [LICENSE](./LICENSE).

```
●  AGPL-3.0  ·  pre-release  ·  substrate under vertex
```
