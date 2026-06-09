<p align="center">
  <img src=".github/banner.svg" alt="Nexum · nectar — low-level Swarm primitives in Rust" width="100%" />
</p>

**Low-level Ethereum Swarm primitives in Rust** — the tedious bits that make the magic happen. Content addressing, chunk management, postage stamps, manifest tries, contract bindings.

Used by [`nxm-rs/vertex`](https://github.com/nxm-rs/vertex) (the Rust Swarm node) and available for anyone building Swarm-powered Rust applications who'd rather import a vetted primitives crate than re-implement the wire format.

> **Pre-release.** APIs may shift. Not yet on crates.io.

Looking for the org overview? See **[github.com/nxm-rs](https://github.com/nxm-rs)**.

---

## Crates

| Crate | What it is |
|---|---|
| **[`nectar-primitives`](./crates/primitives)** | Binary Merkle Tree, chunks, proofs · the foundation |
| **[`nectar-mantaray`](./crates/mantaray)** | Mantaray manifest trie · path-to-reference mapping |
| **[`nectar-postage`](./crates/postage)** | Postage stamp handling + verification |
| **[`nectar-postage-issuer`](./crates/postage-issuer)** | High-performance stamp issuance with parallel signing |
| **[`nectar-contracts`](./crates/contracts)** | Contract bindings for on-chain Swarm interactions |
| **[`nectar-swarms`](./crates/swarms)** | Network identifiers (mainnet, testnet, etc.) |

---

## Quick start

```toml
[dependencies]
nectar-primitives = { git = "https://github.com/nxm-rs/nectar", rev = "..." }
```

```rust
use nectar_primitives::{DefaultHasher, DefaultContentChunk};

let mut hasher = DefaultHasher::new();
hasher.set_span(data.len() as u64);
hasher.update(&data);
let root_hash = hasher.sum();
```

Until crates.io publishing lands, depend by git rev. Each crate will track its own version once the public API is stable.

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
