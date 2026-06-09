# nectar-mantaray

Mantaray manifest trie for Ethereum Swarm. Maps human-readable paths (e.g. `index.html`, `img/logo.png`) to content-addressed chunk references, with XOR obfuscation, versioned binary serialisation, per-path metadata, and efficient partial updates via lazy loading and dirty-reference tracking.

Part of the [nectar](https://github.com/nxm-rs/nectar) workspace, a collection of low-level Ethereum Swarm primitives in Rust. See the [workspace README](https://github.com/nxm-rs/nectar) for the full crate list and project context.

## Usage

```toml
[dependencies]
nectar-mantaray = "0.1"
```

## License

AGPL-3.0-or-later. See [LICENSE](https://github.com/nxm-rs/nectar/blob/main/LICENSE).
