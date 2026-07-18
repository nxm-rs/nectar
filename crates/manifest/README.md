# nectar-manifest

Mantaray 1.0: a content-addressed key-value manifest for Ethereum Swarm, built as a compacted radix-256 trie of content chunks. The frozen wire-format parameters are carried in the type system (a sealed `Format` trait with the `V1` parameter set), and bounded newtypes turn format limits into type invariants; the folder/website behaviour is a view over the KV core.

Part of the [nectar](https://github.com/nxm-rs/nectar) workspace, a collection of low-level Ethereum Swarm primitives in Rust. See the [workspace README](https://github.com/nxm-rs/nectar) for the full crate list and project context.

## Usage

```toml
[dependencies]
nectar-manifest = "0.4"
```

## License

AGPL-3.0-or-later. See [LICENSE](https://github.com/nxm-rs/nectar/blob/main/LICENSE).
