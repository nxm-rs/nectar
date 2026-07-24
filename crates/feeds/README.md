# nectar-feeds

Swarm feeds over single-owner chunks: an owner-signed, topic-keyed sequence of updates at deterministic chunk addresses. Every update is a plain single-owner chunk on the wire (`id = keccak256(topic || index)`, `address = keccak256(id || owner)`); the crate provides the feed identity, sequence indexing, an updater that signs and publishes through the chunk store traits, and a getter that fetches, certifies and locates the latest update.

Part of the [nectar](https://github.com/nxm-rs/nectar) workspace, a collection of low-level Ethereum Swarm primitives in Rust. See the [workspace README](https://github.com/nxm-rs/nectar) for the full crate list and project context.

## Usage

```toml
[dependencies]
nectar-feeds = "0.4"
```

## License

AGPL-3.0-or-later. See [LICENSE](https://github.com/nxm-rs/nectar/blob/main/LICENSE).
