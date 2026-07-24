# nectar-marker

Thread-safety markers for Ethereum Swarm crates: `MaybeSend`/`MaybeSync` bound `Send`/`Sync` on multi-threaded targets and relax to no bound on wasm32 or under the `unsync` feature (the single-thread escape for non-wasm targets such as zkVM guests).

Part of the [nectar](https://github.com/nxm-rs/nectar) workspace, a collection of low-level Ethereum Swarm primitives in Rust. See the [workspace README](https://github.com/nxm-rs/nectar) for the full crate list and project context.

## Usage

```toml
[dependencies]
nectar-marker = "0.4"
```

This crate is `no_std` and dependency-free.

## License

AGPL-3.0-or-later. See [LICENSE](https://github.com/nxm-rs/nectar/blob/main/LICENSE).
