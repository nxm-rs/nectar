# nectar-tasks

Object-safe task spawn seam: a `Spawn` trait over boxed unit futures with an abort-on-drop `TaskHandle`, a tokio spawner behind the `tokio` feature and a browser spawner behind the `wasm` feature. Thread-safety bounds relax on single-threaded targets via `nectar-marker`.

Part of the [nectar](https://github.com/nxm-rs/nectar) workspace, a collection of low-level Ethereum Swarm primitives in Rust. See the [workspace README](https://github.com/nxm-rs/nectar) for the full crate list and project context.

## Usage

```toml
[dependencies]
nectar-tasks = { version = "0.4", features = ["tokio"] }
```

## License

AGPL-3.0-or-later. See [LICENSE](https://github.com/nxm-rs/nectar/blob/main/LICENSE).
