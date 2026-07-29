# nectar-governor

Bounded-admission governor beneath the streaming walkers: the shared `Driver` loop over a `WalkPolicy`, the `FuturesUnordered` in-flight set it drains, the read-ahead `Window`, the head-slot `Admission` predicate, the `AdmitPolicy` adaptive-window seam, and the `BoxFuture` alias whose `Send` bound relaxes on single-threaded targets.

Part of the [nectar](https://github.com/nxm-rs/nectar) workspace, a collection of low-level Ethereum Swarm primitives in Rust. See the [workspace README](https://github.com/nxm-rs/nectar) for the full crate list and project context.

## Usage

```toml
[dependencies]
nectar-governor = "0.4"
```

This crate is `no_std` (alloc only). The `chunk` feature adds a chunk-typed fetch helper over the primitives store surface.

## License

AGPL-3.0-or-later. See [LICENSE](https://github.com/nxm-rs/nectar/blob/main/LICENSE).
