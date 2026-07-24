# nectar-clock

Injectable time source: one `Clock` trait (signed nanoseconds since the unix epoch), a `web-time`-backed system clock behind the `std` feature, and a deterministic `ManualClock` for tests.

Part of the [nectar](https://github.com/nxm-rs/nectar) workspace, a collection of low-level Ethereum Swarm primitives in Rust. See the [workspace README](https://github.com/nxm-rs/nectar) for the full crate list and project context.

## Usage

```toml
[dependencies]
nectar-clock = "0.4"
```

This crate is `no_std` compatible (default features enable `std`).

## License

AGPL-3.0-or-later. See [LICENSE](https://github.com/nxm-rs/nectar/blob/main/LICENSE).
