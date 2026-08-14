# nectar-postage

Postage stamp primitives for Ethereum Swarm: stamp and batch types, bucket math, and stamp verification (with optional parallel verification via rayon).

Part of the [nectar](https://github.com/nxm-rs/nectar) workspace, a collection of low-level Ethereum Swarm primitives in Rust. See the [workspace README](https://github.com/nxm-rs/nectar) for the full crate list and project context.

## Usage

```toml
[dependencies]
nectar-postage = "0.1"
```

This crate is `no_std` compatible (default features enable `std`).

## The reference client's "envelope"

The reference client calls a detached postage stamp an envelope.
`POST /envelope/{address}` creates a postage stamp for one chunk, and the response carries the batch ID, the bucket and index, the timestamp and the signature.
There is no Go type of that name; it is a route and a response name.
`Stamp` in this crate is that object, field for field.
The `envelope` module in `nectar-primitives` is unrelated: it is the HPKE encryption envelope.

## License

AGPL-3.0-or-later. See [LICENSE](https://github.com/nxm-rs/nectar/blob/main/LICENSE).
