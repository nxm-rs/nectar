# nectar-envelope

Sealed messaging envelopes for Ethereum Swarm.
Two schemes share one frozen frame inside a chunk payload: the byte-frozen ECIES construction of the reference client, and RFC 9180 HPKE under the domain label `nectar/env/v1`.

Part of the [nectar](https://github.com/nxm-rs/nectar) workspace, a collection of low-level Ethereum Swarm primitives in Rust.
See the [workspace README](https://github.com/nxm-rs/nectar) for the full crate list and project context.

## Usage

```toml
[dependencies]
nectar-envelope = "0.4"
```

Opening and decap need no random number generator, so they build for `no_std` bare-metal targets.
Sealing needs one and rides the `encryption` feature.

## Stability

No nectar crate reads or writes this frame yet.
The key encapsulation mechanism, the frame and this API can change without a major version until a consumer pins them.

## License

AGPL-3.0-or-later. See [LICENSE](https://github.com/nxm-rs/nectar/blob/main/LICENSE).
