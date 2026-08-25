# nectar-proof

The authentication layer for Swarm descent proofs: how one step's bytes bind to a trusted address, and the replay loop that walks the steps. It owns no key vocabulary, no verdict and no span arithmetic, and the verifier borrows rather than owns, so it is allocation-free in a guest.

Two proof kinds are in scope. A segment of a chunk, which [`Segment`](src/segment.rs) replays as the BMT segment proof from `nectar-primitives` against the chunk root. Membership of a key in an ldb store, implemented by the ldb core crate on the same seam.

A `no_std` crate on the six-crate proving lane, with a `std` feature.

Part of the [nectar](https://github.com/nxm-rs/nectar) workspace, a collection of low-level Ethereum Swarm primitives in Rust. See the [workspace README](https://github.com/nxm-rs/nectar) for the full crate list and project context.
