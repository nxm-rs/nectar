# nectar-primitives-core

The chunk-verification core of [nectar-primitives](../primitives), carved out as a lean `no_std` crate for guests that only verify.

It holds the transitive closure needed to certify a chunk: the binary merkle tree (hasher, inclusion proofs), the content-addressed and single-owner chunk carriers with their acceptance rules, single-owner owner recovery, and the address, error and wire types those need.

Everything a verifier does not need stays in `nectar-primitives`: chunk stores, encryption, envelopes, ECIES, the routing metrics, and the wasm bindings.
`nectar-primitives` depends on this crate and re-exports every item at its original path, so consumers import from `nectar-primitives` as before.

## Features

- `std` (default): parallel and batched Keccak, plus the signer-driven constructors.
- `serde`: `Serialize`/`Deserialize` for the fixed-width types.
- `arbitrary`: `Arbitrary` impls and the valid-by-construction generators.
- `unsync`: relax the `MaybeSend`/`MaybeSync` markers on single-threaded non-wasm targets.

## Licence

AGPL-3.0-or-later.
