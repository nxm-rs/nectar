# Spec references

Source citations for the algorithms and wire formats this crate ports from the reference client. Citations pin the reference client at tag `v2.8.1`; line numbers are valid at that tag and may drift on later revisions.

Module docs link here instead of restating citations inline.

## Overlay derivation

| Citation | Description |
|---|---|
| `pkg/crypto/crypto.go:45-57` | Overlay address is `keccak256(eth_address(20) \|\| network_id_le(8) \|\| nonce(32))`; the network id is little-endian in this hash. |

## Network identifiers

| Citation | Description |
|---|---|
| `pkg/config/chain.go` | Canonical network identifiers (mainnet `1`, testnet `10`). |

## Handshake sign-data

| Citation | Description |
|---|---|
| `pkg/bzz/address.go:138-160` | BzzAddress sign-data layout: magic prefix, underlay bytes, overlay (32), network id big-endian (8), nonce (32), timestamp big-endian (8), chequebook (20). The 14-byte magic prefix is declared at the top of this range. |

## Handshake timestamp

| Citation | Description |
|---|---|
| `pkg/bzz/timestamp.go` | Sign-data timestamp is a signed big-endian `int64` of unix seconds; verification rejects records outside a drift window from the local clock. |

## Kademlia parameters

| Citation | Description |
|---|---|
| `pkg/topology/kademlia/kademlia.go:54-56` | Default saturation (8), over-saturation (18), and bootnode over-saturation (20) peer counts. |

## Neighborhood depth

| Citation | Description |
|---|---|
| `pkg/topology/kademlia/kademlia.go:896-920` | `recalcDepth`: depth candidate is the shallowest unsaturated bin, then anchored by the low-watermark cumulative count over the deepest bins. |

## Single-owner chunks

| Citation | Description |
|---|---|
| `pkg/soc/soc.go` | Single-owner chunk semantics; the SOC address is `keccak256(id \|\| owner)`. |

## Chunk encryption

| Citation | Description |
|---|---|
| `pkg/encryption/chunk_encryption.go` | `ChunkEncrypter`: span and data are encrypted separately with different initial counters. |

## Transformed addresses

| Citation | Description |
|---|---|
| `pkg/storer/sample.go` | `transformedAddressCAC`: anchor-prefixed BMT over span and payload, the redistribution sampler's per-round re-hash. |
| `pkg/storer/sample_test.go` | Deterministic CAC parity vector: `TestSampleVectorCAC`. |
| `pkg/storageincentives/proof_test.go` | Deterministic SOC parity vector: `TestMakeInclusionProofsRegression`. |
