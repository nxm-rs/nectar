# Spec references

Source citations for the algorithms this crate ports from the reference client. Citations pin the reference client at `a17e3a9c` (master, 2025-12-26), except the overlay, neighborhood-depth, single-owner-chunk and transformed-address sections, which pin tag `v2.8.1`. Line numbers are valid at the pinned revisions and may drift on later ones.

Module docs link here instead of restating citations inline.

## Span level decoding

| Citation | Description |
|---|---|
| `pkg/file/redundancy/span.go:13-34` | A redundancy-enabled upload packs the level into the span's most significant byte as `level | 0x80`; `DecodeSpan` returns the level and the span with byte 7 zeroed; `IsLevelEncoded` is the strict `span[7] > 128` predicate, so a byte 7 of exactly `0x80` is not treated as encoded and its value is kept as a plain length. |

## Overlay derivation

| Citation | Description |
|---|---|
| `pkg/crypto/crypto.go:45-57` | Overlay address is `keccak256(eth_address(20) \|\| network_id_le(8) \|\| nonce(32))`; the network id is little-endian in this hash. |

## Neighborhood depth

| Citation | Description |
|---|---|
| `pkg/topology/kademlia/kademlia.go:896-920` | `recalcDepth`: depth candidate is the shallowest unsaturated bin, then anchored by the low-watermark cumulative count over the deepest bins. |

## Single-owner chunks

| Citation | Description |
|---|---|
| `pkg/soc/soc.go` | Single-owner chunk semantics; the SOC address is `keccak256(id \|\| owner)`. |

## Transformed addresses

| Citation | Description |
|---|---|
| `pkg/storer/sample.go` | `transformedAddressCAC`: anchor-prefixed BMT over span and payload, the redistribution sampler's per-round re-hash. |
| `pkg/storer/sample_test.go` | Deterministic CAC parity vector: `TestSampleVectorCAC`. |
| `pkg/storageincentives/proof_test.go` | Deterministic SOC parity vector: `TestMakeInclusionProofsRegression`. |
