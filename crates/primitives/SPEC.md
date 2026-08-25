# Spec references

Source citations for the algorithms and wire formats this crate ports from the reference client. Citations pin the reference client at tag `v2.8.1`; line numbers are valid at that tag and may drift on later revisions.

Module docs link here instead of restating citations inline.

## Handshake sign-data

| Citation | Description |
|---|---|
| `pkg/bzz/address.go:138-160` | BzzAddress sign-data layout: magic prefix, underlay bytes, overlay (32), network id big-endian (8), nonce (32), timestamp big-endian (8), chequebook (20). The 14-byte magic prefix is declared at the top of this range. |

## Handshake timestamp

| Citation | Description |
|---|---|
| `pkg/bzz/timestamp.go` | Sign-data timestamp is a signed big-endian `int64` of unix seconds; verification rejects records outside a drift window from the local clock. |

## Chunk encryption

| Citation | Description |
|---|---|
| `pkg/encryption/chunk_encryption.go` | `ChunkEncrypter`: span and data are encrypted separately with different initial counters. |
