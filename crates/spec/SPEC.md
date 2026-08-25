# Spec references

Source citations for the algorithms this crate ports from the reference client. Citations pin the reference client at tag `v2.8.1`; line numbers are valid at that tag and may drift on later revisions.

Module docs link here instead of restating citations inline.

## Network identifiers

| Citation | Description |
|---|---|
| `pkg/config/chain.go` | Canonical network identifiers (mainnet `1`, testnet `10`). |

## Kademlia parameters

| Citation | Description |
|---|---|
| `pkg/topology/kademlia/kademlia.go:54-56` | Default saturation (8), over-saturation (18), and bootnode over-saturation (20) peer counts. |
