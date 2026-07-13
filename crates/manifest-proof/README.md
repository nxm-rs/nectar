# nectar-manifest-proof

Inclusion and exclusion proofs over a mantaray 1.0 manifest, authenticated
against a trusted root address.

A proof is the authenticated descent path for a key under a root: an ordered
chain of nodes, each authenticated against the reference the node before it
yielded. Inclusion terminates at the key's entry; exclusion terminates where the
descent provably cannot reach the key. Two granularities carry the same
authentication: full-chunk (whole node bytes, re-BMT by the verifier) and
BMT-segment (only the leading segments the descent reads, each behind its
sibling path). `verify` replays the descent over the authenticated bytes and
reports the verdict, so a proof cannot assert what its bytes do not.

Part of the verifiable-manifests set under the mantaray 1.0 epic.
