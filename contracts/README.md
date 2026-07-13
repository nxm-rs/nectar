# nectar contracts: MantarayProofVerifier

A Foundry project holding `MantarayProofVerifier`, a reusable Solidity library
that verifies mantaray 1.0 **inclusion** and **exclusion** proofs on-chain
against a trusted root address, at **BMT-segment** granularity.

The proofs it consumes are the deterministic output of the Rust
`nectar-manifest-proof` generator. Nothing here is hand-authored: the fixtures
under `test/fixtures/` are (re)written by that crate's `emit_solidity_fixtures`
test, and the byte layout below is the single contract both sides share.

## What it proves

- `verifyInclusion(root, key, value, proof) -> bool`: `key` maps to `value`
  under `root`.
- `verifyExclusion(root, key, proof) -> bool`: `key` is provably absent
  under `root`.

The library follows the key itself and accepts a node only when its address
equals the reference the previous authenticated hop supplied, so a whole proof
is a BMT hash chain anchored at the root. Per hop it re-hashes the shipped
leading BMT segments (each behind its seven-deep sibling path) rather than
parsing a whole node, reassembles the covered prefix, and replays the node-body
grammar the descent reads (preamble, flags, root entry, fork index, the followed
record). Soundness rests on canonicalness (spec 6.2): an authenticated node's
fork set is complete and its edges maximal, so local absence is global absence.
The exclusion terminal conditions are the same three the Rust verifier reports:
no fork slot for the next byte (the compact gap), a compacted edge that diverges
or that the key ends inside, or the key exhausting at a fork with no entry.

## Restricted on-chain profile

The library targets the frozen `tag_version 0x01` plaintext layout only:

- **Plaintext 32-byte references** (`ref32`). An encrypted 64-byte child
  reference is not followed; the descent stops as unsupported and both verdicts
  fail closed.
- **Non-spilled nodes.** A node whose flags mark it a spilled segment or a
  segment directory (bits 5 or 6) is rejected. The generator does not emit such
  proofs for this profile.
- **Subtree counts are skipped, not verified.** A referenced-child fork record
  trails an author-asserted subtree count; the descent bounds records by the
  fork-index offsets, so the count bytes are crossed but never read. Counted
  answers (rank, select, count, page) are out of scope here.

Out of profile, and what each would need:

- **Encrypted subtrees / `ref64`.** The descent would carry a decryption key and
  derive the child address from the encrypted reference, not read it verbatim; a
  ciphertext-authenticating step would replace the plain address follow.
- **`0x02` (read profile).** The same record grammar under a distinct preamble
  version with a heavier embedding budget; supporting it is accepting the
  version byte.
- **Counted answers.** Verifying rank, select, count or page on-chain would
  read the subtree counts the descent currently skips and carry the count
  trust-boundary the Rust `prove_count` notes describe.
- **Spilled segment directories on the path.** A node too large to hold its fork
  table inline spills it into a segment directory; verifying across one needs an
  extra authenticated indirection the current descent rejects.

## Rust-to-Solidity byte layout

All framing integers are **big-endian** (cheap to read in Solidity). The raw
mantaray node bytes carried inside each segment keep their own **little-endian**
u16 fields, which the on-chain descent reads as little-endian, byte-identical to
the Rust codec. The BMT sibling path is exactly seven levels (128 leaves), and
content chunks carry no BMT prefix, so none is transmitted.

```text
proof   := u32 n_steps || step[n_steps]
step    := u64 span || u32 n_seg || segment[n_seg]
segment := data[32] || sibling[7][32]        (segment_index is implicit = position)
```

`MantarayProofVerifier.decode(bytes) -> Proof` parses this wire. The counted
on-chain verifier reuses the same layout, so it is versioned here rather than in
a test.

The fixture files wrap a proof with its inputs so a test needs one
`readFileBinary`:

```text
fixture := root[32]
           u32 key_len   || key[key_len]
           u8  present               (1 = inclusion, 0 = exclusion)
           u32 value_len || value[value_len]   (value only when present)
           proof
```

## BMT verification

`_bmtRoot` mirrors nectar's prefix-free content-chunk BMT exactly: keccak256 up
the binary tree of 128 32-byte segments (`keccak(left || right)` per node, no
prefix), then `keccak(span_le[8] || tree_root)` where `span_le` is the node
payload length as eight little-endian bytes. A segment step authenticates one
segment with seven keccak; a covering run of `k` leading segments costs about
`7k` keccak plus the descent.

## Building and testing

Solidity is supplied by nix (no svm download). Run forge under a shell that has
both `foundry` and `solc`, and point forge at that solc:

```sh
nix-shell -p foundry solc --run '
  export FOUNDRY_SOLC=$(which solc)
  forge test
  forge snapshot            # refresh .gas-snapshot
'
```

The fixtures the tests load are regenerated from Rust with:

```sh
nix develop --command cargo test -p nectar-manifest-proof --test solidity_fixtures
```

`test/Harness.sol` vendors the tiny cheatcode interface and assertion helpers
the suite needs, so no `forge install` (and no network) is required.

## Gas

`forge snapshot` records per-test gas in `.gas-snapshot`. The figures include
fixture parsing (`decode` and the byte-wise slicing the harness does), so they
bound rather than isolate verification cost; a single embedded-node inclusion
sits well under the referenced multi-node paths, which scale with proof depth.
