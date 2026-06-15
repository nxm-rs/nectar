# BMT formal verification

A machine-checked model of the Swarm Binary Merkle Tree (BMT) inclusion proof,
plus a runnable differential test that pins the production Rust to the model.

This is the first slice of the verification effort: prove the *kernel that every
chunk hashes through*, end to end, with a prover (F\*), a reference, and a test
that keeps the optimised code honest.

## Why this kernel

The BMT hash is the Swarm content-address function. A divergence here is
catastrophic: a node that computes addresses differently stores and serves the
wrong chunks, fails inclusion proofs / proof-of-custody, and forks itself off the
network. It is also pure, finite, and deterministic — the ideal first target.

## What is proved

`Bmt.fst` models the BMT exactly as `nectar-primitives::bmt` implements it and as
the Book of Swarm specifies it (see *Citations* below), and proves:

- **`completeness`** — an honestly generated proof for any leaf verifies against
  the genuine root.
- **`soundness`** — assuming keccak is collision-free (`keccak2_injective`), the
  *only* segment value that can verify at a given index against the genuine root
  is the genuine leaf. A forged proof for a different segment therefore exhibits a
  keccak collision.

Both lemmas are discharged with **no `admit`s**. The elementary index-arithmetic
helpers (`high_bit_preserves_low`, `low_index_top_bit`, `verify_path_low`) are
fully proved, not assumed.

### Trust base (the only assumptions)

The model assumes exactly three things, all explicit `assume`s at the top of
`Bmt.fst`:

1. `hash` — an abstract 32-byte node type.
2. `keccak2 : hash -> hash -> hash` — keccak256 of two concatenated 32-byte
   nodes. We do **not** re-verify keccak; the production code uses a well-tested
   implementation (alloy `asm-keccak`).
3. `keccak2_injective` — keccak is collision-free on 64-byte inputs. Soundness is
   proved *relative to* this; it is the standard cryptographic assumption.

Everything else (tree structure, proof folding, the span wrap, the LSB index
convention) is proved.

## How the model maps to the code

| Book of Swarm / `nectar-primitives::bmt`            | `Bmt.fst`                |
| --------------------------------------------------- | ------------------------ |
| 128 `=` 2^7 segments of 32 bytes, zero-padded to 4K | `tree 7`, `Leaf`/`Node`  |
| `keccak256(left ‖ right)` internal node             | `keccak2`                |
| BMT root over the segments                          | `root`                   |
| sibling path, leaf-first                            | `siblings`               |
| verify: bit *i* from **LSB**, even ⇒ `keccak(cur‖sib)` | `verify_path` (`idx % 2`) |
| final address `= keccak256(span_le_u64 ‖ root)`     | modelled at the wrap step |

The production hasher's zero-tree rollup, all-zeros fast path, and rayon
parallelism are *optimisations* of this structure — they are not modelled here
because the differential test (below) proves they compute the same function.

## The differential test (runs today)

`crates/primitives/src/bmt/spec_equivalence.rs` is the executable shadow of this
model. It defines a deliberately naive, brute-force BMT reference (the spec made
executable) and asserts the production `Hasher` equals it:

- 512 random `(data, span)` cases,
- every boundary size (0, 1, 31/32/33, 63/64/65, 4095/4096/4097, …),
- the all-zeros fast path,
- proof round-trips for all 128 segment indices, with tamper-detection.

```
nix develop -c cargo test -p nectar-primitives --lib bmt::spec_equivalence
```

**Division of labour:** F\* proves the *reference* satisfies proof
soundness/completeness; the Rust test proves the *production code* equals the
reference. Together: the optimised hasher inherits the proved properties, and can
be optimised further (SIMD, `unsafe`, new parallelism) as long as the test stays
green — that is the "safe max performance" loop.

## Running the proof

```
make verify     # type-check + prove Bmt.fst with F*
```

Requires F\* (provided ephemerally via `nix shell nixpkgs#fstar`; the Makefile
wraps this). No global install needed.

## Next increments

1. **OCaml oracle.** Extract the model's executable core to OCaml
   (`fstar.exe --codegen OCaml`) with a keccak256 realisation, and run it as an
   external differential oracle in the Rust test — replacing the hand-written
   naive reference with a proof-extracted one, closing the last trust gap.
2. **Span/length semantics.** Model the relationship between `span` and the
   payload length, and the single-owner-chunk (SOC) address wrap.
3. **File-level BMT.** Lift to the chunked file hash (intermediate chunks /
   the Swarm hash tree) so inclusion proofs compose across a whole file.

## Citations (Book of Swarm)

`vertex/docs/swarm/reference/book-of-swarm.txt`, §2.2.2:

- §2.7/2.8: "An at most 4KB payload with a 64-bit little-endian encoded span
  prepended … The content address of the chunk is the hash of the byte slice that
  is the span and the BMT root of the payload concatenated."
- "the BMT chunk address is the hash of the 8-byte span and the root hash of a
  binary Merkle tree (BMT) built on the 32-byte segments … padded with all zeros
  up to 4096 bytes."
- §2.9 (inclusion proofs): "The side on which proof item *i* needs to be applied
  depends on the *i*-th bit (starting from least significant) of the binary
  representation of the index. Finally, the span is prepended and the resulting
  hash should match the chunk root hash."
