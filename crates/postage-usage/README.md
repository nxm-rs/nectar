# nectar-postage-usage

Self-hosted postage batch utilization snapshots: a compact, deterministic serialization of a batch's per-bucket slot counters, designed to be stored *inside the batch itself* as single-owner chunks (SOCs) at addresses derivable from the batch id alone.

## Motivation

Issuing postage stamps requires state. A batch of depth `d` and bucket depth `u` has `2^u` collision buckets, each with `2^(d-u)` storage slots. To issue a fresh stamp the issuer must know, per bucket, the next unused within-bucket index. Today that state lives in a node-local store, which chains the user to a single machine: lose the store and the batch becomes unsafe to issue from (re-issuing an index silently overwrites data on mutable batches and is rejected on immutable ones).

This crate defines a snapshot format for that state which is:

- **Compact.** Frame-of-reference bit packing sized by the *spread* of bucket fill levels, not by batch depth. Typical states fit in a handful of chunks; a freshly persisted empty batch fits in a 78-byte root.
- **Self-hosted.** Snapshot chunks are SOCs stamped by the very batch they describe. Their slot usage is recorded in the snapshot itself, and the recursion provably terminates.
- **Predictably addressed.** Chunk `n` of the snapshot has SOC id `keccak256("swarm-batch-usage" || batch_id || u16_be(n))` and owner equal to the batch owner. Anyone holding the batch id and owner address can locate, fetch, and verify the state. A user can roam between machines with nothing but their key and batch id.
- **Dilution-proof.** Increasing batch depth does not change any counter, any chunk boundary, or any byte of the leaf payloads. The structure grows only when the data does, so no slots are reserved up front for growth that may never happen.

## Why this works on the network

Snapshot chunks are SOCs whose ids, and therefore addresses, never change. The Swarm reserve allows a chunk with the same address and the same stamp index to be overwritten by a version carrying a newer stamp timestamp, regardless of batch mutability, replacing the SOC payload in place. Consequently each snapshot chunk consumes **exactly one storage slot for the lifetime of the batch**, no matter how many times the state is updated. The slot is assigned on first allocation, recorded in the root chunk, and reused for every subsequent persist.

## Format (version 1)

All integers are big-endian. The snapshot consists of a **root** payload (chunk `n = 0`) and zero or more **leaf** payloads (chunks `n = 1..=leaf_count`), each at most 4096 bytes.

### Counter encoding

Per-bucket counters are encoded with a patched frame-of-reference scheme:

- A `base` (u32), at most the minimum counter value.
- A `width` `w` (0..=32): each bucket's delta `count - base` is stored as a `w`-bit value.
- An **exception list** (at most 128 entries) of `(bucket: u32, count: u32)` pairs for outlier buckets whose delta does not fit in `w` bits. Exceptions store the absolute count; their packed `w`-bit slot is filled with all one bits and ignored on decode.

The canonical encoder picks `base = min(counts)` and the smallest `w` such that at most 128 buckets overflow. Because `w` tracks the bulk of the distribution rather than the maximum, a single hot bucket cannot inflate the table. For uniform uploads (the economically rational case) counters are binomially concentrated, so `w` stays small: a half-full depth-24 batch needs `w = 7` (15 chunks total) where raw u32 counters would take 256 KiB.

### Root payload

| offset | size | field |
|---|---|---|
| 0 | 4 | magic `"SBU1"` |
| 4 | 32 | batch id |
| 36 | 1 | batch depth `d` |
| 37 | 1 | bucket depth `u` (<= 16, `d - u` <= 31) |
| 38 | 1 | flags (must be zero) |
| 39 | 1 | delta width `w` (<= 32) |
| 40 | 8 | sequence (monotone persist counter) |
| 48 | 8 | total stamps issued (must equal the counter sum) |
| 56 | 4 | base |
| 60 | 2 | allocated count `A` (snapshot chunks ever allocated, >= leaf_count + 1) |
| 62 | 2 | leaf count `L` |
| 64 | 2 | exception count `E` (<= 128) |

followed by exactly:

1. `E` exceptions, 8 bytes each, strictly ascending by bucket.
2. `A` slot entries, 4 bytes each: the within-bucket stamp index assigned to snapshot chunk `n` (entry 0 is the root's own slot).
3. If `L = 0`: the packed delta bitstream inline (only possible when it fits in the root).
   If `L > 0`: `L` leaf entries, 32 bytes each: `keccak256(leaf payload)`. Leaf byte lengths are fully determined by `u` and `w`, so only the digest is stored.

The root is the commit point: leaf digests bind the exact leaf bytes, so a reader always reconstructs a consistent snapshot or detects staleness, even if the network serves a mix of old and new leaf versions.

### Leaf payloads

With `w > 0`, each leaf holds `B = floor(32768 / w)` buckets' deltas, MSB-first bit packed; leaf `n` covers buckets `[(n-1) * B, min(n * B, 2^u))`. The final byte of a leaf is zero-padded. `L = ceil(2^u / B)`.

### Self-accounting and bounded recursion

Persisting the snapshot stamps its own chunks, which increments counters, which can change the encoding. The planner runs this to a fixed point: allocate a slot for any snapshot chunk not yet allocated, fold the increment into the table, re-encode, repeat. Termination: allocation is monotone, slot indices are reused forever after first allocation (so steady-state persists allocate nothing), and `L` is bounded above (64 leaves at `w = 32`, `u = 16`), so the loop runs at most a handful of iterations and in practice one. The `allocated count` never shrinks even if a later, smaller encoding needs fewer leaves; this guarantees a leaf that reappears reuses its original slot instead of burning a new one.

Worst case the snapshot costs 65 slots out of at least `2^17`: under 0.05% of capacity, and under one millionth for realistic depths.

### Dilution

Dilution doubles `2^(d-u)` but changes no counter. The new depth is written to the root header on the next persist; leaf bytes are untouched. `w` grows only if and when the counter spread grows, one leaf at a time, with new leaf chunks allocating their single slot on first appearance. Nothing is reserved ahead of time.

### Concurrency

The format is single-writer. Counters are monotone, so the elementwise maximum of two divergent tables is a well-defined join and is provided as a recovery primitive (`merge_max`), but it cannot retroactively resolve two writers having issued the same index; true multi-writer coordination is out of scope for version 1. The `sequence` field makes divergence detectable: readers take the higher sequence, and equal sequences with different content signal a conflict.

## Crate layout

- `UsageTable`: in-memory counters plus batch geometry; implements slot assignment, dilution, and `merge_max`. With the `issuer` feature it implements `nectar_postage_issuer::StampIssuer`, so it drops into `BatchStamper` directly.
- `Snapshot`: a `UsageTable` plus persistence state (sequence, allocated snapshot-chunk slots).
- `Snapshot::plan_persist`: runs the self-accounting fixed point and returns the payloads, SOC ids, and stamp indices to publish.
- `RootInfo::parse` / `RootInfo::assemble`: two-phase decode with full structural validation and digest verification.
- `usage_chunk_id` / `usage_chunk_address`: deterministic addressing.
- `seal` feature: turns a `PersistPlan` into signed `SingleOwnerChunk`s and `Stamp`s given a signer.
