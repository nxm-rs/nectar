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

The canonical encoder picks `base = min(counts)` and the width minimizing the encoded byte size (packed bits, plus 8 bytes per exception, plus 32 bytes per leaf digest when the table is not inline), breaking ties toward the smaller width, subject to at most 128 exceptions. Decoders accept any structurally valid `(base, w, exceptions)`, so this policy can evolve without a format change. Because `w` tracks the bulk of the distribution rather than the maximum, a single hot bucket cannot inflate the table. For uniform uploads (the economically rational case) counters are binomially concentrated, so `w` stays small: a half-full depth-24 batch needs `w = 7` (15 chunks total) where raw u32 counters would take 256 KiB.

### Root payload

| offset | size | field |
|---|---|---|
| 0 | 4 | magic `"SBU1"` |
| 4 | 32 | batch id |
| 36 | 1 | batch depth `d` |
| 37 | 1 | bucket depth `u` (<= 16, `d - u` <= 31) |
| 38 | 1 | flags (bit 0: mutable batch; all other bits must be zero) |
| 39 | 1 | delta width `w` (<= 32) |
| 40 | 8 | sequence (monotone persist counter) |
| 48 | 8 | counter sum (immutable: lifetime stamps issued; mutable: a checksum over the cursors; must equal the sum of the decoded counters either way) |
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

### Worked example

A complete root payload, byte for byte, so the record structure can be reasoned about. This exact vector is pinned by `tests/vector.rs`, so the documentation cannot drift from the implementation.

Scenario: batch id `0x42` repeated, owner `0x11` repeated, depth 12, bucket depth 8 (256 buckets of 16 slots). Counters follow the pattern `count(b) = 3 + (b mod 4)`, except bucket 200 (0xc8) which is completely full at 16. Persisting allocates the snapshot's own root chunk a slot: its address hashes into bucket 41 (0x29), bumping that counter from 4 to 5 and `total_issued` to 1166.

The encoder picks `base = 3` and `w = 2` (deltas 0..=3 cover every bucket except the hot one, which becomes the single exception: 1 exception at 8 bytes beats widening 256 buckets to 4 bits). The packed table is 64 bytes and fits inline, so the whole snapshot is one 142-byte chunk:

```
offset  bytes                            field
0x00    53425531                         magic "SBU1"
0x04    4242..42 (32 bytes)              batch id
0x24    0c                               batch depth 12
0x25    08                               bucket depth 8
0x26    00                               flags
0x27    02                               delta width w = 2
0x28    0000000000000001                 sequence 1
0x30    000000000000048e                 total issued 1166
0x38    00000003                         base 3
0x3c    0001                             allocated count A = 1
0x3e    0000                             leaf count L = 0 (inline)
0x40    0001                             exception count E = 1
0x42    000000c8 00000010                exception: bucket 200, count 16
0x4a    00000004                         slot of snapshot chunk 0: index 4
0x4e    1b1b1b1b1b1b1b1b1b1b 2b 1b..1b   packed deltas, 64 bytes
0x80    db 1b1b1b1b1b1b1b1b1b1b1b1b1b
```

The packed section reads MSB first, two bits per bucket, four buckets per byte. Three byte values appear:

- `1b = 00 01 10 11`: deltas 0,1,2,3, the background `b mod 4` pattern (counts 3,4,5,6).
- `2b = 00 10 10 11` at offset 0x58 (buckets 40..43): bucket 41 reads delta 2 instead of 1, the root chunk's own stamp, recorded by the snapshot in the batch it is stored in.
- `db = 11 01 10 11` at offset 0x80 (buckets 200..203): bucket 200 carries the all-ones exception filler; its real count (16) lives in the exception entry above.

To recover the table: `count(b) = 3 + packed_delta(b)` for every bucket, then overlay `count(200) = 16`. The reader checks the sum against `total issued` (1165 from counters present before persist, plus 1 for the root's own stamp) and knows from the slot section that stamp index `(bucket 41, index 4)` belongs to the snapshot itself and must never be reused for another chunk.

### Worked example, large batch (multi-leaf)

At mainnet scale (`u = 16`) the packed table no longer fits inline, so the root carries keccak digests of leaf chunks instead. The second vector in `tests/vector.rs` pins this shape: batch depth 29, bucket depth 16 (65536 buckets of 8192 slots), counts `100 + (b mod 50)`, with bucket 0x1234 at 5000 and bucket 0xCBE5 completely full at 8192.

The encoder picks `base = 100` and `w = 6` (the two hot buckets become the exception list rather than forcing 13-bit counters on all 65536 buckets). The snapshot is 14 chunks, and its 554-byte root reads:

```
offset  size  field
0x000   66    header: magic, batch id, depth 1d, bucket depth 10, w = 06,
              sequence 1, total issued 0x7cb199 (8171929), base 0x64 (100),
              A = 14, L = 13, E = 2
0x042   16    exceptions: (0x1234, 5000), (0xcbe5, 8192)
0x052   56    14 slot entries: 105, 125, 145, ... the within-bucket index
              each snapshot chunk occupies, root first
0x08a   416   13 keccak digests, one per leaf payload
```

Each leaf is a plain slice of the same delta bitstream: `floor(32768 / 6) = 5461` buckets per leaf, so twelve full 4096-byte leaves and a final 3-byte leaf carrying the last 4 buckets (24 bits exactly). Leaf 0 opens `00 10 83 10 51 87 20 92`, which is just deltas 0,1,2,3,... at 6 bits MSB-first; the hot buckets sit in leaves 0 and 9 as all-ones filler.

Points worth reasoning from this vector:

- The root's own slot is 105: the watermark of its (hash-determined) bucket at allocation time, `100 + (0x296d mod 50)`. Snapshot slots are ordinary stamps drawn from the same counters they record.
- Pinning the root pins everything: the digests bind every leaf byte, so a reader holding the root either reconstructs this exact table or fails with a digest mismatch. There is no version skew across the 14 chunks.
- `total issued` (8171929) equals the counter sum including the 14 stamps the snapshot spent on itself: state storage costs 14 slots out of 2^29, about 0.0000026%.

### Self-accounting and bounded recursion

Persisting the snapshot stamps its own chunks, which increments counters, which can change the encoding. The planner runs this to a fixed point: allocate a slot for any snapshot chunk not yet allocated, fold the increment into the table, re-encode, repeat. Termination: allocation is monotone, slot indices are reused forever after first allocation (so steady-state persists allocate nothing), and `L` is bounded above (64 leaves at `w = 32`, `u = 16`), so the loop runs at most a handful of iterations and in practice one. The `allocated count` never shrinks even if a later, smaller encoding needs fewer leaves; this guarantees a leaf that reappears reuses its original slot instead of burning a new one.

Worst case the snapshot costs 65 slots out of at least `2^17`: under 0.05% of capacity, and under one millionth for realistic depths.

### Dilution

Dilution doubles `2^(d-u)` but changes no counter. The new depth is written to the root header on the next persist; leaf bytes are untouched. `w` grows only if and when the counter spread grows, one leaf at a time, with new leaf chunks allocating their single slot on first appearance. Nothing is reserved ahead of time.

### Immutable batches

The default, and the simplest case. Each counter is a monotone fill watermark: `count(b)` is the next unused index in bucket `b`, and issuing a stamp returns `count(b)` then increments it. A bucket is full at `2^(d-u)` and issuance there fails rather than overwriting. The snapshot's own chunks draw their slots from the same watermark, so they sit below every future watermark forever: fresh issuance cannot collide with them by construction. The counter sum is the lifetime stamp count, dilution changes no counter, and `merge_max` is a valid join (counters only ever rise).

### Mutable batches

A mutable batch is a per-bucket ring buffer: once a bucket fills, the issuer wraps its cursor back to `0` and overwrites the oldest chunk, so the bucket churns instead of rejecting. The snapshot models this by reinterpreting each counter as a **ring cursor** in `[0, 2^(d-u)]`: the next index to write, wrapping at capacity. Writing at the cursor evicts the chunk currently in that slot, which is the oldest live chunk because writes advance in cursor order; the monotone stamp timestamp is what authorizes the network to replace the evicted chunk's stamp at that `(bucket, index)`. Position selects the victim, the timestamp makes the overwrite valid on the wire.

Flags byte bit 0 marks the snapshot mutable, so a reader interprets the counters as cursors rather than fills. A reader that predates the flag rejects the snapshot (any nonzero flag byte is rejected), so a mutable snapshot is never silently misread as an immutable one.

Two consequences follow for the snapshot's own chunks:

- **Reserved slots are carved out of the ring.** A snapshot chunk sits at a fixed index but is re-stamped with a fresh timestamp on every persist, so a naive position-based FIFO would treat it as the oldest slot and evict the very data that records the batch state. The cursor therefore skips every index in the root's allocated section: a bucket holding `r` reserved slots is a ring of length `2^(d-u) - r`. This promotes `Snapshot::reserved_stamp_indices` / `Snapshot::is_reserved` from advisory hints to an invariant enforced inside issuance. At minimum depth (`d - u = 1`, two slots per bucket) a bucket containing a reserved slot is a one-deep ring that churns on every write; the geometry still permits it.
- **`merge_max` is immutable-only.** A cursor is not monotone (it falls on wrap), so two divergent copies cannot be reconciled by elementwise maximum. `merge_max` rejects mutable tables. Mutable divergence is a genuine conflict, surfaced by the `sequence` number, not silently joined.

The counter sum is read accordingly. For an immutable batch it is the lifetime stamp count and the decoder checks it exactly. For a mutable batch the cursors sum to nothing semantic (a wrapped bucket is fully occupied yet its cursor may be small), so the field is a deterministic checksum over the cursor table: the decoder still recomputes and verifies it, catching corruption, but it is not a utilization figure. Exact per-bucket occupancy for a wrapped bucket is the full capacity; surfacing it precisely would need one saturated bit per bucket and is a candidate v1.x extension.

Dilution is still free. Raising the depth enlarges every ring without touching a cursor: a bucket that had wrapped simply gains headroom above its current cursor and stops evicting until it fills again. No counter changes and no leaf byte changes, exactly as for immutable batches. Storing the cursor rather than a lifetime count is what buys this: a lifetime count would have to be remapped modulo the new capacity on every dilution.

Recovery is cursor-only. As with the reference issuer, the snapshot records where each bucket's cursor points, not which chunk occupies which slot (tracking that would cost a hash per live stamp). Recovering a mutable batch on a new machine restores correct issuance order and protects the metadata slots; it does not enumerate live content. A managed free-list of deliberately released slots remains a candidate format extension (the flags byte and version magic exist for it) and is out of scope for version 1.

### Same-address re-stamping

For both batch types, re-publishing the same address with the same slot and a newer timestamp replaces the payload in place (the snapshot's own chunks, and feeds, rely on this). For user-owned single-owner chunks no local state is needed: the live chunk's stamp is stored with it on the network, so the slot to reuse is recoverable by fetching the current version.

### Concurrency

The format is single-writer. On an immutable batch counters are monotone, so the elementwise maximum of two divergent tables is a well-defined join and is provided as a recovery primitive (`merge_max`); it still cannot retroactively resolve two writers having issued the same index, and true multi-writer coordination is out of scope for version 1. On a mutable batch the counters are wrapping cursors and have no monotone join, so `merge_max` rejects mutable tables and divergence must be resolved by sequence. The `sequence` field makes divergence detectable for both: readers take the higher sequence, and equal sequences with different content signal a conflict.

### Recovery and the sequence

A recovered snapshot carries a sequence and a set of allocated slots that a fresh persist must preserve. Rebuild recovered or extracted state only through `Snapshot::from_parts`, which keeps the table, the sequence, and the slots bound together. `RootInfo::assemble` does this for you when decoding from the network, and `Snapshot::into_parts` returns the same indivisible `SnapshotParts` value when you extract state from a live snapshot. `Snapshot::new` is for a genuinely fresh, never-persisted table only: it starts the history at sequence 0 with no slots, so handing it a recovered table would downgrade the version at the snapshot's own chunk addresses and re-allocate colliding slots, overwriting a newer persisted version in place.

The API closes both in-memory routes that would otherwise downgrade a recovered snapshot. The move route is closed because `SnapshotParts` holds its table privately and yields it only by borrow. The clone route is closed because `Snapshot::table` and `SnapshotParts::table` return a borrowed `TableView` rather than `&UsageTable`: the view exposes the counters and geometry a caller needs to inspect, but only borrows the table and does not deref to it, so cloning or copying it produces another view, never an owned `UsageTable` that `Snapshot::new` would accept. No public API hands out an owned table taken from a recovered snapshot.

Two residual paths to a sequence-0 persist are protocol-level rather than in-memory representability concerns, so the type guards here do not close them; the `PublishedSequence` floor on `Snapshot::revalidate` does (issue #70). First, the public table constructors (`UsageTable::new` and friends) must keep minting a fresh table for a genuinely new batch, so a forged fresh table persisted at sequence 0 is caught by the floor, not by the type system here. Second, the reserve overwrites a snapshot chunk by stamp *timestamp*, not by snapshot *sequence*, so complete cross-version protection against a stale persist requires a compare-and-swap of the persisted sequence against the live root chunk's sequence. `Snapshot::revalidate` performs exactly that compare-and-swap: the consumer reads the published sequence live from the root chunk, hands it in as the floor, and a persist whose next sequence does not strictly exceed it is rejected with `UsageError::StaleSequence`. This crate closes the in-memory clone and move downgrade routes, the floor closes the persist-time downgrade, and the `checked_add` guard ahead of the floor check keeps a single writer's sequence from silently wrapping.

## Crate layout

- `UsageTable`: in-memory counters plus batch geometry, constructed for a batch and inspected through a read-only `TableView`. Slot assignment, dilution, and `merge_max` now live on the owner-aware `Snapshot` handle. A table can be immutable (monotone fill watermarks) or mutable (wrapping ring cursors that skip the snapshot's reserved slots).
- `Snapshot`: a `UsageTable` plus persistence state (sequence, allocated snapshot-chunk slots).
- `SnapshotIssuer` (`issuer` feature): the sole `nectar_postage_issuer::StampIssuer`, owner-aware so it drops into `BatchStamper` while content stamping and snapshot allocation share one table and never collide; a bare `UsageTable` has no reserved set and is deliberately not an issuer, so it cannot evict the snapshot's own chunks.
- `Snapshot::revalidate` / `Validated::plan_persist`: `revalidate` admits the snapshot against a `PublishedSequence` floor read live from the network and returns the only handle that can plan a persist; `plan_persist` then runs the self-accounting fixed point and returns the payloads, SOC ids, and stamp indices to publish.
- `RootInfo::parse` / `RootInfo::assemble`: two-phase decode with full structural validation and digest verification.
- `usage_chunk_id` / `usage_chunk_address`: deterministic addressing.
- `seal` feature: turns a `PersistPlan` into signed `SingleOwnerChunk`s and `Stamp`s given a signer.
