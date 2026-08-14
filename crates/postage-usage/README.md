# nectar-postage-usage

Self-hosted postage batch utilization snapshots: a compact, deterministic serialization of a batch's per-bucket slot counters, designed to be stored *inside the batch itself* as single-owner chunks (SOCs) at addresses derivable from the batch id alone.

The wire format is specified in [docs/spec/sbu1.md](https://github.com/nxm-rs/nectar/blob/main/docs/spec/sbu1.md).
That document is normative; this one is not.

## Motivation

Issuing postage stamps requires state. A batch of depth `d` and bucket depth `u` has `2^u` collision buckets, each with `2^(d-u)` storage slots. To issue a fresh stamp the issuer must know, per bucket, the next unused within-bucket index. Today that state lives in a node-local store, which chains the user to a single machine: lose the store and the batch becomes unsafe to issue from (re-issuing an index silently overwrites data on mutable batches and is rejected on immutable ones).

This crate implements a snapshot format for that state which is:

- **Compact.** Frame-of-reference bit packing sized by the *spread* of bucket fill levels, not by batch depth. Typical states fit in a handful of chunks; a freshly persisted empty batch fits in a 78-byte root.
- **Self-hosted.** Snapshot chunks are SOCs stamped by the very batch they describe. Their slot usage is recorded in the snapshot itself, and the recursion provably terminates.
- **Predictably addressed.** Chunk `n` of the snapshot has SOC id `keccak256("swarm-batch-usage" || batch_id || u16_be(n))` and owner equal to the batch owner. Anyone holding the batch id and owner address can locate, fetch, and verify the state. A user can roam between machines with nothing but their key and batch id.
- **Dilution-proof.** Increasing batch depth does not change any counter, any chunk boundary, or any byte of the leaf payloads. The structure grows only when the data does, so no slots are reserved up front for growth that may never happen.

Each snapshot chunk occupies exactly one storage slot for the lifetime of the batch, however many times the state is updated. That claim rests on an assumption about the reserve of the reference client, stated in section 10 of the specification.

## Choosing an issuance path

Where the issuer counters live depends on the batch and on whether you want them stored on the network. Pick the row that matches your batch and tracking model:

| Batch | Counters tracked | Issuance path |
|---|---|---|
| Immutable | Self-hosted (inside the batch) | Fill watermark. `Snapshot` over a `UsageTable::new(.., Mutability::Immutable)`; issue through `Snapshot::issuer`. |
| Mutable | Self-hosted (inside the batch) | Ring cursor. `Snapshot` over a mutable `UsageTable`; issue through `Snapshot::issuer`, which carves out the snapshot's own reserved slots. |
| Mutable | External (tracked outside the batch) | `nectar_postage_issuer::RingIssuer::external`. No usage state is stored in the batch, so there is nothing for this crate to persist; use it when the cursor lives in your own store. |

Self-hosted usage state, immutable or mutable, goes through this crate's `Snapshot`: that is the whole point, the state roams with the batch. Reach for `RingIssuer::external` only when the mutable issuance is tracked outside the batch and you do not want a self-hosted snapshot.

## Persistence cadence

The snapshot in memory is the only durable record of counter advances since the last persist. Issuing a stamp advances a counter; if the process exits before the next persist, those advances are lost, and re-issuing the same index silently overwrites data on a mutable batch (or is rejected on an immutable one). So:

- **Persist after a batch of issuance, and always before dropping the snapshot.** Batch the writes, then `revalidate` -> `plan_persist` -> `seal_plan` -> upload. The cadence is yours to tune (every N stamps, every T seconds), but a snapshot must never be dropped while `Snapshot::is_dirty` is true without a final persist.
- **Read the published-sequence floor live every time.** The floor handed to `Snapshot::revalidate` must come from a fresh network read of the live root chunk (parse it with `RootInfo::parse` and take `PublishedSequence::from(&root)`), never from a cache and never from the snapshot being persisted. A stale floor is exactly the value the floor exists to defeat; caching it reopens the downgrade window it closes.

A snapshot taken while issuance runs is a monotone under-approximation rather than a byte-exact instant, which is safe on an immutable batch and is why the cadence above is a tuning choice rather than a correctness one. Use `StampSink::pause` when an exact checkpoint is genuinely wanted. Section 11 of the specification gives the argument.

## Recovery

`Snapshot::new` is for a genuinely fresh, never-persisted table only: it starts the history at sequence 0 with no slots, so handing it a recovered table would downgrade the version at the snapshot's own chunk addresses and re-allocate colliding slots. Rebuild recovered or extracted state only through `Snapshot::from_parts`, which keeps the table, the sequence and the slots bound together. `RootInfo::assemble` does this for you when decoding from the network, and `Snapshot::into_parts` returns the same indivisible `SnapshotParts` value when you extract state from a live snapshot.

The API closes both in-memory routes that would otherwise downgrade a recovered snapshot, and the `PublishedSequence` floor on `Snapshot::revalidate` closes the persist-time route. The crate documentation gives the detail.

## Crate layout

- `UsageTable`: in-memory counters plus batch geometry, constructed for a batch and inspected through a read-only `TableView`. Slot assignment, dilution, and `merge_max` live on the owner-aware `Snapshot` handle. A table can be immutable (monotone fill watermarks) or mutable (wrapping ring cursors that skip the snapshot's reserved slots).
- `Snapshot`: a `UsageTable` plus persistence state (sequence, allocated snapshot-chunk slots).
- `SnapshotIssuer` (`issuer` feature): the sole `nectar_postage_issuer::StampIssuer`, owner-aware so it drops into `BatchStamper` while content stamping and snapshot allocation share one table and never collide; a bare `UsageTable` has no reserved set and is deliberately not an issuer, so it cannot evict the snapshot's own chunks.
- `Snapshot::revalidate` / `Validated::plan_persist`: `revalidate` admits the snapshot against a `PublishedSequence` floor read live from the network and returns the only handle that can plan a persist; `plan_persist` then runs the self-accounting fixed point and returns the payloads, SOC ids, and stamp indices to publish.
- `RootInfo::parse` / `RootInfo::assemble`: two-phase decode with full structural validation and digest verification.
- `usage_chunk_id` / `usage_chunk_address`: deterministic addressing.
- `seal` feature: turns a `PersistPlan` into signed `SingleOwnerChunk`s and `Stamp`s given a signer.

The golden vectors that pin the format live in `crates/postage-usage/tests/vector.rs`.
