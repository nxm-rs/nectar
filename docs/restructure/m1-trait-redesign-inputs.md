# M1 trait redesign inputs

This file captures the real callers of the traits that M1 removes, so the M2 redesign does not re-derive them.
It records what each trait is, who implements it outside nectar, and the seam decisions this review locked.
The source of the plan is `docs/RESTRUCTURE-PLAN.md`, milestone M1.
The captured node source is vertex main at `a8180ec1`; the captured nectar source is this tree at `0ece5710`.

## Scope

Six public traits leave the workspace in M1.
Each has zero production implementation inside nectar.
The premise holds in nectar and fails across the tree.
The node implements two of them and the browser client implements two more.
The capture below is the reference for the replacement in M2.
The node has no trait to implement in the interval between the deletion and the new seam.

| trait | location | out-of-tree caller |
|---|---|---|
| `StampValidator` | `crates/postage/src/validation.rs:42` | none; the live seam is the standalone `StoreValidator` |
| `BatchStore` | `crates/postage/src/store.rs:20` | the node `DbBatchStore`, which also implements `BatchEventHandler` |
| `SnapshotStore` | `crates/postage/src/snapshot_store.rs:53` | none; the node `peers` snapshot store is a name collision |
| `SnapshotSource` | `crates/postage-usage/src/client.rs:57` | the browser client `BrowserUsageSource` |
| `SnapshotSink` | `crates/postage-usage/src/client.rs:78` | the browser client `BrowserUsageSink` |
| `BatchFactory` | `crates/postage-issuer/src/factory.rs:42` | none; test doubles only |

## The real callers

### The node batch store and ingest

The node persists batches and drives ingest from the network.
`vertex` `crates/swarm/postage/src/store.rs` defines `DbBatchStore<DB: Database>`.
It implements `BatchStore` and `BatchEventHandler` (the `impl` blocks are at `store.rs:142` and `store.rs:211`).
The `BatchStore` half is a synchronous red-backed map keyed on `BatchId`, plus one global `PostageContext` row.
It carries the methods `get`, `put`, `remove`, `contains`, `batch_ids`, and `count`.
It also carries `context` and `set_context`, which read and write that single `PostageContext` row.
The store resolves an eligible batch through `BatchStoreExt::get_usable(&self, id, threshold)`.

The `BatchEventHandler` half is the ingest seam.
It is synchronous and drives one batch write per event variant.
`Created` stores the batch.
`TopUp` sets the batch `value` to the event `new_value`.
`DepthIncrease` currently sets only the batch `depth` to the event `new_depth`.
`Expired` removes the batch through the reserve's evict-then-acknowledge path.

The node consumers of the batch store are `db_reserve/store.rs`, `storer/src/expiry.rs`, `puller/src/verifier.rs`, and `builder/src/storer.rs`.

### The node admission composite

`vertex` `crates/swarm/postage/src/admission.rs` defines `AdmissionValidator`.
It holds one `confirmation_threshold: u64` and runs the full checklist against a batch the caller already fetched.
Its entry point is `validate(&self, stamp: &Stamp, address: &ChunkAddress, batch: &Batch, context: &PostageContext)`.
It maps the nectar `StampError` onto its own `AdmissionError`.
The `AdmissionError` variants are `UnknownBatch`, `BatchNotUsable`, `BatchExpired`, `OwnerMismatch`, and `Stamp`.
That mapping is lossy only at the top: `UnknownBatch` wraps a `BatchId`, `Stamp` wraps the source, and the two eligibility variants carry no fields.

### The browser client transport

`vertex` `bin/swarm-demo/src/client/usage.rs` defines the layer-2 client over the browser routing provider.
`BrowserUsageSource` implements `SnapshotSource`; its `fetch` reads a chunk's payload by its `ChunkAddress` and answers a tri-state.
`BrowserUsageSink` implements `SnapshotSink`; its `push` publishes one `SealedChunk`, which is a single-owner chunk with its stamp.
Both futures carry no `Send` bound, which is what lets the `!Send` browser transport implement the seam.
This file imports `SwarmAddress` (`usage.rs:10`), which is removed by the dead-alias work.

## The validation checklist

The checklist answers "can this node accept this chunk for this stamp".
It runs against a `Batch` and the store-global `PostageContext`.

Eligibility uses the `PostageContext`:
- `batch.is_usable(context.block(), threshold)`: the batch start is at least `threshold` confirms before the current block.
- `batch.is_expired(context.total_amount())`: the batch is not spent past the total amount the network has consumed.

Identity uses the `Batch` and the chunk address:
- batch id match: the stamp's batch id equals the batch id.
- `batch.validate_index(&stamp.index)`: the stamp index is within the batch depth and bucket bounds.
- `batch.validate_bucket(&stamp.index, &address)`: the stamp bucket equals the chunk address bucket under the batch bucket depth.
- `stamp.verify(&address, batch.owner())`: the stamp is a signature by the batch owner for this address.

The `PostageContext` is one store-global snapshot of `{ block, total_amount }`.
It is a single row, not a per-batch value.
The `Block` field drives eligibility and the `total_amount` drives expiry.
The `Batch` owns its own `owner`, `id`, `depth`, and `value`.
The `value` is the batch balance, updated by the `BatchEvent` variants.

## The three implementations

Three shapes of the checksum exist today.

- The nectar `StoreValidator<S: BatchStore>` at `crates/postage/src/validation.rs:112`.
  It is a two-argument `validate(stamp, address)`.
  It resolves the batch through `get_usable` on the threshold, then runs identity through `StampedAddress::from_parts(address, stamp).validate(&batch)`.
  It carries the issue and acceptance confirmation thresholds.
  It is the live seam and it stays as the replacement for the dead trait.

- The node `AdmissionValidator`.
  It is a four-argument `validate(stamp, address, &Batch, &PostageContext)`.
  It runs the same checklist against a batch the caller already fetched.

- The dead nectar `StampValidator` trait.
  It is a three-argument `validate(stamp, address, &PostageContext)` with a `validate_structure` default.
  It has zero implementors.
  The third argument carries the global context but no `Batch`, so the identity half cannot run.
  That is why no caller implements it.

## The four rulings

These are the validated direction for the M2 replacement.

1. The dilution rescale stays out of band.
   The contract computes the `normalisedBalance` and the ingest event carries it as `new_value`.
   The ingest handler applies `new_depth`, `new_value`, and `block` to the batch.
   The leaf validator reads only the post-application `value` and `depth`.

2. One composite owns the whole checklist.
   Eligibility and identity live in a single `validate`.
   The store-resolving caller and the batch-provided caller share it.

3. The composite takes a read-only `&Batch` and the `&PostageContext`.
   The store-resolving path is a convenience that fetches the batch and passes it to the same checklist.
   No `&mut Batch` crosses into validation.

4. One composite lives in nectar as the replacement for the dead trait.
   The node and the gRPC client delegate to it.
   The node `AdmissionValidator` and `AdmissionError` are dropped.
   The node maps onto the nectar `StampError`.

## The pre-M1 defect

The node ingest seam applies `DepthIncrease` as `batch.set_depth(new_depth)` only.
It drops the rescaled balance on `new_value` and the confirmation-gate block on `block`.
`TopUp` applies `new_value` but not a block, so the two variants diverge.
The replacement ingest handler must apply all three fields on `DepthIncrease`.
nectar carries the rescaled balance on the event.
The write site is the batch store.

## Cross-crate coupling to migrate

The node `BrowserUsageSource` and `BrowserUsageSink` import `SwarmAddress`.
`SwarmAddress` is a deprecated alias removed in this release.
The node must switch to `ChunkAddress` before `nectar-primitives` drops the alias.

The node has five names in its own `MaybeSend` family and zero `MaybeSync` sites.
Removing the three nectar `MaybeSend*` aliases means the node inlines two of them as bounds.

The node implements `BatchStore` and `BatchEventHandler` through `DbBatchStore`.
M1 removes both traits.
M2 restores the batch-store seam and the ingest seam in the api crate.
The node re-implements them there.

## Open for the next decision

- `SnapshotStore` and the `SnapshotSource`/`SnapshotSink` transport are the browser-client seam.
  They are candidates for one layer-2 client postage-usage store that web, embedded, and gRPC clients share.
  They are otherwise candidates for deletion if no client caches usage snapshots.
  The next decision sets whether M1 deletes them or defers them to that store.

- The gRPC client consumes the leaf validator and is a plan input.
  Its surface is not captured here yet.
