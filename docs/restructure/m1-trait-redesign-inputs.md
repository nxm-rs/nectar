# M1 trait redesign inputs

Captured 2026-08-24, per the M1 ordering rule "capture the caller before you delete."

This document holds the redesign inputs for the six-trait deletion (#824, epic #799, milestone
M1 of `docs/RESTRUCTURE-PLAN.md`). It lets M2 write the replacement against the real callers
instead of against a trait that nothing in the tree implements.
The deletion itself is M1.
The redesign lands in M2, when `nectar-postage-api` is created fresh.

## Baseline

`nectar` `main` at `0ece5710`.
`vertex` `main` at `a8180ec1`, which pins `nectar` at `ef89a5df` through a single shared git source
(`Cargo.toml:148-155`).
`ef89a5df` is behind `main`: `nectar` changed the `DepthIncrease` event between the two revs,
and that divergence is the load-bearing fact in this document.

## The premise correction

The issue premise is that the six traits have no production implementation inside `nectar`.
That holds inside `nectar` and fails across the tree.
Three of the six have real out-of-tree implementors; the other three are pure deletions.

| Trait | In `#824`'s six | Real implementor | Disposition |
|---|---|---|---|
| `BatchStore` | yes | `vertex` `DbBatchStore` | delete in M1, redesign as the store seam in M2 |
| `SnapshotSource` | yes | `vertex` browser `BrowserUsageSource` | delete in M1, redesign as the stamper seam in M2 |
| `SnapshotSink` | yes | `vertex` browser `BrowserUsageSink` | delete in M1, redesign as the stamper seam in M2 |
| `SnapshotStore` | yes | none | pure delete in M1 |
| `BatchFactory` | yes | none | pure delete in M1 |
| `StampValidator` | yes | none | pure delete in M1 |
| `BatchEventHandler` | no | `vertex` `DbBatchStore` | not a `#824` deletion; the ingest half of the store seam, captured for the M2 redesign |

`BatchEventHandler` is captured because the same `vertex` type implements it alongside `BatchStore`
and together they are the batch-store-and-ingest seam; it is a redesign input, not a `#824`
deletion.
`StampValidator` is the dead one: zero implementors, and the wrong arity for its only real caller
(`validate` takes three arguments where the live `StoreValidator::validate` takes two).

`SnapshotStore` has no real implementor.
`vertex` does have a `DbPeerSnapshotStore`, but it is a name collision in a different crate and does
not implement `nectar` `SnapshotStore`.
Its layer-2 stamp-index design is M4 donation material, not an M2 redesign input, so it is a pure
delete here.

## Seam one: the batch store and the chain-ingest seam

`vertex` `crates/swarm/postage/src/store.rs` implements both traits on one type.
The type persists the node batch set and drives the on-chain ingest.

### The persist half, `BatchStore`

```rust
pub struct DbBatchStore<DB: Database> {
    db: Arc<DB>,
}
```

`Clone` is hand-written so it shares the one `Arc<DB>` without bounding `DB` (the backend is never
`Clone`); every clone reads and writes the same tables, and the reserve and the puller's funding
verifier hold independent handles onto the same batch set.

The `BatchStore` impl is a thin map over the `Database`:

```rust
impl<DB: Database> BatchStore for DbBatchStore<DB> {
    type Error = DbBatchStoreError;
    fn get(&self, id: &BatchId) -> Result<Option<Batch>, Self::Error> { ... }
    fn put(&self, batch: Batch) -> Result<(), Self::Error> { ... }
    fn remove(&self, id: &BatchId) -> Result<bool, Self::Error> { ... }
    fn contains(&self, id: &BatchId) -> Result<bool, Self::Error> { ... }
    fn context(&self) -> Result<PostageContext, Self::Error> { ... }
    fn set_context(&self, state: PostageContext) -> Result<(), Self::Error> { ... }
    fn batch_ids(&self) -> Result<Vec<BatchId>, Self::Error> { ... }
    fn count(&self) -> usize { ... }
}
```

Two facts the redesign must keep.
`contains` is a key-presence probe that never decodes the batch value.
`batch_ids` walks the key order of the 32-byte big-endian id, which is ascending, and retains only
the keys.
`context` and `set_context` read and write a single `PostageContext` row (`ContextKey::SINGLETON`),
and a fresh store reports `PostageContext::default()`.

### The ingest half, `BatchEventHandler`

The ingest is one transaction per event.
Unknown batches are an idempotent no-op, and ordering is the indexer's responsibility.
The shared mutation primitive is an atomic read-modify-write in one transaction, so a concurrent
writer cannot clobber the update.

```rust
fn mutate_sync(
    &self,
    id: &BatchId,
    mutate: impl FnOnce(&mut Batch),
) -> Result<(), DbBatchStoreError> {
    self.db.update(|tx| {
        if let Some(mut batch) = tx.get::<Batches>(BatchIdKey(*id))? {
            mutate(&mut batch);
            tx.put::<Batches>(BatchIdKey(*id), batch)?;
        }
        Ok(())
    })
}
```

```rust
impl<DB: Database> BatchEventHandler for DbBatchStore<DB> {
    type Error = DbBatchStoreError;

    fn handle_event(&mut self, event: BatchEvent) -> Result<(), Self::Error> {
        match event {
            BatchEvent::Created { batch } => self.put(batch),
            BatchEvent::TopUp { batch_id, new_value } =>
                self.mutate_sync(&batch_id, |batch| batch.set_value(new_value)),
            BatchEvent::DepthIncrease { batch_id, new_depth } =>
                self.mutate_sync(&batch_id, |batch| batch.set_depth(new_depth)),
            BatchEvent::Expired { batch_id } => {
                self.acknowledge_expired(&batch_id)?;
                Ok(())
            }
        }
    }
}
```

### The `Expired` contract

`Expired` in the handler is the bare removal: it drops the batch row and evicts nothing.
Removing the batch first orphans the reserve entries stamped under it, inflating the reserve size
and the consensus-committed storage radius.
Live ingest therefore routes `Expired` through the reserve's evict-then-acknowledge entry point
(passing `acknowledge_expired`) rather than calling the handler directly.
`acknowledge_expired` is the acknowledgement half: it drops the batch row idempotently and reports
whether it existed, and is driven only by that path.

## The `DepthIncrease` drift

This is the fact that makes the capture load-bearing.
At the pinned nectar rev `ef89a5df` the event is two fields:

```rust
DepthIncrease {
    batch_id: BatchId,
    new_depth: u8,
},
```

At `nectar` `main` `0ece5710` it is four fields:

```rust
DepthIncrease {
    batch_id: BatchId,
    new_depth: u8,
    /// The rescaled balance, the event's `normalisedBalance`.
    new_value: u128,
    /// The mined-in block, from the log envelope; the issuance gate
    /// counts confirmations from it.
    block: u64,
},
```

The `vertex` handler compiles against the two-field shape and applies `set_depth` only.
It does not apply the rescaled balance and has no access to the mined-in block.
The node therefore stores a depth increase without revaluing the batch or advancing the
confirmation reference, and the next usability and expiry decisions run against a stale balance.
`TopUp` already applies its `new_value`, so the asymmetry is local to `DepthIncrease`.

The M1 decision (D1, below) resolves where this lands.
The M2 batch-store seam must apply all three on a depth increase, and `vertex` must bump its nectar
pin to the rev that carries the four-field variant in the same change that extends the handler.

## Seam two: the snapshot transport

`vertex` `bin/swarm-demo/src/client/usage.rs` implements the two usage traits over the browser
routing provider and sender.
The transport error carries a `SwarmError` string and is never an absence.

```rust
pub struct BrowserUsageSource {
    provider: Arc<dyn SwarmChunkProvider>,
}

impl SnapshotSource for BrowserUsageSource {
    type Error = UsageAdapterError;
    async fn fetch(&self, address: &SwarmAddress) -> Result<Option<Bytes>, Self::Error> {
        match self.provider.retrieve_chunk(address).await {
            Ok(result) => Ok(Some(result.chunk.data().clone())),
            Err(SwarmError::RetrievalExhausted { .. }) => Ok(None),
            Err(e) => Err(UsageAdapterError::from(e)),
        }
    }
}

pub struct BrowserUsageSink {
    sender: Arc<dyn SwarmChunkSender>,
}

impl SnapshotSink for BrowserUsageSink {
    type Error = UsageAdapterError;
    async fn push(&self, sealed: &SealedChunk) -> Result<(), Self::Error> {
        let chunk: AnyChunk = AnyChunk::from(sealed.chunk.clone());
        let stamped = StampedChunk::new(chunk, sealed.stamp.clone());
        self.sender.send_chunk(stamped).await?;
        Ok(())
    }
}
```

Two facts the redesign must keep.
A retrieved chunk is already address-validated before it is handed back, so `fetch` returns the data
payload directly and maps exhausted retrieval to `Ok(None)` rather than an error.
The sink wraps the sealed chunk and its stamp in a `StampedChunk` before sending.

## The validator: current surface and the locked decisions

The current surface has three pieces.
`nectar` `StampedAddress::validate(&Batch)` is the live leaf predicate: batch-id match, index
bounds, bucket bounds, and the owner signature check, returning a `Validated` marker.
`nectar` `StoreValidator` is the live store-coupled composite: it fetches the batch through
`get_usable` (which applies the store-level `PostageContext`) and then runs the leaf predicate.
`vertex` `AdmissionValidator` is the admission composite the node actually calls:

```rust
pub enum AdmissionError {
    UnknownBatch(BatchId),
    BatchNotUsable,
    BatchExpired,
    OwnerMismatch,
    Stamp(#[from] StampError),
}

pub struct AdmissionValidator {
    confirmation_threshold: u64,
}

impl AdmissionValidator {
    pub const fn new(confirmation_threshold: u64) -> Self { ... }
    pub fn validate(
        &self,
        stamp: &Stamp,
        address: &ChunkAddress,
        batch: &Batch,
        context: &PostageContext,
    ) -> Result<(), AdmissionError> {
        if !batch.is_usable(context.block(), self.confirmation_threshold) {
            return Err(AdmissionError::BatchNotUsable);
        }
        if batch.is_expired(context.total_amount()) {
            return Err(AdmissionError::BatchExpired);
        }
        batch.validate_index(&stamp.stamp_index())?;
        batch.validate_bucket(&stamp.stamp_index(), address)?;
        match stamp.verify(address, batch.owner()) {
            Ok(()) => Ok(()),
            Err(StampError::OwnerMismatch { .. }) => Err(AdmissionError::OwnerMismatch),
            Err(e) => Err(AdmissionError::Stamp(e)),
        }
    }
}
```

It carries no store dependency: the caller loads the batch and passes it in.
That is the reserve `put`, which runs admission inside its own write transaction, and the puller's
funding verifier.

### The locked decisions

Four decisions were settled before M1 and bound the M2 redesign.

D1. The store enforces the dilution revalue.
A `DepthIncrease` writes the rescaled balance, the new depth, and the mined-in block into the
store.
The validator treats the stored `(value, depth)` and the store-level `PostageContext` as facts and
runs its thresholds against them; it re-derives nothing.
This is the fix for the drift above, and it is a cross-tree change: `vertex` bumps its nectar pin
and extends the handler in one step.

D2. The leaf predicate owns the identity and the signature.
The leaf is the batch-id match, the index bounds, the bucket bounds, and the owner signature check,
over the post-dilution `Batch`.
It is the `StampedAddress::validate` predicate, kept intact.

D3. The usability and expiry gates read the store-level `PostageContext`.
`is_usable` consumes `context.block()` against the confirmation threshold.
`is_expired` consumes `context.total_amount()` against the batch value.
No new per-batch context is introduced, and the leaf stays synchronous and store-free.

D4. One unified composite is the canonical admission.
`nectar` holds the composite (`AdmissionValidator` plus `AdmissionError`).
`vertex`'s standalone `admission.rs` implementation is deleted the moment the composite lands, and
the two names join the existing `pub use nectar_postage::{ ... }` facade in `vertex` `lib.rs`.
The admission decision stays in `vertex`: the reserve `put` and the puller's verifier call the
shared composite through the facade.
Because every `vertex` call site already imports through that facade and none imports the nested
`admission::` path, all call sites compile unchanged.
The puller's verifier matches on all five `AdmissionError` variants, so the variant set and names
move byte-identical.

### The target shape for `nectar-postage-api`

The M2 `nectar-postage-api` crate takes the rewritten stamp validator and the batch-store seam.
The validator surface it ships is the leaf predicate (D2), the composite (D4), and the
`AdmissionError` type, with the usability and expiry gates reading the store-level context (D3).
`Batch` is the unit that carries the post-dilution `value` and `depth` and the `is_usable` and
`is_expired` methods the composite calls.
The batch-store seam it ships is the `BatchStore` interface the node `DbBatchStore` implements,
with the ingest handler applying the full depth-increase write (D1).

## The pure deletions

These three need no redesign input and delete cleanly in M1.

`BatchFactory` has zero references outside `nectar` and is not an interface the node or the browser
bind to.
`StampValidator` has zero implementors and is superseded by the leaf predicate and the composite.
`SnapshotStore` has no real implementor and is a name collision with `vertex`'s unrelated
`DbPeerSnapshotStore`.

The remaining M1 removal set is tracked on its own issues and needs no redesign input: the dead
aliases, `ChunkType`, `SwarmAddress` and `StampedPut` (#823); the `bmt-wasm-demo` member and the
`wasm-bindgen` glue (#825); the parking of `nectar-envelope`; and the `no_std` duplication inside
the proving lane.

## What the interval costs

Between the M1 deletion and the M2 rewrite the batch-store-and-ingest seam and the snapshot
transport are not a stable `nectar` surface; the node and the browser client both re-bind once the
M2 seams land.
The capture above is the contract the M2 seams must satisfy so that the interval is short and the
node re-binds without a second redesign.
The `DepthIncrease` four-field variant and the `vertex` pin bump land together in M2, not in M1.

AI Assistance: Claude Code used for the caller capture, the drift analysis, and drafting this body.
