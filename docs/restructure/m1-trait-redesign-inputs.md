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
`ef89a5df` is behind `main`: `nectar` changed the `DepthIncrease` event and the issuance seam
surface between the two revs, and that divergence is the load-bearing fact in this document.

## The premise correction

The issue premise is that the six traits have no production implementation inside `nectar`.
That holds inside `nectar` and fails across the tree.
Three of the six have real out-of-tree implementors; the other three are pure deletions.

| Trait | In `#824`'s six | Real implementor | Disposition |
|---|---|---|---|
| `BatchStore` | yes | `vertex` `DbBatchStore` | delete in M1, redesign as the store seam in M2 |
| `SnapshotSource` | yes | `vertex` browser `BrowserUsageSource` | delete in M1; the batch-hosted issuer read transport, role re-homed in the M2 stamper seam |
| `SnapshotSink` | yes | `vertex` browser `BrowserUsageSink` | delete in M1; the batch-hosted issuer write transport, role re-homed in the M2 stamper seam |
| `SnapshotStore` | yes | none | pure delete in M1 |
| `BatchFactory` | yes | only the in-memory `MemoryBatchFactory` | pure delete in M1; the dead batch-creation seam, distinct from the live issuance seam |
| `StampValidator` | yes | none | pure delete in M1 |
| `BatchEventHandler` | no | `nectar` `IssuerRegistry`, `vertex` `DbBatchStore` | not a `#824` deletion; the ingest half of the store seam, captured for the M2 redesign |
| `StampIssuer` | no | `MemoryIssuer`, `RingIssuer`, `SnapshotIssuer` | not a `#824` deletion; the live issuance seam, re-homed into `nectar-postage-api` in M2 |

`BatchEventHandler` is captured because the same `vertex` type implements it alongside `BatchStore`
and together they are the batch-store-and-ingest seam; it is a redesign input, not a `#824`
deletion.
`nectar` also ships one production implementor of the seam.
The dilution `IssuerRegistry` in `postage-issuer/src/dilute_handler.rs` reacts to a
`DepthIncrease` with the confirmation-gated dilution of its tracked issuers and advances its block
head, and treats the other events as no-ops.
`StampIssuer` (and its `Stamper` companion) is the live issuance seam the snapshot machinery
implements; like `BatchEventHandler` it is a redesign input, re-homed into `nectar-postage-api` in
M2, not a `#824` deletion.
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
    fn count(&self) -> Result<usize, Self::Error> { ... }
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

The same interval moved the issuance seam.
At the pinned rev the seam was `prepare_stamp(&mut self, &SwarmAddress) -> Result<StampDigest, StampError>`;
at `main` it is `reserve(&self, &ChunkAddress) -> Result<Prepared<S>, StampError>`.
`SwarmAddress` was renamed `ChunkAddress`, `BatchId` was promoted from a `B256` alias, and
`BucketDepth` became a newtype.
The M2 rebind carries all of it, not only the event variant.

The M1 decision (D1, below) resolves where this lands.
The M2 batch-store seam must apply all three on a depth increase, and `vertex` must bump its nectar
pin to the rev that carries the four-field variant in the same change that extends the handler.

## Seam two: the stamp-issuance and stamper seam

The machinery a first draft of this capture called the snapshot transport is the state retention of
one concrete stamp issuer.
The redesign input that matters is the issuer seam itself: which stamp is issued next for a chunk.
The seam and its batch-hosted retention live in `nectar-postage-issuer` and
`nectar-postage-usage`.

### The seam: `StampIssuer` and `Stamper`

`nectar-postage-issuer/src/issuer.rs` defines the issuance seam.
`StampIssuer::reserve(address, timestamp) -> Prepared<S>` claims the next slot in the bucket the
`address` falls into and returns the prepared permit for it.
The batch geometry (`batch_id`, `batch_depth`, `bucket_depth`) and the per-bucket utilisation reads
(`bucket_utilization`, `bucket_has_capacity`, `max_bucket_utilization`) ride alongside it.
Slot allocation is the reserve half of a three-phase issue; signing and commit are outside it.
The companion `Stamper` trait (`src/stamper.rs`) carries the reserve-plus-sign-plus-commit role, and
`BatchStamper` wraps an issuer, a signer, and a clock to run it.
How an implementor knows what is next is not part of the seam: it exposes only `reserve` and the
geometry and capacity reads, so the state retention stays implementer-defined.

### The concrete issuers

Three retention strategies implement the seam today.
`MemoryIssuer` (`src/issuer.rs`) is fill-only and in-memory, the immutable-batch path;
`MemoryIssuer::from_batch` refuses a mutable batch so a ring is never produced by accident.
`RingIssuer` is the mutable, overwrite-aware path, with `external` for external tracking and
`reserved` for self-hosting, where the protected slots come from `nectar-postage-usage`.
`SnapshotIssuer` (`postage-usage/src/issuer.rs`) is the self-hosted, batch-hosted issuer: it
implements `StampIssuer` over a `Snapshot`'s table so content stamping and the snapshot's own
allocation share one table and never collide.

### The batch-hosted retention (SBU1)

The `Snapshot` is the state an issuer carries: a per-bucket counter table (`UsageTable`, immutable
with monotone watermarks or a mutable ring), a published sequence, and the slots it has allocated.
The snapshot persists inside the batch it describes, as single-owner chunks.
Snapshot chunk `n` carries the single-owner id `keccak256("swarm-batch-usage" || batch_id ||
u16_be(n))` and the address `keccak256(id || owner)`, owned and stamped by the batch owner, so a user
recovers their issuer state on any machine from just their key and the batch id.
Chunk 0 is the root; `RootInfo` commits to the batch geometry, the published sequence, the slots the
snapshot chunks occupy, and the digests of the leaf counter-table chunks.
Chunk ids never change for the life of the batch, so a persist overwrites in place and needs a
strictly newer seal timestamp; `seal_plan` refuses `SealError::NonIncreasingTimestamp` otherwise.

A persist is planned and sealed in two steps.
`Snapshot::plan_persist(owner)` yields a `PersistPlan`, one `PlannedChunk` per chunk, each carrying
the `stamp_index` it is stamped with.
`seal_plan(owner_signer, plan)` signs each single-owner chunk and stamps it, returning
`SealedChunk`s; the signer must be the batch owner because it signs both the single-owner chunks
and the stamps.

### The browser client transport

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

### What is deleted and what survives

`#824` deletes two of the surface pieces here: the `SnapshotSource` and `SnapshotSink` transport
traits and the `BatchFactory` creation seam.
`BatchFactory` (`postage-issuer/src/factory.rs`) is the batch-creation seam (create, top up,
dilute a batch); its only implementor is the in-memory `MemoryBatchFactory`, so it is dead.
It is distinct from the issuance seam: `StampIssuer` and `Stamper` have real implementors and
survive M1, re-homed into the `nectar-postage-api` issuer and stamper seams in M2.
Their implementations (`MemoryIssuer`, `RingIssuer`, `SnapshotIssuer`), the SBU1 codec, and the
`Snapshot`/`UsageTable` state carry into `nectar-postage-issuer`, which absorbs
`nectar-postage-usage`.

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
The issuer and stamper seams it ships are `StampIssuer`, whose `reserve` (the slot allocation, the
next stamp for a chunk) returns the `Prepared` permit and whose geometry and capacity reads ride
alongside it, and `Stamper`, the reserve-plus-sign-plus-commit role.
The concrete issuers and the SBU1 retention stay downstream: `MemoryIssuer`, `RingIssuer` and
`SnapshotIssuer`, the `Snapshot`/`UsageTable` state, and the SBU1 codec sit in
`nectar-postage-issuer`, which absorbs `nectar-postage-usage`.
The `SnapshotSource` and `SnapshotSink` transport traits retire in M1 and their read and write role
re-homes into the M2 stamper seam.

## Redesign guidance (reth and alloy idioms)

The captured seams were audited against the reth and alloy trait architecture, the reference
standard for this workspace.
Most of the surface already conforms, and the divergences below are the design direction for the
M2 rewrite.

### Already conforming

- The `SigningError::is_systemic` and `UsageError::is_corruption` and `is_recoverable` predicates
  give the public errors their retryability classification.
- `ClientError` keeps its `#[source]` and `#[from]` chains and never interpolates an inner error
  into message text.
- The sealed `ValidationState` marker follows the private `Sealed` supertrait pattern, and
  `Prepared` keeps its `Verified` and `Validated` state machine.
- `#[auto_impl(&, Arc, Box)]` rides the source and sink seams.
- `MemoryIssuer` keeps its state lock-free over atomics behind a `&self` API.
- `handle_events` defaults to a sequential fold.

### One core trait per concern

- `StampIssuer` mixes the one mutating verb, `reserve`, with seven geometry and capacity reads.
- M2 keeps `reserve` and the three geometry reads on the core seam.
  The capacity and utilization reads move to an extension trait keyed on the core.
- `BatchStore` mixes the core map verbs with the singleton context management (`context`,
  `set_context`) and the scan reads (`batch_ids`, `count`).
  The context and scan verbs are extension-trait candidates.
  `BatchStoreExt::get_usable` is the same split done right and stays as it is.
- The core map moves to `contains_key` so it matches the workspace map vocabulary.
- `Stamper` re-declares the geometry reads that `StampIssuer` already carries.
  M2 composes the stamper from the issuer seam instead of duplicating the reads.

### The dyn boundary

- The defaultless `type Error` on `BatchStore`, `BatchEventHandler`, and `Stamper` blocks a bare
  `dyn` (rustc `E0191`).
- The `BatchStore` doc comment claims object safety.
  That claim is false and comes out in M2.
- M2 gives the store-side seam error a `BoxedError`-based default or a concrete seam error so a
  bare `dyn BatchStore` works, or documents the pinned `dyn X<Error = E>` form at the boundary.
- `BatchEvent` carries no `#[non_exhaustive]`.
  The four-field `DepthIncrease` break is exactly what forced the `vertex` matcher to recompile, so
  the tag lands in M2 with the seam move.
- `StampIssuer` keeps its spec associated type, and the pinned `dyn StampIssuer<Spec = Mainnet>`
  form gets an `_ObjectSafe` compile test so a later generic-method addition breaks CI visibly.
- The `SnapshotSource` and `SnapshotSink` `impl Future` returns stay.
  A seam that returns `impl Future` stays generic and is never dyn'd.
  The deliberate no-`Send` future is already tested.

### Error boundaries

- `SnapshotIssuer::map_usage_error` collapses the twenty-one `UsageError` variants into two
  `StampError` variants.
  It maps `RingExhausted` to `InvalidIndex` and drops the `is_corruption` and `is_recoverable`
  classification.
  M2 replaces it with a structured `From` that keeps the variants.
- `StoreValidator` collapses `BatchStoreError::Store` into `StampError::BatchNotFound`.
  It drops the source and the structured fields.
  The wrap carries `#[source]` or its own variant instead.
- The `unreachable!` in the `RingIssuer` slot mapping is a panic on a hot production path.
  It leaves the house no-panic set and is replaced with a real error mapping in the M1 code wave,
  independent of the deletion.
- The store-side error bounds are inconsistent: `BatchStore::Error` is bounded by
  `std::error::Error` while `BatchEventHandler::Error` is unbounded.
  M2 places the bound once, on the store-side seams.

### Generics

- `Prepared::seal` carries a method-level const generic for the body size, which forces a turbofish
  at every call site.
  reth and alloy carry no const generics on public seams.
  The M3 `SwarmPrimitives` bundle absorbs the body size, and the const leaves the signature with
  it.
- The client `BatchStamper` is generic over five parameters and spells its `ClientError` generic
  pair in every method signature.
  M2 converges the facade on one facade error, or on a builder that captures the generics once.
- The client seam bounds the signer as `SignerSync + Signer` where the postage-issuer seam bounds
  `SignerSync`.
  The bound unifies on whichever the facade actually needs.
- Both `BatchStamper` names collide across the two crates.
  M2 renames the client facade to a distinct name.

### Concurrency

- `RingIssuer` and `SnapshotIssuer` are documented `!Sync` over `RefCell` cells with a `&self`
  mutating API.
  `MemoryIssuer` proves the atomic shape already works in-tree, so M2 atomizes the ring state or
  stops handing out the `RefCell` guards that panic inside `reserve`.
- `Stamper::stamp` takes `&mut self`, but the issuance core is `&self` over atomics and
  `MemoryIssuer` is `Sync`.
  The signature relaxes to `&self` where the signer is `Sync`.

### Typestate markers and naming

- Both `StampedAddress` transitions exit into the same `Validated` state.
  The full `validate` check and the weaker `issued_by` check (no signature recovery, no index
  bound) produce indistinguishable values.
  M2 splits the state or drops the weak transition.
- The `ValidationState` marker trait carries `Send`, `Sync`, and `'static` bounds that its two
  `ZST` markers do not need.
- The crate docs call `Prepared` the permit while the type is named `Prepared`.
  The name and the doc line up on one.

## The pure deletions

These three need no redesign input and delete cleanly in M1.

`BatchFactory` is the batch-creation seam (create, top up, dilute); its only implementor is the
in-memory test double `MemoryBatchFactory`, and it is distinct from the live issuance seam
`StampIssuer`, captured under seam two.
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
The pin bump carries the issuance-seam rename and the `BatchId` and `BucketDepth` shape changes with
it.

AI Assistance: Claude Code used for the caller capture, the drift analysis, and drafting this body.
