# Restructure plan

Written 2026-08-21, against `main` at `5fc8c434`.
This document supersedes `docs/PRODUCTION-PLAN.md`, which was written on 2026-08-12 and does not contain the postage redesign that landed in #772 to #794.
`docs/PHASE-0-GOVERNOR.md` is already self-marked historical and is deleted by milestone 0.

The target of the plan is one release, 0.5.0, cut from a restructured tree.

## Decisions that frame the plan

These were settled by interview on 2026-08-21.
They are not reopened by any milestone below.

1. **nectar is the shared node substrate, not a primitives library.**
   It absorbs the consensus-observable pure functions that live in vertex by accident, and it grows a synchronous store seam that a transactional reserve can implement.
2. **The restructure comes before the release.**
   Consumers migrate once, not twice.
3. **The crate set grows to about 20 published crates.**
   The 12 to 14 target in #677 is retired: small single-purpose crates are what makes the layering enforceable by cargo rather than by review.
4. **`no_std` narrows to the proving lane, and the proving lane is a named list of six crates.**
   They are `nectar-primitives`, `nectar-primitives-traits`, `nectar-spec`, `nectar-postage-primitives`, `nectar-proof` and `nectar-ldb-core`, plus `nectar-marker`, which they link.
   That is what a zkVM guest links: chunk verification, BMT hashing and segment proofs, single-owner-chunk recovery, stamp signature verification and binding, and ldb node decode and descent.
   **Everything else is std-only**, including `nectar-file`, `nectar-ldb`, `nectar-mantaray`, `nectar-feeds`, `nectar-manifest-api`, `nectar-storage-api`, `nectar-contracts` and the whole postage issuer.
   This overrides the "no_std stays mandatory" constraint recorded for the issuer redesign: a signed stamp is its own proof, so a guest never issues one.
   `nectar-tasks`, `nectar-governor` and `nectar-clock` keep their existing unconditional `#![no_std]` because it is already true and costs nothing to hold, but they carry no proving-lane guarantee and the CI lane does not cover them.
   The rule replaces case-by-case judgement: if a crate is not on the list, it does not get a `std` feature.
5. **Two proof kinds are in scope, and mantaray is excluded permanently.**
   They are a segment of a chunk, which already exists, and membership or exclusion of a key in an ldb store, which has working prior art in `/code/nxm/nectar-simv3/crates/manifest-proof/`.
   A proof over a section of a file is **out of scope**: it has no stated consumer, and its interesting case is a byte range, which is a frontier rather than a path and so a second type regardless.
   **mantaray will never implement the proof seam.** A mantaray root differs for the same content depending on insertion order, so it cannot anchor a claim about content. History independence is an ldb guarantee only.
   The two kinds are not forced behind one trait; see the tier-1 table.
6. **The store seam is two named seams.**
   A synchronous, stamp-aware `ChunkStore` for transactional local storage, and the asynchronous `ChunkGet`/`ChunkPut` for network and client use, with an adapter between them.
7. **One `SwarmPrimitives` bundle replaces both `SwarmSpec` and `const BODY_SIZE`.**
8. **`nectar-envelope` is parked at `publish = false`** and leaves the release.
9. **The structural work happens on a long-lived restructure branch.**
   `main` stays open only for defect fixes.
10. **vertex migrates in lockstep**, against the branch, so the absorbed code and the new synchronous seam are validated by their real consumer before the tag.
11. **The tracker is reset.**
    New epics are written from this plan; stale epics and registers are closed.

## Ordering rules

Carried forward unchanged from the superseded plan, because they are what stops work being done twice.

1. Delete before you refactor.
2. Gate before the work it guards, and require an aggregator rather than the leaf jobs.
3. Design decision before dependent code; rename before the specification that describes it.
4. Workspace-wide sweeps get their own window.
5. Anything that enumerates the tree goes last.
6. Scope from the tree, never from the issue body.

## Target crate set

Twenty published crates, three unpublished.
The suffix determines what a crate may depend on: a crate may only depend on its own tier and below.

### Runtime infrastructure, no Swarm vocabulary

| Crate | Contents |
|---|---|
| `nectar-marker` | `MaybeSend`, `MaybeSync`. Unchanged. |
| `nectar-tasks` | `Spawn`, the single `BoxFuture` alias. Gains `Sleeper`. |
| `nectar-governor` | `Window`, `Admission`, `AdmitPolicy`, `PutSink`. Unchanged. |
| `nectar-clock` | `Clock`, `SystemClock`, `ManualClock`. |

These four are already the model. They keep unconditional `#![no_std]` and no `std` feature.

### Tier 0, data, `no_std`

| Crate | Contents |
|---|---|
| `nectar-spec` | The `SwarmSpec` trait, the `Mainnet` and `Testnet` marker types, `NetworkId`, `NamedSwarm`, and the protocol constants. Moved out of `nectar-primitives`. Depends on `alloy-primitives` alone. |
| `nectar-primitives` | Today's `primitives-core`, plus overlay derivation, proximity order, XOR metric, bins and neighbourhood depth moved down. Chain identity moves out to `nectar-spec`. The signer stack moves behind a `sign` feature. |
| `nectar-postage-primitives` | `Stamp`, `Batch`, `BatchId`, `StampIndex`, bucket geometry, `StampedAddress<V>`, signature recovery and stamp-to-address binding. |

No behaviour traits live here.

`nectar-spec` holds no addresses and no bindings, and the dependency runs **contracts to spec**, not the other way round.
`nectar-contracts` is std, depends on `nectar-spec`, and adds a `Deployment` extension trait implemented for the same marker types, carrying the addresses and deploy blocks.
A deployment is therefore still keyed by the specification that owns it, while a guest links `MIN_BUCKET_DEPTH` without pulling `alloy-sol-types`.
An earlier draft had this dependency backwards, and then had it as a fold, and both were wrong.

### Tier 1, abstractions over tier 0, `no_std`

| Crate | Contents |
|---|---|
| `nectar-primitives-traits` | `ChunkHeader`, `ChunkOps`, `ChunkRegistry`, `Reference`, `TrustState`, and the `SwarmPrimitives` bundle. |
| `nectar-proof` | The authentication layer only: how one step's bytes bind to a trusted address, and the replay loop. It owns no key vocabulary, no verdict and no span arithmetic. |

The segment proof stays in `nectar-primitives`, because it is the primitive the others are built from rather than a proof over a structure.
It needs one tightening while it is touched: `prefix: Option<Vec<u8>>` becomes `Option<B256>`, which makes `Proof` `Copy` and removes the last allocation from the verify path.
`Present` and `Absent` stay in `nectar-ldb-core` as the terminal type, so the generic layer never learns a vocabulary only one implementor uses.
Redistribution does not sit behind this trait; it is three flat single-level proofs plus a stamp, with no descent, and it gets its own crate when M6 absorbs it.
The verifier borrows rather than owns, which makes it allocation-free in a guest: node payloads are bounded at 4094 bytes, segment paths are exactly seven hashes, and ldb depth is bounded by the key.
Fold the shared sibling levels across a contiguous segment run, which turns `8n` hashes into roughly `n + 7`; the prior art verifies each segment independently and that is the highest-value change in the port.

### Tier 2, behaviour traits, errors and `noop` implementations

| Crate | Contents |
|---|---|
| `nectar-errors` | The shared error types, so the api crates and the implementation crates do not depend on each other. |
| `nectar-storage-api` | `ChunkStore` (synchronous, stamp-keyed, object-safe), `ChunkGet`/`ChunkPut` (asynchronous), `StoreError`, `StoreKey`, `PutUnit`, `Source`, and the adapter that lifts a sync store onto the async seam. |
| `nectar-postage-api` | The rewritten stamp validator, the batch store seam, the issuer and stamper seams, and stamp-index arbitration. |
| `nectar-manifest-api` | Today's `nectar-manifest`. `Manifest`, `ManifestView`, and cursors expressed as `Stream`. |

Every api crate ships a `noop` implementation so downstream crates never depend on an implementation in order to compile.
No api crate contains a real implementation.

### Tier 3, implementations, `std`

`nectar-contracts`, `nectar-file`, `nectar-ldb-core`, `nectar-ldb`, `nectar-mantaray`, `nectar-feeds`, `nectar-postage-issuer` (absorbs `nectar-postage-usage`).

`nectar-ldb-core` is the one crate here that is `no_std`: node decode, descent verification and the `nectar-proof` implementation.
`nectar-ldb` above it is the engine, and is std: builder, apply, cursors, read-ahead.

Every other crate in this tier is std-only, and each deletes a `std` feature that was never honoured.

- `nectar-feeds` has no `#![no_std]` attribute at all today, so its `std` feature is a claim it never made good on.
- `nectar-mantaray` gates every functional module *and* `extern crate alloc` behind `std`, so its bare-metal build exposes two constant tables. Its order-dependent roots keep it out of the proving lane permanently, so the gate has no future consumer either.
- `nectar-file` is unconditionally `#![no_std]` today and loses it. Nothing in it is in the proving lane, and its positional IO is inherently a transfer concern.
- `nectar-contracts` gains `nectar-spec` as a dependency and the `Deployment` extension trait.

### Tier 4, facade

`nectar`, with feature-gated re-exports.
Its acceptance test is that the eight items which account for sixty per cent of downstream usage are reachable from one import.

### Unpublished

`nectar-testing`, `nectar-integration-tests`, `nectar-envelope`.

## Milestones

### M0. Ground clearing, on `main`

Everything here is a defect fix or a demotion, and none of it conflicts with the branch.

- Fix the release-build defect at `crates/postage-issuer/src/pipeline/bridge.rs:17`.
  A `debug_assert!` guards a pended sign job, so in release the job is dropped silently and surfaces as a `None` that cannot be told from a legitimate one.
- Fix #669.
  A redundancy-enabled root reports about 9.2 exabytes because the level flag is not decoded out of the span.
  Its stated blocker, #708, has closed, so the deferral no longer holds.
  This is a prerequisite for any proof over a file, not a display bug: a section proof must hash the raw eight span bytes, because that is what the address commits, but do all tree arithmetic on the decoded span.
- Fix the compare-and-swap divergence in `crates/postage-issuer/src/watermarks.rs`.
  The `no_std` arm of `mod word` is backed by `Cell`, and its `claim` at line 101 silently discards the `from` argument, so the compare-and-swap contract is enforced on only one of the two arms.
  The atomics it stands in for are in `core`, not `std`, and both gated bare-metal targets support them, so the fix is to delete the `Cell` arm rather than repair it.
  The same duplication exists in `permit.rs`.
- Correct the `IRedistribution::claim` binding at `crates/contracts/src/lib.rs:207-214`.
  It still declares the stale six-argument form; the live contract takes three `ChunkInclusionProof` tuples.
- Pin apiarist's nectar dependency.
  It is currently an unpinned git dependency, so any `cargo update` moves it 357 commits.
- Add `default-features = false` to the six workspace dependency entries that omit it.
  This alone unblocks a bare-metal build of `nectar-postage-usage`.
- Apply the `no_std` line from decision 4 to the feature tables.
  Delete the `std` feature from `nectar-feeds` and `nectar-mantaray`, which never honoured it, and from `nectar-file`, which loses its unconditional `#![no_std]`.
  Narrow the `nostd.yml` matrix to the six proving-lane crates.
  This is a rename-and-delete pass, so it runs before anything is restructured on top of it.
- Rename the ldb wire format to "ldb v1" and remove the "mantaray 1.0" framing.
  The rename precedes the specification that describes it.
- Reset the tracker: strip the five wrong `blocked` labels, close the epics whose registers sample a tree that no longer exists, and re-point the issues citing `crates/ldb-sim`.
- Delete `docs/PHASE-0-GOVERNOR.md` and replace `docs/PRODUCTION-PLAN.md` with this document.

**Exit gate.** Both defects have regression tests. `cargo check --no-default-features --target riscv64imac-unknown-none-elf` passes for `nectar-feeds` and `nectar-postage-usage`. No open issue cites a path that does not resolve.

### M1. Gates, before the work they guard

The reinvention gate was deleted with no replacement, and the tree has already drifted back over its ban list.
The `no_std` lanes type-check but never execute.

- Re-specify and restore the reinvention gate.
  The ban list needs rewriting first, because some `FuturesUnordered::new()` sites are the sanctioned per-walker loops that the governor decision created.
  Current drift: `Option<Waker>` slots at `pipeline/stamp_sink.rs:82` and `pipeline/sign_stage.rs:90`, `std::thread::park()` at `pipeline/mod.rs:406`, `catch_unwind` at `pipeline/task.rs:19,32`.
- Extend `nostd.yml` to cover `nectar-feeds`, `nectar-postage-usage` and `nectar-tasks`, and add a consumer-edge pass.
  The current lanes check a crate with its own default off, which is why the workspace-table leakage was invisible.
- Split the `primitives-core` tests into a `std` arm and a core arm, then run the core arm under nextest.
  There are two implementations of `hash_pairs`, one SIMD and one sequential, with no differential test between them, on the path that must produce identical roots inside and outside a proof.
- Import the reference inclusion-proof segments from `tools/stamp-vectors/testdata/inclusion-proofs.json` as a conformance test for `Proof::verify`, and run it in the `no_std` lane.
  The file is byte-identical to the reference client's own golden output, it is already in the tree, and nothing in Rust reads it.
  Note that the fixture vertex tests its redistribution path against is an independently generated corpus whose values differ from the reference client's, so the three-proof path has never been checked against the reference implementation.
  The reference fixture also exercises the single-owner witness path, which vertex cannot currently represent at all, because its `ChunkInclusionProof` has no `socProof` field.
- Add the rustdoc job denying broken intra-doc links, appended to the existing aggregator.
- Add the release-build guard that fails if `arbitrary` or `proptest` reaches a default build.

**Exit gate.** The gate catches a deliberately reintroduced hand-rolled waker. An executed, not merely checked, `no_std` test lane is green. `Proof::verify` is asserted against reference bytes.

### M2. Delete and demote

Delete before you refactor.
Everything here is removed before anything moves.

- `nectar-envelope` to `publish = false`, out of the publish order and the release.
- Delete the dead public aliases `MaybeSendStream`, `MaybeSendIter` and `MaybeSendBoxFuture` at `crates/primitives/src/marker.rs:22-38`.
  They have zero references and duplicate the single sanctioned alias.
- Delete the vestigial `ChunkType` trait, which restates a `TYPE_ID` that `ChunkHeader` already owns.
- Delete the deprecated `SwarmAddress` alias, which is marked for removal in this release.
- Delete `StampedPut`, superseded by `StagedPut`.
- Delete the five service traits with no production implementations: `StampValidator` (which also has the wrong arity), `BatchStore`, `SnapshotStore`, `SnapshotSource`, `SnapshotSink`, `BatchFactory`.
  They are designed fresh in the api crates in M3, against their real callers.
- Drop `bmt-wasm-demo` from the workspace members and move the wasm-bindgen glue out of `nectar-primitives`.
- Remove the `clippy.toml` pointer to `tools/reinvention-gate.sh`, which does not exist.

Then delete what the blanket `no_std` policy duplicated.
Most of it dissolves for free once M0 narrows the lane, because the duplication lives in the crates that are leaving it: the `Cell`-versus-atomics arms in the issuer, the cfg-duplicated trait alias in `ldb/src/frontier.rs`, the `RefCell` shim standing in for `RwLock` in the memory store, and the recursion alias in `mantaray/src/node.rs` all simply go.
What remains is the part inside the proving lane, plus one piece of hygiene.

- Collapse the `OnceCache` cfg-split. `once_cell::race::OnceBox` works under `std` too, and the same crate already uses it unconditionally for the zero-hash table.
  More importantly it heap-allocates two or three times per chunk to cache `Copy` values of at most 32 bytes, which a guest pays on every chunk it verifies.
- Replace the byte-at-a-time copies written to satisfy `indexing_slicing` with `get_mut` plus `copy_from_slice`.
  Two are on the BMT hot path, at `bmt/hasher.rs:451` and `bmt/proof.rs:110`.
  They are not quadratic, since `Skip<slice::Iter>` specialises through `nth`, but they lose vectorisation, which is roughly an order of magnitude against `memcpy`.
- Rewrite `xor_metric.rs`, which builds a 32-byte exclusive-or one byte at a time under an `#[allow]` and then calls `U256::from_be_bytes` on the result, when the whole operation is one exclusive-or on the `U256` that is already the return type.
- Move the remaining `std::error::Error` bounds to `core::error::Error` as hygiene.
  It has been stable since 1.81 against an MSRV of 1.94 and is already the convention at 25-plus sites, and `postage-issuer/src/factory.rs` additionally reaches for `std::future::Future`, `std::sync::atomic::AtomicU64` and `std::convert::Infallible`, all of which are in `core`.

`nectar-marker`, `nectar-clock` and `wire::Cursor` were audited and are justified; leave them alone.
`bytes::Buf::get_u8` panics on underrun with no fallible variant, so a hand-written fallible cursor is mandatory under a `panic` denial.

**Exit gate.** No public trait in the workspace has zero implementations. No `cfg`-split stands in for an item that is in `core`. `cargo machete` and the unused-crate-dependency lints are clean.

### M3. The tier carve

The branch starts here.
Each step is a stacked car based on the previous car's head.

1. Move `Sleeper` into `nectar-tasks`.
   Its only implementation today is a test double, so `RetryingChunkGet` cannot be instantiated by any downstream consumer.
2. Rename `primitives-core` to `nectar-primitives`; move overlay, proximity order, XOR metric, bins and neighbourhood depth down into it; move `spec`, `network_id` and the `alloy-chains` dependency out into a new `nectar-spec` crate that sits above `nectar-contracts`; move the signer stack off the `std` feature and onto a `sign` feature.
3. Carve `nectar-postage-primitives` out of `nectar-postage`.
4. Create `nectar-primitives-traits` and define `SwarmPrimitives` there, with projection aliases beside it.
5. Create `nectar-proof`.
6. Create `nectar-errors`, `nectar-storage-api`, `nectar-postage-api`; rename `nectar-manifest` to `nectar-manifest-api`.
   Each ships a `noop`.
7. Split `nectar-ldb-core` out of `nectar-ldb` and implement `nectar-proof` in it.
   `Format::READ_AHEAD` moves to the engine side and is bounded by the governor, not by a wire-format version marker.
8. Create the `nectar` facade.

**Exit gate.** No crate depends on a crate above its tier. `nectar-manifest-api` no longer dev-depends on its own implementors: the cycle is broken by `nectar-errors` and the `noop` implementations. The facade reaches the eight hot items in one import.

### M4. Seam repair

The defects, now that the crates they belong in exist.

**Delete `ChunkHas` and classify `ChunkGet::Error` in one change.**
The Swarm wire protocol has no presence verb: the reference client's entire network fetch surface is `RetrieveChunk`, and the pullsync want-bitvector is computed from the node's own reserve.
So a networked presence answer is always manufactured, and a three-valued answer would not fix it, because `Ok(false)` from a network medium is epistemically identical to an error.
`ChunkGet::Error` is opaque today, so no generic consumer can separate a miss from a failure; a `StoreError` trait carrying `is_definitely_absent` and `is_transient` is the replacement for both.
These land together, or the feed-truncation defect simply moves from one verb to the other.
Presence survives only on the synchronous local seam, as a fallible `contains`.

- Rewrite the feeds reader onto a classified `get`.
  It is the only production consumer of `has`, and its own documentation concedes that a wide probe window can induce timeouts that read as absence, so widening the window makes the reader more likely to return a stale update.
- Stop `RetryingChunkGet` retrying a definite miss eight times.
  This falls out of the classification for free.
- Land the synchronous `ChunkStore` seam, keyed on `{address, batch, stamp_hash}` rather than on an address, because one address holds many stamps.
  It is object-safe, because the node holds its stores behind `Arc<dyn _>`.
  Grouping is a batch verb, not a transaction verb on the seam: the reference client tried generic per-store transactionality and deleted about 1300 lines of it.
- Add `PutUnit::Validation`, `()` locally and a stamp on the network, and drop the `= Chunk<Verified>` default from `ChunkPut` so an unstamped put is never what a caller gets by not thinking.
  This is additive to what #778 landed; do not replace `PutUnit` with an associated type on the trait, which would push the stamping decorator's job into every producer signature.
- Delete `DataSink`, `MemSink`, `FsSink` and the bespoke `ReadAt`, and adopt `positioned_io::{ReadAt, WriteAt, Size}`.
  Positional IO is a transfer concern at both ends, not just on download: chunks become available across threads as a file is split, and forcing them into cursor order reintroduces the head-of-line blocking that the completion-order frame stream exists to avoid.
  That is the real justification for the positional contract, and it is a better one than the recorded claim that `AsyncWrite` cannot express it.
  Since both halves are now std, neither needs to be bespoke, and the hand-written unix and windows implementations go with them.
  The one mismatch is `Size::size() -> io::Result<Option<u64>>` against a definite `len()`, which is one `ok_or` at the split-engine boundary.
- Bound `Source::Error` and `BatchEventHandler::Error`.
  The `SinkError` blanket trait, `DynSink` and `SinkBridge` exist only because those associated types are unbounded, and they delete themselves once the sink traits do.
- Reserve the name `Sink` for types that implement `futures::Sink`.
  Express `PutSink` and `StampSink` as `Sink` plus `Stream`, which is what their four-method protocols already are; `StampSink` already has the completion queue `PutSink` lacks.
- Express `ManifestCursor`, `RawCursor` and the ldb cursors as `Stream`.
  Both ldb and mantaray already have the poll machinery and hide it behind `async fn`, so the seam is strictly weaker than its implementations.
- Fix the `PutSink` boxing at its cause: `push` carries `F: Unpin`, needed only for a noop-waker opening poll, while `FuturesUnordered::push` has no such bound.
  `ChunkPut::put` returns `impl Future`, which is `!Unpin`, so every real caller boxes and the generic that exists to avoid boxing buys nothing.
- Rewrite the stamp validator against its real caller.
- Settle on one asynchronous convention across postage.
  There are currently four in one domain: synchronous, RPITIT with a hard `Send`, RPITIT with no bound at all, and RPITIT with `MaybeSend`.
- Window the serial round trips: `postage-usage::open()` performs up to 65,535 strictly serial fetches.
- Land erasure coding: parity-aware fan-out, the write side and the recovery getter.
  It waits on the span level decode in M0, and it belongs here rather than later because the write side changes the split engine's fan-out and the recovery getter changes how the walk engine uses the store, which are the same surfaces this milestone reworks.
  Landing it against the old seams would mean writing it twice.

**Exit gate.** No presence verb on the asynchronous seam. Every public store error answers `is_definitely_absent`. Every cursor is a `Stream`, and every type named `Sink` implements `futures::Sink`. The membound integration tests still hold, and a benchmark shows the allocation count per gibibyte split has dropped.

### M5. The workspace-wide sweeps

Each gets a window with nothing else in flight.
All three run after the tier carve and the seam repair, because each enumerates or edits a surface those milestones change.

1. The `SwarmPrimitives` collapse, absorbing `SwarmSpec` and `const BODY_SIZE`.
   This is 180 plus 262 sites.
2. Error-rule conformance: `#[non_exhaustive]` on all 27 public error types that lack it, no stringly-typed variants, and a retryability predicate on every public error with an exhaustive classification test.
3. The restriction-lint suppression burn-down in shipped source.
   Re-baseline the register first: the recorded count samples a crate set that no longer exists, and roughly two-thirds of the suppressions it counted were in the postage crates, which have since been rewritten wholesale.

**Exit gate.** No `const B: usize` in any public signature. The `AGENTS.md` error conformance checklist passes, and it names items rather than line numbers, because two of its current citations have already drifted. The suppression count is measured against the tree as it stands.

### M6. Substrate absorption and the vertex migration

- Move stamp-index arbitration up into `nectar-postage-api`.
  It is a consensus-observable protocol rule with no upstream home, and a second Rust node would have to copy it verbatim.
- Move the redistribution sample, transformed-address ordering, witness selection and inclusion proofs up into nectar.
  They are pure functions with no node state, and only the BMT prover comes from nectar today.
- vertex deletes its duplicate `MaybeSend` family, its duplicate `Clock`, its `swarm-stream` crate, and its three replacement store traits.
- Audit vertex's 825 `OverlayAddress` sites, which collide with nectar's newtype of the same name.
- Migrate dipper and apiarist.

**Exit gate.** vertex builds and its tests pass against the branch. No concept has two canonical homes across the two repositories. vertex implements nectar's store seam rather than wrapping it.

### M7. Specifications and vectors

Names have settled by this point, so the specifications can be written once.

- Import or publish TR-004 so the `spec N.N` citations in ldb resolve.
  They currently point at a document outside the repository.
- Write the normative ldb v1 wire specification.
- Complete the reference-vector anchoring: file-tree roots, overlay derivation vectors, the mantaray encoder golden, and the `vectors.toml` registry in `upstream-check`.

**Exit gate.** Every `spec N.N` citation resolves inside the repository. Every shipped wire format is asserted against bytes that came from the reference client rather than from nectar.

### M8. Release

Everything that enumerates the tree goes last.

- README crate table, publish flags, the first-publish order in dependency order, the semver-checks baseline, and the changelog.
  All three of the current issues covering this name `nectar-swarms`, which is no longer a member, and none of them mention `nectar-envelope`.
- Decide whether the release stays local-only or gains a workflow.
- First-publish the never-published names, then republish the rest.
- Tag 0.5.0.

**Exit gate.** A clean publish from a clean `main`, one tag, one shared version.

## Open items

The release stays **0.5.0**: more is due before 1.0, and a 0.x minor may carry breaking changes.

Nothing else is open. The five questions this plan opened are answered above: mantaray never implements the proof seam, the file-section proof is out of scope, positional IO is std and adopts `positioned-io` on both sides, mantaray is std-only, and the `no_std` line is the named six-crate list in decision 4.
