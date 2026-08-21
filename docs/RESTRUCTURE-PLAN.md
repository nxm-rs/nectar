# Restructure plan

Written 2026-08-21, against `main` at `5fc8c434`.
This document supersedes `docs/PRODUCTION-PLAN.md`, which was written on 2026-08-12 and does not contain the postage redesign that landed in #772 to #794.
`docs/PHASE-0-GOVERNOR.md` was already self-marked historical and is deleted alongside it.

The target is one release, 0.5.0, cut from a restructured tree.

The tracker mirrors this document: #795 is the root epic, each milestone below is a GitHub milestone, and the epics inside it carry the issues.
Where the two disagree, the tracker is authoritative, because it is derived from the tree and this document is not.

## Decisions that frame the plan

Settled by interview on 2026-08-21.
None is reopened by any milestone below.

1. **nectar is the shared node substrate, not a primitives library.**
   It absorbs the consensus-observable pure functions that live in the node by accident, and it grows a synchronous store seam that a transactional reserve can implement.
2. **The restructure comes before the release.**
   Consumers migrate once, not twice.
3. **The crate set grows to about twenty published crates.**
   The twelve to fourteen target is retired: small single-purpose crates are what makes the layering enforceable by cargo rather than by review.
4. **`no_std` narrows to a named six-crate proving lane.**
   They are `nectar-primitives`, `nectar-spec`, `nectar-postage-primitives`, `nectar-proof` and `nectar-ldb-core`, plus `nectar-marker`, which they link.
   That is what a zkVM guest links: chunk verification, BMT hashing and segment proofs, single-owner-chunk recovery, stamp signature verification and binding, and ldb node decode and descent.
   **Everything else is std-only.**
   This overrides the "no_std stays mandatory" constraint recorded during the issuer redesign: a signed stamp is its own proof, so a guest never issues one.
   `nectar-tasks`, `nectar-governor` and `nectar-clock` keep their unconditional `no_std` because it is already true and costs nothing to hold, but they carry no proving-lane guarantee.
   If a crate is not on the list, it does not get a `std` feature.
5. **Two proof kinds are in scope, and mantaray is excluded permanently.**
   A segment of a chunk, which already exists, and membership or exclusion of a key in an ldb store.
   A proof over a section of a file is out of scope: it has no stated consumer, and its interesting case is a byte range, which is a frontier rather than a path.
   A mantaray root differs for the same content depending on insertion order, so it cannot anchor a claim about content.
6. **The store seam is two named seams.**
   A synchronous, stamp-keyed `ChunkStore` for transactional local storage, and the asynchronous `ChunkGet` and `ChunkPut` for network and client use, with an adapter between them.
7. **One `SwarmPrimitives` bundle replaces `SwarmSpec` and `const BODY_SIZE`, and it lives in `nectar-spec`.**
8. **`nectar-envelope` parks at `publish = false`** and leaves the release.
9. **Structural work happens on a long-lived restructure branch, and the carve is serial.**
   The carve's true dependency depth is four, but every car edits the workspace member and dependency tables and four of the nine are themselves sweeps, so it runs as stacked cars against a green tree.
10. **The node migrates in lockstep, at milestone exits rather than continuously.**
    Work done between freeze points is done twice.
11. **The tier ladder is the freeze mechanism, and the milestones are the cutovers.**
    Every successful freeze in comparable Rust projects was enforced by a crate boundary rather than a policy document.
    We already have the boundary, so the structure is the policy.
12. **The carve and the seam shaping merge into one milestone.**
    The api crates are the crates consumers bind to, and they are being written fresh, so writing them in final shape costs nothing extra and moves the consumer cutover forward a whole milestone.
13. **Disjoint sweeps may overlap.**
    The generic-parameter collapse, the error-variant sweep and the lint burn-down edit different things, so the one-sweep-at-a-time rule is relaxed for them specifically.

## Ordering rules

1. Delete before you refactor.
   A deletion must first capture its real downstream caller, or the replacement is designed against nothing.
2. Gate before the work the gate guards, and require an aggregator rather than the leaf jobs.
3. Design decision before dependent code; rename before the specification that describes it.
4. A sweep gets a window against anything that touches the same surface.
   Two sweeps over provably disjoint surfaces may overlap.
5. Anything that enumerates the tree goes last.
6. Scope from the tree, never from the issue body.

## The freeze ladder

Each milestone is a cutover with a stated contract.
A consumer reads the ladder to know when it may start, and what it may rely on when it does.

| Milestone | Contract | Enforced by |
|---|---|---|
| **M0** ground clearing | Baseline, not a freeze. Nothing is renamed or removed. A consumer may repin with no source change. | existing CI |
| **M1** removal set final | The deletion list is closed. Nothing further leaves the public surface. | none needed |
| **M2** vocabulary and seams | Crate names, module homes and api trait signatures are final. **This is the cutover consumers key off.** | crate-set snapshot, plus an additive-only public-API diff on the api crates |
| **M3** generics final | `SwarmPrimitives` has absorbed `SwarmSpec` and `BODY_SIZE`. No `const B` in a public signature. | the same diff, now blocking |
| **M4** absorption complete | Stamp-index arbitration and redistribution live in nectar. | the donation window |
| **M5** specs and vectors | Every shipped wire format is asserted against bytes the reference client produced. | `upstream-check` |
| **M6** release 0.5.0 | Published and tagged. | semver check against the previous revision |

Four things freeze independently and must not be conflated.
The **name** vocabulary and the **api shape** freeze together at M2, because a consumer cannot bind to one without the other.
The **wire** formats are already frozen by reference-client interop and are only being documented, not decided.
The **crate set** cannot freeze until the carve finishes, which is why the release paperwork is last.

Two consequences for ordering, both from how the tooling works.
`#[non_exhaustive]` must be applied before M2, because adding it is itself a breaking change.
Sealing the extension-point traits during the api-crate creation converts a later method addition from breaking to additive, and the tooling recognises sealed traits, so the benefit is machine-visible rather than a convention.

## Target crate set

The suffix determines what a crate may depend on: a crate may only depend on its own tier and below.

### Runtime infrastructure, no Swarm vocabulary

`nectar-marker`, `nectar-tasks` (gains `Sleeper`), `nectar-governor`, `nectar-clock`.

These four are already the model.
They keep unconditional `no_std` and no `std` feature.

### Tier 0, data, `no_std`

| Crate | Contents |
|---|---|
| `nectar-spec` | The `SwarmSpec` trait, the `Mainnet` and `Testnet` markers, `NetworkId`, `NamedSwarm`, the protocol constants, and the `SwarmPrimitives` bundle. Depends on `alloy-primitives` alone. |
| `nectar-primitives` | Today's `primitives-core`, plus overlay derivation, proximity order, XOR metric, bins and neighbourhood depth moved down. The chunk traits stay here. The signer stack moves behind a `sign` feature. |
| `nectar-postage-primitives` | `Stamp`, `Batch`, `BatchId`, `StampIndex`, bucket geometry, `StampedAddress`, signature recovery and stamp-to-address binding. |

No behaviour traits live here.

`nectar-spec` holds no addresses and no bindings, and the dependency runs **contracts to spec**, not the other way round.
`nectar-contracts` is std, depends on `nectar-spec`, and adds a `Deployment` extension trait implemented for the same marker types.
A deployment stays keyed by the specification that owns it, while a guest links the protocol constants without pulling the ABI machinery.

**Correction to an earlier draft: there is no `nectar-primitives-traits` crate.**
It was to hold `ChunkHeader`, `ChunkOps`, `ChunkRegistry`, `Reference` and `TrustState`, but tier 0 declares `Chunk` and `EntryRef` over three of those as bounds, and a tier-0 crate cannot name a tier-1 item.
It was then to hold `SwarmPrimitives` alone, but tier-0 `nectar-postage-primitives` is generic over that across dozens of sites in its geometry and batch modules.
Both constraints are hard, so nothing is left for the crate.
The chunk traits stay in `nectar-primitives` and the bundle lives in `nectar-spec`.

### Tier 1, abstractions over tier 0, `no_std`

| Crate | Contents |
|---|---|
| `nectar-proof` | The authentication layer only: how one step's bytes bind to a trusted address, and the replay loop. No key vocabulary, no verdict, no span arithmetic. |

The segment proof stays in `nectar-primitives`, because it is the primitive the others are built from rather than a proof over a structure.
It needs one tightening while it is touched: the anchor prefix becomes a fixed 32-byte value rather than an owned vector, which makes the proof copyable and removes the last allocation from the verify path.
`Present` and `Absent` stay in `nectar-ldb-core`, so the generic layer never learns a vocabulary only one implementor uses.
Redistribution does not sit behind this trait; it is three flat single-level proofs plus a stamp, with no descent.
The verifier borrows rather than owns, which makes it allocation-free in a guest.
Fold the shared sibling levels across a contiguous segment run, which turns the hash count from quadratic in the run length to roughly linear.

### Tier 2, behaviour traits, errors and `noop` implementations

| Crate | Contents |
|---|---|
| `nectar-errors` | The shared error types and the `StoreError` classification, so the api crates and the implementation crates do not depend on each other. |
| `nectar-storage-api` | `ChunkStore` (synchronous, stamp-keyed, object-safe), `ChunkGet` and `ChunkPut` (asynchronous), `StoreKey`, `PutUnit`, `Source`, and the adapter that lifts a sync store onto the async seam. |
| `nectar-postage-api` | The rewritten stamp validator, the batch store seam, the issuer and stamper seams, and stamp-index arbitration. |
| `nectar-manifest-api` | `Manifest`, `ManifestView`, and cursors expressed as `Stream`. |

Every api crate ships a `noop` implementation, so a downstream crate never depends on an implementation in order to compile.
No api crate contains a real implementation.

**Open: the concrete stores have no home.**
`MemoryStore`, `VerifyingStore`, `Tee`, `RetryingChunkGet` and `NullLoader` are used by eleven crates in tests.
The `noop` covers compiling without an implementor; it does not cover storing chunks.
Either a tier-3 `nectar-storage`, or a `memory` feature on the api crate, or `nectar-testing`, which would lose them for downstream consumers.
This blocks the api-crate car and must be settled first.

### Tier 3, implementations, `std`

`nectar-contracts`, `nectar-file`, `nectar-ldb-core`, `nectar-ldb`, `nectar-mantaray`, `nectar-feeds`, `nectar-postage-issuer` (absorbs `nectar-postage-usage`).

`nectar-ldb-core` is the one crate here that is `no_std`: node decode, descent verification and the proof implementation.
`nectar-ldb` above it is the engine, and is std.

Every other crate in this tier is std-only, and each deletes a `std` feature that was never honoured.
`nectar-feeds` has no `no_std` attribute at all today.
`nectar-mantaray` gates every functional module and even `extern crate alloc` behind `std`, so its bare-metal build exposes two constant tables, and its order-dependent roots keep it out of the proving lane permanently.
`nectar-file` is unconditionally `no_std` today and loses it, because nothing in it is in the proving lane and its positional IO is a transfer concern.

### Tier 4, facade

`nectar`, with feature-gated re-exports.
Its acceptance test is that the eight items which account for sixty per cent of downstream usage are reachable from one import.

### Unpublished

`nectar-testing`, `nectar-integration-tests`, `nectar-envelope`.

## Milestones

### M0. Ground clearing, on `main`

Defect fixes, demotions and renames that are safe before the branch is cut.
Nothing here depends on the carve, and everything here is more expensive afterwards.

Two live defects: a release build silently drops a pending sign job because a `debug_assert` guards it, and the `no_std` arm of the issuer's word module discards the compare argument so the compare-and-swap contract is enforced on only one of two arms.
The redundancy level flag is not decoded out of the span, which is a prerequisite for any proof over a file rather than a display bug: a proof must hash the raw span bytes because that is what the address commits, but do the tree arithmetic on the decoded value.

Then the demotions that apply decision 4 to the feature tables, the ldb v1 rename, the vector-provenance rule, and pinning the unpinned consumer.

The gates also land here, because a gate precedes the work it guards.
Re-specify the reinvention ban list before restoring it: some of the patterns it names are now the sanctioned idiom.
Split the proving-core tests into a std arm and a core arm and make the lane execute rather than only type-check, because two implementations of the same hash step ship with no differential test between them.
Assert the segment proof against the reference corpus that is already in the tree and that nothing in Rust reads.

**Exit gate.** Both defects have regression tests. The bare-metal matrix covers the six proving-lane crates and no others. An executed, not merely checked, `no_std` test lane is green. No open issue cites a path that does not resolve.

### M1. Removal set final

Delete before you refactor, and capture the caller before you delete.

Six public traits go: a stamp validator with zero implementors and the wrong arity for its only real caller, and five service traits backed only by test doubles.
**The premise that they have no production implementation holds inside nectar and fails across the tree.**
The node implements two of them, and the browser client implements two more.
Capture those files as the redesign inputs first, or the replacement is designed against nothing and the node has no trait to implement in the interval.

Then the dead aliases, the vestigial chunk-type trait, the deprecated address alias, the superseded put decorator, the wasm demo member and its glue, and the parking of the envelope crate.
Then the duplication the blanket `no_std` policy caused inside the proving lane: a cache that heap-allocates per chunk for values that fit in a register pair, two deoptimised byte loops on the BMT hot path, and an exclusive-or built one byte at a time.

**Exit gate.** No public trait in the workspace has zero implementations. No cfg-split stands in for an item that is in `core`.

### M2. Vocabulary and seams

The largest milestone, and the one consumers key off.

The carve runs as stacked cars in dependency order: move the sleeper into the tasks crate; rename the proving core and move the routing predicates down; carve the spec crate and invert the contracts dependency; carve the postage data crate; create the proof crate; create the errors and api crates; split the ldb core out and implement the proof there; create the facade.

**The rename comes last, not first.**
Both crates want the name `nectar-primitives` at once, and the old one does not dissolve until its store module leaves and its sink module is deleted.
Only that one step is red at a commit boundary; every other move is green if the shim discipline holds.

The seams reach their final shape in the same milestone, because these crates are being written fresh.
The presence verb is deleted from the asynchronous seam and the get error is classified in the same change: the protocol has no wire presence verb, so a networked negative is always manufactured, and a three-valued answer does not fix it.
The synchronous store seam lands keyed on the address, the batch and the stamp hash, because one address holds many stamps.
The put unit gains a validation type.
Positional IO adopts the standard traits, and the name `Sink` is reserved for types that implement the standard trait.
The cursors become streams, which both formats already have the machinery for and hide.

`fuzz/` is a separate workspace with its own lock file, so `cargo check --workspace` never covers it.
Several of these moves break it silently, and its check must run per car.

**Exit gate.** No crate depends on a crate above its tier. The seam crate no longer dev-depends on its own implementors. No presence verb on the asynchronous seam. Every public store error answers whether an absence is definite. Every cursor is a stream, and every type named `Sink` implements the standard trait. The facade reaches the eight hot items in one import.

### M3. Generics final

Three sweeps, which may overlap each other because they edit disjoint surfaces, but not anything else.

The generic-parameter collapse absorbs the spec parameter and the body-size constant into the bundle.
It is 180 plus 262 sites and is the single largest change in the programme.
The error contract sweep applies the six rules and re-pins the conformance checklist to name items rather than line numbers, because two of its citations have already drifted.
The lint burn-down re-baselines first: its recorded count samples a crate set that no longer exists.

**Exit gate.** No `const B: usize` in any public signature. The error conformance checklist passes and is stable against line movement. The suppression count is measured against the tree as it stands.

### M4. Absorption complete

The node stops being a fork of nectar in three places, and gives up two things that were never its to hold.

Stamp-index arbitration is a consensus-observable rule with no upstream home.
The redistribution sample, ordering, witness selection and proofs are pure functions, and only the BMT prover comes from nectar today.
Three defects are fixed during the move rather than before it, so the break happens once: a stale contract binding, a missing single-owner proof field, and a proof path that has never been checked against the reference client because the fixture in use is independently generated.

The node also deletes its duplicate marker family, its duplicate clock, its duplicate stream crate and its three replacement store traits.
Its `SwarmPrimitives` collides with the one this plan introduces and must be renamed out of the way first, which needs no nectar change and can start immediately.

This is a donation, so it is the one window where both repositories land together.

**Exit gate.** The node builds and its tests pass against the branch. No concept has two canonical homes across the two repositories. The node implements the store seam rather than wrapping it.

### M5. Specifications and vectors

Names have settled, so the specifications can be written once.

Import or publish the external document the ldb citations resolve into, write the normative wire specification, and complete the reference-vector anchoring across every shipped format.

**Exit gate.** Every specification citation resolves inside the repository. Every shipped wire format is asserted against bytes that came from the reference client.

### M6. Release 0.5.0

Everything that enumerates the tree goes last.

The current release issues were written against a different crate set and cannot run as they stand.
Every list is re-derived from `cargo metadata` rather than from another issue's prose.
Several already-published crates depend on names that have never been published, so nothing can republish until those first-publish in dependency order.

Then the consumers flip their pins to the published version.

**Exit gate.** A clean publish from a clean `main`, one tag, one shared version.

## Open items

- The home for the concrete stores, which blocks the api-crate car.
- Whether a default survives on the chunk parameter after the generic collapse.
  If it does not, every chunk constructor call site in every consumer gains a turbofish, which roughly doubles the smallest consumer's diff.
- Whether mantaray's `std` gate is deleted outright or its trie is made unconditional first.

The release stays 0.5.0: more is due before 1.0, and a 0.x minor may carry breaking changes.
