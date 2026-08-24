# Store decorator set decisions

Recorded 2026-08-24 against `nectar` `main` at `b4a22f6d`, with the downstream
survey against `vertex` `main` at `504affe`.

This document settles #841: which of the seven types in
`crates/primitives/src/store/` survives, and therefore what the tier carve
moves.
The issue's own terms bind the home: whatever survives moves to the tier-3
`nectar-storage` crate, keeping the rule that no api crate holds a real
implementation.
That home decision is recorded here so the api-crate car reads it.
The deletion that this decision allows lands with this document.
The carve, not this change, moves the survivors.

## The verdicts

| Type | Verdict | Home after the carve |
|---|---|---|
| `MemoryStore` | survives | `nectar-storage` |
| `ContentGet` (+ `ContentGetError`) | survives | `nectar-storage` |
| `VerifyingStore` (+ `VerifyError`) | survives | `nectar-storage` |
| `Tee` (+ `TeeError`) | survives | `nectar-storage` |
| `NullLoader` | survives | stays api-side, `nectar-manifest` |
| `SingleOwnerGet` (+ `SingleOwnerGetError`) | deleted here | none |
| `RetryingChunkGet` (+ `Sleeper`, `RetryConfig`) | survives | `nectar-storage` (`Sleeper` to `nectar-tasks`) |

## The reasons

`MemoryStore` is the workspace test fixture and the mantaray editor-oracle
store, which the committed fuzz corpus exercises.
No downstream crate names it, and it is the inner store of most of the
other decorators.

`ContentGet` has two production sites, both in mantaray: the
`MantarayManifest::over` data seam and the `NodeLoadSaver` load and
load-traced paths.
The wide-to-narrow bridge it provides is structural, not incidental.
The write side of the same handle types the put at the wide registry
(`ChunkPut<Chunk<Verified, AnyChunkSet<B>>>`), while the read side demands
the narrow one (`TrustedGet<ContentOnlyChunkSet<B>>`).
One store value therefore cannot satisfy both bounds, and a store typed wide
can also serve the put side, so the dominant flow keeps the store wide and
narrows the read.
The one way to avoid the decorator is to construct the store directly at
the narrow registry, which the feeds and some file test sites already do.
That flow has no put side, so it does not replace the adapter here.

`VerifyingStore` lifts an untrusted medium to `Trust = Verified` and runs
the full acceptance rule per get.
No current production site instantiates it, but it is the only in-tree
expression of the trust lift the retrieval path needs: a store that pulls
from the network hands back unverified bytes, and something must certify
them before a verified consumer reads them.
Its misrouted-store semantics are pinned by the file-walk and mantaray
cursor boundary tests.
Deleting it would force the lift to be re-derived the first time a
downstream retrieval path needs it.

`Tee` fans one put to a local leg and a forward leg, fail-fast in that
order.
The postage stamped-unit seam test pins its behaviour through the seam.
It is the write shape of a node that stores locally and forwards to the
network.

`NullLoader` always answers `NotFound`.
Its only production role is the `impl NodeLoader` on the manifest crate
side, which lets `Node`'s in-memory verbs run with zero backing.
The plan ships a `noop` implementation with every api crate so a downstream
crate never depends on an implementation to compile.
`NullLoader` is exactly that `noop`, and the plan's rule bans real
implementations in the api crate, not `noop`s.
It is the one survivor outside the `nectar-storage` move.

`SingleOwnerGet` is the narrowing adapter the issue asked about, and it
does not survive: there is no production consumer, in the workspace or in
vertex.
Its only in-tree users are two tests in the feeds round-trip file.
One is rewired onto a store typed directly at the narrow registry, the
adapter-free pattern the same file already uses in every other test.
The other exists to test the adapter itself and is deleted with it, because
direct narrow-store coverage already exists in the file.
The negative-path guard (a content chunk vouched for at a feed slot is
rejected with a typed store error) survives in the rewired test.
The registry narrowing mechanism it drove, `narrow_single_owner` on
`Chunk` in the core crate, is an inherent method with its own unit tests.
The method and its tests stay.

`RetryingChunkGet` survives on production use.
The vertex browser client wraps its network getter in the decorator for
every download and manifest-open path, with a real browser sleeper behind
the `Sleeper` trait.
The issue's either-or, a real implementation or downstream's code,
resolves as neither.
The decorator is not downstream-only: the browser client consumes it in
production.
It also does not need a shipped implementation: the sleeper is
injectable by design, and the consumer brings its own timer.
The `Sleeper` trait moves to `nectar-tasks` with the M2 car the plan
already schedules (move the sleeper into the tasks crate), and the
decorator and `RetryConfig` move to `nectar-storage`.

## The two questions the issue asked

1. `ContentGet` and `SingleOwnerGet`: do they survive the generic collapse?
   The collapse's stated scope, the M3 exit gate, absorbs the spec
   parameter and the body-size constant.
   It does not absorb the registry parameter of the store trait, and no
   plan item moves the registry from the trait onto the store value.
   The wide-to-narrow bridge therefore stays needed after the bundle
   lands: the put side still types at the wide registry, and one store
   value still cannot serve both bounds.
   `ContentGet` survives the collapse and moves in the carve.
   `SingleOwnerGet` did not need a collapse verdict: its in-tree need was
   already gone before the collapse, so it is deleted now rather than
   moved a milestone earlier than the collapse would have killed it.
   If a later design moves the registry off the store trait and onto the
   value, both adapters die with it.
   This document is the input to that deletion.
2. `RetryingChunkGet`: a real implementation or downstream's code?
   The decorator has a production consumer and a downstream-supplied
   timer, so it keeps and moves, with `Sleeper` on the tasks car, as the
   verdicts above record.

## What the carve moves

The tier-3 `nectar-storage` set is: `MemoryStore`, `ContentGet` with
`ContentGetError`, `VerifyingStore` with `VerifyError`, `Tee` with
`TeeError`, and `RetryingChunkGet` with `RetryConfig`.
`Sleeper` moves to `nectar-tasks` with the M2 car.
`NullLoader` stays api-side under the `noop` rule.
The store traits (`ChunkGet`, `ChunkPut`, `ChunkHas`, `TrustedGet`,
`PutUnit`) are the seam move into `nectar-storage-api`, which is carve
scope under the api-crate issue, not this one.

## The vertex note

The browser client imports `RetryingChunkGet` from `nectar-primitives`
and names `Sleeper` beside its browser sleeper.
At the M2 pin bump the import rebinds: `RetryingChunkGet` re-homes to
`nectar-storage` and `Sleeper` to `nectar-tasks`.
The sleeper impl stays vertex's, unchanged.
The rebind lands at the milestone exit with the pin bump, not
continuously, per the plan's lockstep rule.

AI Assistance: opencode (qwen3.8) used for the usage and downstream survey
and this body.
