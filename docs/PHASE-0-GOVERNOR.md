# Phase 0 governor: home and API

Design note for the cruft-cut roadmap Phase 0 (issues #610, #611).
It fixes the one decision that gates every consumer rewire: where the bounded-admission governor lives once the `nectar-kernel` name goes away, and what its surface is.
Read it against `docs/CRUFT-CUT-ROADMAP.md` §Phase 0.

## 1. What Phase 0 actually deletes

`nectar-kernel` is not a reinvented executor.
It holds no `RawWaker`, no manual poll loop, and no hand-rolled future set: it already re-exports `futures_util::stream::FuturesUnordered` and `futures_core::future::BoxFuture`.
It is five pieces, and the roadmap standards keep most of them.

| Module | Item | Verdict |
|---|---|---|
| `window.rs` | `Window` (slot count plus Little's-law sizing) | **Keep.** This is the thin governor. |
| `admission.rs` | `Admission` (head-slot liveness predicate) | **Keep.** Proptest liveness suite. |
| `policy.rs` | `AdmitPolicy` / `Fixed` / `FromFn` / `Observations` | **Keep.** The adaptive-window seam. |
| `put_sink.rs` | `PutSink` (bounded put window over `FuturesUnordered`) | **Keep.** Already `FuturesUnordered` plus admission. |
| `driver.rs` | `Driver` / `StaticDriver` / `WalkPolicy` | **Delete.** The shared walk-loop abstraction. |
| `future.rs` | `BoxFuture` | **Delete.** Duplicate of `nectar-tasks`; folds in (#611). |

So the excision is narrow: the `WalkPolicy` trait and the `Driver::poll` loop, plus the duplicate `BoxFuture` alias.
The admission math and the put window survive.

## 2. The decision: governor home

Two viable homes once `nectar-kernel` (a name that reads as an executor) retires.

- **(A) Rename `nectar-kernel` to `nectar-governor`.**
  Delete `driver.rs` and `future.rs` from it; keep `window`/`admission`/`policy`/`put_sink` and their test suites in place.
  Consumers import `Window` / `Admission` / `AdmitPolicy` / `PutSink` from the renamed crate and drive their own `FuturesUnordered` loops.
- **(B) Fold the governor into `nectar-tasks`.**
  Both crates are `no_std` plus `alloc` and depend on `futures-core`, so it compiles.
  `nectar-tasks` becomes the single async-primitives crate: spawn seam plus bounded admission.

**Decision: (A).** Confirmed with the maintainer.

- `nectar-tasks` owns one documented concern: the runtime-agnostic spawn seam (`Spawn`, `TaskHandle`, handoff, tokio/wasm spawners).
  Bounded admission over a `FuturesUnordered` is flow control, a different concern.
  Keeping two small single-purpose crates honours the no-domain-bleed standard; (B) makes a grab-bag.
- The `Window`/`Admission` proptest liveness suite stays put under (A); (B) forces it to move.
- `BoxFuture` still centralises in `nectar-tasks` under (A). That is #611, and it is independent of the home choice.

Distributing the governor into each consumer (no shared crate) is rejected: it re-scatters the exact per-walker hand-rolling the shared `Admission` predicate removed, and it contradicts the roadmap's one-bounded-concurrency-substrate standard.

## 3. Consumer rewire surface

Eleven files across five crates import from `nectar-kernel`.
Only the `Driver`/`WalkPolicy` users carry real work; the rest is a one-line import-path change (`nectar_kernel` to `nectar_governor`).

**Real rewire, the walk loop dies (drive `FuturesUnordered` directly, admit via `Admission`):**

- `feeds/src/getter.rs`: `Driver`, `WalkPolicy`
- `ldb/src/{traverse,store,scan,apply}.rs`: `Driver`, `WalkPolicy`
- `mantaray/src/editor.rs`: `Driver`, `WalkPolicy`; `mantaray/src/cursor.rs`: `StaticDriver`, `WalkPolicy`
- `file/src/walk/engine.rs`: `StaticDriver`

**Import-path only, items kept:**

- `PutSink` users: `file/src/split/{engine,relay}.rs`
- `Admission` / `Window` users: `postage-issuer/src/pipeline/*`, `ldb/src/{builder,frontier}.rs`, `file/src/{store,config}.rs`

`file/src/inflight.rs` does not exist; the roadmap's step 0.2 wording is stale.
The file fan-out already sits in `walk/engine.rs` (`StaticDriver`) and `split/{engine,relay}.rs` (`PutSink`).
Only `walk/engine.rs` needs the loop rebuilt; the split engines keep `PutSink`.

## 4. Exit gate (QA, not checked in)

The Phase 0 gate is a verification pass, not a committed CI fixture.
Run it once at phase close:

```
rg -n 'RawWaker|from_raw|WalkPolicy|\bDriver\b|StaticDriver' crates --glob '*.rs'
```

Expect zero hits outside the deleted modules.
Sanctioned `Waker::noop` fast paths stay: `file/src/sync.rs`, `file/src/read/cancel.rs`, `put_sink.rs`, and the `postage-issuer` bridge and stamp drivers.
There is no `RawWaker` in the tree today, so no standing lint is warranted.

## 5. Sequencing

1. **#611 first.** Centralise `BoxFuture` in `nectar-tasks`, delete `kernel/src/future.rs`, repoint the consumers.
   Cheap (`effort/hours`), and it shrinks the #610 diff.
2. **#610.** Rename the crate, delete `driver.rs`, rebuild the eight `Driver`/`WalkPolicy` sites onto `FuturesUnordered` plus `Admission`, repoint the import-path-only sites.
3. Re-aim the CONFLICT issues (#584, #553, #502/#418, #484) and mark #474 superseded before anyone hardens the deleted `Driver`.
