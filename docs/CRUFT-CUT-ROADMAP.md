# Cruft-cut roadmap

Authoritative, dependency-ordered plan to bring nectar onto a consistent set of
layering, concurrency, and packaging standards. It **supersedes** the wave plan
in issue #474: the previous waves did not converge on a regimented, executable
structure, and several of them harden or extend machinery this plan deletes.

The plan is organised as **seven phases**. Phases 0–4 are the cruft-cut itself
(all breaking, all foundational); phases 5–6 re-sequence the pre-existing epics
onto the new substrate. Each phase is a tracked epic with an explicit **exit
gate**; nothing in a later phase starts until the earlier phase's gate is green.

---

## 1. Standards (the source of truth)

These were converged with the maintainer and are non-negotiable for this plan.

### Layer discipline
- **Layer 1 — the single chunk (atomic swarm unit).** Only `get` / `put` /
  `has`, over the Store abstraction. Traits live in `nectar-primitives::store`:
  `ChunkGet<R>` (associated `Trust` typestate: local = `Verified`, remote =
  `Unverified`), `ChunkPut` (accepts `Chunk<Verified, R>` **only**), `ChunkHas`,
  `TrustedGet`. **Kept exactly as-is** — vertex implements remote
  retrieve/pushsync behind these; nectar ships in-memory test stores. `get`/`put`
  exist *nowhere else*.
- **Layer 2 — files, feeds, manifests.** Never `get`/`put`. The **read handle is
  uniformly `Reader`** (feeds, mantaray, file). The **write verb is
  domain-specific**: files `save`, feeds `publish`, manifests `save`/`build`.

### Concurrency
- One bounded-concurrency substrate: **`futures_util::stream::FuturesUnordered`**
  plus a thin bounded-admission **governor** (`Window` / `Admission` /
  `AdmitPolicy`). No hand-rolled future-set, executor, waker, or channel.
- **Never** reimplement futures machinery under a `no_std` excuse:
  `futures-util`/`alloc` compiles for wasm32 and riscv64 today.
- `nectar-tasks` (runtime-agnostic *spawn* across tokio/wasm) is the one
  justified async abstraction and is kept; it owns the single `BoxFuture` alias.

### Parallelism & target gating
- Thread-parallelism is **opt-in, per-workload cargo features**, off by default:
  `bmt-parallel`, `ecrecover-parallel` (primitives), `sign-parallel`
  (postage-issuer).
- Native → rayon; wasm → serial unless `wasm-threads`; no_std → always serial.
- Rayon pool = physical cores, **auto-halved when AVX-512 double-pump (Zen4) is
  detected**, env override wins.
- The real heavy-CPU win is **SIMD** (`keccak-batch`, runtime AVX/NEON/simd128),
  not rayon. Batched primitives: `bmt_many` / `ecrecover_many` / `sign_many`.

### no_std / proving
- Carve **`nectar-primitives-core`** (no_std: BMT verify + keccak scalar + SOC
  address + ecrecover) for the zkvm/proving lane. Full `nectar-primitives` stays
  std and re-exports core.

### Crate topology
- Current `nectar-manifest` (a sorted-KV store) → **`nectar-ldb`**, a
  first-class on-swarm key-value database (`iter`/`range`/`prefix`/`floor`/
  `subtree` + `Builder`/`apply` stay public).
- The freed name **`nectar-manifest`** becomes the **abstraction crate**: the
  `Manifest` trait + shared vocab (reference/key types, metadata abstraction),
  `no_std`-friendly. `mantaray` (trie) and `nectar-ldb::website` both implement
  it.
- `Manifest` (read + write): `list(root, dir)`, `load(root, path, sink)`,
  `save(base, path, meta, src) -> Root` (immutable new root), associated
  `type Metadata` (mantaray = string map; ldb = typed `KeyId` registry).

### Packaging
- Dissolve `crates/benches` + `crates/examples`; each `[[bench]]`/`[[example]]`
  moves into its host crate as a dev-dep target (invisible to normal builds).
- `crates/integration-tests` keeps **only** the global-allocator-swap probes and
  the `mantaray-old = 0.3.0` differential oracle; the rest folds into per-crate
  `tests/`.
- Drop the dormant codspeed shim → plain `criterion`.

---

## 2. Phase plan

Legend for per-item labels mirrors the repo's taxonomy.

### Phase 0 — Excise the reinvented concurrency machinery `[epic] [breaking] [debt]`
**The keystone. Blocks phases 3–6 and every rescoped streaming epic.**

- **0.1 Delete `nectar-kernel`** — remove `InFlight`/`Driver`/`WalkPolicy`/
  `future`/`BoxFuture`. Stand up `FuturesUnordered` + the thin `Window` /
  `Admission` / `AdmitPolicy` governor as the *only* bounded-concurrency
  substrate. `[breaking][debt][product/swarm][effort/weeks]`
- **0.2 Delete `file/src/inflight.rs`** and rewire `Split`/`Walk` fan-out onto
  the governor. `[breaking][debt][effort/days]`
- **0.3 Centralize `BoxFuture`** in `nectar-tasks`; delete the 3 duplicate
  aliases. `[debt][dx][effort/hours]`

**Exit gate:** `rg` finds no hand-rolled future-set / manual `RawWaker` / custom
poll-loop outside sanctioned sites (`nectar-tasks`, `sync::drive`,
`Waker::noop` fast paths); every fan-out consumer (file, feeds, manifest,
mantaray, postage-issuer) compiles and passes on `FuturesUnordered`.

### Phase 1 — Crate topology `[epic] [breaking]`
**Depends on: nothing (parallel with Phase 0). Blocks phases 2–3, 5.**

- **1.1 Rename `nectar-manifest` → `nectar-ldb`** — the KV database. Keep
  `iter`/`range`/`prefix`/`floor`/`subtree` + `Builder`/`apply` public. Update
  all downstream references (feeds, zkvm design issues). `[breaking][debt][effort/days]`
- **1.2 New `nectar-manifest` trait crate** — `Manifest` trait + shared vocab,
  `no_std`-friendly. `mantaray` and `nectar-ldb::website` implement it. Extend
  the ldb read path to surface the per-key metadata it already stores.
  `[breaking][feature][dx][effort/days]`
- **1.3 Carve `nectar-primitives-core`** — `no_std` subset (BMT verify + keccak
  scalar + SOC address + ecrecover). `nectar-primitives` stays std, re-exports
  core. `[breaking][debt][product/swarm][effort/weeks]`

**Exit gate:** workspace builds; `nectar-ldb`, `nectar-manifest` (trait),
`nectar-primitives-core` exist; both manifest formats sit behind the trait;
`nectar-primitives-core` builds for `riscv64imac-unknown-none-elf`.

### Phase 2 — Parallelism & gating discipline `[epic] [perf] [dx]`
**Depends on: Phase 1.3 (primitives-core) + Phase 0 (governor).**

- **2.1 Per-workload features** — `bmt-parallel`, `ecrecover-parallel`
  (primitives), `sign-parallel` (postage-issuer), off by default; wasm serial
  unless `wasm-threads`; no_std serial. `[perf][dx][effort/days]`
- **2.2 Rayon pool policy** — physical cores, Zen4 AVX-512 auto-halving, env
  override. Delete the dead `get_level_segments` rayon site. `[perf][effort/days]`
- **2.3 Batched primitives** — `bmt_many` / `ecrecover_many` / `sign_many`
  behind the gate. `[perf][feature][effort/days]`

**Exit gate:** default build links no rayon; feature matrix green on
native/wasm/no_std; SIMD path unchanged; `get_level_segments` gone.

### Phase 3 — Layer-2 API normalization `[epic] [breaking] [dx]`
**Depends on: Phase 0 (governor) + Phase 1.2 (Manifest trait).**

- **3.1 Feeds** — `getter.rs`→`reader.rs`/`Reader`; `updater.rs`→`publisher.rs`/
  `Publisher` (`publish`/`publish_at`); `update.rs` stays = `FeedUpdate`;
  `latest()`→`Latest`, `at(index)` for a slot. Fold `topic.rs`→`feed.rs`;
  `generators.rs`→`arbitrary.rs`; sim→`#[cfg(feature="sim")]` test. Epoch
  deferred (keep the `Index` seam, document as future). `[breaking][dx][effort/days]`
- **3.2 File** — `File` handle (`File::new(store, policy)`,
  `f.load(root, &mut sink)`, `f.save(src) -> Root`); one `Source` trait +
  adapters (`&[u8]`/`ReadAt`/`AsyncRead`); keep positional `DataSink`; engines
  internal; `tokio/` optional shim. `[breaking][dx][effort/days]`
- **3.3 Manifest via the trait** — `mantaray` and `nectar-ldb::website` expose
  `list`/`load`/`save` + `Metadata` through the trait. `[dx][effort/days]`

**Exit gate:** no L2 `get`/`put` verbs anywhere; read handle uniformly `Reader`;
write verbs are `save`/`publish`/`build`.

### Phase 4 — Test/bench topology `[epic] [debt]`
**Depends on: nothing structurally (do after Phase 0 to avoid churn on moved files).**

- **4.1 Dissolve `crates/benches` + `crates/examples`** → per-host
  `[[bench]]`/`[[example]]` dev-dep targets. Keep `dump_deployments` runnable
  (repoint `upstream-addresses.yml` to `-p nectar-contracts`). `[debt][dx][effort/days]`
- **4.2 Slim `crates/integration-tests`** to allocator-swap probes +
  `mantaray-old` differential; fold conformance/order/scan/reader suites into
  per-crate `tests/`. `[debt][effort/days]`
- **4.3 codspeed → plain `criterion`.** `[debt][effort/hours]`

**Exit gate:** `crates/benches` and `crates/examples` gone; normal `cargo build`
unaffected (no dev-dep leakage); CI green; benches still compile under the lint
gate.

### Phase 5 — Rescoped downstream epics `[various]`
**Depends on: phases 0–3.** These pre-existing issues survive with their goals
intact but their mechanism re-aimed (see §3):
- File read-ahead & read-path hardening (**#473**) on `Window`/`Admission`.
- Streaming-first write / no-HOL (**#553**, **#584**, **#484**) on the governor.
- Manifest builder streaming (**#568**, **#558**) on `nectar-ldb` + governor.
- L2 chunk primitives (**#82**): feeds renames done in 3.1; PSS/ACT (**#86**,
  **#87**, **#88**) on the `Manifest` trait + typed stores (**#522**).
- Feeds legacy v1 payloads (**#92**), unchanged in substance.
- zkvm cluster (**#301**–**#306**, **#490**) — someday, now structurally
  unblocked by `nectar-primitives-core` + `nectar-ldb`.

### Phase 6 — Lint burn-down `[epic] [debt] [breaking]`
**Depends on: everything.** **#230** runs last and is re-baselined onto the
renamed/new crate set (`nectar-ldb`, `nectar-manifest` trait, `nectar-primitives-core`)
after the delete/rename/gating churn has settled the lines it must refactor.

---

## 3. Per-issue disposition (all open issues)

Verdicts: **ALIGNED** (keep as written) · **RESCOPE** (goal kept, mechanism/
wording changes) · **SUPERSEDED** (obsolete, fold/close) · **CONFLICT**
(contradicts a decision; re-aim or park) · **BLOCKED** (valid, waits on a phase)
· **UNAFFECTED** (orthogonal).

| # | Verdict | Phase | Action |
|---|---|---|---|
| #584 | CONFLICT | 0, 5 | Consolidation epic — keep the "use idiomatic crates" thesis, but its substrate *keeps* the kernel Driver/PutSink; re-aim onto `FuturesUnordered` + governor. Kernel half deleted, not a target. |
| #553 | CONFLICT | 0, 5 | "Fold onto walk-driver patterns" = the kernel. Keep streaming-first/no-HOL goals; rebuild on `FuturesUnordered`+`Admission`. |
| #502 / #418 | CONFLICT | 0 | #418 is literally "bounded-admission kernel crate". Convert to the thin `Window`/`Admission` governor over `FuturesUnordered` (no `Driver`); keep the "no shared walker engine" verdict + property suite. |
| #84 | CONFLICT | 3.1 | Implements epoch-grid indexing/finders — **deferred**. Re-aim to "document the `Index` seam for a future epoch" or park. Do not implement. |
| #568 | RESCOPE | 5 | Forward-streaming builder input — retarget onto `nectar-ldb` + `FuturesUnordered`/`Admission`. |
| #558 | RESCOPE | 5 | Overlap per-file splits — retarget onto governor; whole-`Bytes` fix becomes the `Source`/`DataSink` seam in `nectar-ldb`. |
| #540 | RESCOPE | 2, 4 | Wrapped-split throughput bench moves to host crate; `PutWindow`→`Admission`; `SignWindow` under `sign-parallel`. |
| #484 | RESCOPE | 0, 2 | Streaming stamping — engine→governor; sign path→`sign-parallel` + `sign_many`; no_std→primitives-core; drop #418 dep. |
| #474 | SUPERSEDED | — | Roadmap replaced by this document; rewrite its body to point here. |
| #473 | RESCOPE | 3.2, 5 | Read-ahead rides `Window`/`Admission`; `File` `load`/`save`/`Source` folds in; `#405` keeps `DataSink`. |
| #472 | RESCOPE | 1.3, 2 | Becomes the primitives-core split + `bmt-parallel`/`ecrecover-parallel` gating; keep #407 (`nectar-tasks`), drop #418. |
| #538 | BLOCKED | 2 | AdaptiveWindow-through-a-real-walk test — waits on the Phase 0 governor; API names change to `Window`/`Admission`. |
| #522 | ALIGNED | 5 | Kind-narrowed typed stores over `ChunkGet<R>` — exactly the kept L1 design; DoD carries into #86/#88. |
| #504 | UNAFFECTED | — | Uniform curve-point encoding — orthogonal. |
| #490 | BLOCKED | 5 | zkvm stamp aggregation — rests on primitives-core; someday. |
| #466 | UNAFFECTED | 4 | Alloc-witness probes are exactly the suite Phase 4 keeps in integration-tests. |
| #429 | UNAFFECTED | — | SwarmSpec-vs-chain verify already in `nectar-contracts`; matches no-domain-bleed. |
| #405 | ALIGNED | 3.2 | OPFS sync handle maps onto the kept positional `DataSink`. |
| #401 | ALIGNED | — | Single-hash SOC seal fast path — consistent with `ChunkPut` Verified-only. |
| #319 | UNAFFECTED | 6 | Stringly-error burn-down — orthogonal; sequence near #230. |
| #306 | BLOCKED | 5 | Proof-size/gas bench — zkvm someday; harness moves under Phase 4; crate is `nectar-ldb`. |
| #305 | RESCOPE | 5 | Prove/verify demo — retarget guest reuse to `nectar-ldb`; someday. |
| #304 | RESCOPE | 5 | On-chain verify — manifest reader it proves over is `nectar-ldb`; someday. |
| #303 | RESCOPE | 5 | Order-statistic statements over `nectar-ldb`'s counted baseline; someday. |
| #302 | RESCOPE | 5 | Aggregate statements reuse `nectar-ldb` reader/apply in-guest; someday. |
| #301 | RESCOPE | 5 | Foundation guest depends on `nectar-primitives-core` + `nectar-ldb` on rv64; someday. |
| #230 | RESCOPE | 6 | Re-baseline the suppression register onto the renamed/new crate set; sequence last. |
| #116 | RESCOPE | 2 | Rayon local encrypted `read_all` → per-workload gated feature; note SIMD is the bigger win; someday. |
| #92 | UNAFFECTED | 5 | Legacy v1 feed decoder — orthogonal; lands in the renamed `reader.rs`. |
| #88 | RESCOPE | 5 | ACT "small manifest seam" becomes the new `Manifest` trait; bind typed single-owner store (#522). |
| #87 | RESCOPE | 5 | Reply-envelope SOC mining — add #522 typed store DoD; parallel mining is a gated feature. |
| #86 | RESCOPE | 5 | Trojan-chunk nonce mining → per-workload gated feature; bind content-typed store. |
| #82 | RESCOPE | 3, 5 | L2 umbrella reframed to new naming (`Reader`/`Publisher`/`Latest`), `Manifest` trait home, typed stores, epoch deferred. |
| #43 | UNAFFECTED | — | PeerRecord byte type — orthogonal. |

---

## 4. New issues to file (gaps with no existing coverage)

Each is a sub-issue of its phase epic.

1. **Delete `nectar-kernel` + `file/src/inflight.rs`; adopt `FuturesUnordered` +
   governor** — Phase 0. `[epic child][breaking][debt][product/swarm][effort/weeks]`
2. **Centralize `BoxFuture` in `nectar-tasks`** — Phase 0.3. `[debt][dx][effort/hours]`
3. **Rename `nectar-manifest` → `nectar-ldb`** — Phase 1.1. `[breaking][debt][effort/days]`
4. **New `nectar-manifest` trait crate + `Manifest` trait + shared vocab; ldb
   metadata read-path** — Phase 1.2. `[breaking][feature][dx][effort/days]`
5. **Carve `nectar-primitives-core` (no_std)** — Phase 1.3 (may fold into #472).
   `[breaking][debt][product/swarm][effort/weeks]`
6. **Per-workload parallel features + rayon pool policy + delete dead
   `get_level_segments` + batched `*_many`** — Phase 2 (folds fragments of #116/
   #86/#87/#484). `[perf][dx][effort/days]`
7. **Feeds `Reader`/`Publisher`/`Latest` renames + file folds (topic→feed,
   generators→arbitrary, sim→cfg test)** — Phase 3.1. `[breaking][dx][effort/days]`
8. **`File` handle (`new`/`load`/`save`) + `Source` trait + adapters** — Phase
   3.2. `[breaking][dx][effort/days]`
9. **Dissolve `crates/benches` + `crates/examples`; slim `integration-tests`;
   codspeed→criterion** — Phase 4. `[debt][dx][effort/days]`

---

## 5. Dependency graph (execution order)

```
Phase 0 (delete kernel → FuturesUnordered+governor) ─┐
Phase 1.1 (ldb rename) ──► 1.2 (Manifest trait) ─────┤
Phase 1.3 (primitives-core) ─────────────────────────┤
                                                     ├─► Phase 2 (gating)
Phase 4 (bench/test topology) ───────────────────────┤     needs 0 + 1.3
                                                     └─► Phase 3 (L2 APIs)
                                                           needs 0 + 1.2
Phase 3 + 2 done ──► Phase 5 (rescoped epics) ──► Phase 6 (#230 lint)
```

- Phases 0, 1, 4 have no cross-dependencies among themselves → run in parallel.
- Phase 2 needs 0 (governor) + 1.3 (primitives-core).
- Phase 3 needs 0 (governor) + 1.2 (Manifest trait).
- Phase 5 needs 0–3. Phase 6 is last.
