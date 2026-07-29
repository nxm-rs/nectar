# AGENTS.md

Guidance for agents and contributors working in nectar. Terse by design. When in
doubt, two gates win over anything written here: `cargo clippy -- -D warnings` and
`tools/reinvention-gate.sh`.

`CLAUDE.md` at the same level is a symlink to this file.

## What nectar is

Low-level Ethereum Swarm primitives in Rust: content addressing (BMT), chunks,
proofs, postage stamps, manifest tries, feeds, contract bindings. A Cargo
workspace of `nectar-*` crates published to crates.io. Pre-1.0; APIs may shift
between minor versions. Consumed by the vertex Swarm node. AGPL-3.0-or-later.

## Direction

nectar is converging on one set of layering, concurrency, gating, and packaging
standards. The moves below are in progress; prefer them for new code and align
existing code when you touch it.

- One concurrency substrate. The bespoke kernel and hand-rolled in-flight
  tracking are being removed; every fan-out consumer runs on
  `futures_util::stream::FuturesUnordered` plus a thin bounded-admission
  governor. Delete reinvented machinery rather than extend it.
- Layer discipline. Layer 1 is the single chunk: `get` / `put` / `has` only, over
  the store traits in `nectar-primitives::store`. `get` / `put` exist nowhere
  else. Layer 2 (files, feeds, manifests) never uses `get` / `put`: the read
  handle is uniformly `Reader`; the write verb is domain-specific (`save` /
  `publish` / `build`).
- Packaging. A no_std `nectar-primitives-core` carries the verify subset (BMT
  verify, keccak, SOC address, ecrecover) for the proving lane. The on-swarm KV
  database is `nectar-ldb`; `nectar-manifest` is the trait crate holding the
  `Manifest` trait and shared vocabulary. Benches and examples fold back into
  their crates rather than living as standalone members.
- Parallelism is opt-in, per-workload cargo features, off by default. The default
  build links no rayon. Native uses rayon; wasm stays serial unless a threads
  feature is set; no_std stays serial. SIMD (keccak-batch) is the real heavy-CPU
  path, not rayon.

## Invariants (do not regress)

- One concurrency substrate: `FuturesUnordered` plus the governor. No hand-rolled
  future-set, executor, waker, oneshot, or channel. The no_std excuse is bogus:
  futures-core / util / channel are no_std + alloc and compile for wasm32 and
  riscv64. `nectar-tasks` is the one sanctioned spawn seam and owns the single
  `BoxFuture` alias.
- The reinvention gate (`tools/reinvention-gate.sh`) fails CI if a deleted
  primitive creeps back: hand-rolled `BoxFuture` aliases, copied `MaybeSend` /
  `MaybeSync`, `Mutex<Waker>` cells, `waker: Option<Waker>` slots, copied
  `Unpark` wakers, ad-hoc `impl Wake`, hand-rolled one-shots, `thread::park`
  loops, stray `FuturesUnordered::new()` put-windows, unpaired rayon + oneshot
  submits, stray `catch_unwind`. Each shape has exactly one sanctioned home; a
  copy elsewhere is a reinvention.
- Panic-free production code. The clippy deny set forbids `unwrap` / `expect`,
  indexing / slicing, `as` casts, arithmetic overflow, `panic` / `todo` /
  `unimplemented` and friends. Test code is exempt via
  `#![cfg_attr(test, allow(...))]`. A provably-safe internal site gets a
  justified `#[allow(...)]`; untrusted and parse paths are hardened for real.
- Async tests run through `nectar_testing::run` (or `#[tokio::test]` in a tokio
  adapter module). `clippy.toml` bans every other `block_on` entry point.

## Workflow

- `cargo fmt`; `cargo clippy --all-targets -- -D warnings` (clean); `cargo nextest
  run` (a per-test 60s slow-timeout kills hangs by name). Run
  `tools/reinvention-gate.sh` before pushing concurrency changes.
- Doctests and the wasm smoke test stay on `cargo test` (`--doc`, and the
  wasm-bindgen runner); nextest drives the native host tests.
- Conventional Commits. Open an issue before non-trivial PRs. Tests for
  protocol-touching changes are non-optional; wire-format decoders are fuzzed.
  CLA in `CLA.md`.
- No em-dashes in source, rustdoc, or markdown; `.claude/hooks/content-lint.sh`
  blocks any edit that introduces one. Keep commits, PR bodies, and chat
  em-dash-free too.
- Disclose AI assistance (nxm-rs org policy): add an honest `AI Assistance:` line
  to PR bodies and commit messages. Never the `Co-Authored-By: Claude Code` or
  `Generated with Claude Code` boilerplate footer.

## Claude hooks

`.claude/` ships shared hooks: `rustfmt` on every `.rs` edit, `cargo nextest run`
on touched crates when a turn ends, and the content-lint above. They no-op
outside the dev shell.
