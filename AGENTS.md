# AGENTS.md

This file gives guidance to agents and contributors who work in nectar.
It is terse by design.
When in doubt, one gate wins over anything written here: `cargo clippy -- -D warnings`.

`CLAUDE.md` at the same level is a symlink to this file.

## What nectar is

nectar provides low-level Ethereum Swarm primitives in Rust.
These primitives are content addressing (BMT), chunks, proofs, postage stamps, manifest tries, feeds, and contract bindings.
nectar is a Cargo workspace of `nectar-*` crates that publish to crates.io.
nectar is pre-1.0, so the APIs can change between minor versions.
The vertex Swarm node consumes nectar.
The license is AGPL-3.0-or-later.

## Direction

nectar is converging on one set of layering, concurrency, gating, and packaging standards.
The moves below are in progress.
Prefer them for new code, and align existing code when you touch it.

- Use one concurrency substrate.
  The bespoke kernel and the hand-rolled in-flight tracking are being removed.
  Every fan-out consumer runs on `futures_util::stream::FuturesUnordered` plus a thin bounded-admission governor.
  Delete reinvented machinery.
  Do not extend it.
- Name a surface after what it is: a map, or structured content.
  A map speaks the `HashMap` vocabulary.
  The content-addressed pool is a map whose key is the hash of the value, so the chunk store keeps `get`, `put` and `has` over the store traits in `nectar-primitives::store`.
  It is `put`, not `insert`, because the caller supplies no key.
  An arbitrary-key map (`nectar-ldb`, the manifest formats, the `Manifest` trait) uses `get`, `contains_key`, `insert`, `remove`, `range`, `floor` and `iter`.
  An `insert` replaces the whole binding, so it clears the metadata the key carried unless the call attaches new metadata.
  A manifest key is absolute: the canonical `ManifestPath` starts at the separator, in the stored bytes of both formats, so `"/"` is the manifest root and `"/index.html"` a file below it.
  The root is an ordinary key.
  `get`, `contains_key`, `floor`, `iter` and `range` answer at `"/"` the way they answer anywhere, and nothing special-cases it.
  `dir` lists the children of a path, and no path is a child of itself, which is the only reason `dir("/")` lists the top level without `"/"` in it.
  Well-known metadata carries what used to want a verb of its own: the site index and error documents are metadata on the `"/"` entry, set through the chainable `with_index_document`, `with_error_document` and `with_content_type` builders on the insert guard, on top of the generic `with(key, value)`.
  Do not add a `set_*` verb for a metadata key.
  Because they are metadata on an insert, the replace contract still holds: a bare `insert("/", reference)` clears the site documents.
  These maps are immutable, so a write yields a new root: bind a root with `at` for reads, bind a base with `edit` for writes, and let `commit` hand back the new root.
  One-shot `insert` and `remove` on the store are the sugar over an edit of one op.
  Structured content is not a map.
  Files, feeds and builders speak `save`, `publish` and `build`.
  Segregate the write verbs.
  A content-addressed map writes with `put`, an arbitrary-key map writes with `insert` and `remove`, and structured content writes with `save`, `publish` or `build`.
  Do not lend a write verb across that line in either direction.
  Reads cross the line by design: `load` is the sanctioned read bridge on a map view, because it pulls the bytes a reference points at.
  It is feature-gated, because it needs `nectar-file`, so it lives on the manifest-featured view rather than on the bare map handle.
- Follow the packaging plan.
  A no_std `nectar-primitives-core` carries the verify subset for the proving lane: BMT verify, keccak, SOC address, and ecrecover.
  The on-swarm KV database is `nectar-ldb`.
  `nectar-manifest` is the trait crate that holds the `Manifest` trait and the shared vocabulary.
  Benches and examples fold back into their crates.
  They do not live as standalone members.
- Make parallelism opt-in.
  Parallelism uses per-workload cargo features and is off by default.
  The default build links no rayon.
  Native uses rayon.
  wasm stays serial unless a threads feature is set.
  no_std stays serial.
  SIMD (keccak-batch) is the real heavy-CPU path, not rayon.

## Invariants (do not regress)

- Use one concurrency substrate: `FuturesUnordered` plus the governor.
  Do not hand-roll a future-set, an executor, a waker, a oneshot, or a channel.
  The no_std excuse is false: futures-core, futures-util, and futures-channel are no_std plus alloc and compile for wasm32 and riscv64.
  `nectar-tasks` is the one sanctioned spawn seam and owns the single `BoxFuture` alias.
- One sanctioned home per concurrency primitive; a copy elsewhere is a reinvention (code review enforces this).
  Do not reintroduce hand-rolled `BoxFuture` aliases, copied `MaybeSend` or `MaybeSync`, `Mutex<Waker>` cells, `waker: Option<Waker>` slots, copied `Unpark` wakers, ad-hoc `impl Wake`, hand-rolled one-shots, `thread::park` loops, stray `FuturesUnordered::new()` put-windows, unpaired rayon plus oneshot submits, or stray `catch_unwind`.
- Keep production code panic-free.
  The clippy deny set forbids `unwrap`, `expect`, indexing, slicing, `as` casts, arithmetic overflow, `panic`, `todo`, `unimplemented`, and other panics.
  Test code is exempt through `#![cfg_attr(test, allow(...))]`.
  A provably-safe internal site gets a justified `#[allow(...)]`.
  Harden untrusted and parse paths for real.
- Run async tests through `nectar_testing::run`, or through `#[tokio::test]` in a tokio adapter module.
  `clippy.toml` bans every other `block_on` entry point.

## Workflow

- Run `cargo fmt`.
  Run `cargo clippy --all-targets -- -D warnings` and keep it clean.
  Run `cargo nextest run`; a per-test 60s slow-timeout kills hangs by name.
- Doctests and the wasm smoke test stay on `cargo test`: the `--doc` run and the wasm-bindgen runner.
  nextest drives the native host tests.
- Use Conventional Commits.
  Open an issue before a non-trivial PR.
  Tests for protocol-touching changes are not optional.
  Fuzz the wire-format decoders.
  The CLA is in `CLA.md`.
- Use no em-dashes in source, rustdoc, or markdown.
  `.claude/hooks/content-lint.sh` blocks any edit that introduces one.
  Keep commits, PR bodies, and chat em-dash-free too.
- Disclose AI assistance under nxm-rs org policy.
  Add an honest `AI Assistance:` line to PR bodies and commit messages.
  Never add the `Co-Authored-By: Claude Code` or `Generated with Claude Code` boilerplate footer.

## Claude hooks

`.claude/` ships shared hooks.
The hooks are `rustfmt` on every `.rs` edit, `cargo nextest run` on touched crates when a turn ends, and the content-lint above.
They no-op outside the dev shell.

## Documentation

Write all documentation in ASD-STE100 Simplified Technical English.
Use short sentences, the active voice, and one idea per sentence.
In markdown files, put each sentence on its own line and do not wrap within a sentence; GitHub reflows the file when it displays it.
This keeps a diff to one changed line per changed sentence.
In PR and issue bodies, keep one line per paragraph, because GitHub renders single newlines in a comment as line breaks.
