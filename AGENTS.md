# AGENTS.md

This file gives guidance to agents and contributors who work in nectar.
It is terse by design.
When in doubt, one gate wins over anything written here: `cargo clippy -- -D warnings`.

`CLAUDE.md` at the same level is a symlink to this file.

## What nectar is

nectar provides low-level Ethereum Swarm primitives in Rust.
These primitives are content addressing (BMT), chunks, proofs, postage stamps, manifest tries, feeds, and contract bindings.
nectar is a Cargo workspace of `nectar-*` crates, most of which publish to crates.io.
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
  A `remove` is exact-key, so it clears that key's value and metadata and no other key's: a key with children keeps every one of them, a childless leaf is pruned, and removing an unbound or absent key is a no-op that leaves the root where it was.
  Never let a map `remove` take a subtree or a prefix.
  mantaray keeps a boundary remove, `remove_subtree`, outside the map vocabulary; its consumer is the editor oracle in `crates/mantaray/src/oracles.rs`, which the committed fuzz corpus exercises, so do not delete it as unused.
  A manifest content key is bare and verbatim: the canonical `ManifestPath` stores the bytes it was given, so `index.html` is the key `index.html` on the wire of both formats.
  mantaray therefore stays byte-identical to the reference client's v0.2 wire, and `crates/integration-tests/tests/mantaray/bee_vectors.rs` guards that byte for byte against three roots the reference client produced, with `bee_layout.rs` pinning the shape those bytes carry.
  Never root a stored key at the separator, and never model the manifest's own configuration as a key.
  The site index and error documents are an explicit Option-typed API: `index_document()` and `error_document()` on the read view answer `None` when unset, and the chainable `set_index_document` and `set_error_document` on the batch set them.
  Each lands in the format's native root slot: mantaray writes the `"/"` node's metadata beside a zero-address entry, which is the layout the reference client reads, and `nectar-ldb` writes its root manifest-metadata.
  The content map never surfaces that slot, and never surfaces an empty key: `get`, `insert`, `remove`, `contains_key`, `iter`, `range`, `floor`, `dir` and `load` are over content keys alone.
  The empty path is a listing prefix and not a key, so `dir` lists the top level with it while every other verb answers absent.
  These maps are immutable, so a write yields a new root: bind a root with `at` for reads, and hand a base plus a staged batch to a write that returns the new root.
  The `Manifest` seam spells that write `apply(base, batch)` over a seam-owned `Batch`; a native database keeps its own handle, `edit(base)` plus `commit`.
  Ops fold in submission order, so the last verb staged at one key is the one that lands.
  One-shot `insert` and `remove` are the sugar over a batch of one op.
  Structured content is not a map.
  Files, feeds and builders speak `save`, `publish` and `build`.
  Segregate the write verbs.
  A content-addressed map writes with `put`, an arbitrary-key map writes with `insert` and `remove`, and structured content writes with `save`, `publish` or `build`.
  Do not lend a write verb across that line in either direction.
  Reads cross the line by design: `load` is the sanctioned read bridge on a map view, because it pulls the bytes a reference points at.
  It is feature-gated, because it needs `nectar-file`, so it lives on the manifest-featured view rather than on the bare map handle.
- Follow the packaging plan.
  `docs/RESTRUCTURE-PLAN.md` carries the detail and the crate set, so this file states the shape only.
  The crates form a tier ladder: data and proofs at the bottom, behaviour traits with `noop` implementations next, implementations above them, and the `nectar` facade on top.
  A crate depends on its own tier and below only.
  The api crates hold the behaviour traits and the `noop` implementations and no real implementation, so a downstream crate compiles without an implementation.
  The ladder is the freeze mechanism: a milestone cutover is a crate boundary, not policy text.
  Two names move at the carve.
  The proving core, today `nectar-primitives-core`, takes the `nectar-primitives` name, and the routing predicates move into it.
  The manifest trait crate, today `nectar-manifest`, becomes `nectar-manifest-api` beside three further api crates.
  `no_std` is the proving lane and not a workspace property.
  Six crates carry the proving-lane guarantee.
  A guest links chunk verification, BMT hashing and segment proofs, single-owner chunk recovery, stamp verification and binding, and ldb node decode and descent.
  Everything else is std-only.
  Three infrastructure crates keep an unconditional `no_std` without the lane guarantee, because it is already true.
  The on-swarm KV database is `nectar-ldb`.
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
- One sanctioned home per concurrency primitive; a copy elsewhere is a reinvention, and `tools/reinvention-gate.sh` enforces the list in CI.
  The gate fails only on an unannotated occurrence: a sanctioned site carries a `// reinvention: <reason>` comment on the line above, in the shape of the tree's `#[allow(..., reason = "...")]` attributes, so a rewire annotates its own line inside the same diff and the gate file never changes.
  Do not reintroduce hand-rolled `BoxFuture` aliases, copied `MaybeSend` or `MaybeSync`, `Mutex`-guarded waker cells, copied `Unpark` wakers, `impl Wake` outside the sanctioned homes, hand-rolled one-shots, `thread::park` loops, the `settle_one`/`sweep` put-window drain outside `nectar_governor::PutSink`, unpaired rayon plus oneshot submits, or stray `catch_unwind`.
  `FuturesUnordered` itself is banned nowhere: a walker owns its own set, and the governor deliberately does not re-export the type.
  A single `waker: Option<Waker>` slot on a `&mut self` poll API is the parking idiom, and a write window that settles unordered over a bare admission window records itself as sanctioned with the same comment.
- Keep production code panic-free.
  The clippy deny set forbids `unwrap`, `expect`, indexing, slicing, `as` casts, arithmetic overflow, `panic`, `todo`, `unimplemented`, and other panics.
  Test code is exempt through `#![cfg_attr(test, allow(...))]`.
  A provably-safe internal site gets a justified `#[allow(...)]`.
  Harden untrusted and parse paths for real.
- Run async tests through `nectar_testing::run`, or through `#[tokio::test]` in a tokio adapter module.
  `clippy.toml` bans every other `block_on` entry point.

## Errors

The error layer is where duplication now lives, so these six rules are the standard the rest of the workspace is held to.

1. Every public error type is `#[non_exhaustive]`.
   Adding a variant is then not a breaking change.
2. Every wrapping variant carries a `#[source]` or a `#[from]`.
   Never interpolate an inner error into the message text: that loses the chain and makes the inner error impossible to downcast.
3. Errors carry structured fields, not strings.
   A string field survives only where the payload genuinely is text.
4. A cross-crate boundary erases to a boxed error, not to a string literal.
   Use the `BoxedError` alias so the choice is uniform.
5. One error condition has one home.
   A variant with the same shape in two crates gets one owner and a conversion, rather than being declared twice.
6. Every public error exposes a retryability predicate.
   Classify with `const fn` predicates, and guard the classification with an exhaustive test so a new variant cannot be added without being classified.

### Conformance checklist

Each rule below names a conforming item, so a reviewer can check the claim rather than take it.
Items name the type, not a line, so the checklist cannot drift with an unrelated edit.

- Rule 1: `FeedError` in `crates/feeds/src/error.rs` carries `#[non_exhaustive]`.
- Rule 2: `AnySealError` in `crates/envelope/src/lib.rs` wraps each scheme seal error through a `#[from]` variant.
- Rule 3: `AddressMismatch` in `crates/feeds/src/error.rs` carries typed `expected` and `actual` fields rather than a formatted string.
- Rule 4: the `BoxedError` alias in `crates/primitives-core/src/error.rs`.
- Rule 5: the one `RingExhausted` in `crates/postage-issuer/src/error.rs`, which `IssuerError`, `CounterError` and `UsageError` each convert from with `#[from]`.
- Rule 6: `UsageError::is_corruption` and `UsageError::is_recoverable` in `crates/postage-usage/src/error.rs`, with the `every_variant_is_classified_into_exactly_one_group` test that fails to compile when a variant is added without being classified.

Applying rule 3 across the workspace is #319, and rule 1 is #690.
This section is the standard; those issues are the migration.

## Vector provenance

A pinned vector asserts bytes produced elsewhere.
Without its origin, the vector only re-asserts nectar's own behaviour under a different name.
Every pinned vector in the tree states its origin.

The origin takes one of two forms, in a header beside the vector.
Inline values carry the header in the module docs or the comment of their asserting file; golden files carry it beside the fixture.

- Upstream: the value was copied or re-run from upstream.
  State the upstream file path and the upstream test or data name.
- Generated: no upstream value exists.
  State what produced the value (a pinned reference-client module, a nectar generator tool, or nectar's own earlier output), the fixed inputs, and why no upstream vector exists.

Values guarded by a periodic drift check against the canonical upstream source state the check and the source it compares.
The contract deployments are guarded that way: `tools/upstream-check` compares them against `go-storage-incentives-abi` weekly in CI.

A file that is committed generator output carries the machine-readable `provenance` block beside the prose header.
The block records the `generator`, the regeneration `command`, the pinned `reference`, and the `generated_at` timestamp, with `notes` for the facts the bytes cannot show.
`crates/postage/tests/testdata/reference-stamps.json` conforms: the block is written by `tools/stamp-vectors`, which pins the reference client in `go.mod`.

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

### How much to write

The default is no comment.
A comment earns its place only when it says something the code cannot: a non-obvious invariant, a wire or specification constraint, why the obvious approach is wrong, or a footgun.
One line is the target and three is the ceiling.
A paragraph belongs in the issue or the pull request body.
Never restate the signature, narrate the next statement, explain the language, or record rationale.
Rationale goes in the pull request body, where review can see it and history keeps it.
Do not add a worked example unless the API is genuinely unobvious.
The same limit applies to comments in TOML, YAML and shell.
Apply this test before you keep a comment: if a reviewer learns nothing from it that the identifier and the body already tell them, delete it.
`crates/tasks/src/lib.rs:143` conforms: one line, and it states the drop behaviour that the signature does not show.

### No project management in source

Source carries no issue numbers, no `owner/repo#NNN` references, no tracker links, no `Tracking:` lines and no `TODO(#N)` markers.
This applies to every comment, in Rust, TOML, YAML and shell.
A comment states what is true of the code; where the work is tracked is not, and the reference rots as soon as the issue closes or moves.
Remove such a reference from any file you edit, not only from the lines you add.
Three things are not project management and stay: the `repository` field in `Cargo.toml`, links in README files, and identifiers such as RUSTSEC advisory and CVE numbers.
`deny.toml` conforms: it explains why its ignore list diverges from `.cargo/audit.toml` without naming an issue.

### No upstream file citations in code

Do not cite upstream files, paths, line numbers or commits in rustdoc or comments.
A cited location rots when the upstream repository moves, and nothing detects the break.
The provenance of a ported algorithm lives in the crate's root `SPEC.md`, beside the ported algorithm, where a review catches the drift.
Pinned-vector provenance headers keep the form the `## Vector provenance` section defines.
