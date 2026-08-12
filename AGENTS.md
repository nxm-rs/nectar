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

Each rule below names a file and line that already conforms, so a reviewer can check the claim rather than take it.

- Rule 1: `crates/feeds/src/error.rs:13`, `#[non_exhaustive]` on `FeedError`.
- Rule 2: `crates/primitives/src/envelope/mod.rs:580`, a `#[from]` wrapping variant.
- Rule 3: `crates/feeds/src/error.rs:17`, `AddressMismatch` carrying typed `expected` and `actual` fields rather than a formatted string.
- Rule 4: `crates/primitives-core/src/error.rs:163`, the `BoxedError` alias.
- Rule 5: no conforming example exists yet.
  `RingExhausted` is declared in more than one crate with the same shape and no conversion between them, which is the violation this rule exists to stop; #688 gives it one owner.
- Rule 6: `crates/postage-usage/src/error.rs:271` and `:330`, `UsageError::is_corruption` and `is_recoverable`, with the exhaustive classification test at `:387` that fails to compile when a variant is added without being classified.

Applying rule 3 across the workspace is #319, rule 1 is #690, and rules 2 and 5 for the postage family are #688.
This section is the standard; those issues are the migration.

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
