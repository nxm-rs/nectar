# nectar: production plan

Status as of 2026-08-12.
This document supersedes the cruft-cut roadmap that lived only inside draft pull request #609, which itself superseded the #474 wave plan.
Neither of those ever merged, so this file is the first plan of record in the tree.

## Decisions that shape this plan

1. Priority order is architecture completion and interop correctness together, then release, then new capability.
2. The manifest stack lands in order.
3. Decruft is aggressive, targeting 12 to 14 publishable crates.
4. The novel formats get written specifications, and the ldb wire format is renamed to "ldb v1".
5. Agents pre-review the stack and post findings to the pull requests, and the maintainer spot-checks the flagged items.
6. There is no v0.4.1, and the next release is 0.5.0.

## Where the repository stands

The code quality bar is high.
There is one `unsafe` function in the workspace.
The workspace denies the whole panic lint family.
There are no TODO, FIXME or `todo!()` sites.
Clippy is clean across the workspace, and now fails the build on any warning.

The problems are structural rather than local, and the plan below is ordered to avoid paying for the same work twice.

## Ordering rules

Apply these to any issue filed later, not only to the ones listed here.

1. Delete before you refactor.
   An issue that removes a crate, a module, a type or a dependency comes before any issue that touches the same code for quality reasons.
   This one rule reordered six scheduled items when the plan was written.
2. Turn a gate on before the work it guards, and require an aggregator rather than the leaf jobs.
   A later job appended to the aggregator becomes blocking with no second ruleset edit.
3. Put a design decision before the code that depends on it, and a rename before the specification that describes it.
   A specification written first is written twice.
4. Give workspace-wide sweeps their own window.
   A change that edits one line in fifty files conflicts with everything, and is cheap to redo but expensive to merge.
5. Write anything that enumerates the tree last.
   A crate count, a publish order or a README table written before the member set settles is wrong on the day it is read.
6. Scope from the tree, never from the issue body.
   Six bodies were provably stale against HEAD when this plan was written, and three more were found overstated during stage 1.

## The stages

### Stage 0: decide and clear the dead work

In progress.
Land this document, write the error section in `AGENTS.md` (#686), and clear the tracker of work that no longer applies.

Closed as part of this stage: #522 (drained, every roll-up child landed), #646 (superseded by the manifest KV decision, which deliberately kept map vocabulary), #558 (folded into #645), and pull request #649 (depends on the dissolved `nectar-loadsave`, re-cut as #680).

Gate: this document exists in the tree, `AGENTS.md` has an error section, and no in-tree citation of the unmerged roadmap survives.

### Stage 1: turn the merge gate on

Complete.
#698, #700, #702 and #699 landed as pull requests #724, #726, #727 and #728, followed by #733 which closed the loose ends.

`lint.yml` now fails on any clippy warning across all twelve invocations, runs `cargo deny` over advisories, bans, sources and licences, compiles the declared minimum Rust version, and enforces formatting and the em dash ban.
All five leaves report through one `lint success` aggregator, so a later job becomes blocking by appending to a `needs` list rather than by editing the branch ruleset.

Ruleset 5744032 requires six contexts: `unit success`, `no_std success`, `lint success`, `fuzz success`, `audit` and `feature propagation`.
Strict mode is off, because it would serialize every merge behind a rebase.

Two decisions were taken during this stage.
The declared minimum Rust version became 1.94, matching the version the repository actually builds, because nothing had ever compiled the declared 1.92.
The `nectar-swarms` crate folds into `nectar-primitives` and is deprecated (#679), rather than being kept as a consumer-only crate.

Verified rather than assumed: a pull request carrying a deliberate lint failure reported `BLOCKED`, and a pull request touching only markdown produced all required contexts rather than stranding one at `Expected`.

### Stage 2: delete

#681 and #678 run in parallel, because `governor` and `postage-issuer` share no file.
#618, then #679 execution, then #680 run in series, because all three edit the workspace member list and the two CI crate lists.

#678 alone removes over two thousand lines carrying six `calculate_bucket` call sites, the hand-written `RingExhausted` producer and two shard cursors that three later issues would otherwise refactor first.

Gate: `cargo metadata` reports the final member count and it is written down.

### Stage 3: the breaking trains

#685, #687, #689 and #688 in that order, in parallel with #683 then #682, in parallel with #708.
The three tracks share no file.

The design question in #689 is settled: `StampIndex` gains the spec parameter and carries a `Bucket<S>` rather than a bare integer.
The generic is viral, so `Stamp` and its codec impls are expected to gain it too.
The change must be visible in the type system and invisible on the wire.

Gate: no public postage API takes a raw `u8` depth, `NodeGet` and `NodePut` are gone, and one `Listing`, one `Cursor` and one `AddressStream` remain.

### Stage 4: vectors and specifications

Against the settled names: #671 first, then #669, #670, #672, #673, #675, #674, #710, #712 and #707.

This is the largest gap between what nectar claims and what it proves.
Only three capabilities are anchored to bytes the reference client produced, and everything else rests on constants nectar generated for itself.

Gate: every wire format nectar ships has at least one vector whose expected bytes came from the reference client, every test vector carries a provenance header, and every `spec N.N` citation in `crates/ldb` resolves.

### Stage 5: the quiet window

Workspace-wide sweeps, with nothing else in flight: #319, #690, #691, #699 follow-ups and #701.

Gate: no public error variant carries a bare `String`, and every public error enum is `non_exhaustive`.

### Stage 6: the streaming seam

#568, #645, #692 and #615.
Decide at the head of this stage whether #645 lands or `nectar-manifest` ships with `publish = false`.

Gate: a memory-bounded read and write of a large manifest completes with peak live bytes bounded.

### Stage 7: release paperwork, then cut 0.5.0

#716, #713 and #715.
All three enumerate the tree, so all three go last, per ordering rule 5.

Gate: the milestone is empty and the tag fires.

### Stage 8: after the tag

#717, #718, #719 and #720, then #230 and the long tail.

## The critical path

#678, #685, #687, #689, #688, then #319 in the quiet window.

Nothing parallelizes it.
All of them edit `crates/postage/src/batch.rs`, `postage/src/util.rs`, `postage-issuer/src/counter.rs` and `postage-usage/src/table.rs`.

One cost stated plainly: #669 is a live correctness bug and this order delays it to stage 4, because it shares `crates/file/src/read/file.rs` with #708.
Until then a redundancy-enabled root reports about 9.2 exabytes, because the reference client sets `span[7] = level | 0x80` and nectar decodes the span as a plain little-endian integer.

## One hazard to handle by hand

Strike the clause "a grep over the layer-2 crates shows no `pub fn get` or `pub fn put`" from #683 before stage 3 opens.

Applied to the tree it renames away `ManifestView::get`, `Database::get`, `Reader::get`, `Metadata::get` and `ForkTable::get`, which is the map vocabulary the seam deliberately landed and `AGENTS.md` now mandates.
The same reasoning closed #646.

## Deferred

These are real gaps, and none is on the critical path.

- Erasure coding, both the write side and the recovery getter.
  This is the one large item.
- Dispersed replica production and the racing getter.
- Access control trie.
- PSS trojan packaging and target mining.
  The cryptographic envelope is about 60 percent done.
- Epoch feeds.
  The sequential path is complete.
- Cross-format traversal, the legacy v1 feed payload, batch time-to-live estimation, per-chunk stamp reuse and a keystore.
