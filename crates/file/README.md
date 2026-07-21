# nectar-file

Streaming file pipeline for Ethereum Swarm: bounded chunk-tree reads and writes over a chunk store.

The crate currently carries the pipeline's foundations:

- **Geometry** (`geometry`): every fan-out fact of a chunk tree derives from its body size and reference mode (plain 32-byte references, encrypted 64-byte references). Each concrete profile is pinned at compile time by `assert_tree_geometry!`, whose checks run in `u128` so coverage of the full `u64` length range is provable without overflow.
- **Admission budgets** (`config`): `Window` (bounded fetch window), `PutWindow` (bounded put window) and the derived `BranchBudget` that keeps tree descent live at any window size.
- **Walk engine** (`walk`): the one poll-native descent every read mode drains. Two-budget admission with a head-liveness reservation, range pruning, address-equality checks on every completion, typed terminal errors and no retries; ordered and completion-order drains over the same state machine.

The core is `#![no_std]`; the write engine stacks on the same invariants. Feature flags: `std` (default), `tokio`, `rayon`, `encryption`, `unsync`.
