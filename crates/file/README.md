# nectar-file

Streaming file pipeline for Ethereum Swarm: bounded chunk-tree reads and writes over a chunk store.

The crate currently carries the pipeline's foundations:

- **Geometry** (`geometry`): every fan-out fact of a chunk tree derives from its body size and reference mode (plain 32-byte references, encrypted 64-byte references). Each concrete profile is pinned at compile time by `assert_tree_geometry!`, whose checks run in `u128` so coverage of the full `u64` length range is provable without overflow.
- **Admission budgets** (`config`): `Window` (bounded fetch window), `PutWindow` (bounded put window) and the derived `BranchBudget` that keeps tree descent live at any window size.

The core is `#![no_std]`; the read and write engines stack on top of these invariants. Feature flags: `std` (default), `tokio`, `rayon`, `encryption`.
