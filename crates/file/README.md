# nectar-file

Streaming file pipeline for Ethereum Swarm: bounded chunk-tree reads and writes over a chunk store.

`File` is the whole surface. Bind a store to a `Policy`, then:

- `File::load(root, &mut sink)` drains the chunk tree at `root` into a positional `DataSink`. The sink is positional on purpose: frames land at their offsets in completion order, which is what makes unordered retrieval possible.
- `File::save(src)` drains a `Source` into a fresh tree and returns its root. Adapters cover the three shapes the pipeline meets: an in-memory slice, a positional `ReadAt` target through `ReadAtSource`, and an async byte stream through `AsyncReadSource`.
- `File::open(root)` hands back a `Reader` for an ordered, seekable read, and `File::collect(root, max)` assembles a bounded in-memory copy.

Supporting cast:

- **Geometry** (`geometry`): every fan-out fact of a chunk tree derives from its body size and reference mode (plain 32-byte references, encrypted 64-byte references). Each concrete profile is pinned at compile time by `assert_tree_geometry!`, whose checks run in `u128` so coverage of the full `u64` length range is provable without overflow.
- **Admission budgets** (`config`): `Window` (bounded fetch window), `PutWindow` (bounded put window), `HashWindow` (bounded pool leaf seals) and the derived `BranchBudget` that keeps tree descent live at any window size. A `Policy` carries them.
- **Walk and split engines**: the one poll-native descent every read drains and the one bounded ascent every write feeds. Both are crate-private; `File` is the only way in.

The core is `#![no_std]`. Feature flags: `std` (default), `tokio`, `rayon`, `encryption`, `unsync`.
