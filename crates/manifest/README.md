# nectar-manifest

The manifest seam for Ethereum Swarm: one `Manifest` trait, one shared operation vocabulary, and an object-safe `DynManifest` wrapper over both manifest formats (the mantaray trie and the `nectar-ldb` key-value website view).
A manifest is a map, so it reads through a root-bound handle: `at(root)` gives a `MapView` with `get`, `contains_key`, `range`, `iter`, `dir` and `load`.
It writes through one seam-owned value: stage a `Batch` with `insert`, `insert_with`, `remove`, `extend` and the two site-document setters, then `apply(base, batch)` folds it in atomically and returns the new root.
The static trait keeps each format's native metadata and reference width, so nothing is erased on the zero-cost path; the erased path unifies metadata behind a well-known-key view.

Part of the [nectar](https://github.com/nxm-rs/nectar) workspace, a collection of low-level Ethereum Swarm primitives in Rust. See the [workspace README](https://github.com/nxm-rs/nectar) for the full crate list and project context.

## Usage

```toml
[dependencies]
nectar-manifest = "0.4"
```

## License

AGPL-3.0-or-later. See [LICENSE](https://github.com/nxm-rs/nectar/blob/main/LICENSE).
