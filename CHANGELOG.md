# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0](https://github.com/nxm-rs/nectar/releases/tag/v0.2.0) - 2026-06-15

### Bug Fixes

- [time] Unify wall-clock on web-time so primitives run on wasm32 ([#52](https://github.com/nxm-rs/nectar/issues/52))

### Documentation

- [postage-usage] Tighten rustdoc and fix broken intra-doc links
- [postage-usage] Pin a multi-leaf golden vector for large batches
- [postage-usage] Worked example of the record structure, byte-minimal width

### Features

- [primitives] Default-off wasm-threads plus a parallel feature ([#80](https://github.com/nxm-rs/nectar/issues/80))
- [postage-usage] BatchStamper open/stamp/flush client facade ([#78](https://github.com/nxm-rs/nectar/issues/78))
- [postage-usage] Cross-machine example, error taxonomy, caller docs ([#76](https://github.com/nxm-rs/nectar/issues/76))
- [postage-usage] Seal-timestamp monotonicity, dirty/evict signals, steady-state no-clone ([#75](https://github.com/nxm-rs/nectar/issues/75))
- [postage-usage] Gate persist behind a published-sequence floor ([#74](https://github.com/nxm-rs/nectar/issues/74))
- [postage-usage] Add from_batch constructors and Mutability enum ([#73](https://github.com/nxm-rs/nectar/issues/73))
- [postage-issuer] Map DepthIncrease events to issuer dilution ([#72](https://github.com/nxm-rs/nectar/issues/72))
- [postage-issuer] Make MemoryIssuer fill-only and refuse mutable batches ([#69](https://github.com/nxm-rs/nectar/issues/69))
- [postage] Add std-gated generic SnapshotStore trait for warm-path batch snapshot caching ([#67](https://github.com/nxm-rs/nectar/issues/67))
- [postage-usage] Mutable batches and a shared single-table issuance path
- [postage-usage] Expose reserved stamp indices for slot-reuse tooling
- [postage-usage] Self-hosted batch utilization snapshots

### Miscellaneous Tasks

- [wasm] Build the default wasm artifacts single-threaded ([#81](https://github.com/nxm-rs/nectar/issues/81))

### Refactor

- [postage-issuer] Unify per-bucket counter logic behind a shared CounterTable ([#71](https://github.com/nxm-rs/nectar/issues/71))
- [postage-usage] Make SnapshotIssuer the sole stamp-issuance path ([#68](https://github.com/nxm-rs/nectar/issues/68))

## [0.1.1](https://github.com/nxm-rs/nectar/releases/tag/v0.1.1) - 2026-06-10

### Bug Fixes

- [release] Correct git-cliff invocation and tag message template
- [docs] Repair docs.rs builds and broken intra-doc links

## [0.1.0](https://github.com/nxm-rs/nectar/releases/tag/v0.1.0) - 2026-06-09

### Bug Fixes

- [wasm-demo] Fix Pages build (threaded wasm __heap_base) ([#50](https://github.com/nxm-rs/nectar/issues/50))
- [wasm-demo] Make the Pages WASM demo build and run ([#48](https://github.com/nxm-rs/nectar/issues/48))
- [mantaray] Tolerate bee's `ref_size = 0` wire form on decode ([#35](https://github.com/nxm-rs/nectar/issues/35))
- [primitives] Fix proptest Arbitrary impl and WASM demo CI ([#26](https://github.com/nxm-rs/nectar/issues/26))
- [swarms] Serialize testnet as "testnet" instead of "sepolia" ([#22](https://github.com/nxm-rs/nectar/issues/22))
- [primitives] Wasm-demo import paths ([#5](https://github.com/nxm-rs/nectar/issues/5))
- [cicd] Correct base url ([#4](https://github.com/nxm-rs/nectar/issues/4))
- [cicd] Root dir for workflow ([#3](https://github.com/nxm-rs/nectar/issues/3))

### Dependencies

- [deps] Make workspace RUSTSEC advisory-clean ([#46](https://github.com/nxm-rs/nectar/issues/46))
- [deps] Ignore RUSTSEC-2026-0173 (proc-macro-error2 unmaintained) ([#45](https://github.com/nxm-rs/nectar/issues/45))
- [deps] Bump alloy, digest, rand, strum to latest ([#41](https://github.com/nxm-rs/nectar/issues/41))
- Bump alloy deps ([#11](https://github.com/nxm-rs/nectar/issues/11))

### Documentation

- Brand README in Nexum visual style + banner SVG ([#33](https://github.com/nxm-rs/nectar/issues/33))
- Remove redundant style guide section
- Update to Oxford English style
- Update README and add CLA/security workflows

### Features

- [primitives] Typed Swarm spec primitives + signing layout ([#40](https://github.com/nxm-rs/nectar/issues/40))
- [primitives,mantaray] Encryption, entry refs, mantaray manifest, and store cleanup ([#29](https://github.com/nxm-rs/nectar/issues/29))
- [primitives] Add file splitting, joining, and parallel IO ([#25](https://github.com/nxm-rs/nectar/issues/25))
- [primitives] Make chunk body size a compile-time const generic ([#17](https://github.com/nxm-rs/nectar/issues/17))
- [primitives,contracts] Add chunk type system and contract bindings ([#16](https://github.com/nxm-rs/nectar/issues/16))
- Replace devcontainer with nix flake ([#14](https://github.com/nxm-rs/nectar/issues/14))
- Serde ([#13](https://github.com/nxm-rs/nectar/issues/13))
- [primitives] Chunks ([#8](https://github.com/nxm-rs/nectar/issues/8))
- [primitives] Address operations ([#7](https://github.com/nxm-rs/nectar/issues/7))
- Primitives ([#2](https://github.com/nxm-rs/nectar/issues/2))
- [devcontainers] Init commit ([#1](https://github.com/nxm-rs/nectar/issues/1))

### Miscellaneous Tasks

- Remove codecov integration ([#15](https://github.com/nxm-rs/nectar/issues/15))
- Cargo lock ([#12](https://github.com/nxm-rs/nectar/issues/12))
- Agpl ([#10](https://github.com/nxm-rs/nectar/issues/10))
- Agpl license ([#9](https://github.com/nxm-rs/nectar/issues/9))

### Other

- Pin GitHub Actions to commit SHAs ([#34](https://github.com/nxm-rs/nectar/issues/34))
- Initial commit

### Performance

- [bmt] Optimize hasher with zero-tree detection and parallel threshold ([#18](https://github.com/nxm-rs/nectar/issues/18))
- [primitives] Random wasm benches ([#6](https://github.com/nxm-rs/nectar/issues/6))

### Styling

- Raise MSRV to 1.92 and reconcile rand 0.10 usage ([#49](https://github.com/nxm-rs/nectar/issues/49))

<!-- generated by git-cliff -->
