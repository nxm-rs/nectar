# Fuzzing nectar

[cargo-fuzz](https://rust-fuzz.github.io/book/cargo-fuzz.html) (libFuzzer)
harness for nectar's wire-format decoders — the code that parses untrusted
bytes off the Swarm network. This directory is an **independent cargo
workspace** (it carries its own `[workspace]` table), so the stable toolchain
building the parent workspace never touches the nightly-only fuzz crate.

## Quickstart

```sh
nix develop .#fuzz        # nightly cargo + cargo-fuzz + clang on PATH
cargo fuzz list           # run from the repo root; cargo-fuzz finds fuzz/

# Run a target, growing fuzz/corpus/<target> and merging the committed seeds:
cargo fuzz run stamp_decode fuzz/corpus/stamp_decode fuzz/seeds/stamp_decode

# Housekeeping:
cargo fuzz cmin stamp_decode                                # minimise corpus
cargo fuzz tmin stamp_decode fuzz/artifacts/stamp_decode/…  # minimise a crash
cargo fuzz coverage stamp_decode                            # llvm-cov profdata
```

New corpus entries are written to the **first** directory passed to
`cargo fuzz run`; further directories (the seeds) are read-only inputs.
`cargo fuzz coverage` finds `llvm-profdata`/`llvm-cov` through the rustc
sysroot — the fuzz shell's nightly ships the `llvm-tools` component.

Release-profile `overflow-checks` and `debug-assertions` are enabled in
`Cargo.toml`, so arithmetic overflow and `debug_assert!` violations are fuzz
oracles, not silent wraps.

## Target catalogue

Decode targets take raw `&[u8]` — the decoder itself is the structure
recoverer — and a returned `Err` is success; the invariant is *no panic, no
OOM, no hang*:

| Target | Entry point | Invariant |
|---|---|---|
| `mantaray_node_decode` | `hazmat::decode` over raw bytes | manifest decoding never panics |
| `chunk_decode` | `AnyChunk::from_wire_bytes` + direct CAC/SOC `TryFrom` | chunk decoding, BMT address forcing, and SOC owner recovery never panic |
| `stamp_decode` | `Stamp::try_from_slice` (+ `recover_signer` over a stamp‖address split) | stamp decoding and EIP-191 signer recovery never panic |
| `usage_snapshot_decode` | `RootInfo::parse` | SBU1 root parsing (geometry/packed-length arithmetic) never panics |

Round-trip targets take a structured value via valid-by-construction
`arbitrary::Arbitrary` impls (behind each crate's `arbitrary` feature), so the
invariant is stronger — encode must decode back to an equal value, and where
the encoding is canonical, re-encoding must be byte-identical. The one
exception is `mantaray_record_roundtrip`, which takes raw `&[u8]` and is seeded
from the `mantaray_node_decode` corpus: the encoder emits v0.2 only, so it
decodes a real wire image (either version, either ref width) to recover its
records, then asserts the encode/decode pair reaches a byte- and
structure-canonical fixed point:

| Target | Invariant |
|---|---|
| `mantaray_node_roundtrip` | `decode(encode(node)) == node`, canonical re-encode |
| `mantaray_record_roundtrip` | decoded manifests re-encode to a byte- and structure-canonical fixed point, both ref widths, both wire versions |
| `chunk_roundtrip` | decoded CAC/SOC preserves address, span, data (+ signature/owner for SOC) |
| `stamp_roundtrip` | `from_bytes(to_bytes(stamp)) == stamp`, canonical re-encode |
| `usage_snapshot_roundtrip` | `revalidate → plan_persist → parse + assemble` reproduces the snapshot |

Every decode target has a stable-gated **seed replay test in the library
crate** that pushes the committed seed bytes through the exact same decode
function, so plain `cargo nextest run` proves the seeds stay panic-free
without nightly or libFuzzer:

- `seed_replay_mantaray_node_decode` — `crates/mantaray/src/codec.rs`
- `seed_replay_chunk_decode` — `crates/primitives/src/chunk/chunk_type_set.rs`
- `seed_replay_stamp_decode` — `crates/postage/src/stamp.rs`
- `seed_replay_usage_snapshot_decode` — `crates/postage-usage/src/codec.rs`

The round-trip invariants are pinned on stable by the `arbitrary_*` tests next
to the replay tests (run with `--features arbitrary`) and the chunk proptests
in `crates/primitives/src/chunk/{content,single_owner}.rs`. The corpus-seeded
`mantaray_record_roundtrip` is additionally pinned by
`seed_replay_mantaray_record_roundtrip` in `crates/mantaray/src/codec.rs`,
which replays its seeds through the same fixed-point round trip.

## Corpus & seed policy

- `fuzz/seeds/<target>/` is **committed**: a small curated set per decode
  target — a few valid encodings, interesting invalid/edge encodings, and
  minimized crash inputs from fixed bugs (e.g.
  `mantaray_node_decode/crash-v01-header-only-64b.bin`, the input behind the
  bound-check fix in `crates/mantaray/src/codec.rs`). Name seeds
  `valid-*`/`invalid-*`/`edge-*`/`crash-*` with a size hint.
- `fuzz/corpus/`, `fuzz/artifacts/`, `fuzz/coverage/` are **gitignored**; the
  corpus lives in the CI cache and on developer machines.
- When a fuzzer finds a crash: `cargo fuzz tmin` it, commit the minimized
  bytes as a `crash-*` seed, extend the crate's `seed_replay_*` test, fix the
  bug (fix and seed in the same commit), then `cargo fuzz cmin`.
- `fuzz/Cargo.lock` is committed so CI builds are reproducible.

## CI cadence

`.github/workflows/fuzz.yml` (existing `unit.yml`/`audit.yml` are untouched):

- **Every PR / push to main** — `fuzz build` compiles all targets (the
  harness can't rot), and `fuzz smoke` runs each target for 60 s
  (`-rss_limit_mb=2048`) on a per-target cached corpus with the committed
  seeds merged in. Crash artifacts are uploaded on failure.
- **Nightly cron** — 10 minutes per target on the same corpus caches,
  followed by `cargo fuzz cmin` so the caches don't grow without bound.

## NixOS gotchas

- Use `nix develop .#fuzz`. `libfuzzer-sys` compiles the libFuzzer C++
  runtime through the `cc` crate, which is why the shell carries `clang`;
  outside the shell the build fails at that step.
- Don't force `lld`/`mold` via `RUSTFLAGS` for fuzz builds — the sanitizer
  runtimes are linked by rustc's defaults and alternative-linker flags are a
  recurring source of broken ASan link steps. Plain defaults work.
- If an ASan-instrumented run dies immediately with an endlessly repeating
  `DEADLYSIGNAL` banner, the kernel's ASLR entropy is too high for ASan's
  shadow mapping (Linux ≥ 6.5 defaults): `sudo sysctl vm.mmap_rnd_bits=28`.
- `cargo fuzz coverage` needs the toolchain's llvm-tools; the fuzz shell's
  nightly includes the component, so no extra install is needed.

