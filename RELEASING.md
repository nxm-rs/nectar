# Releasing nectar

This workspace is released locally by a maintainer with [`cargo-release`](https://github.com/crate-ci/cargo-release) and [`git-cliff`](https://github.com/orhun/git-cliff). There is no release CI workflow; cutting a release is a deliberate, signed, local action. The configuration lives in `release.toml` (cargo-release) and `cliff.toml` (git-cliff).

The project is licensed `AGPL-3.0-or-later`. Every publishable crate ships the AGPL text and inherits `license.workspace = true`.

## Versioning

All publishable crates share a single workspace version, defined once in `[workspace.package].version` in the root `Cargo.toml`. They bump together (`shared-version = true`) and are tagged once per release as `vX.Y.Z`. The workspace is at `0.x`, so a `minor` bump is the normal release level and may carry breaking changes until `1.0`.

## Publishable crates and publish order

cargo-release computes the dependency order automatically and publishes leaves before dependents. For reference, the order is:

1. `nectar-marker`
2. `nectar-clock` (depends on `nectar-marker`)
3. `nectar-primitives` (depends on `nectar-marker`)
4. `nectar-contracts`
5. `nectar-swarms`
6. `nectar-postage` (depends on `nectar-primitives`)
7. `nectar-mantaray` (depends on `nectar-primitives`)
8. `nectar-postage-issuer` (depends on `nectar-postage`, `nectar-primitives`)

The `bmt-wasm-demo` example crate is `publish = false` and is skipped.

## Prerequisites

- A crates.io account that is an owner of all eight crates, and a token available to cargo: run `cargo login`, or export `CARGO_REGISTRY_TOKEN`.
- Your hardware signing key (YubiKey) unlocked in the gpg-agent, so cargo-release can create the signed release commit and signed `vX.Y.Z` tag without prompting.
- A clean checkout of `main` with no uncommitted changes, up to date with the remote.
- CI green on the commit you are about to release.
- `cargo-release` and `git-cliff` installed locally. On the Nix dev shell: `nix-shell -p cargo-release git-cliff`.

## Cutting a release

From a clean `main`:

```bash
# Dry run first: shows the version bump, changelog, publish plan, and tag.
cargo release minor

# When it looks right, execute it.
cargo release minor --execute
```

`cargo release minor --execute` will, in order:

1. Bump `[workspace.package].version` to the next minor.
2. Run the pre-release hook (`git-cliff`) to regenerate `CHANGELOG.md` for the new version.
3. Create the signed release commit `chore(release): X.Y.Z`.
4. Create the signed tag `vX.Y.Z`.
5. Publish all eight crates to crates.io in dependency order.
6. Push the release commit and tag to the remote.

Use `patch` instead of `minor` for a patch release, or `cargo release X.Y.Z --execute` to set an exact version.

## Post-release

- Verify each crate is live on crates.io and that docs.rs has built (`https://docs.rs/nectar-primitives`, etc.).
- Create a GitHub release for the `vX.Y.Z` tag. Paste the matching `CHANGELOG.md` section as the body:

  ```bash
  gh release create vX.Y.Z --title vX.Y.Z --notes-file <(git-cliff --config cliff.toml --latest)
  ```

- Bump downstream consumers (for example `nxm-rs/vertex`) to the published version if appropriate.

## Appendix: optional CI release workflow

Releases are local today. If the project later wants CI-driven releases, a workflow can be added once a token with `workflow` scope is available. Two common options:

- [`release-plz`](https://release-plz.dev/) opens a release PR and publishes on merge.
- A cargo-release GitHub Action runs the same local flow on a tag push.

A minimal cargo-release-on-tag workflow, ready to drop into `.github/workflows/release.yml` when scope allows:

```yaml
name: release

on:
  push:
    tags:
      - "v*"

jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0
      - uses: dtolnay/rust-toolchain@stable
      - uses: taiki-e/install-action@v2
        with:
          tool: cargo-release,git-cliff
      - name: Publish to crates.io
        env:
          CARGO_REGISTRY_TOKEN: ${{ secrets.CARGO_REGISTRY_TOKEN }}
        run: cargo release --execute --no-confirm --no-tag --no-push
```

Note that CI cannot produce GPG-signed tags with the maintainer's hardware key, so the signed tag should still be created locally (or by a signing-capable runner) even if publishing moves to CI.
