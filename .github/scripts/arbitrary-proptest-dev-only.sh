#!/usr/bin/env bash
# Fails when `arbitrary` or `proptest` reaches a workspace crate's default
# build.
#
# Both are property-test tooling, no production surface. `proptest` is a
# dev-only harness and must never be a normal or build dependency. `arbitrary`
# is the one exception: crates expose an optional `arbitrary` feature so a
# consumer can derive `Arbitrary` for that crate's types, so an `optional`
# `arbitrary` normal dependency is allowed. Only an unconditional (non-optional)
# `arbitrary` normal or build dependency is compiled into a default build and is
# rejected.
set -eu

offenders=$(
    cargo metadata --format-version 1 --no-deps \
        | jq -r --arg arb "arbitrary" --arg pt "proptest" '
            .packages[]
            | .name as $p
            | .dependencies[]
            | select(.kind == null or .kind == "build")
            | select(.name == $arb or .name == $pt)
            | if .name == $pt
              then "\($p): \(.name) is a \(.kind // "normal") dependency; it must be dev-only"
              elif .optional == true
              then empty
              else "\($p): \(.name) is an unconditional \(.kind // "normal") dependency; only an optional dependency is allowed"
              end
        '
)

if [ -n "$offenders" ]; then
    echo "$offenders" | while IFS= read -r line; do
        echo "::error::$line"
    done
    echo "::error::arbitrary and proptest must not reach a default build; proptest is dev-only and arbitrary must stay optional"
    exit 1
fi
