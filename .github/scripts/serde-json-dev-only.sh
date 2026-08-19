#!/usr/bin/env bash
# Fails when `serde_json` is a non-dev dependency of a workspace crate.
#
# JSON is a convenience for fixtures and tests, not something the shipped
# crates should carry. `nectar-mantaray` is the one exception: mantaray
# metadata is JSON on the wire, so the format mandates the dependency.
set -eu

allowed='nectar-mantaray'

offenders=$(
    cargo metadata --format-version 1 --no-deps \
        | jq -r --arg allowed "$allowed" '
            .packages[]
            | .name as $p
            | select($p != $allowed)
            | .dependencies[]
            | select(.name == "serde_json")
            | select(.kind == null or .kind == "build")
            | "\($p): serde_json is a \(.kind // "normal") dependency"
        '
)

if [ -n "$offenders" ]; then
    echo "$offenders" | while IFS= read -r line; do
        echo "::error::$line"
    done
    echo "::error::serde_json must be a dev-dependency only; $allowed is the sole exception"
    exit 1
fi
