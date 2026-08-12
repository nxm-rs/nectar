#!/usr/bin/env bash
# Prints the minimum supported Rust version the workspace declares.
#
# Only `[workspace.package]` counts: every member inherits the field through
# `rust-version.workspace = true`, so the root manifest is the whole declaration.
# An absent field is an error, because a caller that took it as empty would fall
# back to the pinned channel and report green for an untested declaration.
set -eu

manifest="${1:-Cargo.toml}"

msrv="$(awk '
    /^\[workspace\.package\]/ { in_section = 1; next }
    /^\[/                     { in_section = 0 }
    in_section && /^rust-version[ \t]*=/ {
        if (match($0, /"[^"]+"/)) {
            print substr($0, RSTART + 1, RLENGTH - 2)
            exit
        }
    }' "$manifest")"

if [ -z "$msrv" ]; then
    echo "no rust-version in [workspace.package] of $manifest" >&2
    exit 1
fi

echo "$msrv"
