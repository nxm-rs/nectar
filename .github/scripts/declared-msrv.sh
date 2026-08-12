#!/usr/bin/env bash
# Prints the workspace's declared minimum supported Rust version.
#
# Only `[workspace.package]` counts; members inherit it. An absent field is an
# error, not an empty string, which a caller would resolve to the pinned channel.
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
