#!/usr/bin/env bash
# Pins `declared-msrv.sh` to the declaration, not to whatever this repository
# happens to declare today. It does not police how the workflow uses the script;
# the rustc-version guard in the msrv job does that.
set -eu

here="$(cd "$(dirname "$0")" && pwd)"
subject="$here/declared-msrv.sh"
root="$(cd "$here/../.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

failures=0

pass() { printf 'ok   %s\n' "$1"; }
fail() { printf 'FAIL %s\n' "$1"; failures=$((failures + 1)); }

# Asserts the printed version for a manifest built from the repository's own,
# with `edit` applied to it as a sed programme.
expect_version() {
    name="$1" want="$2" edit="$3"
    sed "$edit" "$root/Cargo.toml" > "$work/Cargo.toml"
    if got="$("$subject" "$work/Cargo.toml")" && [ "$got" = "$want" ]; then
        pass "$name"
    else
        fail "$name (want $want, got ${got:-error})"
    fi
}

expect_error() {
    name="$1" edit="$2"
    sed "$edit" "$root/Cargo.toml" > "$work/Cargo.toml"
    if "$subject" "$work/Cargo.toml" > /dev/null 2>&1; then
        fail "$name"
    else
        pass "$name"
    fi
}

# The repository as it stands.
if got="$("$subject" "$root/Cargo.toml")" && [ -n "$got" ]; then
    pass "reads the workspace manifest ($got)"
else
    fail "reads the workspace manifest"
fi

# The anti-hardcode case: a raised declaration must move the toolchain with it.
expect_version "follows a raised declaration" "1.99" 's/^rust-version = .*/rust-version = "1.99"/'

# Spacing is not load-bearing.
expect_version "tolerates unspaced toml" "1.99" 's/^rust-version = .*/rust-version="1.99"/'

# An absent declaration is an error, not an empty string that would leave the
# toolchain action on the pinned channel.
expect_error "rejects an absent declaration" '/^rust-version/d'

# A member's own declaration is not the workspace's, so it must not stand in.
{
    sed '/^rust-version/d' "$root/Cargo.toml"
    printf '\n[package]\nrust-version = "1.60"\n'
} > "$work/member.toml"
if "$subject" "$work/member.toml" > /dev/null 2>&1; then
    fail "reads only [workspace.package]"
else
    pass "reads only [workspace.package]"
fi

# The no-argument path the job uses, run in a tree whose pin has moved ahead of
# the declaration: the answer must stay with the declaration.
mkdir -p "$work/drift"
cp "$root/Cargo.toml" "$work/drift/Cargo.toml"
printf '[toolchain]\nchannel = "1.99"\n' > "$work/drift/rust-toolchain.toml"
declared="$("$subject" "$root/Cargo.toml")"
if got="$(cd "$work/drift" && "$subject")" && [ "$got" = "$declared" ]; then
    pass "ignores a raised rust-toolchain.toml pin"
else
    fail "ignores a raised rust-toolchain.toml pin (want $declared, got ${got:-error})"
fi

printf '\n%s failure(s)\n' "$failures"
[ "$failures" -eq 0 ]
