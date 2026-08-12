#!/usr/bin/env bash
# Fails when U+2014 appears in a scanned file.
#
# The pattern lives here rather than in the workflow so that `*.yml` can be
# scanned: an inline pattern would match its own workflow file forever. Never
# add `*.sh` to the glob list below for the same reason.
set -eu

globs=('*.rs' '*.md' '*.toml' '*.yml' '*.yaml' '*.svg')

# git grep exits 1 for no match and above 1 for a scan that broke.
git grep -nIF -e '—' -- "${globs[@]}" && found=0 || found=$?

case "$found" in
    0)
        echo "::error::U+2014 found; house style bans it"
        exit 1
        ;;
    1) ;;
    *)
        echo "::error::the U+2014 scan failed with status $found"
        exit 1
        ;;
esac
