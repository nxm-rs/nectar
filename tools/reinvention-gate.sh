#!/usr/bin/env bash
# Reinvention gate: fails if a burned-down concurrency shape reappears without
# its in-source justification.
#
# The burn-down replaced hand-rolled executors, completion cells, boxed-future
# aliases, and ad-hoc put windows with ecosystem crates and nectar-governor.
# Each shape is sanctioned in exactly one home. A sanctioned site carries the
# justification in source: a "// reinvention: <reason>" comment on the line
# above it, in the shape of the tree's #[allow(..., reason = "...")]
# attributes. This script fails only on an unannotated occurrence, so a
# rewire annotates its own line inside the same diff and this file never
# changes. clippy.toml gates the block_on entry points in parallel; this
# covers the structural shapes clippy cannot name.
#
# Every .rs file under crates/ is scanned, test modules included: a test site
# annotates itself like any other.
#
# The ban list is re-specified where the old shape missed:
#   FuturesUnordered itself is banned nowhere. A walker owns its own set, and
#   the governor states it deliberately does not re-export the type. What is
#   banned is the put-window drain vocabulary (settle_one/sweep) outside
#   nectar_governor::PutSink. A write window that settles unordered over a
#   bare Admission records itself as sanctioned with the same comment.
#   A single waker: Option<Waker> slot on a &mut self poll API is the parking
#   idiom, not a reinvention. What is banned is the guarded waker cell.
#   A rayon submit whose reply arrives on a oneshot is the pair the handoff
#   owns; a bare pool adapter is left alone.

set -euo pipefail

cd "$(dirname "$0")/.."

readonly ANNOTATION='^[[:space:]]*//[[:space:]]*reinvention: .+'
fail=0

# line_above <file> <line>
# Prints the physical line directly above <line>; an empty result means the
# occurrence starts the file and is therefore unannotated.
line_above() {
    sed -n "$(( $2 - 1 ))p" "$1"
}

# shape <label> <ere>
# Reports every crates/**/*.rs line matching <ere> that is not annotated on
# the line above.
shape() {
    local label=$1 pattern=$2
    local hits hit file line above bad=''
    hits=$(grep -rEn --include='*.rs' -- "$pattern" crates/ || true)
    [ -n "$hits" ] || return 0
    while IFS= read -r hit; do
        file=${hit%%:*}
        line=${hit#*:}
        line=${line%%:*}
        above=$(line_above "$file" "$line")
        [[ $above =~ $ANNOTATION ]] || bad+="$hit"$'\n'
    done <<<"$hits"
    if [ -n "$bad" ]; then
        printf 'reinvention gate: %s\n' "$label" >&2
        printf '%s' "$bad" | sed 's/^/  /' >&2
        fail=1
    fi
}

# pool_submit <label>
# Reports every rayon::spawn line in a file that also opens a
# futures_channel oneshot: the submit-and-reply pairing the handoff owns.
# Files that spawn without a oneshot reply are not this shape.
pool_submit() {
    local label=$1
    local files file hits hit line above bad=''
    files=$(grep -rlE --include='*.rs' -- 'rayon::spawn' crates/ || true)
    [ -n "$files" ] || return 0
    while IFS= read -r file; do
        grep -qE 'oneshot::channel' "$file" || continue
        hits=$(grep -nE 'rayon::spawn[[:space:]]*\(' "$file" || true)
        while IFS= read -r hit; do
            [ -n "$hit" ] || continue
            line=${hit%%:*}
            above=$(line_above "$file" "$line")
            [[ $above =~ $ANNOTATION ]] || bad+="$file:$hit"$'\n'
        done <<<"$hits"
    done <<<"$files"
    if [ -n "$bad" ]; then
        printf 'reinvention gate: %s\n' "$label" >&2
        printf '%s' "$bad" | sed 's/^/  /' >&2
        fail=1
    fi
}

# A boxed-future alias declared instead of consumed from nectar-tasks. A
# re-export uses `pub use`; a `type (Local)?BoxFuture = ...` declaration is
# always the shape.
shape 'hand-rolled BoxFuture alias (consume nectar_tasks::BoxFuture)' \
    'type[[:space:]]+(Local)?BoxFuture[[:space:]]*[<=]'

# The Send/Sync cfg dance re-declared instead of consumed from nectar-marker.
shape 'copied MaybeSend/MaybeSync marker (consume nectar_marker)' \
    'trait[[:space:]]+Maybe(Send|Sync)'

# A waker parked under a mutex guard: the completion cell the burn-down
# replaced with futures_channel::oneshot.
shape 'Mutex-guarded waker cell (use futures_channel::oneshot)' \
    'Mutex[[:space:]]*<[^>]*Waker'

# The thread-unpark waker copied out of nectar-tasks.
shape 'copied Unpark waker (use nectar_tasks::unpark_current)' \
    'struct[[:space:]]+Unpark[[:space:]]*\([[:space:]]*Thread'

# A fresh Wake impl outside its sanctioned home (the test drivers and the
# pump each record themselves).
shape 'ad-hoc Wake impl (route through nectar_testing or nectar_tasks)' \
    'impl[[:space:]]+Wake[[:space:]]+for[[:space:]]'

# A hand-rolled oneshot by name; the sanctioned oneshot is futures_channel's.
shape 'hand-rolled oneshot type (use futures_channel::oneshot)' \
    'struct[[:space:]]+[A-Za-z_]*[Oo]ne[Ss]hot'

# A park-loop executor or pump admission loop: poll-then-park on the calling
# thread.
shape 'thread::park loop (use nectar_testing::run or nectar_tasks)' \
    'thread::park(_timeout)?[[:space:]]*\('

# The bounded put window's drain vocabulary; nectar_governor::PutSink is its
# sole home.
shape 'hand-rolled put window (use nectar_governor::PutSink)' \
    'fn[[:space:]]+(settle_one|sweep)([[:space:]]*\(|[[:space:]]*<)'

# The pool submit whose reply arrives on a oneshot.
pool_submit 'hand-rolled pool submit (use nectar_tasks::submit / submit_on)'

# A panic boundary outside the sanctioned ones (the sign job, the pool
# worker, the seed-replay harness each record themselves).
shape 'catch_unwind panic boundary (use the sanctioned homes)' \
    'catch_unwind[[:space:]]*\('

if [ "$fail" -ne 0 ]; then
    printf '\nreinvention gate failed: the occurrences above carry no "// reinvention:" justification on the line above.\n' >&2
    exit 1
fi

printf 'reinvention gate: clean.\n'
