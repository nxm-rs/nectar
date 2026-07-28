#!/usr/bin/env bash
# Reinvention gate: fails if a deleted concurrency primitive creeps back.
#
# The burn-down replaced hand-rolled executors, completion cells, and boxed
# future aliases with ecosystem crates. Each shape below is now sanctioned in
# exactly one home; a copy anywhere else is a reinvention and fails the gate.
# clippy.toml gates the block_on entry points in parallel; this covers the
# structural shapes clippy cannot name.
#
# Sanctioned homes:
#   nectar-testing        the sole block_on entry (`run`) plus its Drive waker
#   nectar-tasks wake.rs  the shared thread-unpark waker (`unpark_current`)
#   nectar-tasks handoff  the pool-to-poll blocking bridge
#   postage-issuer pump   the Stamped sign-admission pump
#   nectar-marker         the sole MaybeSend/MaybeSync cfg dance

set -euo pipefail

cd "$(dirname "$0")/.."

fail=0

# deny <label> <ere> [allow-path-prefix ...]
# Reports every `crates/**/*.rs` line matching <ere> outside the allowed paths.
deny() {
    local label=$1 pattern=$2
    shift 2
    local hits
    hits=$(grep -rEn --include='*.rs' -- "$pattern" crates/ || true)
    local allow
    for allow in "$@"; do
        hits=$(printf '%s\n' "$hits" | grep -vE "^${allow}" || true)
    done
    hits=$(printf '%s\n' "$hits" | grep -v '^$' || true)
    if [ -n "$hits" ]; then
        printf 'reinvention gate: %s\n' "$label" >&2
        printf '%s\n' "$hits" | sed 's/^/  /' >&2
        fail=1
    fi
}

# A boxed-future alias hand-rolled instead of re-exported from futures-core.
# Re-exports use `pub use`; a `type BoxFuture = ...` is always a reinvention.
deny 'hand-rolled BoxFuture alias (re-export futures_core::future::BoxFuture)' \
    'type[[:space:]]+(Local)?BoxFuture[[:space:]]*[<=]'

# The Send/Sync cfg dance re-declared instead of consumed from nectar-marker.
# The marker traits live in exactly one home; a fresh declaration elsewhere is
# a copy. Re-exports use `pub use` and do not match `trait`.
deny 'copied MaybeSend/MaybeSync marker (consume from nectar_marker)' \
    'trait[[:space:]]+Maybe(Send|Sync)' \
    'crates/marker/src/lib.rs'

# A completion cell: a waker parked inside a mutex, woken on result. Use
# futures_channel::oneshot.
deny 'Mutex-guarded waker cell (use futures_channel::oneshot)' \
    'Mutex[[:space:]]*<[^>]*Waker'

# The same cell by its waker slot; the pump keeps one cooperative slot. Only
# the pump sink is sanctioned; test modules must not grow one.
deny 'hand-rolled completion-cell waker slot (use futures_channel::oneshot)' \
    '[A-Za-z_]*waker[[:space:]]*:[[:space:]]*Option[[:space:]]*<[[:space:]]*Waker' \
    'crates/postage-issuer/src/pipeline/stamp_sink.rs'

# The thread-unpark waker copied out of nectar-tasks. Sanctioned in exactly one
# home; a copy anywhere else, test modules included, is a reinvention.
deny 'copied Unpark waker (use nectar_tasks::unpark_current)' \
    'struct[[:space:]]+Unpark[[:space:]]*\([[:space:]]*Thread' \
    'crates/tasks/src/wake.rs'

# Any ad-hoc waker/executor: a fresh Wake impl outside the sanctioned homes.
deny 'ad-hoc Wake impl (route through nectar_testing/nectar_tasks)' \
    'impl[[:space:]]+Wake[[:space:]]+for[[:space:]]' \
    'crates/testing/src/lib.rs' \
    'crates/tasks/src/wake.rs' \
    'crates/postage-issuer/src/pipeline/'

# A hand-rolled oneshot by name; the sanctioned oneshot is futures_channel's.
deny 'hand-rolled oneshot type (use futures_channel::oneshot)' \
    'struct[[:space:]]+[A-Za-z_]*[Oo]ne[Ss]hot'

# A park-loop executor: poll-then-park on the calling thread. Bridges belong in
# nectar-tasks; futures run through nectar_testing::run. The pump admission loop
# and the duplicate-share test driver each park against unpark_current; every
# other file, test modules included, is a reinvention.
deny 'thread::park executor loop (use nectar_testing::run or nectar_tasks)' \
    'thread::park(_timeout)?[[:space:]]*\(' \
    'crates/tasks/src/handoff.rs' \
    'crates/postage-issuer/src/pipeline/mod.rs' \
    'crates/postage-issuer/src/pipeline/stamped_put.rs'

if [ "$fail" -ne 0 ]; then
    printf '\nreinvention gate failed: see matches above.\n' >&2
    exit 1
fi

printf 'reinvention gate: clean.\n'
