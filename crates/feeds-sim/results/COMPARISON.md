# Finder lookup cost versus the reference client

The harness measures nectar's `latest()` and a faithful port of the reference client's concurrent finder over the same counting presence store, so every figure below is a deterministic work count from `feeds-perf-results.json`, not wall time. The nectar column uses window width 8 to match the reference finder's fixed eight-way lookahead; the reference series has no width sweep because its concurrency is not tunable. The lengths cover every prefill the reference client's own lookup benchmark exercises (1, 100, 1000, 5000) plus longer feeds its benchmark does not reach.

## Measured comparison, nectar width 8 versus the reference port

| n | nectar rounds | nectar probes | reference rounds | reference probes | verdict |
|---|---|---|---|---|---|
| 1 | 1 | 8 | 2 | 9 | better |
| 10 | 2 | 15 | 4 | 13 | better on rounds, worse on probes |
| 100 | 4 | 31 | 6 | 23 | better on rounds, worse on probes |
| 1000 | 5 | 39 | 10 | 56 | better |
| 5000 | 6 | 43 | 26 | 178 | better |
| 10000 | 6 | 47 | 45 | 334 | better |
| 100000 | 9 | 65 | 397 | 3154 | better |
| 1000000 | 9 | 67 | 3926 | 31389 | better |

Rounds are concurrent probe batches, one network round trip each; probes are the lookups issued, speculation included.

## Why

The root cause is that the reference finder's exponential lookahead is bounded: it probes each interval at offsets 2^k - 1 for k = 1..8, so one batch reaches at most 255 slots past its base. Within one 256-slot block that brackets the boundary in about one batch and narrows logarithmically, which is why both finders are within a few rounds of each other up to n = 100. Across blocks the reference base can only advance 255 slots per batch, so its round count is linear in n / 255 (10 rounds at 1000, 3926 at 1000000). nectar's ladder is unbounded, doubling per rung with a tunable concurrency window on top, so it is logarithmic at every scale (5 rounds at 1000, 9 at 1000000), and wider windows buy still fewer rounds (at n = 5000: 6 rounds at width 8, 4 at width 16, 3 at width 64).

Where the reference is better: on small feeds past the first probe (n = 10 and n = 100) its shrinking narrowing batches issue fewer total lookups (13 and 23) than nectar's fixed-width ladder (15 and 31), a bounded speculation cost that nectar pays for its round advantage; the gap is capped by one window per round and inverts from n = 1000 on. The probe metrics also favour the reference port on paper: in the original client each probe is a full chunk retrieval with absence inferred from a timeout, whereas nectar issues cheap presence probes, answers absence explicitly, and pays exactly one certified retrieval for the committed update, so equal probe counts are not equal cost.

At no measured length is nectar worse on rounds.

## Reference client wall-time cross-check

The reference client's own lookup benchmark was run with its own toolchain as a supplementary sanity check (linux/amd64, Ryzen 9 7940HS; prefill p stores p + 1 updates). It is a different metric: wall time over a store that simulates latency (a 30 ms sleep per miss, a random 0..9 ms sleep per hit), so it only corroborates the asymptotic shape, never the checked-in counts.

| prefill | sequential finder ns/op | concurrent finder ns/op |
|---|---|---|
| 1 | 38447136 | 64306432 |
| 100 | 502400210 | 97404134 |
| 1000 | 4611100102 | 145004861 |
| 5000 | 22771829530 | 211065172 |

The shapes match the port: the sequential finder is linear at about 4.5 ms per update (the mean hit latency), and the concurrent finder is affine with a marginal cost of about 16.5 us per update between prefill 1000 and 5000, agreeing with the port's linear climb of one batch per 255 slots at one hit answer (about 4.5 ms / 255 = 17.6 us per update) on top of a roughly constant narrowing tail that pays the absence timeouts.

## Interoperability

nectar and the reference client share identical wire indexing: 8-byte big-endian index, keccak(topic || index) id, keccak(id || owner) address. The comparison is lookup strategy only; there is no interop difference.
