# SBU1: postage batch usage snapshot

SBU1 is the wire format of a self-hosted postage batch usage snapshot.
A snapshot holds the per-bucket slot counters of one postage batch.
The snapshot is stored inside the batch that it describes, as single-owner chunks stamped by that batch.

This document is normative.
`nectar-postage-usage` is the reference implementation.
`crates/postage-usage/README.md` gives the motivation, the API guidance and the persistence cadence, and states nothing normative.

## 1. Conformance

"Must" states a requirement on a conforming encoder or a conforming decoder.
"Must not" states a prohibition.
"May" states a permitted choice.
A decoder that meets a "must not" rejects the payload.
A decoder must not repair a payload.

The SBU1 bytes are frozen.
Any change to a field, an offset, a width or a validation rule in this document is a new format version, and it takes a new magic.

## 2. Conventions

All integers are unsigned and big-endian.
`d` is the batch depth and `u` is the bucket depth.
A batch has `2^u` collision buckets.
Each bucket holds `capacity = 2^(d - u)` slots.
`w` is the delta width in bits.
`A` is the allocated count, `L` is the leaf count and `E` is the exception count.
`keccak256` is the Ethereum keccak-256 digest.
A payload is at most 4096 bytes, which is the Swarm chunk body size.
`||` is byte concatenation.

## 3. Chunk addressing

A snapshot is one root payload and `L` leaf payloads.
Payload `n` is the root when `n = 0`, and it is leaf `n - 1` when `n >= 1`.

Payload `n` is carried by the single-owner chunk with this id:

```
id(n)      = keccak256("swarm-batch-usage" || batch_id || u16_be(n))
address(n) = keccak256(id(n) || owner)
```

The domain separator is the 17 ASCII bytes `swarm-batch-usage`, with no terminator.
`batch_id` is the 32-byte batch id.
`owner` is the 20-byte address of the batch owner.
The single-owner chunk owner must be the batch owner.

An id depends on the batch id and the payload index alone.
A holder of the batch id and the owner address therefore locates every snapshot chunk without any other state.
An id never changes, so a persist republishes each payload at the address that it already used.

## 4. Counter encoding

Counters are encoded with a patched frame-of-reference scheme.
The scheme has three parts:

- A base, a `u32` that is at most the smallest counter in the table.
- A delta width `w` in the range 0 to 32.
  The delta `count - base` of each bucket is stored as a `w`-bit field.
- An exception list of at most 128 entries.
  An entry is a `(bucket, count)` pair of `u32` values, and it carries the absolute count of one bucket.

An encoder must list a bucket as an exception when its delta does not fit `w` bits.
An encoder must write all one bits into the `w`-bit field of an exception bucket.
A decoder must ignore the `w`-bit field of an exception bucket and must take the count from the exception entry.
An encoder may list a bucket whose delta fits `w` bits, and a decoder must accept that.

At `w = 0` every field is zero bits wide, so every bucket whose count is above the base must be an exception.

A decoder must accept any structurally valid `(base, w, exception list)`.
The encoder policy is therefore free to change without a format change.
The canonical encoder takes `base = min(counts)`.
It then takes the width that minimizes the encoded size, which is the packed bits, plus 8 bytes for each exception, plus 32 bytes for each leaf digest when the table does not fit the root.
It breaks a tie toward the smaller width, and it rejects a width that needs more than 128 exceptions.

## 5. Root payload

The root payload starts with a fixed 66-byte header.

| offset | size | field |
|---|---|---|
| 0 | 4 | magic `"SBU1"` |
| 4 | 32 | batch id |
| 36 | 1 | batch depth `d` |
| 37 | 1 | bucket depth `u` |
| 38 | 1 | flags |
| 39 | 1 | delta width `w` |
| 40 | 8 | sequence |
| 48 | 8 | counter sum |
| 56 | 4 | base |
| 60 | 2 | allocated count `A` |
| 62 | 2 | leaf count `L` |
| 64 | 2 | exception count `E` |

Bit 0 of the flags byte selects the reading of the counter field, as section 8 states.
All other flag bits are reserved, and an encoder must write them as zero.

The sequence is a persist counter.
It must increase on each persist of the same batch.

The counter sum must equal the sum of the decoded counters.
Section 8 states what the value means in each mode.

Three variable sections follow the header, in this order:

1. `E` exception entries, 8 bytes each.
   An entry is the bucket index then the absolute count, both `u32`.
   The entries must be in strictly ascending bucket order.
2. `A` slot entries, 4 bytes each.
   Entry `n` is the within-bucket stamp index that snapshot chunk `n` occupies.
   Entry 0 is the slot of the root itself.
3. The table section.
   When `L = 0` this is the packed delta bitstream of every bucket, held inline in the root.
   When `L > 0` this is `L` digests of 32 bytes, where digest `i` is `keccak256` of leaf payload `i`.

The root is the commit point.
The leaf digests bind the exact leaf bytes, so a reader either rebuilds one consistent snapshot or detects a stale leaf.

## 6. Leaf payloads

A leaf holds a slice of the same packed delta bitstream that the inline form holds.
Deltas are packed most significant bit first, with no gap between fields.

```
B = floor(32768 / w)
L = ceil(2^u / B)
```

`B` is the number of buckets in a full leaf, and 32768 is the payload size in bits.
Leaf `i`, which is chunk `n = i + 1`, covers the buckets `[i * B, min((i + 1) * B, 2^u))`.
The last leaf is short when `2^u` is not a multiple of `B`.
The trailing bits of the last byte of a leaf are padding, and they must be zero.
A leaf length is fully determined by `u` and `w`, so the root holds only the digest.

The inline table follows the same packing over the buckets `[0, 2^u)`, and its trailing bits must be zero as well.

## 7. Decoding

A decoder validates the root before it reads any variable section.
It must reject the payload when any of these fails:

- The magic is not `"SBU1"`.
- `u` is 0, or `u` is above 16, or `u` is below the bucket-depth floor of the network that the decoder decodes for.
- `d` is below `u`, or `d - u` is above 31.
- Any flag bit other than bit 0 is set.
  This keeps an older reader from silently ignoring a later flag.
- `w` is above 32.
- `E` is above 128.
- `A` is 0.
- `L > 0` and `w = 0`.
- `L > 0` and `L` is not `ceil(2^u / B)`.
- `L > 0` and `A <= L`, because the root and every leaf need a slot.
- The payload length is not exactly `66 + 8 * E + 4 * A + T`, where `T` is `32 * L` when `L > 0` and the packed length of `2^u` deltas at `w` bits when `L = 0`.
- That length is above 4096.

The decoder then validates the sections.
It must reject the payload when any of these fails:

- An exception bucket is at or above `2^u`.
- The exception buckets are not in strictly ascending order.
- An exception count is above `capacity`.
- A slot entry is at or above `capacity`.
- An inline padding bit is not zero.

Assembly needs the leaf payloads.
A decoder must reject the snapshot when any of these fails:

- The number of supplied leaves is not `L`.
- A leaf length is not the length that `u` and `w` determine for that leaf.
- A leaf digest is not the digest that the root holds for that leaf.
- A leaf padding bit is not zero.
- A decoded counter is above `capacity`.
- The sum of the decoded counters is not the counter sum in the header.

The decoder overlays the exception counts after it unpacks the deltas.
A counter is therefore in the range `[0, capacity]` in both modes.

## 8. Counter semantics

Bit 0 of the flags byte selects one of two readings of the same counter field.
The two readings share the encoding, the range and the validation.

### 8.1 Immutable batch, bit 0 clear

`count(b)` is a monotone fill watermark.
It is the next unused index of bucket `b`.
Issuance returns `count(b)` and then increments it.
A bucket is full at `capacity`, and issuance there fails rather than overwrites.

The counter sum is the lifetime count of stamps issued.
Dilution raises `capacity` and changes no counter.
An elementwise maximum is a valid join of two divergent tables, because a watermark only rises.

The snapshot draws the slots of its own chunks from the same watermark.
Those slots therefore sit below every later watermark, so fresh issuance cannot collide with them.

### 8.2 Mutable batch, bit 0 set

`count(b)` is a ring cursor in `[0, capacity]`.
It is the next index to write, and it wraps at `capacity`.
A write at the cursor evicts the chunk in that slot, which is the oldest live chunk because writes advance in cursor order.
Position selects the victim, and the newer stamp timestamp is what makes the replacement valid on the wire.

The counter sum has no utilization meaning, because a wrapped bucket is full while its cursor may be small.
The field is a deterministic checksum of the cursor table in this mode.
A decoder still recomputes and verifies it, so it still catches corruption.

The cursor must skip every index in the slot section of the root.
A bucket that holds `r` such slots is therefore a ring of length `capacity - r`.
Without that rule a position-based ring would evict the very chunks that record the batch state, because a snapshot chunk is re-stamped with a fresh timestamp on each persist and so looks like the oldest slot.

An elementwise maximum is not a valid join in this mode, because a cursor falls on wrap.
Divergence between two mutable copies is a conflict, and the sequence field is what surfaces it.

A reader that predates the flag rejects any nonzero flag byte, so a mutable snapshot is never read as an immutable one.

## 9. Self-accounting and termination

A persist stamps the chunks of the snapshot itself.
Each such stamp advances a counter, which can change the chosen width, which can change `L`.
The planner runs this to a fixed point:

1. Allocate a slot for each snapshot chunk that has no slot yet, and fold the advance into the table.
2. Encode.
3. Repeat while the slot count does not exceed the leaf count.

The loop terminates for three reasons.
Allocation is monotone, because a slot is never released.
A slot is reused for every later persist, so a steady-state persist allocates nothing.
`L` has an upper bound of 64, which is `ceil(2^16 / floor(32768 / 32))`, so the number of chunks to allocate is bounded.

`A` must never decrease, even when a later and smaller encoding needs fewer leaves.
A leaf that reappears then reuses its original slot instead of burning a new one.

The worst case cost is 65 slots, which is one root and 64 leaves.
The shallowest batch at `u = 16` holds `2^17` slots, so the worst case is under 0.05 percent of the batch.

## 10. Protocol assumption on the reserve

SBU1 assumes this behaviour of the reserve of the reference client:

> A chunk at the same address and the same stamp index is replaced in place by a version that carries a newer stamp timestamp, whatever the mutability of the batch.

This is an assumption about the reference client, and not a rule that nectar can enforce.
It is the single assumption that the whole cost argument rests on.

The cost argument is this.
A snapshot chunk id never changes, so each persist republishes the same address with the same stamp index.
Under the assumption each snapshot chunk therefore holds exactly one storage slot for the lifetime of the batch.
The slot is assigned on first allocation, recorded in the slot section of the root, and reused by every later persist.

Without the assumption every persist burns a fresh slot, and the claim of one slot for the lifetime of the batch fails.
An implementation that targets a reserve with different replacement rules must recompute the cost of a persist.

## 11. Concurrency

The format is single-writer.
Two writers that issue the same index cannot be reconciled after the fact, and multi-writer coordination is out of scope for version 1.
One writer may nevertheless allocate from many threads.

A fill watermark is allocated lock-free.
An allocation compare-and-swaps one counter and takes no lock over the table, so a read of the whole table while issuance runs observes no single instant.
An encoder may snapshot such a table without stopping issuance.
The result is a monotone under-approximation of the table at the end of the read, and it is never an over-approximation, because a fill watermark only rises.

This is deliberate and it is safe.
Every slot below a restored watermark was already burned by the issuance that the snapshot did not observe, so restoring an older count re-burns nothing.
A restored count can reissue an index whose chunk is still live.
That is the same data-loss window that the persistence cadence covers, and it costs the batch no further capacity.

The encoder must still write the counter sum of the exact counter values that it encodes.
An incrementally maintained total that ran ahead of the encoded counters fails the sum check of section 7 at the decoder.

A ring cursor must not be read this way.
A cursor falls on wrap, so it is not monotone and an inconsistent read of a mutable table has no under-approximation argument.
A mutable table must be read under the lock that serializes its cursor advances.

An exact checkpoint is available when a caller wants one.
The `StampSink::pause` hook parks admission, and a drain of the sink after the pause reaches a quiescent point.
A snapshot taken there is byte-exact for both modes.

## 12. Test vectors

`crates/postage-usage/tests/vector.rs` pins the vectors.
A failure there means the wire format changed.

- A single-chunk snapshot with a 142-byte root.
  The geometry is `d = 12` and `u = 8`, the counts are `3 + (b mod 4)` with bucket 200 full at 16, and the encoder takes `base = 3` and `w = 2`.
  The layout is the 66-byte header, one 8-byte exception, one 4-byte slot and 64 packed bytes.
- A 14-chunk snapshot with a 554-byte root.
  The geometry is `d = 29` and `u = 16`, the counts are `100 + (b mod 50)` with bucket 0x1234 at 5000 and bucket 0xcbe5 full at 8192, and the encoder takes `base = 100` and `w = 6`.
  The layout is the 66-byte header, two 8-byte exceptions, fourteen 4-byte slots and thirteen 32-byte digests.
  The leaves are twelve full 4096-byte payloads and one 3-byte payload that holds the last four buckets.
- A mutable vector with the geometry and the counters of the first vector, which differs from it in the flags byte alone.

The bucket depth of the first vector is below the mainnet floor of 16.
The format supports the bucket depths 1 to 16, and the floor of the network decides which of them a batch may declare, so that vector is pinned for a deployment with a lower floor.

## 13. Version and extensions

The magic carries the format version.
A decoder must reject a payload whose magic is not `"SBU1"`.

Two extension points exist for a later version.
The reserved flag bits carry a new mode, and a new magic carries a new layout.
A managed free list of released slots, and a saturated bit for each bucket that would give exact occupancy for a wrapped ring, are both candidates for a later version and are out of scope for version 1.
