# Spec references

Source citations for the algorithms this crate ports from the reference client. Citations pin the reference client at `a17e3a9c` (master, 2025-12-26); line numbers are valid at that revision and may drift on later ones.

Module docs link here instead of restating citations inline.

## Span level decoding

| Citation | Description |
|---|---|
| `pkg/file/redundancy/span.go:13-34` | A redundancy-enabled upload packs the level into the span's most significant byte as `level | 0x80`; `DecodeSpan` returns the level and the span with byte 7 zeroed; `IsLevelEncoded` is the strict `span[7] > 128` predicate, so a byte 7 of exactly `0x80` is not treated as encoded and its value is kept as a plain length. |
