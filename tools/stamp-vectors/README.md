# stamp-vectors

Generates the postage stamp test vectors that `crates/postage/tests/reference_stamps.rs` asserts against.

The expected bytes must come from the reference client, not from nectar, or the vectors only re-assert nectar's own behaviour under a different name.
No golden stamp vectors exist upstream to port: the reference client's stamp tests generate a key at random and round-trip against themselves.
So this program links the reference client's `pkg/postage` and emits what it produces.

Three arms:

- `corpus`, the three stamps inside the reference client's committed inclusion-proof regression data (`pkg/storageincentives/testdata/inclusion-proofs.json`, copied verbatim to `testdata/`).
  Signing is re-run here from the same inputs `pkg/storageincentives/proof_test.go` uses, and the program fails unless every signature reproduces the committed bytes.
  Those stamps sign a literal index, so their bucket is 0.
- `boundary`, the all-fields-zero stamp nectar already carried, re-signed with the reference client's standard test key from `pkg/crypto/signer_test.go` so it too is checkable.
- `stamper`, stamps driven end to end through `postage.NewStamper`, so the bucket comes from the reference client's own address derivation and the index word from its own packing.
  This is the arm with a non-zero bucket and a full-width timestamp.

Every vector is checked against the reference client before it is written: owner recovery throughout, and `Stamp.Valid` for the stamper arm.

Regenerate with:

```sh
cd tools/stamp-vectors && go run . -out ../../crates/postage/tests/testdata/reference-stamps.json
```

The reference client is pinned in `go.mod` and recorded in the generated file's `provenance` block.
The stamper arm takes its timestamp from the wall clock, exactly as the reference client does, so regenerating produces different bytes.
The vectors are committed output, not a reproducible build artefact.
