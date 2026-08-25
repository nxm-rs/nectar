// The 32-byte anchor arm of stamp-vectors. See README.md.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash"
	"math/big"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethersphere/bee/v2/pkg/bmt"
	"github.com/ethersphere/bee/v2/pkg/bmtpool"
	"github.com/ethersphere/bee/v2/pkg/cac"
	"github.com/ethersphere/bee/v2/pkg/crypto"
	"github.com/ethersphere/bee/v2/pkg/postage"
	postagetesting "github.com/ethersphere/bee/v2/pkg/postage/testing"
	"github.com/ethersphere/bee/v2/pkg/soc"
	"github.com/ethersphere/bee/v2/pkg/storageincentives/redistribution"
	storer "github.com/ethersphere/bee/v2/pkg/storer"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

const (
	// The arm's output document, beside the upstream corpus it extends.
	anchor32Path = "testdata/inclusion-proofs-anchor32.json"

	// The fixed anchor for the arm: 31 zero bytes then one, kept sparse on
	// purpose so the anchored zero subtrees are exercised.
	anchor32Hex = "0x0000000000000000000000000000000000000000000000000000000000000001"
)

type anchorProofDocument struct {
	Provenance         provenance                          `json:"provenance"`
	Anchor             string                              `json:"anchor"`
	SampleChunkAddress string                              `json:"sampleChunkAddress"`
	Proofs             redistribution.ChunkInclusionProofs `json:"proofs"`
}

// regressionChunks rebuilds the 16 sample chunks of the reference client's
// TestMakeInclusionProofsRegression, from the same literals.
func regressionChunks() ([]swarm.Chunk, error) {
	privKey, err := crypto.DecodeSecp256k1PrivateKey([]byte(corpusKey))
	if err != nil {
		return nil, err
	}
	signer := crypto.NewDefaultSigner(privKey)

	batchID, err := crypto.LegacyKeccak256([]byte(corpusLabel))
	if err != nil {
		return nil, err
	}
	index := []byte{0, 0, 0, 0, 0, 8, 3, 3}
	timestamp := []byte{0, 0, 0, 0, 0, 3, 3, 8}
	stamper := func(addr swarm.Address) *postage.Stamp {
		sig := postagetesting.MustNewValidSignature(signer, addr, batchID, index, timestamp)
		return postage.NewStamp(batchID, index, timestamp, sig)
	}

	chunks := make([]swarm.Chunk, 0, storer.SampleSize)
	for i := range storer.SampleSize {
		ch, err := cac.New(fmt.Appendf(nil, "Unstoppable data! Chunk #%d", i+1))
		if err != nil {
			return nil, err
		}
		if i%2 == 0 {
			id, err := crypto.LegacyKeccak256(fmt.Appendf(nil, "ID #%d", i+1))
			if err != nil {
				return nil, err
			}
			socCh, err := soc.New(id, ch).Sign(signer)
			if err != nil {
				return nil, err
			}
			ch = socCh
		}
		chunks = append(chunks, ch.WithStamp(stamper(ch.Address())))
	}
	return chunks, nil
}

// spanOffset is the reference client's offset rule: a single-owner chunk
// carries its seal before the stored span.
func spanOffset(item storer.SampleItem) uint8 {
	ch := swarm.NewChunk(item.ChunkAddress, item.ChunkData)
	if soc.Valid(ch) {
		return swarm.HashSize + swarm.SocSignatureSize
	}
	return 0
}

// anchor32Proofs reproduces the reference client's inclusion-proof
// construction (makeInclusionProofs) with a 32-byte anchor in place of the
// regression test's one-byte anchor. Every emitted proof self-verifies with
// the reference client's own Prover.Verify before the document is written.
func anchor32Proofs() error {
	anchor1 := common.HexToHash(anchor32Hex)
	anchor2 := big.NewInt(30).Bytes() // the regression test's anchor2, unchanged.

	chunks, err := regressionChunks()
	if err != nil {
		return fmt.Errorf("regression chunks: %w", err)
	}
	sample, err := storer.MakeSampleUsingChunks(chunks, anchor1[:])
	if err != nil {
		return fmt.Errorf("sample: %w", err)
	}

	// The sample chunk is the CAC over the (address || transformed address)
	// pairs; re-derived, not trusted.
	var sampleContent []byte
	for _, item := range sample.Items {
		sampleContent = append(sampleContent, item.ChunkAddress.Bytes()...)
		sampleContent = append(sampleContent, item.TransformedAddress.Bytes()...)
	}
	sampleChunk, err := cac.New(sampleContent)
	if err != nil {
		return fmt.Errorf("sample chunk: %w", err)
	}

	require3 := storer.SampleSize - 1
	require1 := new(big.Int).Mod(new(big.Int).SetBytes(anchor2), big.NewInt(int64(require3))).Uint64()
	require2 := new(big.Int).Mod(new(big.Int).SetBytes(anchor2), big.NewInt(int64(require3-1))).Uint64()
	if require2 >= require1 {
		require2++
	}
	segmentIndex := int(new(big.Int).Mod(new(big.Int).SetBytes(anchor2), big.NewInt(128)).Uint64())

	rccontent := bmt.Prover{Hasher: bmtpool.Get()}
	rccontent.SetHeaderInt64(swarm.HashSize * storer.SampleSize * 2)
	if _, err := rccontent.Write(sampleContent); err != nil {
		return fmt.Errorf("sample prover: %w", err)
	}
	if _, err := rccontent.Hash(nil); err != nil {
		return fmt.Errorf("sample root: %w", err)
	}

	prefixHasherFactory := func() hash.Hash {
		return swarm.NewPrefixHasher(anchor1[:])
	}
	prefixHasherPool := bmt.NewPool(bmt.NewConf(prefixHasherFactory, swarm.BmtBranches, 8))

	witnesses := []uint64{require1, require2, uint64(require3)}

	claim := redistribution.ChunkInclusionProofs{}
	for pos, witness := range witnesses {
		item := sample.Items[int(witness)]
		offset := spanOffset(item)
		payload := item.ChunkData[offset+swarm.SpanSize:]
		header := item.ChunkData[offset : offset+swarm.SpanSize]

		ogContent := bmt.Prover{Hasher: bmtpool.Get()}
		ogContent.SetHeader(header)
		if _, err := ogContent.Write(payload); err != nil {
			return fmt.Errorf("witness %d prover: %w", witness, err)
		}
		if _, err := ogContent.Hash(nil); err != nil {
			return fmt.Errorf("witness %d root: %w", witness, err)
		}

		anchorContent := bmt.Prover{Hasher: prefixHasherPool.Get()}
		anchorContent.SetHeader(header)
		if _, err := anchorContent.Write(payload); err != nil {
			return fmt.Errorf("witness %d anchor prover: %w", witness, err)
		}
		if _, err := anchorContent.Hash(nil); err != nil {
			return fmt.Errorf("witness %d anchor root: %w", witness, err)
		}

		// The level-two root is the inner CAC over the stored content: for a
		// single-owner witness that is the wrapped chunk address, not the
		// single-owner seal address.
		inner, err := cac.NewWithDataSpan(item.ChunkData[offset:])
		if err != nil {
			return fmt.Errorf("witness %d inner cac: %w", witness, err)
		}

		sampleProof := rccontent.Proof(int(witness) * 2)
		plainProof := ogContent.Proof(segmentIndex)
		anchorProof := anchorContent.Proof(segmentIndex)

		// Self-verify against the reference client's own verify path, at
		// the roots the construction pins.
		root, err := rccontent.Verify(int(witness)*2, sampleProof)
		if err != nil || !bytes.Equal(root, sampleChunk.Address().Bytes()) {
			return fmt.Errorf("level one, witness %d: self-verification failed", witness)
		}
		root, err = ogContent.Verify(segmentIndex, plainProof)
		if err != nil || !bytes.Equal(root, inner.Address().Bytes()) {
			return fmt.Errorf("level two, witness %d: self-verification failed", witness)
		}
		root, err = anchorContent.Verify(segmentIndex, anchorProof)
		if err != nil {
			return fmt.Errorf("level three, witness %d: verify %w", witness, err)
		}
		if soc.Valid(swarm.NewChunk(item.ChunkAddress, item.ChunkData)) {
			// The sample pins keccak(wrapped || TR(inner)); the anchor proof
			// must bind to the TR(inner) half of that value.
			socHasher := swarm.NewHasher()
			if _, err := socHasher.Write(item.ChunkAddress.Bytes()); err != nil {
				return fmt.Errorf("level three, witness %d: %w", witness, err)
			}
			if _, err := socHasher.Write(root); err != nil {
				return fmt.Errorf("level three, witness %d: %w", witness, err)
			}
			if !bytes.Equal(socHasher.Sum(nil), item.TransformedAddress.Bytes()) {
				return fmt.Errorf("level three, witness %d: self-verification failed", witness)
			}
		} else if !bytes.Equal(root, item.TransformedAddress.Bytes()) {
			return fmt.Errorf("level three, witness %d: self-verification failed", witness)
		}

		bmtpool.Put(ogContent.Hasher)
		prefixHasherPool.Put(anchorContent.Hasher)

		switch pos {
		case 0:
			claim.A, err = redistribution.NewChunkInclusionProof(sampleProof, plainProof, anchorProof, item)
		case 1:
			claim.B, err = redistribution.NewChunkInclusionProof(sampleProof, plainProof, anchorProof, item)
		default:
			claim.C, err = redistribution.NewChunkInclusionProof(sampleProof, plainProof, anchorProof, item)
		}
		if err != nil {
			return fmt.Errorf("witness %d claim: %w", witness, err)
		}
	}
	bmtpool.Put(rccontent.Hasher)

	doc := anchorProofDocument{
		Provenance: provenance{
			Generator:   "tools/stamp-vectors",
			Command:     "go run . -out ../../crates/postage/tests/testdata/reference-stamps.json",
			Reference:   referenceModule,
			GeneratedAt: time.Now().UTC().Format(time.RFC3339),
			Notes: []string{
				"the arm re-runs the reference client's makeInclusionProofs construction with a 32-byte anchor in place of the regression test's one-byte anchor",
				"the witness content is the literal chunks Unstoppable data! Chunk #1..16, with the regression test's identifiers, secp256k1 key and stamp fields",
				"the anchor is 31 zero bytes followed by one, kept sparse on purpose",
				"every emitted proof self-verifies with the reference client's own bmt Prover.Verify before the document is written",
			},
		},
		Anchor:             anchor32Hex,
		SampleChunkAddress: hexed(sampleChunk.Address().Bytes()),
		Proofs:             claim,
	}

	encoded, err := json.MarshalIndent(doc, "", "    ")
	if err != nil {
		return fmt.Errorf("marshal anchor32 document: %w", err)
	}
	encoded = append(encoded, '\n')
	return os.WriteFile(anchor32Path, encoded, 0o644)
}
