// Command stamp-vectors emits postage stamp test vectors whose bytes come from
// the reference client rather than from nectar, so a nectar regression cannot
// move the expectation with it.
//
// Three arms:
//
//   - corpus: the three stamps inside the reference client's committed
//     inclusion-proof regression corpus. Signing is re-run here with the same
//     inputs the corpus was made from, and the tool fails unless every
//     signature reproduces the committed bytes. Those stamps sign a literal
//     index, so their bucket is 0 and they carry no batch geometry.
//   - boundary: the all-zero-field stamp nectar already carried, re-signed with
//     the reference client's standard test key so it too is checkable.
//   - stamper: stamps driven end to end through postage.NewStamper, so the
//     bucket comes from the reference client's own address derivation and the
//     index word from its own packing. This is the arm with a non-zero bucket.
//
// Every emitted vector is checked against the reference client before it is
// written: owner recovery throughout, and Stamp.Valid for the stamper arm.
//
// The stamper arm takes its timestamp from the wall clock, exactly as the
// reference client does, so regenerating produces different bytes. The vectors
// are committed output, not a reproducible build artefact.
package main

import (
	"bytes"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/big"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethersphere/bee/v2/pkg/crypto"
	"github.com/ethersphere/bee/v2/pkg/postage"
	postagetesting "github.com/ethersphere/bee/v2/pkg/postage/testing"
	"github.com/ethersphere/bee/v2/pkg/storage/inmemstore"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

const (
	referenceModule = "github.com/ethersphere/bee/v2 v2.7.0"
	corpusPath      = "pkg/storageincentives/testdata/inclusion-proofs.json"

	// The reference client's own regression inputs, pkg/storageincentives/proof_test.go.
	corpusKey   = "00000000000000000000000000000000"
	corpusLabel = "The Inverted Jenny"

	// The reference client's standard test key, pkg/crypto/signer_test.go.
	stamperKey   = "634fb5a872396d9693e5c9f9d7233cfa93f395c093371017ff44aa9ae6564cdd"
	stamperLabel = "nectar reference stamp vectors"

	// The signature of the stamp nectar already carried, re-derived rather than
	// trusted; a mismatch means the arm below is signing different inputs.
	inTreeBoundarySig = "0x496cb9ac06221d39c3f6a7dd3b9c2301c1f923162b90d5443e42023f34ff908945b0da1c297190f111b7c6ebc828648ead8f7fce06c0364cb5a833410230c5c01c"

	batchDepth  = 32
	bucketDepth = 16
)

//go:embed testdata/inclusion-proofs.json
var corpusJSON []byte

type postageProof struct {
	Signature []byte      `json:"signature"`
	PostageID common.Hash `json:"postageId"`
	Index     uint64      `json:"index"`
	TimeStamp uint64      `json:"timeStamp"`
}

type inclusionProof struct {
	ProveSegment common.Hash  `json:"proveSegment"`
	PostageProof postageProof `json:"postageProof"`
}

type inclusionProofs struct {
	A inclusionProof `json:"proof1"`
	B inclusionProof `json:"proof2"`
	C inclusionProof `json:"proofLast"`
}

type geometry struct {
	BucketDepth uint8 `json:"bucket_depth"`
	BatchDepth  uint8 `json:"batch_depth"`
}

type vector struct {
	Name         string    `json:"name"`
	Origin       string    `json:"origin"`
	ChunkAddress string    `json:"chunk_address"`
	Owner        string    `json:"owner"`
	BatchID      string    `json:"batch_id"`
	Bucket       uint32    `json:"bucket"`
	Index        uint32    `json:"index"`
	Timestamp    uint64    `json:"timestamp"`
	Prehash      string    `json:"prehash"`
	Stamp        string    `json:"stamp"`
	Geometry     *geometry `json:"geometry,omitempty"`
}

type provenance struct {
	Generator   string   `json:"generator"`
	Command     string   `json:"command"`
	Reference   string   `json:"reference"`
	Corpus      string   `json:"corpus"`
	GeneratedAt string   `json:"generated_at"`
	Notes       []string `json:"notes"`
}

type document struct {
	Provenance provenance `json:"provenance"`
	Vectors    []vector   `json:"vectors"`
}

func main() {
	out := flag.String("out", "", "write the vectors here instead of stdout")
	flag.Parse()

	vectors, err := generate()
	if err != nil {
		fmt.Fprintln(os.Stderr, "stamp-vectors:", err)
		os.Exit(1)
	}

	doc := document{
		Provenance: provenance{
			Generator: "tools/stamp-vectors",
			Command:   "go run . -out ../../crates/postage/tests/testdata/reference-stamps.json",
			Reference: referenceModule,
			Corpus:    corpusPath,
			// RFC3339 rather than the wall-clock nanoseconds below: this only
			// dates the file, it is not a stamp field.
			GeneratedAt: time.Now().UTC().Format(time.RFC3339),
			Notes: []string{
				"corpus vectors reproduce the committed upstream signatures byte for byte",
				"corpus vectors sign a literal index, so their bucket is 0 and no geometry applies",
				"stamper vectors are wall-clock timestamped, so regenerating changes their bytes",
			},
		},
		Vectors: vectors,
	}

	encoded, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, "stamp-vectors:", err)
		os.Exit(1)
	}
	encoded = append(encoded, '\n')

	if *out == "" {
		os.Stdout.Write(encoded)
		return
	}
	if err := os.WriteFile(*out, encoded, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "stamp-vectors:", err)
		os.Exit(1)
	}
}

func generate() ([]vector, error) {
	corpus, err := corpusVectors()
	if err != nil {
		return nil, fmt.Errorf("corpus arm: %w", err)
	}
	boundary, err := boundaryVector()
	if err != nil {
		return nil, fmt.Errorf("boundary arm: %w", err)
	}
	stamper, err := stamperVectors()
	if err != nil {
		return nil, fmt.Errorf("stamper arm: %w", err)
	}
	return append(append(corpus, boundary), stamper...), nil
}

// boundaryVector re-signs the all-zero-field stamp nectar already carried, so
// the vector that was in the tree with no traceable origin gains one.
func boundaryVector() (vector, error) {
	raw, err := hex.DecodeString(stamperKey)
	if err != nil {
		return vector{}, err
	}
	privKey, err := crypto.DecodeSecp256k1PrivateKey(raw)
	if err != nil {
		return vector{}, err
	}
	signer := crypto.NewDefaultSigner(privKey)
	owner, err := crypto.NewEthereumAddress(privKey.PublicKey)
	if err != nil {
		return vector{}, err
	}

	batchID := make([]byte, 32)
	batchID[31] = 1
	addrBytes := make([]byte, 32)
	addrBytes[31] = 2
	addr := swarm.NewAddress(addrBytes)
	index := uint64Bytes(0)
	timestamp := uint64Bytes(3)

	sig := postagetesting.MustNewValidSignature(signer, addr, batchID, index, timestamp)
	if hexed(sig) != inTreeBoundarySig {
		return vector{}, errors.New("re-signing did not reproduce the stamp already in the tree")
	}

	return newVector("boundary/all-fields-zero", "pkg/crypto/signer_test.go key", addr, owner, postage.NewStamp(batchID, index, timestamp, sig), nil)
}

func corpusVectors() ([]vector, error) {
	privKey, err := crypto.DecodeSecp256k1PrivateKey([]byte(corpusKey))
	if err != nil {
		return nil, err
	}
	signer := crypto.NewDefaultSigner(privKey)
	owner, err := crypto.NewEthereumAddress(privKey.PublicKey)
	if err != nil {
		return nil, err
	}

	batchID, err := crypto.LegacyKeccak256([]byte(corpusLabel))
	if err != nil {
		return nil, err
	}

	var proofs inclusionProofs
	if err := json.Unmarshal(corpusJSON, &proofs); err != nil {
		return nil, err
	}

	named := []struct {
		name  string
		proof inclusionProof
	}{
		{"corpus/proof1", proofs.A},
		{"corpus/proof2", proofs.B},
		{"corpus/proofLast", proofs.C},
	}

	vectors := make([]vector, 0, len(named))
	for _, n := range named {
		proof := n.proof.PostageProof
		if !bytes.Equal(proof.PostageID.Bytes(), batchID) {
			return nil, fmt.Errorf("%s: batch id is not keccak256(%q)", n.name, corpusLabel)
		}

		addr := swarm.NewAddress(n.proof.ProveSegment.Bytes())
		index := uint64Bytes(proof.Index)
		timestamp := uint64Bytes(proof.TimeStamp)

		sig := postagetesting.MustNewValidSignature(signer, addr, batchID, index, timestamp)
		if !bytes.Equal(sig, proof.Signature) {
			return nil, fmt.Errorf("%s: re-signing did not reproduce the committed signature", n.name)
		}

		v, err := newVector(n.name, corpusPath, addr, owner, postage.NewStamp(batchID, index, timestamp, sig), nil)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", n.name, err)
		}
		vectors = append(vectors, v)
	}
	return vectors, nil
}

func stamperVectors() ([]vector, error) {
	raw, err := hex.DecodeString(stamperKey)
	if err != nil {
		return nil, err
	}
	privKey, err := crypto.DecodeSecp256k1PrivateKey(raw)
	if err != nil {
		return nil, err
	}
	signer := crypto.NewDefaultSigner(privKey)
	owner, err := crypto.NewEthereumAddress(privKey.PublicKey)
	if err != nil {
		return nil, err
	}

	batchID, err := crypto.LegacyKeccak256([]byte(stamperLabel))
	if err != nil {
		return nil, err
	}

	wanted := []struct {
		name   string
		bucket uint16
		index  uint32
	}{
		{"stamper/bucket-only", 0xA53F, 0},
		{"stamper/bucket-and-index", 0xA53F, 1234},
		{"stamper/index-upper-bound", 0x8001, 1<<(batchDepth-bucketDepth) - 1},
	}

	geo := &geometry{BucketDepth: bucketDepth, BatchDepth: batchDepth}
	vectors := make([]vector, 0, len(wanted))
	for _, w := range wanted {
		// A fresh issuer per vector: the wanted index is the number of stamps
		// already taken from the bucket, and reuse would carry a count over.
		issuer := postage.NewStampIssuer(stamperLabel, "", batchID, big.NewInt(1), batchDepth, bucketDepth, 0, false)
		st := postage.NewStamper(inmemstore.New(), issuer, signer)

		var (
			addr  swarm.Address
			stamp *postage.Stamp
		)
		for i := uint32(0); i <= w.index; i++ {
			addr, err = bucketAddress(w.bucket, i)
			if err != nil {
				return nil, err
			}
			stamp, err = st.Stamp(addr, addr)
			if err != nil {
				return nil, fmt.Errorf("%s: stamp %d: %w", w.name, i, err)
			}
		}

		if err := stamp.Valid(addr, owner, batchDepth, bucketDepth, false); err != nil {
			return nil, fmt.Errorf("%s: %w", w.name, err)
		}
		bucket, index := postage.BucketIndexFromBytes(stamp.Index())
		if uint32(w.bucket) != bucket || w.index != index {
			return nil, fmt.Errorf("%s: wanted bucket %d index %d, got %d and %d", w.name, w.bucket, w.index, bucket, index)
		}

		v, err := newVector(w.name, "postage.NewStamper", addr, owner, stamp, geo)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", w.name, err)
		}
		vectors = append(vectors, v)
	}
	return vectors, nil
}

// bucketAddress returns a distinct address whose leading 16 bits, the bucket key
// at bucket depth 16, are bucket.
func bucketAddress(bucket uint16, n uint32) (swarm.Address, error) {
	h, err := crypto.LegacyKeccak256(fmt.Appendf(nil, "nectar stamp vector %d", n))
	if err != nil {
		return swarm.ZeroAddress, err
	}
	h[0] = byte(bucket >> 8)
	h[1] = byte(bucket)
	return swarm.NewAddress(h), nil
}

func newVector(name, origin string, addr swarm.Address, owner []byte, stamp *postage.Stamp, geo *geometry) (vector, error) {
	recovered, err := postage.RecoverBatchOwner(addr, stamp)
	if err != nil {
		return vector{}, err
	}
	if !bytes.Equal(recovered, owner) {
		return vector{}, fmt.Errorf("recovered %x, wanted owner %x", recovered, owner)
	}

	prehash, err := postage.ToSignDigest(addr.Bytes(), stamp.BatchID(), stamp.Index(), stamp.Timestamp())
	if err != nil {
		return vector{}, err
	}
	image, err := stamp.MarshalBinary()
	if err != nil {
		return vector{}, err
	}
	bucket, index := postage.BucketIndexFromBytes(stamp.Index())

	return vector{
		Name:         name,
		Origin:       origin,
		ChunkAddress: hexed(addr.Bytes()),
		Owner:        hexed(owner),
		BatchID:      hexed(stamp.BatchID()),
		Bucket:       bucket,
		Index:        index,
		Timestamp:    postage.TimestampFromBytes(stamp.Timestamp()),
		Prehash:      hexed(prehash),
		Stamp:        hexed(image),
		Geometry:     geo,
	}, nil
}

func uint64Bytes(v uint64) []byte {
	buf := make([]byte, 8)
	for i := range buf {
		buf[7-i] = byte(v >> (8 * i))
	}
	return buf
}

func hexed(b []byte) string {
	return "0x" + hex.EncodeToString(b)
}
