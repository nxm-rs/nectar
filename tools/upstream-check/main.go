// Command upstream-check establishes that the deployed contract addresses and
// deploy blocks hard-coded in nectar-contracts match the canonical values
// published by ethersphere's go-storage-incentives-abi, the same package the
// bee node compiles against.
//
// The two sides emit structured data rather than parsing each other's source:
// the nectar-contracts `dump_deployments` example prints its constants as JSON
// (from the real typed constants, so it can never drift from the crate), and
// this program prints the upstream values from the imported Go package. This
// program reads the nectar JSON (via -nectar) and compares the two, so there is
// no fragile source grepping on either side.
//
// It is meant to run in CI on a schedule: when ethersphere ships a redeployment
// (a new release with a changed address or deploy block), this check flips red
// so we know to update nectar's constants. The workflow fetches the latest
// release before running, so "upstream" always means the newest tag.
//
// Scope: the five storage-incentives contracts the abi package exports
// (BZZ token, PostageStamp, PriceOracle, Redistribution, Staking) on mainnet and
// testnet. The swap contracts (chequebook factory, swap price oracle) are not in
// this package and are not checked. Both address and deploy block are compared;
// the block is the start point for on-chain event indexing (e.g. postage-stamp
// scanning), so a moved block matters on its own. The BZZ token block is the one
// exception: nectar stores 0 (the token is not event-indexed), so it is printed
// but not compared.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/ethersphere/go-storage-incentives-abi/abi"
)

// contract is one comparable deployment slot. checkBlock is false for the token,
// whose block nectar deliberately stores as 0.
type contract struct {
	key        string
	checkBlock bool
}

var contracts = []contract{
	{"bzz_token", false},
	{"postage_stamp", true},
	{"price_oracle", true},
	{"redistribution", true},
	{"staking", true},
}

// deployment is the JSON shape both sides speak: an address and its deploy block.
type deployment struct {
	Address string `json:"address"`
	Block   uint64 `json:"block"`
}

// upstream returns the canonical address + block from go-storage-incentives-abi.
func upstream() map[string]map[string]deployment {
	return map[string]map[string]deployment{
		"mainnet": {
			"bzz_token":      {abi.MainnetBzzTokenAddress, uint64(abi.MainnetBzzTokenBlockNumber)},
			"postage_stamp":  {abi.MainnetPostageStampAddress, uint64(abi.MainnetPostageStampBlockNumber)},
			"price_oracle":   {abi.MainnetPriceOracleAddress, uint64(abi.MainnetPriceOracleBlockNumber)},
			"redistribution": {abi.MainnetRedistributionAddress, uint64(abi.MainnetRedistributionBlockNumber)},
			"staking":        {abi.MainnetStakingAddress, uint64(abi.MainnetStakingBlockNumber)},
		},
		"testnet": {
			"bzz_token":      {abi.TestnetBzzTokenAddress, uint64(abi.TestnetBzzTokenBlockNumber)},
			"postage_stamp":  {abi.TestnetPostageStampAddress, uint64(abi.TestnetPostageStampBlockNumber)},
			"price_oracle":   {abi.TestnetPriceOracleAddress, uint64(abi.TestnetPriceOracleBlockNumber)},
			"redistribution": {abi.TestnetRedistributionAddress, uint64(abi.TestnetRedistributionBlockNumber)},
			"staking":        {abi.TestnetStakingAddress, uint64(abi.TestnetStakingBlockNumber)},
		},
	}
}

// norm lowercases an address and strips an optional 0x prefix so checksummed and
// un-prefixed forms compare equal.
func norm(a string) string {
	return strings.TrimPrefix(strings.ToLower(a), "0x")
}

func main() {
	nectarPath := flag.String("nectar", "nectar.json", "path to the JSON emitted by the nectar-contracts dump_deployments example")
	flag.Parse()

	raw, err := os.ReadFile(*nectarPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error reading nectar JSON:", err)
		os.Exit(2)
	}
	var nec map[string]map[string]deployment
	if err := json.Unmarshal(raw, &nec); err != nil {
		fmt.Fprintln(os.Stderr, "error parsing nectar JSON:", err)
		os.Exit(2)
	}

	up := upstream()
	drift := false
	for _, net := range []string{"mainnet", "testnet"} {
		fmt.Printf("== %s ==\n", net)
		nn, ok := nec[net]
		if !ok {
			fmt.Fprintf(os.Stderr, "nectar JSON is missing network %q\n", net)
			os.Exit(2)
		}
		for _, c := range contracts {
			u := up[net][c.key]
			n, ok := nn[c.key]
			if !ok {
				fmt.Fprintf(os.Stderr, "nectar JSON is missing %s/%s\n", net, c.key)
				os.Exit(2)
			}

			addrOK := norm(u.Address) == norm(n.Address)
			blockOK := !c.checkBlock || u.Block == n.Block
			if !addrOK || !blockOK {
				drift = true
			}

			addrField := "addr ok"
			if !addrOK {
				addrField = fmt.Sprintf("addr DRIFT (nectar=%s upstream=%s)", n.Address, u.Address)
			}
			var blockField string
			switch {
			case !c.checkBlock:
				blockField = fmt.Sprintf("block %d (token, not compared)", n.Block)
			case blockOK:
				blockField = fmt.Sprintf("block ok (%d)", n.Block)
			default:
				blockField = fmt.Sprintf("block DRIFT (nectar=%d upstream=%d)", n.Block, u.Block)
			}
			fmt.Printf("  %-15s %-58s %s\n", c.key, addrField, blockField)
		}
	}

	if drift {
		fmt.Fprintln(os.Stderr, "\nUPSTREAM DRIFT DETECTED: nectar-contracts no longer matches go-storage-incentives-abi.")
		fmt.Fprintln(os.Stderr, "Update the address and/or block in crates/contracts/src/lib.rs to the upstream values above.")
		os.Exit(1)
	}
	fmt.Println("\nAll addresses and deploy blocks match go-storage-incentives-abi.")
}
