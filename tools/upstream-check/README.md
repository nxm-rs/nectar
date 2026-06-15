# upstream-check

Guards against drift between the contract addresses and deploy blocks hard-coded
in `nectar-contracts` and the canonical values ethersphere publishes in
[`go-storage-incentives-abi`](https://github.com/ethersphere/go-storage-incentives-abi),
the package the bee node compiles against.

Rather than parsing each other's source, the two sides emit structured data and
compare it:

- the `nectar-contracts` `dump_deployments` example prints its `mainnet` /
  `testnet` constants as JSON, straight from the real typed constants;
- this Go program imports `go-storage-incentives-abi` and compares the upstream
  address + deploy block against that JSON.

The deploy block is compared as well as the address (the block is the start point
for on-chain event indexing such as postage-stamp scanning). The BZZ token block
is the one exception: nectar stores `0` (the token is not event-indexed), so it
is printed but not compared.

Run locally:

```sh
cargo run -p nectar-contracts --example dump_deployments --quiet > nectar.json
cd tools/upstream-check && go run . -nectar ../../nectar.json
```

A non-zero exit means nectar's constants need updating to match upstream. In CI
the `Upstream addresses` workflow runs this weekly (and on changes to the
relevant files), fetching the latest `go-storage-incentives-abi` release first so
a redeployment upstream turns the check red.
