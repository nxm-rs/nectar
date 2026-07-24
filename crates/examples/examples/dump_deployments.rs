//! Print nectar-contracts' deployed addresses and blocks as JSON.
//!
//! Emitted from the real typed `mainnet`/`testnet` constants, so this output can
//! never drift from the crate. Consumed by `tools/upstream-check` to compare
//! against ethersphere's go-storage-incentives-abi without parsing source.
//!
//! Run: `cargo run -p nectar-examples --example dump_deployments`

use nectar_contracts::{mainnet, testnet};

/// Render the five storage-incentives deployments of one network as a JSON object.
/// Every deployment struct exposes `address` (EIP-55 `Display`) and `block`.
macro_rules! network_json {
    ($net:ident) => {
        format!(
            concat!(
                "{{",
                r#""bzz_token":{{"address":"{}","block":{}}},"#,
                r#""postage_stamp":{{"address":"{}","block":{}}},"#,
                r#""price_oracle":{{"address":"{}","block":{}}},"#,
                r#""redistribution":{{"address":"{}","block":{}}},"#,
                r#""staking":{{"address":"{}","block":{}}}"#,
                "}}"
            ),
            $net::BZZ_TOKEN.address,
            $net::BZZ_TOKEN.block,
            $net::POSTAGE_STAMP.address,
            $net::POSTAGE_STAMP.block,
            $net::STORAGE_PRICE_ORACLE.address,
            $net::STORAGE_PRICE_ORACLE.block,
            $net::REDISTRIBUTION.address,
            $net::REDISTRIBUTION.block,
            $net::STAKING.address,
            $net::STAKING.block,
        )
    };
}

fn main() {
    println!(
        r#"{{"mainnet":{},"testnet":{}}}"#,
        network_json!(mainnet),
        network_json!(testnet)
    );
}
