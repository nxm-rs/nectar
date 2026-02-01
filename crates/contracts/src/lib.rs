//! Swarm contract bindings and deployment information.
//!
//! This crate provides type-safe Solidity contract bindings using Alloy's `sol!` macro,
//! along with deployment information for mainnet and testnet.
//!
//! # Deployment Information
//!
//! Each contract has a deployment struct that bundles address and deployment block:
//!
//! ```
//! use nectar_contracts::mainnet;
//!
//! let postage = mainnet::POSTAGE_STAMP;
//! assert_ne!(postage.address, alloy_primitives::Address::ZERO);
//! assert!(postage.block > 0);
//! ```
//!
//! # Contract Bindings
//!
//! The `sol!` macro generates call types, return types, and event types that can be
//! used with alloy providers:
//!
//! ```ignore
//! use alloy_sol_types::SolCall;
//! use nectar_contracts::{IPostageStamp, mainnet};
//!
//! // Encode a call
//! let call = IPostageStamp::batchOwnerCall { batchId: batch_id };
//! let encoded = call.abi_encode();
//! ```

#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

use alloy_primitives::{Address, address};
use alloy_sol_types::sol;

// Deployment Info Macro

/// Macro to define a contract deployment struct with address and block.
macro_rules! define_deployment {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub struct $name {
            /// Contract address.
            pub address: Address,
            /// Deployment block number.
            pub block: u64,
        }

        impl $name {
            /// Creates a new deployment.
            #[must_use]
            pub const fn new(address: Address, block: u64) -> Self {
                Self { address, block }
            }
        }
    };
}

// Deployment Information Types

define_deployment!(
    /// BZZ token deployment information.
    Token
);

define_deployment!(
    /// Postage stamp contract deployment information.
    PostageStamp
);

define_deployment!(
    /// Stake registry contract deployment information.
    StakeRegistry
);

define_deployment!(
    /// Redistribution contract deployment information.
    Redistribution
);

define_deployment!(
    /// Storage price oracle contract deployment information.
    StoragePriceOracle
);

define_deployment!(
    /// Chequebook factory contract deployment information.
    ChequebookFactory
);

define_deployment!(
    /// Swap price oracle contract deployment information.
    SwapPriceOracle
);

// Token Interface

sol! {
    /// Standard ERC20 token interface.
    #[derive(Debug, PartialEq, Eq)]
    interface IERC20 {
        function name() external view returns (string memory);
        function symbol() external view returns (string memory);
        function decimals() external view returns (uint8);
        function totalSupply() external view returns (uint256);
        function balanceOf(address account) external view returns (uint256);
        function transfer(address to, uint256 amount) external returns (bool);
        function allowance(address owner, address spender) external view returns (uint256);
        function approve(address spender, uint256 amount) external returns (bool);
        function transferFrom(address from, address to, uint256 amount) external returns (bool);

        event Transfer(address indexed from, address indexed to, uint256 value);
        event Approval(address indexed owner, address indexed spender, uint256 value);
    }
}

// Storage Incentive Contract Interfaces

sol! {
    /// Postage stamp contract interface.
    ///
    /// Manages postage stamp batches required for uploading data to Swarm.
    #[derive(Debug, PartialEq, Eq)]
    interface IPostageStamp {
        function withdraw(address beneficiary) external;
        function setPrice(uint256 price) external;
        function validChunkCount() external view returns (uint256);
        function batchOwner(bytes32 batchId) external view returns (address);
        function batchDepth(bytes32 batchId) external view returns (uint8);
        function batchBucketDepth(bytes32 batchId) external view returns (uint8);
        function remainingBalance(bytes32 batchId) external view returns (uint256);
        function minimumInitialBalancePerChunk() external view returns (uint256);
        function batches(bytes32 batchId) external view returns (
            address owner,
            uint8 depth,
            uint8 bucketDepth,
            bool immutableFlag,
            uint256 normalisedBalance,
            uint256 lastUpdatedBlockNumber
        );
    }

    /// Stake registry contract interface.
    ///
    /// Manages staking for nodes participating in storage incentives.
    #[derive(Debug, PartialEq, Eq)]
    interface IStakeRegistry {
        function stakes(address owner) external view returns (
            bytes32 overlay,
            uint256 committedStake,
            uint256 potentialStake,
            uint256 lastUpdatedBlockNumber,
            uint8 height
        );
        function overlayOfAddress(address owner) external view returns (bytes32);
        function heightOfAddress(address owner) external view returns (uint8);
        function nodeEffectiveStake(address owner) external view returns (uint256);
        function lastUpdatedBlockNumberOfAddress(address owner) external view returns (uint256);
        function freezeDeposit(address owner, uint256 time) external;

        event StakeUpdated(
            address indexed owner,
            uint256 committedStake,
            uint256 potentialStake,
            bytes32 overlay,
            uint256 lastUpdatedBlock,
            uint8 height
        );
        event StakeSlashed(address slashed, bytes32 overlay, uint256 amount);
        event StakeFrozen(address frozen, bytes32 overlay, uint256 time);
        event StakeWithdrawn(address node, uint256 amount);
    }

    /// Redistribution contract interface.
    ///
    /// Implements the Schelling coordination game for storage reward distribution.
    #[derive(Debug, PartialEq, Eq)]
    interface IRedistribution {
        function currentRound() external view returns (uint64);
        function currentPhaseCommit() external view returns (bool);
        function currentPhaseReveal() external view returns (bool);
        function currentPhaseClaim() external view returns (bool);
        function isParticipatingInUpcomingRound(bytes32 overlay, uint8 depth) external view returns (bool);
        function isWinner(bytes32 overlay) external view returns (bool);
        function claim(
            bytes32[] calldata proofSegments,
            bytes32 proveSegment,
            bytes32[] calldata proofSegments2,
            bytes32 proveSegment2,
            uint64 chunkSpan,
            bytes32[] calldata proofSegments3
        ) external;
    }

    /// Storage price oracle contract interface.
    ///
    /// Controls the price per chunk for postage stamp batches.
    #[derive(Debug, PartialEq, Eq)]
    interface IStoragePriceOracle {
        function PRICE_UPDATER_ROLE() external view returns (uint256);
        function postageStamp() external view returns (address);
        function currentPrice() external view returns (uint32);
        function minimumPrice() external view returns (uint32);
        function currentRound() external view returns (uint64);
        function lastAdjustedRound() external view returns (uint64);
        function isPaused() external view returns (bool);
        function setPrice(uint32 price) external returns (bool);
        function adjustPrice(uint16 redundancy) external returns (bool);
        function pause() external;
        function unPause() external;

        event PriceUpdate(uint256 price);
        event StampPriceUpdateFailed(uint256 attemptedPrice);
    }
}

// Swap Contract Interfaces (Chequebook)

#[cfg(feature = "serde")]
sol! {
    /// EIP-712 cheque struct for chequebook payments.
    ///
    /// This is the typed data structure used for signing cheques off-chain.
    /// The EIP-712 domain uses:
    /// - Name: "Chequebook"
    /// - Version: "1.0"
    /// - ChainId: network-specific (100 for Gnosis, 11155111 for Sepolia)
    #[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    struct Cheque {
        address chequebook;
        address beneficiary;
        uint256 cumulativePayout;
    }
}

#[cfg(not(feature = "serde"))]
sol! {
    /// EIP-712 cheque struct for chequebook payments.
    ///
    /// This is the typed data structure used for signing cheques off-chain.
    /// The EIP-712 domain uses:
    /// - Name: "Chequebook"
    /// - Version: "1.0"
    /// - ChainId: network-specific (100 for Gnosis, 11155111 for Sepolia)
    #[derive(Debug, PartialEq, Eq)]
    struct Cheque {
        address chequebook;
        address beneficiary;
        uint256 cumulativePayout;
    }
}

sol! {
    /// Chequebook contract interface (ERC20SimpleSwap).
    ///
    /// Allows the issuer to send cheques to counterparties for peer-to-peer payments.
    #[derive(Debug, PartialEq, Eq)]
    interface IChequebook {
        function issuer() external view returns (address);
        function token() external view returns (address);
        function balance() external view returns (uint256);
        function liquidBalance() external view returns (uint256);
        function liquidBalanceFor(address beneficiary) external view returns (uint256);
        function paidOut(address beneficiary) external view returns (uint256);
        function totalPaidOut() external view returns (uint256);
        function totalHardDeposit() external view returns (uint256);
        function defaultHardDepositTimeout() external view returns (uint256);
        function bounced() external view returns (bool);
        function hardDeposits(address beneficiary) external view returns (
            uint256 amount,
            uint256 decreaseAmount,
            uint256 timeout,
            uint256 canBeDecreasedAt
        );
        function init(address _issuer, address _token, uint256 _defaultHardDepositTimeout) external;
        function cashCheque(
            address beneficiary,
            address recipient,
            uint256 cumulativePayout,
            bytes memory beneficiarySig,
            uint256 callerPayout,
            bytes memory issuerSig
        ) external;
        function cashChequeBeneficiary(address recipient, uint256 cumulativePayout, bytes memory issuerSig) external;
        function increaseHardDeposit(address beneficiary, uint256 amount) external;
        function prepareDecreaseHardDeposit(address beneficiary, uint256 decreaseAmount) external;
        function decreaseHardDeposit(address beneficiary) external;
        function setCustomHardDepositTimeout(address beneficiary, uint256 hardDepositTimeout, bytes memory beneficiarySig) external;
        function withdraw(uint256 amount) external;

        event ChequeCashed(
            address indexed beneficiary,
            address indexed recipient,
            address indexed caller,
            uint256 totalPayout,
            uint256 cumulativePayout,
            uint256 callerPayout
        );
        event ChequeBounced();
        event HardDepositAmountChanged(address indexed beneficiary, uint256 amount);
        event HardDepositDecreasePrepared(address indexed beneficiary, uint256 decreaseAmount);
        event HardDepositTimeoutChanged(address indexed beneficiary, uint256 timeout);
        event Withdraw(uint256 amount);
    }

    /// Chequebook factory contract interface (SimpleSwapFactory).
    #[derive(Debug, PartialEq, Eq)]
    interface IChequebookFactory {
        function ERC20Address() external view returns (address);
        function master() external view returns (address);
        function deployedContracts(address addr) external view returns (bool);
        function deploySimpleSwap(address issuer, uint256 defaultHardDepositTimeoutDuration, bytes32 salt) external returns (address);

        event SimpleSwapDeployed(address contractAddress);
    }

    /// Swap price oracle contract interface.
    #[derive(Debug, PartialEq, Eq)]
    interface ISwapPriceOracle {
        function price() external view returns (uint256);
        function chequeValueDeduction() external view returns (uint256);
        function getPrice() external view returns (uint256 price, uint256 chequeValueDeduction);
        function updatePrice(uint256 newPrice) external;
        function updateChequeValueDeduction(uint256 newChequeValueDeduction) external;

        event PriceUpdate(uint256 price);
        event ChequeValueDeductionUpdate(uint256 chequeValueDeduction);
    }
}

// Gnosis Chain Mainnet Deployments

/// Gnosis Chain mainnet contract deployments.
pub mod mainnet {
    use super::*;

    /// BZZ token (xBZZ on Gnosis Chain).
    pub const BZZ_TOKEN: Token =
        Token::new(address!("dBF3Ea6F5beE45c02255B2c26a16F300502F68da"), 0);

    /// Postage stamp contract.
    pub const POSTAGE_STAMP: PostageStamp = PostageStamp::new(
        address!("5b53f7a1975eb212d4b20b7cdd443baa189af7c9"),
        31305656,
    );

    /// Stake registry contract.
    pub const STAKING: StakeRegistry = StakeRegistry::new(
        address!("0c6aa197271466f0afe3818ca03ac47d8f5c2f8a"),
        40430237,
    );

    /// Redistribution contract.
    pub const REDISTRIBUTION: Redistribution = Redistribution::new(
        address!("eb210c2e166f61b3fd32246d53893f8b9d2a624c"),
        41105199,
    );

    /// Storage price oracle contract.
    pub const STORAGE_PRICE_ORACLE: StoragePriceOracle = StoragePriceOracle::new(
        address!("47EeF336e7fE5bED98499A4696bce8f28c1B0a8b"),
        37339168,
    );

    /// Chequebook factory contract.
    pub const CHEQUEBOOK_FACTORY: ChequebookFactory = ChequebookFactory::new(
        address!("c2d5a532cf69aa9a1378737d8ccdef884b6e7420"),
        39939970,
    );

    /// Swap price oracle contract.
    pub const SWAP_PRICE_ORACLE: SwapPriceOracle = SwapPriceOracle::new(
        address!("A57A50a831B31c904A770edBCb706E03afCdbd94"),
        39939970,
    );
}

// Sepolia Testnet Deployments

/// Sepolia testnet contract deployments.
pub mod testnet {
    use super::*;

    /// BZZ token (sBZZ on Sepolia).
    pub const BZZ_TOKEN: Token =
        Token::new(address!("6e01ee6183721ae9a006fd4906970c1583863765"), 0);

    /// Postage stamp contract.
    pub const POSTAGE_STAMP: PostageStamp = PostageStamp::new(
        address!("621c2e0fa5ed488c7124eb55cc7eb3af75d0d9e8"),
        6596277,
    );

    /// Stake registry contract.
    pub const STAKING: StakeRegistry = StakeRegistry::new(
        address!("6f252dd6f340f6c6d2f6ee8954b011dd5aba4350"),
        8262529,
    );

    /// Redistribution contract.
    pub const REDISTRIBUTION: Redistribution = Redistribution::new(
        address!("fb6c7d33be1fb12f4c5da71df7c9d5c22970ba7a"),
        8646721,
    );

    /// Storage price oracle contract.
    pub const STORAGE_PRICE_ORACLE: StoragePriceOracle = StoragePriceOracle::new(
        address!("95Dc18380e92C13E4F8a4e94C99FB1b97250174B"),
        8226873,
    );

    /// Chequebook factory contract.
    pub const CHEQUEBOOK_FACTORY: ChequebookFactory = ChequebookFactory::new(
        address!("0fF044F6bB4F684a5A149B46D7eC03ea659F98A1"),
        4752810,
    );

    /// Swap price oracle contract.
    pub const SWAP_PRICE_ORACLE: SwapPriceOracle = SwapPriceOracle::new(
        address!("1814e9b3951Df0CB8e12b2bB99c5594514588936"),
        4752810,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::U256;

    #[test]
    fn test_deployments_non_zero() {
        // Mainnet
        assert_ne!(mainnet::BZZ_TOKEN.address, Address::ZERO);
        assert_ne!(mainnet::POSTAGE_STAMP.address, Address::ZERO);
        assert_ne!(mainnet::STAKING.address, Address::ZERO);
        assert_ne!(mainnet::REDISTRIBUTION.address, Address::ZERO);
        assert_ne!(mainnet::STORAGE_PRICE_ORACLE.address, Address::ZERO);
        assert_ne!(mainnet::CHEQUEBOOK_FACTORY.address, Address::ZERO);
        assert_ne!(mainnet::SWAP_PRICE_ORACLE.address, Address::ZERO);

        // Testnet
        assert_ne!(testnet::BZZ_TOKEN.address, Address::ZERO);
        assert_ne!(testnet::POSTAGE_STAMP.address, Address::ZERO);
        assert_ne!(testnet::STAKING.address, Address::ZERO);
        assert_ne!(testnet::REDISTRIBUTION.address, Address::ZERO);
        assert_ne!(testnet::STORAGE_PRICE_ORACLE.address, Address::ZERO);
        assert_ne!(testnet::CHEQUEBOOK_FACTORY.address, Address::ZERO);
        assert_ne!(testnet::SWAP_PRICE_ORACLE.address, Address::ZERO);
    }

    #[test]
    fn test_sol_types_generated() {
        let _ = IERC20::balanceOfCall {
            account: Address::ZERO,
        };
        let _ = IPostageStamp::batchOwnerCall {
            batchId: [0u8; 32].into(),
        };
        let _ = IStakeRegistry::overlayOfAddressCall {
            owner: Address::ZERO,
        };
        let _ = IChequebookFactory::deploySimpleSwapCall {
            issuer: Address::ZERO,
            defaultHardDepositTimeoutDuration: U256::ZERO,
            salt: [0u8; 32].into(),
        };
    }
}
