//! The runtime and type-level views of a network agree, and both resolve from
//! the crate root.

use nectar_primitives::{
    Mainnet, NamedSwarm, NetworkId, Swarm, SwarmKind, SwarmSpec as _, Testnet,
};

#[test]
fn named_ids_match_the_spec_network_ids() {
    assert_eq!(NetworkId::from(NamedSwarm::Mainnet), Mainnet::NETWORK_ID);
    assert_eq!(NetworkId::from(NamedSwarm::Testnet), Testnet::NETWORK_ID);
}

#[test]
fn spec_network_ids_classify_as_their_named_swarm() {
    let mainnet = Swarm::from_id(Mainnet::NETWORK_ID.get());
    let testnet = Swarm::from_id(Testnet::NETWORK_ID.get());

    assert_eq!(mainnet.named(), Some(NamedSwarm::Mainnet));
    assert_eq!(testnet.named(), Some(NamedSwarm::Testnet));
    assert_eq!(NetworkId::from(mainnet), Mainnet::NETWORK_ID);
}

#[test]
fn an_unknown_id_keeps_its_raw_network_id() {
    let custom = Swarm::from_id(999_999);

    assert_eq!(*custom.kind(), SwarmKind::Id(999_999));
    assert_eq!(NetworkId::from(custom), NetworkId::new(999_999));
}
