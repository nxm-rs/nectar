//! The runtime and type-level views of a network agree, and both resolve from
//! the crate root.

use nectar_primitives::{Mainnet, NamedSwarm, Swarm, SwarmKind, SwarmSpec as _, Testnet};

#[test]
fn named_ids_match_the_spec_network_ids() {
    assert_eq!(NamedSwarm::Mainnet.id(), Mainnet::NETWORK_ID.get());
    assert_eq!(NamedSwarm::Testnet.id(), Testnet::NETWORK_ID.get());
}

#[test]
fn spec_network_ids_classify_as_their_named_swarm() {
    assert_eq!(
        Swarm::from_id(Mainnet::NETWORK_ID.get()).named(),
        Some(NamedSwarm::Mainnet)
    );
    assert_eq!(
        Swarm::from_id(Testnet::NETWORK_ID.get()).named(),
        Some(NamedSwarm::Testnet)
    );
}

#[test]
fn an_unknown_id_stays_unnamed() {
    let custom = Swarm::from_id(999_999);

    assert_eq!(custom.named(), None);
    assert_eq!(*custom.kind(), SwarmKind::Id(999_999));
    assert_eq!(custom.id(), 999_999);
}
