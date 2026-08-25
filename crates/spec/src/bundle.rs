//! The `SwarmPrimitives` bundle.
//!
//! The bundle a generic parameter names when it would otherwise name the
//! network specification and the chunk body size apart. It is methodless:
//! a marker a type carries, not a value a runtime builds.

use core::fmt;

use crate::SwarmSpec;

/// The primitives a Swarm deployment is parameterized by, threaded through a
/// single generic parameter.
///
/// A bundle is a marker: implementors are unit types that name the network
/// specification they target and the body size their chunk registry cuts.
pub trait SwarmPrimitives: Copy + fmt::Debug + Send + Sync + 'static {
    /// The network specification this bundle keys off, so the routing knobs a
    /// consumer reads stay one projection away.
    type Spec: SwarmSpec;

    /// The body size, in bytes, the bundle's chunk registry cuts.
    const BODY_SIZE: usize;
}

/// The spec marker a [`SwarmPrimitives`] bundle keys off.
pub type SpecOf<P> = <P as SwarmPrimitives>::Spec;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Mainnet, NetworkId};

    /// The test bundle the shape assertions run against.
    #[derive(Debug, Clone, Copy)]
    struct StandardBundle;

    impl SwarmPrimitives for StandardBundle {
        type Spec = Mainnet;
        const BODY_SIZE: usize = 4096;
    }

    #[test]
    fn bundle_projects_its_spec_and_size() {
        assert_eq!(SpecOf::<StandardBundle>::NETWORK_ID, NetworkId::MAINNET);
        assert_eq!(SpecOf::<StandardBundle>::MIN_BUCKET_DEPTH.get(), 16);
        assert_eq!(StandardBundle::BODY_SIZE, 4096);
    }

    /// The bundle is a token: a generic can name it without constructing it.
    fn name_bundle<P: SwarmPrimitives>() -> usize {
        P::BODY_SIZE
    }

    #[test]
    fn generic_names_the_bundle() {
        assert_eq!(name_bundle::<StandardBundle>(), 4096);
    }
}
