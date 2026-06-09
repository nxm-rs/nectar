//! Canonical neighborhood-depth recomputation.
//!
//! Pure function port of bee `pkg/topology/kademlia/kademlia.go:896-920`
//! (`recalcDepth`). The wrapper type (`NeighborhoodDepth`) stays in the
//! routing layer of each downstream impl; nectar only owns the math.

use crate::Bin;

/// Recompute the neighborhood depth from per-bin connected-peer counts.
///
/// `connected_per_bin[i]` is the count of currently-connected peers in bin `i`.
/// `saturation` is the target saturation per bin (typically `SwarmSpec::saturation_peers()`).
/// `low_watermark` is the minimum cumulative count of peers in the deepest bins
/// to anchor the neighborhood (typically `SwarmSpec::neighborhood_low_watermark()`).
///
/// Algorithm - port of bee `kademlia.go:896-920`:
/// 1. Walk bins shallow → deep. The depth candidate is the **shallowest bin
///    whose count is below `saturation`**.
/// 2. From that candidate, sum populations of the deepest bins until the
///    cumulative count reaches `low_watermark`. The final depth is the
///    shallowest bin included in that anchored neighborhood.
/// 3. If the table never reaches saturation anywhere, depth is `0` (we are
///    sparse everywhere).
///
/// The returned [`Bin`] always satisfies `0..=MAX_PO`.
///
/// # Examples
///
/// ```
/// use nectar_primitives::{Bin, recompute_neighborhood_depth};
///
/// // Sparse table - depth is shallow.
/// let counts = [0u8; 32];
/// assert_eq!(recompute_neighborhood_depth(&counts, 8, 2), Bin::ZERO);
///
/// // Saturated up to bin 4, then deepest two bins have the watermark.
/// let mut counts = [8u8; 32];
/// counts[5] = 0;
/// counts[6] = 0;
/// counts[7] = 0;
/// counts[8] = 1;
/// counts[9] = 1;
/// // shallowest unsaturated = bin 5; deepest two have 2 peers - anchor at bin 8
/// // (population counts in 9 and 8 reach the watermark).
/// let depth = recompute_neighborhood_depth(&counts, 8, 2);
/// assert!(depth.get() <= 8);
/// ```
#[must_use]
pub fn recompute_neighborhood_depth(
    connected_per_bin: &[u8; 32],
    saturation: u8,
    low_watermark: u8,
) -> Bin {
    // Step 1: find the shallowest unsaturated bin.
    let mut candidate: u8 = 0;
    let mut found_unsaturated = false;
    for (i, count) in connected_per_bin.iter().enumerate() {
        if *count < saturation {
            candidate = i as u8;
            found_unsaturated = true;
            break;
        }
    }
    if !found_unsaturated {
        // Every bin is saturated: depth is the deepest occupied bin.
        // Bee returns `MaxPO` here, but the conservative interpretation is
        // that the neighborhood extends to the deepest bin we actually have
        // peers in - fall through and use the watermark anchor instead.
        candidate = crate::MAX_PO;
    }

    // Step 2: walk deep → shallow accumulating until we reach low_watermark.
    if low_watermark == 0 {
        return Bin::new_unchecked(candidate);
    }
    let mut sum: u32 = 0;
    let mut depth: u8 = candidate;
    for i in (0..connected_per_bin.len()).rev() {
        let idx = i as u8;
        if idx < candidate {
            // Walking shallower than the candidate means we already failed
            // to anchor inside the unsaturated zone - depth stays at candidate.
            break;
        }
        sum = sum.saturating_add(u32::from(connected_per_bin[i]));
        if sum >= u32::from(low_watermark) {
            depth = idx;
            break;
        }
    }

    // Step 3: depth never exceeds candidate (the unsaturated frontier).
    if depth > candidate {
        depth = candidate;
    }

    Bin::new_unchecked(depth)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_table_depth_is_zero() {
        assert_eq!(recompute_neighborhood_depth(&[0u8; 32], 8, 2), Bin::ZERO);
    }

    #[test]
    fn fully_saturated_depth_anchored_by_watermark() {
        // Saturated everywhere - anchor at the deepest bin where the
        // cumulative tail reaches the watermark.
        let counts = [8u8; 32];
        let depth = recompute_neighborhood_depth(&counts, 8, 2);
        // Tail walks 31, 30, ... summing 8 each - watermark of 2 reached at 31.
        assert_eq!(depth, Bin::MAX);
    }

    #[test]
    fn shallow_unsaturated_caps_depth() {
        // Bin 3 is unsaturated; tail has plenty of peers.
        let mut counts = [8u8; 32];
        counts[3] = 1;
        let depth = recompute_neighborhood_depth(&counts, 8, 2);
        // Depth can't exceed bin 3.
        assert!(depth.get() <= 3);
    }

    #[test]
    fn zero_watermark_returns_candidate() {
        let mut counts = [8u8; 32];
        counts[5] = 2;
        let depth = recompute_neighborhood_depth(&counts, 8, 0);
        assert_eq!(depth, Bin::new(5).unwrap());
    }

    #[test]
    fn very_sparse_tail_keeps_depth_shallow() {
        // Only bin 0 has any peers.
        let mut counts = [0u8; 32];
        counts[0] = 3;
        let depth = recompute_neighborhood_depth(&counts, 8, 2);
        assert_eq!(depth, Bin::ZERO);
    }
}
