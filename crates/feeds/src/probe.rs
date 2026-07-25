//! Pure replay resolvers and round planning for the windowed finders.
//!
//! A resolver replays a sequential scan's decision procedure over a map of
//! completed presence answers and faults on the first index it consults
//! without one. The driver answers faults in rounds of bounded concurrent
//! probes; speculative answers the replay never consults are inert, so any
//! window width commits the boundary the sequential scan would.

use std::collections::{BTreeMap, VecDeque};

use core::num::NonZeroUsize;

/// Completed presence answers by index. Only a completed answer may enter;
/// an absent key is "not yet asked", never "absent".
pub(crate) type Answers = BTreeMap<u64, bool>;

/// One replay outcome over a fixed answer map.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Step {
    /// The replay consulted an index with no completed answer.
    Fault(Fault),
    /// Boundary committed: the update at `lo` is the result.
    Commit {
        /// Highest index observed present with an observed-absent successor,
        /// or the top slot.
        lo: u64,
    },
    /// The floor slot is absent.
    Empty,
}

/// A consulted-but-unanswered index plus the replay phase that consulted it,
/// the seed of one round's probe plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Fault {
    index: u64,
    phase: Phase,
}

/// Replay phase at the fault, selecting the speculation shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// The floor probe; speculation climbs the ladder from the floor.
    Floor,
    /// The exponential ladder at offset `off` from `base`.
    Ladder { base: u64, off: u64 },
    /// The top-slot arm after the offset left the index space.
    Top,
    /// The binary phase over the open bracket `(lo, hi)`.
    Binary { lo: u64, hi: u64 },
    /// The stepwise scan; speculation is consecutive successors.
    Linear,
}

impl Fault {
    /// The index this round must answer.
    #[cfg(test)]
    const fn index(&self) -> u64 {
        self.index
    }

    /// Plan one round into `out`: the faulting index first, then speculative
    /// indices along the replay's own future consult path, at most `width`
    /// in total.
    pub(crate) fn plan(&self, width: NonZeroUsize, out: &mut Vec<u64>) {
        out.clear();
        out.push(self.index);
        let cap = width.get();
        match self.phase {
            Phase::Floor => ladder(self.index, 1, cap, out),
            Phase::Ladder { base, off } => ladder(base, off.saturating_mul(2), cap, out),
            Phase::Top => {}
            Phase::Binary { lo, hi } => bisections(lo, hi, cap, out),
            Phase::Linear => successors(self.index, cap, out),
        }
    }
}

/// Replay of the exponential-then-binary scan from `base`.
pub(crate) fn resolve_probing(base: u64, answers: &Answers) -> Step {
    match answers.get(&base) {
        None => {
            return Step::Fault(Fault {
                index: base,
                phase: Phase::Floor,
            });
        }
        Some(false) => return Step::Empty,
        Some(true) => {}
    }

    // Exponential phase: double the probe offset until a miss brackets the
    // boundary; past the end of the index space the top slot decides.
    let mut lo = base;
    let mut off: u64 = 1;
    let mut hi = loop {
        let (index, phase) = base
            .checked_add(off)
            .map_or((u64::MAX, Phase::Top), |index| {
                (index, Phase::Ladder { base, off })
            });
        match answers.get(&index) {
            None => return Step::Fault(Fault { index, phase }),
            Some(true) if index == u64::MAX => return Step::Commit { lo: u64::MAX },
            Some(true) => {
                lo = index;
                off = off.saturating_mul(2);
            }
            Some(false) => break index,
        }
    };

    // Binary phase: `lo` present, `hi` absent; converge to adjacency.
    while let Some(gap) = hi.checked_sub(lo) {
        if gap <= 1 {
            break;
        }
        let Some(mid) = lo.checked_add(gap / 2) else {
            break;
        };
        match answers.get(&mid) {
            None => {
                return Step::Fault(Fault {
                    index: mid,
                    phase: Phase::Binary { lo, hi },
                });
            }
            Some(true) => lo = mid,
            Some(false) => hi = mid,
        }
    }
    Step::Commit { lo }
}

/// Replay of the stepwise scan from `base`.
pub(crate) fn resolve_linear(base: u64, answers: &Answers) -> Step {
    match answers.get(&base) {
        None => {
            return Step::Fault(Fault {
                index: base,
                phase: Phase::Linear,
            });
        }
        Some(false) => return Step::Empty,
        Some(true) => {}
    }
    let mut last = base;
    loop {
        let Some(candidate) = last.checked_add(1) else {
            return Step::Commit { lo: last };
        };
        match answers.get(&candidate) {
            None => {
                return Step::Fault(Fault {
                    index: candidate,
                    phase: Phase::Linear,
                });
            }
            Some(true) => last = candidate,
            Some(false) => return Step::Commit { lo: last },
        }
    }
}

/// Ladder rungs `base + first_off`, `base + 2 * first_off`, ...; past the
/// end of the index space the top slot is planned once.
fn ladder(base: u64, first_off: u64, cap: usize, out: &mut Vec<u64>) {
    let mut off = first_off;
    while out.len() < cap {
        let index = base.saturating_add(off);
        push_unique(out, index);
        if index == u64::MAX {
            break;
        }
        off = off.saturating_mul(2);
    }
}

/// Breadth-first midpoints of the decision tree under `(lo, hi)`; the root
/// midpoint is the faulting index already planned.
fn bisections(lo: u64, hi: u64, cap: usize, out: &mut Vec<u64>) {
    let mut intervals = VecDeque::from([(lo, hi)]);
    while let Some((low, high)) = intervals.pop_front() {
        if out.len() >= cap {
            break;
        }
        let Some(gap) = high.checked_sub(low) else {
            continue;
        };
        if gap <= 1 {
            continue;
        }
        let Some(mid) = low.checked_add(gap / 2) else {
            continue;
        };
        push_unique(out, mid);
        intervals.push_back((low, mid));
        intervals.push_back((mid, high));
    }
}

/// Consecutive successors of `from`, stopping at the top of the index space.
fn successors(from: u64, cap: usize, out: &mut Vec<u64>) {
    let mut index = from;
    while out.len() < cap {
        let Some(next) = index.checked_add(1) else {
            break;
        };
        push_unique(out, next);
        index = next;
    }
}

fn push_unique(out: &mut Vec<u64>, index: u64) {
    if !out.contains(&index) {
        out.push(index);
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    /// Total presence oracle: a finite pattern at the floor, a constant
    /// beyond it. Arbitrary, so gappy and adversarial maps are covered.
    struct Oracle {
        base: u64,
        pattern: Vec<bool>,
        beyond: bool,
    }

    impl Oracle {
        fn has(&self, index: u64) -> bool {
            index
                .checked_sub(self.base)
                .and_then(|off| usize::try_from(off).ok())
                .and_then(|off| self.pattern.get(off).copied())
                .unwrap_or(self.beyond)
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Outcome {
        Empty,
        Commit(u64),
    }

    /// Direct port of the sequential exponential-then-binary scan, recording
    /// its consult order.
    fn sequential_probing(base: u64, oracle: &Oracle, trace: &mut Vec<u64>) -> Outcome {
        let mut consult = |index: u64| {
            trace.push(index);
            oracle.has(index)
        };
        if !consult(base) {
            return Outcome::Empty;
        }
        let mut lo = base;
        let mut off: u64 = 1;
        let mut hi = loop {
            let Some(index) = base.checked_add(off) else {
                if consult(u64::MAX) {
                    return Outcome::Commit(u64::MAX);
                }
                break u64::MAX;
            };
            if consult(index) {
                if index == u64::MAX {
                    return Outcome::Commit(u64::MAX);
                }
                lo = index;
                off = off.saturating_mul(2);
            } else {
                break index;
            }
        };
        while let Some(gap) = hi.checked_sub(lo) {
            if gap <= 1 {
                break;
            }
            let Some(mid) = lo.checked_add(gap / 2) else {
                break;
            };
            if consult(mid) {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        Outcome::Commit(lo)
    }

    /// Direct port of the sequential stepwise scan, recording its consult
    /// order.
    fn sequential_linear(base: u64, oracle: &Oracle, trace: &mut Vec<u64>) -> Outcome {
        let mut consult = |index: u64| {
            trace.push(index);
            oracle.has(index)
        };
        if !consult(base) {
            return Outcome::Empty;
        }
        let mut last = base;
        loop {
            let Some(candidate) = last.checked_add(1) else {
                return Outcome::Commit(last);
            };
            if !consult(candidate) {
                return Outcome::Commit(last);
            }
            last = candidate;
        }
    }

    /// Pure round driver: faults are always answered, speculative probes may
    /// be adversarially dropped (left unanswered) per the mask.
    fn simulate(
        resolve: fn(u64, &Answers) -> Step,
        base: u64,
        oracle: &Oracle,
        width: NonZeroUsize,
        drops: &[bool],
        trace: &mut Vec<u64>,
    ) -> Outcome {
        let mut answers = Answers::new();
        let mut plan = Vec::new();
        let mut cursor = 0usize;
        loop {
            match resolve(base, &answers) {
                Step::Empty => return Outcome::Empty,
                Step::Commit { lo } => return Outcome::Commit(lo),
                Step::Fault(fault) => {
                    fault.plan(width, &mut plan);
                    plan.retain(|index| !answers.contains_key(index));
                    for &index in &plan {
                        let dropped =
                            index != fault.index() && drops.get(cursor).copied().unwrap_or(false);
                        cursor = cursor.wrapping_add(1);
                        if !dropped {
                            trace.push(index);
                            answers.insert(index, oracle.has(index));
                        }
                    }
                }
            }
        }
    }

    const ONE: NonZeroUsize = NonZeroUsize::MIN;

    fn width(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).unwrap()
    }

    #[test]
    fn empty_answers_fault_on_the_floor() {
        let answers = Answers::new();
        for resolve in [resolve_probing, resolve_linear] {
            let Step::Fault(fault) = resolve(7, &answers) else {
                panic!("expected a floor fault");
            };
            assert_eq!(fault.index(), 7);
        }
    }

    #[test]
    fn floor_fault_plans_the_ladder() {
        let answers = Answers::new();
        let Step::Fault(fault) = resolve_probing(0, &answers) else {
            panic!("expected a floor fault");
        };
        let mut plan = Vec::new();
        fault.plan(width(4), &mut plan);
        assert_eq!(plan, vec![0, 1, 2, 4]);
    }

    #[test]
    fn binary_fault_plans_the_decision_tree() {
        // Present through 8, absent at 16: the bracket is (8, 16), the
        // faulting midpoint 12, and speculation covers both child midpoints.
        let mut answers = Answers::new();
        for index in [0, 1, 2, 4, 8] {
            answers.insert(index, true);
        }
        answers.insert(16, false);
        let Step::Fault(fault) = resolve_probing(0, &answers) else {
            panic!("expected a binary fault");
        };
        assert_eq!(fault.index(), 12);
        let mut plan = Vec::new();
        fault.plan(width(3), &mut plan);
        assert_eq!(plan, vec![12, 10, 14]);
    }

    #[test]
    fn ladder_near_the_top_plans_the_top_slot_once() {
        let answers = Answers::new();
        let Step::Fault(fault) = resolve_probing(u64::MAX - 2, &answers) else {
            panic!("expected a floor fault");
        };
        let mut plan = Vec::new();
        fault.plan(width(8), &mut plan);
        assert_eq!(plan, vec![u64::MAX - 2, u64::MAX - 1, u64::MAX]);
    }

    proptest! {
        /// Any window width, any drop pattern: the windowed probing finder
        /// reaches the sequential scan's verdict.
        #[test]
        fn windowed_probing_matches_sequential(
            base in prop_oneof![0u64..=192, u64::MAX - 192..=u64::MAX],
            pattern in proptest::collection::vec(any::<bool>(), 0..96),
            beyond in any::<bool>(),
            w in 1usize..=16,
            drops in proptest::collection::vec(any::<bool>(), 0..256),
        ) {
            let oracle = Oracle { base, pattern, beyond };
            let expected = sequential_probing(base, &oracle, &mut Vec::new());
            let got = simulate(
                resolve_probing, base, &oracle, width(w), &drops, &mut Vec::new(),
            );
            prop_assert_eq!(got, expected);
        }

        /// Any window width, any drop pattern: the windowed stepwise finder
        /// reaches the sequential scan's verdict.
        #[test]
        fn windowed_linear_matches_sequential(
            base in prop_oneof![0u64..=192, u64::MAX - 192..=u64::MAX],
            pattern in proptest::collection::vec(any::<bool>(), 0..96),
            w in 1usize..=16,
            drops in proptest::collection::vec(any::<bool>(), 0..256),
        ) {
            // A constant-present tail would make the stepwise reference walk
            // the whole index space; the finite pattern keeps it bounded.
            let oracle = Oracle { base, pattern, beyond: false };
            let expected = sequential_linear(base, &oracle, &mut Vec::new());
            let got = simulate(
                resolve_linear, base, &oracle, width(w), &drops, &mut Vec::new(),
            );
            prop_assert_eq!(got, expected);
        }

        /// Width one issues exactly the sequential consult sequence.
        #[test]
        fn width_one_is_the_sequential_trace(
            base in prop_oneof![0u64..=192, u64::MAX - 192..=u64::MAX],
            pattern in proptest::collection::vec(any::<bool>(), 0..96),
            beyond in any::<bool>(),
        ) {
            let oracle = Oracle { base, pattern, beyond };

            let mut expected_trace = Vec::new();
            let expected = sequential_probing(base, &oracle, &mut expected_trace);
            let mut trace = Vec::new();
            let got = simulate(resolve_probing, base, &oracle, ONE, &[], &mut trace);
            prop_assert_eq!(got, expected);
            prop_assert_eq!(&trace, &expected_trace);

            let oracle = Oracle { beyond: false, ..oracle };
            let mut expected_trace = Vec::new();
            let expected = sequential_linear(base, &oracle, &mut expected_trace);
            let mut trace = Vec::new();
            let got = simulate(resolve_linear, base, &oracle, ONE, &[], &mut trace);
            prop_assert_eq!(got, expected);
            prop_assert_eq!(trace, expected_trace);
        }
    }
}
