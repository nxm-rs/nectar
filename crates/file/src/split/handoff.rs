//! Bounded handoff from the thread pool back to the polling future.

use nectar_tasks::{Handoff, handoff};

/// Queue `job` on the pool, returning the handoff its reply arrives on.
///
/// Submission only enqueues, so neither building nor polling the caller's
/// future ever blocks on the pool. A panicking job is caught on the pool
/// thread, so the receiver sees a dropped job instead of a process abort.
pub(super) fn submit<T, F>(job: F) -> Handoff<T>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let (sender, receiver) = handoff();
    rayon::spawn(move || sender.run(job));
    receiver
}

#[cfg(test)]
mod tests {
    use super::*;

    use core::future::poll_fn;

    /// The pool path end to end: a panicking job reads as a drop.
    #[test]
    fn a_panicking_job_reads_as_a_drop() {
        let mut handoff = submit(|| -> u32 { panic!("job panicked") });
        let value = nectar_tasks::block_on(poll_fn(|cx| handoff.poll_recv(cx)));
        assert_eq!(value, None);
    }
}
