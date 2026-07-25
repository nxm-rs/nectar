//! Tokio spawner.

use crate::{BoxFuture, Spawn, TaskHandle};

/// Spawner onto the ambient tokio runtime. Spawning outside a runtime
/// panics, as `tokio::spawn` does.
#[derive(Debug, Clone, Copy, Default)]
pub struct TokioSpawner;

impl Spawn for TokioSpawner {
    fn spawn(&self, task: BoxFuture<'static, ()>) -> TaskHandle {
        let handle = ::tokio::spawn(task);
        TaskHandle::new(move || handle.abort())
    }
}

// Sanctioned tokio spawner tests: the test macro expands to `Runtime::block_on`.
#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use alloc::boxed::Box;
    use core::future::pending;

    use ::tokio::sync::oneshot;

    use super::TokioSpawner;
    use crate::Spawn;

    #[tokio::test]
    async fn runs_the_task() {
        let (sender, receiver) = oneshot::channel();
        let handle = TokioSpawner.spawn(Box::pin(async move {
            let _ = sender.send(());
        }));
        receiver.await.unwrap();
        drop(handle);
    }

    #[tokio::test]
    async fn drop_aborts_the_task() {
        let (sender, receiver) = oneshot::channel::<()>();
        let handle = TokioSpawner.spawn(Box::pin(async move {
            let _guard = sender;
            pending::<()>().await;
        }));
        drop(handle);
        assert!(receiver.await.is_err());
    }

    #[tokio::test]
    async fn detach_lets_the_task_finish() {
        let (sender, receiver) = oneshot::channel();
        TokioSpawner
            .spawn(Box::pin(async move {
                let _ = sender.send(());
            }))
            .detach();
        receiver.await.unwrap();
    }
}
