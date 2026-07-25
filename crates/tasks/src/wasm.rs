//! Browser spawner.

use futures_util::future::{AbortHandle, Abortable};

use crate::{BoxFuture, Spawn, TaskHandle};

/// Spawner onto the browser event loop. An abort is cooperative: it lands
/// at the task's next poll.
#[derive(Debug, Clone, Copy, Default)]
pub struct WasmSpawner;

impl Spawn for WasmSpawner {
    fn spawn(&self, task: BoxFuture<'static, ()>) -> TaskHandle {
        let (abort, registration) = AbortHandle::new_pair();
        wasm_bindgen_futures::spawn_local(async move {
            let _ = Abortable::new(task, registration).await;
        });
        TaskHandle::new(move || abort.abort())
    }
}
