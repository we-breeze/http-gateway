//! Keep proxy tunnels under their accepting connection's permit and shutdown.

use std::future::Future;
use std::sync::{Arc, Mutex};

use tokio::task::{JoinError, JoinHandle};

#[derive(Clone, Default)]
pub(crate) struct UpgradeTasks(Arc<Mutex<Vec<AbortOnDrop>>>);

impl UpgradeTasks {
    pub(crate) fn spawn(&self, tunnel: impl Future<Output = ()> + Send + 'static) {
        self.0
            .lock()
            .expect("upgrade tasks lock poisoned")
            .push(AbortOnDrop(tokio::spawn(tunnel)));
    }

    pub(crate) async fn finish(&self) -> Result<(), JoinError> {
        // Hyper hands the upgraded socket off before the connection future
        // completes. The proxy registered every tunnel before returning 101.
        let tasks = std::mem::take(&mut *self.0.lock().expect("upgrade tasks lock poisoned"));
        for mut task in tasks {
            (&mut task.0).await?;
        }
        Ok(())
    }
}

struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        // Aborting the accepting connection at shutdown must also close the
        // tunnel's two sockets, including while an upgrade is still pending.
        self.0.abort();
    }
}
