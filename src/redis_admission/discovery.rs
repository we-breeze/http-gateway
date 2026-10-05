use std::sync::Mutex;
use std::time::Duration;

use tokio::time::Instant;

/// Requests share a cooldown after missing discovery or failed Redis admission.
pub(super) const RECORDER_RETRY_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Default)]
pub(super) struct RecorderDiscovery {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    retry_after: Option<Instant>,
    checking: bool,
}

pub(super) enum LookupUnavailable {
    CoolingDown,
    InProgress,
}

impl RecorderDiscovery {
    pub(super) fn try_lookup(&self) -> Result<Lookup<'_>, LookupUnavailable> {
        let mut state = self.state.lock().expect("recorder discovery lock poisoned");
        if state
            .retry_after
            .is_some_and(|deadline| Instant::now() < deadline)
        {
            return Err(LookupUnavailable::CoolingDown);
        }
        if state.checking {
            // Never queue application requests behind another admission attempt.
            return Err(LookupUnavailable::InProgress);
        }
        state.checking = true;
        Ok(Lookup {
            discovery: self,
            available: false,
        })
    }
}

pub(super) struct Lookup<'a> {
    discovery: &'a RecorderDiscovery,
    available: bool,
}

impl Lookup<'_> {
    pub(super) fn available(mut self) {
        self.available = true;
    }
}

impl Drop for Lookup<'_> {
    fn drop(&mut self) {
        let mut state = self
            .discovery
            .state
            .lock()
            .expect("recorder discovery lock poisoned");
        state.checking = false;
        // Cancellation from GET or SET acquisition timeouts also backs off.
        state.retry_after = (!self.available).then(|| Instant::now() + RECORDER_RETRY_INTERVAL);
    }
}
