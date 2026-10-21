use std::pin::Pin;
use std::task::Context;
use std::time::Duration;

use futures::Future;
use futures_timer::Delay;

use crate::handler::REQUEST_TIMEOUT;

/// A background job run periodically.
#[derive(Debug)]
pub(crate) struct PeriodicJob {
    interval: Duration,
    delay: Delay,
}

impl PeriodicJob {
    /// Returns `true` if the job is currently not running but ready
    /// to be run, `false` otherwise.
    pub fn poll_ready(&mut self, cx: &mut Context<'_>) -> bool {
        if !Future::poll(Pin::new(&mut self.delay), cx).is_pending() {
            self.delay.reset(self.interval);
            // Poll the reset timer once so it holds this waker. Without it the next run happened
            // only if something else woke the handler: on a quiet connection a command, or a
            // target's initialisation, that timed out was never evicted (review F5).
            let _ = Future::poll(Pin::new(&mut self.delay), cx);
            return true;
        }
        false
    }
    pub fn new(interval: Duration) -> Self {
        Self {
            delay: Delay::new(interval),
            interval,
        }
    }
}

impl Default for PeriodicJob {
    fn default() -> Self {
        Self::new(Duration::from_millis(REQUEST_TIMEOUT))
    }
}
