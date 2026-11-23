//! The lifecycle of one command on a pipe connection (screen-play-rs#1735, increment B1).
//!
//! A pipe connection sends one command at a time and cannot take a written command back, so the
//! deadline and the cancellation of a call have to reach the layer that writes it. A
//! [`CommandLifecycle`] carries both from the API call that made the command, through the
//! handler, to the connection:
//!
//! * the **deadline** is absolute. It is stamped once, on the first poll of the API future
//!   (never at construction, never at handler receipt), from a [`LifecycleClock`], and nothing
//!   renews it: not an event, not a dequeue, not a flush.
//! * **cancellation** is a flag the caller's future sets when it is dropped unfinished. Setting it
//!   wakes the driver directly ([`DriverWaker`]), so it does not depend on getting a message into a
//!   full queue.
//!
//! Lifecycle metadata never enters the CDP JSON.
//!
//! One clock domain: production uses [`SystemClock`] (`std::time::Instant`), tests inject a
//! [`ManualClock`] whose instants are `std::time::Instant`s too. Every deadline of one pipe
//! connection is stamped and compared on that connection's clock.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use futures::task::AtomicWaker;

/// A future that resolves once a clock reaches a deadline.
pub type Sleep = Pin<Box<dyn Future<Output = ()> + Send>>;

/// The clock a pipe connection stamps and checks its deadlines with.
pub trait LifecycleClock: Send + Sync + fmt::Debug + 'static {
    /// The current instant on this clock.
    fn now(&self) -> Instant;
    /// A future that resolves once [`LifecycleClock::now`] is at or past `deadline`, and wakes
    /// its task then.
    fn sleep_until(&self, deadline: Instant) -> Sleep;
}

/// The production clock: `Instant::now()`, with `futures_timer` wake-ups.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl LifecycleClock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn sleep_until(&self, deadline: Instant) -> Sleep {
        let wait = deadline.saturating_duration_since(Instant::now());
        Box::pin(futures_timer::Delay::new(wait))
    }
}

/// A clock that only moves when told to, for deterministic deadline tests.
///
/// It starts at the `Instant` it was created at and advances by [`ManualClock::advance`] only;
/// advancing wakes every sleeper whose deadline it reached.
#[derive(Debug, Clone)]
pub struct ManualClock {
    inner: Arc<Mutex<ManualInner>>,
}

#[derive(Debug)]
struct ManualInner {
    start: Instant,
    now: Instant,
    sleepers: Vec<(Instant, Waker)>,
}

impl ManualClock {
    /// A clock standing at the current instant.
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            inner: Arc::new(Mutex::new(ManualInner {
                start: now,
                now,
                sleepers: Vec::new(),
            })),
        }
    }

    /// Move the clock forward by `by` and wake the sleepers it reached.
    pub fn advance(&self, by: Duration) {
        let due: Vec<Waker> = {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            inner.now += by;
            let now = inner.now;
            let (due, keep): (Vec<_>, Vec<_>) =
                inner.sleepers.drain(..).partition(|(at, _)| *at <= now);
            inner.sleepers = keep;
            due.into_iter().map(|(_, w)| w).collect()
        };
        for waker in due {
            waker.wake();
        }
    }

    /// Move the clock to `elapsed` after its start (never backwards).
    pub fn advance_to(&self, elapsed: Duration) {
        let current = self.elapsed();
        if elapsed > current {
            self.advance(elapsed - current);
        }
    }

    /// How far the clock has moved since it was created.
    pub fn elapsed(&self) -> Duration {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.now - inner.start
    }
}

impl Default for ManualClock {
    fn default() -> Self {
        Self::new()
    }
}

impl LifecycleClock for ManualClock {
    fn now(&self) -> Instant {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).now
    }

    fn sleep_until(&self, deadline: Instant) -> Sleep {
        Box::pin(ManualSleep {
            clock: self.inner.clone(),
            deadline,
        })
    }
}

struct ManualSleep {
    clock: Arc<Mutex<ManualInner>>,
    deadline: Instant,
}

impl Future for ManualSleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut inner = self.clock.lock().unwrap_or_else(|e| e.into_inner());
        if inner.now >= self.deadline {
            return Poll::Ready(());
        }
        // One entry per waker that is still waiting: replace an older registration of the same
        // task instead of growing the list on every poll.
        let deadline = self.deadline;
        inner
            .sleepers
            .retain(|(at, w)| !(*at == deadline && w.will_wake(cx.waker())));
        inner.sleepers.push((deadline, cx.waker().clone()));
        Poll::Pending
    }
}

/// Wakes the task that drives a pipe connection (the handler, or the connection's own owner).
pub(crate) type DriverWaker = Arc<AtomicWaker>;

/// The deadline and cancellation of one command, shared by its caller, the handler and the
/// connection.
#[derive(Debug)]
pub(crate) struct CommandLifecycle {
    deadline: Instant,
    cancelled: AtomicBool,
    driver: DriverWaker,
}

impl CommandLifecycle {
    pub(crate) fn new(deadline: Instant, driver: DriverWaker) -> Arc<Self> {
        Arc::new(Self {
            deadline,
            cancelled: AtomicBool::new(false),
            driver,
        })
    }

    pub(crate) fn deadline(&self) -> Instant {
        self.deadline
    }

    /// The caller stopped waiting: mark it and wake the driver, which settles the command.
    pub(crate) fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        self.driver.wake();
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    pub(crate) fn is_expired(&self, now: Instant) -> bool {
        now >= self.deadline
    }

    /// Neither cancelled nor past its deadline at `now`.
    pub(crate) fn is_live(&self, now: Instant) -> bool {
        !self.is_cancelled() && !self.is_expired(now)
    }
}

/// Cancels its lifecycle when dropped, unless disarmed: held by the caller's future, so dropping
/// an unfinished call reaches the connection without a message through any queue.
#[derive(Debug)]
pub(crate) struct CancelGuard(Option<Arc<CommandLifecycle>>);

impl CancelGuard {
    pub(crate) fn new(lifecycle: Arc<CommandLifecycle>) -> Self {
        Self(Some(lifecycle))
    }

    /// The call settled with a result of its own: a later drop cancels nothing.
    pub(crate) fn disarm(&mut self) {
        self.0 = None;
    }
}

impl Drop for CancelGuard {
    fn drop(&mut self) {
        if let Some(lifecycle) = self.0.take() {
            lifecycle.cancel();
        }
    }
}

/// What a pipe browser's API futures need to stamp their lifecycles: the connection's clock, its
/// configured request budget and its driver.
#[derive(Debug, Clone)]
pub(crate) struct PipeContext {
    pub(crate) clock: Arc<dyn LifecycleClock>,
    pub(crate) budget: Duration,
    pub(crate) driver: DriverWaker,
}

impl PipeContext {
    /// Stamp a lifecycle now. `requested` is capped by the configured budget.
    pub(crate) fn start(&self, requested: Option<Duration>) -> Arc<CommandLifecycle> {
        let budget = requested.map_or(self.budget, |r| r.min(self.budget));
        CommandLifecycle::new(self.clock.now() + budget, self.driver.clone())
    }
}

/// The lifecycle a command carries from its caller to the connection, if any. Without the
/// `pipe-transport` feature, and on a WebSocket connection, it carries nothing.
#[derive(Debug, Clone, Default)]
pub(crate) struct CallContext {
    pub(crate) lifecycle: Option<Arc<CommandLifecycle>>,
}

impl CallContext {
    pub(crate) fn of(lifecycle: Arc<CommandLifecycle>) -> Self {
        Self {
            lifecycle: Some(lifecycle),
        }
    }
}
