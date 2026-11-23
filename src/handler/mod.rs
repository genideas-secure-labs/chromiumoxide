use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::time::{Duration, Instant};

use fnv::FnvHashMap;
use futures::channel::mpsc::Receiver;
use futures::channel::oneshot::Sender as OneshotSender;
use futures::stream::{Fuse, Stream, StreamExt};
use futures::task::{Context, Poll};

use crate::listeners::{EventListenerRequest, EventListeners};
use chromiumoxide_cdp::cdp::browser_protocol::browser::*;
use chromiumoxide_cdp::cdp::browser_protocol::target::*;
use chromiumoxide_cdp::cdp::events::CdpEvent;
use chromiumoxide_cdp::cdp::events::CdpEventMessage;
use chromiumoxide_types::{CallId, Message, Method, Response};
use chromiumoxide_types::{MethodId, Request as CdpRequest};
pub(crate) use page::{PageInner, page_channel_error};

use crate::cmd::{CallContext, CommandMessage, to_command_response};
use crate::conn::Connection;
use crate::error::{CdpError, Result};
use crate::handler::browser::BrowserContext;
use crate::handler::frame::FrameNavigationRequest;
use crate::handler::frame::{NavigationError, NavigationId, NavigationOk};
use crate::handler::job::PeriodicJob;
use crate::handler::session::Session;
use crate::handler::target::TargetEvent;
use crate::handler::target::{Target, TargetConfig};
use crate::handler::viewport::Viewport;
use crate::page::Page;

/// Standard timeout in MS
pub const REQUEST_TIMEOUT: u64 = 30_000;

pub mod browser;
pub mod commandfuture;
pub mod domworld;
pub mod emulation;
pub mod frame;
pub mod http;
pub mod httpfuture;
mod job;
pub mod network;
mod page;
mod session;
pub mod target;
pub mod target_message_future;
pub mod viewport;

#[cfg(test)]
mod scripted_peer_tests;

/// What reaches the handler after its pipe connection ended (screen-play-rs#1735).
///
/// In a running handler that window is a cross-thread race: intake is drained at the start of
/// each pass, so a message is still buffered at termination only if it arrived after that drain
/// and before the connection's end was read in the same pass. This test opens the window by
/// calling `terminate` directly with messages buffered, which is the state that race produces.
#[cfg(all(test, feature = "pipe-transport"))]
mod pipe_terminate_tests {
    use super::*;
    use crate::cmd::lifecycle::{ManualClock, PipeContext};
    use crate::conn::pipe::PipeSettings;
    use crate::error::{PipeClosed, PipeError};
    use futures::channel::mpsc::channel;
    use futures::channel::oneshot::channel as oneshot_channel;
    use std::sync::Arc;

    #[tokio::test]
    async fn messages_buffered_at_the_end_get_the_connections_reason() {
        let (conn_w, _peer_r) = tokio::io::duplex(1 << 16);
        let (_peer_w, conn_r) = tokio::io::duplex(1 << 16);
        let clock: Arc<dyn crate::cmd::lifecycle::LifecycleClock> = Arc::new(ManualClock::new());
        let driver = Arc::new(futures::task::AtomicWaker::new());
        let budget = Duration::from_secs(30);
        let conn = Connection::<CdpEventMessage>::pipe(
            conn_r,
            conn_w,
            PipeSettings {
                clock: clock.clone(),
                default_timeout: budget,
                driver: driver.clone(),
            },
        )
        .expect("pipe connection");
        let (mut tx, rx) = channel(8);
        let ctx = PipeContext {
            clock,
            budget,
            driver,
        };
        let mut handler =
            Handler::new_pipe(conn, rx, HandlerConfig::default(), ctx).expect("pipe handler");

        let (plain_tx, mut plain_rx) = oneshot_channel();
        tx.try_send(HandlerMessage::FetchTargets(plain_tx))
            .expect("buffered");
        let (wrapped_tx, mut wrapped_rx) = oneshot_channel();
        tx.try_send(HandlerMessage::WithContext(
            Box::new(HandlerMessage::CloseBrowser(wrapped_tx)),
            CallContext::default(),
        ))
        .expect("buffered");

        handler.terminate(PipeClosed::Malformed);

        let want = PipeError::Closed(PipeClosed::Malformed);
        match plain_rx.try_recv() {
            Ok(Some(Err(CdpError::Pipe(got)))) => assert_eq!(got, want),
            other => panic!("a buffered fetch_targets got {other:?}"),
        }
        match wrapped_rx.try_recv() {
            Ok(Some(Err(CdpError::Pipe(got)))) => assert_eq!(got, want),
            other => panic!("a buffered close got {other:?}"),
        }
        assert!(
            tx.try_send(HandlerMessage::GetPages(oneshot_channel().0))
                .is_err(),
            "the handler takes nothing more after its end"
        );
    }
}

/// How many messages one pass of a pipe handler reads before it yields (and wakes itself).
const PIPE_READ_BUDGET: usize = 64;

/// The handler that monitors the state of the chromium browser and drives all
/// the requests and events.
#[must_use = "streams do nothing unless polled"]
#[derive(Debug)]
pub struct Handler {
    /// Commands that are being processed and awaiting a response from the
    /// chromium instance together with the timestamp when the request
    /// started.
    pending_commands: FnvHashMap<CallId, (PendingRequest, MethodId, Instant)>,
    /// Connection to the browser instance
    from_browser: Fuse<Receiver<HandlerMessage>>,
    default_browser_context: BrowserContext,
    browser_contexts: HashSet<BrowserContext>,
    /// Used to loop over all targets in a consistent manner
    target_ids: Vec<TargetId>,
    /// The created and attached targets
    targets: HashMap<TargetId, Target>,
    /// Currently queued in navigations for targets
    navigations: FnvHashMap<NavigationId, NavigationRequest>,
    /// Keeps track of all the current active sessions
    ///
    /// There can be multiple sessions per target.
    sessions: HashMap<SessionId, Session>,
    /// The websocket connection to the chromium instance
    conn: Connection<CdpEventMessage>,
    /// Evicts timed out requests periodically
    evict_command_timeout: PeriodicJob,
    /// The internal identifier for a specific navigation
    next_navigation_id: usize,
    /// How this handler will configure targets etc,
    config: HandlerConfig,
    /// All registered event subscriptions
    event_listeners: EventListeners,
    /// Keeps track is the browser is closing
    closing: bool,
    /// Set on a pipe connection (screen-play-rs#1735): its clock, budget and driver.
    #[cfg(feature = "pipe-transport")]
    pipe: Option<crate::cmd::lifecycle::PipeContext>,
    /// The pipe connection ended and every waiter was settled: `None` from now on.
    #[cfg(feature = "pipe-transport")]
    ended: bool,
}

impl Handler {
    /// Create a new `Handler` that drives the connection and listens for
    /// messages on the receiver `rx`.
    pub(crate) fn new(
        mut conn: Connection<CdpEventMessage>,
        rx: Receiver<HandlerMessage>,
        config: HandlerConfig,
    ) -> Self {
        let discover = SetDiscoverTargetsParams::new(true);
        let _ = conn.submit_command(
            discover.identifier(),
            None,
            serde_json::to_value(discover).unwrap(),
        );
        Self::build(conn, rx, config)
    }

    /// A handler over a pipe connection. Target discovery is its first command, bounded by the
    /// configured budget like any other; if it cannot be admitted, construction fails and the
    /// connection (both endpoints) is dropped.
    #[cfg(feature = "pipe-transport")]
    pub(crate) fn new_pipe(
        mut conn: Connection<CdpEventMessage>,
        rx: Receiver<HandlerMessage>,
        config: HandlerConfig,
        pipe: crate::cmd::lifecycle::PipeContext,
    ) -> Result<Self> {
        let discover = SetDiscoverTargetsParams::new(true);
        conn.submit_command_with(
            discover.identifier(),
            None,
            serde_json::to_value(discover)?,
            CallContext::of(pipe.start(None)),
        )?;
        let mut handler = Self::build(conn, rx, config);
        handler.pipe = Some(pipe);
        Ok(handler)
    }

    fn build(
        conn: Connection<CdpEventMessage>,
        rx: Receiver<HandlerMessage>,
        config: HandlerConfig,
    ) -> Self {
        let browser_contexts = config
            .context_ids
            .iter()
            .map(|id| BrowserContext::from(id.clone()))
            .collect();

        Self {
            pending_commands: Default::default(),
            from_browser: rx.fuse(),
            default_browser_context: Default::default(),
            browser_contexts,
            target_ids: Default::default(),
            targets: Default::default(),
            navigations: Default::default(),
            sessions: Default::default(),
            conn,
            evict_command_timeout: PeriodicJob::new(config.request_timeout),
            next_navigation_id: 0,
            config,
            event_listeners: Default::default(),
            closing: false,
            #[cfg(feature = "pipe-transport")]
            pipe: None,
            #[cfg(feature = "pipe-transport")]
            ended: false,
        }
    }

    /// Whether this handler drives a pipe connection.
    fn is_pipe(&self) -> bool {
        #[cfg(feature = "pipe-transport")]
        {
            self.pipe.is_some()
        }
        #[cfg(not(feature = "pipe-transport"))]
        {
            false
        }
    }

    /// The handler's notion of now: the pipe connection's clock, so every deadline of a pipe
    /// connection is in one clock domain; `Instant::now()` otherwise.
    fn clock_now(&self) -> Instant {
        #[cfg(feature = "pipe-transport")]
        if let Some(pipe) = &self.pipe {
            return pipe.clock.now();
        }
        Instant::now()
    }

    /// A lifecycle for a command the handler issues on its own (target initialization,
    /// discovery, a refused session's detach): bounded by the configured budget, stamped now.
    fn internal_ctx(&self) -> CallContext {
        #[cfg(feature = "pipe-transport")]
        if let Some(pipe) = &self.pipe {
            return CallContext::of(pipe.start(None));
        }
        CallContext::default()
    }

    /// `ctx` if it carries a lifecycle, otherwise a handler-owned one.
    fn ctx_or_internal(&self, ctx: CallContext) -> CallContext {
        #[cfg(feature = "pipe-transport")]
        if ctx.lifecycle.is_some() {
            return ctx;
        }
        let _ = ctx;
        self.internal_ctx()
    }

    /// Admit a command to the connection. On a pipe a refusal is typed and wrote nothing; on a
    /// WebSocket this is `Connection::submit_command`.
    fn submit(
        &mut self,
        method: MethodId,
        session_id: Option<SessionId>,
        params: serde_json::Value,
        ctx: CallContext,
    ) -> Result<CallId> {
        #[cfg(feature = "pipe-transport")]
        if self.conn.is_pipe() {
            return self
                .conn
                .submit_command_with(method, session_id, params, ctx);
        }
        let _ = ctx;
        Ok(self.conn.submit_command(method, session_id, params)?)
    }

    /// Return the target with the matching `target_id`
    pub fn get_target(&self, target_id: &TargetId) -> Option<&Target> {
        self.targets.get(target_id)
    }

    /// Iterator over all currently attached targets
    pub fn targets(&self) -> impl Iterator<Item = &Target> + '_ {
        self.targets.values()
    }

    /// The default Browser context
    pub fn default_browser_context(&self) -> &BrowserContext {
        &self.default_browser_context
    }

    /// Iterator over all currently available browser contexts
    pub fn browser_contexts(&self) -> impl Iterator<Item = &BrowserContext> + '_ {
        self.browser_contexts.iter()
    }

    /// received a response to a navigation request like `Page.navigate`
    fn on_navigation_response(&mut self, id: NavigationId, resp: Response) {
        if let Some(nav) = self.navigations.remove(&id) {
            match nav {
                NavigationRequest::Navigate(mut nav) => {
                    if resp.error.is_some() {
                        // The navigation was refused: no page load follows an error answer, so
                        // it completes now, with that error. It used to be stored while the
                        // navigation waited for a load that never comes, until the 30 s timeout
                        // (winclaw#2121, U-3 fix 3). The frame's watcher is dropped too, or the
                        // page's next navigation would queue behind it until that timeout.
                        for target in self.targets.values_mut() {
                            target.frame_manager_mut().abort_navigation(id);
                        }
                        let _ = nav.tx.send(Ok(resp));
                    } else if nav.navigated {
                        let _ = nav.tx.send(Ok(resp));
                    } else {
                        nav.set_response(resp);
                        self.navigations
                            .insert(id, NavigationRequest::Navigate(nav));
                    }
                }
            }
        }
    }

    /// A navigation has finished.
    fn on_navigation_lifecycle_completed(&mut self, res: Result<NavigationOk, NavigationError>) {
        match res {
            Ok(ok) => {
                let id = *ok.navigation_id();
                if let Some(nav) = self.navigations.remove(&id) {
                    match nav {
                        NavigationRequest::Navigate(mut nav) => {
                            if let Some(resp) = nav.response.take() {
                                let _ = nav.tx.send(Ok(resp));
                            } else {
                                nav.set_navigated();
                                self.navigations
                                    .insert(id, NavigationRequest::Navigate(nav));
                            }
                        }
                    }
                }
            }
            Err(err) => {
                if let Some(nav) = self.navigations.remove(err.navigation_id()) {
                    match nav {
                        NavigationRequest::Navigate(nav) => {
                            let _ = nav.tx.send(Err(err.into()));
                        }
                    }
                }
            }
        }
    }

    /// Received a response to a request.
    fn on_response(&mut self, resp: Response) {
        if let Some((req, method, _)) = self.pending_commands.remove(&resp.id) {
            match req {
                PendingRequest::CreateTarget(tx) => {
                    match to_command_response::<CreateTargetParams>(resp, method) {
                        Ok(resp) => {
                            if let Some(target) = self.targets.get_mut(&resp.target_id) {
                                // move the sender to the target that sends its page once
                                // initialized
                                target.set_initiator(tx);
                            } else {
                                // TODO can this even happen?
                                panic!("Created target not present")
                            }
                        }
                        Err(err) => {
                            let _ = tx.send(Err(err)).ok();
                        }
                    }
                }
                PendingRequest::GetTargets(tx) => {
                    match to_command_response::<GetTargetsParams>(resp, method) {
                        Ok(resp) => {
                            let targets: Vec<TargetInfo> = resp.result.target_infos;
                            let results = targets.clone();
                            for target_info in targets {
                                let event: EventTargetCreated = EventTargetCreated { target_info };
                                self.on_target_created(event);
                                // Do NOT attach here. `Target::poll` already sends
                                // `Target.attachToTarget { flatten: true }` for every
                                // page target it tracks, so this was a second attach on
                                // the same target — and an UN-flattened one. Chrome then
                                // delivers that session's events wrapped in
                                // `Target.receivedMessageFromTarget`, which this crate
                                // does not handle, so they were dropped; and which of
                                // the two sessions the `Target` ended up bound to (via
                                // `on_attached_to_target`) was a race.
                                //
                                // When the race was lost, `Page.enable` came back
                                // "Session with given id not found." — and because
                                // `Target::on_response` advances the init `CommandChain`
                                // regardless of the response's error, the target still
                                // reported `Initialized`. No Page/lifecycle events ever
                                // reached its `FrameManager` after that, so a later
                                // `Page.goto` never had its navigation marked complete
                                // and failed with `CdpError::Timeout` after 30s — even
                                // though the page had navigated perfectly well.
                                //
                                // Measured on Linux/Chromium 141, screen-play-rs #1026
                                // (`Browser::connect` to a Chrome launched with a URL in
                                // argv, then `page.goto(other_url)`), 8 runs per cell:
                                //   as-shipped .......................... 4/8 ok
                                //   skip the clobber in on_target_created  0/8 ok
                                //   attach here, but flattened ........... 3/8 ok
                                //   no attach here (this change) ......... 8/8 ok
                            }

                            let _ = tx.send(Ok(results)).ok();
                        }
                        Err(err) => {
                            let _ = tx.send(Err(err)).ok();
                        }
                    }
                }
                PendingRequest::Navigate(id) => {
                    self.on_navigation_response(id, resp);
                }
                PendingRequest::ExternalCommand(tx) => {
                    let _ = tx.send(Ok(resp)).ok();
                }
                PendingRequest::Detach(target_id, session, decision, tx) => {
                    let result = match resp.error {
                        Some(err) => {
                            self.on_detach_failed(&target_id, &session, decision);
                            Err(CdpError::Chrome(err))
                        }
                        None => Ok(()),
                    };
                    let _ = tx.send(result);
                }
                PendingRequest::InternalCommand(target_id, session, attempt) => {
                    if let Some(target) = self.targets.get_mut(&target_id) {
                        // A command sent on a session the target no longer has
                        // answers for that old session, not the current
                        // initialization (screen-play-rs#1041, codex R1); an
                        // attach answers for its own attempt (codex R4).
                        if target.answers_current(session.as_deref(), attempt) {
                            target.on_response(resp, method.as_ref());
                        }
                    }
                }
                PendingRequest::CloseBrowser(tx) => {
                    self.closing = true;
                    let _ = tx.send(Ok(CloseReturns {})).ok();
                }
            }
        }
    }

    /// Submit a command initiated via channel
    pub(crate) fn submit_external_command(
        &mut self,
        msg: CommandMessage,
        now: Instant,
    ) -> Result<()> {
        let ctx = msg.call_context();
        let CommandMessage {
            method,
            session_id,
            params,
            sender,
            ..
        } = msg;
        match self.submit(method.clone(), session_id, params, ctx) {
            Ok(call_id) => {
                self.pending_commands.insert(
                    call_id,
                    (PendingRequest::ExternalCommand(sender), method, now),
                );
                Ok(())
            }
            // A pipe's refusal is routine: the caller gets it, the handler goes on.
            Err(err) if self.is_pipe() => {
                let _ = sender.send(Err(err));
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    /// `attempt`: the attach attempt, for a target's `Target.attachToTarget`.
    pub(crate) fn submit_internal_command(
        &mut self,
        target_id: TargetId,
        req: CdpRequest,
        attempt: Option<u64>,
        now: Instant,
        ctx: CallContext,
    ) -> Result<()> {
        let session = req.session_id.clone();
        let call_id = self.submit(
            req.method.clone(),
            req.session_id.map(Into::into),
            req.params,
            ctx,
        )?;
        self.pending_commands.insert(
            call_id,
            (
                PendingRequest::InternalCommand(target_id, session, attempt),
                req.method,
                now,
            ),
        );
        Ok(())
    }

    fn submit_fetch_targets(
        &mut self,
        tx: OneshotSender<Result<Vec<TargetInfo>>>,
        now: Instant,
        ctx: CallContext,
    ) {
        let msg = GetTargetsParams { filter: None };
        let method = msg.identifier();
        let ctx = self.ctx_or_internal(ctx);
        let submitted = serde_json::to_value(msg)
            .map_err(CdpError::from)
            .and_then(|params| self.submit(method.clone(), None, params, ctx));
        match submitted {
            Ok(call_id) => {
                self.pending_commands
                    .insert(call_id, (PendingRequest::GetTargets(tx), method, now));
            }
            Err(err) => {
                let _ = tx.send(Err(err));
            }
        }
    }

    /// Send the Request over to the server and store its identifier to handle
    /// the response once received. A refusal completes the navigation with it and releases
    /// the frame's navigation slot, so the page's next navigation is not queued behind it.
    fn submit_navigation(
        &mut self,
        target: &mut Target,
        id: NavigationId,
        req: CdpRequest,
        now: Instant,
    ) {
        let ctx = match self.navigations.get(&id) {
            Some(NavigationRequest::Navigate(nav)) => nav.ctx.clone(),
            None => CallContext::default(),
        };
        let ctx = self.ctx_or_internal(ctx);
        match self.submit(
            req.method.clone(),
            req.session_id.map(Into::into),
            req.params,
            ctx,
        ) {
            Ok(call_id) => {
                self.pending_commands
                    .insert(call_id, (PendingRequest::Navigate(id), req.method, now));
            }
            Err(err) => {
                target.frame_manager_mut().abort_navigation(id);
                if let Some(NavigationRequest::Navigate(nav)) = self.navigations.remove(&id) {
                    let _ = nav.tx.send(Err(err));
                }
            }
        }
    }

    fn submit_close(
        &mut self,
        tx: OneshotSender<Result<CloseReturns>>,
        now: Instant,
        ctx: CallContext,
    ) {
        let close_msg = CloseParams::default();
        let method = close_msg.identifier();
        let ctx = self.ctx_or_internal(ctx);
        let submitted = serde_json::to_value(close_msg)
            .map_err(CdpError::from)
            .and_then(|params| self.submit(method.clone(), None, params, ctx));
        match submitted {
            Ok(call_id) => {
                self.pending_commands
                    .insert(call_id, (PendingRequest::CloseBrowser(tx), method, now));
            }
            Err(err) => {
                let _ = tx.send(Err(err));
            }
        }
    }

    /// Process a message received by the target's page via channel
    fn on_target_message(&mut self, target: &mut Target, msg: CommandMessage, now: Instant) {
        // if let some
        if msg.is_navigation() {
            let ctx = msg.call_context();
            let (req, tx) = msg.split();
            let id = self.next_navigation_id();
            target.goto(FrameNavigationRequest::new(id, req));
            self.navigations.insert(
                id,
                NavigationRequest::Navigate(NavigationInProgress::new(tx, ctx)),
            );
        } else {
            // A refusal reaches the command's own caller inside; only a WebSocket serialization
            // failure comes back here, and it never did (unchanged).
            let _ = self.submit_external_command(msg, now);
        }
    }

    /// An identifier for queued `NavigationRequest`s.
    fn next_navigation_id(&mut self) -> NavigationId {
        let id = NavigationId(self.next_navigation_id);
        self.next_navigation_id = self.next_navigation_id.wrapping_add(1);
        id
    }

    /// Create a new page and send it to the receiver when ready
    ///
    /// First a `CreateTargetParams` is send to the server, this will trigger
    /// `EventTargetCreated` which results in a new `Target` being created.
    /// Once the response to the request is received the initialization process
    /// of the target kicks in. This triggers a queue of initialization requests
    /// of the `Target`, once those are all processed and the `url` fo the
    /// `CreateTargetParams` has finished loading (The `Target`'s `Page` is
    /// ready and idle), the `Target` sends its newly created `Page` as response
    /// to the initiator (`tx`) of the `CreateTargetParams` request.
    fn create_page(
        &mut self,
        params: CreateTargetParams,
        tx: OneshotSender<Result<Page>>,
        ctx: CallContext,
    ) {
        match url::Url::parse(&params.url) {
            Ok(_) => {
                let method = params.identifier();
                let ctx = self.ctx_or_internal(ctx);
                match serde_json::to_value(params) {
                    Ok(params) => match self.submit(method.clone(), None, params, ctx) {
                        Ok(call_id) => {
                            let now = self.clock_now();
                            self.pending_commands
                                .insert(call_id, (PendingRequest::CreateTarget(tx), method, now));
                        }
                        Err(err) => {
                            let _ = tx.send(Err(err)).ok();
                        }
                    },
                    Err(err) => {
                        let _ = tx.send(Err(err.into())).ok();
                    }
                }
            }
            Err(err) => {
                let _ = tx.send(Err(err.into())).ok();
            }
        }
    }

    /// Process an incoming event read from the websocket
    fn on_event(&mut self, event: CdpEventMessage) {
        if let Some(ref session_id) = event.session_id {
            if let Some(session) = self.sessions.get(session_id.as_str()) {
                if let Some(target) = self.targets.get_mut(session.target_id()) {
                    if !target.accepts_event_from(session_id) {
                        // A superseded session's late event (#1041, codex R7).
                        return;
                    }
                    // A target's automatic attachment reports its children on its own session.
                    let child = match &event.params {
                        CdpEvent::TargetAttachedToTarget(ev)
                            if ev.target_info.r#type == "iframe" =>
                        {
                            Some(Ok((**ev).clone()))
                        }
                        CdpEvent::TargetDetachedFromTarget(ev) => Some(Err(ev.session_id.clone())),
                        _ => None,
                    };
                    target.on_event(event);
                    match child {
                        Some(Ok(attached)) => self.on_child_attached(attached),
                        Some(Err(detached)) => self.on_child_detached(&detached),
                        None => {}
                    }
                    return;
                }
            }
        }
        let CdpEventMessage { params, method, .. } = event;
        match params.clone() {
            CdpEvent::TargetTargetCreated(ev) => self.on_target_created(*ev),
            CdpEvent::TargetAttachedToTarget(ev) => self.on_attached_to_target(ev),
            CdpEvent::TargetTargetDestroyed(ev) => self.on_target_destroyed(ev),
            CdpEvent::TargetDetachedFromTarget(ev) => self.on_detached_from_target(ev),
            _ => {}
        }
        chromiumoxide_cdp::consume_event!(match params {
            |ev| self.event_listeners.start_send(ev),
            |json| { let _ = self.event_listeners.try_send_custom(&method, json);}
        });
    }

    /// Fired when a new target was created on the chromium instance
    ///
    /// Creates a new `Target` instance and keeps track of it
    fn on_target_created(&mut self, event: EventTargetCreated) {
        // Target discovery is not idempotent unless this returns early.
        // `Browser::fetch_targets()` funnels every `TargetInfo` it gets back
        // through here, so a second fetch used to REPLACE a live `Target` with
        // a fresh one in `TargetInit::AttachToTarget` — throwing away its
        // session, its `Page`, and its `FrameManager`, and opening yet another
        // session on the same target. Screen-play re-fetches on ordinary paths
        // (`connect`, `find_page_by_url`, `active_page`, `wait_for_popup`), so
        // that was reachable in normal use, not just at startup. Measured with
        // a fake CDP server: two `Target.getTargets` responses produced two
        // `Target.attachToTarget` calls for the same target before this guard,
        // one after.
        //
        // "Already tracked" is NOT the same as "healthy", so the guard is not
        // simply `contains_key`. CDP may detach a session "for any reason", and
        // `on_detached_from_target` clears `session_id` for the current one —
        // leaving a target that finished initializing and can no longer make
        // progress, because `Target::poll` needs a session. Re-discovery was
        // the (accidental) way back from that, so it stays the way back: a
        // target that is `Initialized` with no session is replaced. Everything
        // else is left alone, including an attach still in flight
        // (`InitializingFrame` with no session yet) — replacing that is exactly
        // the double-attach this guard exists to prevent.
        //
        // The init-failure path does not need this: `on_initialization_failed`
        // moves the target to `Closing` and sends `Target.closeTarget`, and the
        // resulting `targetDestroyed` removes the entry outright.
        //
        // screen-play-rs#1041 changed the second half: discovery no longer
        // REPLACES a target that lost its session — replacing threw away its
        // network settings, listeners and page channel, and could not tell an
        // intentional detach from a lost session. A tracked target is never
        // replaced. If it should be attached and has no session, discovery
        // starts the same in-place re-attach a lost session starts (rationed
        // by the recovery budget); a target detached on purpose
        // (`Browser::detach_target`) is left alone.
        let now = self.clock_now();
        if let Some(existing) = self.targets.get_mut(&event.target_info.target_id) {
            // Do not let its metadata freeze at the first snapshot — this
            // crate does not handle `Target.targetInfoChanged`, so discovery
            // is the only refresh.
            existing.on_rediscovered(&event.target_info, now);
            return;
        }
        let browser_ctx = event
            .target_info
            .browser_context_id
            .clone()
            .map(BrowserContext::from)
            .filter(|id| self.browser_contexts.contains(id))
            .unwrap_or_else(|| self.default_browser_context.clone());
        #[allow(unused_mut)]
        let mut target = Target::new(
            event.target_info,
            TargetConfig {
                ignore_https_errors: self.config.ignore_https_errors,
                request_timeout: self.config.request_timeout,
                viewport: self.config.viewport.clone(),
                request_intercept: self.config.request_intercept,
                cache_enabled: self.config.cache_enabled,
                stealth_mode: self.config.stealth_mode,
                session_recovery: self.config.session_recovery,
            },
            browser_ctx,
        );
        #[cfg(feature = "pipe-transport")]
        if let Some(pipe) = &self.pipe {
            target.set_pipe_context(pipe.clone());
        }
        // Only schedule an ID that is not scheduled yet. Reaching here for a
        // target that already has an entry means the revival path above fell
        // through, and that entry's ID is already in `target_ids` — pushing
        // unconditionally would leave a DUPLICATE, permanently: the poll loop
        // re-pushes whatever it finds, so every later tick would poll that
        // target once per copy and every further detach/revival would add
        // another. Found by review round 7.
        //
        // The gate is `target_ids` itself, and that correction is round 8's.
        // Round 7 gated on `self.targets` and asserted the two were the same
        // set. They are NOT: `on_target_destroyed` removes from `targets`
        // ONLY, and the id stays in `target_ids` until the poll loop next
        // fails to find it and drops it. So after any destruction the two
        // legitimately diverge — and a target destroyed and rediscovered
        // before that sweep would have pushed a duplicate anyway (release) or
        // tripped the assertion (debug). Asking the scheduler's own list
        // whether this id is scheduled needs no cross-container invariant, and
        // it closes the destroy-then-rediscover case that predates round 7.
        let target_id = target.target_id().clone();
        if !self.target_ids.contains(&target_id) {
            self.target_ids.push(target_id.clone());
        }
        self.targets.insert(target_id.clone(), target);
        // The property that actually matters, checked after the fact rather
        // than assumed: a duplicate here is permanent, because the poll loop
        // re-pushes whatever it finds. `debug_assert!` so it runs in every
        // downstream debug test build — screen-play-rs#1026's protocol tests
        // drive this path on every `cargo test`, and the defect is invisible
        // from the wire, so a comment could never be more than a claim.
        debug_assert_eq!(
            self.target_ids
                .iter()
                .filter(|id| **id == target_id)
                .count(),
            1,
            "target {target_id:?} must appear in `target_ids` exactly once"
        );
    }

    /// A new session is attached to a target
    fn on_attached_to_target(&mut self, event: Box<EventAttachedToTarget>) {
        let session = Session::new(event.session_id.clone(), event.target_info.target_id);
        let mut refuse = false;
        if let Some(target) = self.targets.get_mut(session.target_id()) {
            if !target.desired_attached() {
                // Detached on purpose while this attach was in flight
                // (screen-play-rs#1041, codex R1): end the session it brought
                // instead of adopting it.
                target.abandon_attachment();
                refuse = true;
            } else if target.expects_session() {
                target.set_session_id(session.session_id().clone())
            } else {
                // A session nothing is waiting for — a timed-out attach that
                // answered after recovery gave up: adopting it would expose a
                // page whose session never ran its initialization (codex R4).
                refuse = true;
            }
        }
        if refuse {
            let params = crate::cdp::browser_protocol::target::DetachFromTargetParams::builder()
                .session_id(session.session_id().clone())
                .build();
            let target_id = session.target_id().clone();
            let now = self.clock_now();
            let ctx = self.internal_ctx();
            if let Err(err) = self.submit_internal_command(
                target_id,
                CdpRequest {
                    method: params.identifier(),
                    session_id: None,
                    params: serde_json::to_value(params).unwrap(),
                },
                None,
                now,
                ctx,
            ) {
                // Nothing waits on this cleanup; the stray session stays until the peer ends it.
                tracing::debug!(%err, "detach of a refused session was not sent");
            }
        }
        self.sessions.insert(event.session_id, session);
    }

    /// An `iframe` target was auto-attached under a tracked target's session: an out-of-process
    /// frame. It is tracked as a target of its own, bound to the session it came with and
    /// initialised like the root, so its events reach it and a frame nested in it attaches too.
    /// It used to be sent `Runtime.runIfWaitingForDebugger` (by its parent, which still does)
    /// and nothing else: no domain enabled, so no event ever came from it, and no automatic
    /// attachment, so a frame nested in it never attached (winclaw#2121, U-3 fix 4).
    ///
    /// Not a page: `Browser::pages` still lists pages only. `Browser::get_page` with its id gives
    /// a handle on its own session.
    ///
    /// Not in stealth mode: there a child is left as before, with no domain enabled.
    fn on_child_attached(&mut self, event: EventAttachedToTarget) {
        // Stealth keeps the old behaviour: no domain is enabled on a child. The out-of-process
        // frames are exactly where captcha and anti-bot scripts run, and `Runtime.enable` there
        // is the fingerprint stealth exists to avoid (review F1). Stealth also attaches children
        // without pausing them (`waitForDebuggerOnStart: false`), so none waits to be resumed.
        if self.config.stealth_mode {
            return;
        }
        let target_id = event.target_info.target_id.clone();
        // A repeated attachment on the same session changes nothing. Anything else tracked under
        // this id — the same frame on a new session, or a target discovery reported and nobody
        // polls — is replaced, so the child starts from a clean state on its current session.
        if let Some(existing) = self.targets.get(&target_id) {
            if existing.is_child() && existing.session_id() == Some(&event.session_id) {
                return;
            }
            if let Some(old) = existing.session_id().cloned() {
                self.sessions.remove(&old);
            }
        }
        let browser_ctx = event
            .target_info
            .browser_context_id
            .clone()
            .map(BrowserContext::from)
            .filter(|id| self.browser_contexts.contains(id))
            .unwrap_or_else(|| self.default_browser_context.clone());
        #[allow(unused_mut)]
        let mut target = Target::new_child(
            event.target_info,
            TargetConfig {
                ignore_https_errors: self.config.ignore_https_errors,
                request_timeout: self.config.request_timeout,
                // The viewport is the page's.
                viewport: None,
                // A request paused on the child's session would reach no listener of the page's.
                request_intercept: false,
                cache_enabled: self.config.cache_enabled,
                stealth_mode: self.config.stealth_mode,
                session_recovery: self.config.session_recovery,
            },
            browser_ctx,
            event.session_id.clone(),
        );
        #[cfg(feature = "pipe-transport")]
        if let Some(pipe) = &self.pipe {
            target.set_pipe_context(pipe.clone());
        }
        if !self.target_ids.contains(&target_id) {
            self.target_ids.push(target_id.clone());
        }
        self.targets.insert(target_id.clone(), target);
        self.sessions.insert(
            event.session_id.clone(),
            Session::new(event.session_id, target_id),
        );
    }

    /// A session was detached, reported on its parent's session. If it is an auto-attached
    /// child's, the child is gone: its session and its target are dropped (a new attachment
    /// brings it back). Any other session is left to `on_detached_from_target`'s browser-level
    /// path, as before.
    fn on_child_detached(&mut self, session_id: &SessionId) {
        let child = self
            .sessions
            .get(session_id)
            .map(|session| session.target_id().clone())
            .filter(|id| {
                self.targets
                    .get(id)
                    .is_some_and(|t| t.is_child() && t.session_id() == Some(session_id))
            });
        if let Some(target_id) = child {
            self.sessions.remove(session_id);
            self.targets.remove(&target_id);
        }
    }

    /// The session was detached from target.
    /// Can be issued multiple times per target if multiple session have been
    /// attached to it.
    fn on_detached_from_target(&mut self, event: EventDetachedFromTarget) {
        let now = self.clock_now();
        // remove the session
        if let Some(session) = self.sessions.remove(&event.session_id) {
            if let Some(target) = self.targets.get_mut(session.target_id()) {
                // A single target can have multiple attached sessions
                // simultaneously. Only the CURRENT session is cached
                // on `target.session_id` / `target.page`; if the event
                // is for an older/secondary session, leave the live
                // session intact. Without this guard, detaching an
                // older session would wipe the cached handle for the
                // session that's still live, and the next `get_page`
                // would spuriously return `CdpError::NotFound`.
                let is_current = target.session_id() == Some(&event.session_id);
                if is_current {
                    // screen-play-rs#1041: the target decides what follows —
                    // re-attach in place if it should stay attached, stay
                    // detached if it was detached on purpose.
                    target.on_session_lost(now);
                    // Also drop the cached PageHandle so any caller
                    // that races a `Target.attachToTarget` reply against
                    // the matching `attachedToTarget` event window —
                    // the handler drains `HandlerMessage` commands
                    // (`GetPage`) before connection events in a single
                    // poll tick, so it's reachable — receives a clean
                    // `CdpError::NotFound` instead of a `PageHandle`
                    // bound to the dead session (every command would
                    // otherwise come back as `-32001: Session with
                    // given id not found`). Callers treat `NotFound`
                    // as a retryable signal until the next attached
                    // event lands.
                    target.clear_page();
                }
            }
        }
    }

    /// Fired when the target was destroyed in the browser
    fn on_target_destroyed(&mut self, event: EventTargetDestroyed) {
        if let Some(target) = self.targets.remove(&event.target_id) {
            // TODO shutdown?
            if let Some(session) = target.session_id() {
                self.sessions.remove(session);
            }
        }
    }

    /// House keeping of commands
    ///
    /// Remove all commands where `now` > `timestamp of command starting point +
    /// request timeout` and notify the senders that their request timed out.
    /// Record that `target_id` should stay detached and end its session on the
    /// browser's own session (screen-play-rs#1041). Answers `tx` when Chrome
    /// does; a target without a session is answered at once.
    fn detach_target(
        &mut self,
        target_id: TargetId,
        tx: OneshotSender<Result<()>>,
        now: Instant,
        ctx: CallContext,
    ) {
        let ctx = self.ctx_or_internal(ctx);
        let Some(target) = self.targets.get_mut(&target_id) else {
            let _ = tx.send(Ok(()));
            return;
        };
        let Some((session, decision)) = target.begin_detach() else {
            let _ = tx.send(Ok(()));
            return;
        };
        let params = crate::cdp::browser_protocol::target::DetachFromTargetParams::builder()
            .session_id(session.clone())
            .build();
        let method = params.identifier();
        match self.submit(
            method.clone(),
            None,
            serde_json::to_value(params).unwrap(),
            ctx,
        ) {
            Ok(call_id) => {
                self.pending_commands.insert(
                    call_id,
                    (
                        PendingRequest::Detach(
                            target_id,
                            session.as_ref().to_string(),
                            decision,
                            tx,
                        ),
                        method,
                        now,
                    ),
                );
            }
            Err(err) => {
                self.on_detach_failed(&target_id, session.as_ref(), decision);
                let _ = tx.send(Err(err));
            }
        }
    }

    /// An intentional detach of `session` failed: if that session is still
    /// the target's and no newer decision replaced this one, the target is
    /// attached after all (codex R9: an overlapping detach that succeeded
    /// must not be undone by one that failed because the session was gone).
    /// Returns whether the target must be polled again.
    fn on_detach_failed(&mut self, target_id: &TargetId, session: &str, decision: u64) -> bool {
        self.targets
            .get_mut(target_id)
            .is_some_and(|target| target.on_detach_failed(session, decision))
    }

    /// Returns whether a target was told its attach timed out: it must be
    /// polled again, which nothing else may trigger on an idle connection.
    fn evict_timed_out_commands(&mut self, now: Instant) -> bool {
        let mut marked = false;
        let timed_out = self
            .pending_commands
            .iter()
            .filter(|(_, (_, _, timestamp))| now > (*timestamp + self.config.request_timeout))
            .map(|(k, _)| *k)
            .collect::<Vec<_>>();
        for call in timed_out {
            if let Some((req, _, _)) = self.pending_commands.remove(&call) {
                if self.fail_request(req, || CdpError::Timeout) {
                    marked = true;
                }
            }
        }
        marked
    }

    /// Settle a request that will get no answer with `err`. Returns whether a target must be
    /// polled again to act on it.
    fn fail_request(&mut self, req: PendingRequest, err: impl Fn() -> CdpError) -> bool {
        let mut marked = false;
        match req {
            PendingRequest::CreateTarget(tx) => {
                let _ = tx.send(Err(err()));
            }
            PendingRequest::GetTargets(tx) => {
                let _ = tx.send(Err(err()));
            }
            PendingRequest::Navigate(nav) => {
                if let Some(nav) = self.navigations.remove(&nav) {
                    match nav {
                        NavigationRequest::Navigate(nav) => {
                            let _ = nav.tx.send(Err(err()));
                        }
                    }
                }
            }
            PendingRequest::ExternalCommand(tx) => {
                let _ = tx.send(Err(err()));
            }
            PendingRequest::Detach(target_id, session, decision, tx) => {
                // A restarted initialization needs a poll, which an
                // idle connection would not bring (codex R10).
                if self.on_detach_failed(&target_id, &session, decision) {
                    marked = true;
                }
                let _ = tx.send(Err(err()));
            }
            PendingRequest::InternalCommand(target_id, _, Some(attempt)) => {
                // An attach that never answered leaves no session to
                // time out on; tell the target, or it counts as
                // attaching forever (screen-play-rs#1041, codex R3) —
                // unless a newer attempt replaced it (codex R4).
                if let Some(target) = self.targets.get_mut(&target_id) {
                    if target.on_attach_timed_out(attempt) {
                        marked = true;
                    }
                }
            }
            PendingRequest::InternalCommand(..) => {}
            PendingRequest::CloseBrowser(tx) => {
                let _ = tx.send(Err(err()));
            }
        }
        marked
    }

    /// Settle what a pipe connection dropped unsent, and navigations whose caller's lifecycle
    /// ended. Returns whether anything changed that needs another pass.
    #[cfg(feature = "pipe-transport")]
    fn sweep_pipe(&mut self, now: Instant) -> bool {
        let mut marked = false;
        for call in self.conn.take_retired() {
            if let Some((req, _, _)) = self.pending_commands.remove(&call) {
                self.fail_request(req, || CdpError::Timeout);
                marked = true;
            }
        }
        let dead: Vec<NavigationId> = self
            .navigations
            .iter()
            .filter(|(_, nav)| match nav {
                NavigationRequest::Navigate(nav) => nav
                    .ctx
                    .lifecycle
                    .as_ref()
                    .is_some_and(|lc| !lc.is_live(now)),
            })
            .map(|(id, _)| *id)
            .collect();
        for id in dead {
            for target in self.targets.values_mut() {
                target.frame_manager_mut().abort_navigation(id);
            }
            if let Some(NavigationRequest::Navigate(nav)) = self.navigations.remove(&id) {
                let _ = nav.tx.send(Err(CdpError::Timeout));
            }
            marked = true;
        }
        marked
    }

    /// The pipe connection ended for `reason`: every waiter is settled now, exactly once —
    /// commands, navigations, page creations, re-attaches, and messages still queued for the
    /// handler — and the handler yields `None` from now on.
    #[cfg(feature = "pipe-transport")]
    fn terminate(&mut self, reason: crate::error::PipeClosed) {
        let err = move || CdpError::Pipe(crate::error::PipeError::Closed(reason));
        // Dropped unsent before the end: those ran out of time or were cancelled.
        for call in self.conn.take_retired() {
            if let Some((req, _, _)) = self.pending_commands.remove(&call) {
                self.fail_request(req, || CdpError::Timeout);
            }
        }
        let pending: Vec<PendingRequest> = self
            .pending_commands
            .drain()
            .map(|(_, (req, _, _))| req)
            .collect();
        for req in pending {
            self.fail_request(req, err);
        }
        for (_, nav) in self.navigations.drain() {
            match nav {
                NavigationRequest::Navigate(nav) => {
                    let _ = nav.tx.send(Err(err()));
                }
            }
        }
        for target in self.targets.values_mut() {
            target.fail_waiters(err);
        }
        self.targets.clear();
        self.target_ids.clear();
        self.sessions.clear();
        let rx = self.from_browser.get_mut();
        rx.close();
        // `try_next`, not `try_recv`: the manifest allows any futures 0.3, and `try_recv` only
        // exists from futures-channel 0.3.32 on; `try_next` exists in every 0.3 release.
        #[allow(deprecated)]
        while let Ok(Some(msg)) = rx.try_next() {
            Self::refuse(msg, &err);
        }
        self.ended = true;
    }

    /// Answer a message that reached a handler whose pipe connection ended.
    #[cfg(feature = "pipe-transport")]
    fn refuse(msg: HandlerMessage, err: &impl Fn() -> CdpError) {
        match msg {
            HandlerMessage::Command(cmd) => {
                let _ = cmd.sender.send(Err(err()));
            }
            HandlerMessage::FetchTargets(tx) => {
                let _ = tx.send(Err(err()));
            }
            HandlerMessage::CloseBrowser(tx) => {
                let _ = tx.send(Err(err()));
            }
            HandlerMessage::CreatePage(_, tx) => {
                let _ = tx.send(Err(err()));
            }
            HandlerMessage::GetPages(tx) => {
                let _ = tx.send(Vec::new());
            }
            HandlerMessage::GetPage(_, tx) => {
                let _ = tx.send(None);
            }
            HandlerMessage::Detach(_, tx) => {
                let _ = tx.send(Err(err()));
            }
            HandlerMessage::Reattach(_, tx) => {
                let _ = tx.send(Err(err()));
            }
            HandlerMessage::WithContext(inner, _) => Self::refuse(*inner, err),
            HandlerMessage::InsertContext(_)
            | HandlerMessage::DisposeContext(_)
            | HandlerMessage::AddEventListener(_) => {}
        }
    }

    /// One message from the `Browser` (or a `Page`), with the lifecycle its caller stamped, if
    /// any.
    fn on_browser_message(
        &mut self,
        msg: HandlerMessage,
        now: Instant,
        ctx: CallContext,
    ) -> Result<()> {
        match msg {
            HandlerMessage::Command(cmd) => {
                #[cfg(feature = "pipe-transport")]
                let cmd = if ctx.lifecycle.is_some() {
                    cmd.with_context(ctx.clone())
                } else {
                    cmd
                };
                self.submit_external_command(cmd, now)?;
            }
            HandlerMessage::FetchTargets(tx) => {
                self.submit_fetch_targets(tx, now, ctx);
            }
            HandlerMessage::CloseBrowser(tx) => {
                self.submit_close(tx, now, ctx);
            }
            HandlerMessage::CreatePage(params, tx) => {
                self.create_page(params, tx, ctx);
            }
            HandlerMessage::GetPages(tx) => {
                let pages: Vec<_> = self
                    .targets
                    .values_mut()
                    .filter(|p| p.is_page())
                    .filter_map(|target| target.get_or_create_page())
                    .map(|page| Page::from(page.clone()))
                    .collect();
                let _ = tx.send(pages);
            }
            HandlerMessage::InsertContext(ctx) => {
                self.browser_contexts.insert(ctx);
            }
            HandlerMessage::DisposeContext(ctx) => {
                self.browser_contexts.remove(&ctx);
            }
            HandlerMessage::GetPage(target_id, tx) => {
                let page = self
                    .targets
                    .get_mut(&target_id)
                    .and_then(|target| target.get_or_create_page())
                    .map(|page| Page::from(page.clone()));
                let _ = tx.send(page);
            }
            HandlerMessage::Detach(target_id, tx) => {
                self.detach_target(target_id, tx, now, ctx);
            }
            HandlerMessage::Reattach(target_id, tx) => match self.targets.get_mut(&target_id) {
                Some(target) => target.reattach_with(now, tx, ctx),
                None => {
                    let _ = tx.send(Err(CdpError::NotFound));
                }
            },
            HandlerMessage::AddEventListener(req) => {
                self.event_listeners.add_listener(req);
            }
            #[cfg(feature = "pipe-transport")]
            HandlerMessage::WithContext(inner, ctx) => {
                return self.on_browser_message(*inner, now, ctx);
            }
        }
        Ok(())
    }

    pub fn event_listeners_mut(&mut self) -> &mut EventListeners {
        &mut self.event_listeners
    }
}

impl Stream for Handler {
    type Item = Result<()>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let pin = self.get_mut();

        #[cfg(feature = "pipe-transport")]
        if pin.ended {
            // A terminal pipe connection: its one error (if any) was returned, every waiter was
            // settled, and nothing can ever arrive again.
            return Poll::Ready(None);
        }

        loop {
            let now = pin.clock_now();
            // temporary pinning of the browser receiver should be safe as we are pinning
            // through the already pinned self. with the receivers we can also
            // safely ignore exhaustion as those are fused.
            while let Poll::Ready(Some(msg)) = Pin::new(&mut pin.from_browser).poll_next(cx) {
                pin.on_browser_message(msg, now, CallContext::default())?;
            }

            for n in (0..pin.target_ids.len()).rev() {
                let target_id = pin.target_ids.swap_remove(n);
                if let Some((id, mut target)) = pin.targets.remove_entry(&target_id) {
                    while let Some(event) = target.poll(cx, now) {
                        match event {
                            TargetEvent::Request(req) => {
                                let method = req.method.clone();
                                let ctx = pin.internal_ctx();
                                if let Err(err) = pin.submit_internal_command(
                                    target.target_id().clone(),
                                    req,
                                    None,
                                    now,
                                    ctx,
                                ) {
                                    // Refused before any byte was written: the target's
                                    // initialization fails now instead of waiting for an answer
                                    // that cannot come.
                                    if pin.is_pipe() {
                                        target.on_internal_refused(&method, &err);
                                    }
                                }
                            }
                            TargetEvent::Attach(req, attempt) => {
                                // A re-attaching caller's lifecycle bounds its attach.
                                let ctx = target.take_attach_context();
                                let ctx = pin.ctx_or_internal(ctx);
                                if pin
                                    .submit_internal_command(
                                        target.target_id().clone(),
                                        req,
                                        Some(attempt),
                                        now,
                                        ctx,
                                    )
                                    .is_err()
                                    && pin.is_pipe()
                                {
                                    // As if it timed out: the target gives up this attempt and
                                    // answers its waiter.
                                    target.on_attach_timed_out(attempt);
                                }
                            }
                            TargetEvent::Command(msg) => {
                                pin.on_target_message(&mut target, msg, now);
                            }
                            TargetEvent::NavigationRequest(id, req) => {
                                pin.submit_navigation(&mut target, id, req, now);
                            }
                            TargetEvent::NavigationResult(res) => {
                                pin.on_navigation_lifecycle_completed(res)
                            }
                            TargetEvent::SessionRestored(ev) => {
                                // Also to `Browser::event_listener` subscribers,
                                // so one listener per browser sees every tab's
                                // restoration (screen-play-rs#1041).
                                let _ = pin.event_listeners.try_send_custom(
                                    "Chromiumoxide.sessionRestored",
                                    serde_json::to_value(&ev).unwrap(),
                                );
                            }
                        }
                    }

                    // poll the target's event listeners
                    target.event_listeners_mut().poll(cx);

                    pin.targets.insert(id, target);
                    pin.target_ids.push(target_id);
                }
            }

            let mut done = true;
            // On a pipe, how many messages this pass may still read before yielding, so a flood
            // of events cannot keep the task from its sweeps (and other tasks) forever.
            let mut read_budget = PIPE_READ_BUDGET;

            loop {
                if pin.is_pipe() {
                    if read_budget == 0 {
                        break;
                    }
                    read_budget -= 1;
                }
                let ev = match Pin::new(&mut pin.conn).poll_next(cx) {
                    Poll::Ready(Some(ev)) => ev,
                    // The connection ended (the peer closed the socket, or it dropped after an
                    // error item). Nothing can arrive or be sent any more, so the handler ends
                    // too: a caller's `while let Some(_) = handler.next().await` returns, and
                    // dropping the handler fails every pending call. This used to read as
                    // "nothing read": the handler returned `Pending` and never ended — its
                    // eviction timer still woke it, but nothing could end it (winclaw#2121, U-3
                    // fix 2).
                    Poll::Ready(None) => {
                        #[cfg(feature = "pipe-transport")]
                        if pin.is_pipe() {
                            pin.terminate(crate::error::PipeClosed::Eof);
                        }
                        // Deliver what was read before the end, e.g. a page's ending events.
                        pin.event_listeners_mut().poll(cx);
                        return Poll::Ready(None);
                    }
                    Poll::Pending => break,
                };
                match ev {
                    Ok(Message::Response(resp)) => {
                        pin.on_response(resp);
                        if pin.closing {
                            #[cfg(feature = "pipe-transport")]
                            if pin.is_pipe() {
                                pin.terminate(crate::error::PipeClosed::Eof);
                            }
                            // handler should stop processing
                            return Poll::Ready(None);
                        }
                    }
                    Ok(Message::Event(ev)) => {
                        pin.on_event(ev);
                    }
                    #[cfg(feature = "pipe-transport")]
                    Err(CdpError::Pipe(crate::error::PipeError::Closed(reason))) => {
                        // A pipe connection is terminal: settle every waiter once, then end. A
                        // clean end of input is an end, not an error.
                        pin.terminate(reason);
                        pin.event_listeners_mut().poll(cx);
                        if reason == crate::error::PipeClosed::Eof {
                            return Poll::Ready(None);
                        }
                        tracing::error!("pipe connection ended: {:?}", reason);
                        return Poll::Ready(Some(Err(CdpError::Pipe(
                            crate::error::PipeError::Closed(reason),
                        ))));
                    }
                    Err(err @ CdpError::InvalidMessage(_, _)) => {
                        if pin.config.ignore_invalid_messages {
                            // Never the text, and not serde's message either: it can quote the
                            // value it rejected. Category and position are enough to diagnose.
                            if let CdpError::InvalidMessage(text, parse) = &err {
                                tracing::warn!(
                                    category = ?parse.classify(),
                                    line = parse.line(),
                                    column = parse.column(),
                                    len = text.len(),
                                    "WS invalid message"
                                );
                            }
                        } else {
                            return Poll::Ready(Some(Err(err)));
                        }
                    }
                    Err(err) => {
                        // Display, not Debug: Debug of a wrapped WS message prints its payload.
                        tracing::error!("WS Connection error: {}", err);
                        return Poll::Ready(Some(Err(err)));
                    }
                }
                done = false;
            }

            // Poll the handler's own event listeners once per pass, whether or not any target
            // exists. They used to be polled only inside the per-target loop above, so an event
            // that arrived when no target was left — the ending of a session's last page, whose
            // `Target.targetDestroyed` removes that very target — was queued and never delivered
            // (winclaw#2121, U-3 fix 1). Polled after the read, so what was read this pass is
            // delivered this pass.
            pin.event_listeners_mut().poll(cx);

            if pin.is_pipe() {
                // A pipe command's deadline is its caller's lifecycle, checked by the connection
                // on every poll; what the connection dropped unsent is settled here, and so is a
                // navigation whose caller stopped waiting or ran out of time.
                #[cfg(feature = "pipe-transport")]
                if pin.sweep_pipe(now) {
                    done = false;
                }
                if read_budget == 0 {
                    // Yielded with input possibly left: come back right away.
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
            } else if pin.evict_command_timeout.poll_ready(cx) {
                // evict all commands that timed out; a target told its attach
                // timed out is polled again now, not at the next unrelated
                // wakeup (screen-play-rs#1041, codex R4)
                if pin.evict_timed_out_commands(now) {
                    done = false;
                }
            }

            if done {
                // no events/responses were read from the websocket
                return Poll::Pending;
            }
        }
    }
}

/// How to configure the handler
#[derive(Debug, Clone)]
pub struct HandlerConfig {
    /// Whether the `NetworkManager`s should ignore https errors
    pub ignore_https_errors: bool,
    /// Whether to ignore invalid messages
    pub ignore_invalid_messages: bool,
    /// Window and device settings
    pub viewport: Option<Viewport>,
    /// Context ids to set from the get go
    pub context_ids: Vec<BrowserContextId>,
    /// default request timeout to use
    pub request_timeout: Duration,
    /// Whether to enable request interception
    pub request_intercept: bool,
    /// Whether to enable cache
    pub cache_enabled: bool,
    /// Stealth mode: minimize CDP domain enables to reduce bot detection footprint.
    /// When true, skips Performance.enable, Log.enable, Network.enable (unless
    /// request_intercept is on), Page.createIsolatedWorld, and sets
    /// waitForDebuggerOnStart to false.
    pub stealth_mode: bool,
    /// Whether a page that loses its session re-attaches in place (screen-play-rs#1041). `false`
    /// leaves it detached, as an intentional `detach_target` does: for a peer whose detach is the
    /// end of what it lent — WinClaw's CDP relay serves one lent tab, and a session it ended must
    /// see nothing more (winclaw#2202).
    pub session_recovery: bool,
}

impl Default for HandlerConfig {
    fn default() -> Self {
        Self {
            ignore_https_errors: true,
            ignore_invalid_messages: true,
            viewport: Default::default(),
            context_ids: Vec::new(),
            request_timeout: Duration::from_millis(REQUEST_TIMEOUT),
            request_intercept: false,
            cache_enabled: true,
            stealth_mode: false,
            session_recovery: true,
        }
    }
}

/// Wraps the sender half of the channel who requested a navigation
#[derive(Debug)]
pub struct NavigationInProgress<T> {
    /// Marker to indicate whether a navigation lifecycle has completed
    navigated: bool,
    /// The response of the issued navigation request
    response: Option<Response>,
    /// Sender who initiated the navigation request
    tx: OneshotSender<T>,
    /// The initiating caller's lifecycle on a pipe connection; empty otherwise.
    ctx: CallContext,
}

impl<T> NavigationInProgress<T> {
    fn new(tx: OneshotSender<T>, ctx: CallContext) -> Self {
        Self {
            navigated: false,
            response: None,
            tx,
            ctx,
        }
    }

    /// The response to the cdp request has arrived
    fn set_response(&mut self, resp: Response) {
        self.response = Some(resp);
    }

    /// The navigation process has finished, the page finished loading.
    fn set_navigated(&mut self) {
        self.navigated = true;
    }
}

/// Request type for navigation
#[derive(Debug)]
enum NavigationRequest {
    /// Represents a simple `NavigateParams` ("Page.navigate")
    Navigate(NavigationInProgress<Result<Response>>),
    // TODO are there more?
}

/// Different kind of submitted request submitted from the  `Handler` to the
/// `Connection` and being waited on for the response.
#[derive(Debug)]
enum PendingRequest {
    /// A Request to create a new `Target` that results in the creation of a
    /// `Page` that represents a browser page.
    CreateTarget(OneshotSender<Result<Page>>),
    /// A Request to fetch old `Target`s created before connection
    GetTargets(OneshotSender<Result<Vec<TargetInfo>>>),
    /// A Request to navigate a specific `Target`.
    ///
    /// Navigation requests are not automatically completed once the response to
    /// the raw cdp navigation request (like `NavigateParams`) arrives, but only
    /// after the `Target` notifies the `Handler` that the `Page` has finished
    /// loading, which comes after the response.
    Navigate(NavigationId),
    /// A common request received via a channel (`Page`).
    ExternalCommand(OneshotSender<Result<Response>>),
    /// Requests that are initiated directly from a `Target` (all the
    /// initialization commands): its target, the session it was sent on
    /// (`None` for a browser-level command such as the attach), and — for a
    /// `Target.attachToTarget` — which attach attempt of that target it
    /// belongs to, so an abandoned attempt's answer or timeout cannot land on
    /// a newer one (screen-play-rs#1041, codex R4).
    InternalCommand(TargetId, Option<String>, Option<u64>),
    /// An intentional detach (`Browser::detach_target`): the target, the
    /// session it ends, and the caller (screen-play-rs#1041, codex R9).
    Detach(TargetId, String, u64, OneshotSender<Result<()>>),
    // A Request to close the browser.
    CloseBrowser(OneshotSender<Result<CloseReturns>>),
}

/// Events used internally to communicate with the handler, which are executed
/// in the background
// TODO rename to BrowserMessage
#[derive(Debug)]
pub(crate) enum HandlerMessage {
    CreatePage(CreateTargetParams, OneshotSender<Result<Page>>),
    FetchTargets(OneshotSender<Result<Vec<TargetInfo>>>),
    InsertContext(BrowserContext),
    DisposeContext(BrowserContext),
    GetPages(OneshotSender<Vec<Page>>),
    Command(CommandMessage),
    GetPage(TargetId, OneshotSender<Option<Page>>),
    /// Detach a target on purpose: record that it should stay detached and
    /// end its session, both here — so a caller that stops waiting cannot
    /// leave the intent recorded without the detach sent (screen-play-rs#1041,
    /// codex R9).
    Detach(TargetId, OneshotSender<Result<()>>),
    /// Re-attach a target in place and answer with its page once initialized.
    Reattach(TargetId, OneshotSender<Result<Page>>),
    AddEventListener(EventListenerRequest),
    CloseBrowser(OneshotSender<Result<CloseReturns>>),
    /// A message whose command(s) carry its caller's lifecycle on a pipe connection
    /// (screen-play-rs#1735): stamped at the first poll of the API future, not at receipt here.
    #[cfg(feature = "pipe-transport")]
    WithContext(Box<HandlerMessage>, CallContext),
}
