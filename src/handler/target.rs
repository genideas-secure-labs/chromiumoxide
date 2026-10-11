use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use chromiumoxide_cdp::cdp::browser_protocol::target::DetachFromTargetParams;
use futures::channel::oneshot::Sender;
use futures::stream::Stream;
use futures::task::{Context, Poll};

use chromiumoxide_cdp::cdp::CdpEventMessage;
use chromiumoxide_cdp::cdp::browser_protocol::page::{
    CreateIsolatedWorldParams, FrameId, GetFrameTreeParams,
};
use chromiumoxide_cdp::cdp::browser_protocol::{
    browser::BrowserContextId,
    log as cdplog, performance,
    target::{AttachToTargetParams, SessionId, SetAutoAttachParams, TargetId, TargetInfo},
};
use chromiumoxide_cdp::cdp::events::CdpEvent;
use chromiumoxide_types::{Command, Method, Request, Response};

use crate::auth::Credentials;
use crate::cdp::browser_protocol::target::CloseTargetParams;
use crate::cmd::CommandChain;
use crate::cmd::CommandMessage;
use crate::error::{CdpError, Result};
use crate::handler::browser::BrowserContext;
use crate::handler::domworld::DOMWorldKind;
use crate::handler::emulation::EmulationManager;
use crate::handler::frame::{
    FrameEvent, FrameManager, NavigationError, NavigationId, NavigationOk,
};
use crate::handler::frame::{FrameNavigationRequest, UTILITY_WORLD_NAME};
use crate::handler::network::{NetworkEvent, NetworkManager};
use crate::handler::page::PageHandle;
use crate::handler::viewport::Viewport;
use crate::handler::{PageInner, REQUEST_TIMEOUT};
use crate::listeners::{EventListenerRequest, EventListeners};
use crate::{ArcHttpRequest, page::Page};
use chromiumoxide_cdp::cdp::js_protocol::runtime::{
    ExecutionContextId, RunIfWaitingForDebuggerParams,
};
use std::time::Duration;

/// At most this many automatic re-attaches per target within
/// [`RECOVERY_WINDOW`] (screen-play-rs#1041) — a session that keeps dropping
/// must not turn into an attach loop.
pub const RECOVERY_BUDGET: usize = 3;
/// See [`RECOVERY_BUDGET`].
pub const RECOVERY_WINDOW: Duration = Duration::from_secs(60);

/// Sent to a target's event listeners each time it finishes re-attaching on a
/// new DevTools session (screen-play-rs#1041). State this crate holds
/// (network settings, isolated worlds, viewport) is already re-applied; state
/// a caller set on the old session directly — a user-agent override, init
/// scripts, anything sent through `Page::execute` — is gone, and the caller
/// re-applies it on seeing this. `Page` handles bound to the old session are
/// stale; fetch the page again.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventSessionRestored {
    pub target_id: String,
    /// 1 after the first re-attach, 2 after the second, …
    pub generation: u64,
}

impl chromiumoxide_types::MethodType for EventSessionRestored {
    fn method_id() -> chromiumoxide_types::MethodId {
        "Chromiumoxide.sessionRestored".into()
    }
}
impl crate::cdp::CustomEvent for EventSessionRestored {}

macro_rules! advance_state {
    ($s:ident, $cx:ident, $now:ident, $cmds: ident, $next_state:expr ) => {{
        // No session, no commands — every initialization state, not only the
        // first (screen-play-rs#1041). A lost session is handled by
        // `on_session_lost`, which re-attaches or leaves the target detached.
        if $s.session_id.is_none() {
            return None;
        }
        if let Poll::Ready(poll) = $cmds.poll($now) {
            return match poll {
                None => {
                    $s.init_state = $next_state;
                    $s.poll($cx, $now)
                }
                Some(Ok((method, params))) => Some(TargetEvent::Request(Request {
                    method,
                    session_id: $s.session_id.clone().map(Into::into),
                    params,
                })),
                Some(Err(_)) => {
                    let ev = $s.on_initialization_failed($now);
                    $s.continue_after_failure(ev, $cx, $now)
                }
            };
        } else {
            return None;
        }
    }};
}

#[derive(Debug)]
pub struct Target {
    /// Info about this target as returned from the chromium instance
    info: TargetInfo,
    /// The type of this target
    r#type: TargetType,
    /// Configs for this target
    config: TargetConfig,
    /// The context this target is running in
    browser_context: BrowserContext,
    /// The frame manager that maintains the state of all frames and handles
    /// navigations of frames
    frame_manager: FrameManager,
    /// Handles all the https
    network_manager: NetworkManager,
    emulation_manager: EmulationManager,
    /// The identifier of the session this target is attached to
    session_id: Option<SessionId>,
    /// A required initialization command Chrome answered with an error, held
    /// until the next `poll` turns it into a failed initialization.
    /// `on_response` cannot return the close request itself (screen-play-rs#1035).
    init_error: Option<(String, chromiumoxide_types::Error)>,
    /// The handle of the browser page of this target
    page: Option<PageHandle>,
    /// Drives this target towards initialization
    init_state: TargetInit,
    /// Currently queued events to report to the `Handler`
    queued_events: VecDeque<TargetEvent>,
    /// All registered event subscriptions
    event_listeners: EventListeners,
    /// Senders that need to be notified once the main frame has loaded
    wait_for_frame_navigation: Vec<Sender<ArcHttpRequest>>,
    /// The sender who requested the page.
    initiator: Option<Sender<Result<Page>>>,
    /// Whether this target should have a session (screen-play-rs#1041). True
    /// until `Browser::detach_target`; `Browser::reattach_target` sets it
    /// again. A lost session is recovered only while this is true — an
    /// intentional detach stays detached.
    desired_attached: bool,
    /// A `Target.attachToTarget` was sent and its session has not arrived.
    attach_in_flight: bool,
    /// The current initialization is a re-attach of a target that was already
    /// initialized once: on success its network settings are re-applied and
    /// `EventSessionRestored` is sent; on failure it is not closed.
    reattaching: bool,
    /// Start times of the automatic re-attaches inside [`RECOVERY_WINDOW`].
    recoveries: VecDeque<Instant>,
    /// How many re-attaches have completed.
    session_generation: u64,
    /// Whoever called `Browser::reattach_target` and waits for the page.
    reattach_waiter: Option<Sender<Result<Page>>>,
}

impl Target {
    /// Create a new target instance with `TargetInfo` after a
    /// `CreateTargetParams` request.
    pub fn new(info: TargetInfo, config: TargetConfig, browser_context: BrowserContext) -> Self {
        let ty = TargetType::new(&info.r#type);
        let request_timeout = config.request_timeout;
        let mut network_manager = NetworkManager::new(config.ignore_https_errors, request_timeout);

        network_manager.set_cache_enabled(config.cache_enabled);
        network_manager.set_request_interception(config.request_intercept);

        Self {
            info,
            r#type: ty,
            config,
            frame_manager: FrameManager::new(request_timeout),
            network_manager,
            emulation_manager: EmulationManager::new(request_timeout),
            session_id: None,
            init_error: None,
            page: None,
            init_state: TargetInit::AttachToTarget,
            wait_for_frame_navigation: Default::default(),
            queued_events: Default::default(),
            event_listeners: Default::default(),
            initiator: None,
            browser_context,
            desired_attached: true,
            attach_in_flight: false,
            reattaching: false,
            recoveries: VecDeque::new(),
            session_generation: 0,
            reattach_waiter: None,
        }
    }

    pub fn set_session_id(&mut self, id: SessionId) {
        self.session_id = Some(id);
        self.attach_in_flight = false;
    }

    /// Whether this target should have a session (screen-play-rs#1041).
    pub fn desired_attached(&self) -> bool {
        self.desired_attached
    }

    /// Record whether this target should have a session. `false` is what an
    /// intentional detach sets first, so the detach that follows is not
    /// mistaken for a lost session.
    pub fn set_desired_attached(&mut self, desired: bool) {
        self.desired_attached = desired;
        // An attach not sent yet is not sent at all (codex R1). One already in
        // flight is refused by the handler when its session arrives.
        if !desired && matches!(self.init_state, TargetInit::AttachToTarget) {
            self.abandon_attachment();
        }
    }

    /// Give up an attachment that will not happen — the target was detached
    /// on purpose before it finished — and tell whoever waits for its page.
    pub(crate) fn abandon_attachment(&mut self) {
        self.attach_in_flight = false;
        self.reattaching = false;
        self.init_error = None;
        let detached = || {
            CdpError::msg("the target was detached before its session finished initializing")
        };
        if let Some(initiator) = self.initiator.take() {
            let _ = initiator.send(Err(detached()));
        }
        if let Some(waiter) = self.reattach_waiter.take() {
            let _ = waiter.send(Err(detached()));
        }
        self.init_state = TargetInit::Initialized;
    }

    /// How many re-attaches have completed on this target.
    pub fn session_generation(&self) -> u64 {
        self.session_generation
    }

    /// The current session ended (screen-play-rs#1041). Drops what was bound
    /// to it and, if this target should stay attached, re-attaches it — the
    /// same `Target`, so its managers, listeners and waiters survive.
    pub(crate) fn on_session_lost(&mut self, now: Instant) {
        self.session_id = None;
        self.attach_in_flight = false;
        self.clear_page();
        if matches!(self.init_state, TargetInit::Closing) {
            return;
        }
        if !self.desired_attached {
            // Detached on purpose while still initializing: nothing will
            // finish that initialization, so whoever waits is told now.
            if self.init_state.commands_mut().is_some()
                || matches!(self.init_state, TargetInit::AttachToTarget)
            {
                self.abandon_attachment();
            }
            return;
        }
        if !self.is_page() {
            return;
        }
        self.begin_reattach(now, false);
    }

    /// Discovery named this already-tracked target again (screen-play-rs#1041):
    /// refresh its metadata, and — if it should be attached, has no session
    /// and nothing is attaching it — start the same in-place re-attach a lost
    /// session starts, within the recovery budget. Never a replacement.
    pub(crate) fn on_rediscovered(&mut self, info: &TargetInfo, now: Instant) {
        self.refresh_metadata(info);
        if self.wants_revival() {
            self.begin_reattach(now, false);
        }
    }

    /// Whether discovery should start a re-attach for this target: it should
    /// be attached, has no session, and nothing is already attaching it.
    pub(crate) fn wants_revival(&self) -> bool {
        self.is_page()
            && self.desired_attached
            && self.session_id.is_none()
            && !self.attach_in_flight
            && !matches!(
                self.init_state,
                TargetInit::Closing | TargetInit::AttachToTarget
            )
    }

    /// Start re-attaching this target on a new session. `explicit` is a
    /// caller's `reattach_target`, which is not rationed; an automatic
    /// recovery spends one unit of [`RECOVERY_BUDGET`], and none is started
    /// once that is spent. Returns whether a re-attach was started.
    pub(crate) fn begin_reattach(&mut self, now: Instant, explicit: bool) -> bool {
        if explicit {
            self.recoveries.clear();
        } else {
            while self
                .recoveries
                .front()
                .is_some_and(|t| now.duration_since(*t) > RECOVERY_WINDOW)
            {
                self.recoveries.pop_front();
            }
            if self.recoveries.len() >= RECOVERY_BUDGET {
                tracing::warn!(
                    target_id = ?self.info.target_id,
                    budget = RECOVERY_BUDGET,
                    "session recovery budget spent; leaving the target detached"
                );
                return false;
            }
            self.recoveries.push_back(now);
        }
        self.session_id = None;
        self.attach_in_flight = false;
        self.init_error = None;
        self.clear_page();
        self.frame_manager.on_session_replaced();
        self.network_manager.on_session_replaced();
        self.reattaching = true;
        self.init_state = TargetInit::AttachToTarget;
        true
    }

    /// Re-attach on a caller's request (`Browser::reattach_target`). An
    /// attached, initialized target answers at once; otherwise the page is
    /// sent to `tx` when the new session finishes initializing.
    pub(crate) fn reattach(&mut self, now: Instant, tx: Sender<Result<Page>>) {
        self.desired_attached = true;
        if self.session_id.is_some() && self.is_initialized() {
            match self.get_or_create_page() {
                Some(page) => {
                    let _ = tx.send(Ok(page.clone().into()));
                }
                None => {
                    let _ = tx.send(Err(CdpError::NotFound));
                }
            }
            return;
        }
        if let Some(previous) = self.reattach_waiter.replace(tx) {
            let _ = previous.send(Err(CdpError::msg(
                "superseded by a later reattach_target for the same target",
            )));
        }
        let attaching = self.attach_in_flight
            || matches!(self.init_state, TargetInit::AttachToTarget)
            || (self.session_id.is_some() && self.init_state.commands_mut().is_some());
        if !attaching {
            self.begin_reattach(now, true);
        }
    }

    /// Refresh the metadata that can legitimately change while a target lives.
    ///
    /// `Target.targetInfoChanged` is not handled by this crate, and target
    /// discovery deliberately does not replace a live `Target`, so this is the
    /// only path that keeps `info()` from freezing at its first snapshot.
    /// Session, page and frame state are untouched.
    ///
    /// Deliberately narrow, and narrow by field rather than by wholesale
    /// replacement: `type`, `browser_context_id` and `opener_id` are read
    /// elsewhere as if they were fixed for the life of the target — `r#type` is
    /// mirrored into `self.r#type` at construction and `opener_id` is copied
    /// into the `PageHandle` — so overwriting the whole struct could make
    /// `info().type` disagree with `is_page()`, or leave a `Page::opener_id`
    /// that no longer matches `info()`.
    ///
    /// The rule is therefore complete rather than a list of favourites: every
    /// field that has NO cached mirror is refreshed, and only the mirrored ones
    /// (`type`, `opener_id`, `browser_context_id`) plus the identity
    /// (`target_id`, which is this entry's map key) are left alone. Enumerated
    /// against the CDP `Target.TargetInfo` definition rather than from memory —
    /// review round 7 named two stale fields and the protocol has four.
    ///
    /// Failing safe was the deliberate choice over `self.info = info.clone()`:
    /// if upstream adds a field, this leaves it stale, whereas wholesale
    /// replacement would silently overwrite a future mirrored field and put
    /// `info()` back out of step with the cached copy — which is the desync
    /// that made round 4 revert an earlier attempt.
    pub fn refresh_metadata(&mut self, info: &TargetInfo) {
        self.info.title.clone_from(&info.title);
        self.info.url.clone_from(&info.url);
        // Genuinely flips false -> true once a session is opened.
        self.info.attached = info.attached;
        self.info.can_access_opener = info.can_access_opener;
        self.info.opener_frame_id.clone_from(&info.opener_frame_id);
        self.info.parent_frame_id.clone_from(&info.parent_frame_id);
        self.info.subtype.clone_from(&info.subtype);
    }

    pub fn session_id(&self) -> Option<&SessionId> {
        self.session_id.as_ref()
    }

    pub fn browser_context(&self) -> &BrowserContext {
        &self.browser_context
    }

    pub fn session_id_mut(&mut self) -> &mut Option<SessionId> {
        &mut self.session_id
    }

    /// Drop the cached `PageHandle`.
    ///
    /// Called from the handler when a session detaches so that `get_page`
    /// during the detach/reattach race window returns `CdpError::NotFound`
    /// instead of a `PageHandle` bound to a dead session. The next
    /// `attachedToTarget` event reseats `session_id`, and the next
    /// `get_or_create_page` call rebuilds the handle from the fresh session.
    pub(crate) fn clear_page(&mut self) {
        self.page = None;
    }

    /// The identifier for this target
    pub fn target_id(&self) -> &TargetId {
        &self.info.target_id
    }

    /// The type of this target
    pub fn r#type(&self) -> &TargetType {
        &self.r#type
    }

    /// Whether this target is already initialized
    pub fn is_initialized(&self) -> bool {
        matches!(self.init_state, TargetInit::Initialized)
    }

    /// Navigate a frame
    pub fn goto(&mut self, req: FrameNavigationRequest) {
        self.frame_manager.goto(req)
    }

    fn create_page(&mut self) {
        // No active session → can't bind a PageHandle. Leave any stale cache
        // in place; the next attach will trigger a rebuild via the mismatch
        // check below.
        let Some(session) = self.session_id.clone() else {
            return;
        };

        // Rebuild the cached PageHandle when its session no longer matches
        // the target's current session. This is what makes detach + reattach
        // (or any session rotation) safe for callers: previously the first
        // call to `get_or_create_page` cached a PageHandle bound to S1, and
        // subsequent calls — even after S1 was torn down and S2 attached —
        // kept returning the S1-bound handle, so every command came back
        // with `Error -32001: Session with given id not found`.
        let session_matches = self
            .page
            .as_ref()
            .is_some_and(|p| p.inner().session_id() == &session);
        if !session_matches {
            let handle =
                PageHandle::new(self.target_id().clone(), session, self.opener_id().cloned());
            self.page = Some(handle);
        }
    }

    /// Tries to create the `PageInner` if this target is already initialized
    pub(crate) fn get_or_create_page(&mut self) -> Option<&Arc<PageInner>> {
        self.create_page();
        self.page.as_ref().map(|p| p.inner())
    }

    pub fn is_page(&self) -> bool {
        self.r#type().is_page()
    }

    pub fn browser_context_id(&self) -> Option<&BrowserContextId> {
        self.info.browser_context_id.as_ref()
    }

    pub fn info(&self) -> &TargetInfo {
        &self.info
    }

    /// Get the target that opened this target. Top-level targets return `None`.
    pub fn opener_id(&self) -> Option<&TargetId> {
        self.info.opener_id.as_ref()
    }

    pub fn frame_manager(&self) -> &FrameManager {
        &self.frame_manager
    }

    pub fn frame_manager_mut(&mut self) -> &mut FrameManager {
        &mut self.frame_manager
    }

    pub fn event_listeners_mut(&mut self) -> &mut EventListeners {
        &mut self.event_listeners
    }

    /// Received a response to a command issued by this target
    pub fn on_response(&mut self, resp: Response, method: &str) {
        if let Some(cmds) = self.init_state.commands_mut() {
            // An error answer to an initialization command used to advance the
            // chain like a success, so a target whose `Page.enable` was refused
            // still called itself Initialized — and then never saw a page event
            // (screen-play-rs#1035). Fail the initialization instead: the next
            // `poll` reports Chrome's error to whoever waits and closes the
            // target, as a timed-out command already did. Measured before
            // tightening: 1,722 initialization responses over launch, connect,
            // two concurrent clients, OOPIF/worker pages and tabs closed
            // mid-initialization — none was an error.
            if let Some(err) = resp.error.as_ref().filter(|_| {
                // One per frame, for the frames the tree had a moment ago: a
                // frame that went away meanwhile refuses it, and that is frame
                // churn, not a failed page (codex). The page still works; that
                // frame simply has no utility world.
                method != CreateIsolatedWorldParams::IDENTIFIER
            }) {
                tracing::warn!(
                    target_id = ?self.info.target_id,
                    method,
                    code = err.code,
                    message = %err.message,
                    "initialization command failed"
                );
                if self.init_error.is_none() {
                    self.init_error = Some((method.to_string(), err.clone()));
                }
            }
            cmds.received_response(method);
        }
        #[allow(clippy::single_match)] // allow for now
        match method {
            GetFrameTreeParams::IDENTIFIER => {
                if let Some(resp) = resp
                    .result
                    .and_then(|val| GetFrameTreeParams::response_from_value(val).ok())
                {
                    self.frame_manager.on_frame_tree(resp.frame_tree);
                }
            }
            // requests originated from the network manager all return an empty response, hence they
            // can be ignored here
            _ => {}
        }
    }

    pub fn on_event(&mut self, event: CdpEventMessage) {
        let CdpEventMessage { params, method, .. } = event;
        match &params {
            // `FrameManager` events
            CdpEvent::PageFrameAttached(ev) => self
                .frame_manager
                .on_frame_attached(ev.frame_id.clone(), Some(ev.parent_frame_id.clone())),
            CdpEvent::PageFrameDetached(ev) => self.frame_manager.on_frame_detached(ev),
            CdpEvent::PageFrameNavigated(ev) => self.frame_manager.on_frame_navigated(&ev.frame),
            CdpEvent::PageNavigatedWithinDocument(ev) => {
                self.frame_manager.on_frame_navigated_within_document(ev)
            }
            CdpEvent::RuntimeExecutionContextCreated(ev) => {
                self.frame_manager.on_frame_execution_context_created(ev)
            }
            CdpEvent::RuntimeExecutionContextDestroyed(ev) => {
                self.frame_manager.on_frame_execution_context_destroyed(ev)
            }
            CdpEvent::RuntimeExecutionContextsCleared(_) => {
                self.frame_manager.on_execution_contexts_cleared()
            }
            CdpEvent::RuntimeBindingCalled(ev) => {
                // TODO check if binding registered and payload is json
                self.frame_manager.on_runtime_binding_called(ev)
            }
            CdpEvent::PageLifecycleEvent(ev) => self.frame_manager.on_page_lifecycle_event(ev),
            CdpEvent::PageFrameStartedLoading(ev) => {
                self.frame_manager.on_frame_started_loading(ev);
            }

            // `Target` events
            CdpEvent::TargetAttachedToTarget(ev) => {
                if ev.waiting_for_debugger {
                    let runtime_cmd = RunIfWaitingForDebuggerParams::default();

                    self.queued_events.push_back(TargetEvent::Request(Request {
                        method: runtime_cmd.identifier(),
                        session_id: Some(ev.session_id.clone().into()),
                        params: serde_json::to_value(runtime_cmd).unwrap(),
                    }));
                }

                if "service_worker" == &ev.target_info.r#type {
                    let detach_command = DetachFromTargetParams::builder()
                        .session_id(ev.session_id.clone())
                        .build();

                    self.queued_events.push_back(TargetEvent::Request(Request {
                        method: detach_command.identifier(),
                        session_id: self.session_id.clone().map(Into::into),
                        params: serde_json::to_value(detach_command).unwrap(),
                    }));
                }
            }

            // `NetworkManager` events
            CdpEvent::FetchRequestPaused(ev) => self.network_manager.on_fetch_request_paused(ev),
            CdpEvent::FetchAuthRequired(ev) => self.network_manager.on_fetch_auth_required(ev),
            CdpEvent::NetworkRequestWillBeSent(ev) => {
                self.network_manager.on_request_will_be_sent(ev)
            }
            CdpEvent::NetworkRequestServedFromCache(ev) => {
                self.network_manager.on_request_served_from_cache(ev)
            }
            CdpEvent::NetworkResponseReceived(ev) => self.network_manager.on_response_received(ev),
            CdpEvent::NetworkLoadingFinished(ev) => {
                self.network_manager.on_network_loading_finished(ev)
            }
            CdpEvent::NetworkLoadingFailed(ev) => {
                self.network_manager.on_network_loading_failed(ev)
            }
            _ => {}
        }
        chromiumoxide_cdp::consume_event!(match params {
           |ev| self.event_listeners.start_send(ev),
           |json| { let _ = self.event_listeners.try_send_custom(&method, json);}
        });
    }

    /// What a poll returns after a failed initialization: the close request,
    /// or — when a re-attach is retrying — the retry's attach right away. A
    /// `None` here would leave the target unpolled until something else woke
    /// the handler.
    fn continue_after_failure(
        &mut self,
        ev: Option<TargetEvent>,
        cx: &mut Context<'_>,
        now: Instant,
    ) -> Option<TargetEvent> {
        if ev.is_none() && matches!(self.init_state, TargetInit::AttachToTarget) {
            return self.poll(cx, now);
        }
        ev
    }

    /// Called when a init command timed out
    fn on_initialization_failed(&mut self, now: Instant) -> Option<TargetEvent> {
        self.fail_initialization(CdpError::Timeout, now)
    }

    /// End a failed initialization. A target initializing for the first time
    /// reports `error` to whoever waits and is closed. A RE-attach is not
    /// closed — the tab is the user's — but reports `error` to a waiting
    /// `reattach_target` and, if it should stay attached, tries again within
    /// [`RECOVERY_BUDGET`] (the error may be the old session's last word);
    /// once that is spent it stays detached (screen-play-rs#1041).
    fn fail_initialization(&mut self, error: CdpError, now: Instant) -> Option<TargetEvent> {
        if self.reattaching {
            // The failed attempt's session may still be live: end it, or every
            // retry leaves one more session behind whose events reach this
            // target (codex R1). On the browser session, by session id.
            let failed_session = self.session_id.take();
            let detach_failed = failed_session.map(|session| {
                let params = DetachFromTargetParams::builder().session_id(session).build();
                TargetEvent::Request(Request {
                    method: params.identifier(),
                    session_id: None,
                    params: serde_json::to_value(params).unwrap(),
                })
            });
            if self.desired_attached && self.begin_reattach(now, false) {
                // The waiters keep waiting for the retry.
                return detach_failed;
            }
            // Giving up: whoever waits — `reattach_target`, or the original
            // `new_page` whose first initialization lost its session — gets
            // the error instead of waiting forever (codex R1).
            if let Some(waiter) = self.reattach_waiter.take() {
                let _ = waiter.send(Err(CdpError::msg(error.to_string())));
            }
            if let Some(initiator) = self.initiator.take() {
                let _ = initiator.send(Err(error));
            }
            self.reattaching = false;
            self.attach_in_flight = false;
            self.clear_page();
            self.init_state = TargetInit::Initialized;
            return detach_failed;
        }
        if let Some(initiator) = self.initiator.take() {
            let _ = initiator.send(Err(error));
        }
        self.init_state = TargetInit::Closing;
        let close_target = CloseTargetParams::new(self.info.target_id.clone());
        // Sent on the browser's own session, by target id: the page's session
        // may be the thing that failed (`Session with given id not found`),
        // and Chrome would refuse the close on it too, leaving the tab open
        // and this target stuck Closing (codex).
        Some(TargetEvent::Request(Request {
            method: close_target.identifier(),
            session_id: None,
            params: serde_json::to_value(close_target).unwrap(),
        }))
    }

    /// Advance that target's state
    pub(crate) fn poll(&mut self, cx: &mut Context<'_>, now: Instant) -> Option<TargetEvent> {
        if !self.is_page() {
            // can only poll pages
            return None;
        }
        if self.init_state.commands_mut().is_some() {
            if let Some((_method, err)) = self.init_error.take() {
                let ev = self.fail_initialization(CdpError::Chrome(err), now);
                return self.continue_after_failure(ev, cx, now);
            }
        }
        match &mut self.init_state {
            TargetInit::AttachToTarget => {
                self.init_state = TargetInit::InitializingFrame(FrameManager::init_commands(
                    self.config.request_timeout,
                    self.config.stealth_mode,
                ));
                let params = AttachToTargetParams::builder()
                    .target_id(self.target_id().clone())
                    .flatten(true)
                    .build()
                    .unwrap();
                self.attach_in_flight = true;

                return Some(TargetEvent::Request(Request::new(
                    params.identifier(),
                    serde_json::to_value(params).unwrap(),
                )));
            }
            TargetInit::InitializingFrame(cmds) => {
                self.session_id.as_ref()?;
                if let Poll::Ready(poll) = cmds.poll(now) {
                    return match poll {
                        None => {
                            // Stealth: skip createIsolatedWorld — it's a debugger-only
                            // artifact that reCAPTCHA can detect
                            if !self.config.stealth_mode {
                                if let Some(isolated_world_cmds) =
                                    self.frame_manager.ensure_isolated_world(UTILITY_WORLD_NAME)
                                {
                                    *cmds = isolated_world_cmds;
                                    return self.poll(cx, now);
                                }
                            }
                            // Stealth: skip Network.enable unless request_intercept is needed
                            if self.config.stealth_mode && !self.config.request_intercept {
                                self.init_state =
                                    TargetInit::InitializingPage(Self::page_init_commands(
                                        self.config.request_timeout,
                                        self.config.stealth_mode,
                                    ));
                            } else {
                                self.init_state = TargetInit::InitializingNetwork(
                                    self.network_manager.init_commands(),
                                );
                            }
                            self.poll(cx, now)
                        }
                        Some(Ok((method, params))) => Some(TargetEvent::Request(Request {
                            method,
                            session_id: self.session_id.clone().map(Into::into),
                            params,
                        })),
                        Some(Err(_)) => {
                            let ev = self.on_initialization_failed(now);
                            self.continue_after_failure(ev, cx, now)
                        }
                    };
                } else {
                    return None;
                }
            }
            TargetInit::InitializingNetwork(cmds) => {
                advance_state!(
                    self,
                    cx,
                    now,
                    cmds,
                    TargetInit::InitializingPage(Self::page_init_commands(
                        self.config.request_timeout,
                        self.config.stealth_mode,
                    ))
                );
            }
            TargetInit::InitializingPage(cmds) => {
                advance_state!(
                    self,
                    cx,
                    now,
                    cmds,
                    match self.config.viewport.as_ref() {
                        Some(viewport) => TargetInit::InitializingEmulation(
                            self.emulation_manager.init_commands(viewport)
                        ),
                        None => TargetInit::Initialized,
                    }
                );
            }
            TargetInit::InitializingEmulation(cmds) => {
                advance_state!(self, cx, now, cmds, TargetInit::Initialized);
            }
            TargetInit::Initialized => {
                if self.reattaching && self.session_id.is_some() {
                    // A re-attach just finished (screen-play-rs#1041): put the
                    // network settings back on the new session, tell listeners
                    // the session changed, and hand the page to a waiting
                    // `reattach_target` — without waiting for a load, since
                    // the page did not navigate.
                    self.reattaching = false;
                    self.session_generation += 1;
                    self.network_manager.reapply_session_state();
                    // Queue the re-applied settings NOW, ahead of anything a
                    // woken caller sends: queued events are submitted before
                    // the page's own commands are read (codex R1), so the
                    // first request after a re-attach already has them.
                    while let Some(event) = self.network_manager.poll() {
                        if let NetworkEvent::SendCdpRequest((method, params)) = event {
                            self.queued_events.push_back(TargetEvent::Request(Request {
                                method,
                                session_id: self.session_id.clone().map(Into::into),
                                params,
                            }));
                        }
                    }
                    let restored = EventSessionRestored {
                        target_id: self.info.target_id.inner().to_string(),
                        generation: self.session_generation,
                    };
                    let _ = self.event_listeners.try_send_custom(
                        "Chromiumoxide.sessionRestored",
                        serde_json::to_value(&restored).unwrap(),
                    );
                    self.queued_events
                        .push_back(TargetEvent::SessionRestored(restored));
                }
                // A `reattach_target` that arrived while this target was still
                // attaching (first time or not) is answered by ANY successful
                // initialization, not only a recovery (codex R1).
                if self.session_id.is_some() {
                    if let Some(waiter) = self.reattach_waiter.take() {
                        match self.get_or_create_page() {
                            Some(page) => {
                                let _ = waiter.send(Ok(page.clone().into()));
                            }
                            None => {
                                let _ = waiter.send(Err(CdpError::NotFound));
                            }
                        }
                    }
                }
                if let Some(initiator) = self.initiator.take() {
                    // make sure that the main frame of the page has finished loading
                    if self
                        .frame_manager
                        .main_frame()
                        .map(|frame| frame.is_loaded())
                        .unwrap_or_default()
                    {
                        if let Some(page) = self.get_or_create_page() {
                            let _ = initiator.send(Ok(page.clone().into()));
                        } else {
                            self.initiator = Some(initiator);
                        }
                    } else {
                        self.initiator = Some(initiator);
                    }
                }
            }
            TargetInit::Closing => return None,
        };
        loop {
            if let Some(frame) = self.frame_manager.main_frame() {
                if frame.is_loaded() {
                    while let Some(tx) = self.wait_for_frame_navigation.pop() {
                        let _ = tx.send(frame.http_request().cloned());
                    }
                }
            }

            // Drain queued messages first.
            if let Some(ev) = self.queued_events.pop_front() {
                return Some(ev);
            }

            if let Some(handle) = self.page.as_mut() {
                while let Poll::Ready(Some(msg)) = Pin::new(&mut handle.rx).poll_next(cx) {
                    match msg {
                        TargetMessage::Command(cmd) => {
                            self.queued_events.push_back(TargetEvent::Command(cmd));
                        }
                        TargetMessage::MainFrame(tx) => {
                            let _ =
                                tx.send(self.frame_manager.main_frame().map(|f| f.id().clone()));
                        }
                        TargetMessage::AllFrames(tx) => {
                            let _ = tx.send(
                                self.frame_manager
                                    .frames()
                                    .map(|f| f.id().clone())
                                    .collect(),
                            );
                        }
                        TargetMessage::Url(req) => {
                            let GetUrl { frame_id, tx } = req;
                            let frame = if let Some(frame_id) = frame_id {
                                self.frame_manager.frame(&frame_id)
                            } else {
                                self.frame_manager.main_frame()
                            };
                            let _ = tx.send(frame.and_then(|f| f.url().map(str::to_string)));
                        }
                        TargetMessage::Name(req) => {
                            let GetName { frame_id, tx } = req;
                            let frame = if let Some(frame_id) = frame_id {
                                self.frame_manager.frame(&frame_id)
                            } else {
                                self.frame_manager.main_frame()
                            };
                            let _ = tx.send(frame.and_then(|f| f.name().map(str::to_string)));
                        }
                        TargetMessage::Parent(req) => {
                            let GetParent { frame_id, tx } = req;
                            let frame = self.frame_manager.frame(&frame_id);
                            let _ = tx.send(frame.and_then(|f| f.parent_id().cloned()));
                        }
                        TargetMessage::WaitForNavigation(tx) => {
                            if let Some(frame) = self.frame_manager.main_frame() {
                                // TODO submit a navigation watcher: waitForFrameNavigation

                                // TODO return the watchers navigationResponse
                                if frame.is_loaded() {
                                    let _ = tx.send(frame.http_request().cloned());
                                } else {
                                    self.wait_for_frame_navigation.push(tx);
                                }
                            } else {
                                self.wait_for_frame_navigation.push(tx);
                            }
                        }
                        TargetMessage::AddEventListener(req) => {
                            // register a new listener
                            self.event_listeners.add_listener(req);
                        }
                        TargetMessage::GetExecutionContext(ctx) => {
                            let GetExecutionContext {
                                dom_world,
                                frame_id,
                                tx,
                            } = ctx;
                            let frame = if let Some(frame_id) = frame_id {
                                self.frame_manager.frame(&frame_id)
                            } else {
                                self.frame_manager.main_frame()
                            };

                            if let Some(frame) = frame {
                                match dom_world {
                                    DOMWorldKind::Main => {
                                        let _ = tx.send(frame.main_world().execution_context());
                                    }
                                    DOMWorldKind::Secondary => {
                                        let _ =
                                            tx.send(frame.secondary_world().execution_context());
                                    }
                                }
                            } else {
                                let _ = tx.send(None);
                            }
                        }
                        TargetMessage::Authenticate(credentials) => {
                            self.network_manager.authenticate(credentials);
                        }
                    }
                }
            }

            while let Some(event) = self.network_manager.poll() {
                match event {
                    NetworkEvent::SendCdpRequest((method, params)) => {
                        // send a message to the browser
                        self.queued_events.push_back(TargetEvent::Request(Request {
                            method,
                            session_id: self.session_id.clone().map(Into::into),
                            params,
                        }))
                    }
                    NetworkEvent::Request(_) => {}
                    NetworkEvent::Response(_) => {}
                    NetworkEvent::RequestFailed(request) => {
                        self.frame_manager.on_http_request_finished(request);
                    }
                    NetworkEvent::RequestFinished(request) => {
                        self.frame_manager.on_http_request_finished(request);
                    }
                }
            }

            while let Some(event) = self.frame_manager.poll(now) {
                match event {
                    FrameEvent::NavigationResult(res) => {
                        self.queued_events
                            .push_back(TargetEvent::NavigationResult(res));
                    }
                    FrameEvent::NavigationRequest(id, req) => {
                        self.queued_events
                            .push_back(TargetEvent::NavigationRequest(id, req));
                    }
                }
            }

            if self.queued_events.is_empty() {
                return None;
            }
        }
    }

    /// Set the sender half of the channel who requested the creation of this
    /// target
    pub fn set_initiator(&mut self, tx: Sender<Result<Page>>) {
        self.initiator = Some(tx);
    }

    pub(crate) fn page_init_commands(timeout: Duration, stealth_mode: bool) -> CommandChain {
        let attach = SetAutoAttachParams::builder()
            .flatten(true)
            .auto_attach(true)
            // Stealth: don't pause child targets on start — reduces debugger fingerprint
            .wait_for_debugger_on_start(!stealth_mode)
            .build()
            .unwrap();

        let mut cmds = vec![(attach.identifier(), serde_json::to_value(attach).unwrap())];

        // Stealth: skip Performance.enable and Log.enable — pure detection vectors
        // with no functional value for automation
        if !stealth_mode {
            let enable_performance = performance::EnableParams::default();
            let enable_log = cdplog::EnableParams::default();
            cmds.push((
                enable_performance.identifier(),
                serde_json::to_value(enable_performance).unwrap(),
            ));
            cmds.push((
                enable_log.identifier(),
                serde_json::to_value(enable_log).unwrap(),
            ));
        }

        CommandChain::new(cmds, timeout)
    }
}

#[derive(Debug, Clone)]
pub struct TargetConfig {
    pub ignore_https_errors: bool,
    ///  Request timeout to use
    pub request_timeout: Duration,
    pub viewport: Option<Viewport>,
    pub request_intercept: bool,
    pub cache_enabled: bool,
    /// Stealth mode: minimize CDP domain enables to reduce bot detection footprint.
    pub stealth_mode: bool,
}

impl Default for TargetConfig {
    fn default() -> Self {
        Self {
            ignore_https_errors: true,
            request_timeout: Duration::from_secs(REQUEST_TIMEOUT),
            viewport: Default::default(),
            request_intercept: false,
            cache_enabled: true,
            stealth_mode: false,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TargetType {
    Page,
    BackgroundPage,
    ServiceWorker,
    SharedWorker,
    Other,
    Browser,
    Webview,
    Unknown(String),
}

impl TargetType {
    pub fn new(ty: &str) -> Self {
        match ty {
            "page" => TargetType::Page,
            "background_page" => TargetType::BackgroundPage,
            "service_worker" => TargetType::ServiceWorker,
            "shared_worker" => TargetType::SharedWorker,
            "other" => TargetType::Other,
            "browser" => TargetType::Browser,
            "webview" => TargetType::Webview,
            s => TargetType::Unknown(s.to_string()),
        }
    }

    pub fn is_page(&self) -> bool {
        matches!(self, TargetType::Page)
    }

    pub fn is_background_page(&self) -> bool {
        matches!(self, TargetType::BackgroundPage)
    }

    pub fn is_service_worker(&self) -> bool {
        matches!(self, TargetType::ServiceWorker)
    }

    pub fn is_shared_worker(&self) -> bool {
        matches!(self, TargetType::SharedWorker)
    }

    pub fn is_other(&self) -> bool {
        matches!(self, TargetType::Other)
    }

    pub fn is_browser(&self) -> bool {
        matches!(self, TargetType::Browser)
    }

    pub fn is_webview(&self) -> bool {
        matches!(self, TargetType::Webview)
    }
}

#[derive(Debug)]
pub(crate) enum TargetEvent {
    /// An internal request
    Request(Request),
    /// An internal navigation request
    NavigationRequest(NavigationId, Request),
    /// Indicates that a previous requested navigation has finished
    NavigationResult(Result<NavigationOk, NavigationError>),
    /// A new command arrived via a channel
    Command(CommandMessage),
    /// The target finished re-attaching on a new session; the handler also
    /// tells the browser-level listeners (screen-play-rs#1041).
    SessionRestored(EventSessionRestored),
}

// TODO this can be moved into the classes?
#[derive(Debug)]
pub enum TargetInit {
    InitializingFrame(CommandChain),
    InitializingNetwork(CommandChain),
    InitializingPage(CommandChain),
    InitializingEmulation(CommandChain),
    AttachToTarget,
    Initialized,
    Closing,
}

impl TargetInit {
    fn commands_mut(&mut self) -> Option<&mut CommandChain> {
        match self {
            TargetInit::InitializingFrame(cmd) => Some(cmd),
            TargetInit::InitializingNetwork(cmd) => Some(cmd),
            TargetInit::InitializingPage(cmd) => Some(cmd),
            TargetInit::InitializingEmulation(cmd) => Some(cmd),
            TargetInit::AttachToTarget => None,
            TargetInit::Initialized => None,
            TargetInit::Closing => None,
        }
    }
}

#[derive(Debug)]
pub struct GetExecutionContext {
    /// For which world the execution context was requested
    pub dom_world: DOMWorldKind,
    /// The if of the frame to get the `ExecutionContext` for
    pub frame_id: Option<FrameId>,
    /// Sender half of the channel to send the response back
    pub tx: Sender<Option<ExecutionContextId>>,
}

impl GetExecutionContext {
    pub fn new(tx: Sender<Option<ExecutionContextId>>) -> Self {
        Self {
            dom_world: DOMWorldKind::Main,
            frame_id: None,
            tx,
        }
    }
}

#[derive(Debug)]
pub struct GetUrl {
    /// The id of the frame to get the url for (None = main frame)
    pub frame_id: Option<FrameId>,
    /// Sender half of the channel to send the response back
    pub tx: Sender<Option<String>>,
}

impl GetUrl {
    pub fn new(tx: Sender<Option<String>>) -> Self {
        Self { frame_id: None, tx }
    }
}

#[derive(Debug)]
pub struct GetName {
    /// The id of the frame to get the name for (None = main frame)
    pub frame_id: Option<FrameId>,
    /// Sender half of the channel to send the response back
    pub tx: Sender<Option<String>>,
}

#[derive(Debug)]
pub struct GetParent {
    /// The id of the frame to get the parent for (None = main frame)
    pub frame_id: FrameId,
    /// Sender half of the channel to send the response back
    pub tx: Sender<Option<FrameId>>,
}

#[derive(Debug)]
pub enum TargetMessage {
    /// Execute a command within the session of this target
    Command(CommandMessage),
    /// Return the main frame of this target's page
    MainFrame(Sender<Option<FrameId>>),
    /// Return all the frames of this target's page
    AllFrames(Sender<Vec<FrameId>>),
    /// Return the url if available
    Url(GetUrl),
    /// Return the name if available
    Name(GetName),
    /// Return the parent id of a frame
    Parent(GetParent),
    /// A Message that resolves when the frame finished loading a new url
    WaitForNavigation(Sender<ArcHttpRequest>),
    /// A request to submit a new listener that gets notified with every
    /// received event
    AddEventListener(EventListenerRequest),
    /// Get the `ExecutionContext` if available
    GetExecutionContext(GetExecutionContext),
    Authenticate(Credentials),
}

#[cfg(test)]
mod init_error_tests {
    //! screen-play-rs#1035: an error answer to an initialization command fails
    //! the initialization instead of being counted as a completed step.
    use super::*;
    use std::task::Context;

    fn page_target() -> Target {
        let info: TargetInfo = serde_json::from_value(serde_json::json!({
            "targetId": "T1", "type": "page", "title": "", "url": "about:blank",
            "attached": false, "canAccessOpener": false
        }))
        .unwrap();
        let mut t = Target::new(info, TargetConfig::default(), BrowserContext::default());
        t.set_session_id(SessionId::from("S1".to_string()));
        t
    }

    fn reply(error: Option<(i64, &str)>) -> Response {
        Response {
            id: chromiumoxide_types::CallId::new(1),
            result: error.is_none().then(|| serde_json::json!({})),
            error: error.map(|(code, message)| chromiumoxide_types::Error {
                code,
                message: message.to_string(),
            }),
        }
    }

    fn poll(t: &mut Target) -> Option<TargetEvent> {
        let waker = futures::task::noop_waker();
        t.poll(&mut Context::from_waker(&waker), Instant::now())
    }

    #[test]
    fn an_error_answer_fails_initialization_and_closes_the_target() {
        let mut t = page_target();
        let (tx, mut rx) = futures::channel::oneshot::channel();
        t.initiator = Some(tx);
        // Attach is sent; the chain starts.
        assert!(matches!(poll(&mut t), Some(TargetEvent::Request(_))));
        assert!(t.init_state.commands_mut().is_some());

        t.on_response(
            reply(Some((-32001, "Session with given id not found."))),
            "Page.enable",
        );
        match poll(&mut t) {
            Some(TargetEvent::Request(req)) => {
                assert_eq!(req.method, "Target.closeTarget");
                // On the browser session: the page session may be what failed.
                assert!(req.session_id.is_none(), "{req:?}");
                assert_eq!(req.params["targetId"], "T1");
            }
            other => panic!("expected the target to be closed, got {other:?}"),
        }
        assert!(matches!(t.init_state, TargetInit::Closing));
        match rx.try_recv() {
            Ok(Some(Err(CdpError::Chrome(e)))) => {
                assert_eq!(e.message, "Session with given id not found.")
            }
            other => panic!("the waiter should get Chrome's error, got {other:?}"),
        }
    }

    #[test]
    fn a_vanished_frame_refusing_its_isolated_world_is_not_a_failure() {
        let mut t = page_target();
        assert!(matches!(poll(&mut t), Some(TargetEvent::Request(_))));
        t.on_response(
            reply(Some((-32000, "No frame for given id found"))),
            CreateIsolatedWorldParams::IDENTIFIER,
        );
        assert!(t.init_error.is_none());
        assert!(!matches!(t.init_state, TargetInit::Closing));
    }

    #[test]
    fn a_success_answer_still_advances_the_chain() {
        let mut t = page_target();
        assert!(matches!(poll(&mut t), Some(TargetEvent::Request(_))));
        t.on_response(reply(None), "Target.attachToTarget");
        assert!(t.init_error.is_none());
        assert!(!matches!(t.init_state, TargetInit::Closing));
    }

    #[test]
    fn an_error_after_initialization_is_not_an_initialization_failure() {
        let mut t = page_target();
        t.init_state = TargetInit::Initialized;
        t.on_response(reply(Some((-32000, "boom"))), "Runtime.evaluate");
        assert!(t.init_error.is_none());
        assert!(matches!(t.init_state, TargetInit::Initialized));
    }
}

#[cfg(test)]
mod session_recovery_tests {
    //! screen-play-rs#1041: a target that loses its session re-attaches in
    //! place — keeping its network settings and listeners — unless it was
    //! detached on purpose, within a recovery budget.
    use super::*;
    use crate::listeners::{EventListenerRequest, EventStream};
    use futures::StreamExt;
    use std::task::Context;

    fn target() -> Target {
        let info: TargetInfo = serde_json::from_value(serde_json::json!({
            "targetId": "T1", "type": "page", "title": "", "url": "about:blank",
            "attached": false, "canAccessOpener": false
        }))
        .unwrap();
        Target::new(info, TargetConfig::default(), BrowserContext::default())
    }

    fn cx_poll(t: &mut Target) -> Option<TargetEvent> {
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let ev = t.poll(&mut cx, Instant::now());
        t.event_listeners_mut().poll(&mut cx);
        ev
    }

    /// Poll and answer every request with success until the target asks for
    /// nothing more; the attach is answered by giving it `session`. Returns
    /// every request sent, in order.
    fn drive(t: &mut Target, session: &str) -> Vec<Request> {
        let mut sent = Vec::new();
        for _ in 0..500 {
            match cx_poll(t) {
                Some(TargetEvent::Request(req)) => {
                    let method = req.method.to_string();
                    sent.push(req);
                    if method == AttachToTargetParams::IDENTIFIER {
                        t.set_session_id(SessionId::from(session.to_string()));
                    }
                    t.on_response(
                        Response {
                            id: chromiumoxide_types::CallId::new(1),
                            result: Some(serde_json::json!({})),
                            error: None,
                        },
                        &method,
                    );
                }
                Some(_) => {}
                None => break,
            }
        }
        sent
    }

    fn methods(sent: &[Request]) -> Vec<String> {
        sent.iter().map(|r| r.method.to_string()).collect()
    }

    fn initialized(session: &str) -> Target {
        let mut t = target();
        drive(&mut t, session);
        assert!(t.is_initialized());
        assert_eq!(t.session_generation(), 0);
        t
    }

    #[test]
    fn a_lost_session_re_attaches_in_place_and_restores_network_state() {
        let mut t = initialized("S1");
        let (tx, rx) = futures::channel::mpsc::unbounded();
        t.event_listeners_mut()
            .add_listener(EventListenerRequest::new::<EventSessionRestored>(tx));
        let mut restored = EventStream::<EventSessionRestored>::new(rx);
        let mut headers = std::collections::HashMap::new();
        headers.insert("x-a".to_string(), "1".to_string());
        t.network_manager.set_extra_headers(headers);
        drive(&mut t, "S1"); // flush the header command on S1

        t.on_session_lost(Instant::now());
        let sent = drive(&mut t, "S2");
        let names = methods(&sent);
        assert_eq!(names[0], AttachToTargetParams::IDENTIFIER, "{names:?}");
        assert!(names.contains(&"Page.enable".to_string()), "{names:?}");
        // The header the user set is sent again — on the NEW session.
        let reapplied = sent
            .iter()
            .find(|r| r.method == "Network.setExtraHTTPHeaders")
            .unwrap_or_else(|| panic!("headers not re-applied: {names:?}"));
        assert_eq!(reapplied.session_id.as_deref(), Some("S2"));
        assert!(t.is_initialized());
        assert_eq!(t.session_generation(), 1);
        let ev = futures::executor::block_on(restored.next()).unwrap();
        assert_eq!(ev.target_id, "T1");
        assert_eq!(ev.generation, 1);
    }

    #[test]
    fn an_intentional_detach_stays_detached() {
        let mut t = initialized("S1");
        t.set_desired_attached(false);
        t.on_session_lost(Instant::now());
        assert!(
            cx_poll(&mut t).is_none(),
            "no attach after an intentional detach"
        );
        assert!(!t.wants_revival(), "discovery must not revive it either");
        assert_eq!(t.session_generation(), 0);
    }

    #[test]
    fn discovery_revives_only_what_should_be_attached() {
        let mut t = initialized("S1");
        // Recovery gave up (budget) and left it detached.
        for _ in 0..RECOVERY_BUDGET {
            assert!(t.begin_reattach(Instant::now(), false));
            drive(&mut t, "S1");
        }
        t.on_session_lost(Instant::now());
        assert!(cx_poll(&mut t).is_none(), "the budget is spent");
        assert!(t.session_id().is_none());
        // Discovery would revive it only once the window allows.
        assert!(t.wants_revival());
        assert!(!t.begin_reattach(Instant::now(), false));
        assert!(t.begin_reattach(
            Instant::now() + RECOVERY_WINDOW + Duration::from_secs(1),
            false
        ));
    }

    #[test]
    fn rediscovery_re_attaches_a_detached_target_that_should_be_attached() {
        let mut t = initialized("S1");
        let info = t.info().clone();
        // Spend the budget so the lost session is left detached.
        let now = Instant::now();
        for _ in 0..RECOVERY_BUDGET {
            assert!(t.begin_reattach(now, false));
            drive(&mut t, "S1");
        }
        t.on_session_lost(now);
        assert!(cx_poll(&mut t).is_none());
        // Discovery after the window: an attach, on the same target.
        t.on_rediscovered(&info, now + RECOVERY_WINDOW + Duration::from_secs(1));
        let sent = drive(&mut t, "S2");
        assert_eq!(methods(&sent)[0], AttachToTargetParams::IDENTIFIER);
        assert!(t.is_initialized());
        // An intentionally detached one is only refreshed.
        t.set_desired_attached(false);
        t.on_session_lost(now);
        t.on_rediscovered(&info, now + RECOVERY_WINDOW * 3);
        assert!(cx_poll(&mut t).is_none());
    }

    #[test]
    fn the_recovery_budget_stops_an_attach_loop() {
        let mut t = initialized("S1");
        let now = Instant::now();
        let mut attaches = 0;
        for _ in 0..(RECOVERY_BUDGET + 3) {
            t.on_session_lost(now);
            let sent = drive(&mut t, "S1");
            attaches += methods(&sent)
                .iter()
                .filter(|m| *m == AttachToTargetParams::IDENTIFIER)
                .count();
        }
        assert_eq!(attaches, RECOVERY_BUDGET);
    }

    #[test]
    fn a_session_lost_mid_initialization_is_recovered_and_sends_nothing_meanwhile() {
        let mut t = target();
        // Attach, get the session, answer a few init commands.
        assert!(matches!(cx_poll(&mut t), Some(TargetEvent::Request(_))));
        t.set_session_id(SessionId::from("S1".to_string()));
        for _ in 0..3 {
            if let Some(TargetEvent::Request(req)) = cx_poll(&mut t) {
                let m = req.method.to_string();
                t.on_response(
                    Response {
                        id: chromiumoxide_types::CallId::new(1),
                        result: Some(serde_json::json!({})),
                        error: None,
                    },
                    &m,
                );
            }
        }
        assert!(t.init_state.commands_mut().is_some());
        t.on_session_lost(Instant::now());
        let sent = drive(&mut t, "S2");
        let names = methods(&sent);
        assert_eq!(names[0], AttachToTargetParams::IDENTIFIER, "{names:?}");
        assert!(
            sent[1..]
                .iter()
                .all(|r| r.session_id.as_deref() == Some("S2")),
            "{names:?}"
        );
        assert!(t.is_initialized());
    }

    #[test]
    fn no_initialization_state_sends_without_a_session() {
        let mut t = initialized("S1");
        t.set_desired_attached(false);
        // Put it back mid-chain with no session, as an intentional detach
        // during initialization leaves it.
        t.init_state = TargetInit::InitializingNetwork(t.network_manager.init_commands());
        t.on_session_lost(Instant::now());
        assert!(cx_poll(&mut t).is_none());
    }

    #[test]
    fn a_failed_re_attach_does_not_close_the_tab() {
        let mut t = initialized("S1");
        t.on_session_lost(Instant::now());
        assert!(matches!(cx_poll(&mut t), Some(TargetEvent::Request(_)))); // attach
        t.set_session_id(SessionId::from("S2".to_string()));
        t.on_response(
            Response {
                id: chromiumoxide_types::CallId::new(1),
                result: None,
                error: Some(chromiumoxide_types::Error {
                    code: -32001,
                    message: "Session with given id not found.".into(),
                }),
            },
            "Page.enable",
        );
        // Not closed: it tries again (an attach), within the budget.
        match cx_poll(&mut t) {
            Some(TargetEvent::Request(req)) => {
                assert_eq!(req.method, AttachToTargetParams::IDENTIFIER, "{req:?}")
            }
            other => panic!("expected a retry attach, got {other:?}"),
        }
        assert!(!matches!(t.init_state, TargetInit::Closing));
    }

    #[test]
    fn reattach_target_after_an_intentional_detach_hands_back_the_page() {
        let mut t = initialized("S1");
        t.set_desired_attached(false);
        t.on_session_lost(Instant::now());
        assert!(cx_poll(&mut t).is_none());
        let (tx, mut rx) = futures::channel::oneshot::channel();
        t.reattach(Instant::now(), tx);
        assert!(t.desired_attached());
        drive(&mut t, "S2");
        match rx.try_recv() {
            Ok(Some(Ok(page))) => assert_eq!(page.session_id().as_ref(), "S2"),
            other => panic!("expected the re-attached page, got {other:?}"),
        }
        assert_eq!(t.session_generation(), 1);
    }
}
