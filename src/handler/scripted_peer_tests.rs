//! The handler against a scripted CDP peer instead of a browser.
//!
//! A peer that is not a browser-owned session — a relay that lends one tab, say — ends a page,
//! refuses a navigation and closes its socket in ways a browser launched by this crate rarely
//! does, and each of those used to leave the caller waiting. These tests play such a peer over a
//! real WebSocket, so what they measure is the handler's own behaviour, not a browser's.
//!
//! The peer answers every call itself: `Target.getTargets` with one page, `Target.attachToTarget`
//! with that page's session (its `Target.attachedToTarget` first, as Chrome does),
//! `Page.getFrameTree` with one frame per session, `Target.setAutoAttach` with the children a test
//! registered under that session, and everything else with an empty result.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_tungstenite::tungstenite::Message as WsMessage;
use futures::StreamExt;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use chromiumoxide_cdp::cdp::browser_protocol::page::GetFrameTreeParams;
use chromiumoxide_cdp::cdp::browser_protocol::target::{
    EventDetachedFromTarget, EventTargetDestroyed, TargetId,
};

use crate::browser::Browser;
use crate::error::{CdpError, ChannelError};
use crate::handler::{Handler, HandlerConfig};
use crate::page::Page;

const PAGE_TARGET: &str = "PAGE-TARGET-1";
const PAGE_SESSION: &str = "PAGE-SESSION-1";
const PAGE_FRAME: &str = "PAGE-FRAME-1";

/// How long a test waits for something that should happen at once. Far below the crate's 30 s
/// request timeout, which is what every one of these behaviours used to wait out.
const PROMPT: Duration = Duration::from_secs(5);

/// What the peer was told to do, and what it saw.
#[derive(Default)]
struct PeerState {
    /// Every call the peer received, in order.
    calls: Vec<Value>,
    /// The error `Page.navigate` is answered with; `None` answers it and loads the page.
    navigate_error: Option<Value>,
    /// The children each session's `Target.setAutoAttach` attaches: session → (child session,
    /// child target, child frame).
    children: HashMap<String, Vec<(String, String, String)>>,
    /// The frame `Page.getFrameTree` names, per session.
    frames: HashMap<String, String>,
    /// Navigations answered so far, for fresh loader ids.
    navigations: usize,
}

enum Control {
    Send(Value),
    Close,
}

struct Peer {
    url: String,
    state: Arc<Mutex<PeerState>>,
    control: mpsc::UnboundedSender<Control>,
    task: JoinHandle<()>,
}

impl Peer {
    async fn start(setup: impl FnOnce(&mut PeerState)) -> Self {
        let mut state = PeerState::default();
        state
            .frames
            .insert(PAGE_SESSION.to_string(), PAGE_FRAME.to_string());
        setup(&mut state);
        let state = Arc::new(Mutex::new(state));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!(
            "ws://{}/devtools/browser/scripted-peer",
            listener.local_addr().expect("addr")
        );
        let (control, mut control_rx) = mpsc::unbounded_channel();
        let peer_state = Arc::clone(&state);
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut ws = async_tungstenite::tokio::accept_async(stream)
                .await
                .expect("handshake");
            loop {
                tokio::select! {
                    msg = ws.next() => match msg {
                        Some(Ok(WsMessage::Text(text))) => {
                            let call: Value = serde_json::from_str(&text).expect("a JSON call");
                            let out = answer(&peer_state, call);
                            for msg in out {
                                if ws.send(WsMessage::Text(msg.to_string().into())).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Some(Ok(_)) => {}
                        _ => return,
                    },
                    ctl = control_rx.recv() => match ctl {
                        Some(Control::Send(msg)) => {
                            if ws.send(WsMessage::Text(msg.to_string().into())).await.is_err() {
                                return;
                            }
                        }
                        Some(Control::Close) => {
                            let _ = ws.close(None).await;
                            // Keep reading until the client's side of the closing handshake.
                            while let Some(Ok(_)) = ws.next().await {}
                            return;
                        }
                        None => return,
                    },
                }
            }
        });
        Self {
            url,
            state,
            control,
            task,
        }
    }

    fn send(&self, msg: Value) {
        self.control.send(Control::Send(msg)).expect("peer running");
    }

    fn close(&self) {
        self.control.send(Control::Close).expect("peer running");
    }

    /// Drop the TCP connection without a closing handshake.
    fn drop_connection(&self) {
        self.task.abort();
    }

    fn calls(&self, method: &str, session: Option<&str>) -> Vec<Value> {
        self.state
            .lock()
            .unwrap()
            .calls
            .iter()
            .filter(|c| c["method"] == method && c["sessionId"].as_str() == session)
            .cloned()
            .collect()
    }

    /// Wait, up to [`PROMPT`], for a call to `method` on `session`.
    async fn wait_call(&self, method: &str, session: Option<&str>) -> Value {
        let start = Instant::now();
        loop {
            if let Some(call) = self.calls(method, session).into_iter().next() {
                return call;
            }
            assert!(
                start.elapsed() < PROMPT,
                "no {method} on session {session:?} within {PROMPT:?}; calls: {:?}",
                self.state
                    .lock()
                    .unwrap()
                    .calls
                    .iter()
                    .map(|c| format!("{}@{}", c["method"], c["sessionId"]))
                    .collect::<Vec<_>>()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

fn target_info(target: &str, kind: &str, attached: bool) -> Value {
    json!({
        "targetId": target,
        "type": kind,
        "title": "",
        "url": "about:blank",
        "attached": attached,
        "canAccessOpener": false,
    })
}

fn frame(id: &str, url: &str) -> Value {
    json!({
        "id": id,
        "loaderId": format!("{id}-LOADER-0"),
        "url": url,
        "domainAndRegistry": "",
        "securityOrigin": "null",
        "mimeType": "text/html",
        "secureContextType": "InsecureScheme",
        "crossOriginIsolatedContextType": "NotIsolated",
        "gatedAPIFeatures": [],
    })
}

fn tagged(mut msg: Value, session: Option<&str>) -> Value {
    if let Some(session) = session {
        msg["sessionId"] = json!(session);
    }
    msg
}

/// The peer's answer to one call: what it writes, in order.
fn answer(state: &Mutex<PeerState>, call: Value) -> Vec<Value> {
    let mut state = state.lock().unwrap();
    state.calls.push(call.clone());
    let id = call["id"].clone();
    let session = call["sessionId"].as_str().map(str::to_owned);
    let session = session.as_deref();
    let result = |result: Value| tagged(json!({ "id": id, "result": result }), session);
    match call["method"].as_str().unwrap_or_default() {
        "Target.getTargets" => vec![result(
            json!({ "targetInfos": [target_info(PAGE_TARGET, "page", false)] }),
        )],
        "Target.attachToTarget" => vec![
            json!({
                "method": "Target.attachedToTarget",
                "params": {
                    "sessionId": PAGE_SESSION,
                    "targetInfo": target_info(PAGE_TARGET, "page", true),
                    "waitingForDebugger": false,
                },
            }),
            result(json!({ "sessionId": PAGE_SESSION })),
        ],
        "Page.getFrameTree" => {
            let id = state
                .frames
                .get(session.unwrap_or_default())
                .cloned()
                .unwrap_or_else(|| "UNKNOWN-FRAME".to_string());
            vec![result(
                json!({ "frameTree": { "frame": frame(&id, "about:blank") } }),
            )]
        }
        "Page.createIsolatedWorld" => vec![result(json!({ "executionContextId": 1 }))],
        "Page.navigate" => {
            if let Some(error) = state.navigate_error.clone() {
                return vec![tagged(json!({ "id": id, "error": error }), session)];
            }
            state.navigations += 1;
            let loader = format!("NAV-LOADER-{}", state.navigations);
            let frame_id = state
                .frames
                .get(session.unwrap_or_default())
                .cloned()
                .unwrap_or_default();
            let lifecycle = |name: &str| {
                tagged(
                    json!({
                        "method": "Page.lifecycleEvent",
                        "params": { "frameId": frame_id, "loaderId": loader, "name": name, "timestamp": 1.0 },
                    }),
                    session,
                )
            };
            vec![
                result(json!({ "frameId": frame_id, "loaderId": loader })),
                lifecycle("init"),
                lifecycle("load"),
            ]
        }
        "Target.setAutoAttach" => {
            let mut out = vec![result(json!({}))];
            let children = state
                .children
                .get(session.unwrap_or_default())
                .cloned()
                .unwrap_or_default();
            for (child_session, child_target, child_frame) in children {
                state.frames.insert(child_session.clone(), child_frame);
                out.push(tagged(
                    json!({
                        "method": "Target.attachedToTarget",
                        "params": {
                            "sessionId": child_session,
                            "targetInfo": target_info(&child_target, "iframe", true),
                            "waitingForDebugger": true,
                        },
                    }),
                    session,
                ));
            }
            out
        }
        _ => vec![result(json!({}))],
    }
}

/// The handler on its own task, as a caller runs it: polled until it ends (an `Err` item does not
/// stop it), or until `stop` fires. The task gives the handler back, so a test can look inside.
fn run_handler(mut handler: Handler) -> (JoinHandle<Handler>, oneshot::Sender<()>) {
    let (stop, mut stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                item = handler.next() => if item.is_none() { break },
                _ = &mut stopped => break,
            }
        }
        handler
    });
    (task, stop)
}

fn config() -> HandlerConfig {
    HandlerConfig {
        // A frame these tests write wrong must fail the test, not vanish.
        ignore_invalid_messages: false,
        ..HandlerConfig::default()
    }
}

/// Connect to `peer` and adopt its one page, the way a caller adopts a page it did not create.
async fn attach(peer: &Peer) -> (Browser, Page, JoinHandle<Handler>, oneshot::Sender<()>) {
    let (mut browser, handler) = Browser::connect_with_config(peer.url.clone(), config())
        .await
        .expect("connect");
    let (task, stop) = run_handler(handler);
    browser.fetch_targets().await.expect("targets");
    let start = Instant::now();
    let page = loop {
        let pages = browser.pages().await.expect("pages");
        if let [page] = pages.as_slice() {
            if page.mainframe().await.ok().flatten().is_some() {
                break page.clone();
            }
        }
        assert!(start.elapsed() < PROMPT, "the page was not adopted");
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    (browser, page, task, stop)
}

/// The page's ending, as a peer that lends one tab sends it: untagged, at browser level.
fn end_page(peer: &Peer) {
    peer.send(json!({
        "method": "Target.detachedFromTarget",
        "params": { "sessionId": PAGE_SESSION, "targetId": PAGE_TARGET },
    }));
    peer.send(json!({
        "method": "Target.targetDestroyed",
        "params": { "targetId": PAGE_TARGET },
    }));
}

/// U-3 fix 1 (winclaw#2121): the ending of the session's LAST page reaches browser-level
/// listeners. They used to be flushed only inside the per-target loop, so once
/// `Target.targetDestroyed` removed the last target, neither ending event was ever delivered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_last_pages_ending_reaches_browser_listeners() {
    let peer = Peer::start(|_| {}).await;
    let (browser, _page, _task, _stop) = attach(&peer).await;
    let mut detached = browser
        .event_listener::<EventDetachedFromTarget>()
        .await
        .expect("listen");
    let mut destroyed = browser
        .event_listener::<EventTargetDestroyed>()
        .await
        .expect("listen");
    end_page(&peer);
    let detach = tokio::time::timeout(PROMPT, detached.next())
        .await
        .expect("the page's detach did not reach the browser listener")
        .expect("listener open");
    assert_eq!(detach.session_id.as_ref(), PAGE_SESSION);
    let destroy = tokio::time::timeout(PROMPT, destroyed.next())
        .await
        .expect("the page's destruction did not reach the browser listener")
        .expect("listener open");
    assert_eq!(destroy.target_id.as_ref(), PAGE_TARGET);
}

/// U-3 fix 1, the caller's half: once its target ended, every kind of call on the page fails at
/// once with the typed [`ChannelError::PageClosed`], never `send failed because receiver is gone`,
/// and none of them is sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_call_on_an_ended_page_fails_page_closed() {
    let peer = Peer::start(|_| {}).await;
    let (browser, page, _task, _stop) = attach(&peer).await;
    let mut destroyed = browser
        .event_listener::<EventTargetDestroyed>()
        .await
        .expect("listen");
    end_page(&peer);
    // The handler has removed the target once it has handled the destruction. (On a base without
    // fix 1 this wait itself fails; the poll below stands in for it there.)
    let start = Instant::now();
    loop {
        if matches!(
            tokio::time::timeout(Duration::from_millis(50), destroyed.next()).await,
            Ok(Some(_))
        ) {
            break;
        }
        if browser
            .get_page(TargetId::from(PAGE_TARGET.to_string()))
            .await
            .is_err()
        {
            break;
        }
        assert!(start.elapsed() < PROMPT, "the target was never removed");
    }
    let calls: Vec<(&str, crate::error::Result<()>)> = vec![
        ("url", page.url().await.map(|_| ())),
        ("mainframe", page.mainframe().await.map(|_| ())),
        (
            "execute",
            page.execute(GetFrameTreeParams::default())
                .await
                .map(|_| ()),
        ),
        (
            "goto",
            page.goto("http://127.0.0.1:9/next").await.map(|_| ()),
        ),
        ("evaluate", page.evaluate("1").await.map(|_| ())),
    ];
    for (what, result) in calls {
        let err = result.expect_err(what);
        let shown = err.to_string();
        assert!(
            matches!(err, CdpError::ChannelSendError(ChannelError::PageClosed)),
            "{what}: {err:?}"
        );
        assert!(!shown.contains("receiver is gone"), "{what}: {shown}");
    }
    // None of them reached the peer: the target is gone, so there is nothing to send them to.
    assert!(peer.calls("Page.navigate", Some(PAGE_SESSION)).is_empty());
    assert!(
        peer.calls("Runtime.evaluate", Some(PAGE_SESSION))
            .is_empty()
    );
    assert_eq!(peer.calls("Page.getFrameTree", Some(PAGE_SESSION)).len(), 1);
}

/// U-3 fix 2: a closed WebSocket ends the `Handler` stream, so a caller's
/// `while let Some(_) = handler.next().await` returns. The connection's end used to read as
/// "nothing read": the handler returned `Pending`, and never ended.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_handler_ends_when_the_peer_closes_the_socket() {
    let peer = Peer::start(|_| {}).await;
    let (_browser, _page, task, _stop) = attach(&peer).await;
    peer.close();
    let _handler = tokio::time::timeout(PROMPT, task)
        .await
        .expect("the handler did not end after the peer closed the socket")
        .expect("handler task");
}

/// U-3 fix 2, without a closing handshake: the connection's error is one item, and the stream
/// ends after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_handler_ends_when_the_connection_drops() {
    let peer = Peer::start(|_| {}).await;
    let (_browser, _page, task, _stop) = attach(&peer).await;
    peer.drop_connection();
    let _handler = tokio::time::timeout(PROMPT, task)
        .await
        .expect("the handler did not end after the connection dropped")
        .expect("handler task");
}

/// U-3 fix 3: an error answer to `Page.navigate` completes the navigation at once, with that
/// error. It used to be stored while the navigation waited for a page load that never comes,
/// until the 30 s timeout (29.99 s measured on WinClaw's relay). The next navigation on the page
/// is not held behind the failed one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_error_answer_to_navigate_fails_the_navigation_at_once() {
    let peer = Peer::start(|s| {
        s.navigate_error = Some(json!({
            "code": -32000,
            "message": "session_ended: the session ended (devtools)",
            "data": "session_ended",
        }))
    })
    .await;
    let (_browser, page, _task, _stop) = attach(&peer).await;
    let start = Instant::now();
    let got = tokio::time::timeout(PROMPT, page.goto("http://127.0.0.1:9/refused"))
        .await
        .unwrap_or_else(|_| panic!("the refused navigation was still waiting after {PROMPT:?}"));
    let waited = start.elapsed();
    match got {
        Err(CdpError::Chrome(err)) => {
            assert_eq!(err.code, -32000);
            assert!(err.message.starts_with("session_ended"), "{err}");
        }
        other => panic!("expected the peer's error, got {other:?}"),
    }
    assert!(waited < Duration::from_secs(1), "{waited:?}");

    peer.state.lock().unwrap().navigate_error = None;
    tokio::time::timeout(PROMPT, page.goto("http://127.0.0.1:9/next"))
        .await
        .expect("the next navigation was held behind the refused one")
        .expect("the next navigation");
}

/// U-3 fix 4: an `iframe` target auto-attached under the page (an out-of-process frame) is
/// initialised like the root on its own session — its domains enabled, so its events flow, and its
/// own automatic attachment set, so a frame nested inside it attaches and is initialised too. Its
/// events reach its own frame tracking, and a page handle for it finds its frame. It used to get
/// `Runtime.runIfWaitingForDebugger` and nothing else.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_auto_attached_child_is_initialised_like_the_root() {
    const CHILD_SESSION: &str = "CHILD-SESSION-1";
    const CHILD_TARGET: &str = "CHILD-TARGET-1";
    const CHILD_FRAME: &str = "CHILD-FRAME-1";
    const NESTED_SESSION: &str = "NESTED-SESSION-1";
    const NESTED_TARGET: &str = "NESTED-TARGET-1";
    const NESTED_FRAME: &str = "NESTED-FRAME-1";
    let peer = Peer::start(|s| {
        s.children.insert(
            PAGE_SESSION.to_string(),
            vec![(
                CHILD_SESSION.into(),
                CHILD_TARGET.into(),
                CHILD_FRAME.into(),
            )],
        );
        s.children.insert(
            CHILD_SESSION.to_string(),
            vec![(
                NESTED_SESSION.into(),
                NESTED_TARGET.into(),
                NESTED_FRAME.into(),
            )],
        );
    })
    .await;
    let (browser, _page, task, stop) = attach(&peer).await;

    for session in [CHILD_SESSION, NESTED_SESSION] {
        for method in [
            "Runtime.runIfWaitingForDebugger",
            "Page.enable",
            "Page.getFrameTree",
            "Page.setLifecycleEventsEnabled",
            "Runtime.enable",
            "Target.setAutoAttach",
        ] {
            peer.wait_call(method, Some(session)).await;
        }
    }

    // A child's event reaches the child's own frame tracking.
    peer.send(json!({
        "method": "Page.frameNavigated",
        "params": { "frame": frame(CHILD_FRAME, "http://child.test/"), "type": "Navigation" },
        "sessionId": CHILD_SESSION,
    }));
    let child = browser
        .get_page(TargetId::from(CHILD_TARGET.to_string()))
        .await
        .expect("a page handle for the child");
    let start = Instant::now();
    loop {
        if child.url().await.expect("child url").as_deref() == Some("http://child.test/") {
            break;
        }
        assert!(
            start.elapsed() < PROMPT,
            "the child's event did not reach its frame"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        child
            .mainframe()
            .await
            .expect("child frame")
            .map(|f| f.as_ref().to_owned()),
        Some(CHILD_FRAME.to_owned())
    );

    // Unchanged for the caller: still one page, no second attachment, nothing closed.
    assert_eq!(browser.pages().await.expect("pages").len(), 1);
    assert_eq!(peer.calls("Target.attachToTarget", None).len(), 1);
    assert!(peer.calls("Target.closeTarget", None).is_empty());
    for session in [
        Some(PAGE_SESSION),
        Some(CHILD_SESSION),
        Some(NESTED_SESSION),
    ] {
        assert!(peer.calls("Target.closeTarget", session).is_empty());
    }

    let _ = stop.send(());
    let handler = task.await.expect("handler task");
    let nested = handler
        .get_target(&TargetId::from(NESTED_TARGET.to_string()))
        .expect("the nested child is tracked");
    assert_eq!(
        nested.session_id().map(|s| s.as_ref()),
        Some(NESTED_SESSION)
    );
    assert_eq!(
        nested.frame_manager().main_frame().map(|f| f.id().as_ref()),
        Some(NESTED_FRAME)
    );
}
