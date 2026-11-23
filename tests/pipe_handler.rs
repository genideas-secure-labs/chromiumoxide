//! The real `Browser`/`Page`/`Handler` over an owned byte pipe, against a scripted CDP peer
//! (screen-play-rs#1735, increment B1). No browser, no socket, no network: the pipe is two
//! in-memory Tokio duplex streams, and the peer is a task that reads NUL-delimited calls and
//! answers them as Chrome would.
//!
//! Every deadline here runs on a [`ManualClock`]: it moves only when a test advances it, so a
//! deadline assertion does not depend on how loaded the machine is. Real time is used only for
//! "this should happen promptly" waits, which fail the test when they run out.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use futures::task::{ArcWake, waker};
use futures::{FutureExt, StreamExt};
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use chromiumoxide::cdp::browser_protocol::browser::GetVersionParams;
use chromiumoxide::cdp::browser_protocol::page::NavigateParams;
use chromiumoxide::cdp::browser_protocol::target::{CreateTargetParams, EventTargetCreated};
use chromiumoxide::cdp::js_protocol::runtime::{
    EvaluateParams, EvaluateReturns, EventConsoleApiCalled,
};
use chromiumoxide::cmd::lifecycle::ManualClock;
use chromiumoxide::conn::pipe::{MAX_ADMITTED, MAX_FRAME_BYTES};
use chromiumoxide::error::{CdpError, PipeClosed, PipeError};
use chromiumoxide::handler::{HandlerConfig, REQUEST_TIMEOUT};
use chromiumoxide::types::{Command, Method, MethodId};
use chromiumoxide::{Browser, Handler, Page, PipeConfig, Transport};

const PAGE_TARGET: &str = "PAGE-TARGET-1";
const PAGE_SESSION: &str = "PAGE-SESSION-1";
const PAGE_FRAME: &str = "PAGE-FRAME-1";

/// How long a test waits, in real time, for something that should happen at once.
const PROMPT: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------------------------
// The scripted peer
// ---------------------------------------------------------------------------------------------

#[derive(Default)]
struct PeerState {
    /// Every complete call, in order.
    calls: Vec<Value>,
    /// Every byte read from the browser, delimiters included.
    raw: Vec<u8>,
    /// The browser closed its writer.
    eof: bool,
    /// Read no further than this many bytes in total.
    budget: Option<usize>,
    /// Calls whose text contains one of these are recorded and not answered.
    withheld: Vec<String>,
    /// The withheld calls.
    held: Vec<Value>,
}

enum Ctl {
    Send(Value),
    Raw(Vec<u8>),
    /// Answer the held calls containing this text, and stop withholding it.
    Release(String),
    /// Answer the held calls containing this text with a CDP error.
    ReleaseError(String),
    CloseWrite,
    Budget(Option<usize>),
}

struct PipePeer {
    st: Arc<Mutex<PeerState>>,
    ctl: mpsc::UnboundedSender<Ctl>,
}

fn target_info(target: &str, kind: &str, attached: bool) -> Value {
    json!({
        "targetId": target, "type": kind, "title": "", "url": "about:blank",
        "attached": attached, "canAccessOpener": false,
    })
}

fn frame_json(id: &str) -> Value {
    json!({
        "id": id, "loaderId": format!("{id}-LOADER-0"), "url": "about:blank",
        "domainAndRegistry": "", "securityOrigin": "null", "mimeType": "text/html",
        "secureContextType": "InsecureScheme", "crossOriginIsolatedContextType": "NotIsolated",
        "gatedAPIFeatures": [],
    })
}

fn tagged(mut msg: Value, session: Option<&str>) -> Value {
    if let Some(session) = session {
        msg["sessionId"] = json!(session);
    }
    msg
}

/// The peer's answer to one call, as Chrome would give it.
fn answer(call: &Value, navigations: &mut usize) -> Vec<Value> {
    let id = call["id"].clone();
    let session = call["sessionId"].as_str();
    let result = |r: Value| tagged(json!({ "id": id, "result": r }), session);
    match call["method"].as_str().unwrap_or_default() {
        "Target.getTargets" => vec![result(
            json!({ "targetInfos": [target_info(PAGE_TARGET, "page", false)] }),
        )],
        "Target.attachToTarget" => vec![
            json!({ "method": "Target.attachedToTarget", "params": {
                "sessionId": PAGE_SESSION,
                "targetInfo": target_info(PAGE_TARGET, "page", true),
                "waitingForDebugger": false,
            }}),
            result(json!({ "sessionId": PAGE_SESSION })),
        ],
        "Target.detachFromTarget" => vec![
            result(json!({})),
            json!({ "method": "Target.detachedFromTarget", "params": {
                "sessionId": call["params"]["sessionId"], "targetId": PAGE_TARGET,
            }}),
        ],
        "Page.getFrameTree" => vec![result(
            json!({ "frameTree": { "frame": frame_json(PAGE_FRAME) } }),
        )],
        "Page.createIsolatedWorld" => vec![result(json!({ "executionContextId": 1 }))],
        "Runtime.evaluate" => vec![result(
            json!({ "result": { "type": "number", "value": 7, "description": "7" } }),
        )],
        "Browser.getVersion" => vec![result(json!({
            "protocolVersion": "1.3", "product": "Scripted/1", "revision": "0",
            "userAgent": "scripted", "jsVersion": "0",
        }))],
        "Target.createTarget" => vec![result(json!({ "targetId": "NEW-TARGET" }))],
        "Page.navigate" => {
            *navigations += 1;
            let loader = format!("NAV-LOADER-{navigations}");
            let lifecycle = |name: &str| {
                tagged(
                    json!({ "method": "Page.lifecycleEvent", "params": {
                        "frameId": PAGE_FRAME, "loaderId": loader, "name": name, "timestamp": 1.0,
                    }}),
                    session,
                )
            };
            vec![
                result(json!({ "frameId": PAGE_FRAME, "loaderId": loader })),
                lifecycle("init"),
                lifecycle("load"),
            ]
        }
        _ => vec![result(json!({}))],
    }
}

fn error_answer(call: &Value) -> Value {
    tagged(
        json!({ "id": call["id"], "error": { "code": -32000, "message": "scripted failure" } }),
        call["sessionId"].as_str(),
    )
}

async fn write_frames(wr: &mut Option<DuplexStream>, msgs: &[Value]) {
    if let Some(w) = wr.as_mut() {
        for m in msgs {
            let mut b = serde_json::to_vec(m).unwrap();
            b.push(0);
            if w.write_all(&b).await.is_err() {
                return;
            }
        }
    }
}

impl PipePeer {
    fn spawn(mut rd: DuplexStream, wr: DuplexStream, setup: impl FnOnce(&mut PeerState)) -> Self {
        let mut st = PeerState::default();
        setup(&mut st);
        let st = Arc::new(Mutex::new(st));
        let (ctl, mut ctl_rx) = mpsc::unbounded_channel();
        let peer_st = st.clone();
        tokio::spawn(async move {
            let mut wr = Some(wr);
            let mut buf = vec![0u8; 64 * 1024];
            let mut carry = Vec::new();
            let mut navigations = 0;
            loop {
                let allowed = {
                    let s = peer_st.lock().unwrap();
                    if s.eof {
                        0
                    } else {
                        match s.budget {
                            Some(b) => b.saturating_sub(s.raw.len()),
                            None => buf.len(),
                        }
                        .min(buf.len())
                    }
                };
                tokio::select! {
                    biased;
                    c = ctl_rx.recv() => match c {
                        None => return,
                        Some(Ctl::Send(v)) => write_frames(&mut wr, &[v]).await,
                        Some(Ctl::Raw(b)) => if let Some(w) = wr.as_mut() { let _ = w.write_all(&b).await; },
                        Some(Ctl::CloseWrite) => wr = None,
                        Some(Ctl::Budget(b)) => peer_st.lock().unwrap().budget = b,
                        Some(ctl @ (Ctl::Release(_) | Ctl::ReleaseError(_))) => {
                            let (pat, error) = match ctl {
                                Ctl::Release(p) => (p, false),
                                Ctl::ReleaseError(p) => (p, true),
                                _ => unreachable!(),
                            };
                            let calls: Vec<Value> = {
                                let mut s = peer_st.lock().unwrap();
                                s.withheld.retain(|w| *w != pat);
                                let (take, keep): (Vec<_>, Vec<_>) =
                                    s.held.drain(..).partition(|c| c.to_string().contains(&pat));
                                s.held = keep;
                                take
                            };
                            for call in calls {
                                let out = if error { vec![error_answer(&call)] } else { answer(&call, &mut navigations) };
                                write_frames(&mut wr, &out).await;
                            }
                        }
                    },
                    r = rd.read(&mut buf[..allowed.max(1)]), if allowed > 0 => match r {
                        Ok(0) | Err(_) => peer_st.lock().unwrap().eof = true,
                        Ok(n) => {
                            let mut outs = Vec::new();
                            {
                                let mut s = peer_st.lock().unwrap();
                                s.raw.extend_from_slice(&buf[..n]);
                                carry.extend_from_slice(&buf[..n]);
                                while let Some(pos) = carry.iter().position(|b| *b == 0) {
                                    let frame: Vec<u8> = carry.drain(..=pos).collect();
                                    let call: Value = match serde_json::from_slice(&frame[..frame.len() - 1]) {
                                        Ok(v) => v,
                                        Err(_) => continue,
                                    };
                                    s.calls.push(call.clone());
                                    let text = call.to_string();
                                    if s.withheld.iter().any(|w| text.contains(w.as_str())) {
                                        s.held.push(call);
                                    } else {
                                        outs.push(call);
                                    }
                                }
                            }
                            for call in outs {
                                let out = answer(&call, &mut navigations);
                                write_frames(&mut wr, &out).await;
                            }
                        }
                    },
                }
            }
        });
        Self { st, ctl }
    }

    fn send(&self, v: Value) {
        let _ = self.ctl.send(Ctl::Send(v));
    }
    fn raw(&self, b: Vec<u8>) {
        let _ = self.ctl.send(Ctl::Raw(b));
    }
    fn release(&self, pat: &str) {
        let _ = self.ctl.send(Ctl::Release(pat.to_string()));
    }
    fn release_error(&self, pat: &str) {
        let _ = self.ctl.send(Ctl::ReleaseError(pat.to_string()));
    }
    fn close_write(&self) {
        let _ = self.ctl.send(Ctl::CloseWrite);
    }
    fn budget(&self, b: Option<usize>) {
        let _ = self.ctl.send(Ctl::Budget(b));
    }
    fn raw_len(&self) -> usize {
        self.st.lock().unwrap().raw.len()
    }
    fn raw_contains(&self, text: &str) -> bool {
        let s = self.st.lock().unwrap();
        s.raw.windows(text.len()).any(|w| w == text.as_bytes())
    }
    fn raw_since(&self, from: usize) -> Vec<u8> {
        self.st.lock().unwrap().raw[from..].to_vec()
    }
    fn eof(&self) -> bool {
        self.st.lock().unwrap().eof
    }
    fn calls(&self) -> Vec<Value> {
        self.st.lock().unwrap().calls.clone()
    }
    fn saw_call(&self, text: &str) -> bool {
        self.st
            .lock()
            .unwrap()
            .calls
            .iter()
            .any(|c| c.to_string().contains(text))
    }
}

// ---------------------------------------------------------------------------------------------
// Endpoints that can be told to fail
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Default)]
struct Faults {
    read: Arc<AtomicBool>,
    write: Arc<AtomicBool>,
    flush: Arc<AtomicBool>,
}

struct FaultReader {
    inner: DuplexStream,
    faults: Faults,
}

impl AsyncRead for FaultReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.faults.read.load(Ordering::SeqCst) {
            return Poll::Ready(Err(io::ErrorKind::ConnectionReset.into()));
        }
        let before = buf.filled().len();
        let res = Pin::new(&mut self.inner).poll_read(cx, buf);
        if self.faults.read.load(Ordering::SeqCst) && buf.filled().len() > before {
            return Poll::Ready(Err(io::ErrorKind::ConnectionReset.into()));
        }
        res
    }
}

struct FaultWriter {
    inner: DuplexStream,
    faults: Faults,
}

impl AsyncWrite for FaultWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.faults.write.load(Ordering::SeqCst) {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.faults.flush.load(Ordering::SeqCst) {
            return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
        }
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

// ---------------------------------------------------------------------------------------------
// The handler on its own task, with an explicit hold
// ---------------------------------------------------------------------------------------------

/// Runs the handler as a caller does (`while let Some(_) = handler.next().await`), recording its
/// error items, and can stop polling it on request (`hold`) with an acknowledgement, so a test
/// knows the handler receives nothing until `release`.
struct Driver {
    hold: watch::Sender<bool>,
    held: mpsc::UnboundedReceiver<()>,
    task: JoinHandle<(Handler, Vec<CdpError>)>,
}

impl Driver {
    fn spawn(mut handler: Handler) -> Self {
        let (hold, mut hold_rx) = watch::channel(false);
        let (held_tx, held) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut errs = Vec::new();
            loop {
                if *hold_rx.borrow_and_update() {
                    let _ = held_tx.send(());
                    if hold_rx.changed().await.is_err() {
                        break;
                    }
                    continue;
                }
                tokio::select! {
                    item = handler.next() => match item {
                        Some(Ok(())) => {}
                        Some(Err(e)) => errs.push(e),
                        None => break,
                    },
                    _ = hold_rx.changed() => {}
                }
            }
            (handler, errs)
        });
        Self { hold, held, task }
    }

    async fn hold(&mut self) {
        self.hold.send(true).unwrap();
        tokio::time::timeout(PROMPT, self.held.recv())
            .await
            .expect("the driver did not acknowledge the hold");
    }

    fn release(&self) {
        self.hold.send(false).unwrap();
    }

    fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    /// Wait for the handler to end; returns it and the error items it yielded.
    async fn join(self) -> (Handler, Vec<CdpError>) {
        tokio::time::timeout(PROMPT, self.task)
            .await
            .expect("the handler did not end")
            .expect("the driver panicked")
    }
}

/// After it ended, polling the handler again returns `None` at once, every time.
fn assert_stays_ended(handler: &mut Handler) {
    for n in 0..3 {
        match handler.next().now_or_never() {
            Some(None) => {}
            other => panic!("poll {n} after the end returned {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------------------------

struct Fixture {
    browser: Browser,
    peer: PipePeer,
    driver: Driver,
    clock: ManualClock,
    faults: Faults,
}

struct Opts {
    budget_ms: u64,
    out_capacity: usize,
    ignore_invalid_messages: bool,
}

impl Default for Opts {
    fn default() -> Self {
        Self {
            budget_ms: 30_000,
            out_capacity: 1 << 20,
            ignore_invalid_messages: false,
        }
    }
}

fn start(opts: Opts, setup: impl FnOnce(&mut PeerState)) -> Fixture {
    let (browser_w, peer_r) = tokio::io::duplex(opts.out_capacity);
    let (peer_w, browser_r) = tokio::io::duplex(8 << 20);
    let peer = PipePeer::spawn(peer_r, peer_w, setup);
    let faults = Faults::default();
    let clock = ManualClock::new();
    let config = PipeConfig {
        handler: HandlerConfig {
            request_timeout: Duration::from_millis(opts.budget_ms),
            ignore_invalid_messages: opts.ignore_invalid_messages,
            ..HandlerConfig::default()
        },
        clock: Arc::new(clock.clone()),
    };
    let reader = FaultReader {
        inner: browser_r,
        faults: faults.clone(),
    };
    let writer = FaultWriter {
        inner: browser_w,
        faults: faults.clone(),
    };
    let (browser, handler) = Browser::connect_pipe(reader, writer, config).expect("connect_pipe");
    Fixture {
        browser,
        peer,
        driver: Driver::spawn(handler),
        clock,
        faults,
    }
}

/// Adopt the peer's one page, as a caller adopts a page it did not create.
async fn adopt(f: &mut Fixture) -> Page {
    tokio::time::timeout(PROMPT, f.browser.fetch_targets())
        .await
        .expect("fetch_targets in time")
        .expect("fetch_targets");
    let start = std::time::Instant::now();
    loop {
        let pages = f.browser.pages().await.expect("pages");
        if let [page] = pages.as_slice() {
            if page.mainframe().await.ok().flatten().is_some() {
                return page.clone();
            }
        }
        assert!(start.elapsed() < PROMPT, "the page was not adopted");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn eventually(what: &str, f: impl Fn() -> bool) {
    let start = std::time::Instant::now();
    while !f() {
        assert!(start.elapsed() < PROMPT, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Let the handler process what it was sent (real time; nothing here depends on it being long).
async fn settle() {
    tokio::time::sleep(Duration::from_millis(150)).await;
}

fn eval(marker: &str) -> EvaluateParams {
    EvaluateParams::new(marker)
}

fn closed(err: &CdpError) -> Option<PipeClosed> {
    match err {
        CdpError::Pipe(PipeError::Closed(reason)) => Some(*reason),
        _ => None,
    }
}

async fn joined<T>(h: JoinHandle<T>) -> T {
    tokio::time::timeout(PROMPT, h)
        .await
        .expect("the call did not settle")
        .expect("the call panicked")
}

// ---------------------------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------------------------

/// The real Browser/Page over the pipe: the first request appears on the supplied endpoints, the
/// browser reports no network endpoint, the page's call runs on its own session, and an event
/// from another session does not reach the page.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn page_session_routing_with_decoy() {
    let mut f = start(Opts::default(), |_| {});
    eventually("the first request", || !f.peer.calls().is_empty()).await;
    assert_eq!(
        f.peer.calls()[0]["method"],
        "Target.setDiscoverTargets",
        "the first request is discovery, on the supplied pipe"
    );
    assert_eq!(f.browser.transport(), Transport::Pipe);
    assert_eq!(f.browser.websocket_endpoint(), None);
    assert!(
        f.browser.websocket_address().is_empty(),
        "the legacy address is empty, never a usable endpoint"
    );

    let page = adopt(&mut f).await;
    let mut console = page
        .event_listener::<EventConsoleApiCalled>()
        .await
        .expect("listen");
    let console_event = |session: &str, value: &str| {
        json!({ "method": "Runtime.consoleAPICalled", "sessionId": session, "params": {
            "type": "log", "args": [{ "type": "string", "value": value }],
            "executionContextId": 1, "timestamp": 1.0,
        }})
    };
    f.peer.send(console_event("DECOY-SESSION", "decoy"));
    f.peer.send(console_event(PAGE_SESSION, "real"));
    let ev = tokio::time::timeout(PROMPT, console.next())
        .await
        .expect("the page's event")
        .expect("listener open");
    assert_eq!(
        ev.args[0].value,
        Some(json!("real")),
        "only the page's own session"
    );

    let res = tokio::time::timeout(PROMPT, page.execute(eval("1 + 6")))
        .await
        .expect("evaluate in time")
        .expect("evaluate");
    assert_eq!(res.result.result.value, Some(json!(7)));
    let call = f
        .peer
        .calls()
        .into_iter()
        .find(|c| c["method"] == "Runtime.evaluate")
        .expect("the evaluate call");
    assert_eq!(
        call["sessionId"], PAGE_SESSION,
        "sent on the page's session"
    );
}

/// The WebSocket constructor is unchanged: WebSocket transport, its URL, its request and event.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn websocket_constructor_stays_compatible() {
    use async_tungstenite::tungstenite::Message as WsMessage;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "ws://{}/devtools/browser/scripted",
        listener.local_addr().unwrap()
    );
    let (send_tx, mut send_rx) = mpsc::unbounded_channel::<Value>();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = async_tungstenite::tokio::accept_async(stream)
            .await
            .unwrap();
        let mut navigations = 0;
        loop {
            tokio::select! {
                msg = ws.next() => match msg {
                    Some(Ok(WsMessage::Text(text))) => {
                        let call: Value = serde_json::from_str(&text).unwrap();
                        for out in answer(&call, &mut navigations) {
                            let _ = futures::SinkExt::send(&mut ws, WsMessage::Text(out.to_string().into())).await;
                        }
                    }
                    Some(Ok(_)) => {}
                    _ => return,
                },
                v = send_rx.recv() => match v {
                    Some(v) => { let _ = futures::SinkExt::send(&mut ws, WsMessage::Text(v.to_string().into())).await; }
                    None => return,
                },
            }
        }
    });
    let (browser, mut handler) = Browser::connect(url.clone()).await.expect("connect");
    tokio::spawn(async move { while handler.next().await.is_some() {} });
    assert_eq!(browser.transport(), Transport::WebSocket);
    assert_eq!(browser.websocket_endpoint(), Some(url.as_str()));
    assert_eq!(browser.websocket_address(), &url);
    let version = tokio::time::timeout(PROMPT, browser.version())
        .await
        .expect("version in time")
        .expect("version");
    assert_eq!(version.product, "Scripted/1");
    let mut created = browser
        .event_listener::<EventTargetCreated>()
        .await
        .unwrap();
    send_tx
        .send(json!({ "method": "Target.targetCreated", "params": {
            "targetInfo": target_info("OTHER", "other", false) }}))
        .unwrap();
    let ev = tokio::time::timeout(PROMPT, created.next())
        .await
        .expect("the event")
        .expect("listener open");
    assert_eq!(ev.target_info.target_id.as_ref(), "OTHER");
}

/// A command blocked half-way through its write, under a continuous stream of valid events,
/// times out at its original deadline; the half-written frame ends the connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn absolute_deadline_survives_event_flood() {
    let mut f = start(
        Opts {
            budget_ms: 100,
            out_capacity: 256,
            ..Opts::default()
        },
        |_| {},
    );
    let page = adopt(&mut f).await;
    let from = f.peer.raw_len();
    f.peer.budget(Some(from + 100));
    let marker = format!("FLOOD-MARK{}", "y".repeat(4096));
    let call = tokio::spawn({
        let page = page.clone();
        async move { page.execute(eval(&marker)).await }
    });
    eventually("a strict prefix of the command", || {
        f.peer.raw_len() == from + 100
    })
    .await;

    let flooding = Arc::new(AtomicBool::new(true));
    let flood = tokio::spawn({
        let ctl = f.peer.ctl.clone();
        let flooding = flooding.clone();
        async move {
            let mut n = 0u64;
            while flooding.load(Ordering::SeqCst) {
                n += 1;
                let _ = ctl.send(Ctl::Send(json!({ "method": "Runtime.consoleAPICalled",
                    "sessionId": PAGE_SESSION, "params": {
                    "type": "log", "args": [{ "type": "number", "value": n }],
                    "executionContextId": 1, "timestamp": 1.0 }})));
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    });

    f.clock.advance(Duration::from_millis(99));
    settle().await;
    assert!(!call.is_finished(), "the call must still wait at 99 ms");
    assert!(!f.driver.is_finished(), "the connection is alive at 99 ms");

    f.clock.advance(Duration::from_millis(1));
    let res = joined(call).await;
    assert!(
        matches!(res, Err(CdpError::Timeout)),
        "the original deadline holds under the flood: {:?}",
        res.as_ref().err()
    );
    let (mut handler, errs) = f.driver.join().await;
    flooding.store(false, Ordering::SeqCst);
    let _ = flood.await;
    assert_eq!(
        errs.iter().map(closed).collect::<Vec<_>>(),
        vec![Some(PipeClosed::ActiveExpired)],
        "a half-written command that expired ends the connection"
    );
    assert_stays_ended(&mut handler);
    f.peer.budget(None);
    eventually("the browser's writer closes", || f.peer.eof()).await;
    assert!(
        !f.peer.raw_since(from).contains(&0),
        "the half-written frame was never completed"
    );
}

/// A shorter-budget command queued behind a live one expires in the queue: it times out, the
/// peer never receives a byte of it, the live one completes, and the connection stays usable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_expiry_writes_zero_bytes() {
    let mut f = start(
        Opts {
            budget_ms: 1000,
            ..Opts::default()
        },
        |s| s.withheld.push("A-MARK".into()),
    );
    let page = adopt(&mut f).await;
    let a = tokio::spawn({
        let page = page.clone();
        async move { page.execute(eval("A-MARK")).await }
    });
    eventually("A on the wire", || f.peer.saw_call("A-MARK")).await;
    let b = tokio::spawn({
        let page = page.clone();
        async move {
            page.execute_with_timeout(eval("B-MARK"), Duration::from_millis(100))
                .await
        }
    });
    settle().await;
    assert!(!b.is_finished(), "B waits in the queue");

    f.clock.advance(Duration::from_millis(100));
    let res = joined(b).await;
    assert!(
        matches!(res, Err(CdpError::Timeout)),
        "B times out: {:?}",
        res.as_ref().err()
    );
    f.peer.release("A-MARK");
    let res = joined(a).await;
    assert!(
        res.is_ok(),
        "A completes before its own deadline: {:?}",
        res.as_ref().err()
    );
    settle().await;
    assert!(!f.peer.raw_contains("B-MARK"), "no byte of B was written");

    let res = tokio::time::timeout(PROMPT, page.execute(eval("C-MARK")))
        .await
        .expect("C in time");
    assert!(
        res.is_ok(),
        "the connection stays usable: {:?}",
        res.as_ref().err()
    );
    assert!(!f.peer.raw_contains("B-MARK"));
}

/// Dropping a call whose frame is partly written ends the connection: the peer reads end of
/// stream after the prefix, the call queued behind fails, and the handler ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn caller_drop_retires_partial_write() {
    let mut f = start(
        Opts {
            out_capacity: 64,
            ..Opts::default()
        },
        |_| {},
    );
    let page = adopt(&mut f).await;
    let from = f.peer.raw_len();
    f.peer.budget(Some(from + 40));
    let marker = format!("PART-MARK{}", "z".repeat(4096));
    let a = tokio::spawn({
        let page = page.clone();
        async move { page.execute(eval(&marker)).await }
    });
    eventually("a strict prefix of A", || f.peer.raw_len() == from + 40).await;
    let q = tokio::spawn({
        let page = page.clone();
        async move { page.execute(eval("Q-MARK")).await }
    });
    settle().await;

    a.abort();
    let (mut handler, errs) = f.driver.join().await;
    assert_eq!(
        errs.iter().map(closed).collect::<Vec<_>>(),
        vec![Some(PipeClosed::ActiveCancelled)],
        "dropping a partly written call ends the connection"
    );
    assert_stays_ended(&mut handler);
    let res = joined(q).await;
    assert_eq!(
        res.as_ref().err().and_then(closed),
        Some(PipeClosed::ActiveCancelled),
        "the queued call fails with the connection: {:?}",
        res.as_ref().err()
    );
    f.peer.budget(None);
    eventually("end of stream after the prefix", || f.peer.eof()).await;
    assert!(
        !f.peer.raw_since(from).contains(&0),
        "A was never completed"
    );
    assert!(!f.peer.raw_contains("Q-MARK"), "Q was never written");
}

/// A command fully written and flushed whose answer never comes expires terminally: its caller
/// times out, the connection ends, and the call queued behind it fails with the connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flushed_no_reply_expires_terminally() {
    let mut f = start(
        Opts {
            budget_ms: 1000,
            ..Opts::default()
        },
        |s| s.withheld.push("F-MARK".into()),
    );
    let page = adopt(&mut f).await;
    let a = tokio::spawn({
        let page = page.clone();
        async move {
            page.execute_with_timeout(eval("F-MARK"), Duration::from_millis(100))
                .await
        }
    });
    eventually("A fully received", || f.peer.saw_call("F-MARK")).await;
    let q = tokio::spawn({
        let page = page.clone();
        async move { page.execute(eval("Q2-MARK")).await }
    });
    settle().await;

    f.clock.advance(Duration::from_millis(100));
    let res = joined(a).await;
    assert!(
        matches!(res, Err(CdpError::Timeout)),
        "A times out: {:?}",
        res.as_ref().err()
    );
    let (mut handler, errs) = f.driver.join().await;
    assert_eq!(
        errs.iter().map(closed).collect::<Vec<_>>(),
        vec![Some(PipeClosed::ActiveExpired)]
    );
    assert_stays_ended(&mut handler);
    let res = joined(q).await;
    assert_eq!(
        res.as_ref().err().and_then(closed),
        Some(PipeClosed::ActiveExpired),
        "the queued call settles with the connection, not at its own deadline: {:?}",
        res.as_ref().err()
    );
    eventually("the browser's writer closes", || f.peer.eof()).await;
    assert!(!f.peer.raw_contains("Q2-MARK"));
}

/// Two separate cases. A re-attach cancelled while its attach is on the wire ends the connection;
/// a peer that never answers discovery ends the connection at the constructor's budget.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn caller_drop_during_attach_and_silent_readiness() {
    // (a) cancelled during the attach.
    let mut f = start(Opts::default(), |_| {});
    let page = adopt(&mut f).await;
    let target = page.target_id().clone();
    tokio::time::timeout(PROMPT, f.browser.detach_target(target.clone()))
        .await
        .expect("detach in time")
        .expect("detach");
    settle().await;
    f.peer
        .st
        .lock()
        .unwrap()
        .withheld
        .push("Target.attachToTarget".into());
    let attaches = || {
        f.peer
            .calls()
            .iter()
            .filter(|c| c["method"] == "Target.attachToTarget")
            .count()
    };
    let before = attaches();
    let browser = Arc::new(f.browser);
    let r = tokio::spawn({
        let browser = browser.clone();
        async move { browser.reattach_target(target).await }
    });
    eventually("the re-attach on the wire", || attaches() == before + 1).await;
    r.abort();
    let (mut handler, errs) = f.driver.join().await;
    assert_eq!(
        errs.iter().map(closed).collect::<Vec<_>>(),
        vec![Some(PipeClosed::ActiveCancelled)],
        "the attach inherits the caller's cancellation"
    );
    assert_stays_ended(&mut handler);

    // (b) discovery never answered.
    let f = start(
        Opts {
            budget_ms: 100,
            ..Opts::default()
        },
        |s| s.withheld.push("Target.setDiscoverTargets".into()),
    );
    eventually("discovery on the wire", || {
        f.peer.saw_call("Target.setDiscoverTargets")
    })
    .await;
    let mut browser = f.browser;
    let fetch = tokio::spawn(async move { browser.fetch_targets().await });
    settle().await;
    f.clock.advance(Duration::from_millis(100));
    let (mut handler, errs) = f.driver.join().await;
    assert_eq!(
        errs.iter().map(closed).collect::<Vec<_>>(),
        vec![Some(PipeClosed::ActiveExpired)],
        "unanswered discovery ends the connection at the constructor's budget"
    );
    assert_stays_ended(&mut handler);
    assert!(joined(fetch).await.is_err(), "the queued fetch fails");
}

/// One fresh fixture per way the pipe ends. Pending page, navigation and browser work settles
/// once with the connection's error (not at a timeout: the clock never moves), the handler ends
/// with at most one error item even though the Browser is retained, stays ended, and the
/// retained Browser and Page fail without writing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_browser_terminal_matrix() {
    #[derive(Debug, Clone, Copy)]
    enum Case {
        EofIdle,
        EofPending,
        Malformed(bool),
        Oversized,
        ReadError,
        WriteError,
        FlushError,
    }
    let cases = [
        Case::EofIdle,
        Case::EofPending,
        Case::Malformed(true),
        Case::Malformed(false),
        Case::Oversized,
        Case::ReadError,
        Case::WriteError,
        Case::FlushError,
    ];
    for case in cases {
        let ignore = matches!(case, Case::Malformed(true));
        let mut f = start(
            Opts {
                ignore_invalid_messages: ignore,
                ..Opts::default()
            },
            |s| {
                s.withheld.push("PEND-MARK".into());
                s.withheld.push("Page.navigate".into());
            },
        );
        let page = adopt(&mut f).await;
        let pending = |marker: &'static str| {
            let page = page.clone();
            tokio::spawn(async move { page.execute(eval(marker)).await.map(|_| ()) })
        };
        let mut calls = Vec::new();
        let want = match case {
            Case::EofIdle => {
                f.peer.close_write();
                PipeClosed::Eof
            }
            Case::EofPending => {
                calls.push(pending("PEND-MARK"));
                eventually("the pending call on the wire", || {
                    f.peer.saw_call("PEND-MARK")
                })
                .await;
                let nav_page = page.clone();
                calls.push(tokio::spawn(async move {
                    nav_page
                        .goto(NavigateParams::new("about:blank#pending"))
                        .await
                        .map(|_| ())
                }));
                settle().await;
                f.peer.close_write();
                PipeClosed::Eof
            }
            Case::Malformed(_) => {
                calls.push(pending("PEND-MARK"));
                eventually("the pending call on the wire", || {
                    f.peer.saw_call("PEND-MARK")
                })
                .await;
                f.peer.raw(b"{not json}\0".to_vec());
                PipeClosed::Malformed
            }
            Case::Oversized => {
                calls.push(pending("PEND-MARK"));
                eventually("the pending call on the wire", || {
                    f.peer.saw_call("PEND-MARK")
                })
                .await;
                f.peer.raw(vec![b'{'; MAX_FRAME_BYTES + 1]);
                PipeClosed::Oversized
            }
            Case::ReadError => {
                calls.push(pending("PEND-MARK"));
                eventually("the pending call on the wire", || {
                    f.peer.saw_call("PEND-MARK")
                })
                .await;
                f.faults.read.store(true, Ordering::SeqCst);
                f.peer
                    .send(json!({ "method": "Target.targetCreated", "params": {
                    "targetInfo": target_info("WAKE", "other", false) }}));
                PipeClosed::Read(io::ErrorKind::ConnectionReset)
            }
            Case::WriteError => {
                f.faults.write.store(true, Ordering::SeqCst);
                calls.push(pending("PEND-MARK"));
                PipeClosed::Write(io::ErrorKind::BrokenPipe)
            }
            Case::FlushError => {
                f.faults.flush.store(true, Ordering::SeqCst);
                calls.push(pending("PEND-MARK"));
                PipeClosed::Flush(io::ErrorKind::WriteZero)
            }
        };
        for call in calls {
            let res = joined(call).await;
            assert_eq!(
                res.as_ref().err().and_then(closed),
                Some(want),
                "{case:?}: pending work settles with the connection's error: {res:?}"
            );
        }
        let (mut handler, errs) = f.driver.join().await;
        let expect_items = if matches!(case, Case::EofIdle | Case::EofPending) {
            vec![]
        } else {
            vec![Some(want)]
        };
        assert_eq!(
            errs.iter().map(closed).collect::<Vec<_>>(),
            expect_items,
            "{case:?}: at most one error item, then the end"
        );
        assert_stays_ended(&mut handler);
        let written = f.peer.raw_len();
        let res = tokio::time::timeout(PROMPT, f.browser.execute(GetVersionParams::default()))
            .await
            .expect("a retained browser fails at once");
        assert!(
            res.is_err(),
            "{case:?}: the retained browser fails: {:?}",
            res.as_ref().err()
        );
        let res = tokio::time::timeout(PROMPT, page.execute(eval("AFTER-MARK")))
            .await
            .expect("a retained page fails at once");
        assert!(
            res.is_err(),
            "{case:?}: the retained page fails: {:?}",
            res.as_ref().err()
        );
        assert_eq!(f.peer.raw_len(), written, "{case:?}: nothing more written");
    }
}

/// With the admission limit reached by genuine calls, every kind of operation is refused with
/// the typed error, writes nothing and does not panic the handler; once the queue drains, a new
/// call works.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handler_queue_refusal_is_not_a_panic() {
    let mut f = start(Opts::default(), |s| s.withheld.push("HOLD-MARK".into()));
    let page = adopt(&mut f).await;
    let target = page.target_id().clone();
    let get_targets = |f: &Fixture| {
        f.peer
            .calls()
            .iter()
            .filter(|c| c["method"] == "Target.getTargets")
            .count()
    };
    let fetches = get_targets(&f);

    let mut fill = vec![tokio::spawn({
        let page = page.clone();
        async move { page.execute(eval("HOLD-MARK")).await.map(|_| ()) }
    })];
    eventually("the held call on the wire", || f.peer.saw_call("HOLD-MARK")).await;
    for n in 1..MAX_ADMITTED {
        let page = page.clone();
        fill.push(tokio::spawn(async move {
            page.execute(eval(&format!("FILL-{n}"))).await.map(|_| ())
        }));
    }
    settle().await;

    let queue_full = |what: &str, res: Result<(), CdpError>| match res {
        Err(CdpError::Pipe(PipeError::QueueFull(n))) => assert_eq!(n, MAX_ADMITTED, "{what}"),
        other => panic!("{what}: expected the queue-full refusal, got {other:?}"),
    };
    let t = PROMPT;
    queue_full(
        "fetch_targets",
        tokio::time::timeout(t, f.browser.fetch_targets())
            .await
            .unwrap()
            .map(|_| ()),
    );
    queue_full(
        "page command",
        tokio::time::timeout(t, page.execute(eval("REFUSED-CMD")))
            .await
            .unwrap()
            .map(|_| ()),
    );
    queue_full(
        "navigation",
        tokio::time::timeout(t, page.goto(NavigateParams::new("about:blank#refused")))
            .await
            .unwrap()
            .map(|_| ()),
    );
    queue_full(
        "browser command",
        tokio::time::timeout(t, f.browser.execute(GetVersionParams::default()))
            .await
            .unwrap()
            .map(|_| ()),
    );
    queue_full(
        "create target",
        tokio::time::timeout(
            t,
            f.browser.new_page(CreateTargetParams::new("about:blank")),
        )
        .await
        .unwrap()
        .map(|_| ()),
    );
    queue_full(
        "detach",
        tokio::time::timeout(t, f.browser.detach_target(target))
            .await
            .unwrap(),
    );
    queue_full(
        "close",
        tokio::time::timeout(t, f.browser.close())
            .await
            .unwrap()
            .map(|_| ()),
    );
    assert!(!f.driver.is_finished(), "the handler is still running");
    for refused in [
        "REFUSED-CMD",
        "about:blank#refused",
        "Browser.getVersion",
        "Target.createTarget",
        "Target.detachFromTarget",
        "Browser.close",
    ] {
        assert!(!f.peer.raw_contains(refused), "{refused} was written");
    }
    assert_eq!(
        get_targets(&f),
        fetches,
        "the refused fetch was not written"
    );

    f.peer.release("HOLD-MARK");
    for call in fill {
        let res = joined(call).await;
        assert!(res.is_ok(), "an admitted call completes: {res:?}");
    }
    let res = tokio::time::timeout(PROMPT, page.execute(eval("AFTER-MARK")))
        .await
        .expect("in time");
    assert!(
        res.is_ok(),
        "after the queue drains a new call works: {:?}",
        res.as_ref().err()
    );
}

/// A valid CDP error answer goes to its caller; the connection then ends rather than guess what
/// the browser did, and the call queued behind it fails with the connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdp_error_reply_is_terminally_safe() {
    let mut f = start(Opts::default(), |s| s.withheld.push("ERR-MARK".into()));
    let page = adopt(&mut f).await;
    let a = tokio::spawn({
        let page = page.clone();
        async move { page.execute(eval("ERR-MARK")).await.map(|_| ()) }
    });
    eventually("A on the wire", || f.peer.saw_call("ERR-MARK")).await;
    let q = tokio::spawn({
        let page = page.clone();
        async move { page.execute(eval("QERR-MARK")).await.map(|_| ()) }
    });
    settle().await;
    f.peer.release_error("ERR-MARK");

    match joined(a).await {
        Err(CdpError::Chrome(err)) => assert_eq!(err.message, "scripted failure"),
        other => panic!("A gets its method error, got {other:?}"),
    }
    let res = joined(q).await;
    assert_eq!(
        res.as_ref().err().and_then(closed),
        Some(PipeClosed::ProtocolError),
        "the queued call fails with the connection: {res:?}"
    );
    let (mut handler, errs) = f.driver.join().await;
    assert_eq!(
        errs.iter().map(closed).collect::<Vec<_>>(),
        vec![Some(PipeClosed::ProtocolError)]
    );
    assert_stays_ended(&mut handler);
    assert!(!f.peer.raw_contains("QERR-MARK"));
}

/// The deadline starts at the API call's first poll, before the handler has even received it:
/// with the handler held, a short-budget call times out at its own deadline, and once the
/// handler resumes, no byte of it is written; the live call and the connection are unharmed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn api_entry_expiry_before_handler_admission() {
    let mut f = start(
        Opts {
            budget_ms: 1000,
            ..Opts::default()
        },
        |s| s.withheld.push("A11-MARK".into()),
    );
    let page = adopt(&mut f).await;
    let a = tokio::spawn({
        let page = page.clone();
        async move { page.execute(eval("A11-MARK")).await.map(|_| ()) }
    });
    eventually("A on the wire", || f.peer.saw_call("A11-MARK")).await;

    f.driver.hold().await;
    let mut b = Box::pin(page.execute_with_timeout(eval("B11-MARK"), Duration::from_millis(50)));
    assert!(futures::poll!(b.as_mut()).is_pending(), "first poll at t=0");
    f.clock.advance(Duration::from_millis(50));
    match futures::poll!(b.as_mut()) {
        Poll::Ready(Err(CdpError::Timeout)) => {}
        other => panic!(
            "B times out at its entry deadline while the handler is held: {:?}",
            other.map(|r| r.map(|_| ()))
        ),
    }
    drop(b);
    f.driver.release();
    settle().await;
    assert!(
        !f.peer.raw_contains("B11-MARK"),
        "no byte of B after intake resumes"
    );

    f.peer.release("A11-MARK");
    let res = joined(a).await;
    assert!(res.is_ok(), "A completes: {res:?}");
    let res = tokio::time::timeout(PROMPT, page.execute(eval("C11-MARK")))
        .await
        .expect("in time");
    assert!(
        res.is_ok(),
        "the connection accepts a new call: {:?}",
        res.as_ref().err()
    );
    assert!(!f.peer.raw_contains("B11-MARK"));
}

/// `Runtime.evaluate` that counts how often it is serialized.
#[derive(Debug, Clone)]
struct CountingEvaluate {
    marker: String,
    serialized: Arc<AtomicUsize>,
}

impl serde::Serialize for CountingEvaluate {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.serialized.fetch_add(1, Ordering::SeqCst);
        EvaluateParams::new(self.marker.clone()).serialize(s)
    }
}

impl Method for CountingEvaluate {
    fn identifier(&self) -> MethodId {
        "Runtime.evaluate".into()
    }
}

impl Command for CountingEvaluate {
    type Response = EvaluateReturns;
}

#[derive(Default)]
struct FlagWaker(AtomicBool);

impl ArcWake for FlagWaker {
    fn wake_by_ref(arc: &Arc<Self>) {
        arc.0.store(true, Ordering::SeqCst);
    }
}

/// screen-play-rs#1909: the deadline of a public `Page::command_future` starts at its first
/// poll, with the configured budget, not at construction. Budget 100 ms (not `REQUEST_TIMEOUT`);
/// constructed at t=0, first polled at t=60 with admission held; still Pending at t=120 (after a
/// construction-time deadline, before the first-poll one); woken and timed out at t=161 with
/// admission still held, having written zero bytes. Constructing does not serialize.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn command_future_deadline_starts_at_first_poll() {
    let budget = Duration::from_millis(100);
    assert_ne!(budget, Duration::from_millis(REQUEST_TIMEOUT));
    let mut f = start(
        Opts {
            budget_ms: 100,
            ..Opts::default()
        },
        |_| {},
    );
    let page = adopt(&mut f).await;
    assert_eq!(f.clock.elapsed(), Duration::ZERO, "t=0 at construction");
    f.driver.hold().await;

    let serialized = Arc::new(AtomicUsize::new(0));
    let cmd = CountingEvaluate {
        marker: "F1909-MARK".into(),
        serialized: serialized.clone(),
    };
    let mut fut = Box::pin(page.command_future(cmd).expect("command_future"));
    assert_eq!(
        serialized.load(Ordering::SeqCst),
        0,
        "constructing does not serialize"
    );

    f.clock.advance_to(Duration::from_millis(60));
    assert_eq!(
        serialized.load(Ordering::SeqCst),
        0,
        "nor does time passing unpolled"
    );
    let flag = Arc::new(FlagWaker::default());
    let w = waker(flag.clone());
    let mut cx = Context::from_waker(&w);
    assert!(
        fut.as_mut().poll(&mut cx).is_pending(),
        "first poll at t=60"
    );
    assert_eq!(
        serialized.load(Ordering::SeqCst),
        1,
        "serialized at the first poll"
    );

    f.clock.advance_to(Duration::from_millis(120));
    let at_120 = fut.as_mut().poll(&mut cx);
    assert!(
        at_120.is_pending(),
        "#1909 item 2: still Pending at t=120 (a construction-time deadline would have fired at \
         t=100): {:?}",
        at_120.map(|r| r.map(|_| ()))
    );

    flag.0.store(false, Ordering::SeqCst);
    f.clock.advance_to(Duration::from_millis(161));
    assert!(
        flag.0.load(Ordering::SeqCst),
        "#1909 item 3: the deadline wakes the future while admission is held"
    );
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(Err(CdpError::Timeout)) => {}
        other => panic!(
            "#1909 item 3: Timeout at t=161 while admission is held: {:?}",
            other.map(|r| r.map(|_| ()))
        ),
    }
    assert!(!f.peer.raw_contains("F1909-MARK"), "zero bytes while held");
    drop(fut);

    f.driver.release();
    settle().await;
    assert!(
        !f.peer.raw_contains("F1909-MARK"),
        "zero bytes after intake resumes"
    );
    let res = tokio::time::timeout(PROMPT, page.execute(eval("AFTER-1909")))
        .await
        .expect("in time");
    assert!(
        res.is_ok(),
        "the connection is unharmed: {:?}",
        res.as_ref().err()
    );
}
