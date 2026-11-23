//! The pipe backend against a scripted peer on in-memory byte pipes (screen-play-rs#1735, B1).
//!
//! These drive the [`Connection`] itself, polled by hand with a no-op waker, so every step is
//! explicit: what the connection wrote, what it delivered, when it ended. The handler-level
//! behaviour is `tests/pipe_handler.rs`.

use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use chromiumoxide_cdp::cdp::CdpEventMessage;
use chromiumoxide_cdp::cdp::events::CdpEvent;
use chromiumoxide_types::{CallId, Message, MethodCall};
use futures::Stream;
use futures::task::{AtomicWaker, noop_waker_ref};
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf, duplex};

use super::{MAX_ADMITTED, MAX_FRAME_BYTES, PipeSettings};
use crate::cmd::lifecycle::{
    CallContext, CommandLifecycle, DriverWaker, LifecycleClock, ManualClock,
};
use crate::conn::Connection;
use crate::error::{CdpError, PipeClosed, PipeError};

type Conn = Connection<CdpEventMessage>;

const LONG: u64 = 30_000;

/// What one round of polling the connection produced.
#[derive(Default)]
struct Pumped {
    msgs: Vec<Message<CdpEventMessage>>,
    errs: Vec<CdpError>,
    ended: bool,
}

/// Poll the connection until it has nothing ready.
fn pump(conn: &mut Conn) -> Pumped {
    let mut cx = Context::from_waker(noop_waker_ref());
    let mut out = Pumped::default();
    for _ in 0..10_000 {
        match Pin::new(&mut *conn).poll_next(&mut cx) {
            Poll::Ready(Some(Ok(msg))) => out.msgs.push(msg),
            Poll::Ready(Some(Err(err))) => out.errs.push(err),
            Poll::Ready(None) => {
                out.ended = true;
                return out;
            }
            Poll::Pending => return out,
        }
    }
    panic!("the connection never returned Pending");
}

/// Read what is buffered on `stream` without waiting; `true` once it reports end of stream.
fn read_available(stream: &mut (impl AsyncRead + Unpin)) -> (Vec<u8>, bool) {
    let mut cx = Context::from_waker(noop_waker_ref());
    let mut out = Vec::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let mut rb = ReadBuf::new(&mut buf);
        match Pin::new(&mut *stream).poll_read(&mut cx, &mut rb) {
            Poll::Ready(Ok(())) if rb.filled().is_empty() => return (out, true),
            Poll::Ready(Ok(())) => out.extend_from_slice(rb.filled()),
            Poll::Ready(Err(e)) => panic!("peer read failed: {e}"),
            Poll::Pending => return (out, false),
        }
    }
}

/// A connection and the peer's ends of its two pipes.
struct Wire {
    conn: Conn,
    /// What the connection writes.
    to_peer: DuplexStream,
    /// What the connection reads. `None` once the peer closed it.
    from_peer: Option<DuplexStream>,
    clock: ManualClock,
    driver: DriverWaker,
}

fn settings(clock: &ManualClock, driver: &DriverWaker) -> PipeSettings {
    PipeSettings {
        clock: Arc::new(clock.clone()),
        default_timeout: Duration::from_millis(LONG),
        driver: driver.clone(),
    }
}

fn wire(out_capacity: usize) -> Wire {
    let (conn_w, to_peer) = duplex(out_capacity);
    let (from_peer, conn_r) = duplex(4 << 20);
    let clock = ManualClock::new();
    let driver: DriverWaker = Arc::new(AtomicWaker::new());
    let conn = Conn::pipe(conn_r, conn_w, settings(&clock, &driver)).expect("pipe connection");
    Wire {
        conn,
        to_peer,
        from_peer: Some(from_peer),
        clock,
        driver,
    }
}

impl Wire {
    fn lifecycle(&self, ms: u64) -> Arc<CommandLifecycle> {
        CommandLifecycle::new(
            self.clock.now() + Duration::from_millis(ms),
            self.driver.clone(),
        )
    }

    fn submit(&mut self, method: &str, params: Value) -> crate::error::Result<CallId> {
        let lc = self.lifecycle(LONG);
        self.submit_with(method, params, lc)
    }

    fn submit_with(
        &mut self,
        method: &str,
        params: Value,
        lc: Arc<CommandLifecycle>,
    ) -> crate::error::Result<CallId> {
        self.conn
            .submit_command_with(method.to_string().into(), None, params, CallContext::of(lc))
    }

    async fn peer_write(&mut self, bytes: &[u8]) {
        self.from_peer
            .as_mut()
            .expect("peer pipe open")
            .write_all(bytes)
            .await
            .expect("peer write");
    }
}

fn frame(v: &Value) -> Vec<u8> {
    let mut b = serde_json::to_vec(v).unwrap();
    b.push(0);
    b
}

fn id_json(id: CallId) -> Value {
    serde_json::to_value(id).unwrap()
}

fn created_event(title: &str) -> Value {
    json!({
        "method": "Target.targetCreated",
        "params": { "targetInfo": {
            "targetId": "T1", "type": "page", "title": title, "url": "about:blank",
            "attached": false, "canAccessOpener": false,
        }},
    })
}

fn created_title(msg: &Message<CdpEventMessage>) -> Option<String> {
    match msg {
        Message::Event(ev) => match &ev.params {
            CdpEvent::TargetTargetCreated(ev) => Some(ev.target_info.title.clone()),
            _ => None,
        },
        Message::Response(_) => None,
    }
}

fn response_id(msg: &Message<CdpEventMessage>) -> Option<CallId> {
    match msg {
        Message::Response(r) => Some(r.id),
        Message::Event(_) => None,
    }
}

fn closed(err: &CdpError) -> Option<PipeClosed> {
    match err {
        CdpError::Pipe(PipeError::Closed(reason)) => Some(*reason),
        _ => None,
    }
}

/// Fragmented, coalesced, escaped and non-ASCII frames arrive whole; every frame sent carries
/// exactly one NUL.
#[tokio::test]
async fn nul_fragmentation_preserves_event_and_reply() {
    let mut w = wire(1 << 20);
    let a = w.submit("Runtime.evaluate", json!({"expression": "a"})).unwrap();
    let b = w.submit("Runtime.evaluate", json!({"expression": "b"})).unwrap();
    let p = pump(&mut w.conn);
    assert!(p.errs.is_empty() && !p.ended, "connection failed early");

    // One command in flight: A, delimited by exactly one NUL at its end.
    let (out, _) = read_available(&mut w.to_peer);
    assert_eq!(out.iter().filter(|b| **b == 0).count(), 1, "A's frame: {out:?}");
    assert_eq!(out.last(), Some(&0));
    let call: Value = serde_json::from_slice(&out[..out.len() - 1]).expect("A is JSON");
    assert_eq!(call["id"], id_json(a));

    // An event and A's reply, coalesced, then cut inside a multi-byte character and inside an
    // escape sequence.
    let title = "한글 \"인용\" \\ é";
    let mut bytes = frame(&created_event(title));
    bytes.extend(frame(&json!({ "id": a, "result": { "v": "값" } })));
    let cut1 = bytes.windows(3).position(|w| w == "한".as_bytes()).unwrap() + 1;
    let cut2 = bytes.windows(2).position(|w| w == b"\\\"").unwrap() + 1;
    let mut got = Vec::new();
    for chunk in [&bytes[..cut1], &bytes[cut1..cut2], &bytes[cut2..]] {
        w.peer_write(chunk).await;
        let p = pump(&mut w.conn);
        assert!(p.errs.is_empty(), "a fragment failed the connection: {:?}", p.errs);
        got.extend(p.msgs);
    }
    assert_eq!(got.len(), 2, "the event and A's reply, each once");
    assert_eq!(created_title(&got[0]).as_deref(), Some(title));
    assert_eq!(response_id(&got[1]), Some(a));

    // A answered: B goes out, again with exactly one NUL.
    let (out, _) = read_available(&mut w.to_peer);
    assert_eq!(out.iter().filter(|b| **b == 0).count(), 1, "B's frame: {out:?}");
    let call: Value = serde_json::from_slice(&out[..out.len() - 1]).expect("B is JSON");
    assert_eq!(call["id"], id_json(b));

    // B's reply and an event in one write.
    let mut bytes = frame(&json!({ "id": b, "result": {} }));
    bytes.extend(frame(&created_event("second")));
    w.peer_write(&bytes).await;
    let p = pump(&mut w.conn);
    assert!(p.errs.is_empty());
    assert_eq!(p.msgs.len(), 2, "B's reply and the second event");
    assert_eq!(response_id(&p.msgs[0]), Some(b));
    assert_eq!(created_title(&p.msgs[1]).as_deref(), Some("second"));
}

/// An event arrives while the connection's write is blocked half-way.
#[tokio::test]
async fn blocked_write_still_delivers_events() {
    let mut w = wire(64);
    w.submit("Runtime.evaluate", json!({"expression": "x".repeat(4096)}))
        .unwrap();
    let p = pump(&mut w.conn);
    assert!(p.errs.is_empty());
    let (written, len) = w.conn.active_written().expect("a command being written");
    assert!(written < len, "the write must be blocked: {written}/{len}");

    w.peer_write(&frame(&created_event("during-write"))).await;
    let p = pump(&mut w.conn);
    assert!(p.errs.is_empty());
    assert_eq!(
        p.msgs.iter().filter_map(created_title).collect::<Vec<_>>(),
        vec!["during-write".to_string()],
        "the event must arrive while the write is blocked"
    );
    let (still, _) = w.conn.active_written().expect("still writing");
    assert!(still < len, "the event was read while the write was blocked");

    // Drain: the write completes once the peer reads.
    let mut seen = Vec::new();
    for _ in 0..1000 {
        let (out, _) = read_available(&mut w.to_peer);
        seen.extend(out);
        if seen.last() == Some(&0) {
            break;
        }
        pump(&mut w.conn);
    }
    assert_eq!(seen.len(), len, "the whole frame was written once");
}

/// The exact-size frame in each direction is accepted; one byte more is refused before the
/// buffer grows past the limit, or before any byte is written; the 129th command is refused.
#[tokio::test]
async fn frame_and_queue_bounds_refuse_before_growth() {
    // Incoming, exact size.
    let mut w = wire(1 << 20);
    let base = serde_json::to_vec(&created_event("")).unwrap().len();
    let title = "a".repeat(MAX_FRAME_BYTES - base);
    let exact = serde_json::to_vec(&created_event(&title)).unwrap();
    assert_eq!(exact.len(), MAX_FRAME_BYTES);
    let mut bytes = exact;
    bytes.push(0);
    w.peer_write(&bytes).await;
    let mut got = Vec::new();
    for _ in 0..100 {
        let p = pump(&mut w.conn);
        assert!(p.errs.is_empty(), "the exact-size frame was refused: {:?}", p.errs);
        got.extend(p.msgs);
        if !got.is_empty() {
            break;
        }
    }
    assert_eq!(got.iter().filter_map(created_title).count(), 1);
    assert_eq!(created_title(&got[0]).map(|t| t.len()), Some(title.len()));

    // Incoming, one byte over, never delimited: refused before the buffer exceeds the limit.
    let over = vec![b'a'; MAX_FRAME_BYTES + 1];
    let mut refused = None;
    for chunk in over.chunks(256 * 1024) {
        w.peer_write(chunk).await;
        let p = pump(&mut w.conn);
        assert!(
            w.conn.buffered_input_len() <= MAX_FRAME_BYTES,
            "buffered {} > {MAX_FRAME_BYTES}",
            w.conn.buffered_input_len()
        );
        if let Some(err) = p.errs.first() {
            refused = closed(err);
            break;
        }
    }
    assert_eq!(refused, Some(PipeClosed::Oversized));

    // Outgoing, exact size: admitted and written whole.
    let mut w = wire(4 << 20);
    let call = |pad: &str, id: usize| MethodCall {
        id: CallId::new(id),
        method: "Runtime.evaluate".into(),
        session_id: None,
        params: json!({ "expression": pad }),
    };
    let base = serde_json::to_vec(&call("", 0)).unwrap().len();
    let pad = "b".repeat(MAX_FRAME_BYTES - base);
    assert_eq!(serde_json::to_vec(&call(&pad, 0)).unwrap().len(), MAX_FRAME_BYTES);
    w.submit("Runtime.evaluate", json!({ "expression": pad }))
        .expect("an exact-size frame is admitted");
    let mut out = Vec::new();
    for _ in 0..1000 {
        pump(&mut w.conn);
        out.extend(read_available(&mut w.to_peer).0);
        if out.last() == Some(&0) {
            break;
        }
    }
    assert_eq!(out.len(), MAX_FRAME_BYTES + 1, "the exact frame plus its NUL");

    // Outgoing, one byte over: refused, zero bytes.
    let pad = "b".repeat(MAX_FRAME_BYTES - base + 1);
    match w.submit("Runtime.evaluate", json!({ "expression": pad })) {
        Err(CdpError::Pipe(PipeError::FrameTooLarge { len, max })) => {
            assert_eq!(len, MAX_FRAME_BYTES + 1);
            assert_eq!(max, MAX_FRAME_BYTES);
        }
        other => panic!("an oversized frame must be refused, got {other:?}"),
    }
    pump(&mut w.conn);
    assert!(read_available(&mut w.to_peer).0.is_empty(), "no byte of it was written");

    // The queue: 128 admitted, the 129th refused.
    let mut w = wire(1 << 20);
    for n in 0..MAX_ADMITTED {
        w.submit("Runtime.evaluate", json!({ "expression": n }))
            .unwrap_or_else(|e| panic!("command {n} refused: {e:?}"));
    }
    assert_eq!(w.conn.admitted_len(), MAX_ADMITTED);
    match w.submit("Runtime.evaluate", json!({ "expression": "one too many" })) {
        Err(CdpError::Pipe(PipeError::QueueFull(n))) => assert_eq!(n, MAX_ADMITTED),
        other => panic!("the 129th command must be refused, got {other:?}"),
    }
    assert_eq!(w.conn.admitted_len(), MAX_ADMITTED);
}

/// One fixture per way the peer can end or corrupt the stream: the connection reports exactly
/// one terminal error, ends, closes its writer, writes nothing more and admits nothing more.
#[tokio::test]
async fn eof_and_invalid_frame_settle_pending_once() {
    let mut big = vec![b'{'; MAX_FRAME_BYTES + 1];
    big.extend_from_slice(b"}");
    let cases: Vec<(&str, Option<Vec<u8>>, PipeClosed)> = vec![
        ("clean EOF", None, PipeClosed::Eof),
        ("truncated", Some(b"{\"id\":0,\"res".to_vec()), PipeClosed::Truncated),
        ("malformed JSON", Some(b"{not json}\0".to_vec()), PipeClosed::Malformed),
        ("raw NUL boundary", Some(b"\0".to_vec()), PipeClosed::Malformed),
        ("oversized", Some(big), PipeClosed::Oversized),
    ];
    for (name, bytes, want) in cases {
        let mut w = wire(1 << 20);
        let a = w.submit("Runtime.evaluate", json!({"expression": "A"})).unwrap();
        pump(&mut w.conn);
        let (a_frame, _) = read_available(&mut w.to_peer);
        let _b = w
            .submit("Runtime.evaluate", json!({"expression": "QUEUED-B"}))
            .unwrap();

        let truncate = want == PipeClosed::Truncated;
        if let Some(bytes) = bytes {
            w.peer_write(&bytes).await;
        }
        if bytes_close(want, truncate) {
            w.from_peer = None;
        }
        let mut errs = Vec::new();
        let mut ended = false;
        for _ in 0..1000 {
            let p = pump(&mut w.conn);
            errs.extend(p.errs);
            if p.ended {
                ended = true;
                break;
            }
        }
        assert_eq!(
            errs.iter().map(closed).collect::<Vec<_>>(),
            vec![Some(want)],
            "{name}: exactly one terminal error"
        );
        assert!(ended, "{name}: the connection must end");
        let again = pump(&mut w.conn);
        assert!(again.ended && again.errs.is_empty(), "{name}: ended stays ended");

        let (rest, eof) = read_available(&mut w.to_peer);
        assert!(rest.is_empty(), "{name}: nothing written after A: {rest:?}");
        assert!(eof, "{name}: the writer must be closed");
        assert!(
            !String::from_utf8_lossy(&a_frame).contains("QUEUED-B"),
            "{name}: B was never sent"
        );
        match w.submit("Runtime.evaluate", json!({"expression": "C"})) {
            Err(e) if closed(&e) == Some(want) => {}
            other => panic!("{name}: a closed connection admits nothing, got {other:?}"),
        }
        let _ = a;
    }
}

/// Whether the case ends by the peer closing its writer.
fn bytes_close(want: PipeClosed, truncate: bool) -> bool {
    want == PipeClosed::Eof || truncate
}

#[derive(Clone, Default)]
struct WriterLog {
    bytes: Arc<Mutex<Vec<u8>>>,
    dropped: Arc<AtomicBool>,
    flushes: Arc<AtomicUsize>,
}

#[derive(Clone, Copy, PartialEq)]
enum Flush {
    PendingThenErr,
    AlwaysPending,
}

/// A writer that accepts `chunk` bytes per call up to `limit`, and flushes as told.
struct ScriptedWriter {
    log: WriterLog,
    chunk: usize,
    limit: usize,
    flush: Flush,
}

impl AsyncWrite for ScriptedWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut bytes = self.log.bytes.lock().unwrap();
        let room = self.limit.saturating_sub(bytes.len());
        if room == 0 {
            return Poll::Pending;
        }
        let n = buf.len().min(self.chunk).min(room);
        bytes.extend_from_slice(&buf[..n]);
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let n = self.log.flushes.fetch_add(1, Ordering::SeqCst);
        match self.flush {
            Flush::PendingThenErr if n == 0 => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Flush::PendingThenErr => Poll::Ready(Err(io::ErrorKind::BrokenPipe.into())),
            Flush::AlwaysPending => Poll::Pending,
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl Drop for ScriptedWriter {
    fn drop(&mut self) {
        self.log.dropped.store(true, Ordering::SeqCst);
    }
}

/// A connection whose writer is scripted; returns the peer's write end (kept open).
fn scripted(
    chunk: usize,
    limit: usize,
    flush: Flush,
) -> (Conn, WriterLog, DuplexStream, ManualClock, DriverWaker) {
    let log = WriterLog::default();
    let writer = ScriptedWriter {
        log: log.clone(),
        chunk,
        limit,
        flush,
    };
    let (peer, conn_r) = duplex(1 << 20);
    let clock = ManualClock::new();
    let driver: DriverWaker = Arc::new(AtomicWaker::new());
    let conn = Conn::pipe(conn_r, writer, settings(&clock, &driver)).expect("pipe connection");
    (conn, log, peer, clock, driver)
}

fn pump_until_end(conn: &mut Conn) -> Vec<CdpError> {
    let mut errs = Vec::new();
    for _ in 0..1000 {
        let p = pump(conn);
        errs.extend(p.errs);
        if p.ended {
            return errs;
        }
    }
    errs
}

/// A flush that fails, and a cancellation while a flush is pending or a write is partial, end
/// the connection: the writer is released, the command is never written again, and nothing more
/// is admitted.
#[tokio::test]
async fn flush_error_and_cancel_cannot_replay() {
    let submit = |conn: &mut Conn, lc: Arc<CommandLifecycle>, expr: &str| {
        conn.submit_command_with(
            "Runtime.evaluate".to_string().into(),
            None,
            json!({ "expression": expr }),
            CallContext::of(lc),
        )
    };
    let lifecycle = |clock: &ManualClock, driver: &DriverWaker| {
        CommandLifecycle::new(clock.now() + Duration::from_millis(LONG), driver.clone())
    };

    // (a) The flush fails after a write in 7-byte pieces.
    let (mut conn, log, _peer, clock, driver) = scripted(7, usize::MAX, Flush::PendingThenErr);
    submit(&mut conn, lifecycle(&clock, &driver), "A").unwrap();
    let errs = pump_until_end(&mut conn);
    assert_eq!(
        errs.iter().map(closed).collect::<Vec<_>>(),
        vec![Some(PipeClosed::Flush(io::ErrorKind::BrokenPipe))],
        "a failed flush ends the connection"
    );
    let written = log.bytes.lock().unwrap().clone();
    assert_eq!(written.iter().filter(|b| **b == 0).count(), 1, "A written exactly once");
    assert!(log.dropped.load(Ordering::SeqCst), "the writer is released");
    assert!(
        submit(&mut conn, lifecycle(&clock, &driver), "B").is_err(),
        "nothing more is admitted"
    );
    assert_eq!(*log.bytes.lock().unwrap(), written, "nothing more is written");

    // (b) Cancelled while its flush is pending.
    let (mut conn, log, _peer, clock, driver) = scripted(1 << 20, usize::MAX, Flush::AlwaysPending);
    let lc = lifecycle(&clock, &driver);
    submit(&mut conn, lc.clone(), "A").unwrap();
    let p = pump(&mut conn);
    assert!(p.errs.is_empty() && !p.ended, "a pending flush is not an error");
    let written = log.bytes.lock().unwrap().clone();
    assert_eq!(written.last(), Some(&0), "A fully written, flush pending");
    lc.cancel();
    let errs = pump_until_end(&mut conn);
    assert_eq!(
        errs.iter().map(closed).collect::<Vec<_>>(),
        vec![Some(PipeClosed::ActiveCancelled)],
        "cancelling a sent command ends the connection"
    );
    assert!(log.dropped.load(Ordering::SeqCst), "the writer is released");
    assert!(submit(&mut conn, lifecycle(&clock, &driver), "B").is_err());
    assert_eq!(*log.bytes.lock().unwrap(), written, "nothing replayed");

    // (c) Cancelled with a strict prefix written.
    let (mut conn, log, _peer, clock, driver) = scripted(1 << 20, 10, Flush::AlwaysPending);
    let lc = lifecycle(&clock, &driver);
    submit(&mut conn, lc.clone(), "A").unwrap();
    let p = pump(&mut conn);
    assert!(p.errs.is_empty() && !p.ended);
    assert_eq!(log.bytes.lock().unwrap().len(), 10, "a strict prefix written");
    lc.cancel();
    let errs = pump_until_end(&mut conn);
    assert_eq!(
        errs.iter().map(closed).collect::<Vec<_>>(),
        vec![Some(PipeClosed::ActiveCancelled)]
    );
    assert!(log.dropped.load(Ordering::SeqCst));
    assert_eq!(log.bytes.lock().unwrap().len(), 10, "no byte after the cancel");
}

/// Ids are allocated with a checked increment: the last id is used once, then admission refuses
/// without writing, and no id wraps to one already used. (The plan places this row in
/// `tests/pipe_handler.rs`; the near-exhaustion seam is crate-private, so it lives here.)
#[tokio::test]
async fn pipe_id_exhaustion_refuses_without_bytes() {
    let mut w = wire(1 << 20);
    w.conn.set_next_call_id(usize::MAX - 1);
    let a = w.submit("Runtime.evaluate", json!({"expression": "A"})).unwrap();
    let b = w.submit("Runtime.evaluate", json!({"expression": "B"})).unwrap();
    assert_eq!(a, CallId::new(usize::MAX - 1));
    assert_eq!(b, CallId::new(usize::MAX));
    match w.submit("Runtime.evaluate", json!({"expression": "WRAPPED-C"})) {
        Err(CdpError::Pipe(PipeError::IdsExhausted)) => {}
        other => panic!("ids are exhausted, got {other:?}"),
    }

    let mut ids = Vec::new();
    for reply_to in [a, b] {
        pump(&mut w.conn);
        let (out, _) = read_available(&mut w.to_peer);
        for f in out.split(|b| *b == 0).filter(|f| !f.is_empty()) {
            let call: Value = serde_json::from_slice(f).unwrap();
            ids.push(call["id"].clone());
        }
        w.peer_write(&frame(&json!({ "id": reply_to, "result": {} })))
            .await;
        let p = pump(&mut w.conn);
        assert!(p.errs.is_empty(), "{:?}", p.errs);
    }
    pump(&mut w.conn);
    let (rest, _) = read_available(&mut w.to_peer);
    assert!(rest.is_empty(), "nothing after the two admitted commands");
    assert_eq!(ids, vec![id_json(a), id_json(b)], "exactly the two ids, no wrap");
}
