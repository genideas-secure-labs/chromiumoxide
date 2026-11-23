//! A CDP connection over an owned byte pipe (screen-play-rs#1735, increment B1).
//!
//! Chrome's `--remote-debugging-pipe` speaks CDP JSON delimited by one NUL byte per message, over
//! two inherited handles. This backend speaks the same over any owned Tokio reader and writer, so
//! the existing handler, targets and sessions run unchanged on top of it, without a URL, a
//! listener or a network endpoint.
//!
//! What it guarantees, and how:
//!
//! * **Bounded.** A frame is at most [`MAX_FRAME_BYTES`] (without its NUL) in both directions;
//!   an outgoing frame over it is refused before a byte is written, an incoming one is refused
//!   before the buffer holding it grows past the limit. At most [`MAX_ADMITTED`] commands are held
//!   (the one awaiting its answer plus the queued); the next is refused at admission.
//! * **One command on the wire at a time.** The next command is written only after the previous
//!   one was answered. Events flow in both directions of that: reading never waits for a write.
//! * **Absolute deadlines, honest cancellation.** Each command carries the lifecycle its caller
//!   stamped ([`crate::cmd::lifecycle`]). A queued command whose caller expired or stopped waiting
//!   is dropped with zero bytes written and reported through [`Connection::take_retired`]. Once
//!   any byte of a command was written, its effect cannot be taken back: if its caller expires or
//!   stops waiting before the answer, the connection ends ([`PipeClosed::ActiveExpired`],
//!   [`PipeClosed::ActiveCancelled`]) instead of pretending nothing happened.
//! * **Terminal, never silent.** End of stream, a truncated, empty, malformed or oversized frame,
//!   a read, write or flush error, an answer to no command, or a CDP error answer ends the
//!   connection: both endpoints are dropped, nothing more is admitted or written, and the stream
//!   yields that reason once and then `None` for ever. Nothing is retried or replayed.
//!
//! Diagnostics name methods, ids and sizes only, never payloads.

use std::collections::VecDeque;
use std::fmt;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use chromiumoxide_cdp::cdp::browser_protocol::target::SessionId;
use chromiumoxide_types::{CallId, EventMessage, Message, MethodCall, MethodId};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::cmd::lifecycle::{CallContext, CommandLifecycle, DriverWaker, LifecycleClock, Sleep};
use crate::conn::{Connection, Io};
use crate::error::{PipeClosed, PipeError, Result};

/// The largest frame, excluding its NUL delimiter, a pipe connection sends or accepts.
pub const MAX_FRAME_BYTES: usize = 1 << 20;

/// The most commands a pipe connection holds at once: the one awaiting its answer plus the queued.
pub const MAX_ADMITTED: usize = 128;

/// How much is read from the pipe at once.
const READ_CHUNK: usize = 64 * 1024;

/// What a pipe connection is built with.
#[derive(Debug, Clone)]
pub(crate) struct PipeSettings {
    pub(crate) clock: Arc<dyn LifecycleClock>,
    /// The budget of a command submitted without a lifecycle of its own.
    pub(crate) default_timeout: Duration,
    pub(crate) driver: DriverWaker,
}

/// A command and its frame.
struct Outgoing {
    id: CallId,
    method: MethodId,
    /// The JSON and its NUL.
    bytes: Vec<u8>,
    lifecycle: Arc<CommandLifecycle>,
}

enum Phase {
    /// `written` of its bytes are written.
    Writing(usize),
    /// Written; the flush has not completed.
    Flushing,
    /// Written and flushed; its answer has not arrived.
    Awaiting,
}

struct InFlight {
    out: Outgoing,
    phase: Phase,
}

impl InFlight {
    fn written(&self) -> usize {
        match self.phase {
            Phase::Writing(n) => n,
            Phase::Flushing | Phase::Awaiting => self.out.bytes.len(),
        }
    }
}

enum State {
    Open,
    /// Ended; the reason has not been reported yet.
    Ending(PipeClosed),
    /// Ended and reported: `None` from now on.
    Done(PipeClosed),
}

pub(crate) struct PipeIo {
    reader: Option<Box<dyn AsyncRead + Send + Unpin>>,
    writer: Option<Box<dyn AsyncWrite + Send + Unpin>>,
    settings: PipeSettings,
    queue: VecDeque<Outgoing>,
    in_flight: Option<InFlight>,
    /// Incoming bytes not yet delivered: complete frames, then at most one partial frame.
    rbuf: Vec<u8>,
    scratch: Box<[u8]>,
    /// Commands dropped with zero bytes written, for the owner to settle.
    retired: Vec<CallId>,
    /// The next id to allocate; `None` once every id was used.
    next_id: Option<usize>,
    /// A CDP error answer was delivered: end before anything else is written.
    end_after_delivery: Option<PipeClosed>,
    timer: Option<(Instant, Sleep)>,
    state: State,
}

impl fmt::Debug for PipeIo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PipeIo")
            .field("queued", &self.queue.len())
            .field("in_flight", &self.in_flight.as_ref().map(|f| f.out.id))
            .field("buffered", &self.rbuf.len())
            .field("open", &matches!(self.state, State::Open))
            .finish()
    }
}

impl PipeIo {
    fn new<R, W>(reader: R, writer: W, settings: PipeSettings) -> Self
    where
        R: AsyncRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        Self {
            reader: Some(Box::new(reader)),
            writer: Some(Box::new(writer)),
            settings,
            queue: VecDeque::new(),
            in_flight: None,
            rbuf: Vec::new(),
            scratch: vec![0u8; READ_CHUNK].into_boxed_slice(),
            retired: Vec::new(),
            next_id: Some(0),
            end_after_delivery: None,
            timer: None,
            state: State::Open,
        }
    }

    fn admitted(&self) -> usize {
        self.queue.len() + usize::from(self.in_flight.is_some())
    }

    fn closed_reason(&self) -> Option<PipeClosed> {
        match self.state {
            State::Open => None,
            State::Ending(reason) | State::Done(reason) => Some(reason),
        }
    }

    pub(crate) fn admit(
        &mut self,
        method: MethodId,
        session_id: Option<SessionId>,
        params: serde_json::Value,
        ctx: CallContext,
    ) -> Result<CallId> {
        if let Some(reason) = self.closed_reason() {
            return Err(PipeError::Closed(reason).into());
        }
        if self.admitted() >= MAX_ADMITTED {
            return Err(PipeError::QueueFull(self.admitted()).into());
        }
        let lifecycle = ctx.lifecycle.unwrap_or_else(|| {
            CommandLifecycle::new(
                self.settings.clock.now() + self.settings.default_timeout,
                self.settings.driver.clone(),
            )
        });
        self.check_admissible(&lifecycle)?;
        let id = self.next_id.ok_or(PipeError::IdsExhausted)?;
        let call = MethodCall {
            id: CallId::new(id),
            method: method.clone(),
            session_id: session_id.map(Into::into),
            params,
        };
        let mut bytes = serde_json::to_vec(&call)?;
        if bytes.len() > MAX_FRAME_BYTES {
            return Err(PipeError::FrameTooLarge {
                len: bytes.len(),
                max: MAX_FRAME_BYTES,
            }
            .into());
        }
        // Serializing is not preemptible: check again rather than queue what is already late.
        self.check_admissible(&lifecycle)?;
        bytes.push(0);
        self.next_id = id.checked_add(1);
        self.queue.push_back(Outgoing {
            id: call.id,
            method,
            bytes,
            lifecycle,
        });
        self.settings.driver.wake();
        Ok(call.id)
    }

    fn check_admissible(&self, lifecycle: &CommandLifecycle) -> Result<(), PipeError> {
        if lifecycle.is_cancelled() {
            return Err(PipeError::CancelledBeforeSend);
        }
        if lifecycle.is_expired(self.settings.clock.now()) {
            return Err(PipeError::ExpiredBeforeSend);
        }
        Ok(())
    }

    /// End the connection: drop both endpoints and every command.
    fn end(&mut self, reason: PipeClosed) {
        if !matches!(self.state, State::Open) {
            return;
        }
        if let Some(f) = &self.in_flight {
            tracing::debug!(
                target: "chromiumoxide::conn::pipe",
                method = %f.out.method,
                id = %f.out.id,
                written = f.written(),
                %reason,
                "pipe connection ends with a command on the wire"
            );
        } else {
            tracing::debug!(target: "chromiumoxide::conn::pipe", %reason, "pipe connection ends");
        }
        self.reader = None;
        self.writer = None;
        self.queue.clear();
        self.in_flight = None;
        self.rbuf = Vec::new();
        self.timer = None;
        self.state = State::Ending(reason);
    }

    /// Drop queued commands whose caller is gone or late (zero bytes written), and end the
    /// connection if the command on the wire is.
    fn check_lifecycles(&mut self) -> Result<(), PipeClosed> {
        let now = self.settings.clock.now();
        let retired = &mut self.retired;
        self.queue.retain(|out| {
            let live = out.lifecycle.is_live(now);
            if !live {
                retired.push(out.id);
            }
            live
        });
        if let Some(f) = &self.in_flight {
            if !f.out.lifecycle.is_live(now) {
                if f.written() == 0 {
                    let id = f.out.id;
                    self.in_flight = None;
                    self.retired.push(id);
                } else if f.out.lifecycle.is_expired(now) {
                    // Checked first: a caller whose deadline passed also cancels on its way out,
                    // and the reason is still the deadline.
                    return Err(PipeClosed::ActiveExpired);
                } else {
                    return Err(PipeClosed::ActiveCancelled);
                }
            }
        }
        Ok(())
    }

    /// Write and flush the command on the wire, taking the next from the queue when none is.
    fn poll_write(&mut self, cx: &mut Context<'_>) -> Result<(), PipeClosed> {
        let Some(writer) = self.writer.as_mut() else {
            return Ok(());
        };
        loop {
            if self.in_flight.is_none() {
                let Some(out) = self.queue.pop_front() else {
                    return Ok(());
                };
                tracing::trace!(method = %out.method, id = %out.id, "Sending");
                self.in_flight = Some(InFlight {
                    out,
                    phase: Phase::Writing(0),
                });
            }
            let f = self.in_flight.as_mut().expect("set above");
            match f.phase {
                Phase::Writing(written) => {
                    match Pin::new(&mut **writer).poll_write(cx, &f.out.bytes[written..]) {
                        Poll::Ready(Ok(0)) => {
                            return Err(PipeClosed::Write(io::ErrorKind::WriteZero));
                        }
                        Poll::Ready(Ok(n)) => {
                            let written = written + n;
                            f.phase = if written == f.out.bytes.len() {
                                Phase::Flushing
                            } else {
                                Phase::Writing(written)
                            };
                        }
                        Poll::Ready(Err(e)) => return Err(PipeClosed::Write(e.kind())),
                        Poll::Pending => return Ok(()),
                    }
                }
                Phase::Flushing => match Pin::new(&mut **writer).poll_flush(cx) {
                    Poll::Ready(Ok(())) => f.phase = Phase::Awaiting,
                    Poll::Ready(Err(e)) => return Err(PipeClosed::Flush(e.kind())),
                    Poll::Pending => return Ok(()),
                },
                Phase::Awaiting => return Ok(()),
            }
        }
    }

    /// The next complete frame, reading more when none is buffered.
    fn poll_frame(&mut self, cx: &mut Context<'_>) -> Poll<Result<Vec<u8>, PipeClosed>> {
        loop {
            if let Some(pos) = self.rbuf.iter().position(|b| *b == 0) {
                let mut frame: Vec<u8> = self.rbuf.drain(..=pos).collect();
                frame.pop();
                if frame.is_empty() {
                    return Poll::Ready(Err(PipeClosed::Malformed));
                }
                return Poll::Ready(Ok(frame));
            }
            let Some(reader) = self.reader.as_mut() else {
                return Poll::Pending;
            };
            let mut buf = ReadBuf::new(&mut self.scratch);
            match Pin::new(&mut **reader).poll_read(cx, &mut buf) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(PipeClosed::Read(e.kind()))),
                Poll::Ready(Ok(())) => {
                    let data = buf.filled();
                    if data.is_empty() {
                        return Poll::Ready(Err(if self.rbuf.is_empty() {
                            PipeClosed::Eof
                        } else {
                            PipeClosed::Truncated
                        }));
                    }
                    // The buffer holds no complete frame here, only (part of) the current one.
                    // Refuse before appending if that frame would exceed the limit. Later frames in
                    // this chunk are each shorter than the chunk, which is shorter than the limit.
                    let first = data.iter().position(|b| *b == 0).unwrap_or(data.len());
                    if self.rbuf.len() + first > MAX_FRAME_BYTES {
                        return Poll::Ready(Err(PipeClosed::Oversized));
                    }
                    self.rbuf.extend_from_slice(data);
                }
            }
        }
    }

    /// Wake the driver at the earliest deadline of any held command.
    fn poll_timer(&mut self, cx: &mut Context<'_>) -> bool {
        let earliest = self
            .queue
            .iter()
            .map(|o| o.lifecycle.deadline())
            .chain(self.in_flight.iter().map(|f| f.out.lifecycle.deadline()))
            .min();
        let Some(deadline) = earliest else {
            self.timer = None;
            return false;
        };
        if self.timer.as_ref().is_none_or(|(at, _)| *at != deadline) {
            self.timer = Some((deadline, self.settings.clock.sleep_until(deadline)));
        }
        let (_, sleep) = self.timer.as_mut().expect("set above");
        if sleep.as_mut().poll(cx).is_ready() {
            self.timer = None;
            return true;
        }
        false
    }

    pub(crate) fn poll_next<T: EventMessage>(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Message<T>>>> {
        match self.state {
            State::Done(_) => return Poll::Ready(None),
            State::Ending(reason) => {
                self.state = State::Done(reason);
                return Poll::Ready(Some(Err(PipeError::Closed(reason).into())));
            }
            State::Open => {}
        }
        self.settings.driver.register(cx.waker());
        if let Some(reason) = self.end_after_delivery.take() {
            self.end(reason);
            return self.poll_next(cx);
        }
        loop {
            if let Err(reason) = self.check_lifecycles() {
                self.end(reason);
                return self.poll_next(cx);
            }
            if let Err(reason) = self.poll_write(cx) {
                self.end(reason);
                return self.poll_next(cx);
            }
            match self.poll_frame(cx) {
                Poll::Ready(Ok(frame)) => match self.deliver::<T>(&frame) {
                    Ok(msg) => return Poll::Ready(Some(Ok(msg))),
                    Err(reason) => {
                        self.end(reason);
                        return self.poll_next(cx);
                    }
                },
                Poll::Ready(Err(reason)) => {
                    self.end(reason);
                    return self.poll_next(cx);
                }
                Poll::Pending => {}
            }
            if self.poll_timer(cx) {
                continue;
            }
            return Poll::Pending;
        }
    }

    /// Parse a frame and account for an answer.
    fn deliver<T: EventMessage>(&mut self, frame: &[u8]) -> Result<Message<T>, PipeClosed> {
        let msg = match serde_json::from_slice::<Message<T>>(frame) {
            Ok(msg) => msg,
            Err(err) => {
                // Never the frame, and not serde's message either: it can quote what it rejected.
                tracing::debug!(
                    target: "chromiumoxide::conn::pipe",
                    category = ?err.classify(),
                    line = err.line(),
                    column = err.column(),
                    len = frame.len(),
                    "Failed to parse pipe frame"
                );
                return Err(PipeClosed::Malformed);
            }
        };
        match &msg {
            Message::Response(resp) => {
                // The answer to the command on the wire, once all its bytes are out (an answer
                // can overtake the flush that confirms them).
                let answers = self
                    .in_flight
                    .as_ref()
                    .is_some_and(|f| f.out.id == resp.id && f.written() == f.out.bytes.len());
                if !answers {
                    tracing::debug!(id = %resp.id, "pipe answer to no command on the wire");
                    return Err(PipeClosed::UnexpectedResponse);
                }
                self.in_flight = None;
                tracing::trace!(id = %resp.id, error = resp.error.is_some(), "Received response");
                if resp.error.is_some() {
                    self.end_after_delivery = Some(PipeClosed::ProtocolError);
                }
            }
            Message::Event(event) => {
                tracing::trace!(method = %event.identifier(), "Received event");
            }
        }
        Ok(msg)
    }
}

impl<T: EventMessage + Unpin> Connection<T> {
    /// A connection that owns `reader` and `writer`.
    pub(crate) fn pipe<R, W>(reader: R, writer: W, settings: PipeSettings) -> Result<Self>
    where
        R: AsyncRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        Ok(Self::from_io(Io::Pipe(Box::new(PipeIo::new(
            reader, writer, settings,
        )))))
    }
}

impl<T: EventMessage> Connection<T> {
    /// Admit a command with the lifecycle its caller stamped. On a WebSocket connection the
    /// lifecycle is ignored and this is [`Connection::submit_command`].
    pub(crate) fn submit_command_with(
        &mut self,
        method: MethodId,
        session_id: Option<SessionId>,
        params: serde_json::Value,
        ctx: CallContext,
    ) -> Result<CallId> {
        match &mut self.io {
            Io::Pipe(pipe) => pipe.admit(method, session_id, params, ctx),
            Io::Ws(_) => Ok(self.submit_command(method, session_id, params)?),
        }
    }

    /// Commands dropped with zero bytes written since the last call.
    pub(crate) fn take_retired(&mut self) -> Vec<CallId> {
        match &mut self.io {
            Io::Pipe(pipe) => std::mem::take(&mut pipe.retired),
            Io::Ws(_) => Vec::new(),
        }
    }

    /// Whether this is a pipe connection.
    pub(crate) fn is_pipe(&self) -> bool {
        matches!(self.io, Io::Pipe(_))
    }

    #[cfg(test)]
    fn pipe_io(&self) -> &PipeIo {
        match &self.io {
            Io::Pipe(pipe) => pipe,
            Io::Ws(_) => panic!("not a pipe connection"),
        }
    }

    /// Test seam: the next command id to allocate.
    #[cfg(test)]
    pub(crate) fn set_next_call_id(&mut self, id: usize) {
        if let Io::Pipe(pipe) = &mut self.io {
            pipe.next_id = Some(id);
        }
    }

    /// How many commands are admitted (awaiting an answer or queued).
    #[cfg(test)]
    pub(crate) fn admitted_len(&self) -> usize {
        self.pipe_io().admitted()
    }

    /// How many incoming bytes are buffered.
    #[cfg(test)]
    pub(crate) fn buffered_input_len(&self) -> usize {
        self.pipe_io().rbuf.len()
    }

    /// Bytes of the command being written that have been written, and its length.
    #[cfg(test)]
    pub(crate) fn active_written(&self) -> Option<(usize, usize)> {
        self.pipe_io()
            .in_flight
            .as_ref()
            .map(|f| (f.written(), f.out.bytes.len()))
    }
}

#[cfg(test)]
mod tests;
