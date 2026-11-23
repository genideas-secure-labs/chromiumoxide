use futures::channel::{
    mpsc,
    oneshot::{self, channel as oneshot_channel},
};
use pin_project_lite::pin_project;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::cmd::{CommandMessage, to_command_response};
use crate::error::Result;
use crate::handler::page_channel_error;
use crate::handler::target::TargetMessage;
use chromiumoxide_cdp::cdp::browser_protocol::target::SessionId;
use chromiumoxide_types::{Command, CommandResponse, MethodId, Response};

pin_project! {
    pub struct CommandFuture<T, M = Result<Response>> {
        #[pin]
        rx_command: oneshot::Receiver<M>,
        #[pin]
        target_sender: mpsc::Sender<TargetMessage>,
        // it is used to timeout the command if page was closed while waiting for response
        // (WebSocket pages; a pipe page's deadline is its lifecycle's, see `PipeCall`)
        delay: Option<futures_timer::Delay>,

        message: Option<TargetMessage>,

        method: MethodId,

        // A pipe page's command: serialized, stamped and admitted from its first poll on.
        pipe: Option<Box<PipeCall<T>>>,

        _marker: PhantomData<T>
    }
}

impl<T: Command> CommandFuture<T> {
    pub fn new(
        cmd: T,
        target_sender: mpsc::Sender<TargetMessage>,
        session: Option<SessionId>,
    ) -> Result<Self> {
        let (tx, rx_command) = oneshot_channel::<Result<Response>>();
        let method = cmd.identifier();

        let message = Some(TargetMessage::Command(CommandMessage::with_session(
            cmd, tx, session,
        )?));

        let delay = futures_timer::Delay::new(std::time::Duration::from_millis(
            crate::handler::REQUEST_TIMEOUT,
        ));

        Ok(Self {
            target_sender,
            rx_command,
            message,
            delay: Some(delay),
            method,
            pipe: None,
            _marker: PhantomData,
        })
    }
}

/// A command of a page on a pipe connection (screen-play-rs#1735, #1909).
///
/// Constructing it does nothing observable: it neither serializes the command nor starts a
/// deadline. Its first poll stamps the absolute deadline (the connection's configured budget,
/// or a shorter requested one) on the connection's clock, then serializes the command and starts
/// admission. From then on every poll checks that deadline, including while the page channel is
/// not ready, and a wake is armed for it. Dropping it before it settled cancels the command:
/// the connection drops it unsent, or ends if any byte of it was sent.
#[cfg(feature = "pipe-transport")]
pub(crate) struct PipeCall<T> {
    ctx: crate::cmd::lifecycle::PipeContext,
    timeout: Option<std::time::Duration>,
    cmd: Option<T>,
    session: Option<SessionId>,
    tx: Option<oneshot::Sender<Result<Response>>>,
    started: Option<Started>,
}

#[cfg(feature = "pipe-transport")]
struct Started {
    lifecycle: std::sync::Arc<crate::cmd::lifecycle::CommandLifecycle>,
    guard: crate::cmd::lifecycle::CancelGuard,
    sleep: crate::cmd::lifecycle::Sleep,
}

#[cfg(not(feature = "pipe-transport"))]
pub(crate) struct PipeCall<T>(PhantomData<T>);

#[cfg(feature = "pipe-transport")]
impl<T: Command> CommandFuture<T> {
    pub(crate) fn deferred(
        cmd: T,
        target_sender: mpsc::Sender<TargetMessage>,
        session: Option<SessionId>,
        ctx: crate::cmd::lifecycle::PipeContext,
        timeout: Option<std::time::Duration>,
    ) -> Self {
        let (tx, rx_command) = oneshot_channel::<Result<Response>>();
        let method = cmd.identifier();
        Self {
            target_sender,
            rx_command,
            message: None,
            delay: None,
            method,
            pipe: Some(Box::new(PipeCall {
                ctx,
                timeout,
                cmd: Some(cmd),
                session,
                tx: Some(tx),
                started: None,
            })),
            _marker: PhantomData,
        }
    }
}

impl<T> Future for CommandFuture<T>
where
    T: Command,
{
    type Output = Result<CommandResponse<T::Response>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        #[cfg(feature = "pipe-transport")]
        if let Some(pipe) = this.pipe.as_mut() {
            use crate::cmd::lifecycle::{CallContext, CancelGuard};
            use crate::error::CdpError;

            if pipe.started.is_none() {
                // First poll: stamp, then serialize, checking the budget on both sides of it.
                let lifecycle = pipe.ctx.start(pipe.timeout);
                let sleep = pipe.ctx.clock.sleep_until(lifecycle.deadline());
                pipe.started = Some(Started {
                    lifecycle: lifecycle.clone(),
                    guard: CancelGuard::new(lifecycle.clone()),
                    sleep,
                });
                let (Some(cmd), Some(tx)) = (pipe.cmd.take(), pipe.tx.take()) else {
                    return Poll::Ready(Err(CdpError::NoResponse));
                };
                if lifecycle.is_live(pipe.ctx.clock.now()) {
                    let msg = CommandMessage::with_session(cmd, tx, pipe.session.take())?
                        .with_context(CallContext::of(lifecycle.clone()));
                    if lifecycle.is_live(pipe.ctx.clock.now()) {
                        *this.message = Some(TargetMessage::Command(msg));
                    }
                }
            }
            let started = pipe.started.as_mut().expect("stamped above");
            let expired = started.sleep.as_mut().poll(cx).is_ready()
                || !started.lifecycle.is_live(pipe.ctx.clock.now());
            if expired {
                // Settle now: the connection drops it unsent, or ends if it was sent.
                *this.message = None;
                started.lifecycle.cancel();
                return Poll::Ready(Err(CdpError::Timeout));
            }
            if this.message.is_some() {
                return match this.target_sender.poll_ready(cx) {
                    Poll::Ready(Err(e)) => Poll::Ready(Err(page_channel_error(e))),
                    Poll::Ready(Ok(_)) => {
                        let message = this.message.take().expect("existence checked above");
                        this.target_sender
                            .start_send(message)
                            .map_err(page_channel_error)?;
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                    Poll::Pending => Poll::Pending,
                };
            }
            return match this.rx_command.as_mut().poll(cx) {
                Poll::Ready(Ok(Ok(response))) => {
                    started.guard.disarm();
                    Poll::Ready(to_command_response::<T>(response, this.method.clone()))
                }
                Poll::Ready(Ok(Err(e))) => {
                    started.guard.disarm();
                    Poll::Ready(Err(e))
                }
                Poll::Ready(Err(e)) => Poll::Ready(Err(page_channel_error(e))),
                Poll::Pending => Poll::Pending,
            };
        }

        if this.message.is_some() {
            match this.target_sender.poll_ready(cx) {
                Poll::Ready(Err(e)) => Poll::Ready(Err(page_channel_error(e))),
                Poll::Ready(Ok(_)) => {
                    let message = this.message.take().expect("existence checked above");
                    this.target_sender
                        .start_send(message)
                        .map_err(page_channel_error)?;

                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Poll::Pending => Poll::Pending,
            }
        } else if this
            .delay
            .as_mut()
            .is_some_and(|d| Pin::new(d).poll(cx).is_ready())
        {
            Poll::Ready(Err(crate::error::CdpError::Timeout))
        } else {
            match this.rx_command.as_mut().poll(cx) {
                Poll::Ready(Ok(Ok(response))) => {
                    Poll::Ready(to_command_response::<T>(response, this.method.clone()))
                }
                Poll::Ready(Ok(Err(e))) => Poll::Ready(Err(e)),
                Poll::Ready(Err(e)) => Poll::Ready(Err(page_channel_error(e))),
                Poll::Pending => Poll::Pending,
            }
        }
    }
}
