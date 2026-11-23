//! A CDP connection over an owned byte pipe (screen-play-rs#1735, B1) — compile-only shell.
//!
//! RED commit: the API the tests drive exists, the behaviour does not.

use std::sync::Arc;
use std::time::Duration;

use chromiumoxide_cdp::cdp::browser_protocol::target::SessionId;
use chromiumoxide_types::{CallId, EventMessage, MethodId};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::cmd::lifecycle::{CallContext, DriverWaker, LifecycleClock};
use crate::conn::Connection;
use crate::error::{CdpError, Result};

/// The largest frame, excluding its NUL delimiter, a pipe connection sends or accepts.
pub const MAX_FRAME_BYTES: usize = 1 << 20;

/// The most commands a pipe connection holds at once: the one awaiting its answer plus the queued.
pub const MAX_ADMITTED: usize = 128;

/// What a pipe connection is built with.
#[derive(Debug, Clone)]
pub(crate) struct PipeSettings {
    pub(crate) clock: Arc<dyn LifecycleClock>,
    /// The budget of a command submitted without a lifecycle of its own.
    pub(crate) default_timeout: Duration,
    pub(crate) driver: DriverWaker,
}

impl<T: EventMessage + Unpin> Connection<T> {
    /// A connection that owns `reader` and `writer`.
    pub(crate) fn pipe<R, W>(_reader: R, _writer: W, _settings: PipeSettings) -> Result<Self>
    where
        R: AsyncRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        Err(CdpError::msg("pipe transport: not implemented"))
    }
}

impl<T: EventMessage> Connection<T> {
    /// Admit a command with the lifecycle its caller stamped.
    pub(crate) fn submit_command_with(
        &mut self,
        _method: MethodId,
        _session_id: Option<SessionId>,
        _params: serde_json::Value,
        _ctx: CallContext,
    ) -> Result<CallId> {
        Err(CdpError::msg("pipe transport: not implemented"))
    }

    /// Commands removed with zero bytes written since the last call.
    pub(crate) fn take_retired(&mut self) -> Vec<CallId> {
        Vec::new()
    }

    /// Test seam: the next command id to allocate.
    #[cfg(test)]
    pub(crate) fn set_next_call_id(&mut self, _id: usize) {}

    /// How many commands are admitted (awaiting an answer or queued).
    #[cfg(test)]
    pub(crate) fn admitted_len(&self) -> usize {
        0
    }

    /// How many incoming bytes are buffered.
    #[cfg(test)]
    pub(crate) fn buffered_input_len(&self) -> usize {
        0
    }

    /// Bytes of the command being written that have been written.
    #[cfg(test)]
    pub(crate) fn active_written(&self) -> Option<(usize, usize)> {
        None
    }
}

#[cfg(test)]
mod tests;
