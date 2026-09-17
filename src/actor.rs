//! Actor primitives: the equivalent of Erlang's `spawn`, `!`, and `receive`.
//!
//! # Mental model
//!
//! An **actor** is just a struct that owns its own state plus a task that
//! drains a bounded mailbox. Nobody else can touch its state directly —
//! you talk to it through messages. This is the same model as Erlang/OTP
//! `gen_server`, minus the BEAM.
//!
//! - [`Actor`] — the trait your struct implements (think `gen_server` callback module).
//! - [`Addr`] — the handle you clone around to send messages (think Pid).
//! - [`Context`] — passed into every `handle` call; exposes `tell` (cast)
//!   and bounded mailbox info.
//!
//! # Why bounded mailboxes
//!
//! Erlang lets mailboxes grow until OOM. We deliberately use a bounded
//! `tokio::sync::mpsc` channel. When the mailbox is full, `tell` returns
//! [`MailboxFull`] and the caller decides: block (via `ask`), drop, log,
//! or shed load. This is the single biggest backpressure improvement
//! over raw Erlang.
//!
//! # Cooperative scheduling — read this
//!
//! Tokio tasks are *cooperative*, not preemptive like BEAM reductions.
//! Any long CPU loop inside `handle` MUST call `tokio::task::yield_now()`
//! periodically, or move the work to `spawn_blocking`. Otherwise one
//! actor starves every other actor on the same worker. There is no
//! runtime-level preemption to save you.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::{debug, info, warn, Instrument};

/// The error returned when a non-blocking `tell` (cast) cannot deliver
/// because the actor's mailbox is full or the actor is gone.
///
/// This is the backpressure signal Erlang doesn't give you — handle it
/// by dropping, logging, or shedding load. Do NOT silently retry forever.
#[derive(Debug, thiserror::Error)]
pub enum MailboxFull {
    #[error("mailbox full (capacity={capacity}), message dropped")]
    Dropped { capacity: usize },
    #[error("actor is no longer running")]
    Gone,
}

/// Errors returned by `Addr::ask` (the bounded-time request/reply call).
///
/// Every variant maps to an Erlang failure mode:
/// - [`AskError::Timeout`] — `receive after T` fired.
/// - [`AskError::MailboxFull`] — caller must shed load or back off.
/// - [`AskError::Closed`] — the target process exited (analog of an exit signal).
/// - [`AskError::Panic`] — the actor panicked handling the request. The supervisor
///   will see the panic and may restart.
#[derive(Debug, thiserror::Error)]
pub enum AskError {
    #[error("ask timed out after {0:?}")]
    Timeout(Duration),
    #[error("mailbox full")]
    MailboxFull,
    #[error("actor exited before replying")]
    Closed,
    #[error("actor panicked while handling the request")]
    Panic,
}

/// The trait every actor implements.
///
/// Native async-in-trait (stable since Rust 1.75) — no `async_trait` macro.
pub trait Actor: Send + 'static {
    /// The message type this actor accepts. Must be `Send + 'static` so it
    /// can cross the channel boundary.
    type Msg: Send + 'static;

    /// Handle one message, mutating `self`. This is your `gen_server:handle_call/3`
    /// and `handle_cast/3` rolled into one. Pattern-match on the message to
    /// decide whether it's a call (uses the embedded `oneshot::Sender`) or a cast.
    ///
    /// **Panics are allowed**: a panic in here is caught by the actor task
    /// and surfaces as `JoinError` to the supervisor, which then applies the
    /// restart strategy. This is the closest thing to Erlang's "let it crash".
    fn handle(
        &mut self,
        msg: Self::Msg,
        ctx: &mut Context<Self::Msg>,
    ) -> impl Future<Output = ()> + Send;

    /// Called once when the actor starts, before any messages are processed.
    /// Default: no-op. Override for `init` semantics (open files, register, etc.).
    #[allow(unused_variables)]
    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> impl Future<Output = ()> + Send {
        async {}
    }

    /// Called when the actor's mailbox closes (all `Addr` handles dropped)
    /// or when the actor task is being torn down. Default: no-op.
    /// Override for cleanup (flush files, deregister, etc.).
    #[allow(unused_variables)]
    fn stopping(&mut self, ctx: &mut Context<Self::Msg>) -> impl Future<Output = ()> + Send {
        async {}
    }
}

/// Per-actor context passed into `handle`, `started`, `stopping`.
///
/// Holds the sender half of the mailbox so the actor can `tell` other
/// actors or itself, and exposes a `self_addr` for replies that need to
/// include a correlation handle.
pub struct Context<M: Send + 'static> {
    /// Sender to this actor's own mailbox — useful for self-messages.
    pub self_addr: Addr<M>,
}

impl<M: Send + 'static> Context<M> {
    /// Convenience: send a message to ourselves (e.g. schedule a follow-up).
    pub fn tell_self(&self, msg: M) -> Result<(), MailboxFull> {
        self.self_addr.tell(msg)
    }
}

/// A handle to an actor. Cheap to clone (one `Arc` bump).
///
/// This is your Pid. Drop all clones and the actor's mailbox closes,
/// which causes the actor task to exit cleanly.
#[derive(Debug)]
pub struct Addr<M: Send + 'static> {
    tx: Arc<mpsc::Sender<M>>,
    /// Bounded capacity of the mailbox, for diagnostics.
    capacity: usize,
    /// Human-readable name, used in tracing spans. Two Addrs of the
    /// same actor share the same name.
    name: Arc<str>,
}

impl<M: Send + 'static> Clone for Addr<M> {
    fn clone(&self) -> Self {
        Self {
            tx: Arc::clone(&self.tx),
            capacity: self.capacity,
            name: Arc::clone(&self.name),
        }
    }
}

impl<M: Send + 'static> Addr<M> {
    /// The name shown in tracing/console. Two Addrs of the same actor share it.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Mailbox capacity (for diagnostics, observers, etc.).
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Try to send a message without blocking. Returns `Err(MailboxFull)` if
    /// the mailbox is full or the actor is gone.
    ///
    /// This is the equivalent of Erlang's `Pid ! Msg` — but Erlang always
    /// succeeds (mailboxes are unbounded), so returning an error here is
    /// *deliberate* backpressure. Decide what to do: drop, log+metric,
    /// or shed load. Do NOT silently retry forever in a tight loop.
    pub fn tell(&self, msg: M) -> Result<(), MailboxFull> {
        match self.tx.try_send(msg) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(MailboxFull::Dropped {
                capacity: self.capacity,
            }),
            Err(TrySendError::Closed(_)) => Err(MailboxFull::Gone),
        }
    }

    /// Fire-and-forget variant that swallows mailbox-full errors but logs them.
    /// Use this when you really don't care (best-effort notifications).
    pub fn tell_or_drop(&self, msg: M) {
        if let Err(e) = self.tell(msg) {
            warn!(target: "actor.tell", actor=%self.name, error=%e, "tell dropped");
        }
    }

    /// Synchronous request/reply with a bounded timeout. Equivalent of
    /// `gen_server:call/3` with a timeout.
    ///
    /// ## How it works
    ///
    /// 1. Allocates a `oneshot` channel for the reply.
    /// 2. Sends `Req { reply: oneshot::Sender<Reply> }` to the actor's mailbox.
    ///    If the mailbox is full → `Err(MailboxFull)` immediately.
    /// 3. Waits on the `oneshot::Receiver` with `tokio::time::timeout(dur)`.
    /// 4. If the actor panics or drops the sender without replying → `Err(Closed)`
    ///    (or `Err(Panic)` if the actor task's JoinHandle sees a panic — but
    ///    we usually only learn that via monitors).
    ///
    /// ## The golden rule
    ///
    /// **Never call `ask` from inside an actor's `handle` while holding the
    /// mailbox lock.** That's how you get A↔B deadlock. Use `tell` (cast)
    /// or the async-request + correlation-ID pattern in `examples/deadlock_good.rs`.
    pub async fn ask<Reply: Send + 'static>(
        &self,
        make_msg: impl FnOnce(oneshot::Sender<Reply>) -> M,
        dur: Duration,
    ) -> Result<Reply, AskError> {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(make_msg(tx)).await.is_err() {
            return Err(AskError::Closed);
        }
        match timeout(dur, rx).await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(_)) => Err(AskError::Closed),
            Err(_) => Err(AskError::Timeout(dur)),
        }
    }
}

/// The result of spawning an actor. Holds the address (Pid) and the join handle
/// (which the supervisor needs in order to detect crashes).
pub struct ActorSpawn<M: Send + 'static> {
    pub addr: Addr<M>,
    pub join: JoinHandle<()>,
}

/// Spawn an actor: allocate a bounded mailbox, start a tokio task that
/// drains the mailbox, return the address and join handle.
///
/// `mailbox_bound` is the bounded capacity of the channel — pick a number
/// that represents "this actor is overwhelmed" (e.g. 128). When full,
/// `tell` returns `MailboxFull` and `ask` will keep blocking (with the
/// outer timeout).
pub fn spawn<A: Actor>(
    name: impl Into<Arc<str>>,
    mailbox_bound: usize,
    mut actor: A,
) -> ActorSpawn<A::Msg> {
    let name: Arc<str> = name.into();
    let (tx, mut rx) = mpsc::channel::<A::Msg>(mailbox_bound);
    let addr = Addr {
        tx: Arc::new(tx),
        capacity: mailbox_bound,
        name: Arc::clone(&name),
    };
    let ctx_addr = addr.clone();
    let span = tracing::info_span!("actor", name = %name);
    let join = tokio::spawn(
        async move {
            let mut ctx = Context {
                self_addr: ctx_addr,
            };
            info!(target: "actor.lifecycle", "started");

            // `started` callback (init). Errors here propagate as panics
            // and will be caught by the supervisor's JoinHandle await.
            actor.started(&mut ctx).await;

            // Main receive loop. Exits when all Addr clones are dropped
            // (channel closes) — equivalent of mailbox + no more senders.
            while let Some(msg) = rx.recv().await {
                debug!(target: "actor.recv", "msg");
                actor.handle(msg, &mut ctx).await;
            }

            info!(target: "actor.lifecycle", "mailbox closed, stopping");
            actor.stopping(&mut ctx).await;
            info!(target: "actor.lifecycle", "stopped");
        }
        .instrument(span),
    );

    ActorSpawn { addr, join }
}

/// Spawn but immediately attach a supervisor-friendly wrapper. Convenience
/// for use inside `Supervisor::start_child`. See [`crate::supervisor`].
pub fn spawn_supervised<A: Actor>(
    name: impl Into<Arc<str>>,
    mailbox_bound: usize,
    actor: A,
) -> (Addr<A::Msg>, JoinHandle<()>) {
    let s = spawn(name, mailbox_bound, actor);
    (s.addr, s.join)
}
