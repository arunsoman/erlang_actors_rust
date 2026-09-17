//! Erlang-style monitors.
//!
//! # Erlang/OTP mapping
//!
//! | OTP                   | This module                  |
//! |-----------------------|------------------------------|
//! | `erlang:monitor/2`   | [`Monitor::watch`]          |
//! | `erlang:demonitor/1` | drop the [`Monitor`] handle |
//! | `{'DOWN', Ref, ...}` | message on the supplied `oneshot` |
//! | `erlang:link/1`      | (not built — see note below) |
//!
//! # Why monitors, not links
//!
//! Erlang's `link/1` is bidirectional — a crash propagates both ways, which
//! often makes failure cascades worse. Monitors are unidirectional: the
//! monitoring process gets a `DOWN` message when the monitored process
//! dies, but the monitored process isn't disturbed. We follow the same
//! rule here: a `Monitor` is a one-way watcher.
//!
//! # How it works
//!
//! We don't actually poll the target actor. Instead we use
//! `tokio::task::JoinHandle::abort_handle` (when wrapping a spawned actor)
//! or a *heartbeat* protocol (when we only have an `Addr` and don't own
//! the JoinHandle):
//!
//! - If you have the actor's `JoinHandle` from `crate::actor::spawn`,
//!   use [`Monitor::from_join_handle`]. We spawn a forwarder that awaits
//!   the handle and sends `DownReason` on the supplied channel.
//! - If you only have an `Addr`, use [`Monitor::watch_heartbeat`]. The
//!   monitored actor must support a `Ping(oneshot::Sender<()>)` message;
//!   we periodically `ask` it and declare it down if it times out
//!   `miss_threshold` times in a row.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::task::{AbortHandle, JoinHandle};
use tracing::{debug, info, warn, Instrument};

/// Why the monitored actor went down. Mirrors Erlang's `Reason` field
/// in `{'DOWN', MonitorRef, process, Pid, Reason}`.
#[derive(Debug, Clone)]
pub enum DownReason {
    /// Returned `Ok(())` cleanly — equivalent of `normal`.
    Normal,
    /// Task panicked. Payload stringified.
    Panic(String),
    /// Task was aborted.
    Aborted,
    /// Heartbeat failed N times in a row (only with `watch_heartbeat`).
    HeartbeatTimeout,
    /// Some other join error.
    Other(String),
}

/// A monitor is a one-way watcher: when the watched task ends, a
/// `DownReason` is delivered to the receiver you supplied. Dropping
/// the `Monitor` cancels the watcher (equivalent of `demonitor/1`).
pub struct Monitor {
    abort: AbortHandle,
}

impl Monitor {
    /// Cancel this monitor — equivalent of `erlang:demonitor/1, [flush]`.
    /// Note: a `DOWN` message already in flight may still be delivered.
    pub fn cancel(self) {
        self.abort.abort();
    }

    /// Watch a `JoinHandle` (from `crate::actor::spawn` or anything else).
    /// When it completes, send the `DownReason` on `tx`.
    ///
    /// This is the most direct equivalent of `erlang:monitor(process, Pid)`
    /// — it relies on the task's `JoinHandle` rather than heartbeats.
    pub fn from_join_handle(
        name: impl Into<Arc<str>>,
        join: JoinHandle<()>,
        tx: oneshot::Sender<DownReason>,
    ) -> Self {
        let name: Arc<str> = name.into();
        let span = tracing::info_span!("monitor", target = %name);
        let handle = tokio::spawn(
            async move {
                let reason = match join.await {
                    Ok(()) => DownReason::Normal,
                    Err(e) => {
                        if e.is_panic() {
                            let payload = e.into_panic();
                            DownReason::Panic(format!("{:?}", payload))
                        } else if e.is_cancelled() {
                            DownReason::Aborted
                        } else {
                            DownReason::Other(format!("{e}"))
                        }
                    }
                };
                info!(target: "monitor.down", reason=?reason, "target down");
                let _ = tx.send(reason);
            }
            .instrument(span),
        );
        Self {
            abort: handle.abort_handle(),
        }
    }

    /// Watch an actor via periodic `Ping` heartbeats. Useful when you don't
    /// own the target's `JoinHandle` (e.g. you only have an `Addr`).
    ///
    /// `ping_factory` builds the ping message given a oneshot reply sender.
    /// The actor must respond to a ping within `ping_timeout`, or it counts
    /// as a miss. After `miss_threshold` consecutive misses, the actor is
    /// declared down via `DownReason::HeartbeatTimeout` on `tx`.
    ///
    /// This is heavier than `from_join_handle` but works across processes,
    /// networks, or any addressable target — closer to Erlang's distributed
    /// monitoring semantics.
    pub async fn watch_heartbeat<M, F>(
        name: impl Into<Arc<str>>,
        addr: crate::actor::Addr<M>,
        ping_factory: F,
        ping_timeout: Duration,
        ping_interval: Duration,
        miss_threshold: u32,
        tx: oneshot::Sender<DownReason>,
    ) -> Self
    where
        M: Send + 'static,
        F: Fn(oneshot::Sender<()>) -> M + Send + Sync + 'static,
    {
        let name: Arc<str> = name.into();
        let span = tracing::info_span!("monitor", target = %name, mode="heartbeat");
        let factory = Arc::new(ping_factory);
        let handle = tokio::spawn(
            async move {
                let mut misses: u32 = 0;
                loop {
                    let (rtx, rrx) = oneshot::channel();
                    let msg = factory(rtx);
                    // Use `tell` (cast) + manual timeout on the reply channel.
                    // Don't use `addr.ask` here — it would allocate a second
                    // oneshot that the actor wouldn't know to reply to.
                    let send_outcome = addr.tell(msg);
                    match send_outcome {
                        Ok(()) => match tokio::time::timeout(ping_timeout, rrx).await {
                            Ok(Ok(())) => {
                                misses = 0;
                                debug!(target: "monitor.heartbeat", "ping ok");
                            }
                            Ok(Err(_)) => {
                                warn!(target: "monitor.heartbeat", "target dropped reply channel; down");
                                let _ = tx.send(DownReason::Other("reply channel closed".into()));
                                return;
                            }
                            Err(_) => {
                                misses += 1;
                                warn!(target: "monitor.heartbeat", misses, threshold=miss_threshold, "ping timeout");
                                if misses >= miss_threshold {
                                    let _ = tx.send(DownReason::HeartbeatTimeout);
                                    return;
                                }
                            }
                        },
                        Err(crate::actor::MailboxFull::Dropped { capacity }) => {
                            misses += 1;
                            warn!(target: "monitor.heartbeat", misses, threshold=miss_threshold, capacity, "mailbox full");
                            if misses >= miss_threshold {
                                let _ = tx.send(DownReason::HeartbeatTimeout);
                                return;
                            }
                        }
                        Err(crate::actor::MailboxFull::Gone) => {
                            warn!(target: "monitor.heartbeat", "target gone; down");
                            let _ = tx.send(DownReason::Other("actor exited".into()));
                            return;
                        }
                    }
                    tokio::time::sleep(ping_interval).await;
                }
            }
            .instrument(span),
        );
        Self {
            abort: handle.abort_handle(),
        }
    }
}
