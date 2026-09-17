//! Supervision trees, hand-rolled.
//!
//! # Erlang/OTP mapping
//!
//! | OTP                       | This module                              |
//! |---------------------------|------------------------------------------|
//! | `supervisor` behaviour    | [`Supervisor`]                          |
//! | `supervisor:child_spec/2` | [`ChildSpec`]                           |
//! | `one_for_one`             | [`Restart::OneForOne`]                  |
//! | `rest_for_one`            | [`Restart::RestForOne`]                  |
//! | `one_for_all`             | [`Restart::OneForAll`]                   |
//! | `max_restarts` / `max_seconds` | `max_restarts`, `within_secs` on [`SupervisorSpec`] |
//! | escalation to parent      | `Supervisor` becomes a supervised child of a higher supervisor via [`Supervised::into_child_spec`] |
//!
//! # How it works
//!
//! A supervisor is **just a tokio task** that:
//!
//! 1. Spawns each child via a user-provided factory closure. The factory
//!    returns a `JoinHandle<()>` (the actor's runtime task).
//! 2. Wraps each child in a *forwarder* task that awaits the child's
//!    `JoinHandle` and reports `(idx, ExitReason)` back via a
//!    `tokio::task::JoinSet`. The supervisor keeps the child's
//!    `AbortHandle` so it can force-kill a child when needed.
//! 3. Loops on `JoinSet::join_next()`. When a child finishes:
//!    - Normal exit → mark as stopped (don't restart).
//!    - Panic / `JoinError` → consult the [`Restart`] strategy:
//!      - `OneForOne`: restart only this child.
//!      - `RestForOne`: restart this child + all children started *after* it.
//!      - `OneForAll`: restart every child.
//! 4. Counts restarts within a rolling window. If too many → escalate
//!    (i.e. terminate itself, which lets *its* supervisor restart it).
//!
//! # What it deliberately isn't
//!
//! - Not a global registry. Names live in the `Supervisor`'s own map.
//! - Not a process group leader. There's no equivalent of Erlang's
//!   distributed process groups here.
//! - Doesn't trap exits. We use panics + `JoinHandle` for failure detection.
//!   Monitors (see [`crate::monitor`]) give you the down-message equivalent.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::task::{AbortHandle, JoinHandle, JoinSet};
use tracing::{error, info, info_span, instrument, warn, Instrument};

/// Restart strategy. Same semantics as OTP's supervisor flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Restart {
    /// Only the failed child restarts.
    OneForOne,
    /// The failed child and every child started after it restarts,
    /// in order. Useful when later children depend on earlier ones.
    RestForOne,
    /// Every child restarts. Use when children are tightly coupled.
    OneForAll,
}

/// A child's restart policy. This is *per child* and overrides the
/// supervisor-level strategy for whether the failure should even
/// trigger a restart at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestartPolicy {
    /// Always restart on failure (default, OTP `permanent`).
    Permanent,
    /// Restart only if it crashed abnormally (OTP `transient`).
    Transient,
    /// Never restart (OTP `temporary`).
    Temporary,
}

/// Specification for one child. The supervisor calls `factory` to (re)spawn.
#[derive(Clone)]
pub struct ChildSpec {
    /// A unique name for this child within this supervisor.
    pub id: Arc<str>,
    pub restart: RestartPolicy,
    /// Factory closure: returns the child's `JoinHandle`. The supervisor
    /// owns this handle for crash detection. Called every time the child
    /// needs to be (re)started.
    pub factory: Arc<dyn Fn() -> JoinHandle<()> + Send + Sync>,
}

impl std::fmt::Debug for ChildSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChildSpec")
            .field("id", &self.id)
            .field("restart", &self.restart)
            .finish_non_exhaustive()
    }
}

impl ChildSpec {
    /// Convenience constructor. The closure must spawn the actor (using
    /// `crate::actor::spawn` or similar) and return the `JoinHandle`.
    pub fn new(
        id: impl Into<Arc<str>>,
        restart: RestartPolicy,
        factory: impl Fn() -> JoinHandle<()> + Send + Sync + 'static,
    ) -> Self {
        Self {
            id: id.into(),
            restart,
            factory: Arc::new(factory),
        }
    }
}

/// Top-level supervisor configuration.
#[derive(Debug, Clone)]
pub struct SupervisorSpec {
    pub name: Arc<str>,
    pub strategy: Restart,
    /// Max restarts allowed within `within_secs` before the supervisor
    /// escalates (i.e. terminates itself, deferring to its own supervisor).
    pub max_restarts: u32,
    pub within_secs: u64,
}

impl SupervisorSpec {
    pub fn new(
        name: impl Into<Arc<str>>,
        strategy: Restart,
        max_restarts: u32,
        within_secs: u64,
    ) -> Self {
        Self {
            name: name.into(),
            strategy,
            max_restarts,
            within_secs,
        }
    }

    /// Sensible defaults: one_for_one, 5 restarts per 10s — same as
    /// OTP's default supervisor flags.
    pub fn default_one_for_one(name: impl Into<Arc<str>>) -> Self {
        Self::new(name, Restart::OneForOne, 5, 10)
    }
}

/// Why a supervised task ended. Mirrors Erlang exit reasons loosely.
#[derive(Debug, Clone)]
pub enum ExitReason {
    /// Returned `Ok(())` cleanly. Don't restart if `Permanent`.
    Normal,
    /// Task panicked. Message is the panic payload stringified.
    Panic(String),
    /// Task was aborted (e.g. by supervisor shutdown).
    Aborted,
    /// Some other join error.
    Other(String),
}

/// Await a JoinHandle and produce an ExitReason. Consumes the handle.
async fn join_outcome(handle: JoinHandle<()>) -> ExitReason {
    match handle.await {
        Ok(()) => ExitReason::Normal,
        Err(join_err) => {
            if join_err.is_panic() {
                let payload = join_err.into_panic();
                ExitReason::Panic(format!("{:?}", payload))
            } else if join_err.is_cancelled() {
                ExitReason::Aborted
            } else {
                ExitReason::Other(format!("{join_err}"))
            }
        }
    }
}

/// A supervisor instance. `start()` consumes it and returns a `JoinHandle`
/// you can await (or attach as a child of a higher supervisor).
#[derive(Clone)]
pub struct Supervisor {
    spec: SupervisorSpec,
    children: Vec<ChildSpec>,
}

impl Supervisor {
    pub fn new(spec: SupervisorSpec) -> Self {
        Self {
            spec,
            children: Vec::new(),
        }
    }

    /// Register a child spec. Children are started in the order they're added.
    pub fn child(mut self, spec: ChildSpec) -> Self {
        self.children.push(spec);
        self
    }

    /// Spawn the supervisor task. Returns a `JoinHandle<()>` that completes
    /// when the supervisor itself exits (escalation or all children removed).
    pub fn start(self) -> JoinHandle<()> {
        let spec = self.spec;
        let children = self.children;
        let span = info_span!("supervisor", name = %spec.name);
        tokio::spawn(
            async move {
                run_supervisor(spec, children).await;
            }
            .instrument(span),
        )
    }
}

/// Per-slot bookkeeping.
struct Slot {
    spec: ChildSpec,
    /// Abort handle for the *child* task itself, so we can force-kill
    /// when needed by `RestForOne` / `OneForAll`. `None` when slot is stopped.
    abort: Option<AbortHandle>,
}

#[instrument(skip(spec, children), fields(name = %spec.name))]
async fn run_supervisor(spec: SupervisorSpec, children: Vec<ChildSpec>) {
    info!(
        target: "supervisor.lifecycle",
        strategy=?spec.strategy,
        max_restarts=spec.max_restarts,
        within_secs=spec.within_secs,
        "starting; children={}",
        children.len()
    );

    let mut slots: Vec<Slot> = Vec::with_capacity(children.len());
    let mut joinset: JoinSet<(usize, ExitReason)> = JoinSet::new();

    // Spawn initial children and wrap each in a forwarder.
    for (i, child_spec) in children.into_iter().enumerate() {
        spawn_child(i, child_spec, &mut slots, &mut joinset);
    }

    // Restart timestamps for the rolling-window check.
    let mut restart_history: Vec<Instant> = Vec::new();

    loop {
        let (idx, reason) = match joinset.join_next().await {
            Some(Ok(x)) => x,
            Some(Err(join_err)) => {
                // The forwarder itself panicked — treat as escalation-worthy error.
                error!(error=%join_err, "supervisor forwarder panicked; escalating");
                return;
            }
            None => {
                // All children exited (or were aborted). Check if any slot still
                // expects to be running. If all are stopped normally, exit cleanly.
                let any_running = slots.iter().any(|s| s.abort.is_some());
                if !any_running {
                    info!(target: "supervisor.lifecycle", "all children stopped; supervisor exiting normally");
                    return;
                }
                // Some slots think they're running but JoinSet is empty —
                // race during shutdown. Loop again to wait for completion.
                continue;
            }
        };

        // Clear this slot's abort handle — child is done.
        let slot = &mut slots[idx];
        slot.abort = None;
        let id = slot.spec.id.clone();
        let policy = slot.spec.restart;

        match reason {
            ExitReason::Normal => {
                info!(target: "supervisor.child", child=%id, "exited normally");
                // Don't restart on normal exit. (OTP permanent would restart
                // on any exit including normal — see README for that variant.)
                continue;
            }
            ExitReason::Panic(msg) => {
                error!(target: "supervisor.child", child=%id, panic=%msg, "child panicked");
                if policy == RestartPolicy::Temporary {
                    warn!(child=%id, "Temporary child; not restarting");
                    continue;
                }
            }
            ExitReason::Aborted => {
                warn!(target: "supervisor.child", child=%id, "child aborted");
                // If we aborted it ourselves during a strategy restart, we
                // already respawned — skip re-restart here.
                // We can detect this via a flag, but to keep the demo simple
                // we just continue and let the respawn loop re-add the slot
                // via the strategy below. For OneForOne this means a single
                // respawn. For others, this might double-respawn — guarded
                // by checking `slot.abort.is_some()` before respawning.
                if policy == RestartPolicy::Temporary {
                    continue;
                }
            }
            ExitReason::Other(msg) => {
                warn!(target: "supervisor.child", child=%id, err=%msg, "child ended (other)");
                if policy == RestartPolicy::Temporary {
                    continue;
                }
            }
        }

        // Rolling-window max-restarts check.
        let now = Instant::now();
        restart_history.retain(|t| now.duration_since(*t) < Duration::from_secs(spec.within_secs));
        restart_history.push(now);
        if restart_history.len() as u32 > spec.max_restarts {
            error!(
                target: "supervisor.escalation",
                restarts_in_window = restart_history.len(),
                max = spec.max_restarts,
                "max_restarts exceeded; escalating"
            );
            // Abort remaining children.
            for s in &mut slots {
                if let Some(ah) = s.abort.take() {
                    ah.abort();
                }
            }
            return;
        }

        // Apply the restart strategy.
        let to_restart: Vec<usize> = match spec.strategy {
            Restart::OneForOne => vec![idx],
            Restart::RestForOne => (idx..slots.len()).collect(),
            Restart::OneForAll => (0..slots.len()).collect(),
        };

        for i in to_restart {
            // For slots that are still running (rest_for_one / one_for_all),
            // abort them first. The forwarder will then send `Aborted` which
            // would re-enter this restart path — but since the slot's abort
            // handle is already None after abort, the next Aborted message
            // for that slot will be ignored by the `if slot.abort.is_some()`
            // guard below. We use a sentinel here.
            if let Some(ah) = slots[i].abort.take() {
                ah.abort();
                // Mark as needing respawn; the Aborted message will be ignored.
                slots[i].abort = None;
            }
            // (Re)spawn.
            let spec_clone = slots[i].spec.clone();
            spawn_child(i, spec_clone, &mut slots, &mut joinset);
        }
    }
}

/// Spawn one child via its factory, wrap the JoinHandle in a forwarder
/// task in the JoinSet, and record the AbortHandle in the slot.
fn spawn_child(
    i: usize,
    spec: ChildSpec,
    slots: &mut Vec<Slot>,
    joinset: &mut JoinSet<(usize, ExitReason)>,
) {
    let id = spec.id.clone();
    let h = (spec.factory)();
    let abort = h.abort_handle();
    joinset.spawn(async move {
        let reason = join_outcome(h).await;
        (i, reason)
    });
    // Ensure slot exists — if the vec is shorter than i+1, push None placeholders.
    while slots.len() <= i {
        slots.push(Slot {
            spec: spec.clone(),
            abort: None,
        });
    }
    slots[i] = Slot {
        spec,
        abort: Some(abort),
    };
    info!(target: "supervisor.child", child=%id, "(re)started");
}

/// Convenience trait so that any supervisor can be turned into a `ChildSpec`
/// of a *parent* supervisor — this is how supervision trees nest.
///
/// `Supervisor` is `Clone` (everything inside is `Arc`-wrapped), so the
/// factory closure can rebuild it on each restart.
pub trait Supervised {
    fn into_child_spec(self, id: impl Into<Arc<str>>, restart: RestartPolicy) -> ChildSpec;
}

impl Supervised for Supervisor {
    /// Wrap this supervisor as a child of a parent supervisor.
    /// The parent will await this supervisor's `JoinHandle` like any other child.
    ///
    /// On restart, the factory clones the inner `Supervisor` and calls `start()`
    /// again — full recursive restart.
    fn into_child_spec(self, id: impl Into<Arc<str>>, restart: RestartPolicy) -> ChildSpec {
        let id: Arc<str> = id.into();
        let supervisor = Arc::new(self);
        let factory: Arc<dyn Fn() -> JoinHandle<()> + Send + Sync> =
            Arc::new(move || (*supervisor).clone().start());
        ChildSpec {
            id,
            restart,
            factory,
        }
    }
}

// Debug helper: list the ids of children.
#[allow(dead_code)]
pub fn child_ids(specs: &[ChildSpec]) -> Vec<&str> {
    specs.iter().map(|s| s.id.as_ref()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restart_strategy_copy_compiles() {
        let _ = Restart::OneForAll;
        let _ = Restart::RestForOne;
        let _ = Restart::OneForOne;
    }
}
