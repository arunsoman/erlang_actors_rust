//! # DEADLOCK DEMO — the wrong way
//!
//! Demonstrates the classic A↔B deadlock in an actor system: a handler in
//! A synchronously `ask`s B, while B is synchronously `ask`ing A. Both
//! actor tasks are blocked on `await`, neither is draining its mailbox,
//! so the reply that each is waiting for can never be received.
//!
//! In Erlang this is `gen_server:call(A, ...)` from inside `gen_server:call(B, ...)`
//! from inside `gen_server:call(A, ...)` — both processes stuck in
//! `receive` waiting for a reply that the other can't send.
//!
//! In Rust/tokio the trap is identical: the actor task is `await`ing
//! the `ask` future and is NOT draining its mailbox. The outer timeout
//! fires and the `ask` returns `Err(Timeout)`.
//!
//! ## Run it
//!
//! ```sh
//! cargo run --example deadlock_bad
//! ```
//!
//! You'll see both inner `ask`s time out at ~2s and the outer ask at ~4s.
//! No crash, no supervisor restart — just liveness failure.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use erlang_actors_rust::actor::{self, Actor, Addr, Context};
use tracing::{info, warn};

enum AMsg {
    /// "Hey A, please synchronously ask B and return its reply."
    CallB {
        reply: tokio::sync::oneshot::Sender<&'static str>,
    },
}

enum BMsg {
    /// "Hey B, please synchronously ask A and return its reply."
    CallA {
        reply: tokio::sync::oneshot::Sender<&'static str>,
    },
}

/// A2 reads B's address from a shared `OnceLock` at handle-time. This is the
/// chicken-and-egg workaround for two actors that need to know each other's
/// address — equivalent of Erlang's `register/2` with a tiny registry.
struct A2 {
    b_slot: Arc<OnceLock<Addr<BMsg>>>,
}

impl Actor for A2 {
    type Msg = AMsg;
    async fn handle(&mut self, msg: AMsg, _ctx: &mut Context<AMsg>) {
        let AMsg::CallB { reply } = msg;
        let b = match self.b_slot.get() {
            Some(b) => b.clone(),
            None => {
                let _ = reply.send("B not ready");
                return;
            }
        };
        // ❌ WRONG: blocking `ask` inside A's handler.
        // A's task is now parked on this future; A's mailbox is NOT being
        // drained. Any message B sends to A — including the reply to
        // B's own `ask(A)` — sits in A's mailbox until A returns from
        // this handler. But A is waiting on B. But B is waiting on A.
        let r = b
            .ask::<&'static str>(|r| BMsg::CallA { reply: r }, Duration::from_secs(2))
            .await;
        match r {
            Ok(s) => {
                let _ = reply.send(s);
            }
            Err(e) => warn!(error=%e, "A: ask(B) timed out (deadlock!)"),
        }
    }
}

struct B2 {
    a_slot: Arc<OnceLock<Addr<AMsg>>>,
}

impl Actor for B2 {
    type Msg = BMsg;
    async fn handle(&mut self, msg: BMsg, _ctx: &mut Context<BMsg>) {
        let BMsg::CallA { reply } = msg;
        let a = match self.a_slot.get() {
            Some(a) => a.clone(),
            None => {
                let _ = reply.send("A not ready");
                return;
            }
        };
        // ❌ WRONG: same trap as A.
        let r = a
            .ask::<&'static str>(|r| AMsg::CallB { reply: r }, Duration::from_secs(2))
            .await;
        match r {
            Ok(s) => {
                let _ = reply.send(s);
            }
            Err(e) => warn!(error=%e, "B: ask(A) timed out (deadlock!)"),
        }
    }
}

#[tokio::main]
async fn main() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();

    info!("=== deadlock_bad: synchronous ask in both directions ===");

    let a_slot: Arc<OnceLock<Addr<AMsg>>> = Arc::new(OnceLock::new());
    let b_slot: Arc<OnceLock<Addr<BMsg>>> = Arc::new(OnceLock::new());

    let (a_addr, _a_join) = actor::spawn_supervised(
        "a",
        8,
        A2 {
            b_slot: Arc::clone(&b_slot),
        },
    );
    let (b_addr, _b_join) = actor::spawn_supervised(
        "b",
        8,
        B2 {
            a_slot: Arc::clone(&a_slot),
        },
    );

    let _ = a_slot.set(a_addr.clone());
    let _ = b_slot.set(b_addr.clone());

    info!("triggering A.ask(B) → which triggers B.ask(A) → which triggers A.ask(B) → deadlock");
    let start = std::time::Instant::now();
    let outcome = a_addr
        .ask::<&'static str>(|r| AMsg::CallB { reply: r }, Duration::from_secs(4))
        .await;
    let elapsed = start.elapsed();

    info!(?outcome, elapsed = ?elapsed, "outer ask finished");

    info!("=== this is the A↔B synchronous-call deadlock ===");
    info!("The fix is in examples/deadlock_good.rs: use tell (cast) + correlation ID,");
    info!("or keep the dependency graph acyclic (only parent→child asks).");
}
