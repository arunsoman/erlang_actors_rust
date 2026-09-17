//! # DEADLOCK-FREE — async + correlation ID
//!
//! Same scenario as `deadlock_bad.rs`, but solved. The key change:
//! **actors don't block on `ask` inside their handlers**. Instead they:
//!
//! 1. Receive a request via `tell` (cast) — fire-and-forget from the caller's POV.
//! 2. When they need to "ask" the other actor, they send the other actor a
//!    `tell` containing a correlation ID + reply channel.
//! 3. When the other actor responds (also via `tell`), the original actor
//!    correlates the response back to the original caller's reply channel
//!    via an in-actor map.
//!
//! This is the standard Erlang pattern: stateless `handle_cast` in both
//! directions, no `gen_server:call` across peers, no `receive` inside
//! a `handle_call`. The dependency graph stays acyclic.
//!
//! ## Run it
//!
//! ```sh
//! cargo run --example deadlock_good
//! ```

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use erlang_actors_rust::actor::{self, Actor, Addr, Context};
use tracing::{info, warn};
use uuid::Uuid;

// ─── A's protocol ──────────────────────────────────────────────────────────

enum AMsg {
    /// Initial message from outside: "ask B and reply to me."
    /// Uses a correlation ID we'll use to look up the caller's reply channel
    /// when B's reply comes back.
    StartCallB {
        reply: tokio::sync::oneshot::Sender<&'static str>,
    },
    /// B is responding to a call we initiated.
    BResponse {
        correlation: Uuid,
        payload: &'static str,
    },
}

// ─── B's protocol ──────────────────────────────────────────────────────────

enum BMsg {
    /// A is asking B (asynchronously) for some value.
    ARequest {
        correlation: Uuid,
        reply_to: Addr<AMsg>,
    },
}

// ─── A's actor ────────────────────────────────────────────────────────────

struct A {
    b_slot: Arc<OnceLock<Addr<BMsg>>>,
    /// Pending callers waiting for B's reply, keyed by correlation ID.
    pending: HashMap<Uuid, tokio::sync::oneshot::Sender<&'static str>>,
}

impl Actor for A {
    type Msg = AMsg;

    async fn handle(&mut self, msg: AMsg, _ctx: &mut Context<AMsg>) {
        match msg {
            AMsg::StartCallB { reply } => {
                // Generate a correlation ID, stash the reply channel, then
                // `tell` B (cast) — DO NOT `ask` B and block here.
                let correlation = Uuid::new_v4();
                self.pending.insert(correlation, reply);
                let b = match self.b_slot.get() {
                    Some(b) => b.clone(),
                    None => {
                        warn!("B not registered yet");
                        return;
                    }
                };
                // Cast to B. If B is gone / mailbox full, the pending entry
                // would leak — a production version needs a timeout sweeper
                // that drops pending entries and replies with an error.
                let reply_to = _ctx.self_addr.clone();
                if let Err(e) = b.tell(BMsg::ARequest {
                    correlation,
                    reply_to,
                }) {
                    warn!(error=%e, "A: tell(B) failed; removing pending entry");
                    if let Some(r) = self.pending.remove(&correlation) {
                        let _ = r.send("B unreachable");
                    }
                }
            }
            AMsg::BResponse {
                correlation,
                payload,
            } => {
                // B replied. Look up the caller's reply channel and forward.
                if let Some(reply) = self.pending.remove(&correlation) {
                    let _ = reply.send(payload);
                } else {
                    warn!(%correlation, "B replied for unknown correlation (late or duplicate?)");
                }
            }
        }
    }
}

// ─── B's actor ────────────────────────────────────────────────────────────

struct B2;

impl Actor for B2 {
    type Msg = BMsg;

    async fn handle(&mut self, msg: BMsg, _ctx: &mut Context<BMsg>) {
        let BMsg::ARequest {
            correlation,
            reply_to,
        } = msg;
        // B does its work synchronously here, then `tell`s A the result.
        // B never `ask`s A — the dependency graph stays acyclic (A→B→A's mailbox).
        tokio::time::sleep(Duration::from_millis(5)).await;
        let payload: &'static str = "hello from B";
        if let Err(e) = reply_to.tell(AMsg::BResponse {
            correlation,
            payload,
        }) {
            warn!(error=%e, "B: tell(A) failed");
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

    info!("=== deadlock_good: async + correlation ID ===");

    let a_slot: Arc<OnceLock<Addr<AMsg>>> = Arc::new(OnceLock::new());
    let b_slot: Arc<OnceLock<Addr<BMsg>>> = Arc::new(OnceLock::new());

    let (a_addr, _a_join) = actor::spawn_supervised(
        "a",
        8,
        A {
            b_slot: Arc::clone(&b_slot),
            pending: HashMap::new(),
        },
    );
    let (b_addr, _b_join) = actor::spawn_supervised("b", 128, B2);

    let _ = a_slot.set(a_addr.clone());
    let _ = b_slot.set(b_addr.clone());

    info!("calling A.ask(CallB) — A will tell B, B will tell A back, A will reply to us");
    let start = std::time::Instant::now();
    let outcome = a_addr
        .ask::<&'static str>(|r| AMsg::StartCallB { reply: r }, Duration::from_secs(2))
        .await;
    let elapsed = start.elapsed();

    info!(?outcome, elapsed = ?elapsed, "outer ask finished — no deadlock, no timeouts");

    // Run several to show it's stable, not a fluke.
    info!("running 50 more requests concurrently...");
    let mut tasks = Vec::new();
    for _ in 0..50 {
        let a = a_addr.clone();
        tasks.push(tokio::spawn(async move {
            a.ask::<&'static str>(|r| AMsg::StartCallB { reply: r }, Duration::from_secs(2))
                .await
        }));
    }
    let mut real_ok = 0u32;
    let mut unreachable = 0u32;
    let mut err = 0u32;
    for t in tasks {
        match t.await {
            Ok(Ok("hello from B")) => real_ok += 1,
            Ok(Ok(_)) => unreachable += 1,
            _ => err += 1,
        }
    }
    info!(real_ok, unreachable, err, "concurrent batch finished");

    info!("=== the dependency graph A→B→A's mailbox is acyclic ===");
    info!("No actor ever blocks on `ask` inside its handler. The timeout is just a safety net.");
}
