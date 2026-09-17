//! # Monitors demo
//!
//! Two patterns:
//!
//! 1. **`Monitor::from_join_handle`** — the direct equivalent of
//!    `erlang:monitor(process, Pid)`. You pass in a `JoinHandle` (which
//!    you get from `crate::actor::spawn`), and a `oneshot::Sender<DownReason>`.
//!    When the actor ends, you get a `DownReason` on the channel.
//!
//! 2. **`Monitor::watch_heartbeat`** — for when you only have an `Addr`,
//!    not a `JoinHandle` (e.g. you got the address from a registry, or
//!    across a network). Periodically pings the actor; declares it down
//!    after N consecutive misses.
//!
//! ## Run it
//!
//! ```sh
//! cargo run --example monitors
//! ```

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use erlang_actors_rust::actor::{self, Actor, Addr, Context};
use erlang_actors_rust::monitor::{DownReason, Monitor};
use tracing::info;

enum CounterMsg {
    Incr,
    Get(tokio::sync::oneshot::Sender<u64>),
    Ping(tokio::sync::oneshot::Sender<()>),
    Crash,
}

struct Counter {
    count: u64,
}

impl Actor for Counter {
    type Msg = CounterMsg;

    async fn handle(&mut self, msg: CounterMsg, _ctx: &mut Context<CounterMsg>) {
        match msg {
            CounterMsg::Incr => self.count += 1,
            CounterMsg::Get(reply) => {
                let _ = reply.send(self.count);
            }
            CounterMsg::Ping(reply) => {
                let _ = reply.send(());
            }
            CounterMsg::Crash => panic!("Counter crashed on demand"),
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

    info!("=== monitors demo ===");

    // ─── Pattern 1: join-handle-based monitor ────────────────────────────
    info!("[1] spawning Counter with a join-handle Monitor");
    let (counter_addr, counter_join) =
        actor::spawn_supervised("counter_v1", 16, Counter { count: 0 });
    let (down_tx, down_rx) = tokio::sync::oneshot::channel::<DownReason>();
    let _mon = Monitor::from_join_handle("counter_v1", counter_join, down_tx);

    // Drive the counter: a few increments, a get, then trigger the crash.
    for _ in 0..3 {
        let _ = counter_addr.tell(CounterMsg::Incr);
    }
    let v = counter_addr
        .ask::<u64>(CounterMsg::Get, Duration::from_millis(500))
        .await
        .expect("get");
    info!(count = v, "got counter value");

    info!("sending Crash — the monitor should fire with DownReason::Panic");
    let _ = counter_addr.tell(CounterMsg::Crash);
    let reason = down_rx.await.expect("monitor dropped without sending");
    info!(?reason, "DOWN received");

    // ─── Pattern 2: heartbeat-based monitor ─────────────────────────────
    info!("[2] spawning Counter with a heartbeat Monitor (no JoinHandle access)");
    let (counter_addr2, _counter_join2) =
        actor::spawn_supervised("counter_v2", 16, Counter { count: 0 });

    // Stash the address so a "watcher" task can also use it.
    let slot: Arc<OnceLock<Addr<CounterMsg>>> = Arc::new(OnceLock::new());
    let _ = slot.set(counter_addr2.clone());

    let (hb_down_tx, hb_down_rx) = tokio::sync::oneshot::channel::<DownReason>();
    let _hb_mon = Monitor::watch_heartbeat(
        "counter_v2",
        counter_addr2.clone(),
        CounterMsg::Ping,
        Duration::from_millis(200), // ping_timeout
        Duration::from_millis(100), // ping_interval
        3,                          // miss_threshold
        hb_down_tx,
    )
    .await;

    info!("heartbeat monitor running; will declare down after 3 misses (~700ms)");
    // Don't crash the counter — let it just go silent (no, we can't easily
    // simulate "slow but alive" here without adding a Slow variant). Instead,
    // prove the monitor stays healthy on a live actor: wait 1s and verify
    // no DOWN message arrived.
    let healthy_check = tokio::time::timeout(Duration::from_secs(1), hb_down_rx).await;
    match healthy_check {
        Ok(reason) => info!(?reason, "unexpected DOWN during healthy period"),
        Err(_) => info!("no DOWN in 1s — actor healthy (as expected)"),
    }

    // Now let's actually kill the actor and watch the heartbeat detect it
    // via AskError::Closed → DownReason::Other("actor exited").
    // We spawn a fresh monitor since the old one consumed the oneshot.
    let (hb_down_tx2, hb_down_rx2) = tokio::sync::oneshot::channel::<DownReason>();
    let hb_mon2 = Monitor::watch_heartbeat(
        "counter_v2_b",
        counter_addr2.clone(),
        CounterMsg::Ping,
        Duration::from_millis(200),
        Duration::from_millis(100),
        3,
        hb_down_tx2,
    )
    .await;

    info!("crashing counter_v2 to see if heartbeat monitor detects via Closed error...");
    let _ = counter_addr2.tell(CounterMsg::Crash);
    let reason2 = hb_down_rx2
        .await
        .expect("heartbeat monitor should have fired");
    info!(
        ?reason2,
        "DOWN received (should be Other(\"actor exited\"))"
    );
    hb_mon2.cancel();

    info!("=== monitors demo done ===");
}
