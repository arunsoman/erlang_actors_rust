//! # Worker pool with supervision + backpressure
//!
//! Demonstrates everything in one place:
//!
//! - A bounded job queue (`mpsc::channel(BOUND)`).
//! - N worker actors pulling jobs; each worker periodically panics to
//!   simulate faults, so the supervisor can demonstrate `OneForOne`
//!   restart strategy.
//! - A `Supervisor` watching all workers.
//! - Every `ask` is wrapped in a timeout (no unbounded waits).
//! - `tracing` + `tokio-console` wiring so you can observe like Erlang's
//!   `observer` (run with `--features tokio-console` / set
//!   `RUSTFLAGS="--cfg tokio_unstable"` and `tokio-console`).
//!
//! ## Run it
//!
//! ```sh
//! cargo run --example worker_pool
//! ```

use std::sync::Arc;
use std::time::Duration;

use erlang_actors_rust::{
    actor::{self, Actor, Addr, Context},
    supervisor::{ChildSpec, Restart, RestartPolicy, Supervisor, SupervisorSpec},
};
use tracing::{info, warn};

/// Messages workers understand.
enum WorkerMsg {
    /// Run a job; reply with the result on the embedded oneshot.
    Job {
        payload: u64,
        reply: tokio::sync::oneshot::Sender<u64>,
    },
    /// Ping — used by external heartbeat monitors (see examples/monitors.rs).
    #[allow(dead_code)]
    Ping(tokio::sync::oneshot::Sender<()>),
    /// Worker should crash on the next job (used to demo restart).
    CrashOnNext,
}

struct Worker {
    id: u32,
    /// When true, the next `Job` panics — simulating a fault.
    crash_on_next: bool,
    /// Count of jobs processed (logged on shutdown for visibility).
    processed: u64,
}

impl Actor for Worker {
    type Msg = WorkerMsg;

    async fn started(&mut self, _ctx: &mut Context<WorkerMsg>) {
        info!(worker = self.id, "started");
    }

    async fn handle(&mut self, msg: WorkerMsg, _ctx: &mut Context<WorkerMsg>) {
        match msg {
            WorkerMsg::Job { payload, reply } => {
                if self.crash_on_next {
                    self.crash_on_next = false;
                    panic!("worker {} simulating crash on payload {}", self.id, payload);
                }
                // Pretend to do work. NOTE: we use `tokio::time::sleep`
                // not `std::thread::sleep` — that's the cooperative-yield
                // version, equivalent to a BEAM reduction-yield.
                tokio::time::sleep(Duration::from_millis(10)).await;
                self.processed += 1;
                let result = payload.wrapping_mul(2);
                let _ = reply.send(result);
            }
            WorkerMsg::Ping(reply) => {
                let _ = reply.send(());
            }
            WorkerMsg::CrashOnNext => {
                self.crash_on_next = true;
                info!(worker = self.id, "will crash on next job");
            }
        }
    }

    async fn stopping(&mut self, _ctx: &mut Context<WorkerMsg>) {
        info!(worker = self.id, processed = self.processed, "stopping");
    }
}

#[tokio::main]
async fn main() {
    // tracing_subscriber with env_filter — set RUST_LOG=info to see all.
    // For tokio-console support, run with RUSTFLAGS="--cfg tokio_unstable"
    // and start `console-subscriber`.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();

    info!("=== worker pool demo ===");

    // We need a way to look up worker Addrs by id after restart.
    // In Erlang you'd use a registry (global:register_name/2 etc.).
    // Here we use a simple Arc<Mutex<HashMap>> shared across the
    // factory closures.
    let registry: Arc<std::sync::Mutex<std::collections::HashMap<u32, Addr<WorkerMsg>>>> =
        Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));

    // Build the supervisor with 4 worker children, each via a factory.
    let mut sup = Supervisor::new(SupervisorSpec::new(
        "pool_sup",
        Restart::OneForOne,
        20, // max restarts
        30, // within 30 secs
    ));

    for worker_id in 0..4u32 {
        let reg = Arc::clone(&registry);
        sup = sup.child(ChildSpec::new(
            format!("worker-{worker_id}"),
            RestartPolicy::Permanent,
            move || {
                let wid = worker_id;
                let reg = Arc::clone(&reg);
                let actor_inst = Worker {
                    id: wid,
                    crash_on_next: false,
                    processed: 0,
                };
                let (addr, join) = actor::spawn_supervised(
                    format!("worker-{wid}"),
                    16, // bounded mailbox of 16 — backpressure!
                    actor_inst,
                );
                // Register the new addr so callers can find it post-restart.
                reg.lock().unwrap().insert(wid, addr);
                join
            },
        ));
    }

    // Start the supervisor (returns JoinHandle<()>).
    let sup_join = sup.start();

    // Give children a moment to start.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Now drive the pool from a producer loop: send jobs and read replies.
    // We rate-limit + use bounded mailbox so backpressure becomes visible
    // if workers can't keep up.
    let total_jobs: u64 = 100;
    let mut completed: u64 = 0;
    let mut failures: u64 = 0;

    for i in 0..total_jobs {
        // Pick a worker round-robin (after restart the registry is updated).
        let worker_id = (i % 4) as u32;
        let addr = {
            let reg = registry.lock().unwrap();
            reg.get(&worker_id).cloned().expect("worker missing")
        };

        // Inject a fault every 25 jobs to demonstrate restart.
        if i > 0 && i % 25 == 0 {
            info!(worker = worker_id, "injecting CrashOnNext");
            let _ = addr.tell(WorkerMsg::CrashOnNext);
        }

        let result = addr
            .ask::<u64>(
                |reply| WorkerMsg::Job { payload: i, reply },
                Duration::from_millis(500), // bounded wait — equivalent of receive after T
            )
            .await;

        match result {
            Ok(v) => {
                completed += 1;
                if i % 10 == 0 {
                    info!(job = i, result = v, "completed");
                }
            }
            Err(e) => {
                failures += 1;
                warn!(job = i, error = %e, "ask failed");
                // After a crash, the registry has been updated by the factory
                // to point at the new worker. The next loop iteration will
                // pick up the new Addr.
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    info!(completed, failures, "all jobs attempted");

    // Clean shutdown: drop the supervisor's JoinHandle (which doesn't kill
    // the task) then explicitly abort. In a real app you'd send a shutdown
    // signal to each worker and await their exits.
    // Here we just give it a moment then abort for demo purposes.
    tokio::time::sleep(Duration::from_millis(200)).await;
    sup_join.abort();
    info!("=== demo complete ===");
}
