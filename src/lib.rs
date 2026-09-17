//! # erlang_actors_rust
//!
//! A small library that brings Erlang/OTP-style actor semantics to Rust using
//! only `tokio` as the runtime. No `kameo`, no `ractor` — every mechanism is
//! visible so you can see the Erlang → Rust mapping directly.
//!
//! ## What you get
//!
//! | Erlang/OTP concept         | Where it lives in this crate                          |
//! |----------------------------|------------------------------------------------------|
//! | `spawn` + mailbox          | [`actor`] module (`Addr`, `Actor`, `Context`)       |
//! | `gen_server:call/2`        | `Addr::ask` (with `tokio::time::timeout`)            |
//! | `gen_server:cast/2`        | `Addr::tell`                                         |
//! | `receive after T`          | every `ask` is wrapped in `tokio::time::timeout`    |
//! | supervisor                 | [`supervisor`] module (`Supervisor`, `Restart`)    |
//! | monitors / links           | [`monitor`] module (`Monitor`, down messages)        |
//! | bounded mailbox + backpressure | `mpsc::channel(BOUND)`                          |
//! | observer                   | `tokio-console` + `tracing` spans per actor         |
//!
//! ## What you don't get
//!
//! - BEAM-style preemptive scheduling. Tokio tasks are *cooperative* — any
//!   long CPU loop in an actor handler MUST call `tokio::task::yield_now()`
//!   or offload via `spawn_blocking`. See [`actor::Actor`] docs.
//! - Process-level isolation. A `unsafe` segfault takes the whole binary down.
//!   For real fault isolation run separate OS processes.
//!
//! ## Quick start
//!
//! ```no_run
//! use erlang_actors_rust::{actor::{Actor, Context}, supervisor::{Supervisor, Restart, SupervisorSpec}};
//!
//! struct Counter { count: i64 }
//! enum CounterMsg { Incr, Get(tokio::sync::oneshot::Sender<i64>) }
//!
//! impl Actor for Counter {
//!     type Msg = CounterMsg;
//!     async fn handle(&mut self, msg: Self::Msg, _ctx: &mut Context<Self::Msg>) {
//!         match msg {
//!             CounterMsg::Incr => self.count += 1,
//!             CounterMsg::Get(r) => { let _ = r.send(self.count); }
//!         }
//!     }
//! }
//! ```
//!
//! See `examples/worker_pool.rs` for the full supervised worker-pool demo.

pub mod actor;
pub mod monitor;
pub mod supervisor;

pub use actor::{Actor, Addr, AskError, Context, MailboxFull};
pub use monitor::{DownReason, Monitor};
pub use supervisor::{ChildSpec, Restart, Supervised, Supervisor, SupervisorSpec};
