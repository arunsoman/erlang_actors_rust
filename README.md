# erlang_actors_rust

[![CI](https://github.com/your-org/erlang_actors_rust/actions/workflows/ci.yml/badge.svg)](https://github.com/your-org/erlang_actors_rust/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org/)
[![crates.io](https://img.shields.io/badge/crates.io-not%20published-lightgrey.svg)](https://crates.io)

A small library + examples that bring **Erlang/OTP-style actor semantics to Rust using only `tokio`** as the runtime. No `kameo`, no `ractor`, no `actix` — every mechanism is visible so you can see the Erlang→Rust mapping directly.

> **Not a production framework.** This is a teaching library: the goal is to make
> every Erlang/OTP primitive (spawn, mailbox, `receive after T`, `monitor`,
> supervisor, `one_for_one`, etc.) directly visible in plain Rust/tokio. For real
> systems use [`kameo`](https://crates.io/crates/kameo) or
> [`ractor`](https://crates.io/crates/ractor) — they assemble the same primitives
> for you, but you can't see inside them.

## What's here

```
src/
├── lib.rs           # Module docs + re-exports
├── actor.rs         # Actor trait, Addr (Pid), Context, ask/tell, bounded mailbox
├── supervisor.rs    # Supervisor, ChildSpec, one_for_one/rest_for_one/one_for_all
└── monitor.rs       # Monitor: join-handle-based + heartbeat-based (DOWN messages)
examples/
├── worker_pool.rs   # Supervised worker pool with crash injection + restart
├── deadlock_bad.rs  # A↔B synchronous-ask deadlock (times out, liveness failure)
├── deadlock_good.rs # Same scenario, fixed with tell + correlation ID
└── monitors.rs      # Monitor patterns (join-handle + heartbeat)
```

## Quick start

```sh
git clone https://github.com/your-org/erlang_actors_rust
cd erlang_actors_rust
cargo run --example worker_pool
```

Requires Rust 1.75+ (uses native async-in-trait).

## Erlang → Rust mapping

| Erlang/OTP                          | This crate                                                        |
|-------------------------------------|-------------------------------------------------------------------|
| `spawn/1` + mailbox                 | `actor::spawn` (`tokio::task` + bounded `mpsc::channel`)        |
| `Pid`                               | `actor::Addr<M>` (cheap clone via `Arc`)                         |
| `gen_server:call/3` (sync req/rep)  | `Addr::ask(make_msg, dur)` — bounded by `tokio::time::timeout`   |
| `gen_server:cast/2` (fire-and-forget)| `Addr::tell(msg)` — non-blocking; returns `MailboxFull` on full  |
| `receive after T`                   | every `ask` is wrapped in `tokio::time::timeout(dur, rx)`        |
| `erlang:monitor/2` / `demonitor/1`  | `Monitor::from_join_handle` / `Monitor::cancel`                  |
| (heartbeat-style monitoring)         | `Monitor::watch_heartbeat` (ping + miss threshold)               |
| `{'DOWN', Ref, process, Pid, Reason}`| `oneshot::Receiver<DownReason>`                                 |
| `supervisor` behaviour              | `supervisor::Supervisor`                                          |
| `child_spec`                        | `supervisor::ChildSpec`                                           |
| `one_for_one`                       | `supervisor::Restart::OneForOne`                                 |
| `rest_for_one`                      | `supervisor::Restart::RestForOne`                                |
| `one_for_all`                       | `supervisor::Restart::OneForAll`                                 |
| `permanent` / `transient` / `temporary` | `supervisor::RestartPolicy` (per-child)                     |
| `max_restarts` / `max_seconds`      | `SupervisorSpec { max_restarts, within_secs }`                  |
| supervision tree nesting             | `Supervisor::into_child_spec` (recursive restart)                |
| `observer`                           | `tokio-console` + `tracing` spans per actor                     |
| unbounded mailboxes                 | ❌ **bounded** `mpsc::channel(BOUND)` (deliberate; backpressure) |

## Run the examples

```sh
# Build everything
cargo build --examples

# Supervised worker pool with crash injection (the main demo)
cargo run --example worker_pool

# A↔B synchronous-ask deadlock (both inner asks time out at ~2s)
cargo run --example deadlock_bad

# Same scenario, fixed with tell + correlation ID (50 concurrent requests succeed)
cargo run --example deadlock_good

# Monitor patterns
cargo run --example monitors
```

Set `RUST_LOG=info` (default) or `RUST_LOG=debug` to see tracing output.

### tokio-console (the Erlang `observer` equivalent)

This crate wires up `console-subscriber`. To use it:

```sh
# 1. Build with the tokio_unstable cfg flag (required for console-subscriber)
RUSTFLAGS="--cfg tokio_unstable" cargo build --examples

# 2. Run the example with the console subscriber enabled
RUSTFLAGS="--cfg tokio_unstable" cargo run --example worker_pool

# 3. In another terminal:
tokio-console
```

You'll see every actor and supervisor as a separate task with its own tracing span, just like `observer` shows processes.

## The checklist, revisited

This implements everything from the "Switching to Rust" checklist:

- [x] **Mental model**: actors owning their state — every `Worker`/`Counter` owns its own `&mut self`, messages are the only API.
- [x] **Direct function calls → messages**: `Addr::ask`/`tell` are the only inter-actor API.
- [x] **Unbounded queues → bounded channels**: `mpsc::channel(BOUND)` everywhere; `Addr::tell` returns `MailboxFull` on full.
- [x] **`.unwrap()` everywhere → panic per actor + supervised restart**: `WorkerMsg::CrashOnNext` demonstrates that a panic in `handle` is caught and the supervisor restarts the actor.
- [x] **Fire-and-forget; every ask gets a timeout**: `Addr::ask(make_msg, dur)` always takes a duration. No unbounded waits.
- [x] **Long compute → `spawn_blocking` / yielding**: documented in `actor.rs` (`tokio::task::yield_now()`); `WorkerMsg::Job` uses `tokio::time::sleep` (which yields) to simulate work.
- [x] **`tokio-console` early**: wired up via `console-subscriber`.

## What it deliberately isn't

- **Not preemptive.** Tokio tasks are cooperative. Any long CPU loop in `handle` MUST call `tokio::task::yield_now()` periodically or move to `spawn_blocking`. There's no BEAM-style reduction counting to save you.
- **Not process-isolated.** A `unsafe` segfault takes the whole binary down. For real fault isolation of critical components, run them as separate OS processes or containers talking over IPC.
- **Not a global registry.** The chicken-and-egg problem of two actors knowing each other's address (see `deadlock_*.rs`) is solved with `OnceLock<Addr<M>>` in the examples. In Erlang you'd use `register/2`; in a real Rust app you'd build a small `registry` actor.
- **No `simple_one_for_one` equivalent.** Children are statically registered at supervisor build time. For dynamic pools, build a tiny `pool_sup` actor that manages a `Vec<Addr<...>>` itself.

## Files worth reading in order

1. `src/actor.rs` — start here. `Actor` trait, `Addr::ask/tell`, bounded mailbox, `Context`.
2. `examples/worker_pool.rs` — the most complete example. Reads top-to-bottom.
3. `src/supervisor.rs` — the supervision loop, restart strategies, escalation.
4. `examples/deadlock_bad.rs` → `examples/deadlock_good.rs` — the most important pair. Shows the trap and the fix.
5. `src/monitor.rs` + `examples/monitors.rs` — how to detect actor death asynchronously.

## Honest limitations

This is a teaching library. For production use consider:

- **`kameo`** — built on tokio, gives you `gen_server`-like actors with supervision trees out of the box. Less code, more magic. Uses an `ActorRef` that's almost identical to our `Addr`.
- **`ractor`** — another popular Erlang-style actor framework, with a slightly different API and built-in distributed registry support.
- **`actix`** — the granddaddy; pre-tokio, has its own runtime. Battle-tested but heavier.

If your goal is "ship a real system", use `kameo`. If your goal is "understand how Erlang's primitives map to Rust", read this code.

## License

MIT — see [LICENSE](LICENSE).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). PRs that add new examples or fix bugs
are very welcome. PRs that turn this into a full framework are not — use
`kameo` instead.

## Changelog

See [CHANGELOG.md](CHANGELOG.md).
