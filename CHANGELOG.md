# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Planned
- `simple_one_for_one`-equivalent: dynamic child spawning under a supervisor.
- Timeout sweeper for the `deadlock_good` correlation-ID pattern (currently
  pending entries can leak if the responder dies before replying).
- A small `registry` actor so the `OnceLock` workaround in `deadlock_*.rs`
  examples can be replaced with the OTP-style `register/2` pattern.

## [0.1.0] - 2026-09-17

### Added
- Initial release.
- `actor` module: `Actor` trait (native async-in-trait), `Addr<M>` (Pid),
  bounded `mpsc` mailbox, `tell` (cast), `ask` (call with `tokio::time::timeout`),
  `Context`, `MailboxFull` and `AskError` error types.
- `supervisor` module: `Supervisor`, `ChildSpec`, `SupervisorSpec`,
  `Restart::{OneForOne, RestForOne, OneForAll}`,
  `RestartPolicy::{Permanent, Transient, Temporary}`,
  `max_restarts` / `within_secs` escalation, `Supervised::into_child_spec` for
  nested supervision trees.
- `monitor` module: `Monitor::from_join_handle` (direct equivalent of
  `erlang:monitor/2`), `Monitor::watch_heartbeat` (for addresses-only targets),
  `DownReason`, `Monitor::cancel` (equivalent of `demonitor/1`).
- Examples:
  - `worker_pool.rs` — supervised worker pool with crash injection + restart.
  - `deadlock_bad.rs` — A↔B synchronous-ask deadlock (demonstrates the trap).
  - `deadlock_good.rs` — same scenario, fixed with `tell` + UUID correlation ID.
  - `monitors.rs` — both monitor patterns demonstrated end-to-end.
- `tracing` spans per actor + supervisor; `console-subscriber` wired up for
  `tokio-console` (the closest thing to Erlang's `observer`).
- README with full Erlang→Rust mapping table; CONTRIBUTING guide.

[Unreleased]: https://github.com/your-org/erlang_actors_rust/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/your-org/erlang_actors_rust/releases/tag/v0.1.0
