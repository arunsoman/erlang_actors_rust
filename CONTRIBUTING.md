# Contributing

Thanks for your interest in `erlang_actors_rust`! This is a small teaching
library, so contributions that keep the "every mechanism visible" spirit are
especially welcome.

## Quick start

```sh
git clone https://github.com/<your-fork>/erlang_actors_rust
cd erlang_actors_rust
cargo build --examples
cargo run --example worker_pool       # main demo
```

You'll need a recent stable Rust toolchain (1.75+ for native async-in-trait).
The repo is intentionally dependency-light; if you want to add a crate,
open an issue first to discuss.

## What kinds of contributions are welcome

- **Bug fixes** — anything that makes the examples fail to compile or behave
  incorrectly. Open an issue with the failing case, then a PR.
- **New examples** — keep them small (<300 lines) and self-contained. Each
  example should demonstrate exactly one Erlang/OTP concept mapped to Rust.
- **Documentation improvements** — the README, the inline rustdoc, and this
  CONTRIBUTING file are all fair game. Fix typos, clarify confusing bits,
  add cross-references.
- **Performance / correctness patches** — the supervisor in particular has
  edge cases around concurrent restarts and `max_restarts` windowing.
  Tests that exercise those are very welcome.

## What kinds of contributions probably belong elsewhere

- A full `gen_server` framework → use [`kameo`](https://crates.io/crates/kameo)
  or [`ractor`](https://crates.io/crates/ractor) instead.
- Distributed actor registries, network transparency, etc. — out of scope
  for this teaching repo.
- New `Actor` trait variants or API redesigns that would obscure the
  Erlang→Rust mapping.

## Before opening a PR

Please run all of:

```sh
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test --all
cargo build --examples
```

CI runs the same gates (see `.github/workflows/ci.yml`).

## Commit message style

Conventional Commits are appreciated but not required:

```
feat(supervisor): add simple_one_for_one equivalent
fix(actor): drop mailbox handle on shutdown to unblock callers
docs: clarify the difference between Monitor and link
```

## Branch and PR flow

1. Fork the repo and create a feature branch off `main`.
2. Push to your fork and open a PR against `main`.
3. CI runs on the PR — please address any failures.
4. Squash-merge is the default; rebase if you have noisy history.

## Issue labels

Issues are loosely labeled:

- `bug` — incorrect behavior or crash.
- `example` — a new example request.
- `docs` — documentation improvement.
- `question` — usage question; will be answered then closed.

## Code of conduct

Be kind. Be concrete. Provide a failing test case where possible.

