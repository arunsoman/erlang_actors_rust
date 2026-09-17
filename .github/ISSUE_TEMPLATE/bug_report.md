---
name: Bug report
about: Something doesn't behave like Erlang/OTP does, or crashes, or hangs
labels: bug
---

## What I expected

A clear description of what you thought would happen, with the Erlang/OTP
behaviour as the reference point when relevant.

## What actually happened

What you observed instead — including any panic backtraces, log output,
timeouts, etc. If the process hangs, say so explicitly.

## Reproduction

The smallest reproduction case you can manage. Ideally a diff against one of
the existing examples, or a new example file.

```sh
# Commands to run
cargo run --example ...
```

## Environment

- Rust version (`rustc --version`):
- Crate version (`erlang_actors_rust` git SHA or tag):
- OS:
- If you saw a panic: `RUST_BACKTRACE=1` output.
