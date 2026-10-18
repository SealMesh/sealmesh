# Contributing

Thanks for helping. Sealmesh is small on purpose; a change that keeps it small
is the easiest one to merge.

## Before your first pull request

**Sign the [CLA](CLA.md).** The CLA check asks on your first pull request; you
sign once, with a comment, and it covers every later contribution. It lets the
project relicense and offer Sealmesh under other terms, which is how it can be
AGPL-3.0 and still pay for itself.

## How a change lands

1. Fork, and branch from `main`.
2. Commit messages are conventional: `type(scope): imperative message`, for
   example `fix(router): refuse a join for an unmapped repo`. Types: `feat`,
   `fix`, `perf`, `refactor`, `security`, `chore`, `docs`, `test`, `infra`.
3. Open a pull request against `main`, saying what changed in behavior and how
   you checked it.

## Before you push

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
cargo test -p sealmesh-server --all-targets --features dynamodb
cargo deny check advisories bans licenses sources
gitleaks detect --redact
```

That is what CI runs on every PR. Tests live inline in `#[cfg(test)] mod tests`
beside the code. `git config core.hooksPath scripts/hooks` runs the secrets scan
on every commit, before anything leaves your machine.

## Non-negotiable

- **`crates/sealmesh-wire` has no `String` fields.** Every field is an enum, a
  number, a timestamp, or a newtype whose constructor enforces a charset and a
  length. A canary test plants a fixture prompt in every free-text field of a
  job file and asserts it appears nowhere in the serialized snapshot — if that
  test fails, the field is the bug, not the test.
- **`unsafe` is forbidden**, and `unwrap`, `expect`, `panic`, and slice indexing
  are denied outside tests. Answers arrive from another machine through a
  service that can itself be compromised, so a panic in the reader is a denial
  of service someone else can trigger.
- **One router per organization.** The connection registry is in-process; a
  second instance reports every machine attached to the other as unreachable.
- **No credential ever lives in this repository.** Not in code, not in a
  fixture, not in a workflow. The secrets scan runs on every PR.

The security rules the code cites as "rule N", and the checklist a change to
routes, auth, the wire format, the collector or a dependency runs before it
merges, are in [`docs/security-rules.md`](docs/security-rules.md). Read them
before touching any of those.

## Reporting a vulnerability

Not here. See [`SECURITY.md`](SECURITY.md) — a private report, not an issue,
and not a pull request title.
